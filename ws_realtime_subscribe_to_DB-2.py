"""
시간대별로 서로 다른 TR 호출코드가 적용됨.
    정규장 실시간 체결가: ccnl_krx
    정규장 예상 체결가: exp_ccnl_krx
    시간외 실시간 체결가: overtime_ccnl_krx
    시간외 예상 체결가: overtime_exp_ccnl_krx

스케줄 모드별 구독 구성 현황
    08:29~08:59: exp_ccnl_krx
    08:59~09:00: exp_ccnl_krx + ccnl_krx
    09:00~15:19: ccnl_krx
    15:20~15:30: ccnl_krx + exp_ccnl_krx
    15:59~18:00: overtime_exp_ccnl_krx + overtime_ccnl_krx

=> 현재 지정된 목록에 대해서만 웹소켓 수신 => 웹소켓 수신 및 저장 잘됨.
(매일 자동화 가능=> 현재 DB-1파일로만 진행, DB-2는 자동화에서 제외함.)

계정2(syw_2) 사용 : 한도 초과 시 계정1(메인)으로 재시도
데이터 저장: /home/ubuntu/Stoc_Kis/data/wss_data-2/
"""
# =============================================================================
# 시간대 운영(구독 + 저장)
# - 스케줄에 따라 웹소켓 구독을 재구성(연결 재시작)합니다.
# - 저장은 SAVE_MODE/time_flag에 의해 on/off 됩니다.
# =============================================================================
#260219 수신대상 종목(업종) 목록 : 26개
CODE_TEXT = """
상상인증권
SK증권
SK증권우
와이투솔루션
비엘팜텍
LS증권
상아프론테크
마이크로컨텍솔
현대ADM
유투바이오
SK이터닉스

"""

# Top5 추가 구독 (False면 CODE_TEXT만 18:00까지 수신, Top50 다운/저장만 수행)
TOP5_ADD = False   # False: Top50 다운·저장 O, Top5 추가/해제/종가매매전환/미수신구독해제 X

# 한도 초과 시 폴백 계정 사용 여부 (True: 한도 초과 시 계정1(메인)으로 재시도)
USE_FALLBACK_ACCOUNT = False

"""
실행/모니터링/종료 명령어

nohup 실행:
  nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/ws_realtime_subscribe_to_DB-2.py > /home/ubuntu/Stoc_Kis/out/wss_realtime_DB-2.out 2>&1 &

로그 모니터링:
  <노협 백그라운드 실행 모니터링>
  tail -f /home/ubuntu/Stoc_Kis/out/wss_realtime_DB-2.out
  <터미널 실행 모니터링>
  tail -f /home/ubuntu/Stoc_Kis/out/logs/wss_DB2_260204.log

프로세스 확인:
pgrep -af ws_realtime_subscribe_to_DB-2.py
pkill -f ws_realtime_subscribe_to_DB-2.py

일괄 종료:현재 떠있는 여러 프로세스를 동시에 종료하는 명령어
  pkill -f ws_realtime_subscribe_to_DB-2.py
  
  <정말 안죽으면>
  pkill -9 -f ws_realtime_subscribe_to_DB-2.py

종료 확인:
  pgrep -af ws_realtime_subscribe_to_DB-2.py || echo "no process"



wss 데이터 저장위치 : /home/ubuntu/Stoc_Kis/data/wss_data-2/
"""

import sys
import time
import shutil
import signal
import threading
import traceback
import queue
from pathlib import Path
from datetime import datetime, time as dtime, timedelta
from enum import Enum, auto
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

import logging
from logging.handlers import RotatingFileHandler

try:
    from telegMsg import tmsg
except Exception:
    tmsg = None

# =============================================================================
# [중요] 라이브러리 INFO 로그(예: "received message >>") 원천 차단
# - 반드시 ka / domestic_stock_functions_ws import "전에" 실행되어야 합니다.
# =============================================================================
logging.basicConfig(level=logging.WARNING, force=True)

# 자주 원본 프레임을 찍는 로거들 레벨 상향
logging.getLogger("websockets").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.ERROR)

# 루트 로거에도 필터를 강하게 추가 (handlers 없어도 logger filter는 동작)
class DropReceivedMessageFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        if "received message >>" in msg:
            return False
        return True

class DropStaleWarningFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        if "[stale] no data >=" in msg:
            return False
        return True

root_logger = logging.getLogger()
root_logger.addFilter(DropReceivedMessageFilter())

KST = ZoneInfo("Asia/Seoul")

LOG_ID = "DB-2"  # 로그/텔레그램 메시지 식별용 (여러 파일 동시 실행 시 구분)

def ts_prefix() -> str:
    return datetime.now(KST).strftime(f"[%y%m%d_%H%M%S_{LOG_ID}]")

def _notify(msg: str, tele: bool = False) -> None:
    logger.info(msg)
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()
    if tele and tmsg is not None:
        try:
            tmsg(msg, "-t")
        except Exception:
            pass


# =============================================================================
# 경로
# =============================================================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROGRAM_NAME = Path(__file__).name
OUT_DIR = SCRIPT_DIR / "data" / "wss_data-2"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = SCRIPT_DIR / "config.json"
SUB_CFG_PATH = SCRIPT_DIR / "config_wss_2.json"  # 구독 관리 전용 (DB-2)

LOG_DIR = SCRIPT_DIR / "out" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

today_yymmdd = datetime.now(KST).strftime("%y%m%d")
FINAL_PARQUET_PATH = OUT_DIR / f"{today_yymmdd}_wss_data.parquet"
PART_DIR = OUT_DIR / "parts"
PART_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR = OUT_DIR / "backup"
LOG_PATH = LOG_DIR / f"wss_DB2_{today_yymmdd}.log"

# =============================================================================
# 로깅 (콘솔 + 파일)
# =============================================================================
logger = logging.getLogger("wss")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)   # ★ nohup 이중출력 방지: INFO는 FileHandler(로그파일)만, WARNING+는 stderr에도 출력

fh = RotatingFileHandler(
    filename=str(LOG_PATH),
    maxBytes=50 * 1024 * 1024,  # 50MB
    backupCount=5,
    encoding="utf-8",
)
fh.setLevel(logging.INFO)

fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
fmt.converter = lambda *args: datetime.now(KST).timetuple()  # UTC → KST
ch.setFormatter(fmt)
fh.setFormatter(fmt)

# root 로거에도 파일 핸들러 추가 (WARNING 이상 파일 기록)
root_fh = RotatingFileHandler(
    filename=str(LOG_PATH),
    maxBytes=50 * 1024 * 1024,  # 50MB
    backupCount=5,
    encoding="utf-8",
)
root_fh.setLevel(logging.WARNING)
root_fh.setFormatter(fmt)
root_fh.addFilter(DropStaleWarningFilter())
root_logger.addHandler(root_fh)

logger.handlers.clear()
logger.addHandler(ch)
logger.addHandler(fh)
logger.propagate = False

# wss 로거에도 필터 적용
logger.addFilter(DropReceivedMessageFilter())

# 라이브러리/모듈 로거에도 필터 적용 (WARNING:domestic_stock_functions_ws... 같은 것도 정리)
logging.getLogger("domestic_stock_functions_ws").addFilter(DropReceivedMessageFilter())

logger.info("=== WSS START ===")
logger.info(f"[paths] parquet={FINAL_PARQUET_PATH}  parts={PART_DIR}  log={LOG_PATH}")

def _cleanup_old_temp_dirs() -> None:
    for p in OUT_DIR.glob("*_backup"):
        shutil.rmtree(p, ignore_errors=True)
    for p in OUT_DIR.glob("*_parts"):
        shutil.rmtree(p, ignore_errors=True)
    PART_DIR.mkdir(parents=True, exist_ok=True)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

_cleanup_old_temp_dirs()

def _cleanup_old_logs() -> None:
    """당일 로그만 남기고 이전 날짜 로그 파일 삭제."""
    prefix = "wss_DB2_"
    for p in LOG_DIR.glob(f"{prefix}*.log*"):
        if today_yymmdd not in p.name:
            try:
                p.unlink()
                logger.info(f"[cleanup] 이전 로그 삭제: {p.name}")
            except Exception as e:
                logger.warning(f"[cleanup] 로그 삭제 실패: {p.name} {e}")

_cleanup_old_logs()

# =============================================================================
# open-trading-api 예제 경로 추가
# =============================================================================
OPEN_API_WS_DIR = Path.home() / "open-trading-api" / "examples_user" / "domestic_stock"
sys.path.append(str(OPEN_API_WS_DIR))

import kis_auth_llm as ka  # noqa: E402
sys.modules["kis_auth"] = ka

from domestic_stock_functions_ws import *  # noqa: F403,E402
from kis_utils import is_holiday, load_config, load_symbol_master, save_config  # noqa: E402
from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig  # noqa: E402

# ★ import * 이 logger 를 덮어쓰므로 재설정
logger = logging.getLogger("wss")

# =============================================================================
# 인증 — 계정2(syw_2)로 접속
# =============================================================================
_cfg_json_auth = load_config(str(SCRIPT_DIR / "config.json"))
_acct2_auth = _cfg_json_auth.get("accounts", {}).get("syw_2", {})
if not _acct2_auth.get("appkey") or not _acct2_auth.get("appsecret"):
    raise ValueError("config.json → accounts.syw_2에 appkey/appsecret이 필요합니다.")

# WSS 접속 credential을 계정2로 오버라이드
ka._cfg["my_app"] = _acct2_auth["appkey"]
ka._cfg["my_sec"] = _acct2_auth["appsecret"]
# 토큰 파일을 계정2 전용으로 분리 (계정1과 충돌 방지)
_syw2_token_path = Path(ka.config_root) / f"KIS_syw2_{datetime.today().strftime('%Y%m%d')}"
ka.token_tmp = str(_syw2_token_path)
if not _syw2_token_path.exists():
    _syw2_token_path.touch()
logger.info(f"[auth] 계정2(syw_2) credential 적용, token_file={ka.token_tmp}")

ka.auth(svr="prod")
ka.auth_ws(svr="prod")
if not getattr(ka, "_base_headers_ws", {}).get("approval_key"):
    raise RuntimeError("auth_ws() 실패: 계정2(syw_2)의 앱키/시크릿을 확인하세요.")

trenv = ka.getTREnv()
htsid = getattr(trenv, "my_htsid", "") if trenv is not None else ""
if not htsid:
    htsid = ka.getEnv().get("my_htsid", "")

# =============================================================================
# 사용자 설정
# =============================================================================
print_option = 2  # 1: 수신 DF print, 2: 초당/누적 카운트 1줄 모니터링

# 저장 모드:
# - "always": 시간대와 무관하게 모두 저장
# - "time": 시간대 플래그에 따라 저장
SAVE_MODE = "always"

# flush 정책 : 60초 또는 1000건 중 먼저 도달하면 저장
FLUSH_EVERY_SEC = 60
FLUSH_EVERY_N_ROWS = 1000

# 15:30 장마감 체결 후 구독 강제 중단 시각 (15:31:00)
CLOSE_STOP_TIME = dtime(15, 31)

# 세션당 종목수 제한(서버 제약): 10개
MAX_CODES_PER_SESSION = 10

# 요약 로그 / stale
SUMMARY_EVERY_SEC = 30
FULL_STATUS_EVERY_SEC = 300  # 5분마다 전체 진행상황 출력
STALE_SEC = 20
NO_DATA_WARN_SEC = 10        # 10초 미수신 시 경고 (연결은 살아있으나 데이터 스트림 끊김 감지)
NO_DATA_REBUILD_SEC = 15     # 15초 미수신 시 강제 재구독 (60초→15초: 프레임 에러 후 빠른 복구)

# 출력 시 "초당 카운트"는 1000을 넘으면 0부터 다시(999 -> 0)
PER_SEC_ROLLOVER = 1000

# 현재가 REST 보강 (1초 미수신 종목 보강)
REST_MAX_PER_SEC = 10
REST_AFTER_WINDOW_DELAY = 0.1

# 장중 미수신 감시 — REST 거래상태 확인
STALE_CHECK_SEC = 60            # 종목별 미수신 기준(초) → REST 상태 확인 트리거
STALE_CHECK_HALTED_SEC = 300    # 거래중단 확인 후 재확인 간격(초)
STALE_CHECK_VI_SEC = 30         # VI 발동 종목 재확인 간격(초)
STALE_CHECK_INACTIVE_SEC = 120  # 거래비활성 종목 재확인 간격(초)

# 상승률 상위 종목 추가 구독
TOP_RANK_ENABLED = True
TOP_RANK_END = dtime(15, 20)           # 15:19 종가매매 종목 선정까지 포함
TOP_RANK_N = 50                        # 상승률 상위 다운로드 수
TOP_RANK_ADD_N = 5                     # 랭킹 결과에서 실제 구독 추가할 최대 종목 수
TOP_RANK_REMOVE_THRESHOLD = 20         # 이 순위 밖으로 밀린 종목만 해제
TOP_RANK_MARKET_DIV = "J"
TOP_RANK_OUT_DIR = Path("/home/ubuntu/Stoc_Kis/data/fetch_top_list-1")
MAX_WSS_SUBSCRIBE = 41                 # WSS 구독 총 상한
# 15:19 종가매매 종목 선정
CLOSING_TOP_N = 10                     # 종가매매용 ST 종목 선정 수


# =============================================================================
# 종목 코드 파싱
# =============================================================================
def _parse_codes(text: str) -> list[str]:
    name_to_code = {}
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"]))
    except Exception:
        name_to_code = {}

    out: list[str] = []
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        if raw.isdigit():
            out.append(raw.zfill(6))
        else:
            c = name_to_code.get(raw)
            if c:
                out.append(c)
    return out


def _normalize_code_list(items: list[object]) -> list[str]:
    lines = [str(x) for x in items if str(x).strip()]
    if not lines:
        return []
    return _parse_codes("\n".join(lines))


def _read_cfg() -> dict | None:
    try:
        return load_config(str(CONFIG_PATH))
    except Exception:
        return None


def _read_sub_cfg() -> dict:
    """구독 관리 전용 config 읽기 (없으면 빈 dict)."""
    try:
        return load_config(str(SUB_CFG_PATH))
    except Exception:
        return {}


def _persist_subscription_codes(all_codes: list[str]) -> None:
    cfg = _read_sub_cfg()
    cfg["wss_subscribe_date"] = today_yymmdd
    cfg["wss_subscribe_codes"] = sorted(set(all_codes))
    # 탑 랭킹으로 추가된 종목도 별도 저장 (재시작 시 복원용)
    cfg["wss_top_added_codes"] = sorted(_top_added_codes) if "_top_added_codes" in globals() else []
    try:
        save_config(cfg, str(SUB_CFG_PATH))
    except Exception:
        pass


_loaded_top_added: list[str] = []  # config에서 복원한 top_added (초기화 전 임시 저장)

def _load_codes_from_cfg(base_codes: list[str]) -> list[str]:
    """TOP5_ADD=True → config 우선 복원, False → CODE_TEXT 원본으로 시작."""
    global _loaded_top_added

    # ── TOP5_ADD=False → 항상 CODE_TEXT 원본으로 시작 (config 복원 안 함) ──
    if not TOP5_ADD:
        _loaded_top_added = []
        logger.info(
            f"[codes] TOP5_ADD=False → CODE_TEXT base({len(base_codes)}개)로 시작"
        )
        return list(base_codes)

    # ── TOP5_ADD=True → 기존 로직: config 우선 복원 ──
    now_t = datetime.now(KST).time()

    # 09:00 이전 시작 → config 초기화, CODE_TEXT 원본으로 시작
    if now_t < dtime(9, 0):
        logger.info(
            f"[codes] 09:00 이전 시작 → config 초기화, CODE_TEXT base({len(base_codes)}개)로 시작"
        )
        _loaded_top_added = []
        return list(base_codes)

    # 09:00 이후 시작 (재시작) → 당일 config 복원
    cfg = _read_sub_cfg()

    cfg_date = str(cfg.get("wss_subscribe_date") or "")
    cfg_codes = cfg.get("wss_subscribe_codes")
    cfg_top_added = cfg.get("wss_top_added_codes", [])

    if isinstance(cfg_codes, list) and cfg_codes:
        norm = _normalize_code_list(cfg_codes)
        if norm:
            _loaded_top_added = [c for c in cfg_top_added if c in set(norm)]
            if cfg_date == today_yymmdd:
                # 당일 재시작 → config 그대로 복원
                return norm
            else:
                # 날짜 변경 → CODE_TEXT base만 사용 (이전 날짜 종목 제거)
                logger.info(
                    f"[codes] 날짜 변경 → CODE_TEXT base({len(base_codes)}개)로 초기화"
                )
                _loaded_top_added = []
                return list(base_codes)
    return base_codes


def _code_name_map() -> dict[str, str]:
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        return dict(zip(sdf["code"], sdf["name"].astype(str)))
    except Exception:
        return {}


_code_text_codes = _parse_codes(CODE_TEXT)  # CODE_TEXT 원본 (보호 대상)
codes = _load_codes_from_cfg(_code_text_codes)
if not codes:
    raise RuntimeError("CODE_TEXT에서 종목코드를 찾지 못했습니다.")
code_name_map = _code_name_map()
logger.info(f"[codes] count={len(codes)} codes={codes}")
_codes_lock = threading.RLock()
# base_codes = CODE_TEXT 원본 + config 복원 중 top_added가 아닌 종목 (보호 대상)
_base_codes = set(c for c in codes if c not in set(_loaded_top_added))
if not _base_codes:
    _base_codes = set(_code_text_codes)
logger.info(f"[codes] base={len(_base_codes)}개 (보호), top_added(복원)={len(_loaded_top_added)}개")
_persist_subscription_codes(codes)


def _chunks(lst: list[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


# =============================================================================
# 시간대별 저장 스위치
# =============================================================================
SAVE_REAL_REGULAR = False     # 정규장 실시간체결 저장
SAVE_EXP_REGULAR = False      # 정규장 예상체결 저장
SAVE_REAL_OVERTIME = False    # 시간외 실시간체결 저장
SAVE_EXP_OVERTIME = False     # 시간외 예상체결 저장

_flags_lock = threading.RLock()

# =============================================================================
# 운영 모드(시간대별 구독 스케줄)
# =============================================================================
class RunMode(Enum):
    PREOPEN_WAIT = auto()
    PREOPEN_EXP = auto()
    REGULAR_REAL = auto()
    CLOSE_EXP = auto()
    CLOSE_REAL = auto()
    OVERTIME_EXP = auto()
    OVERTIME_REAL = auto()
    STOP = auto()
    EXIT = auto()

_mode_lock = threading.RLock()
_current_mode: RunMode | None = None
_ws_rebuild_event = threading.Event()
_kws_lock = threading.RLock()
_active_kws = None


def _trigger_ws_rebuild():
    """종목 변경 시 즉시 동적 구독/해제 적용 (재연결 없이)."""
    with _kws_lock:
        if _active_kws is not None:
            desired = _desired_subscription_map(datetime.now(KST))
            _apply_subscriptions(_active_kws, desired)

    # ── [주석] 재연결 방식 (동적 구독 문제 시 아래 주석 해제, 위 블록 주석 처리) ──
    # _ws_rebuild_event.set()
    # if _get_mode() == RunMode.CLOSE_REAL:
    #     return
    # with _kws_lock:
    #     if _active_kws is not None:
    #         _notify(
    #             f"{ts_prefix()} [ws] 종목 변경 감지 → WS 재연결 "
    #             f"(구독 {len(codes)}종목 전체 재등록)"
    #         )
    #         _request_ws_close(_active_kws)


END_TIME = dtime(18, 0)
_end_time_reached = False   # 18:00 종료 시도 시 True (WS 지연 종료 시 병합 판단용)
_regular_real_seen: set[str] = set()
_close_force_stopped = False          # 15:30 유예시간 후 강제 구독 중단 1회 로그용
_overtime_real_seen: set[str] = set()
_overtime_real_active = False
_overtime_real_started_ts: float = 0.0   # 체결가 전환 시점 (타임아웃용)
OVERTIME_REAL_TIMEOUT = 60               # 60초 내 전 종목 미수신 시 강제 복귀
_last_overtime_real_slot: datetime | None = None
_vi_delayed_codes: set[str] = set()   # VI 발동 의심 종목 (09:00~09:02 예상체결 유지)
_vi_delay_until: datetime | None = None  # VI 지연 종료 시각 (09:02:00)
_last_prdy_ctrt: dict[str, float] = {c: 0.0 for c in codes}
_subscribed: dict[str, set[str]] = {}

def calc_mode(now: datetime) -> RunMode:
    t = now.time()
    if t < dtime(8, 50):
        return RunMode.PREOPEN_WAIT
    if dtime(8, 50) <= t < dtime(9, 0):
        return RunMode.PREOPEN_EXP
    if dtime(9, 0) <= t < dtime(15, 20):
        return RunMode.REGULAR_REAL
    if dtime(15, 20) <= t < dtime(15, 30):
        return RunMode.CLOSE_EXP
    if dtime(15, 30) <= t < dtime(16, 0):
        return RunMode.CLOSE_REAL
    if dtime(16, 0) <= t < END_TIME:
        return RunMode.OVERTIME_EXP if not _overtime_real_active else RunMode.OVERTIME_REAL
    return RunMode.EXIT

def _log_mode_transition(prev: RunMode | None, cur: RunMode) -> None:
    sys.stdout.write("\n")  # 제자리 출력 줄바꿈
    if cur == RunMode.PREOPEN_WAIT:
        _notify(f"{ts_prefix()} 장 개시전까지 대기/", tele=True)
    elif cur == RunMode.PREOPEN_EXP:
        _notify(f"{ts_prefix()} 08:50 예상체결가 구독 시작", tele=True)
    elif cur == RunMode.REGULAR_REAL:
        _notify(f"{ts_prefix()} 09:00 예상체결가 구독 중지 -> 실시간 체결가 구독 시작", tele=True)
    elif cur == RunMode.CLOSE_EXP:
        pass  # _switch_to_closing_codes()에서 메시지 출력 완료 (tele 포함)
    elif cur == RunMode.CLOSE_REAL:
        _notify(f"{ts_prefix()} 15:30 종가 체결가 구독 전환")
    elif cur == RunMode.OVERTIME_EXP:
        if prev == RunMode.OVERTIME_REAL:
            _notify(f"{ts_prefix()} [시간외] 체결가 수신 완료 → 예상체결가 복귀")
        else:
            _notify(f"{ts_prefix()} [시간외] 예상체결가 구독 시작", tele=True)
    elif cur == RunMode.OVERTIME_REAL:
        _notify(f"{ts_prefix()} [시간외] 예상체결가 → 체결가 전환")
    elif cur == RunMode.STOP:
        _notify(f"{ts_prefix()} 모든 구독 종료")
    elif cur == RunMode.EXIT:
        pass  # EXIT 순서는 scheduler_loop에서 직접 제어

def _get_mode() -> RunMode:
    with _mode_lock:
        return _current_mode or calc_mode(datetime.now(KST))

def _final_summary() -> str:
    with _lock:
        saved_rows = _written_rows
        saved_codes = sum(1 for c in codes if _total_counts.get(c, 0) > 0)
    try:
        size_mb = FINAL_PARQUET_PATH.stat().st_size / (1024 * 1024)
    except Exception:
        size_mb = -1.0
    return (
        f"{ts_prefix()} [최종] saved_codes={saved_codes}종목 "
        f"saved_rows={saved_rows} file={FINAL_PARQUET_PATH.name} size_mb={size_mb:.2f}"
    )

def scheduler_loop():
    global _current_mode, _last_rebuild_ts, _last_no_data_warn_ts
    global _overtime_real_active, _vi_delayed_codes, _vi_delay_until, _last_overtime_real_slot
    global _end_time_reached
    logger.info(f"{ts_prefix()} [scheduler] started")
    while not _stop_event.is_set():
        now = datetime.now(KST)
        new_mode = calc_mode(now)
        with _mode_lock:
            old_mode = _current_mode
            if old_mode != new_mode:
                _current_mode = new_mode
                _log_mode_transition(old_mode, new_mode)
                if new_mode == RunMode.CLOSE_REAL:
                    _regular_real_seen.clear()
                    global _close_force_stopped
                    _close_force_stopped = False
                if new_mode == RunMode.OVERTIME_REAL:
                    _overtime_real_seen.clear()
                if new_mode == RunMode.EXIT:
                    # ① 구독 종료 + WS 닫기
                    sys.stdout.write("\n")
                    _notify(f"{ts_prefix()} 시간외 체결가 구독 종료")
                    with _kws_lock:
                        if _active_kws is not None:
                            _request_ws_close(_active_kws)
                    time.sleep(1.0)
                    # ② 남은 버퍼 flush
                    _flush_all(force=True)
                    time.sleep(1.0)
                    # ③ 최종 요약
                    _notify(_final_summary())
                    time.sleep(1.0)
                    # ④ 프로그램 종료 안내 (텔레그램)
                    _notify(f"{ts_prefix()} 18:00 이후 시간외 단일가 종료로 프로그램 종료", tele=True)
                    _end_time_reached = True   # WS 지연 종료 시 finally에서 병합 수행
                    _stop_event.set()
                    break

            # ── VI 종목별 구독 관리: 09:00에 prdy_ctrt>=10% 종목만 예상체결 유지 ──
            if dtime(9, 0) <= now.time() <= dtime(9, 0, 1) and not _vi_delayed_codes and _vi_delay_until is None:
                vi_suspects = {
                    c: v for c, v in _last_prdy_ctrt.items()
                    if v >= 10.0 and c in set(codes)
                }
                if vi_suspects:
                    _vi_delayed_codes = set(vi_suspects.keys())
                    _vi_delay_until = now.replace(hour=9, minute=2, second=0, microsecond=0)
                    vi_names = [f"{code_name_map.get(c,c)}({c})={v:.1f}%" for c, v in vi_suspects.items()]
                    normal_count = len(codes) - len(_vi_delayed_codes)
                    _notify(
                        f"{ts_prefix()} VI 급등 조건 → 종목별 구독 분리: "
                        f"실시간체결 {normal_count}종목, 예상체결유지 {len(_vi_delayed_codes)}종목 "
                        f"(대상: {', '.join(vi_names)})"
                    )
            if _vi_delay_until and now >= _vi_delay_until:
                if _vi_delayed_codes:
                    vi_names = [f"{code_name_map.get(c,c)}({c})" for c in _vi_delayed_codes]
                    _notify(
                        f"{ts_prefix()} VI 지연 해제 → 전 종목 실시간체결 전환 "
                        f"(해제: {', '.join(vi_names)})"
                    )
                _vi_delayed_codes.clear()
                _vi_delay_until = None

            if dtime(16, 9, 59) <= now.time() < dtime(17, 50):
                # 마지막 의미 있는 슬롯: 17:49:59 (17:50 체결가 수신)
                # 17:50 이후에는 새 슬롯 전환 없이 예상체결가 유지 → 18:00 EXIT
                total_min = now.hour * 60 + now.minute
                slot_min = ((total_min - 9) // 10) * 10 + 9
                slot = now.replace(hour=slot_min // 60, minute=slot_min % 60, second=59, microsecond=0)
                if now >= slot and _last_overtime_real_slot != slot:
                    _last_overtime_real_slot = slot
                    _overtime_real_active = True
                    _overtime_real_started_ts = time.time()
                    _overtime_real_seen.clear()
                    _log_mode_transition(_current_mode, RunMode.OVERTIME_REAL)

            if _overtime_real_active:
                ot_elapsed = time.time() - _overtime_real_started_ts
                seen_n = len(_overtime_real_seen)
                total_n = len(codes)
                if seen_n >= total_n:
                    # 전 종목 수신 완료 → 예상체결가 복귀
                    _overtime_real_active = False
                    _overtime_real_seen.clear()
                    _log_mode_transition(_current_mode, RunMode.OVERTIME_EXP)
                elif ot_elapsed >= OVERTIME_REAL_TIMEOUT:
                    # 타임아웃 → 미수신 종목 있어도 예상체결가 복귀
                    _overtime_real_active = False
                    logger.info(
                        f"{ts_prefix()} [시간외] 체결가 {OVERTIME_REAL_TIMEOUT}초 타임아웃: "
                        f"{seen_n}/{total_n}종목 수신"
                    )
                    _overtime_real_seen.clear()
                    _log_mode_transition(_current_mode, RunMode.OVERTIME_EXP)

            with _kws_lock:
                if _active_kws is not None:
                    desired = _desired_subscription_map(now)
                    _apply_subscriptions(_active_kws, desired)
                    # 15:30 종가 체결 전 종목 수신 완료 → WS 닫기
                    if (new_mode == RunMode.CLOSE_REAL
                            and not desired
                            and len(_regular_real_seen) >= len(codes)):
                        if not _close_force_stopped:
                            _close_force_stopped = True
                            _notify(
                                f"{ts_prefix()} 15:30 종가 체결 수신 완료 "
                                f"({len(_regular_real_seen)}/{len(codes)}종목), "
                                f"장 마감으로 전체 구독을 중지하고 16:00까지 대기모드로 진입",
                                tele=True,
                            )
                            _request_ws_close(_active_kws)
                    # 15:31 타임아웃 → 미수신 종목 있어도 WS 닫기
                    elif (new_mode == RunMode.CLOSE_REAL
                            and not desired
                            and now.time() >= dtime(15, 31)):
                        if not _close_force_stopped:
                            _close_force_stopped = True
                            _notify(
                                f"{ts_prefix()} 15:31 타임아웃: 종가 체결 "
                                f"{len(_regular_real_seen)}/{len(codes)}종목 수신, "
                                f"전체 구독을 중지하고 16:00까지 대기모드로 진입",
                                tele=True,
                            )
                            _request_ws_close(_active_kws)

            now_ts = time.time()
            nt = now.time()
            # "no data" 경고 억제 구간:
            # - 15:20~15:31 예상체결가/종가 체결 수신 (데이터 간헐적 → 경고/재구독 무의미)
            # - 15:31 이후 종가 수신 완료 후
            # - 16:00~ 시간외 (거래 적음)
            in_no_data_suppress = (
                nt >= dtime(15, 20)
                or _close_force_stopped
            )
            if _last_any_recv_ts > 0 and not in_no_data_suppress:
                idle_sec = now_ts - _last_any_recv_ts
                if idle_sec >= NO_DATA_WARN_SEC and (now_ts - _last_no_data_warn_ts) >= NO_DATA_WARN_SEC:
                    sys.stdout.write("\n")
                    _notify(
                        f"{ts_prefix()} {int(idle_sec)}초 데이터 미수신 (연결 유지, 데이터 스트림 끊김) -> 재구독 시도"
                    )
                    _last_no_data_warn_ts = now_ts
                if idle_sec >= NO_DATA_REBUILD_SEC and (now_ts - _last_rebuild_ts) >= NO_DATA_REBUILD_SEC:
                    logger.warning(
                        f"{ts_prefix()} [ws] no data for {int(idle_sec)}s -> resubscribe"
                    )
                    with _kws_lock:
                        if _active_kws is not None:
                            _apply_subscriptions(_active_kws, _desired_subscription_map(now), force=True)
                    _last_rebuild_ts = now_ts
        time.sleep(0.5)
    logger.info(f"{ts_prefix()} [scheduler] stopped")

def _in_range(now_t: dtime, start: dtime, end: dtime) -> bool:
    return (now_t >= start) and (now_t < end)

def update_time_flags():
    """
    서울 로컬 시간 기준:
      - 08:50~09:00: 예상체결(정규장) 저장 ON
      - 09:00~15:20: 실시간체결(정규장) 저장 ON
      - 15:20~15:30: 예상체결(정규장) 저장 ON
      - 15:30~16:00: 실시간체결(정규장) 저장 ON
      - 16:00~18:00: 시간외(단일가) 예상/체결 저장 ON
    """
    global SAVE_REAL_REGULAR, SAVE_EXP_REGULAR, SAVE_REAL_OVERTIME, SAVE_EXP_OVERTIME
    now = datetime.now(KST)
    nt = now.time()

    if SAVE_MODE == "always":
        save_real_regular = True
        save_exp_regular = True
        save_real_overtime = True
        save_exp_overtime = True
    else:
        save_real_regular = _in_range(nt, dtime(9, 0), dtime(15, 20)) or _in_range(nt, dtime(15, 30), dtime(15, 31))
        save_exp_regular = _in_range(nt, dtime(8, 50), dtime(9, 0)) or _in_range(nt, dtime(15, 20), dtime(15, 30))

        save_overtime = _in_range(nt, dtime(16, 0), dtime(18, 0))
        save_real_overtime = save_overtime
        save_exp_overtime = save_overtime

    with _flags_lock:
        changed = (
            (SAVE_REAL_REGULAR != save_real_regular) or
            (SAVE_EXP_REGULAR != save_exp_regular) or
            (SAVE_REAL_OVERTIME != save_real_overtime) or
            (SAVE_EXP_OVERTIME != save_exp_overtime)
        )
        SAVE_REAL_REGULAR = save_real_regular
        SAVE_EXP_REGULAR = save_exp_regular
        SAVE_REAL_OVERTIME = save_real_overtime
        SAVE_EXP_OVERTIME = save_exp_overtime

    if changed:
        logger.info(
            "[flags] "
            f"real_regular={SAVE_REAL_REGULAR} exp_regular={SAVE_EXP_REGULAR} "
            f"real_overtime={SAVE_REAL_OVERTIME} exp_overtime={SAVE_EXP_OVERTIME}"
        )

def time_flag_loop():
    logger.info("[timeflags] started")
    while not _stop_event.is_set():
        try:
            update_time_flags()
        except Exception as e:
            logger.error(f"[timeflags] error: {e}")
        time.sleep(1.0)
    logger.info("[timeflags] stopped")

# =============================================================================
# 모니터링(콘솔 1줄)
# - 요청 반영:
#   * 맨 앞에 시간: [yymmdd_hhMMss]
#   * 초당 카운트는 3자리 고정 제거 + 999 넘으면 0부터(=mod 1000)
# =============================================================================
_per_sec_counts = {c: 0 for c in codes}
_total_counts = {c: 0 for c in codes}
_since_save_rows = 0
_since_save_counts = {c: 0 for c in codes}
_last_print_ts = time.time()
_last_full_status_ts = time.time()
_written_rows = 0
_last_status_line = ""
_last_flush_time = time.time()
_last_any_recv_ts = 0.0
_last_rebuild_ts = 0.0
_last_no_data_warn_ts = 0.0

def _fmt_now_prefix() -> str:
    return datetime.now(KST).strftime(f"[%y%m%d_%H%M%S_{LOG_ID}]")

def _reset_recv_counters(reason: str) -> None:
    with _lock:
        for c in _per_sec_counts:
            _per_sec_counts[c] = 0
        for c in _total_counts:
            _total_counts[c] = 0
        for c in _since_save_counts:
            _since_save_counts[c] = 0
        for c in _last_rest_req_ts:
            _last_rest_req_ts[c] = 0.0
        _rest_pending.clear()
        global _since_save_rows, _last_any_recv_ts
        _since_save_rows = 0
        _last_any_recv_ts = 0.0
    logger.info(f"{ts_prefix()} [recv] counters reset ({reason})")

def _print_counts():
    prefix = _fmt_now_prefix()
    with _lock:
        per_sec_total = sum(_per_sec_counts.values())
        recv_rows = _since_save_rows
        since_active = sum(1 for c in codes if _since_save_counts.get(c, 0) > 0)
        total_codes = len(codes)
    line = (
        f"{prefix} (초당 수신건수 {per_sec_total:03d}건/초), "
        f"(수신건수 {recv_rows}건/수신 종목 {since_active}개/구독 종목 {total_codes}개)"
    )

    width = shutil.get_terminal_size((120, 20)).columns
    if len(line) > width - 1:
        line = line[: width - 1]

    global _last_status_line
    _last_status_line = line
    sys.stdout.write("\r\033[2K" + line)
    sys.stdout.flush()
    # ★ 제자리 출력은 stdout만, 로그 파일에는 기록하지 않음

def _emit_save_done(rows: int) -> None:
    # stdout: 현재 제자리 출력 오른쪽에 붙여서 한 줄로 종료
    sys.stdout.write(f"=> [{LOG_ID}] [save] {rows} rows done\n")
    sys.stdout.flush()
    # 로그: save 정보만 기록 (제자리 출력 내용은 제외)
    logger.info(f"{_fmt_now_prefix()} [save] {rows} rows done")

def _format_full_entries() -> list[str]:
    out = []
    for code in codes:
        sec_cnt = _per_sec_counts.get(code, 0) % PER_SEC_ROLLOVER
        tot_cnt = _total_counts.get(code, 0)
        name = code_name_map.get(code, code)
        label = name[:2] if len(name) >= 2 else name
        out.append(f"{label}({sec_cnt}/{tot_cnt})")
    return out

def _log_full_progress():
    entries = _format_full_entries()
    if not entries:
        return
    prefix = _fmt_now_prefix()
    indent = " " * len(prefix)
    max_per_line = 20
    for i in range(0, len(entries), max_per_line):
        chunk = " ".join(entries[i : i + max_per_line])
        if i == 0:
            logger.info(f"{prefix} {chunk}")
        else:
            logger.info(f"{indent} {chunk}")

# =============================================================================
# 버퍼/Writer (flush는 워커에서만)
# =============================================================================
_lock = threading.RLock()
_stop_event = threading.Event()
_ingest_queue: "queue.Queue[tuple[pd.DataFrame, str, str, str | None, str]]" = queue.Queue()

_buffers: dict[str, list[pd.DataFrame]] = {c: [] for c in codes}
_buf_rows: dict[str, int] = {c: 0 for c in codes}
_last_flush_ts: dict[str, float] = {c: time.time() for c in codes}

_last_recv_ts: dict[str, float] = {c: 0.0 for c in codes}
_code_added_ts: dict[str, float] = {c: time.time() for c in codes}  # 종목별 추가 시점 (watchdog 3분 기준)
_last_summary_ts = time.time()

# ── 장중 미수신 감시 상태 ──
_last_stale_check_ts: dict[str, float] = {}  # 종목별 마지막 REST 상태확인 시각
_halted_codes: set[str] = set()               # 거래정지 확인된 종목
_vi_active_codes: set[str] = set()            # VI 발동 확인된 종목
_vi_exp_sub_ts: dict[str, float] = {}         # VI 종목 예상체결가 구독 시작 시각
_rest_fail_backoff: dict[str, int] = {}       # REST 500 에러 연속 횟수 (백오프용)

_overwrite_events: list[dict[str, str]] = []
_part_seq = 0

_top_client: KisClient | None = None
_price_client: KisClient | None = None
_top_client_2: KisClient | None = None      # 계정1-메인 (한도 초과 시 fallback)
_price_client_2: KisClient | None = None     # 계정1-메인 (한도 초과 시 fallback)
_price_queue: "queue.Queue[str]" = queue.Queue()
_last_rest_req_ts: dict[str, float] = {c: 0.0 for c in codes}
_rest_req_count = 0
_rest_req_second = int(time.time())
_rest_pending: set[str] = set()
_rest_queue: "queue.Queue[tuple[callable, tuple, dict, float, threading.Event, dict]]" = queue.Queue()
_rest_last_sent_ts = 0.0
_price_req_count = 0
_price_req_second = int(time.time())


# ── PRIMARY: 계정2(syw_2) 클라이언트 ─────────────────────────────
def _load_syw2_cfg() -> dict:
    """config.json → accounts.syw_2 키를 읽어 공통 설정과 병합하여 반환."""
    cfg = load_config(str(SCRIPT_DIR / "config.json"))
    acct = cfg.get("accounts", {}).get("syw_2", {})
    if not acct.get("appkey") or not acct.get("appsecret"):
        raise ValueError("config.json → accounts.syw_2에 appkey/appsecret이 필요합니다.")
    return {
        "appkey": acct["appkey"],
        "appsecret": acct["appsecret"],
        "base_url": cfg.get("base_url") or DEFAULT_BASE_URL,
        "custtype": cfg.get("custtype") or "P",
        "market_div": cfg.get("market_div") or "J",
    }


def _init_top_client() -> KisClient:
    a2 = _load_syw2_cfg()
    return KisClient(KisConfig(
        appkey=a2["appkey"], appsecret=a2["appsecret"],
        base_url=a2["base_url"], custtype=a2["custtype"],
        market_div=a2.get("market_div") or TOP_RANK_MARKET_DIV,
        token_cache_path=str(SCRIPT_DIR / "kis_token_syw2.json"),
    ))


def _init_price_client() -> KisClient:
    a2 = _load_syw2_cfg()
    return KisClient(KisConfig(
        appkey=a2["appkey"], appsecret=a2["appsecret"],
        base_url=a2["base_url"], custtype=a2["custtype"],
        market_div=a2["market_div"],
        token_cache_path=str(SCRIPT_DIR / "kis_token_syw2.json"),
    ))


# ── FALLBACK: 계정1(메인) 클라이언트 (한도 초과 시) ──────────────
def _load_account1_cfg() -> dict:
    """config.json 루트의 메인 계정 appkey/appsecret을 반환."""
    cfg = load_config(str(SCRIPT_DIR / "config.json"))
    appkey = cfg.get("appkey")
    appsecret = cfg.get("appsecret")
    if not appkey or not appsecret:
        return {}
    return {
        "appkey": appkey,
        "appsecret": appsecret,
        "base_url": cfg.get("base_url") or DEFAULT_BASE_URL,
        "custtype": cfg.get("custtype") or "P",
        "market_div": cfg.get("market_div") or "J",
    }


def _init_top_client_2() -> KisClient | None:
    a1 = _load_account1_cfg()
    if not a1:
        return None
    return KisClient(KisConfig(
        appkey=a1["appkey"], appsecret=a1["appsecret"],
        base_url=a1["base_url"], custtype=a1["custtype"],
        market_div=a1.get("market_div") or TOP_RANK_MARKET_DIV,
    ))


def _init_price_client_2() -> KisClient | None:
    a1 = _load_account1_cfg()
    if not a1:
        return None
    return KisClient(KisConfig(
        appkey=a1["appkey"], appsecret=a1["appsecret"],
        base_url=a1["base_url"], custtype=a1["custtype"],
        market_div=a1["market_div"],
    ))


def _is_rate_limit_error(e: Exception) -> bool:
    """한도 초과/타임아웃 관련 에러인지 판별."""
    msg = str(e).lower()
    return any(kw in msg for kw in [
        "egw00201",         # 초당 거래건수 초과
        "max retries",      # 연결 타임아웃 (urllib3)
        "connect timeout",  # 연결 타임아웃
        "too many requests", "429",
        "egw00202",         # 분당 거래건수 초과
    ])


def _fetch_current_price(client: KisClient, code: str) -> dict:
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = client._headers(tr_id="FHKST01010100")
    params = {
        "FID_COND_MRKT_DIV_CODE": client.cfg.market_div,
        "FID_INPUT_ISCD": code,
    }
    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"[현재가] 실패: {j.get('msg1')} raw={j}")
    return j.get("output") or {}


def _enqueue_rest_price_row(code: str, output: dict) -> None:
    # wss 컬럼과 맞춰 저장: 현재가/호가/거래량/상하한가
    # ★ WSS 원본이 소문자이므로 동일하게 소문자 컬럼명 사용 (대소문자 불일치 → 중복 방지)
    row = {
        "mksc_shrn_iscd": str(code).zfill(6),
        "stck_prpr": output.get("stck_prpr"),
        "askp1": output.get("askp1"),
        "bidp1": output.get("bidp1"),
        "acml_vol": output.get("acml_vol"),
        "stck_mxpr": output.get("stck_mxpr"),
        "stck_llam": output.get("stck_llam"),
    }
    df = pd.DataFrame([row])
    mode = _get_mode()
    kind = "overtime_real" if mode in (RunMode.OVERTIME_EXP, RunMode.OVERTIME_REAL) else "regular_real"
    recv_ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S.%f")
    _ingest_queue.put((df, "FHKST01010100", kind, "Y", recv_ts))


def _rest_submit(func, *args, min_interval: float = 0.0, timeout: float = 30.0, **kwargs):
    evt = threading.Event()
    holder: dict = {}
    _rest_queue.put((func, args, kwargs, min_interval, evt, holder))
    if not evt.wait(timeout=timeout):
        raise TimeoutError(f"[rest_submit] {func.__name__} 응답 없음 ({timeout}s timeout)")
    if "error" in holder:
        raise holder["error"]
    return holder.get("result")


def _rest_worker() -> None:
    global _rest_req_count, _rest_req_second, _rest_last_sent_ts
    logger.info(f"{ts_prefix()} [rest] worker started")
    while not _stop_event.is_set():
        try:
            func, args, kwargs, min_interval, evt, holder = _rest_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        now = time.time()
        now_sec = int(now)
        if now_sec != _rest_req_second:
            _rest_req_second = now_sec
            _rest_req_count = 0
        if _rest_req_count >= REST_MAX_PER_SEC:
            sleep_sec = (now_sec + 1 - now) + REST_AFTER_WINDOW_DELAY
            time.sleep(max(0.0, sleep_sec))
            _rest_req_second = int(time.time())
            _rest_req_count = 0
        gap = now - _rest_last_sent_ts
        if min_interval > 0 and gap < min_interval:
            time.sleep(min_interval - gap)
        try:
            result = func(*args, **kwargs)
            holder["result"] = result
        except Exception as e:
            holder["error"] = e
        finally:
            _rest_req_count += 1
            _rest_last_sent_ts = time.time()
            evt.set()
    logger.info(f"{ts_prefix()} [rest] worker stopped")


# REST 보충 정책
_REST_OPEN_START    = dtime(9, 0)     # 개장 직후 적극 보충 시작
_REST_OPEN_END      = dtime(9, 10)    # 개장 직후 적극 보충 종료 (최대)
_REST_OPEN_INTERVAL = 1.0             # 개장 직후: 종목당 1초 간격
_REST_VI_UNTIL      = dtime(9, 2)     # VI 발동 종목 REST 보충 제외 시한
_REST_GRACE_SEC     = 30              # 시작/재시작 후 WSS warmup 대기 시간(초)

def _vi_exp_sub_add(code: str) -> None:
    """VI 발동 종목에 예상체결가 구독 추가."""
    if code in _vi_exp_sub_ts:
        return  # 이미 구독 중
    _vi_exp_sub_ts[code] = time.time()
    _vi_active_codes.add(code)
    with _kws_lock:
        if _active_kws is not None:
            try:
                _send_subscribe(_active_kws, exp_ccnl_krx, [code], "1")  # noqa: F405
                name = code_name_map.get(code, code)
                logger.info(f"{ts_prefix()} [stale] VI 예상체결가 구독 추가: {name}({code})")
            except Exception as e:
                logger.warning(f"{ts_prefix()} [stale] VI exp_ccnl 구독 실패: {code} {e}")

def _vi_exp_sub_unsub(code: str) -> None:
    """VI 해제 또는 타임아웃 → 예상체결가 구독 해제."""
    _vi_exp_sub_ts.pop(code, None)
    _vi_active_codes.discard(code)
    with _kws_lock:
        if _active_kws is not None:
            try:
                _send_subscribe(_active_kws, exp_ccnl_krx, [code], "2")  # noqa: F405
                name = code_name_map.get(code, code)
                logger.info(f"{ts_prefix()} [stale] VI 예상체결가 구독 해제: {name}({code})")
            except Exception as e:
                logger.warning(f"{ts_prefix()} [stale] VI exp_ccnl 해제 실패: {code} {e}")

def _handle_stale_check_result(code: str, output: dict) -> None:
    """REST 상태확인 결과로 종목 상태 판단 및 조치."""
    name = code_name_map.get(code, code)
    pr = output.get("stck_prpr", "")
    vol = output.get("acml_vol", "")
    temp_stop = str(output.get("temp_stop_yn", "N")).strip().upper()
    sltr = str(output.get("sltr_yn", "N")).strip().upper()
    mrkt_warn = str(output.get("mrkt_warn_cls_code", "")).strip()
    invt_caful = str(output.get("invt_caful_yn", "N")).strip().upper()

    def _stale_notify(msg: str) -> None:
        sys.stdout.write("\n")
        _notify(msg)

    # ── 장중 일시정지 (VI 등) → 유지 ──
    if temp_stop == "Y":
        if code not in _halted_codes:
            _halted_codes.add(code)
            _stale_notify(f"{ts_prefix()} [stale] {name}({code}) 일시정지 중")
        _enqueue_rest_price_row(code, output)
        return

    # ── 정리매매 → 해제 ──
    if sltr == "Y":
        if code not in _halted_codes:
            _halted_codes.add(code)
            _stale_notify(f"{ts_prefix()} [stale] {name}({code}) 정리매매 → 구독 해제")
            removed = _remove_code_structs([code], force=True)
            if removed:
                _trigger_ws_rebuild()
        return

    # ── 거래 없음 → 09:10 전 유지, 이후 해제 ──
    pr_val = str(pr).strip()
    vol_val = str(vol).strip()
    if not pr_val or pr_val == "0" or not vol_val or vol_val == "0":
        now_t = datetime.now(KST).time()
        if now_t >= dtime(9, 10) and code not in _halted_codes:
            _halted_codes.add(code)
            _stale_notify(f"{ts_prefix()} [stale] {name}({code}) 거래 없음 → 구독 해제")
            removed = _remove_code_structs([code], force=True)
            if removed:
                _trigger_ws_rebuild()
        return

    # ── 투자위험/투자주의 → 유지 (1회 알람) ──
    if mrkt_warn == "03" or invt_caful == "Y":
        if code not in _halted_codes:
            _halted_codes.add(code)
            tag = "투자위험" if mrkt_warn == "03" else "투자주의"
            _stale_notify(f"{ts_prefix()} [stale] {name}({code}) {tag}")
        _enqueue_rest_price_row(code, output)
        return

    # ── 정상 ──
    _halted_codes.discard(code)
    _enqueue_rest_price_row(code, output)
    logger.info(f"{ts_prefix()} [stale] {name}({code}) 정상 pr={pr} → REST 보충")


def _is_vi_suspect(code: str, now_t) -> bool:
    """09:00 직전 prdy_ctrt >= 10% 이면 VI 발동 가능 → 09:02까지 REST 제외."""
    if now_t >= _REST_VI_UNTIL:
        return False
    return abs(_last_prdy_ctrt.get(code, 0.0)) >= 10.0

def _price_watchdog_loop() -> None:
    logger.info(f"{ts_prefix()} [price] watchdog started")
    start_ts = time.time()
    while not _stop_event.is_set():
        mode = _get_mode()
        if mode in (RunMode.PREOPEN_WAIT, RunMode.STOP, RunMode.EXIT):
            time.sleep(0.5)
            continue
        now = time.time()
        now_t = datetime.now(KST).time()
        # 09:00 이전에는 REST 보충 안 함
        if now_t < _REST_OPEN_START:
            time.sleep(0.5)
            continue

        # --- 모드 판별 ---
        wss_alive = (_last_any_recv_ts > 0 and (now - _last_any_recv_ts) < 5.0)
        wss_ever  = (_last_any_recv_ts > 0)

        # ================================================================
        # (A) 개장 직후 09:00~09:10: 적극 REST 보충
        #     - WSS 체결 데이터가 지연되므로 1초 간격으로 REST 보충
        #     - 한 번이라도 WSS 실시간 체결을 받은 종목(=open가격 수신)은 제외
        #     - VI 발동 의심 종목(prdy_ctrt >= 10%)은 09:02까지 제외
        # ================================================================
        if _REST_OPEN_START <= now_t < _REST_OPEN_END:
            with _lock:
                # 모든 종목이 WSS 수신 완료되면 개장 보충 조기 종료
                all_received = all(
                    _last_recv_ts.get(c, 0.0) > 0 for c in codes
                )
            if all_received:
                # 모든 종목 open 가격 수신 완료 → 개장 보충 불필요, (B)로 진행
                pass
            else:
                with _lock:
                    for c in list(codes):
                        # VI 발동 의심 종목 → 09:02까지 제외
                        if _is_vi_suspect(c, now_t):
                            continue
                        last = _last_recv_ts.get(c, 0.0)
                        if last > 0:
                            continue  # 이미 WSS 체결 수신(=open 가격 확보) → skip
                        if (now - _last_rest_req_ts.get(c, 0.0)) < _REST_OPEN_INTERVAL:
                            continue
                        _last_rest_req_ts[c] = now
                        if c in _rest_pending:
                            continue
                        _rest_pending.add(c)
                        try:
                            _price_queue.put_nowait(c)
                        except Exception:
                            pass
                time.sleep(0.5)
                continue

        # ================================================================
        # (B) 09:10 이후 (또는 개장 보충 조기 종료): WSS 의존
        #     - 시작/재시작 직후 warmup (30초) — WSS가 데이터 보내기 전 REST 폭탄 방지
        #     - WSS 정상 → 한 번도 수신 안 한 종목만 REST
        #     - WSS 5초+ 전체 미수신 → 연결 이상, 전 종목 REST
        # ================================================================
        if (now - start_ts) < _REST_GRACE_SEC:
            time.sleep(0.5)
            continue

        # ── 정규장 시간 외에는 REST 보충/감시 안 함 ──
        # 시간외(16:00~18:00)는 대부분 거래 없어 WSS 미수신이 정상
        if not (dtime(9, 0) <= now_t <= dtime(15, 30)):
            time.sleep(1.0)
            continue

        if wss_alive:
            with _lock:
                _nm = _code_name_map()
                for c in list(codes):
                    last = _last_recv_ts.get(c, 0.0)
                    if last > 0:
                        continue  # WSS 수신 이력 있으면 skip
                    # ── WSS 한 번도 미수신 + 추가 시점부터 10분(600초) 경과 → 구독 해제 (TOP5_ADD=False면 스킵) ──
                    if not TOP5_ADD:
                        continue
                    added_ts = _code_added_ts.get(c, start_ts)
                    if (now - added_ts) >= 600.0:
                        name = _nm.get(c, c)
                        sys.stdout.write("\n")
                        _notify(
                            f"{ts_prefix()} [watchdog] WSS 미수신 10분 → 구독 해제: {name}({c})"
                        )
                        removed = _remove_code_structs([c], force=True)
                        if removed:
                            _top_added_codes.discard(c)
                            _watchdog_removed_codes.add(c)
                            _trigger_ws_rebuild()
                        continue
                    # ── 60초 간격 REST 상태확인 (거래중지/VI 즉시 판단) ──
                    if (now - _last_rest_req_ts.get(c, start_ts)) < 60.0:
                        continue
                    _last_rest_req_ts[c] = now
                    if c in _rest_pending:
                        continue
                    _rest_pending.add(c)
                    try:
                        _price_queue.put_nowait(("stale_check", c))
                    except Exception:
                        pass

        # ================================================================
        # (C) 장중 종목별 미수신 감시 (09:10~15:30)
        #     - WSS 한 번도 미수신 종목: STALE_CHECK_SEC(60초) 간격
        #     - WSS 수신 이력 있는 종목: STALE_CHECK_INACTIVE_SEC(120초) 미수신 시만
        #     - 거래정지 종목: STALE_CHECK_HALTED_SEC(300초) 간격
        # ================================================================
        if dtime(9, 10) <= now_t <= dtime(15, 30):
            with _lock:
                for c in list(codes):
                    if c in _rest_pending:
                        continue
                    last = _last_recv_ts.get(c, 0.0)
                    idle = (now - last) if last > 0 else (now - start_ts)
                    # 상태별 재확인 간격 적용
                    if c in _halted_codes:
                        check_interval = STALE_CHECK_HALTED_SEC    # 300초
                    elif last > 0:
                        # WSS 수신 이력 있음 → 단순 거래 비활성, 넉넉하게 대기
                        check_interval = STALE_CHECK_INACTIVE_SEC  # 120초
                    else:
                        # WSS 한 번도 미수신 → 빈번하게 확인
                        check_interval = STALE_CHECK_SEC           # 60초
                    if idle < check_interval:
                        continue
                    last_check = _last_stale_check_ts.get(c, 0.0)
                    if (now - last_check) < check_interval:
                        continue
                    _last_stale_check_ts[c] = now
                    _rest_pending.add(c)
                    try:
                        _price_queue.put_nowait(("stale_check", c))
                    except Exception:
                        pass

            # VI 예상체결가 구독 자동 해제 (120초 경과)
            for c in list(_vi_exp_sub_ts):
                if (now - _vi_exp_sub_ts[c]) >= 120:
                    _vi_exp_sub_unsub(c)

        if not wss_alive:
            # WSS 5초 이상 전체 미수신 → 연결 이상, 종목별 60초 간격 REST 상태확인
            with _lock:
                for c in list(codes):
                    last = _last_recv_ts.get(c, 0.0)
                    if last > 0 and (now - last) < 5.0:
                        continue
                    if (now - _last_rest_req_ts.get(c, start_ts)) < 60.0:
                        continue
                    _last_rest_req_ts[c] = now
                    if c in _rest_pending:
                        continue
                    _rest_pending.add(c)
                    try:
                        _price_queue.put_nowait(("stale_check", c))
                    except Exception:
                        pass
        time.sleep(0.5)
    logger.info(f"{ts_prefix()} [price] watchdog stopped")


def _price_request_loop() -> None:
    global _price_client, _price_client_2, _price_req_count, _price_req_second
    try:
        _price_client = _init_price_client()
    except Exception as e:
        logger.warning(f"{ts_prefix()} [price] init failed: {e}")
        return
    # 계정1(메인) fallback 초기화 (실패해도 기본 계정으로 계속)
    if USE_FALLBACK_ACCOUNT:
        try:
            _price_client_2 = _init_price_client_2()
            if _price_client_2:
                logger.info(f"{ts_prefix()} [price] 계정1(메인) fallback 초기화 완료")
        except Exception:
            _price_client_2 = None
    logger.info(f"{ts_prefix()} [price] request loop started")
    last_req_ts = 0.0
    while not _stop_event.is_set():
        try:
            item = _price_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        # ── 요청 타입 구분: 일반 가격 보충 vs 장중 상태확인 ──
        is_stale_check = False
        if isinstance(item, tuple) and len(item) == 2 and item[0] == "stale_check":
            is_stale_check = True
            code = item[1]
        else:
            code = item

        now = time.time()
        # 현재가 요청 초당 제한 (8건)
        now_sec = int(now)
        if now_sec != _price_req_second:
            _price_req_second = now_sec
            _price_req_count = 0
        if _price_req_count >= 8:
            sleep_sec = (now_sec + 1 - now) + REST_AFTER_WINDOW_DELAY
            time.sleep(max(0.0, sleep_sec))
            _price_req_second = int(time.time())
            _price_req_count = 0
        try:
            output = _rest_submit(
                _fetch_current_price,
                _price_client,
                code,
            )
            _price_req_count += 1
            last_req_ts = time.time()
            _rest_fail_backoff.pop(code, None)  # 성공 → 거래정지 의심 해제
            _halted_codes.discard(code)
            if is_stale_check:
                _handle_stale_check_result(code, output)
            else:
                _enqueue_rest_price_row(code, output)
                name = code_name_map.get(code, code)
                pr = output.get("stck_prpr", "?")
                logger.info(f"{ts_prefix()} [price] REST보충 {name}({code}) pr={pr}")
        except Exception as e:
            # ── 한도 초과 시 ──
            if _is_rate_limit_error(e):
                if USE_FALLBACK_ACCOUNT and _price_client_2:
                    try:
                        output = _rest_submit(_fetch_current_price, _price_client_2, code)
                        _price_req_count += 1
                        _rest_fail_backoff.pop(code, None)
                        if is_stale_check:
                            _handle_stale_check_result(code, output)
                        else:
                            _enqueue_rest_price_row(code, output)
                            name = code_name_map.get(code, code)
                            pr = output.get("stck_prpr", "?")
                            logger.info(f"{ts_prefix()} [price] REST보충(계정1) {name}({code}) pr={pr}")
                    except Exception as e2:
                        logger.warning(f"{ts_prefix()} [price] fetch failed(계정1) code={code}: {e2}")
                else:
                    # fallback 미사용 → 한도 초과는 건너뜀
                    name = code_name_map.get(code, code)
                    logger.info(f"{ts_prefix()} [price] 한도 초과 → 건너뜀: {name}({code})")
            else:
                # ── 500 등 서버 에러 → 거래정지 의심 처리 ──
                name = code_name_map.get(code, code)
                fc = _rest_fail_backoff.get(code, 0) + 1
                _rest_fail_backoff[code] = fc
                if fc == 1:
                    logger.warning(f"{ts_prefix()} [price] fetch failed {name}({code}): {e}")
                elif fc == 2:
                    # 2회 연속 500 + WSS 미수신 → 거래정지 의심, _halted_codes 등록
                    if code not in _halted_codes:
                        _halted_codes.add(code)
                        _notify(
                            f"{ts_prefix()} [price] ★ REST 500 연속 + WSS 미수신 "
                            f"→ 거래정지 의심: {name}({code})"
                        )
                # _halted_codes 등록 후 → watchdog (C)에서 STALE_CHECK_HALTED_SEC(300초) 간격 적용
                # 인증 오류일 때만 재초기화
                err_str = str(e)
                if "401" in err_str or "403" in err_str or "token" in err_str.lower():
                    try:
                        _price_client = _init_price_client()
                        logger.info(f"{ts_prefix()} [price] client re-initialized")
                    except Exception:
                        pass
        finally:
            _rest_pending.discard(code)
    logger.info(f"{ts_prefix()} [price] request loop stopped")

def _fetch_fluctuation_top(client: KisClient, top_n: int = 10) -> list[dict]:
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/ranking/fluctuation"
    headers = client._headers(tr_id="FHPST01700000")
    params = {
        "fid_cond_mrkt_div_code": client.cfg.market_div,
        "fid_cond_scr_div_code": "20170",
        "fid_input_iscd": "0000",
        "fid_rank_sort_cls_code": "0",
        "fid_input_cnt_1": "0",
        "fid_prc_cls_code": "0",
        "fid_input_price_1": "",
        "fid_input_price_2": "",
        "fid_vol_cnt": "",
        "fid_trgt_cls_code": "0",
        "fid_trgt_exls_cls_code": "0",
        "fid_div_cls_code": "0",
        "fid_rsfl_rate1": "",
        "fid_rsfl_rate2": "",
    }
    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"[등락률순위] 실패: {j.get('msg1')} raw={j}")
    output = j.get("output") or []
    if isinstance(output, dict):
        output = [output]
    if isinstance(output, list) and top_n > 0:
        return output[:top_n]
    return output


def _ensure_code_structs(new_codes: list[str]) -> list[str]:
    added = []
    with _lock:
        for c in new_codes:
            if c in codes:
                continue
            codes.append(c)
            added.append(c)
            _per_sec_counts[c] = 0
            _total_counts[c] = 0
            _since_save_counts[c] = 0
            _last_prdy_ctrt[c] = 0.0
            _buffers[c] = []
            _buf_rows[c] = 0
            _last_flush_ts[c] = time.time()
            _last_recv_ts[c] = 0.0
            _last_rest_req_ts[c] = 0.0
            _code_added_ts[c] = time.time()  # watchdog 3분 기준: 추가 시점부터 계산
        if added:
            try:
                code_name_map.update(_code_name_map())
            except Exception:
                pass
            _persist_subscription_codes(codes)
    return added


# KRX group 필터 캐시 (group == "ST" 인 종목만 구독 대상)
_krx_group_cache: dict[str, str] = {}


def _load_krx_group_cache() -> None:
    """KRX_code.csv에서 code→group 매핑을 로드한다."""
    global _krx_group_cache
    try:
        krx_path = Path("/home/ubuntu/Stoc_Kis/data/admin/symbol_master/KRX_code.csv")
        if not krx_path.exists():
            logger.warning("[top] KRX_code.csv not found, group filter disabled")
            return
        df = pd.read_csv(krx_path, dtype=str, usecols=["code", "group"])
        df["code"] = df["code"].str.strip().str.zfill(6)
        df["group"] = df["group"].str.strip().str.upper()
        _krx_group_cache = dict(zip(df["code"], df["group"]))
        logger.info(f"[top] KRX group cache loaded: {len(_krx_group_cache)} entries")
    except Exception as e:
        logger.warning(f"[top] KRX group cache load failed: {e}")


def _is_stock_group(code: str) -> bool:
    """KRX group이 'ST'(일반 주식)인지 확인. 캐시 없으면 통과."""
    if not _krx_group_cache:
        return True  # 캐시 없으면 필터 미적용
    return _krx_group_cache.get(code, "") == "ST"


# 탑10 구독 추가/해제 이력 관리
_top_added_codes: set[str] = set(_loaded_top_added)  # config에서 복원 또는 빈 set
if _top_added_codes:
    logger.info(f"[codes] top_added 복원: {sorted(_top_added_codes)}")
_watchdog_removed_codes: set[str] = set()  # watchdog WSS 미수신으로 해제된 종목 (재추가 방지)
_top_sub_log_path = TOP_RANK_OUT_DIR / f"wss_sub_add_top10_list_{today_yymmdd}.csv"
_closing_codes: list[str] = []       # 15:19 선정된 종가매매 종목 (ST 10개)


def _log_top_sub_event(code: str, name: str, action: str) -> None:
    """탑10 구독 추가/해제 이벤트를 CSV에 기록한다."""
    TOP_RANK_OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    write_header = not _top_sub_log_path.exists()
    try:
        with open(_top_sub_log_path, "a", encoding="utf-8-sig") as f:
            if write_header:
                f.write("time,code,name,action\n")
            f.write(f"{ts},{code},{name},{action}\n")
    except Exception as e:
        logger.warning(f"[top] sub log write failed: {e}")


def _remove_code_structs(remove_codes: list[str], force: bool = False) -> list[str]:
    """종목을 구독 해제한다. force=False면 base_codes는 제거하지 않는다."""
    removed = []
    with _lock:
        for c in remove_codes:
            if not force and c in _base_codes:
                continue
            if c not in codes:
                continue
            codes.remove(c)
            removed.append(c)
            # 관련 자료구조 정리
            _per_sec_counts.pop(c, None)
            _total_counts.pop(c, None)
            _since_save_counts.pop(c, None)
            _buffers.pop(c, None)
            _buf_rows.pop(c, None)
            _last_flush_ts.pop(c, None)
            _last_recv_ts.pop(c, None)
            _last_rest_req_ts.pop(c, None)
            _last_prdy_ctrt.pop(c, None)
            _code_added_ts.pop(c, None)
        if removed:
            _persist_subscription_codes(codes)
    return removed


def _switch_to_closing_codes() -> None:
    """15:20 종가매매 전환: 기존 종목 전부 해제 → _closing_codes 만 남긴다."""
    global _base_codes
    if not _closing_codes:
        logger.warning(f"{ts_prefix()} [top] _closing_codes 비어 있음, 전환 불가")
        return

    # 기존 종목 버퍼 일괄 flush (종목별 개별 flush → 다건 save 출력 방지)
    try:
        _flush_all(force=True)
    except Exception:
        pass

    with _lock:
        # 기존 종목 전부 제거
        old_codes = list(codes)
        old_names = [code_name_map.get(c, c) for c in old_codes]
        codes.clear()
        _top_added_codes.clear()
        for c in old_codes:
            _code_added_ts.pop(c, None)

        # 종가매매 종목으로 교체
        now_ts = time.time()
        for c in _closing_codes:
            codes.append(c)
            _per_sec_counts[c] = _per_sec_counts.get(c, 0)
            _total_counts[c] = _total_counts.get(c, 0)
            _since_save_counts[c] = _since_save_counts.get(c, 0)
            if c not in _buffers:
                _buffers[c] = []
            if c not in _buf_rows:
                _buf_rows[c] = 0
            _last_flush_ts[c] = now_ts
            _last_recv_ts[c] = _last_recv_ts.get(c, 0.0)
            _last_rest_req_ts[c] = _last_rest_req_ts.get(c, 0.0)
            _code_added_ts[c] = now_ts  # watchdog 3분 기준 리셋 (즉시 삭제 방지)

        # base_codes 갱신 (종가매매 종목이 새 기본)
        _base_codes = set(codes)
        _persist_subscription_codes(codes)

    # 이름 갱신
    try:
        code_name_map.update(_code_name_map())
    except Exception:
        pass

    new_names = [code_name_map.get(c, c) for c in _closing_codes]

    # ── 1) 모드를 CLOSE_EXP로 즉시 전환 (scheduler_loop의 중복 전환 방지) ──
    with _mode_lock:
        global _current_mode
        _current_mode = RunMode.CLOSE_EXP

    # ── 2) 예상체결가로 구독 적용 (15:20부터 _desired_subscription_map이 exp_ccnl 반환) ──
    _trigger_ws_rebuild()

    # ── 3) 메시지 출력: 예상체결가 전환 → 종가매매 전환 완료 순서 ──
    sys.stdout.write("\n")  # 제자리 출력 줄바꿈
    _notify(f"{ts_prefix()} 실시간 구독 중지 -> 예상체결가 구독 시작", tele=True)
    _notify(
        f"{ts_prefix()} [top] ★★ 종가매매 전환 완료\n"
        f"  해제: {len(old_codes)}개 {old_names}\n"
        f"  신규: {len(_closing_codes)}개 {_closing_codes} / {new_names}",
        tele=True,
    )


def _top_rank_loop():
    global _top_client, _top_client_2
    if not TOP_RANK_ENABLED:
        return
    _load_krx_group_cache()
    try:
        _top_client = _init_top_client()
    except Exception as e:
        logger.warning(f"{ts_prefix()} [top] init failed: {e}")
        return
    # 계정1(메인) fallback 초기화 (실패해도 기본 계정으로 계속)
    if USE_FALLBACK_ACCOUNT:
        try:
            _top_client_2 = _init_top_client_2()
            if _top_client_2:
                logger.info(f"{ts_prefix()} [top] 계정1(메인) fallback 초기화 완료")
        except Exception:
            _top_client_2 = None
    logger.info(f"{ts_prefix()} [top] started (n={TOP_RANK_N}, add={TOP_RANK_ADD_N}, max_sub={MAX_WSS_SUBSCRIBE}, until={TOP_RANK_END})")
    schedule = _build_top_rank_schedule(datetime.now(KST))
    idx = 0
    while not _stop_event.is_set():
        now = datetime.now(KST)
        while idx < len(schedule) and now >= schedule[idx]:
            idx += 1
        if idx >= len(schedule) or now.time() >= TOP_RANK_END:
            logger.info(f"{ts_prefix()} [top] end time reached, stopping")
            return
        next_ts = schedule[idx]
        sleep_sec = max(0.1, (next_ts - now).total_seconds())
        time.sleep(min(sleep_sec, 1.0))
        if datetime.now(KST) < next_ts:
            continue
        logger.info(f"{ts_prefix()} [top] 랭킹 조회 시작 (schedule #{idx})")
        is_closing_select = (next_ts.time() >= dtime(15, 19))    # 15:19 종가매매 선정
        is_closing_switch = (next_ts.time() >= dtime(15, 20))   # 15:20 구독 전환
        try:
            # 15:20 구독 전환: 15:19에서 선정된 _closing_codes 사용, fetch 생략
            if is_closing_switch and _closing_codes:
                _switch_to_closing_codes()
                logger.info(f"{ts_prefix()} [top] end time reached (종가매매 전환 완료), stopping")
                return
            # ── 계정2(syw_2) → 실패 시 계정1(메인)으로 재시도 ──
            try:
                rows = _rest_submit(_fetch_fluctuation_top, _top_client, top_n=TOP_RANK_N)
            except Exception as e1:
                logger.warning(f"{ts_prefix()} [top] 계정2(syw_2) 실패: {type(e1).__name__}: {e1}")
                if USE_FALLBACK_ACCOUNT and _top_client_2:
                    rows = _rest_submit(_fetch_fluctuation_top, _top_client_2, top_n=TOP_RANK_N)
                    logger.info(f"{ts_prefix()} [top] 계정1(메인)으로 재시도 성공 ({len(rows)}건)")
                elif _is_rate_limit_error(e1):
                    logger.info(f"{ts_prefix()} [top] 한도 초과 → 이번 스케줄 건너뜀")
                    continue  # 한도 초과는 건너뛰고 다음 스케줄로
                else:
                    raise  # 한도 초과 외 에러는 전파
            TOP_RANK_OUT_DIR.mkdir(parents=True, exist_ok=True)
            ts = next_ts.strftime("%y%m%d_%H%M")
            out_path = TOP_RANK_OUT_DIR / f"Fetch_fluctuation_top_{ts}.csv"
            try:
                pd.DataFrame(rows).to_csv(out_path, index=False, encoding="utf-8-sig")
            except Exception as e:
                logger.warning(f"{ts_prefix()} [top] save failed: {e}")
            msg = f"{ts_prefix()} 상승률 상위 Top{TOP_RANK_N} 다운로드, 저장 완료 : {out_path}"
            sys.stdout.write("\n")  # 제자리 출력 줄바꿈
            _notify(msg)

            # --- 현재 랭킹 종목코드 추출 ---
            current_top_codes: list[str] = []
            for row in rows:
                code = str(
                    row.get("stck_shrn_iscd")
                    or row.get("stck_cd")
                    or row.get("code")
                    or ""
                ).zfill(6)
                if code:
                    current_top_codes.append(code)

            # ============================================================
            # 15:19 종가매매 종목 선정 (TOP5_ADD=False면 스킵)
            # ============================================================
            if TOP5_ADD and is_closing_select and not _closing_codes:
                closing_candidates = []
                for code in current_top_codes:
                    if len(closing_candidates) >= CLOSING_TOP_N:
                        break
                    if not _is_stock_group(code):
                        continue
                    closing_candidates.append(code)
                _closing_codes.clear()
                _closing_codes.extend(closing_candidates)
                # 이름 매핑 갱신
                try:
                    code_name_map.update(_code_name_map())
                except Exception:
                    pass
                closing_names = [code_name_map.get(c, c) for c in _closing_codes]
                msg_cl = (
                    f"{ts_prefix()} [top] ★ 종가매매 ST {len(_closing_codes)}개 선정: "
                    f"{_closing_codes} / {closing_names}"
                )
                sys.stdout.write("\n")
                _notify(msg_cl)

            # ============================================================
            # 15:20 구독 전환: 기존 전부 해제 → 종가매매 종목으로 교체 (TOP5_ADD=False면 스킵)
            # ============================================================
            if TOP5_ADD and is_closing_switch and _closing_codes:
                _switch_to_closing_codes()
                logger.info(f"{ts_prefix()} [top] end time reached (종가매매 전환 완료), stopping")
                return

            # ============================================================
            # 일반 시간대 처리 (15:19/15:20 아닌 경우, TOP5_ADD=True일 때만)
            # ============================================================
            if TOP5_ADD and not is_closing_select:
                changed = False

                # --- 1) Top20 밖으로 밀린 종목 → 구독 해제 ---
                current_top_keep_set = set(current_top_codes[:TOP_RANK_REMOVE_THRESHOLD])
                codes_to_remove = [c for c in list(_top_added_codes) if c not in current_top_keep_set]
                if codes_to_remove:
                    removed = _remove_code_structs(codes_to_remove)
                    for c in removed:
                        _top_added_codes.discard(c)
                        name = code_name_map.get(c, c)
                        _log_top_sub_event(c, name, "해제")
                    if removed:
                        removed_names = [code_name_map.get(c, c) for c in removed]
                        msg_rm = f"{ts_prefix()} [top] 구독 해제 (Top{TOP_RANK_REMOVE_THRESHOLD} 밖): {removed} / {removed_names}"
                        sys.stdout.write("\n")
                        _notify(msg_rm)
                        changed = True

                # --- 2) 상위 종목 중 ST 신규 → 구독 추가 (상한 관리) ---
                candidates = []
                for code in current_top_codes:
                    if len(candidates) >= TOP_RANK_ADD_N:
                        break
                    if code in _base_codes:
                        continue
                    if code in codes:
                        continue  # 이미 구독 중
                    if code in _watchdog_removed_codes:
                        continue  # watchdog WSS 미수신으로 해제됨 → 재추가 방지
                    if not _is_stock_group(code):
                        continue
                    candidates.append(code)

                # 구독 상한 체크
                current_count = len(codes)
                available_slots = max(0, MAX_WSS_SUBSCRIBE - current_count)
                if available_slots < len(candidates):
                    logger.info(
                        f"{ts_prefix()} [top] 구독 상한 도달 ({current_count}/{MAX_WSS_SUBSCRIBE}), "
                        f"추가 가능: {available_slots}/{len(candidates)}"
                    )
                    candidates = candidates[:available_slots]

                added = _ensure_code_structs(candidates)
                for c in added:
                    _top_added_codes.add(c)
                    name = code_name_map.get(c, c)
                    _log_top_sub_event(c, name, "추가")
                added_names = [code_name_map.get(c, c) for c in added]
                if added:
                    msg2 = f"{ts_prefix()} [top] 구독 추가: {added} / {added_names} (총 {len(codes)}/{MAX_WSS_SUBSCRIBE})"
                    changed = True
                else:
                    if available_slots <= 0:
                        reason = f"상한도달({current_count}/{MAX_WSS_SUBSCRIBE})"
                    elif not candidates:
                        reason = "후보없음(Top50이 base/구독중과 겹침 또는 ST아님)"
                    else:
                        reason = "확인필요"
                    msg2 = f"{ts_prefix()} [top] 구독 추가 없음 (총 {len(codes)}/{MAX_WSS_SUBSCRIBE}) [{reason}]"
                sys.stdout.write("\n")
                _notify(msg2)

                # --- 3) 해제/추가 완료 후 1회만 동적 구독 적용 ---
                if changed:
                    _persist_subscription_codes(codes)
                    _trigger_ws_rebuild()
        except Exception as e:
            logger.warning(f"{ts_prefix()} [top] fetch failed (schedule #{idx}): {type(e).__name__}: {e}")
            # ── 15:20 구독 전환은 fetch 실패해도 반드시 실행 (TOP5_ADD=True일 때만) ──
            if TOP5_ADD and is_closing_switch:
                if _closing_codes:
                    _switch_to_closing_codes()
                    logger.info(f"{ts_prefix()} [top] fetch 실패했으나 종가매매 전환 실행 완료")
                else:
                    # 15:19 선정도 실패 → 현재 구독 종목 그대로 유지
                    logger.warning(
                        f"{ts_prefix()} [top] 종가매매 선정 실패 → 현재 구독 유지 "
                        f"(base={len(_base_codes)}개 + top_added={len(_top_added_codes)}개)"
                    )
                return
            # ── 둘 다 실패 → 클라이언트 재초기화 시도 ──
            err_str = str(e).lower()
            if "401" in err_str or "403" in err_str or "token" in err_str:
                try:
                    _top_client = _init_top_client()
                    logger.info(f"{ts_prefix()} [top] client 재초기화 완료")
                except Exception as e2:
                    logger.warning(f"{ts_prefix()} [top] client 재초기화 실패: {e2}")


def _build_top_rank_schedule(now: datetime) -> list[datetime]:
    base = now.date()
    fixed = [
        # 08:50, 08:59 제외 — 장 시작 전 랭킹은 의미 없음
        dtime(9, 0),
        dtime(9, 5),
        dtime(9, 10),
        dtime(9, 15),
        dtime(9, 30),
    ]
    times = [datetime.combine(base, t, tzinfo=KST) for t in fixed]
    cur = datetime.combine(base, dtime(10, 0), tzinfo=KST)
    end = datetime.combine(base, dtime(15, 0), tzinfo=KST)
    while cur <= end:
        times.append(cur)
        cur += timedelta(minutes=30)
    # 종가매매 종목 선정 (15:19) + 구독 전환 (15:20)
    times.append(datetime.combine(base, dtime(15, 19), tzinfo=KST))
    times.append(datetime.combine(base, dtime(15, 20), tzinfo=KST))
    return sorted(set(times))

# TR별 "실체결(Y) / 예상체결(N)" 구분
TRID_TO_IS_REAL: dict[str, str] = {}  # tr_id -> "Y"/"N"
TRID_TO_KIND: dict[str, str] = {}     # tr_id -> "regular_real"/"regular_exp"/"overtime_real"/"overtime_exp"

# TRID fallback 매핑 (extract 실패 대비)
KNOWN_TRID_MAP: dict[str, tuple[str, str]] = {
    "H0UNCNT0": ("regular_real", "Y"),
    "H0STANC0": ("regular_exp", "N"),
    "H0STOUP0": ("overtime_real", "Y"),
    "H0STOAC0": ("overtime_exp", "N"),
}

def _infer_tr_kind(trid: str, is_real: str | None) -> str:
    cur = _get_mode()
    if cur in (RunMode.PREOPEN_EXP, RunMode.CLOSE_EXP):
        return "regular_exp"
    if cur in (RunMode.REGULAR_REAL, RunMode.CLOSE_REAL):
        return "regular_real"
    if cur in (RunMode.OVERTIME_EXP, RunMode.OVERTIME_REAL):
        return "overtime_exp" if is_real == "N" else "overtime_real"
    return "unknown"

# =============================================================================
# [스키마 고정] 스키마 불일치(ValueError) 방지
# - 서로 다른 TR(정규/시간외, 체결/예상)의 컬럼이 약간씩 달라도 한 파일에 저장해야 하므로
#   "통합 컬럼"을 만들고, 모든 컬럼을 string으로 고정합니다.
# =============================================================================
META_COLS = ["recv_ts", "tr_id", "is_real_ccnl"]

def _norm_cols(df: pd.DataFrame) -> list[str]:
    # 모두 소문자 기준으로 통일
    return [str(c).strip().lower() for c in df.columns]

# 통합 컬럼 후보(라이브러리 함수의 columns와 result 실제 컬럼이 섞일 수 있어,
# 안전하게 "우리가 받는 df의 컬럼"을 기반으로 첫 수신 때 확장하는 방식 + 기본 세트.
_union_cols_lock = threading.RLock()
UNION_COLS: list[str] = []   # 소문자
UNION_SCHEMA: pa.Schema | None = None

def _ensure_union_from_df(df: pd.DataFrame):
    global UNION_COLS, UNION_SCHEMA
    cols = _norm_cols(df)
    with _union_cols_lock:
        changed = False
        for c in cols:
            if c not in UNION_COLS:
                UNION_COLS.append(c)
                changed = True
        for c in META_COLS:
            if c not in UNION_COLS:
                UNION_COLS.append(c)
                changed = True
        if changed or (UNION_SCHEMA is None):
            # 전 컬럼 string으로 고정
            fields = [pa.field(c, pa.string()) for c in UNION_COLS]
            UNION_SCHEMA = pa.schema(fields)

def _df_to_table_with_union(df: pd.DataFrame) -> pa.Table:
    """
    df 컬럼이 순간적으로 null/float로 바뀌거나 누락되어도
    UNION_SCHEMA(string)으로 안전하게 변환합니다.
    """
    _ensure_union_from_df(df)
    assert UNION_SCHEMA is not None

    # 컬럼명 소문자 통일 (수신 시점에서 이미 통일되어 있어야 함, flush 단 이중 안전)
    df2 = df.copy()
    df2.columns = _norm_cols(df2)
    if df2.columns.duplicated().any():
        logger.warning(f"[schema] duplicate cols (unexpected): {list(df2.columns[df2.columns.duplicated()].unique())} → keep first")
        df2 = df2.loc[:, ~df2.columns.duplicated()]

    # 누락 컬럼 채우기 + string 캐스팅
    for c in UNION_SCHEMA.names:
        if c not in df2.columns:
            df2[c] = None

    # 컬럼 순서 통일
    df2 = df2[UNION_SCHEMA.names]

    # 전부 string으로 (None은 그대로)
    for c in df2.columns:
        # pandas dtype이 뭔가로 섞여도 안전하게 문자열화
        # 단, None/NaN은 None으로 남기기
        s = df2[c]
        df2[c] = s.where(~s.isna(), None).astype("string")

    # pyarrow schema를 강제로 적용
    return pa.Table.from_pandas(df2, preserve_index=False, schema=UNION_SCHEMA)

def _next_part_path() -> Path:
    global _part_seq
    with _lock:
        _part_seq += 1
        seq = _part_seq
    ts = datetime.now(KST).strftime("%y%m%d_%H%M%S")
    return PART_DIR / f"{today_yymmdd}_wss_data_{ts}.parquet"


def _write_part_table(table: pa.Table) -> Path:
    part_path = _next_part_path()
    PART_DIR.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, part_path, compression="zstd", use_dictionary=True)
    return part_path


def _merge_parts_to(final_path: Path, parts: list[Path]) -> int:
    """parts 파일들을 final_path에 누적 병합 (기존 파일이 있으면 포함)."""
    tables: list[pa.Table] = []
    # 기존 최종 파일이 있으면 포함
    if final_path.exists():
        try:
            if _is_valid_parquet(final_path):
                tables.append(pq.read_table(final_path))
                logger.info(f"[merge] 기존 파일 포함: {final_path.name}")
            else:
                bad = final_path.with_suffix(final_path.suffix + ".bad")
                final_path.rename(bad)
                logger.warning(f"[merge] invalid parquet → {bad.name}")
        except Exception as e:
            logger.warning(f"[merge] 기존 파일 읽기 실패: {e}")
    # parts 읽기
    dataset = ds.dataset([str(p) for p in parts], format="parquet")
    tables.append(dataset.to_table())
    combined = pa.concat_tables(tables, promote_options="permissive") if len(tables) > 1 else tables[0]
    table = _finalize_table(combined)
    pq.write_table(table, final_path, compression="zstd", use_dictionary=True)
    # parts → backup
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    for p in parts:
        p.replace(BACKUP_DIR / p.name)
    logger.info(f"[merge] completed rows={table.num_rows} file={final_path.name}")
    return table.num_rows


def _merge_parts() -> None:
    """당일 parts를 최종 파일에 누적 병합."""
    parts = sorted(PART_DIR.glob("*.parquet"))
    if not parts:
        logger.info("[merge] no parts to merge")
        return
    _merge_parts_to(FINAL_PARQUET_PATH, parts)




def _finalize_table(table: pa.Table) -> pa.Table:
    df = table.to_pandas()
    if "recv_ts" in df.columns:
        try:
            df = df.sort_values("recv_ts", kind="mergesort").reset_index(drop=True)
        except Exception as e:
            logger.warning(f"[merge] recv_ts sort failed: {e}")
    else:
        logger.warning("[merge] recv_ts column not found; skip sort")

    if "name" not in df.columns:
        df["name"] = None
    try:
        name_map = _code_name_map()
        code_col = None
        for cand in ("code", "mksc_shrn_iscd", "stck_shrn_iscd"):
            if cand in df.columns:
                code_col = cand
                break
        if code_col:
            codes = df[code_col].astype(str).str.zfill(6)
            df["name"] = df["name"].fillna(codes.map(name_map))
    except Exception as e:
        logger.warning(f"[merge] name mapping failed: {e}")

    return pa.Table.from_pandas(df, preserve_index=False)

def _handle_existing_parquet(path: Path) -> None:
    ts = datetime.now(KST).strftime("%y%m%d_%H%M%S")
    if not _is_valid_parquet(path):
        backup = path.with_suffix(path.suffix + f".bad.{ts}")
        try:
            path.rename(backup)
            _record_overwrite("invalid_parquet", str(path), str(backup))
            logger.warning(f"[parquet] invalid -> moved to {backup.name}")
        except Exception as e:
            _record_overwrite("invalid_parquet_rename_failed", str(path), str(path))
            logger.warning(f"[parquet] invalid but rename failed: {e}")
        return
    backup = path.with_suffix(path.suffix + f".prev.{ts}")
    try:
        path.rename(backup)
        _record_overwrite("overwrite_existing", str(path), str(backup))
        logger.warning(f"[parquet] existing -> moved to {backup.name}")
    except Exception as e:
        _record_overwrite("overwrite_existing_rename_failed", str(path), str(path))
        logger.warning(f"[parquet] existing but rename failed: {e}")

def _is_valid_parquet(path: Path) -> bool:
    try:
        if path.stat().st_size < 8:
            return False
        with path.open("rb") as f:
            head = f.read(4)
            f.seek(-4, 2)
            tail = f.read(4)
        if head != b"PAR1" or tail != b"PAR1":
            return False
        return True
    except Exception:
        return False

def _record_overwrite(reason: str, src: str, dst: str) -> None:
    _overwrite_events.append(
        {
            "ts": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
            "reason": reason,
            "src": src,
            "dst": dst,
        }
    )

def _flush_overwrite_log() -> None:
    if not _overwrite_events:
        return
    logger.warning("[parquet] overwrite events:")
    for ev in _overwrite_events:
        logger.warning(f" - {ev['ts']} reason={ev['reason']} src={ev['src']} dst={ev['dst']}")

def _log_stale_after_save(threshold_sec: int) -> None:
    now = time.time()
    with _lock:
        stale = [
            c
            for c in codes
            if (_last_recv_ts.get(c, 0.0) > 0 and (now - _last_recv_ts[c]) >= threshold_sec)
        ]
        never = [c for c in codes if _total_counts.get(c, 0) == 0]
    if not stale:
        return
    names = [code_name_map.get(c, c) for c in stale[:10]]
    never_names = [code_name_map.get(c, c) for c in never[:10]]
    msg = f"[{threshold_sec}초 이상 미수신] = {names}" + (" ..." if len(stale) > 10 else "")
    if never_names:
        msg += " / 누적 미수신 : " + ", ".join(never_names)
    logger.info(msg)

    # 20초 미수신 종목 → 현재가 REST 보강 요청 (가져와서 저장)
    for c in stale:
        if c in _rest_pending:
            continue
        _rest_pending.add(c)
        try:
            _price_queue.put_nowait(c)
        except Exception:
            _rest_pending.discard(c)

def _flush_code(code: str, force: bool = False) -> int:
    with _lock:
        if not _buffers[code]:
            return 0
        now = time.time()
        if not force:
            n_ok = _buf_rows[code] >= FLUSH_EVERY_N_ROWS
            if not n_ok:
                return 0

        dfs = _buffers[code]
        _buffers[code] = []
        _buf_rows[code] = 0
        _last_flush_ts[code] = now

    t0 = time.time()
    df = pd.concat(dfs, ignore_index=True)

    # 스키마 고정 변환
    table = _df_to_table_with_union(df)

    part_path = _write_part_table(table)
    with _lock:
        global _written_rows, _since_save_rows, _last_flush_time
        _written_rows += table.num_rows
        _since_save_rows = 0
        for c in _since_save_counts:
            _since_save_counts[c] = 0
        _last_flush_time = time.time()
    t1 = time.time()

    try:
        size_mb = part_path.stat().st_size / (1024 * 1024)
    except Exception:
        size_mb = -1
    logger.info(
        f"[flush] code={code} rows={table.num_rows} sec={t1-t0:.3f} "
        f"part={part_path.name} mb={size_mb:.2f}"
    )
    _emit_save_done(table.num_rows)
    return table.num_rows

def _flush_all(force: bool = False) -> int:
    global _written_rows, _since_save_rows, _last_flush_time

    with _lock:
        total_buf = sum(_buf_rows.values())
        if not force and total_buf < FLUSH_EVERY_N_ROWS:
            return 0
        dfs: list[pd.DataFrame] = []
        now = time.time()
        for c in codes:
            if _buffers[c]:
                dfs.extend(_buffers[c])
                _buffers[c] = []
                _buf_rows[c] = 0
                _last_flush_ts[c] = now

    if not dfs:
        # 데이터 없어도 타이머 리셋 → writer_loop가 매 0.5초마다 재시도하지 않도록
        with _lock:
            _last_flush_time = time.time()
        return 0

    df = pd.concat(dfs, ignore_index=True)
    table = _df_to_table_with_union(df)

    part_path = _write_part_table(table)
    with _lock:
        _written_rows += table.num_rows
        _since_save_rows = 0
        for c in _since_save_counts:
            _since_save_counts[c] = 0
        _last_flush_time = time.time()

    try:
        size_mb = part_path.stat().st_size / (1024 * 1024)
    except Exception:
        size_mb = -1
    logger.info(
        f"[flush] code=ALL rows={table.num_rows} part={part_path.name} mb={size_mb:.2f}"
    )

    _emit_save_done(table.num_rows)
    _log_stale_after_save(max(STALE_SEC, 10))
    return table.num_rows

def writer_loop():
    logger.info("[writer] started")
    while not _stop_event.is_set():
        try:
            with _lock:
                total_buf = sum(_buf_rows.values())
                last_flush = _last_flush_time
            time_ok = (time.time() - last_flush) >= FLUSH_EVERY_SEC
            if total_buf >= FLUSH_EVERY_N_ROWS or (time_ok and total_buf > 0):
                _flush_all(force=True)
        except Exception as e:
            logger.error(f"[writer] flush error: {e}")
            logger.error(traceback.format_exc())
        time.sleep(0.5)

    try:
        _flush_all(force=True)
    except Exception:
        pass
    logger.info("[writer] stopped")

# =============================================================================
# request -> tr_id 추정(가능하면)
# (라이브러리 구현마다 구조가 다를 수 있어 여러 방식으로 시도)
# =============================================================================
def _extract_tr_id(req) -> str | None:
    for key in ("tr_id", "trid", "trId", "TRID", "header_tr_id"):
        if hasattr(req, key):
            v = getattr(req, key)
            if isinstance(v, str) and v:
                return v
    if isinstance(req, dict):
        for key in ("tr_id", "trid", "trId", "TRID"):
            v = req.get(key)
            if isinstance(v, str) and v:
                return v
    return None

# =============================================================================
# 수신 콜백: 버퍼 적재만(저장 게이팅 + 컬럼 추가)
# - is_real_ccnl: "Y"=체결가, "N"=예상체결가
# - tr_kind: 어떤 스트림인지 (regular_real / regular_exp / overtime_real / overtime_exp)
# =============================================================================
def on_result(ws, tr_id, result, data_info):
    if result is None or getattr(result, "empty", True):
        return

    # 수신 시점 컬럼명 소문자 통일 (ccnl_krx 대문자 vs exp_ccnl_krx 소문자 → concat 시 중복 방지)
    result.columns = [str(c).strip().lower() for c in result.columns]
    if result.columns.duplicated().any():
        result = result.loc[:, ~result.columns.duplicated()]

    trid = str(tr_id)
    if trid not in TRID_TO_KIND and trid in KNOWN_TRID_MAP:
        kind, is_real = KNOWN_TRID_MAP[trid]
        TRID_TO_KIND[trid] = kind
        TRID_TO_IS_REAL[trid] = is_real
    else:
        kind = TRID_TO_KIND.get(trid, "unknown")
        is_real = TRID_TO_IS_REAL.get(trid, None)

    recv_ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S.%f")
    _ingest_queue.put((result, trid, kind, is_real, recv_ts))


def ingest_loop():
    global _last_print_ts, _last_summary_ts, _last_full_status_ts, _since_save_rows, _last_any_recv_ts
    logger.info("[ingest] started")
    while not _stop_event.is_set() or not _ingest_queue.empty():
        try:
            item = _ingest_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        if item is None:
            break
        result, trid, kind, is_real, recv_ts = item
        if kind == "unknown":
            kind = "unknown"
        if is_real is None and kind != "unknown":
            is_real = "N" if "exp" in kind else "Y"

        if print_option == 1:
            print(result)
            continue

        col_map = {str(c).strip().lower(): c for c in result.columns}
        code_col = col_map.get("mksc_shrn_iscd") or col_map.get("mksc_shrn_iscd".lower())
        if not code_col:
            continue

        now = time.time()

        # 저장 게이팅(시간대별)
        with _flags_lock:
            if kind == "regular_real":
                save = SAVE_REAL_REGULAR
            elif kind == "regular_exp":
                save = SAVE_EXP_REGULAR
            elif kind == "overtime_real":
                save = SAVE_REAL_OVERTIME
            elif kind == "overtime_exp":
                save = SAVE_EXP_OVERTIME
            else:
                save = (SAVE_MODE == "always")

        # 모니터링 카운트는 저장 여부와 무관하게 올리기
        try:
            tmp = result.copy()
            tmp[code_col] = tmp[code_col].astype(str).str.zfill(6)
            grouped = tmp.groupby(code_col, sort=False)
            with _lock:
                for code, df_code in grouped:
                    if code not in _per_sec_counts:
                        continue
                    n = len(df_code)
                    _per_sec_counts[code] = (_per_sec_counts[code] + n) % PER_SEC_ROLLOVER
                    _total_counts[code] += n
                    _since_save_rows += n
                    _since_save_counts[code] += n
                    _last_recv_ts[code] = now
                    _last_any_recv_ts = now
                    if kind == "regular_real" and _get_mode() == RunMode.CLOSE_REAL:
                        _regular_real_seen.add(code)
                    if kind == "overtime_real":
                        _overtime_real_seen.add(code)
        except Exception:
            pass

        # 최근 전일대비율 캐시 (09:00 급등 조건 판단용)
        try:
            prdy_ctrt_col = col_map.get("prdy_ctrt")
            if prdy_ctrt_col:
                tmp2 = result.copy()
                tmp2[code_col] = tmp2[code_col].astype(str).str.zfill(6)
                for code, df_code in tmp2.groupby(code_col, sort=False):
                    val = df_code[prdy_ctrt_col].iloc[-1]
                    try:
                        _last_prdy_ctrt[code] = float(val)
                    except Exception:
                        pass
        except Exception:
            pass

        if save:
            if is_real is None:
                is_real = "N" if "exp" in kind else "Y"

            try:
                df_save = result.copy()
                df_save[code_col] = df_save[code_col].astype(str).str.zfill(6)
                df_save["recv_ts"] = recv_ts
                df_save["tr_id"] = trid
                df_save["is_real_ccnl"] = is_real

                grouped2 = df_save.groupby(code_col, sort=False)
                with _lock:
                    for code, df_code in grouped2:
                        if code not in _buffers:
                            continue
                        n = len(df_code)
                        _buffers[code].append(df_code)
                        _buf_rows[code] += n
            except Exception:
                logger.error("[ingest] buffer append failed")
                logger.error(traceback.format_exc())

        if print_option == 2 and (now - _last_print_ts) >= 1.0:
            _print_counts()
            for c in _per_sec_counts:
                _per_sec_counts[c] = 0
            _last_print_ts = now

        if (now - _last_summary_ts) >= SUMMARY_EVERY_SEC:
            with _lock:
                total_rows = sum(_total_counts.values())
                buf_rows = sum(_buf_rows.values())
                saved_rows = _written_rows

            logger.info(
            f"[summary] total_rows={total_rows} saved_rows={saved_rows} "
            f"buffer_rows={buf_rows} parquet={FINAL_PARQUET_PATH.name}"
            )
            _last_summary_ts = now

        if (now - _last_full_status_ts) >= FULL_STATUS_EVERY_SEC:
            _log_full_progress()
            _last_full_status_ts = now
    logger.info("[ingest] stopped")

# =============================================================================
# 종료 처리
# =============================================================================
def _shutdown(reason: str):
    if _stop_event.is_set():
        return
    logger.warning(f"[shutdown] {reason}")
    _stop_event.set()
    _ws_rebuild_event.set()
    with _kws_lock:
        if _active_kws is not None:
            _request_ws_close(_active_kws)
    time.sleep(0.8)

def _handle_signal(signum, frame):
    _shutdown(f"signal={signum}")
    raise SystemExit(0)

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

def _request_ws_close(kws) -> None:
    for attr in ("close", "shutdown"):
        try:
            fn = getattr(kws, attr, None)
            if callable(fn):
                fn()
                return
        except Exception:
            pass
    for attr in ("ws", "wsapp", "websocket"):
        try:
            obj = getattr(kws, attr, None)
            fn = getattr(obj, "close", None)
            if callable(fn):
                fn()
                return
        except Exception:
            pass

# =============================================================================
# WebSocket 실행: 시간대별 구독 전환 + 자동 재연결
# =============================================================================
def _register_trid(req) -> str | None:
    rid = _extract_tr_id(req)
    if rid:
        if req in (ccnl_krx, overtime_ccnl_krx):  # noqa: F405
            TRID_TO_IS_REAL[rid] = "Y"
        else:
            TRID_TO_IS_REAL[rid] = "N"

        if req == ccnl_krx:  # noqa: F405
            TRID_TO_KIND[rid] = "regular_real"
        elif req == exp_ccnl_krx:  # noqa: F405
            TRID_TO_KIND[rid] = "regular_exp"
        elif req == overtime_ccnl_krx:  # noqa: F405
            TRID_TO_KIND[rid] = "overtime_real"
        elif req == overtime_exp_ccnl_krx:  # noqa: F405
            TRID_TO_KIND[rid] = "overtime_exp"
        else:
            TRID_TO_KIND[rid] = "unknown"
    return rid

def _send_subscribe(kws, req, data, tr_type: str) -> None:
    rid = _register_trid(req)
    for idx, chunk in enumerate(_chunks(data, MAX_CODES_PER_SESSION), start=1):
        try:
            kws.send_request(request=req, tr_type=tr_type, data=chunk)
            logger.info(
                f"{ts_prefix()} [subscribe] tr_type={tr_type} req={rid or 'unknown'} "
                f"group#{idx} codes={chunk}"
            )
        except Exception as e:
            logger.warning(f"{ts_prefix()} [subscribe] failed tr_type={tr_type} req={rid} err={e}")

def _apply_subscriptions(kws, desired: dict, force: bool = False) -> None:
    """
    desired: {request_func: set(종목코드)} — _desired_subscription_map()의 반환값
    종목별로 올바른 구독 타입을 적용합니다.
    """
    all_reqs = [ccnl_krx, exp_ccnl_krx, overtime_ccnl_krx, overtime_exp_ccnl_krx]  # noqa: F405

    if force:
        # ── force 재구독: UNSUBSCRIBE 없이 SUBSCRIBE만 ──────────────
        for req in all_reqs:
            want = desired.get(req, set())
            if want:
                _send_subscribe(kws, req, list(want), "1")
                _subscribed[req.__name__] = set(want)
            else:
                _subscribed.pop(req.__name__, None)
        return

    # ── 일반 모드 전환: 해지 먼저 (MAX SUBSCRIBE OVER 방지) ──────────────
    pending_adds: list[tuple] = []
    for req in all_reqs:
        want = desired.get(req, set())
        have = _subscribed.get(req.__name__, set())
        if want != have:
            to_add = list(want - have)
            to_del = list(have - want) if want else list(have)
        else:
            to_add = []
            to_del = []
        if to_del:
            _send_subscribe(kws, req, to_del, "2")
        if to_add:
            pending_adds.append((req, to_add))

    # ── 2단계: 신규 구독 (해지 완료 후) ──────────────────────
    for req, add_list in pending_adds:
        _send_subscribe(kws, req, add_list, "1")

    # ── 3단계: 전송 완료 후 _subscribed 갱신 ──────────────────────
    for req in all_reqs:
        want = desired.get(req, set())
        if want:
            _subscribed[req.__name__] = set(want)
        else:
            _subscribed.pop(req.__name__, None)

def _desired_subscription_map(now: datetime) -> dict:
    """
    종목별 구독 매핑을 반환: {request_func: set(종목코드)}
    - VI 종목은 09:00~09:02 예상체결 유지, 나머지는 실시간체결
    - 모드 전환 시 종목별로 정확한 구독 타입 지정
    """
    global _overtime_real_active
    t = now.time()
    all_codes = set(codes)

    if t < dtime(8, 50) or not all_codes:
        return {}
    if dtime(8, 50) <= t < dtime(9, 0):
        return {exp_ccnl_krx: all_codes}  # noqa: F405

    if dtime(9, 0) <= t < dtime(15, 20):
        # ── VI 종목별 분리: VI 종목=예상체결, 나머지=실시간체결 ──
        vi_codes = _vi_delayed_codes & all_codes  # 현재 구독 중인 VI 종목만
        normal_codes = all_codes - vi_codes
        result = {}
        if vi_codes:
            result[exp_ccnl_krx] = vi_codes   # noqa: F405
        if normal_codes:
            result[ccnl_krx] = normal_codes   # noqa: F405
        return result

    if dtime(15, 20) <= t < dtime(15, 30):
        return {exp_ccnl_krx: all_codes}  # noqa: F405

    if dtime(15, 30) <= t < dtime(16, 0):
        # 전 종목 종가 체결 수신 완료 → 구독 종료
        if len(_regular_real_seen) >= len(codes):
            return {}
        # 15:31 타임아웃 → 미수신 종목 있어도 종료
        if t >= dtime(15, 31):
            return {}
        # 15:30~15:31: 종가 체결가 수신 대기
        return {ccnl_krx: all_codes}  # noqa: F405

    if dtime(16, 0) <= t < END_TIME:
        req = overtime_ccnl_krx if _overtime_real_active else overtime_exp_ccnl_krx  # noqa: F405
        return {req: all_codes}

    return {}

def run_ws_forever():
    global _close_force_stopped
    backoff = 2
    attempt = 0

    while not _stop_event.is_set():
        mode = _get_mode()
        if mode == RunMode.EXIT:
            break

        attempt += 1
        kws = None
        try:
            logger.info(f"{ts_prefix()} [ws] connect attempt={attempt} mode={mode.name}")

            # open_map 초기화 후 현재 시각에 맞는 구독 등록 (종목별 매핑)
            ka.open_map.clear()
            _subscribed.clear()
            desired_map = _desired_subscription_map(datetime.now(KST))
            total_sub = sum(len(v) for v in desired_map.values())
            if total_sub > MAX_WSS_SUBSCRIBE:
                logger.warning(
                    f"{ts_prefix()} [ws] 구독 상한 초과 예상: {total_sub} > {MAX_WSS_SUBSCRIBE}"
                )
            for req, code_set in desired_map.items():
                ka.KISWebSocket.subscribe(req, list(code_set))
                _subscribed[req.__name__] = set(code_set)
            sub_desc = ", ".join(f"{r.__name__}={len(c)}종목" for r, c in desired_map.items())
            logger.info(
                f"{ts_prefix()} [ws] open_map prepared: {sub_desc} total={total_sub}"
            )

            # 재연결 도중 쌓인 이벤트 클리어
            # (open_map에 최신 codes가 이미 반영되었으므로 이전 이벤트는 무효)
            _ws_rebuild_event.clear()

            kws = ka.KISWebSocket(api_url="")
            with _kws_lock:
                global _active_kws
                _active_kws = kws

            def _on_system(rsp):
                if rsp.isOk and rsp.tr_msg and "SUCCESS" in rsp.tr_msg:
                    return
                if rsp.tr_msg:
                    logger.warning(
                        f"{ts_prefix()} [ws][system] tr_id={rsp.tr_id} tr_key={rsp.tr_key} msg={rsp.tr_msg}"
                    )

            kws.on_system = _on_system

            # 시간 플래그 즉시 1회 갱신
            update_time_flags()

            logger.info(f"{ts_prefix()} [ws] start on_result (parquet={FINAL_PARQUET_PATH.name})")
            kws.start(on_result=on_result)

            logger.info(f"{ts_prefix()} [ws] kws.start returned (mode={mode.name})")

        except SystemExit:
            break
        except Exception as e:
            logger.error(f"{ts_prefix()} [ws] connection exception: {e}")
            logger.error(traceback.format_exc())
        finally:
            with _kws_lock:
                if _active_kws is kws:
                    _active_kws = None
            # ── 연결 끊김 → 즉시 타이머 리셋 ──
            # scheduler_loop의 60초 미수신 타이머가 끊긴 시점 기준으로
            # 불필요한 UNSUBSCRIBE→SUBSCRIBE를 하지 않도록 리셋
            global _last_any_recv_ts
            _last_any_recv_ts = 0.0
            _subscribed.clear()  # 서버 세션 사라짐 → 구독 상태 초기화

        if _stop_event.is_set():
            break

        # 종가 체결 수신 완료 후 OR 15:31~16:00 구간 → 16:00까지 대기
        # (_close_force_stopped 플래그뿐 아니라 시간도 직접 체크하여 레이스 컨디션 방지)
        now_t = datetime.now(KST).time()
        should_wait = (
            _close_force_stopped
            or (dtime(15, 31) <= now_t < dtime(16, 0))
        )
        if should_wait and now_t < dtime(16, 0):
            _close_force_stopped = True  # 플래그 보정
            logger.info(f"{ts_prefix()} [ws] 종가 체결 수신 완료, 16:00 시간외까지 대기")
            while not _stop_event.is_set():
                if datetime.now(KST).time() >= dtime(16, 0):
                    break
                time.sleep(1.0)
            logger.info(f"{ts_prefix()} [ws] 16:00 도달 → 시간외 단일가 연결 시작")
        else:
            logger.info(f"{ts_prefix()} [ws] reconnect in {backoff}s...")
            time.sleep(backoff)

    logger.info(f"{ts_prefix()} [ws] stopped")

# =============================================================================
# main
# =============================================================================
if __name__ == "__main__":
    if is_holiday():
        msg = f"{ts_prefix()} {PROGRAM_NAME} => 오늘은 휴일이므로 프로그램을 종료합니다."
        _notify(msg, tele=True)
        sys.exit(0)
    else:
        msg = f"{ts_prefix()} {PROGRAM_NAME} => 오늘은 개장일이므로 프로그램을 시작합니다."
        _notify(msg, tele=True)
    _notify(f"{ts_prefix()} {PROGRAM_NAME} 시작", tele=True)
    logger.info(f"[save_mode] {SAVE_MODE}")
    logger.info(f"[flush] total buffer >= {FLUSH_EVERY_N_ROWS} rows OR {FLUSH_EVERY_SEC}s")
    logger.info(f"[monitor] summary_every={SUMMARY_EVERY_SEC}s stale={STALE_SEC}s")
    logger.info(f"[parquet] -> {FINAL_PARQUET_PATH}")

    if datetime.now(KST).time() >= END_TIME:
        parts = list(PART_DIR.glob("*.parquet"))
        parts_count = len(parts)
        prefix = ts_prefix()
        sys.stdout.write(f"{prefix} 종료 시각 경과 -> 병합 점검")
        sys.stdout.write(f"=> 병합대상 파일 : 총 {parts_count}개\n")
        sys.stdout.flush()
        # stdout는 이미 출력했으므로, 로그/텔레그램만 추가
        logger.info(f"{prefix} 종료 시각 경과 -> 병합 점검 => 병합대상 파일 : 총 {parts_count}개")
        if tmsg is not None:
            try:
                tmsg(
                    f"{prefix} 종료 시각 경과 -> 병합 점검 => 병합대상 파일 : 총 {parts_count}개",
                    "-t",
                )
            except Exception:
                pass

        _notify(f"{ts_prefix()} 파일 병합 시작", tele=True)
        _merge_parts()
        _notify(f"{ts_prefix()} 파일 병합 완료 file={FINAL_PARQUET_PATH}", tele=True)
        _notify(f"{ts_prefix()} {PROGRAM_NAME} 종료", tele=True)
        raise SystemExit(0)

    # 워커 시작
    t_writer = threading.Thread(target=writer_loop, daemon=True)
    t_writer.start()

    t_ingest = threading.Thread(target=ingest_loop, daemon=True)
    t_ingest.start()

    # 시간 플래그 루프 시작
    t_flags = threading.Thread(target=time_flag_loop, daemon=True)
    t_flags.start()

    # 스케줄러 시작
    t_sched = threading.Thread(target=scheduler_loop, daemon=True)
    t_sched.start()

    # REST 요청 워커
    t_rest = threading.Thread(target=_rest_worker, daemon=True)
    t_rest.start()

    # REST 현재가 보강 워치독/요청 루프
    t_price_watch = threading.Thread(target=_price_watchdog_loop, daemon=True)
    t_price_watch.start()
    t_price_req = threading.Thread(target=_price_request_loop, daemon=True)
    t_price_req.start()

    # 상위 랭킹 추가 구독 루프 시작
    t_top = threading.Thread(target=_top_rank_loop, daemon=True)
    t_top.start()

    try:
        run_ws_forever()
    finally:
        _shutdown("finally")
        try:
            t_writer.join(timeout=2.0)
            t_ingest.join(timeout=2.0)
            t_flags.join(timeout=2.0)
            t_sched.join(timeout=2.0)
            t_rest.join(timeout=2.0)
            t_price_watch.join(timeout=2.0)
            t_price_req.join(timeout=2.0)
            t_top.join(timeout=2.0)
        except Exception:
            pass
        # 18:00 종료 시도했거나, 현재 18:00 이후면 병합 (WS 지연 종료 시에도 병합)
        if _end_time_reached or datetime.now(KST).time() >= END_TIME:
            _notify(f"{ts_prefix()} 파일 병합 시작: {FINAL_PARQUET_PATH}", tele=True)
            _merge_parts()
            _notify(f"{ts_prefix()} 파일 병합 완료: {FINAL_PARQUET_PATH}", tele=True)
        else:
            parts_n = len(list(PART_DIR.glob("*.parquet")))
            _notify(f"{ts_prefix()} 중간 종료 → parts {parts_n}개 유지 (다음 시작 시 병합)")
        _flush_overwrite_log()
        _notify(f"{ts_prefix()} {PROGRAM_NAME} 종료", tele=True)
        logger.info("=== WSS END ===")
