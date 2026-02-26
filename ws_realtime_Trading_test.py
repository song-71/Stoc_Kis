"""
data2 모드 명령어
입력	동작
Enter	다음 틱 1건 진행
HHMMSS	해당 시각까지 일괄 진행 (예: 0930, 093000)
c	    다음 액션(매수/매도/이벤트)이 발생할 때까지 진행 — 어떤 종목이든
cc	    현재 보고 있는 종목의 다음 액션까지 진행 — 다른 종목 액션은 무시
q	    끝까지 멈춤 없이 실행
"""
# =============================================================================
from datetime import time as dt_time  # 설정부 전용 early import
DATA_MODE = "data"  # "wss" / "data" / "data2" / "data3"
#  wss   : 실시간 웹소켓
#  data  : parquet 재생 (멈춤 없이)
#  data2 : parquet 재생 + 틱별 대화형 검증
#  data3 : data2 + 틱마다 주요 state 변수 표시
DATA_DATE = "260213"  # data/data2 모드에서 사용할 일자(YYMMDD)
DATA_SIM_END = None   # data/data2 모드 시뮬레이션 종료 시각 (None=15:30까지 전체, 예: dt_time(9,15) → 09:15에 종료)
_IS_DATA_MODE = DATA_MODE in ("data", "data2", "data3")  # data 계열 모드 판별
LOG_RESET = True  # True면 오늘 로그파일 삭제 후 새로 기록
pdy_close_buy = 0     # 전일 종가 매수 여부 설정, True면 전일 종가 매수, False면 시가>=전일 종가일 때 시가 매수
print_option = 0  # 이곳의 내용을 초기에 로드하되, 이후 config에 등록된 값을 읽어 적용하도록 함
#   - 0 : 수신현황 로그 출력은 생략하고 트레이딩 로그 위주로 출력
#   - 1 : 수신되는 카운트를 종목별로 "삼성(0/00)..." 형식으로 출력
#   - 2 : 수신되는 카운트를 초당 수신수량, 이번건 누적 수량/수신되고 있는 종목수/ 총 종목수 등으로 표시 (기본값)

#260213 수신대상 종목(업종) 목록 : 26개
CODE_TEXT = """
아로마티카
오리엔트바이오
태광산업
"""
"""
삼진식품
아로마티카
오리엔트바이오
태광산업
뉴인텍
제이케이시냅스
오리엔트정공
제이티
에스코넥
아이센스
현대ADM
유투바이오
원익IPS
이지스
서남
SKAI
레이저쎌
씨케이솔루션

"""

"""
nohup 실행:
  nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/ws_realtime_Trading_test.py > /home/ubuntu/Stoc_Kis/out/wss_trading_test.out 2>&1 &

로그 모니터링:
  <노협 백그라운드 실행 모니터링>
  tail -f /home/ubuntu/Stoc_Kis/out/wss_trading_test.out
  <터미널 실행 모니터링>
  tail -f /home/ubuntu/Stoc_Kis/out/logs/wss_{YYMMDD}_trade_test.log

프로세스 확인:
  pgrep -af ws_realtime_Trading_test.py

일괄 종료:현재 떠있는 여러 프로세스를 동시에 종료하는 명령어
  pkill -f ws_realtime_Trading_test.py
  
  <정말 안죽으면>
  pkill -9 -f ws_realtime_Trading_test.py

종료 확인:
  pgrep -af ws_realtime_Trading_test.py || echo "no process"


항목	이전	변경 후
K	0.3	0.6
K2	0.5	0.7
TRAIL_BUFFER_TABLE	1.0% / 0.7% / 0.5%	3.0% / 2.0% / 1.5%
TRAIL_CONFIRM_TICKS	2	5
BREAK_CONFIRM_TICKS	3	10
BELOW_BUY_BUFFER	1.0%	2.5%
BELOW_BUY_CONFIRM_TICKS	3	5
EARLY_SESSION_MULTIPLIER	2.0	1.5
추가된 새로운 로직 (Phase 2)

wss 데이터 파일 최종 저장위치 : /home/ubuntu/Stoc_Kis/data/wss_data/
wss 데이터 부분 파일 저장위치 : /home/ubuntu/Stoc_Kis/data/wss_data/parts_tr/
wss 데이터 백업 파일 저장위치 : /home/ubuntu/Stoc_Kis/data/wss_data/backup_tr/
데이터 파일명: {YYMMDD}_wss_data_tr.parquet  (parquet-2와 중복 방지)

"""


import sys
import os
import time
import shutil
import signal
import threading
import traceback
import queue
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import requests

import logging
from logging.handlers import RotatingFileHandler

try:
    from telegMsg import tmsg
except Exception:
    tmsg = None



# =============================================================================
# 사용자 설정
# =============================================================================
# 모드 설정:


SAVE_MODE = "always"  # 저장 스위치 모드
FLUSH_EVERY_SEC = 60  # 강제 flush 주기(초)- 즉, 60초에 한번 저장(60초, 1000건 중 먼저 도달하면 저장)
FLUSH_EVERY_N_ROWS = 1000  # 누적 행수 flush 기준- 즉, 1000행에 한번 저장(60초, 1000건 중 먼저 도달하면 저장)
CLOSE_STOP_TIME = dt_time(15, 31)  # 15:30 장마감 체결 후 구독 강제 중단 시각
MAX_CODES_PER_SESSION = 10  # 구독 요청당 종목 수
SUMMARY_EVERY_SEC = 30  # 요약 출력 주기(초)
STALE_SEC = 20  # 미수신 경고 기준(초)
NO_DATA_WARN_SEC = 60  # 전체 미수신 경고 기준(초)
NO_DATA_REBUILD_SEC = 60  # 전체 미수신 재구독 기준(초)
PER_SEC_ROLLOVER = 1000  # 초당 카운트 롤오버
ENABLE_SAVE = (DATA_MODE == "wss")  # wss 모드만 저장

# REST 초당 호출한도 중앙 제어(초당 10건 이내)
REST_MAX_PER_SEC = 10
REST_AFTER_WINDOW_DELAY = 0.1

# 장중 미수신 감시 — REST 거래상태 확인
STALE_CHECK_SEC = 60            # 종목별 미수신 기준(초) → REST 상태 확인 트리거
STALE_CHECK_HALTED_SEC = 300    # 거래중단 확인 후 재확인 간격(초)
STALE_CHECK_VI_SEC = 30         # VI 발동 종목 재확인 간격(초)
STALE_CHECK_INACTIVE_SEC = 120  # 거래비활성 종목 재확인 간격(초)

# 상승률 상위 종목 추가 구독 (wss 모드만)
TOP_RANK_ENABLED = True
TOP_RANK_END = dt_time(15, 10)         # 15:09 종가매매 종목 선정까지 포함
TOP_RANK_N = 50                        # 상승률 상위 다운로드 수
TOP_RANK_ADD_N = 5                     # 랭킹 결과에서 실제 구독 추가할 최대 종목 수
TOP_RANK_REMOVE_THRESHOLD = 20         # 이 순위 밖으로 밀린 종목만 해제
TOP_RANK_MARKET_DIV = "J"
TOP_RANK_OUT_DIR = Path("/home/ubuntu/Stoc_Kis/data/fetch_top_list")
MAX_WSS_SUBSCRIBE = 41                 # WSS 구독 총 상한
# 15:09 종가매매 종목 선정
CLOSING_TOP_N = 10                     # 종가매매용 ST 종목 선정 수

# 거래 전략 설정
TRAIL_STEPS = sorted([0.01, 0.02, 0.03, 0.04, 0.05, 0.10, 0.15, 0.20, 0.25, 0.29])
""" => 정식 트레이딩시에는 실제 거래내역으로 대체 필요 """

# 트레일링 스톱 버퍼 설정 (고점 대비 하락 허용 비율)
#  - 수익률 구간별로 버퍼를 다르게 적용
#  - 수익 초기: 넓은 버퍼 (호가 노이즈 흡수)
#  - 수익 확대: 좁은 버퍼 (이익 보호)
#  형식: (수익률_하한, 고점대비_하락허용비율)  — 수익률 오름차순
TRAIL_BUFFER_TABLE = [
    (0.00, 0.030),   # 0~3% 수익: 고점 대비 3.0% 하락 허용
    (0.03, 0.020),   # 3~10% 수익: 고점 대비 2.0% 하락 허용
    (0.10, 0.015),   # 10%+ 수익: 고점 대비 1.5% 하락 허용
]
# 상한가 근접 트레일 설정 (전일종가 대비 29% 이상일 때)
#  - 상한가(+30%) 부근에서는 추가 상승 여지가 거의 없음
#  - trail_stop = bid1 (매수1호가)로 동적 추적
#  - 장 초반 배율 미적용 (상한가 근접은 시간 불문 타이트 유지)
#  - TRAIL_CONFIRM_TICKS 연속 price < bid1 → 매도 (bid1 이탈 = 상한가 풀림 신호)
LIMIT_UP_THRESHOLD = 0.29            # 전일종가 대비 이 비율 이상이면 상한가 근접
LIMIT_UP_TRAIL_BUFFER = 0.003        # fallback: bid1 없을 때 고점 대비 0.3% 하락 허용
# 트레일 매도 확인 횟수 (이 횟수 연속 trail_stop 이하일 때만 매도)
#  - 1: 즉시 매도 (기존 방식)
#  - 2~3: 호가 노이즈 1~2틱 무시
TRAIL_CONFIRM_TICKS = 5

# 매수가 이탈(below_buy) 버퍼 설정
#  - 매수 직후 트레일 미작동 구간에서 호가 노이즈로 인한 조기 손절 방지
#  - 매수가 × (1 - BELOW_BUY_BUFFER) 이하로 연속 N틱 이탈 시 매도
BELOW_BUY_BUFFER = 0.025         # 매수가 대비 2.5% 하락 허용
BELOW_BUY_CONFIRM_TICKS = 5     # 5틱 연속 이탈 시 매도

# 중첩돌파 매수 확인 횟수
#  - price >= break_pr 이 연속 N틱 이상이어야 매수 (가짜 돌파 방지)
#  - 1: 즉시 매수 (기존 방식, 노이즈에 취약)
#  - 3: 3틱 연속 돌파 확인 후 매수 (안정적)
BREAK_CONFIRM_TICKS = 10

# 장 초반(09:00~09:30) 버퍼 배율
#  - 장 초반은 변동성이 크므로 버퍼를 넓게 적용
#  - 아래 시간대에는 TRAIL_BUFFER_TABLE과 BELOW_BUY_BUFFER에 이 배율을 곱함
EARLY_SESSION_END = dt_time(9, 30, 0)   # 장 초반 종료 시각
EARLY_SESSION_MULTIPLIER = 1.5          # 버퍼 1.5배 (3%→4.5%, 2%→3%, 1.5%→2.25%)

SLIPPAGE = 0.005   
K = 0.6
K2 = 0.7
PREOPEN_BUY_ENABLED = True                 # 프리오픈(시가) 매수 활성화 여부
TRADE_START_DELAY = dt_time(9, 10, 0)       # 중첩돌파 매매 시작 시각 (프리오픈과 별도)
MAX_TRADES_PER_STOCK = 30                    # 종목당 하루 최대 거래 횟수  3
COOLDOWN_SECONDS = 0                      # 매도 후 재진입 대기 시간(초) 120
MIN_HOLD_SECONDS = 0                       # 최소 보유 시간(초)  30
VWAP_FILTER_ENABLED = True                  # VWAP 추세 필터 활성화
REAL_TRADE_START = dt_time(9, 0, 0)
REAL_TRADE_END = DATA_SIM_END if (_IS_DATA_MODE and DATA_SIM_END is not None) else dt_time(15, 30, 0)
max_loss_rt = 5  # 연속 손실 한계 횟수(이 횟수가 도달하면 그 종목은 거래 종료)
NO_HIGH_STOP_TIME = dt_time(9, 29, 0)  # 이 시각까지 고가가 전일종가 미달이면 거래 종료
NO_OPEN_WAIT_SEC = 2  # 09:00 이후 예상체결 미수신 N초 + 시가 미형성 기준
VI_OPEN_WAIT_SEC = 60  # VI 종목 시가 형성 대기 시간(초) — 09:02:00 이후 ~09:02:30 시가 생성
PREOPEN_ORDER_START = dt_time(8, 59, 55)  # 이 시각부터 예상체결가 주문 시작
PREOPEN_VI_CUTOFF = dt_time(8, 59, 59)  # VI 연장 판단 마감 시각
VI_ORDER_START = dt_time(9, 1, 58)  # VI 종목 주문 시작 시각 (09:02:00 일괄체결 2초 전)

# 장 운영시간 (트레이딩 판단용)
PREOPEN_START = dt_time(8, 50, 0)
PREOPEN_END = dt_time(9, 0, 0)
PREOPEN_EXTEND_END = dt_time(9, 2, 0)  # VI 일괄 체결 시점 (예상체결가 수신 종료)
CLOSE_AUC_START = dt_time(15, 10, 0)
CLOSE_AUC_END = dt_time(15, 30, 0)
OVERTIME_START = dt_time(16, 0, 0)
OVERTIME_END = dt_time(18, 0, 0)

# 거래량 기반 중첩돌파 옵션
VOLUME_BREAKOUT_ENABLED = False
VOLUME_LOOKBACK_MIN = 5

INITIAL_CAPITAL = 10_000_000
# 휴무일(공휴일) 등록: YYMMDD 문자열 리스트, wss 모드에서만 적용 (data 모드에는 영향 없음)
# - 다가오는 휴무일을 필요에 따라 추가
HOLIDAYS = [
    "260216",  # 설 연휴
    "260217",  # 설 연휴
    "260218",  # 설 연휴
    "260302",  # 삼일절 대체공휴일(예시)
]


# =============================================================================
# [중요] 라이브러리 INFO 로그(예: "received message >>") 원천 차단
# =============================================================================
logging.basicConfig(level=logging.WARNING, force=True)
logging.getLogger("websockets").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.ERROR)


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
_log_time_override = threading.local()
_last_data_dt: datetime | None = None
_data_first_dt: datetime | None = None
_data_last_dt: datetime | None = None
_data_row_count: int = 0
_summary_written = False
_all_trading_stopped = False
_data_sim_start_time: float | None = None  # data 모드 시뮬레이션 시작 시각 (첫 틱 처리 시점)
_data_sim_elapsed: float = 0.0   # 거래종료까지 소요(초)
_data_summary_elapsed: float = 0.0  # 요약 생성만 소요(초)
_last_data_progress_ts: float = 0.0  # data 모드 1초 간격 진행 출력용
_data_row_count_at_last_progress: int = 0  # 직전 진행 출력 시점의 누적 row 수


def _current_log_dt() -> datetime:
    override_dt = getattr(_log_time_override, "dt", None)
    if override_dt is not None:
        return override_dt
    if _IS_DATA_MODE and _last_data_dt is not None:
        return _last_data_dt
    return datetime.now(KST)


def ts_prefix() -> str:
    return _current_log_dt().strftime("[%y%m%d_%H%M%S]")


def _is_weekend(now: datetime) -> bool:
    return now.weekday() >= 5


def _is_holiday(now: datetime) -> bool:
    yymmdd = now.strftime("%y%m%d")
    return yymmdd in set(HOLIDAYS)


# =============================================================================
# 출력: kis_utils LogManager 공통 사용 (ts_prefix 포함 메시지는 log_tm_raw)
#   1단계 _out_screen : 화면만
#   2단계 _out_log    : 화면 + 로그
#   3단계 _notify     : 화면 + 로그 + 텔레그램 (호출 시 f"{ts_prefix()} 메시지" 형태로 전달)
# =============================================================================
def _out_screen(msg: str) -> None:
    """1단계: 화면만 (접두어 자동 추가)"""
    _log_mgr.log_screen(msg)


def _out_log(msg: str) -> None:
    """2단계: 화면 + 로그 (접두어 자동 추가)"""
    _log_mgr.log(msg)


def _notify(msg: str) -> None:
    """3단계: 화면 + 로그 (+ 텔레그램, data 모드에서는 미발송)"""
    if _IS_DATA_MODE:
        _log_mgr.log_raw(msg)
    else:
        _log_mgr.log_tm_raw(msg)


def _finish_progress_and_notify(msg: str) -> None:
    """진행 라인 유지 후 다음 줄에 메시지 출력 (\\n + _notify)"""
    sys.stdout.write("\n")
    sys.stdout.flush()
    _notify(msg)


# =============================================================================
# 경로
# =============================================================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROGRAM_NAME = Path(__file__).name
OUT_DIR = SCRIPT_DIR / "data" / "wss_data"  
OUT_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = SCRIPT_DIR / "config.json"
ACCOUNT_ID = os.environ.get("KIS_ACCOUNT_ID") or None

LOG_DIR = SCRIPT_DIR / "out" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

today_yymmdd = DATA_DATE if _IS_DATA_MODE else datetime.now(KST).strftime("%y%m%d")
_data_base_date = datetime.strptime(DATA_DATE, "%y%m%d").date() if _IS_DATA_MODE else None
FINAL_PARQUET_PATH = OUT_DIR / f"{today_yymmdd}_wss_data_tr.parquet"
PART_DIR = OUT_DIR / "parts_tr"
PART_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR = OUT_DIR / "backup_tr"
LOG_SUFFIX = "_data_mode" if _IS_DATA_MODE else ""
LOG_PATH = LOG_DIR / f"wss_{today_yymmdd}_trade_test{LOG_SUFFIX}.log"

if LOG_RESET:
    for p in LOG_DIR.glob(f"wss_{today_yymmdd}_trade_test{LOG_SUFFIX}*.log"):
        try:
            p.unlink()
        except Exception:
            pass

# 거래 장부
LEDGER_DIR = SCRIPT_DIR / "out" / "ledgers"
LEDGER_DIR.mkdir(parents=True, exist_ok=True)
TRADE_LEDGER_PATH = LEDGER_DIR / f"trade_ledger_{today_yymmdd}.csv"

# =============================================================================
# 로깅 (콘솔 + 파일)
# =============================================================================
logger = logging.getLogger("wss_trade_test")
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

class KstFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = _current_log_dt()
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S")


fmt = KstFormatter("%(asctime)s | %(levelname)s | %(message)s")
class DropPlainNotifyFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not getattr(record, "plain_notify", False)

ch.addFilter(DropPlainNotifyFilter())
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

logger.addFilter(DropReceivedMessageFilter())
logging.getLogger("domestic_stock_functions_ws").addFilter(DropReceivedMessageFilter())

logger.info("=== WSS TRADE TEST START ===")
logger.info(f"[paths] parquet={FINAL_PARQUET_PATH}  parts={PART_DIR}  log={LOG_PATH}")

# 전략 오류 전용 로그
TRADE_ERR_LOG_PATH = LOG_DIR / f"wss_{today_yymmdd}_trade_test_errors.log"
trade_err_logger = logging.getLogger("wss_trade_test_errors")
trade_err_logger.setLevel(logging.ERROR)
trade_err_fh = RotatingFileHandler(
    filename=str(TRADE_ERR_LOG_PATH),
    maxBytes=20 * 1024 * 1024,
    backupCount=3,
    encoding="utf-8",
)
trade_err_fh.setLevel(logging.ERROR)
trade_err_fh.setFormatter(fmt)
trade_err_logger.handlers.clear()
trade_err_logger.addHandler(trade_err_fh)
trade_err_logger.propagate = False

def _cleanup_old_temp_dirs() -> None:
    for p in OUT_DIR.glob("*_backup"):
        shutil.rmtree(p, ignore_errors=True)
    for p in OUT_DIR.glob("*_parts"):
        shutil.rmtree(p, ignore_errors=True)
    PART_DIR.mkdir(parents=True, exist_ok=True)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

_cleanup_old_temp_dirs()

# =============================================================================
# open-trading-api 예제 경로 추가
# =============================================================================
logger.info(f"{ts_prefix()} [init] loading api modules...")
OPEN_API_WS_DIR = Path.home() / "open-trading-api" / "examples_user" / "domestic_stock"
sys.path.append(str(OPEN_API_WS_DIR))

ka = None
if DATA_MODE == "wss":
    logger.info(f"{ts_prefix()} [init] import kis_auth_llm...")
    import kis_auth_llm as ka  # noqa: E402
    logger.info(f"{ts_prefix()} [init] import kis_auth_llm done")
    sys.modules["kis_auth"] = ka

    _import_watchdog_stop = threading.Event()

    def _import_watchdog(label: str, stop_evt: threading.Event) -> None:
        while not stop_evt.wait(5.0):
            logger.info(f"{ts_prefix()} [init] still importing {label}...")

    logger.info(f"{ts_prefix()} [init] import domestic_stock_functions_ws...")
    _import_watchdog_stop.clear()
    _t_import = threading.Thread(
        target=_import_watchdog,
        args=("domestic_stock_functions_ws", _import_watchdog_stop),
        daemon=True,
    )
    _t_import.start()
    from domestic_stock_functions_ws import *  # noqa: F403,E402
    _import_watchdog_stop.set()

    # ★ import * 이 logger 를 덮어쓰므로 재설정
    logger = logging.getLogger("wss_trade_test")  # noqa: F811

    logger.info(f"{ts_prefix()} [init] import domestic_stock_functions_ws done")

logger.info(f"{ts_prefix()} [init] import kis_utils...")
import kis_utils  # noqa: E402
from kis_utils import (  # noqa: E402
    load_config,
    resolve_account_config,
    load_symbol_master,
    save_config,
    stQueryDB,
    calc_limit_up_price,
    print_table,
    LogManager,
)
from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig  # noqa: E402
logger.info(f"{ts_prefix()} [init] import kis_utils done")
logger.info(f"{ts_prefix()} [init] api modules loaded")

# LogManager: kis_utils 공통 3단계 출력 (화면/로그/텔레그램), prefix_callback으로 ts_prefix 사용
_log_mgr = LogManager(
    str(LOG_DIR),
    log_name=LOG_PATH.name,
    prefix_callback=ts_prefix,
)

# =============================================================================
# 인증
# =============================================================================
def _auth() -> None:
    logger.info(f"{ts_prefix()} [auth] start")
    ka.auth(svr="prod")
    ka.auth_ws(svr="prod")
    if not getattr(ka, "_base_headers_ws", {}).get("approval_key"):
        raise RuntimeError("auth_ws() 실패: kis_devlp.yaml의 앱키/시크릿을 확인하세요.")
    logger.info(f"{ts_prefix()} [auth] ok")


def _parse_codes(text: str) -> list[str]:
    name_to_code = {}
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"]))
    except Exception:
        name_to_code = {}
    codes: list[str] = []
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        if raw.isdigit():
            codes.append(raw.zfill(6))
            continue
        code = name_to_code.get(raw)
        if code:
            codes.append(code)
    return codes


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


def _refresh_print_option() -> None:
    global print_option
    cfg = _read_cfg()
    if not cfg:
        return
    val = cfg.get("print_option", "")
    if val == "":
        return
    try:
        num = int(val)
    except Exception:
        return
    if num in (0, 1, 2):
        print_option = num


def _print_option_loop() -> None:
    logger.info("[print_option] watcher started")
    # data 모드: 1초마다 체크해 종료 시 빠르게 exit (wss는 60초)
    sleep_sec = 1.0 if _IS_DATA_MODE else 60.0
    while not _stop_event.is_set():
        try:
            _refresh_print_option()
        except Exception:
            pass
        # wait() 사용 시 _stop_event 설정 즉시 wake → join 대기 최소화
        _stop_event.wait(timeout=sleep_sec)
    if _IS_DATA_MODE:
        logger.info("[모든 종목 거래 종료] 현재 더 이상 모니터링 대상 종목이 없어 거래를 종료합니다.(현재 data 모드 시뮬레이션을 종료함.)")
    else:
        logger.info("[모든 종목 거래 종료] 현재 더 이상 모니터링 대상 종목이 없어 거래를 종료합니다.(다만 wss 수신은 지속합니다.)")


def _persist_subscription_codes(all_codes: list[str]) -> None:
    cfg = _read_cfg()
    if cfg is None:
        return
    cfg["wss_subscribe_date"] = today_yymmdd
    cfg["wss_subscribe_codes"] = sorted(set(all_codes))
    # 탑 랭킹으로 추가된 종목도 별도 저장 (재시작 시 복원용)
    cfg["wss_top_added_codes"] = sorted(_top_added_codes) if "_top_added_codes" in globals() else []
    try:
        save_config(cfg, str(CONFIG_PATH))
    except Exception:
        pass


_loaded_top_added: list[str] = []  # config에서 복원한 top_added (초기화 전 임시 저장)

def _load_codes_from_cfg(base_codes: list[str]) -> list[str]:
    """config 우선 로드. 09:00 이전 시작 시 config 초기화 → CODE_TEXT만 사용."""
    global _loaded_top_added
    now_t = datetime.now(KST).time()

    # ── 09:00 이전 시작 → config 초기화, CODE_TEXT 원본으로 시작 ──
    if now_t < dt_time(9, 0):
        logger.info(
            f"[codes] 09:00 이전 시작 → config 초기화, CODE_TEXT base({len(base_codes)}개)로 시작"
        )
        _loaded_top_added = []
        return list(base_codes)

    # ── 09:00 이후 시작 (재시작) → 당일 config 복원 ──
    cfg = _read_cfg()
    if cfg is None:
        return base_codes

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


def _load_1d_cache() -> dict[str, dict[str, float]]:
    """1d parquet DB에서 종목별 pdy_close, R_avr 등을 캐시로 로드.
    - data 모드: DATA_DATE에 해당하는 행 사용 (시뮬레이션 날짜의 전일종가)
    - wss 모드:  최신 행 사용 (오늘의 전일종가)
    """
    base_dir = Path(__file__).resolve().parent
    path = base_dir / "data" / "1d_data" / "kis_1d_unified_parquet_DB.parquet"
    if not path.exists():
        logger.warning(f"{ts_prefix()} [init] 1d parquet not found: {path}")
        return {}
    cols = ["date", "symbol", "close", "pdy_close", "pdy_ctrt", "R_avr"]
    t0 = time.perf_counter()
    try:
        df = pl.read_parquet(path, columns=cols)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [init] 1d parquet read failed: {e}")
        return {}
    if df.is_empty():
        return {}
    if "date" in df.columns:
        if _IS_DATA_MODE and _data_base_date is not None:
            # data 모드: DATA_DATE에 해당하는 행 사용
            target = _data_base_date
            df_target = df.filter(pl.col("date").cast(pl.Date) == target)
            if df_target.is_empty():
                # 해당 날짜가 없으면 직전 날짜의 close를 pdy_close로 사용
                df_before = df.filter(pl.col("date").cast(pl.Date) < target)
                if not df_before.is_empty():
                    df_target = df_before.sort("date", descending=True).group_by("symbol").first()
                    logger.info(f"{ts_prefix()} [init] DATA_DATE {target} not in DB, using previous date's close as pdy_close")
                else:
                    df_target = df.sort("date", descending=True).group_by("symbol").first()
                    logger.warning(f"{ts_prefix()} [init] no data before {target}, falling back to latest")
            else:
                logger.info(f"{ts_prefix()} [init] 1d cache using DATA_DATE={target} rows")
            df = df_target
        else:
            # wss 모드: 최신 행 사용
            df = df.sort("date", descending=True).group_by("symbol").first()
    cache: dict[str, dict[str, float]] = {}
    if "symbol" in df.columns:
        sel_cols = ["symbol", "close", "pdy_close", "pdy_ctrt", "R_avr"]
        sel_cols = [c for c in sel_cols if c in df.columns]
        rows = df.select(sel_cols).iter_rows(named=True)
        for row in rows:
            code = str(row["symbol"]).zfill(6)
            cache[code] = {
                "close": float(row.get("close") or 0),
                "pdy_close": float(row.get("pdy_close") or 0),
                "pdy_ctrt": float(row.get("pdy_ctrt") or 0),
                "R_avr": float(row.get("R_avr") or 0),
            }
    elapsed = (time.perf_counter() - t0) * 1000
    logger.info(f"{ts_prefix()} [init] 1d cache loaded rows={len(cache)} in {elapsed:.1f}ms")
    return cache


def _chunks(lst: list[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


# =============================================================================
# 시간대별 저장 스위치
# =============================================================================
SAVE_REAL_REGULAR = False
SAVE_EXP_REGULAR = False
SAVE_REAL_OVERTIME = False
SAVE_EXP_OVERTIME = False

_flags_lock = threading.RLock()


class RunMode:
    PREOPEN_WAIT = "PREOPEN_WAIT"
    PREOPEN_EXP = "PREOPEN_EXP"
    REGULAR_REAL = "REGULAR_REAL"
    CLOSE_EXP = "CLOSE_EXP"
    CLOSE_REAL = "CLOSE_REAL"
    OVERTIME_EXP = "OVERTIME_EXP"
    OVERTIME_REAL = "OVERTIME_REAL"
    STOP = "STOP"
    EXIT = "EXIT"


_mode_lock = threading.RLock()
_current_mode: str | None = None
_ws_rebuild_event = threading.Event()
_kws_lock = threading.RLock()
_active_kws = None
_last_wait_mode: str | None = None

END_TIME = dt_time(18, 0)
_regular_real_seen: set[str] = set()
_close_force_stopped = False          # 15:30 유예시간 후 강제 구독 중단 1회 로그용
_overtime_real_seen: set[str] = set()
_overtime_real_active = False
_last_overtime_real_slot: datetime | None = None
_real_start_delay_until: datetime | None = None
_last_prdy_ctrt: dict[str, float] = {c: 0.0 for c in codes}
_subscribed: dict[str, set[str]] = {}


def calc_mode(now: datetime) -> str:
    t = now.time()
    if t < dt_time(8, 50):
        return RunMode.PREOPEN_WAIT
    if dt_time(8, 50) <= t < dt_time(9, 0):
        return RunMode.PREOPEN_EXP
    if dt_time(9, 0) <= t < dt_time(15, 10, 10):
        return RunMode.REGULAR_REAL
    if dt_time(15, 10, 10) <= t < dt_time(15, 30):
        return RunMode.CLOSE_EXP
    if dt_time(15, 30) <= t < dt_time(16, 0):
        return RunMode.CLOSE_REAL
    if dt_time(16, 0) <= t < END_TIME:
        return RunMode.OVERTIME_EXP if not _overtime_real_active else RunMode.OVERTIME_REAL
    return RunMode.EXIT


def _log_mode_transition(prev: str | None, cur: str) -> None:
    if cur == RunMode.PREOPEN_WAIT:
        _notify(f"{ts_prefix()} 장 개시전까지 대기/")
    elif cur == RunMode.PREOPEN_EXP:
        _notify(f"{ts_prefix()} 08:50 예상체결가 구독 시작")
    elif cur == RunMode.REGULAR_REAL:
        _notify(f"{ts_prefix()} 09:00 예상체결가 구독 중지 -> 실시간 체결가 구독 시작")
    elif cur == RunMode.CLOSE_EXP:
        _notify(f"{ts_prefix()} 15:10:10 실시간 구독 중지 -> 예상체결가 구독 시작")
    elif cur == RunMode.CLOSE_REAL:
        _notify(f"{ts_prefix()} 15:30:00 예상체결가 구독 중지 -> 실시간 체결가 구독 시작")
    elif cur == RunMode.OVERTIME_EXP:
        _notify(f"{ts_prefix()} 16:00:00 시간외 단일가 예상체결가 구독 시작")
    elif cur == RunMode.OVERTIME_REAL:
        _notify(f"{ts_prefix()} 시간외 단일가 체결가 구독 시작")
    elif cur == RunMode.STOP:
        _notify(f"{ts_prefix()} 모든 구독 종료")
    elif cur == RunMode.EXIT:
        _notify(f"{ts_prefix()} 18:00 이후 시간외 단일가 종료로 프로그램 종료")


def _get_mode() -> str:
    with _mode_lock:
        return _current_mode or calc_mode(datetime.now(KST))


def scheduler_loop():
    global _current_mode, _last_rebuild_ts, _last_no_data_warn_ts
    global _overtime_real_active, _real_start_delay_until, _last_overtime_real_slot
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
                    _notify(f"{ts_prefix()} 구독 종료")
                    with _kws_lock:
                        if _active_kws is not None:
                            _request_ws_close(_active_kws)
                    _stop_event.set()
                    break

            if dt_time(9, 0) <= now.time() <= dt_time(9, 0, 1) and _real_start_delay_until is None:
                if any(v >= 10.0 for v in _last_prdy_ctrt.values()):
                    _real_start_delay_until = now.replace(hour=9, minute=2, second=0, microsecond=0)
                    _notify(f"{ts_prefix()} 09:02:00 실시간 체결가 구독 시작(VI 급등 조건)")
            if _real_start_delay_until and now >= _real_start_delay_until:
                _real_start_delay_until = None

            if dt_time(16, 9, 59) <= now.time() < END_TIME:
                total_min = now.hour * 60 + now.minute
                slot_min = ((total_min - 9) // 10) * 10 + 9
                slot = now.replace(hour=slot_min // 60, minute=slot_min % 60, second=59, microsecond=0)
                if now >= slot and _last_overtime_real_slot != slot:
                    _last_overtime_real_slot = slot
                    _overtime_real_active = True
                    _overtime_real_seen.clear()
                    _log_mode_transition(_current_mode, RunMode.OVERTIME_REAL)

            if _overtime_real_active and len(_overtime_real_seen) >= len(codes):
                _overtime_real_active = False
                _overtime_real_seen.clear()
                _log_mode_transition(_current_mode, RunMode.OVERTIME_EXP)

            with _kws_lock:
                if _active_kws is not None:
                    _apply_subscriptions(_active_kws, _desired_reqs(now))

            now_ts = time.time()
            if _last_any_recv_ts > 0:
                idle_sec = now_ts - _last_any_recv_ts
                if idle_sec >= NO_DATA_WARN_SEC and (now_ts - _last_no_data_warn_ts) >= NO_DATA_WARN_SEC:
                    _notify(
                        f"{ts_prefix()} {NO_DATA_WARN_SEC}초 이상 데이터 미수신(핑퐁만 수신 가능) -> 재구독 대기"
                    )
                    _last_no_data_warn_ts = now_ts
                if idle_sec >= NO_DATA_REBUILD_SEC and (now_ts - _last_rebuild_ts) >= NO_DATA_REBUILD_SEC:
                    logger.warning(
                        f"{ts_prefix()} [ws] no data for {int(idle_sec)}s -> resubscribe"
                    )
                    with _kws_lock:
                        if _active_kws is not None:
                            _apply_subscriptions(_active_kws, _desired_reqs(now), force=True)
                    _last_rebuild_ts = now_ts
        time.sleep(0.5)
    logger.info(f"{ts_prefix()} [scheduler] stopped")


def _in_range(now_t: dt_time, start: dt_time, end: dt_time) -> bool:
    return (now_t >= start) and (now_t < end)


def update_time_flags():
    global SAVE_REAL_REGULAR, SAVE_EXP_REGULAR, SAVE_REAL_OVERTIME, SAVE_EXP_OVERTIME
    now = datetime.now(KST)
    nt = now.time()
    if SAVE_MODE == "always":
        save_real_regular = True
        save_exp_regular = True
        save_real_overtime = True
        save_exp_overtime = True
    else:
        save_real_regular = _in_range(nt, dt_time(9, 0), dt_time(15, 10, 10)) or _in_range(nt, dt_time(15, 30), dt_time(16, 0))
        save_exp_regular = _in_range(nt, dt_time(8, 50), dt_time(9, 0)) or _in_range(nt, dt_time(15, 10, 10), dt_time(15, 30))
        save_overtime = _in_range(nt, dt_time(16, 0), dt_time(18, 0))
        save_real_overtime = save_overtime
        save_exp_overtime = save_overtime

    with _flags_lock:
        SAVE_REAL_REGULAR = save_real_regular
        SAVE_EXP_REGULAR = save_exp_regular
        SAVE_REAL_OVERTIME = save_real_overtime
        SAVE_EXP_OVERTIME = save_exp_overtime


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
# 모니터링/저장
# =============================================================================
_per_sec_counts = {c: 0 for c in codes}
_total_counts = {c: 0 for c in codes}
_since_save_rows = 0
_last_print_ts = time.time()
_written_rows = 0
_last_status_line = ""
_last_flush_time = time.time()
_last_any_recv_ts = 0.0
_last_rebuild_ts = 0.0
_last_no_data_warn_ts = 0.0

_lock = threading.RLock()
_stop_event = threading.Event()
_ingest_queue: "queue.Queue[tuple[pd.DataFrame, str, str, str | None, str]]" = queue.Queue()

_buffers: dict[str, list[pd.DataFrame]] = {c: [] for c in codes}
_buf_rows: dict[str, int] = {c: 0 for c in codes}
_last_flush_ts: dict[str, float] = {c: time.time() for c in codes}

_last_recv_ts: dict[str, float] = {c: 0.0 for c in codes}
_last_summary_ts = time.time()

# ── 장중 미수신 감시 상태 ──
_last_stale_check_ts: dict[str, float] = {}  # 종목별 마지막 REST 상태확인 시각
_halted_codes: set[str] = set()               # 거래정지 확인된 종목
_vi_active_codes: set[str] = set()            # VI 발동 확인된 종목
_vi_exp_sub_ts: dict[str, float] = {}         # VI 종목 예상체결가 구독 시작 시각

_overwrite_events: list[dict[str, str]] = []
_part_seq = 0

_top_client: KisClient | None = None
_price_client: KisClient | None = None
_top_client_2: KisClient | None = None      # 계정2 (한도 초과 시 fallback)
_price_client_2: KisClient | None = None     # 계정2 (한도 초과 시 fallback)

# REST 요청 중앙 큐
_rest_req_count = 0
_rest_req_second = int(time.time())
_rest_last_sent_ts = 0.0
_rest_queue: "queue.Queue[tuple[callable, tuple, dict, float, threading.Event, dict]]" = queue.Queue()

# REST 현재가 보충 (1초 미수신 종목 보강)
_price_queue: "queue.Queue[str]" = queue.Queue()
_last_rest_req_ts: dict[str, float] = {c: 0.0 for c in codes}
_rest_pending: set[str] = set()
_price_req_count = 0
_price_req_second = int(time.time())

# 거래 장부 (메모리 → 주기적 CSV 저장)
_trade_ledger_rows: list[dict] = []
_last_ledger_flush = time.time()
SUMMARY_DIR = SCRIPT_DIR
SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
SUMMARY_PATH = SUMMARY_DIR / ("TR_tot_result_data_" + ("wss.csv" if DATA_MODE == "wss" else "data.csv"))


def _init_top_client() -> KisClient:
    cfg = resolve_account_config(str(CONFIG_PATH), account_id=ACCOUNT_ID) if ACCOUNT_ID else load_config(str(CONFIG_PATH))
    appkey = cfg.get("appkey")
    appsecret = cfg.get("appsecret")
    if not appkey or not appsecret:
        raise ValueError("appkey/appsecret이 필요합니다. config.json에 설정하세요.")
    base_url = cfg.get("base_url") or DEFAULT_BASE_URL
    custtype = cfg.get("custtype") or "P"
    market_div = cfg.get("market_div") or TOP_RANK_MARKET_DIV
    kis_cfg = KisConfig(
        appkey=appkey,
        appsecret=appsecret,
        base_url=base_url,
        custtype=custtype,
        market_div=market_div,
    )
    return KisClient(kis_cfg)


def _init_price_client() -> KisClient:
    cfg = resolve_account_config(str(CONFIG_PATH), account_id=ACCOUNT_ID) if ACCOUNT_ID else load_config(str(CONFIG_PATH))
    appkey = cfg.get("appkey")
    appsecret = cfg.get("appsecret")
    if not appkey or not appsecret:
        raise ValueError("appkey/appsecret이 필요합니다. config.json에 설정하세요.")
    base_url = cfg.get("base_url") or DEFAULT_BASE_URL
    custtype = cfg.get("custtype") or "P"
    market_div = cfg.get("market_div") or "J"
    kis_cfg = KisConfig(
        appkey=appkey,
        appsecret=appsecret,
        base_url=base_url,
        custtype=custtype,
        market_div=market_div,
    )
    return KisClient(kis_cfg)


# ── 계정2 (syw_2) 클라이언트 초기화 ─────────────────────────────
def _load_account2_cfg() -> dict:
    """config.json → accounts.syw_2 키를 읽어 공통 설정과 병합하여 반환."""
    cfg = load_config(str(CONFIG_PATH))
    acct = cfg.get("accounts", {}).get("syw_2", {})
    if not acct.get("appkey") or not acct.get("appsecret"):
        return {}
    return {
        "appkey": acct["appkey"],
        "appsecret": acct["appsecret"],
        "base_url": cfg.get("base_url") or DEFAULT_BASE_URL,
        "custtype": cfg.get("custtype") or "P",
        "market_div": cfg.get("market_div") or "J",
    }


def _init_top_client_2() -> KisClient | None:
    a2 = _load_account2_cfg()
    if not a2:
        return None
    return KisClient(KisConfig(
        appkey=a2["appkey"], appsecret=a2["appsecret"],
        base_url=a2["base_url"], custtype=a2["custtype"],
        market_div=a2.get("market_div") or TOP_RANK_MARKET_DIV,
        token_cache_path=str(SCRIPT_DIR / "kis_token_syw2.json"),
    ))


def _init_price_client_2() -> KisClient | None:
    a2 = _load_account2_cfg()
    if not a2:
        return None
    return KisClient(KisConfig(
        appkey=a2["appkey"], appsecret=a2["appsecret"],
        base_url=a2["base_url"], custtype=a2["custtype"],
        market_div=a2["market_div"],
        token_cache_path=str(SCRIPT_DIR / "kis_token_syw2.json"),
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


def _enqueue_rest_price_row(code: str, output: dict) -> None:
    """REST 현재가 조회 결과를 ingest 큐에 주입 (WSS 공백 보강)."""
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


# REST 보충 정책
_REST_OPEN_START    = dt_time(9, 0)   # 개장 직후 적극 보충 시작
_REST_OPEN_END      = dt_time(9, 10)  # 개장 직후 적극 보충 종료 (최대)
_REST_OPEN_INTERVAL = 1.0             # 개장 직후: 종목당 1초 간격
_REST_VI_UNTIL      = dt_time(9, 2)   # VI 발동 종목 REST 보충 제외 시한
_REST_GRACE_SEC     = 30              # 시작/재시작 후 WSS warmup 대기 시간(초)

def _vi_exp_sub_add(code: str) -> None:
    """VI 발동 종목에 예상체결가 구독 추가."""
    if code in _vi_exp_sub_ts:
        return
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
    """REST 상태확인 응답을 파싱하여 거래중단/VI/정상 판단 후 조치."""
    name = code_name_map.get(code, code)
    trht_yn = str(output.get("trht_yn", "N")).strip().upper()
    vi_stts = str(output.get("vi_stts", "")).strip().upper()
    mkop_cls = str(output.get("new_mkop_cls_code", "")).strip()
    is_vi = (vi_stts == "Y" or mkop_cls == "02")
    pr = output.get("stck_prpr", "?")

    if trht_yn == "Y":
        if code not in _halted_codes:
            _halted_codes.add(code)
            sys.stdout.write("\n")
            _notify(f"{ts_prefix()} [stale] ★ 거래정지 확인: {name}({code}) → 구독 해제")
            removed = _remove_code_structs([code], force=True)
            if removed:
                _ws_rebuild_event.set()
        return

    if is_vi:
        if code not in _vi_active_codes:
            sys.stdout.write("\n")
            _notify(f"{ts_prefix()} [stale] VI 발동 확인: {name}({code}) pr={pr} → 예상체결가 구독")
        _vi_exp_sub_add(code)
        _enqueue_rest_price_row(code, output)
        return

    if code in _vi_active_codes:
        sys.stdout.write("\n")
        _notify(f"{ts_prefix()} [stale] VI 해제 확인: {name}({code}) pr={pr} → 예상체결가 구독 해제")
        _vi_exp_sub_unsub(code)

    _halted_codes.discard(code)
    _enqueue_rest_price_row(code, output)
    logger.info(f"{ts_prefix()} [stale] 정상 거래 확인: {name}({code}) pr={pr} → REST 보충")


def _is_vi_suspect(code: str, now_t) -> bool:
    """09:00 직전 prdy_ctrt >= 10% 이면 VI 발동 가능 → 09:02까지 REST 제외."""
    if now_t >= _REST_VI_UNTIL:
        return False
    return abs(_last_prdy_ctrt.get(code, 0.0)) >= 10.0

def _price_watchdog_loop() -> None:
    """REST 현재가 보충 감시: 개장 직후 적극 보충 + 이후 WSS 의존."""
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
                all_received = all(
                    _last_recv_ts.get(c, 0.0) > 0 for c in codes
                )
            if all_received:
                pass  # 모든 종목 open 가격 수신 완료 → (B)로 진행
            else:
                with _lock:
                    for c in list(codes):
                        if _is_vi_suspect(c, now_t):
                            continue  # VI 발동 의심 → 09:02까지 제외
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
        # ================================================================
        if (now - start_ts) < _REST_GRACE_SEC:
            time.sleep(0.5)
            continue

        # ── 정규장 시간 외에는 REST 보충/감시 안 함 ──
        # 시간외(16:00~18:00)는 대부분 거래 없어 WSS 미수신이 정상
        if not (dt_time(9, 0) <= now_t <= dt_time(15, 30)):
            time.sleep(1.0)
            continue

        if wss_alive:
            with _lock:
                _nm = _code_name_map()
                for c in list(codes):
                    last = _last_recv_ts.get(c, 0.0)
                    if last > 0:
                        continue
                    # ── WSS 한 번도 미수신 + 3분(180초) 경과 → 구독 해제 ──
                    if (now - start_ts) >= 180.0:
                        # Trading_test: 보유 중이면 해제하지 않음
                        holding = False
                        if _last_states:
                            st = _last_states.get(c)
                            if st and getattr(st, "qty", 0) > 0:
                                holding = True
                        if not holding:
                            name = _nm.get(c, c)
                            sys.stdout.write("\n")
                            _notify(
                                f"{ts_prefix()} [watchdog] WSS 미수신 3분 → 구독 해제: {name}({c})"
                            )
                            removed = _remove_code_structs([c], force=True)
                            if removed:
                                _ws_rebuild_event.set()
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
        #     - STALE_CHECK_SEC(60초) 이상 미수신 → REST 상태확인 큐에 등록
        #     - 거래정지/VI/비활성 종목은 재확인 간격 완화
        #     - VI 예상체결가 구독 자동 해제 (120초 경과)
        # ================================================================
        if dt_time(9, 10) <= now_t <= dt_time(15, 30):
            with _lock:
                for c in list(codes):
                    if c in _rest_pending:
                        continue
                    last = _last_recv_ts.get(c, 0.0)
                    idle = (now - last) if last > 0 else (now - start_ts)
                    if c in _halted_codes:
                        check_interval = STALE_CHECK_HALTED_SEC
                    elif c in _vi_active_codes:
                        check_interval = STALE_CHECK_VI_SEC
                    else:
                        check_interval = STALE_CHECK_SEC
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
    """REST 현재가 요청 큐에서 종목을 꺼내 API 호출 후 ingest 큐에 주입."""
    global _price_client, _price_client_2, _price_req_count, _price_req_second
    try:
        _price_client = _init_price_client()
    except Exception as e:
        logger.warning(f"{ts_prefix()} [price] init failed: {e}")
        return
    # 계정2 초기화 (실패해도 기본 계정으로 계속)
    try:
        _price_client_2 = _init_price_client_2()
        if _price_client_2:
            logger.info(f"{ts_prefix()} [price] 계정2 초기화 완료 (fallback 준비)")
    except Exception:
        _price_client_2 = None
    logger.info(f"{ts_prefix()} [price] request loop started")
    _fail_counts: dict[str, int] = {}
    _REST_FAIL_MAX = 3
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
            _fail_counts.pop(code, None)
            if is_stale_check:
                _handle_stale_check_result(code, output)
            else:
                _enqueue_rest_price_row(code, output)
                name = code_name_map.get(code, code)
                pr = output.get("stck_prpr", "?")
                logger.info(f"{ts_prefix()} [price] REST보충 {name}({code}) pr={pr}")
        except Exception as e:
            # ── 한도 초과 시 계정2로 재시도 ──
            if _is_rate_limit_error(e) and _price_client_2:
                try:
                    output = _rest_submit(_fetch_current_price, _price_client_2, code)
                    _price_req_count += 1
                    _fail_counts.pop(code, None)
                    if is_stale_check:
                        _handle_stale_check_result(code, output)
                    else:
                        _enqueue_rest_price_row(code, output)
                        name = code_name_map.get(code, code)
                        pr = output.get("stck_prpr", "?")
                        logger.info(f"{ts_prefix()} [price] REST보충(계정2) {name}({code}) pr={pr}")
                except Exception as e2:
                    logger.warning(f"{ts_prefix()} [price] fetch failed(계정2) code={code}: {e2}")
                    _fail_counts[code] = _fail_counts.get(code, 0) + 1
            else:
                fc = _fail_counts.get(code, 0) + 1
                _fail_counts[code] = fc
                name = code_name_map.get(code, code)
                logger.warning(
                    f"{ts_prefix()} [price] fetch failed {name}({code}) ({fc}/{_REST_FAIL_MAX}): {e}"
                )
                # ── 3회 연속 실패 + 보유 중 아님 → 구독 해제 ──
                if fc >= _REST_FAIL_MAX:
                    # 보유 중(qty>0)이면 해제하지 않음
                    holding = False
                    if _last_states:
                        st = _last_states.get(code)
                        if st and getattr(st, "qty", 0) > 0:
                            holding = True
                    if holding:
                        logger.info(
                            f"{ts_prefix()} [price] {name}({code}) 보유 중 → 구독 유지 (실패 카운트 리셋)"
                        )
                        _fail_counts.pop(code, None)
                    else:
                        sys.stdout.write("\n")
                        _notify(
                            f"{ts_prefix()} [price] ★ REST {_REST_FAIL_MAX}회 연속 실패 → 구독 해제: "
                            f"{name}({code})"
                        )
                        removed = _remove_code_structs([code], force=True)
                        if removed:
                            _ws_rebuild_event.set()
                        _fail_counts.pop(code, None)
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
            _buffers[c] = []
            _buf_rows[c] = 0
            _last_flush_ts[c] = time.time()
            _last_recv_ts[c] = 0.0
            _last_rest_req_ts[c] = 0.0
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
        return True
    return _krx_group_cache.get(code, "") == "ST"


# 탑 랭킹 구독 추가/해제 이력 관리
_top_added_codes: set[str] = set(_loaded_top_added)  # config에서 복원 또는 빈 set
if _top_added_codes:
    logger.info(f"[codes] top_added 복원: {sorted(_top_added_codes)}")
_top_sub_log_path = TOP_RANK_OUT_DIR / f"wss_sub_add_top10_list_{today_yymmdd}.csv"
_closing_codes: list[str] = []       # 15:09 선정된 종가매매 종목 (ST 10개)


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


def _switch_to_closing_codes() -> None:
    """15:10 종가매매 전환: 기존 종목 전부 해제 → _closing_codes 만 남긴다."""
    global _base_codes
    if not _closing_codes:
        logger.warning(f"{ts_prefix()} [top] _closing_codes 비어 있음, 전환 불가")
        return

    # 기존 종목 버퍼 flush (lock 밖에서 — _flush_code 내부에서 _lock 획득)
    for c in list(codes):
        try:
            _flush_code(c, force=True)
        except Exception:
            pass

    with _lock:
        # 기존 종목 전부 제거
        old_codes = list(codes)
        old_names = [code_name_map.get(c, c) for c in old_codes]
        codes.clear()
        _top_added_codes.clear()

        # 종가매매 종목으로 교체
        for c in _closing_codes:
            codes.append(c)
            _per_sec_counts[c] = _per_sec_counts.get(c, 0)
            _total_counts[c] = _total_counts.get(c, 0)
            if c not in _buffers:
                _buffers[c] = []
            if c not in _buf_rows:
                _buf_rows[c] = 0
            _last_flush_ts[c] = time.time()
            _last_recv_ts[c] = _last_recv_ts.get(c, 0.0)
            _last_rest_req_ts[c] = _last_rest_req_ts.get(c, 0.0)

        # base_codes 갱신 (종가매매 종목이 새 기본)
        _base_codes = set(codes)
        _persist_subscription_codes(codes)

    # 이름 갱신
    try:
        code_name_map.update(_code_name_map())
    except Exception:
        pass

    new_names = [code_name_map.get(c, c) for c in _closing_codes]
    msg = (
        f"{ts_prefix()} [top] ★★ 종가매매 구독 전환 완료\n"
        f"  해제: {len(old_codes)}개 {old_names}\n"
        f"  신규: {len(_closing_codes)}개 {_closing_codes} / {new_names}"
    )
    sys.stdout.write("\n")  # 제자리 출력 줄바꿈
    _notify(msg)

    # WSS 재구독 트리거
    _ws_rebuild_event.set()


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
            _per_sec_counts.pop(c, None)
            _total_counts.pop(c, None)
            _buffers.pop(c, None)
            _buf_rows.pop(c, None)
            _last_flush_ts.pop(c, None)
            _last_recv_ts.pop(c, None)
            _last_rest_req_ts.pop(c, None)
            _last_prdy_ctrt.pop(c, None)
        if removed:
            _persist_subscription_codes(codes)
    return removed


def _top_rank_loop():
    global _top_client, _top_client_2
    if not TOP_RANK_ENABLED or DATA_MODE != "wss":
        return
    _load_krx_group_cache()
    try:
        _top_client = _init_top_client()
    except Exception as e:
        logger.warning(f"{ts_prefix()} [top] init failed: {e}")
        return
    # 계정2 초기화 (실패해도 기본 계정으로 계속)
    try:
        _top_client_2 = _init_top_client_2()
        if _top_client_2:
            logger.info(f"{ts_prefix()} [top] 계정2 초기화 완료 (fallback 준비)")
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
        is_closing_select = (next_ts.time() >= dt_time(15, 9))    # 15:09 종가매매 선정
        is_closing_switch = (next_ts.time() >= dt_time(15, 10))   # 15:10 구독 전환
        try:
            # ── 메인 클라이언트 → 실패 시 계정2로 재시도 ──
            try:
                rows = _rest_submit(_fetch_fluctuation_top, _top_client, top_n=TOP_RANK_N)
            except Exception as e1:
                logger.warning(f"{ts_prefix()} [top] 메인 계정 실패: {type(e1).__name__}: {e1}")
                if _top_client_2:
                    rows = _rest_submit(_fetch_fluctuation_top, _top_client_2, top_n=TOP_RANK_N)
                    logger.info(f"{ts_prefix()} [top] 계정2로 재시도 성공 ({len(rows)}건)")
                else:
                    raise  # 계정2 없으면 원래 에러 전파
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
            # 15:09 종가매매 종목 선정
            # ============================================================
            if is_closing_select and not _closing_codes:
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
                _notify(msg_cl)

            # ============================================================
            # 15:10 구독 전환: 기존 전부 해제 → 종가매매 종목으로 교체
            # ============================================================
            if is_closing_switch and _closing_codes:
                _switch_to_closing_codes()
                logger.info(f"{ts_prefix()} [top] end time reached (종가매매 전환 완료), stopping")
                return

            # ============================================================
            # 일반 시간대 처리 (15:09/15:10 아닌 경우)
            # ============================================================
            if not is_closing_select:
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
                        _notify(msg_rm)
                        _persist_subscription_codes(codes)
                        _ws_rebuild_event.set()

                # --- 2) 상위 종목 중 ST 신규 → 구독 추가 (상한 관리) ---
                candidates = []
                for code in current_top_codes:
                    if len(candidates) >= TOP_RANK_ADD_N:
                        break
                    if code in _base_codes:
                        continue
                    if code in codes:
                        continue
                    if not _is_stock_group(code):
                        continue
                    candidates.append(code)

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
                msg2 = (
                    f"{ts_prefix()} [top] 구독 추가: {added} / {added_names} (총 {len(codes)}/{MAX_WSS_SUBSCRIBE})"
                    if added
                    else f"{ts_prefix()} [top] 구독 추가 없음 (총 {len(codes)}/{MAX_WSS_SUBSCRIBE})"
                )
                _notify(msg2)
                if added:
                    _persist_subscription_codes(codes)
                    _ws_rebuild_event.set()
        except Exception as e:
            logger.warning(f"{ts_prefix()} [top] fetch failed (schedule #{idx}): {type(e).__name__}: {e}")
            # ── 15:10 구독 전환은 fetch 실패해도 반드시 실행 ──
            if is_closing_switch:
                if _closing_codes:
                    _switch_to_closing_codes()
                    logger.info(f"{ts_prefix()} [top] fetch 실패했으나 종가매매 전환 실행 완료")
                else:
                    with _lock:
                        to_remove = [c for c in list(codes) if c not in _base_codes]
                    if to_remove:
                        removed = _remove_code_structs(to_remove)
                        if removed:
                            _ws_rebuild_event.set()
                    logger.warning(
                        f"{ts_prefix()} [top] 종가매매 선정 실패 → top_added 해제, "
                        f"base({len(_base_codes)}개)만 유지"
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
        dt_time(9, 0),
        dt_time(9, 5),
        dt_time(9, 10),
        dt_time(9, 15),
        dt_time(9, 30),
    ]
    times = [datetime.combine(base, t, tzinfo=KST) for t in fixed]
    cur = datetime.combine(base, dt_time(10, 0), tzinfo=KST)
    end = datetime.combine(base, dt_time(15, 0), tzinfo=KST)
    while cur <= end:
        times.append(cur)
        cur += timedelta(minutes=30)
    # 종가매매 종목 선정 (15:09) + 구독 전환 (15:10)
    times.append(datetime.combine(base, dt_time(15, 9), tzinfo=KST))
    times.append(datetime.combine(base, dt_time(15, 10), tzinfo=KST))
    return sorted(set(times))
_last_states: dict[str, "TradeState"] | None = None

META_COLS = ["recv_ts", "tr_id", "is_real_ccnl"]
_union_cols_lock = threading.RLock()
UNION_COLS: list[str] = []
UNION_SCHEMA: pa.Schema | None = None

TRID_TO_IS_REAL: dict[str, str] = {}
TRID_TO_KIND: dict[str, str] = {}

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


def on_result(ws, tr_id, result, data_info):
    if result is None or getattr(result, "empty", True):
        return
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


@dataclass
class TradeState:
    code: str
    name: str
    pdy_close: float
    pdy_ctrt: float
    r_avr: float
    rk: float
    cash: float
    alloc_cash: float
    qty: int = 0
    buy_pr: float = 0.0
    trail_step: float = 0.0
    trail_stop: float = 0.0
    peak_price: float = 0.0
    trail_below_count: int = 0        # trail_stop 이하 연속 틱 수
    below_buy_count: int = 0          # 매수가 이탈 연속 틱 수
    break_above_count: int = 0        # break_pr 이상 연속 틱 수 (매수 확인용)
    min_low: float | None = None
    loss_streak: int = 0
    stop_trading: bool = False
    last_real: float | None = None
    last_exp: float | None = None
    last_real_ts: dt_time | None = None
    last_exp_ts: dt_time | None = None
    open_price: float | None = None
    open_confirmed: bool = False
    total_buy: float = 0.0
    total_sell: float = 0.0
    rk1: float = 0.0
    rk2: float = 0.0
    dy_low: float | None = None
    new_low: float | None = None
    dy_high: float | None = None
    break_pr1: float = 0.0
    break_pr2: float = 0.0
    break_pr: float = 0.0
    preopen_active: bool = False
    preopen_order_pr: float = 0.0
    preopen_last_exp_ts: dt_time | None = None
    preopen_extend_until: dt_time | None = None
    preopen_failed: bool = False
    preopen_filled: bool = False
    initial_buy_done: bool = False
    no_high_stop_done: bool = False
    post_open_wait_start: int | None = None
    last_rest_poll_sec: int | None = None
    trade_count: int = 0
    stop_reason: str | None = None
    stop_time: str | None = None
    cum_ret: float = 1.0
    last_break_pr: float | None = None
    last_new_low: float | None = None
    last_minute_key: int | None = None
    last_sell_ts: dt_time | None = None     # 마지막 매도 시각 (쿨다운용)
    buy_ts: dt_time | None = None           # 매수 시각 (최소 보유시간 체크용)
    cum_price_vol: float = 0.0              # VWAP 계산: 가격×거래량 누적
    cum_vol: float = 0.0                    # VWAP 계산: 거래량 누적
    vwap: float = 0.0                       # VWAP 값
    last_acml_vol: float = 0.0
    vol_1m: float = 0.0
    vol_hist: list[float] = field(default_factory=list)


def _format_amount(v: float) -> str:
    return f"{v:,.0f}"


def _ret_arrow(v: float) -> str:
    if v > 0:
        return "△"
    if v < 0:
        return "▼"
    return "-"


def _reason_label(reason: str) -> str:
    mapping = {
        "nested_breakout": "r_breakout",
        "preopen_fill": "r_preopen",
        "close_buy": "r_close",
        "pdy_close_hold": "r_pdy_close",
        "trail_stop": "r_trail",
        "below_buy": "r_below",
    }
    return mapping.get(reason, reason)


def _emit_stop_summary(state: TradeState, reason: str) -> None:
    if state.stop_reason:
        return
    state.stop_reason = reason
    if state.stop_time is None:
        state.stop_time = _current_log_dt().strftime("%H:%M:%S")
    tot_ret = state.cum_ret - 1
    msg = (
        f"(TM) {state.name} {reason} tr_{state.trade_count:02d}회, "
        f"tot_ret_{tot_ret:.4f} " + ("=" * 20)
    )
    _log_trade(f"{ts_prefix()} {msg}", {state.code: state})


def _append_trade_ledger(
    state: TradeState,
    action: str,
    price: float,
    break_pr: float,
    break_pr1: float,
    break_pr2: float,
    trail_r: float,
    trail_v: float,
    ret: float | None,
    now_t: dt_time,
) -> None:
    tot_ret = state.cum_ret - 1
    # 시각: _current_log_dt()에서 밀리초 포함
    log_dt = _current_log_dt()
    time_str = log_dt.strftime("%H:%M:%S.%f")[:-3]  # HH:MM:SS.mmm
    _trade_ledger_rows.append(
        {
            "date": _current_log_dt().strftime("%Y-%m-%d"),
            "time": time_str,
            "code": state.code,
            "name": state.name,
            "stck_prpr": f"{price:.0f}",
            "break_pr": f"{break_pr:.0f}" if break_pr > 0 else "",
            "buy_pr": f"{state.buy_pr:.0f}" if action == "buy" else "",
            "sell_pr": f"{price:.0f}" if action == "sell" else "",
            "qty": state.qty,
            "eval_amt": f"{state.qty * price:.0f}" if state.qty > 0 else "",
            "cash": f"{state.cash:.0f}",
            "trail_r": f"{trail_r:.4f}" if trail_r > 0 else "",
            "trail_v": f"{trail_v:.4f}" if trail_v > 0 else "",
            "ret": f"{ret:.4f}" if ret is not None else "",
            "tot_ret": f"{tot_ret:.4f}",
            "action": action,
            "R_avr": f"{state.r_avr:.4f}",
            "Rk": f"{state.rk1:.4f}",
            "dy_low": f"{state.dy_low:.0f}" if state.dy_low else "",
            "new_low": f"{state.new_low:.0f}" if state.new_low else "",
            "Rk2": f"{state.rk2:.4f}",
            "break_pr1": f"{break_pr1:.0f}" if break_pr1 > 0 else "",
            "break_pr2": f"{break_pr2:.0f}" if break_pr2 > 0 else "",
            "stop_time": "",
        }
    )


def _build_summary(states: dict[str, TradeState]) -> pd.DataFrame:
    base_dt = _current_log_dt().date()
    rows: list[dict] = []
    for st in states.values():
        open_pr = float(st.open_price or 0)
        high_pr = float(st.dy_high or 0)
        low_pr = float(st.dy_low or 0)
        close_pr = float(st.last_real or st.last_exp or 0)
        tdy_oc_rt = (close_pr / open_pr - 1) if open_pr > 0 else 0.0
        tdy_ctrt = (close_pr / st.pdy_close - 1) if st.pdy_close > 0 else 0.0
        tot_ret = st.cum_ret - 1
        stop_time = st.stop_time or (
            st.last_real_ts.strftime("%H:%M:%S") if st.last_real_ts else (
                st.last_exp_ts.strftime("%H:%M:%S") if st.last_exp_ts else ""
            )
        )
        rows.append(
            {
                "date": str(base_dt),
                "code": st.code,
                "name": st.name,
                "pdy_close": f"{st.pdy_close:.0f}",
                "open": f"{open_pr:.0f}" if open_pr > 0 else "",
                "high": f"{high_pr:.0f}" if high_pr > 0 else "",
                "low": f"{low_pr:.0f}" if low_pr > 0 else "",
                "close": f"{close_pr:.0f}" if close_pr > 0 else "",
                "tdy_oc_rt": f"{tdy_oc_rt:.6f}",
                "prdy_ctrt": f"{st.pdy_ctrt:.6f}",
                "tdy_ctrt": f"{tdy_ctrt:.6f}",
                "buy_cnt": st.trade_count + (1 if st.qty > 0 else 0),
                "sell_cnt": st.trade_count,
                "tot_ret": f"{tot_ret:.6f}",
                "stop_time": stop_time,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        avg = {
            "date": "average",
            "code": "",
            "name": "",
            "pdy_close": "",
            "open": "",
            "high": "",
            "low": "",
            "close": "",
            "tdy_oc_rt": f"{df['tdy_oc_rt'].astype(float).mean():.6f}",
            "prdy_ctrt": f"{df['prdy_ctrt'].astype(float).mean():.6f}",
            "tdy_ctrt": f"{df['tdy_ctrt'].astype(float).mean():.6f}",
            "buy_cnt": "",
            "sell_cnt": "",
            "tot_ret": f"{df['tot_ret'].astype(float).mean():.6f}",
            "stop_time": "",
        }
        df = pd.concat([df, pd.DataFrame([avg])], ignore_index=True)
    return df


def _write_summary(states: dict[str, TradeState]) -> bool:
    """종합성과 요약을 출력/저장. 이미 작성됐으면 False 반환."""
    global _summary_written
    if _summary_written:
        return False
    df = _build_summary(states)
    if df.empty:
        logger.info("[summary] empty")
        _summary_written = True
        return False
    rows = df.to_dict("records")
    # 화면 출력용: code 제외, tdy_oc_rt/prdy_ctrt/tdy_ctrt/tot_ret 소수 3자리
    _DISPLAY_DEC3 = ("tdy_oc_rt", "prdy_ctrt", "tdy_ctrt", "tot_ret")
    display_rows = []
    for r in rows:
        d = {k: v for k, v in r.items() if k != "code"}
        for col in _DISPLAY_DEC3:
            if col in d and d[col] != "":
                try:
                    d[col] = f"{float(d[col]):.3f}"
                except (ValueError, TypeError):
                    pass
        display_rows.append(d)
    display_cols = [c for c in df.columns if c != "code"]
    align = {col: "right" for col in display_cols}
    for col in ("date", "name", "stop_time"):
        if col in align:
            align[col] = "left"
    print_table(display_rows, display_cols, align)
    # CSV는 원래 포맷 유지 (code 포함, 소수 6자리)
    header = not SUMMARY_PATH.exists()
    df.to_csv(SUMMARY_PATH, mode="a", index=False, header=header, encoding="utf-8-sig")
    _summary_written = True
    return True


_ledger_first_flush = True  # data 모드 첫 flush 시 기존 파일 덮어쓰기용

def _flush_trade_ledger(force: bool = False) -> None:
    global _last_ledger_flush, _ledger_first_flush
    if not _trade_ledger_rows:
        return
    now = time.time()
    if not force and (now - _last_ledger_flush) < 300:
        return
    df = pd.DataFrame(_trade_ledger_rows)
    # data 모드: 첫 flush 시 기존 파일 덮어쓰기 (중복 방지)
    if _IS_DATA_MODE and _ledger_first_flush:
        df.to_csv(TRADE_LEDGER_PATH, mode="w", index=False, header=True, encoding="utf-8-sig")
        _ledger_first_flush = False
    else:
        header = not TRADE_LEDGER_PATH.exists()
        df.to_csv(TRADE_LEDGER_PATH, mode="a", index=False, header=header, encoding="utf-8-sig")
    _trade_ledger_rows.clear()
    _last_ledger_flush = now


def _round_up_tick(price: float) -> float:
    if price <= 0:
        return price
    step = _tick_size(price)
    p = int(round(price))
    remainder = p % step
    if remainder == 0:
        return float(p)
    return float(p + step - remainder)


def _tick_size(price: float) -> int:
    if price < 1000:
        return 1
    if price < 5000:
        return 5
    if price < 10000:
        return 10
    if price < 50000:
        return 50
    if price < 100000:
        return 100
    if price < 500000:
        return 500
    return 1000


def _time_to_sec(t: dt_time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second


def _get_total_assets(states: dict[str, TradeState]) -> float:
    total = 0.0
    for st in states.values():
        price = st.last_real or st.last_exp or st.buy_pr or st.open_price or 0.0
        total += st.cash + st.qty * price
    return total


def _print_status(states: dict[str, TradeState]) -> None:
    with _lock:
        per_sec_total = sum(_per_sec_counts.values())
        recv_rows = _since_save_rows
        recv_codes = sum(1 for c in codes if _total_counts.get(c, 0) > 0)
    total_assets = _get_total_assets(states)
    ret = (total_assets / INITIAL_CAPITAL - 1) if INITIAL_CAPITAL > 0 else 0.0
    buy_cnt = sum(1 for st in states.values() if st.total_buy > 0)
    sell_cnt = sum(1 for st in states.values() if st.total_sell > 0)
    line = (
        f"{ts_prefix()} ({per_sec_total:03d}/초) "
        f"(누적수신 {recv_rows} rows/ 수신 {recv_codes}종목 / 총 {len(codes)}종목 대상) "
        f"(buy {buy_cnt}건, sell {sell_cnt}건, "
        f"총 자산 {_format_amount(total_assets)}원, 수익율 {ret*100:0.2f}%)"
    )
    width = shutil.get_terminal_size((120, 20)).columns
    if len(line) > width - 1:
        line = line[: width - 1]
    global _last_status_line
    _last_status_line = line
    sys.stdout.write("\r\033[2K" + line)
    sys.stdout.flush()
    # ★ 제자리 출력은 stdout만, 로그 파일에는 기록하지 않음 (nohup 시 tail -f에서도 동일하게 표시)

def _emit_save_done(rows: int) -> None:
    # stdout: 현재 제자리 출력 오른쪽에 붙여서 한 줄로 종료
    sys.stdout.write(f"=> [save] {rows} rows done\n")
    sys.stdout.flush()
    # 로그: save 정보만 기록 (제자리 출력 내용은 제외)
    logger.info(f"{ts_prefix()} [save] {rows} rows done")


def _log_trade(msg: str, states: dict[str, TradeState]) -> None:
    global _data2_action_occurred
    logger.info(msg)
    if DATA_MODE in ("data2", "data3"):
        sys.stdout.write(f"  >>> {msg}\n")
        sys.stdout.flush()
        _data2_action_occurred = True
    if print_option in (1, 2):
        _print_status(states)


def _early_multiplier(now_t: dt_time) -> float:
    """장 초반이면 버퍼 배율 반환, 아니면 1.0."""
    if now_t < EARLY_SESSION_END:
        return EARLY_SESSION_MULTIPLIER
    return 1.0


def _time_diff_sec(t1: dt_time, t2: dt_time) -> float:
    """두 dt_time 사이의 초 차이 (t2 - t1). 같은 날 가정."""
    s1 = t1.hour * 3600 + t1.minute * 60 + t1.second + t1.microsecond / 1e6
    s2 = t2.hour * 3600 + t2.minute * 60 + t2.second + t2.microsecond / 1e6
    return s2 - s1


def _update_trail(state: TradeState, price: float, now_t: dt_time,
                  bidp1: float | None = None) -> None:
    if state.buy_pr <= 0:
        return
    if price > state.peak_price:
        state.peak_price = price
    peak_ret = (state.peak_price / state.buy_pr - 1) if state.buy_pr > 0 else 0.0

    # ── 상한가 근접 판정 (전일종가 대비) ──
    near_limit_up = (
        state.pdy_close > 0
        and (state.peak_price / state.pdy_close - 1) >= LIMIT_UP_THRESHOLD
    )

    # ── 단계 결정 (기존 TRAIL_STEPS 래칫) ──
    new_step = 0.0
    for step in TRAIL_STEPS:
        if peak_ret >= step:
            new_step = step
    if near_limit_up:
        new_step = max(new_step, LIMIT_UP_THRESHOLD)

    if new_step > state.trail_step:
        state.trail_step = new_step

    # ── 트레일 스톱 계산 ──
    if state.trail_step > 0:
        if near_limit_up and bidp1 and bidp1 > 0:
            # 상한가 근접: trail_stop = bid1 (매수1호가) 동적 추적
            # bid1이 올라가면 trail도 따라 올라감 (래칫)
            # price < bid1 이 TRAIL_CONFIRM_TICKS 연속 → 상한가 풀림 감지 → 매도
            new_stop = max(bidp1, state.buy_pr)
        elif near_limit_up:
            # bid1 데이터 없을 때 fallback: 고점 기준 타이트 버퍼
            buffer = LIMIT_UP_TRAIL_BUFFER
            new_stop = state.peak_price * (1 - buffer)
            new_stop = max(new_stop, state.buy_pr)
        else:
            # 일반 구간: 수익률 구간에 따라 버퍼 결정
            buffer = TRAIL_BUFFER_TABLE[0][1]  # 기본값
            for threshold, buf in TRAIL_BUFFER_TABLE:
                if peak_ret >= threshold:
                    buffer = buf
            # 장 초반 배율 적용
            buffer *= _early_multiplier(now_t)
            new_stop = state.peak_price * (1 - buffer)
            new_stop = max(new_stop, state.buy_pr)
        # 트레일 스톱은 올라가기만 함 (내려가지 않음)
        if new_stop > state.trail_stop:
            state.trail_stop = new_stop


def _buy(
    state: TradeState,
    price: float,
    reason: str,
    states: dict[str, TradeState],
    now_t: dt_time,
    break_pr1: float,
    break_pr2: float,
    cur_price: float | None = None,
) -> None:
    if price <= 0 or state.cash <= 0 or state.stop_trading:
        return
    qty = int(state.cash // price)
    if qty <= 0:
        return
    cost = qty * price
    state.cash -= cost
    state.qty = qty
    state.buy_pr = price
    state.peak_price = price
    state.trail_below_count = 0
    state.below_buy_count = 0
    state.break_above_count = 0
    state.buy_ts = now_t  # 최소 보유시간 체크용
    # 매수 직후 초기 트레일 설정: 매수가 - buffer로 즉시 보호
    state.trail_step = 0.01  # 최소 단계 활성화
    init_buffer = TRAIL_BUFFER_TABLE[0][1]  # 0% 수익 구간 버퍼 (기본 3.0%)
    init_buffer *= _early_multiplier(now_t)
    state.trail_stop = price * (1 - init_buffer)
    state.total_buy += cost
    pr = cur_price if cur_price is not None else price
    brk = state.break_pr
    _log_trade(
        f"{ts_prefix()} {state.name} (buy) pr_{pr:.0f} brk_{brk:.0f} buy_{price:.0f} qty_{qty}",
        states,
    )
    _append_trade_ledger(
        state,
        "buy",
        price,
        state.break_pr,
        break_pr1,
        break_pr2,
        state.trail_step,
        state.trail_stop,
        None,
        now_t,
    )


def _sell(
    state: TradeState,
    price: float,
    reason: str,
    states: dict[str, TradeState],
    low_price: float,
    now_t: dt_time,
) -> None:
    """price = 실제 시장가. 슬리피지는 내부에서 1회만 적용."""
    if state.qty <= 0:
        return
    sell_price = price * (1 - SLIPPAGE)
    amount = state.qty * sell_price
    state.cash += amount
    state.total_sell += amount
    ret = (sell_price / state.buy_pr - 1) if state.buy_pr > 0 else 0.0
    state.cum_ret *= (1 + ret)
    tot_ret = state.cum_ret - 1
    trail = state.trail_stop
    # trail_v: peak_ret - buffer (실제 트레일 작동 비율)
    peak_ret = (state.peak_price / state.buy_pr - 1) if state.buy_pr > 0 else 0.0
    near_limit = (
        state.pdy_close > 0
        and state.peak_price > 0
        and (state.peak_price / state.pdy_close - 1) >= LIMIT_UP_THRESHOLD
    )
    if near_limit:
        trail_v_pct = peak_ret  # 상한가: bid1 추적이므로 peak_ret 그대로
    elif state.trail_step > 0:
        buf = TRAIL_BUFFER_TABLE[0][1]
        for threshold, b in TRAIL_BUFFER_TABLE:
            if peak_ret >= threshold:
                buf = b
        buf *= _early_multiplier(now_t)
        trail_v_pct = peak_ret - buf
    else:
        trail_v_pct = 0.0
    _log_trade(
        f"{ts_prefix()} {state.name} (sell) pr_{price:.0f} trail_{trail:.0f} "
        f"sell_{sell_price:.0f} ret_{ret:.4f} tot_ret_{tot_ret:.4f}",
        states,
    )
    _append_trade_ledger(
        state,
        "sell",
        sell_price,
        state.break_pr,
        state.break_pr1,
        state.break_pr2,
        state.trail_step,
        trail_v_pct,
        ret,
        now_t,
    )
    state.qty = 0
    state.buy_pr = 0.0
    state.trail_step = 0.0
    state.trail_stop = 0.0
    state.peak_price = 0.0
    # trail_below_count: 매도 후에도 보존 (마지막 매도 시 몇 틱 연속 이탈이었는지 확인용)
    state.below_buy_count = 0
    state.new_low = price  # 실제 시장가 (슬리피지 미적용)
    state.last_sell_ts = now_t  # 재진입 쿨다운 기준 시각
    state.buy_ts = None
    state.trade_count += 1
    if ret < 0:
        state.loss_streak += 1
    else:
        state.loss_streak = 0
    if max_loss_rt > 0 and state.loss_streak >= max_loss_rt:
        state.stop_trading = True
        _emit_stop_summary(state, f"max_loss={max_loss_rt}")


def _trade_tick(
    state: TradeState,
    price: float,
    low_price: float,
    high_price: float,
    askp1: float | None,
    bidp1: float | None,
    now_t: dt_time,
    states: dict[str, TradeState],
    acml_vol: float | None = None,
) -> None:
    if state.stop_trading:
        return
    # VI 연장 종목: 시가 확인 전까지 중첩돌파 미실행
    if state.preopen_extend_until is not None and not state.open_confirmed:
        return
    if now_t >= CLOSE_AUC_END and state.last_real_ts and state.last_real_ts >= CLOSE_AUC_END:
        state.stop_trading = True
        return
    if not (REAL_TRADE_START <= now_t <= REAL_TRADE_END):
        return
    if state.open_price is None:
        state.open_price = price
    if state.rk1 <= 0:
        state.rk1 = state.r_avr * K
    if state.rk2 <= 0:
        state.rk2 = state.r_avr * K * K2

    tick_low = min(low_price, price)
    tick_high = max(high_price, price)
    if state.dy_low is None:
        state.dy_low = tick_low
    else:
        if tick_low < state.dy_low:
            state.dy_low = tick_low
    if state.dy_high is None:
        state.dy_high = tick_high
    else:
        if tick_high > state.dy_high:
            state.dy_high = tick_high

    # ── VWAP 업데이트 ──
    if VWAP_FILTER_ENABLED:
        if acml_vol is not None and acml_vol > 0:
            vol_delta = acml_vol - state.cum_vol if acml_vol > state.cum_vol else 1.0
            state.cum_price_vol += price * vol_delta
            state.cum_vol = acml_vol
            state.vwap = state.cum_price_vol / state.cum_vol if state.cum_vol > 0 else 0.0
        elif state.vwap <= 0:
            # acml_vol 없을 때: 단순 틱 평균으로 대체
            state.cum_price_vol += price
            state.cum_vol += 1.0
            state.vwap = state.cum_price_vol / state.cum_vol

    if not state.no_high_stop_done and now_t >= NO_HIGH_STOP_TIME:
        state.no_high_stop_done = True
        if state.pdy_close > 0 and (state.dy_high or 0) <= state.pdy_close:
            state.stop_trading = True
            _emit_stop_summary(state, "no_high_0929")
            return

    if state.qty > 0:
        _update_trail(state, price, now_t, bidp1=bidp1)
        # ── 최소 보유시간 미경과 시 매도 판정 건너뛰기 ──
        if MIN_HOLD_SECONDS > 0 and state.buy_ts is not None:
            hold_sec = _time_diff_sec(state.buy_ts, now_t)
            if hold_sec < MIN_HOLD_SECONDS:
                return
        # ── 트레일 스톱 매도 판정 (연속 N틱 확인) ──
        if state.trail_stop > 0 and price < state.trail_stop:
            state.trail_below_count += 1
            if state.trail_below_count >= TRAIL_CONFIRM_TICKS:
                _sell(state, price, "trail_stop", states, low_price, now_t)
                return
        else:
            state.trail_below_count = 0
        # ── 매수가 이탈 매도 판정 (버퍼 + 연속 N틱 확인) ──
        below_buy_line = state.buy_pr * (1 - BELOW_BUY_BUFFER * _early_multiplier(now_t))
        if price < below_buy_line:
            state.below_buy_count += 1
            if state.below_buy_count >= BELOW_BUY_CONFIRM_TICKS:
                _sell(state, price, "below_buy", states, low_price, now_t)
                return
        else:
            state.below_buy_count = 0
        return

    if state.new_low is None:
        state.new_low = price
    else:
        if price < state.new_low:
            state.new_low = price

    if state.dy_low is not None and state.new_low is not None:
        break1 = state.dy_low + state.rk1
        break2 = state.new_low + state.rk2
        state.break_pr1 = break1
        state.break_pr2 = break2
        state.break_pr = _round_up_tick(break1 if break1 >= break2 else break2)
        if state.last_new_low is None or state.last_new_low != state.new_low:
            _log_trade(
                f"{ts_prefix()} {state.name} ( brk_{state.break_pr:.0f}) pr_{price:.0f} "
                f"dy_low_{state.dy_low:.0f} new_low_{state.new_low:.0f} "
                f"rk1_{state.rk1:.2f} rk2_{state.rk2:.2f} "
                f"brk1_{break1:.0f} brk2_{break2:.0f}",
                states,
            )
            state.last_new_low = state.new_low
        if state.last_break_pr is not None and state.break_pr != state.last_break_pr:
            _log_trade(
                f"{ts_prefix()} {state.name} pr_{price:.0f} brk {state.last_break_pr:.0f} -> {state.break_pr:.0f}",
                states,
            )
        state.last_break_pr = state.break_pr

    # ── TRADE_START_DELAY 이전에는 매수 실행 안 함 (break_pr 추적만) ──
    if now_t < TRADE_START_DELAY:
        state.break_above_count = 0
        return
    # ── 종목별 최대 거래 횟수 제한 ──
    if MAX_TRADES_PER_STOCK > 0 and state.trade_count >= MAX_TRADES_PER_STOCK:
        return
    # ── 매도 후 재진입 쿨다운 ──
    if state.last_sell_ts is not None and COOLDOWN_SECONDS > 0:
        elapsed = _time_diff_sec(state.last_sell_ts, now_t)
        if elapsed < COOLDOWN_SECONDS:
            state.break_above_count = 0
            return
    # ── VWAP 추세 필터: price > vwap 일 때만 매수 허용 ──
    if VWAP_FILTER_ENABLED and state.vwap > 0 and price <= state.vwap:
        state.break_above_count = 0
        return

    if state.break_pr > 0 and price >= state.break_pr:
        state.break_above_count += 1
        if state.break_above_count >= BREAK_CONFIRM_TICKS:
            if _spread_ok(askp1, bidp1) and _volume_breakout_ok(state):
                _buy(
                    state,
                    price,
                    "nested_breakout",
                    states,
                    now_t,
                    state.break_pr1,
                    state.break_pr2,
                    cur_price=price,
                )
                state.break_above_count = 0
    else:
        state.break_above_count = 0


def _spread_ok(askp1: float | None, bidp1: float | None) -> bool:
    if askp1 is None or bidp1 is None:
        return True
    if askp1 <= 0 or bidp1 <= 0:
        return True
    return (askp1 / bidp1 - 1.0) <= 0.03


def _preopen_order_price(exp_price: float) -> float:
    tick = _tick_size(exp_price)
    return _round_up_tick(exp_price + tick)


def _handle_preopen_exp_tick(
    state: TradeState,
    exp_price: float,
    now_t: dt_time,
    askp1: float | None,
    bidp1: float | None,
    states: dict[str, TradeState],
) -> None:
    if not PREOPEN_BUY_ENABLED:
        return
    if pdy_close_buy:
        return
    if state.preopen_failed or state.preopen_filled:
        return
    extend_until = state.preopen_extend_until or PREOPEN_END
    if not (PREOPEN_START <= now_t <= extend_until):
        return
    exp_prdy_ctrt = (exp_price / state.pdy_close - 1) if state.pdy_close > 0 else 0.0
    if exp_prdy_ctrt >= 0.10 and state.preopen_extend_until is None and now_t >= PREOPEN_VI_CUTOFF:
        state.preopen_extend_until = PREOPEN_EXTEND_END
        # VI 감지 → 기존 주문 취소, 09:01:58에 재주문
        if state.preopen_order_pr > 0:
            _log_trade(
                f"{ts_prefix()} [pre] VI_연장(~090200) {state.name} 기존주문 취소 "
                f"ord_{state.preopen_order_pr:.0f} → 090158 재주문 대기",
                states,
            )
            state.preopen_order_pr = 0
            state.preopen_active = False
        _log_trade(
            f"{ts_prefix()} [pre] VI_연장(~090200) {state.name} pr_{exp_price:.0f} "
            f"pdy_c_{state.pdy_close:.0f} tdy_ctrt_{exp_prdy_ctrt:.4f}",
            states,
        )
    state.preopen_last_exp_ts = now_t
    # VI 종목: 09:01:58부터 주문, 일반 종목: 08:59:55부터 주문
    order_start = VI_ORDER_START if state.preopen_extend_until is not None else PREOPEN_ORDER_START
    if now_t < order_start:
        return
    if exp_price < state.pdy_close:
        return
    if not _spread_ok(askp1, bidp1):
        return
    order_pr = _preopen_order_price(exp_price)
    if state.preopen_order_pr <= 0:
        state.preopen_order_pr = order_pr
        state.preopen_active = True
        tdy_ctrt = (order_pr / state.pdy_close - 1) if state.pdy_close > 0 else 0.0
        _log_trade(
            f"{ts_prefix()} [pre] order buy_order {state.name} pr_{order_pr:.0f} "
            f"pdy_c_{state.pdy_close:.0f} tdy_ctrt_{tdy_ctrt:.4f}",
            states,
        )
        return
    if now_t <= extend_until and order_pr > state.preopen_order_pr:
        _log_trade(
            f"{ts_prefix()} [pre] replace buy_order {state.name} {state.preopen_order_pr:.0f} ▷ {order_pr:.0f}",
            states,
        )
        state.preopen_order_pr = order_pr


def _handle_preopen_real_tick(
    state: TradeState,
    price: float,
    now_t: dt_time,
    volume: float | None,
    states: dict[str, TradeState],
) -> None:
    if not PREOPEN_BUY_ENABLED:
        return
    if pdy_close_buy:
        return
    if state.preopen_failed or state.preopen_filled:
        return
    extend_until = state.preopen_extend_until or PREOPEN_END
    if now_t < PREOPEN_START or now_t > extend_until:
        return
    if state.preopen_order_pr <= 0:
        return
    if now_t >= PREOPEN_END and volume is not None and volume <= 0:
        return
    if price <= state.preopen_order_pr:
        state.preopen_filled = True
        state.preopen_active = False
        _buy(state, price, "preopen_fill", states, now_t, state.break_pr1, state.break_pr2, cur_price=price)
    else:
        state.preopen_failed = True
        state.preopen_active = False
        _log_trade(
            f"{ts_prefix()} [preopen] fail {state.name} open={price:.0f} ord={state.preopen_order_pr:.0f}",
            states,
        )


def _update_volume_state(state: TradeState, acml_vol: float, now_t: dt_time) -> None:
    minute_key = now_t.hour * 60 + now_t.minute
    if state.last_minute_key is None:
        state.last_minute_key = minute_key
        state.last_acml_vol = acml_vol
        return
    if minute_key != state.last_minute_key:
        v1m = acml_vol - state.last_acml_vol
        if v1m < 0:
            v1m = 0
        state.vol_1m = v1m
        state.vol_hist.append(v1m)
        if len(state.vol_hist) > VOLUME_LOOKBACK_MIN:
            state.vol_hist = state.vol_hist[-VOLUME_LOOKBACK_MIN:]
        state.last_minute_key = minute_key
        state.last_acml_vol = acml_vol


def _volume_breakout_ok(state: TradeState) -> bool:
    if not VOLUME_BREAKOUT_ENABLED:
        return True
    if not state.vol_hist:
        return False
    avg_v = sum(state.vol_hist) / len(state.vol_hist)
    return state.vol_1m > avg_v


def _norm_cols(df: pd.DataFrame) -> list[str]:
    return [str(c).strip().lower() for c in df.columns]


def _parse_recv_ts(val) -> dt_time | None:
    try:
        s = str(val).strip()
    except Exception:
        return None
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
    return dt.time()


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
            fields = [pa.field(c, pa.string()) for c in UNION_COLS]
            UNION_SCHEMA = pa.schema(fields)


def _df_to_table_with_union(df: pd.DataFrame) -> pa.Table:
    _ensure_union_from_df(df)
    assert UNION_SCHEMA is not None
    df2 = df.copy()
    df2.columns = _norm_cols(df2)
    if df2.columns.duplicated().any():
        dupes = [c for c in df2.columns if list(df2.columns).count(c) > 1]
        sys.stdout.write("\n")
        logger.warning(f"[schema] duplicate cols removed: {sorted(set(dupes))}")
        df2 = df2.loc[:, ~df2.columns.duplicated()]
    for c in UNION_SCHEMA.names:
        if c not in df2.columns:
            df2[c] = None
    df2 = df2[UNION_SCHEMA.names]
    for c in df2.columns:
        s = df2[c]
        df2[c] = s.where(~s.isna(), None).astype("string")
    return pa.Table.from_pandas(df2, preserve_index=False, schema=UNION_SCHEMA)


def _is_valid_parquet(path: Path) -> bool:
    try:
        if path.stat().st_size < 8:
            return False
        with path.open("rb") as f:
            head = f.read(4)
            f.seek(-4, 2)
            tail = f.read(4)
        return head == b"PAR1" and tail == b"PAR1"
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


def _handle_existing_parquet(path: Path) -> None:
    ts = datetime.now(KST).strftime("%y%m%d_%H%M%S")
    if not _is_valid_parquet(path):
        backup = path.with_suffix(path.suffix + f".bad.{ts}")
        try:
            path.rename(backup)
            _record_overwrite("invalid_parquet", str(path), str(backup))
        except Exception as e:
            _record_overwrite("invalid_parquet_rename_failed", str(path), str(path))
            logger.warning(f"[parquet] invalid but rename failed: {e}")
        return
    backup = path.with_suffix(path.suffix + f".prev.{ts}")
    try:
        path.rename(backup)
        _record_overwrite("overwrite_existing", str(path), str(backup))
    except Exception as e:
        _record_overwrite("overwrite_existing_rename_failed", str(path), str(path))
        logger.warning(f"[parquet] existing but rename failed: {e}")


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
    sys.stdout.write("\n")  # 제자리 출력 줄바꿈
    _notify(msg)


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
    dataset = ds.dataset([str(p) for p in parts], format="parquet")
    tables.append(dataset.to_table())
    combined = pa.concat_tables(tables, promote_options="permissive") if len(tables) > 1 else tables[0]
    table = _finalize_table(combined)
    pq.write_table(table, final_path, compression="zstd", use_dictionary=True)
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

    df = pd.concat(dfs, ignore_index=True)
    table = _df_to_table_with_union(df)
    _write_part_table(table)
    with _lock:
        global _written_rows, _since_save_rows, _last_flush_time
        _written_rows += table.num_rows
        _since_save_rows = 0
        _last_flush_time = time.time()
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
        # wait() 사용 시 _stop_event 설정 즉시 wake → join 대기 최소화
        _stop_event.wait(timeout=0.5)
    try:
        _flush_all(force=True)
    except Exception:
        pass
    logger.info("[writer] stopped")


# ── data2 모드: 틱 데이터 출력 & 대화형 입력 ──────────────────────
# 출력할 컬럼 목록 (None → 전체 컬럼, 리스트 → 지정 컬럼만)
_DATA2_COLS: list[str] | None = [
    "name", "recv_ts", "stck_prpr", "prdy_ctrt",
    "stck_oprc", "stck_hgpr", "stck_lwpr", "cntg_vol",
    "askp1", "bidp1", "askp_rsqn1", "bidp_rsqn1",
    "total_askp_rsqn", "total_bidp_rsqn",
]
_data2_fast_forward_until: str | None = None   # 예: "090200" → 해당 시각 첫 틱까지 무정지
_data2_run_to_end: bool = False                 # True → 끝까지 멈춤 없이 실행
_data2_col_widths: dict[str, int] = {}          # 현재 테이블 컬럼 폭
_data2_action_occurred: bool = False            # 매매/오더 액션 발생 플래그
_data2_last_code: str | None = None             # 직전 출력 종목코드 (헤더 재출력 판단)
_data2_run_to_action: bool = False              # c: 다음 액션(이벤트)까지 진행
_data2_run_to_action_code: str | None = None    # cc: 특정 종목의 다음 액션까지 진행
_data2_prompt_shown: bool = False               # 안내 프롬프트 1회 출력 여부


def _data2_fmt_recv_ts(val) -> str:
    """recv_ts를 HHMMSS.f 형식으로 변환"""
    s = str(val)
    # 2026-02-06 08:50:00.452364 → 085000.4
    try:
        if " " in s:
            s = s.split(" ", 1)[1]  # 시간 부분만
        # HH:MM:SS.ffffff → HHMMSS.f
        if ":" in s:
            parts = s.split(":")
            hh, mm = parts[0], parts[1]
            ss_frac = parts[2] if len(parts) > 2 else "00"
            if "." in ss_frac:
                ss, frac = ss_frac.split(".", 1)
                return f"{hh}{mm}{ss}.{frac[:1]}"
            else:
                return f"{hh}{mm}{ss_frac}.0"
    except Exception:
        pass
    return s


def _data2_get_val(row, col: str) -> str:
    """data2 출력용 값 포맷팅"""
    val = row.get(col, "")
    if val is None:
        return ""
    if col == "recv_ts":
        return _data2_fmt_recv_ts(val)
    return str(val)


def _data2_print_header(cols: list[str]) -> None:
    """컬럼 헤더 출력 (print_table 스타일)"""
    header = "  ".join(
        kis_utils.pad_text(c, _data2_col_widths.get(c, len(c)), "left")
        for c in cols
    )
    sys.stdout.write(header + "\n")
    sys.stdout.flush()


def _data2_print_row(row, cols: list[str]) -> None:
    """데이터 한 줄 출력 (print_table 스타일, 숫자 우측정렬)"""
    num_cols = {"stck_prpr", "prdy_ctrt", "stck_oprc", "stck_hgpr", "stck_lwpr",
                "cntg_vol", "askp1", "bidp1", "askp_rsqn1", "bidp_rsqn1",
                "total_askp_rsqn", "total_bidp_rsqn", "acml_vol"}
    line = "  ".join(
        kis_utils.pad_text(
            _data2_get_val(row, c),
            _data2_col_widths.get(c, len(c)),
            "right" if c in num_cols else "left",
        )
        for c in cols
    )
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def _data2_update_widths(row, cols: list[str]) -> None:
    """현재 row 기준 컬럼 폭 갱신"""
    for c in cols:
        val_str = _data2_get_val(row, c)
        w = max(kis_utils.display_width(c), kis_utils.display_width(val_str))
        _data2_col_widths[c] = max(_data2_col_widths.get(c, 0), w)


def _data3_print_state(state, price: float, tick_ts: str = "") -> None:
    """data3 모드: 주요 state 변수를 한 줄로 출력.
    tick_ts: 틱의 recv_ts 문자열 (예: "091810.0")
    """
    if state is None:
        return
    ret = (price / state.buy_pr - 1) if state.buy_pr > 0 else 0.0
    peak_ret = (state.peak_price / state.buy_pr - 1) if state.buy_pr > 0 else 0.0
    # 틱 시각 파싱 (버퍼 배율 표시용)
    now_t = datetime.now(KST).time()
    if tick_ts:
        try:
            ts_str = tick_ts.split(".")[0].zfill(6)
            now_t = dt_time(int(ts_str[:2]), int(ts_str[2:4]), int(ts_str[4:6]))
        except Exception:
            pass
    # 현재 적용 중인 버퍼 계산 (표시용)
    near_limit = (
        state.pdy_close > 0
        and state.peak_price > 0
        and (state.peak_price / state.pdy_close - 1) >= LIMIT_UP_THRESHOLD
    )
    if near_limit:
        cur_buffer = 0.0  # bid1 추적 모드에서는 버퍼 대신 bid1 직접 사용
    elif state.trail_step > 0:
        cur_buffer = TRAIL_BUFFER_TABLE[0][1]
        for threshold, buf in TRAIL_BUFFER_TABLE:
            if peak_ret >= threshold:
                cur_buffer = buf
        cur_buffer *= _early_multiplier(now_t)
    else:
        cur_buffer = 0.0
    # trail 표시: 상한가 근접 → trail=값(upT), 일반 → trail=값(peak_ret-buffer%)
    if near_limit:
        trail_tag = "upT"
    elif state.trail_stop > 0 and state.buy_pr > 0:
        trail_pct = peak_ret - cur_buffer
        trail_tag = f"{trail_pct:.2%}"
    else:
        trail_tag = ""
    parts = []
    parts.append(f"qty={state.qty}")
    if state.qty > 0:
        # 보유 중: buy, peak, trail 위주
        parts.append(f"buy={state.buy_pr:.0f}")
        parts.append(f"peak={state.peak_price:.0f}")
    else:
        # 미보유: brk 표시 (다음 매수 돌파가)
        parts.append(f"buy={state.buy_pr:.0f}")
        parts.append(f"brk={state.break_pr:.0f}")
        parts.append(f"peak={state.peak_price:.0f}")
    if trail_tag:
        parts.append(f"trail={state.trail_stop:.0f}({trail_tag})")
    else:
        parts.append(f"trail={state.trail_stop:.0f}")
    parts.append(f"peak_ret={peak_ret:.2%}")
    if state.dy_low is not None:
        parts.append(f"dy_low={state.dy_low:.0f}")
    if state.new_low is not None:
        parts.append(f"new_low={state.new_low:.0f}")
    parts.append(f"pdy_c={state.pdy_close:.0f}")
    parts.append(f"cash={state.cash:,.0f}")
    parts.append(f"tot_ret={(state.cum_ret - 1):.2%}")
    parts.append(f"t_cnt={state.trail_below_count}")
    if state.break_above_count > 0:
        parts.append(f"brk_cnt={state.break_above_count}/{BREAK_CONFIRM_TICKS}")
    sys.stdout.write(f"  ◇ {' | '.join(parts)}\n")
    sys.stdout.flush()


def _data2_print_tick(row, code: str, state=None) -> None:
    """data2/data3 모드: 틱 한 건 출력.
    같은 종목 연속 → 데이터만 추가.
    종목 변경 또는 액션 발생 → 헤더 재출력.
    data3: 틱 아래에 state 변수 한 줄 추가.
    """
    global _data2_action_occurred, _data2_last_code
    cols = _DATA2_COLS if _DATA2_COLS else [c for c in (row.index if hasattr(row, "index") else row) if c != "mksc_shrn_iscd"]

    _data2_update_widths(row, cols)

    need_header = (code != _data2_last_code) or _data2_action_occurred
    if need_header:
        _data2_print_header(cols)
        if code == _data2_last_code:
            # 같은 종목인데 액션으로 헤더 재출력한 경우만 리셋
            # (종목 변경은 _data2_last_code 갱신으로 자동 처리)
            _data2_action_occurred = False

    _data2_print_row(row, cols)
    _data2_last_code = code

    # data3: state 정보는 _data2_print_tick에서 출력하지 않고,
    # 프롬프트 직전에 _data3_show_before_prompt()에서 출력
    pass


def _data2_wait_input() -> None:
    """data2 모드: 사용자 입력 대기.
    Enter  → 다음 틱
    HHMMSS → 해당 시각까지 일괄진행
    c      → 다음 액션(매수/매도/이벤트)까지 진행
    cc     → 현재 종목의 다음 액션까지 진행
    q      → 끝까지 실행
    """
    global _data2_fast_forward_until, _data2_run_to_end
    global _data2_run_to_action, _data2_run_to_action_code
    global _data2_action_occurred
    if _data2_run_to_end:
        return
    if _data2_fast_forward_until is not None:
        return  # fast-forward 중이면 대기 안 함
    if _data2_run_to_action or _data2_run_to_action_code is not None:
        return  # 액션 대기 중이면 대기 안 함
    _data2_action_occurred = False  # 입력 대기 진입 시 플래그 리셋
    global _data2_prompt_shown
    if not _data2_prompt_shown:
        sys.stdout.write("[data2] Enter=다음, HHMMSS=시각, c=종목액션, cc=전체액션, q=끝까지\n")
        sys.stdout.flush()
        _data2_prompt_shown = True
    try:
        user = input("").strip()
    except EOFError:
        _data2_run_to_end = True
        return
    if user == "":
        return
    if user.lower() == "q":
        _data2_run_to_end = True
        return
    if user.lower() == "c":
        # 현재 종목의 다음 액션까지 진행
        _data2_run_to_action_code = _data2_last_code
        return
    if user == "cc":
        # 아무 종목이든 다음 액션까지 진행
        _data2_run_to_action = True
        return
    # HHMMSS 또는 HHMM 패턴
    if user.isdigit() and len(user) in (4, 6):
        _data2_fast_forward_until = user.ljust(6, "0")  # HHMM → HHMM00
        return


def _data2_should_pause(row_t, code: str | None = None) -> bool:
    """fast-forward 목표 조건에 도달했으면 True(=멈춰야 함)"""
    global _data2_fast_forward_until, _data2_run_to_action, _data2_run_to_action_code
    global _data2_action_occurred
    if _data2_run_to_end:
        return False
    # c: 다음 액션까지 진행 — 액션 발생 시 멈춤
    if _data2_run_to_action:
        if _data2_action_occurred:
            _data2_run_to_action = False
            _data2_action_occurred = False
            return True
        return False
    # cc: 특정 종목의 다음 액션까지 진행 — 해당 종목 액션 시 멈춤
    if _data2_run_to_action_code is not None:
        if _data2_action_occurred and code == _data2_run_to_action_code:
            _data2_run_to_action_code = None
            _data2_action_occurred = False
            return True
        return False
    # 시각 fast-forward
    if _data2_fast_forward_until is not None:
        if row_t is None:
            return False
        cur = row_t.strftime("%H%M%S")
        if cur >= _data2_fast_forward_until:
            _data2_fast_forward_until = None
            return True
        return False
    return True  # 일반 모드 → 항상 멈춤


def ingest_loop(states: dict[str, TradeState]):
    global _last_print_ts, _last_summary_ts, _since_save_rows, _last_any_recv_ts, _last_data_dt
    global _data_first_dt, _data_last_dt, _data_row_count, _data_sim_start_time
    global _data_sim_elapsed, _data_summary_elapsed, _last_data_progress_ts
    global _data_row_count_at_last_progress
    logger.info("[ingest] started")
    while not _stop_event.is_set() or not _ingest_queue.empty():
        try:
            item = _ingest_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        if item is None:
            # DATA 모드: 데이터 소진 시 종합집계 생성 후 종료
            if _IS_DATA_MODE and not _summary_written and _data_sim_start_time is not None:
                t_sim_end = time.perf_counter()
                _data_sim_elapsed = t_sim_end - _data_sim_start_time
                _finish_progress_and_notify(
                    f"{ts_prefix()} [시뮬레이션 완료] 시뮬레이션 시작~거래종료(데이터소진): {_data_sim_elapsed:.2f}초"
                )
                _notify(f"{ts_prefix()} [종합집계 생성] 요약 생성 시작")
                t_write_start = time.perf_counter()
                wrote = _write_summary(states)
                _data_summary_elapsed = time.perf_counter() - t_write_start
                if wrote:
                    _notify(f"{ts_prefix()} 종합집계 저장: {SUMMARY_PATH}")
                _notify(
                    f"{ts_prefix()} [시뮬레이션 완료] 요약 생성(빌드+저장): {_data_summary_elapsed:.3f}초"
                )
            _stop_event.set()
            break
        result, trid, kind, is_real, recv_ts = item
        if kind == "unknown":
            kind = "unknown"
        if is_real is None and kind != "unknown":
            is_real = "N" if "exp" in kind else "Y"

        col_map = {str(c).strip().lower(): c for c in result.columns}
        code_col = col_map.get("mksc_shrn_iscd")
        price_col = col_map.get("stck_prpr")
        if not code_col:
            continue

        now = time.time()

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

        if price_col:
            if _IS_DATA_MODE and "recv_ts" in result.columns:
                try:
                    result = result.sort_values("recv_ts", kind="mergesort")
                except Exception:
                    pass
            for t in result.itertuples(index=False):
                row = t._asdict()
                code = str(row.get(code_col, "")).zfill(6)
                st = states.get(code)
                if not st:
                    continue
                try:
                    price = float(row[price_col])
                except Exception:
                    continue
                if price <= 0:
                    continue
                row_t = _parse_recv_ts(row.get("recv_ts"))
                now_t = row_t or datetime.now(KST).time()
                if _IS_DATA_MODE and row_t is not None:
                    try:
                        recv_dt = datetime.combine(_data_base_date or datetime.now(KST).date(), row_t, tzinfo=KST)
                        _log_time_override.dt = recv_dt
                        _last_data_dt = recv_dt
                        if _data_first_dt is None:
                            _data_first_dt = recv_dt
                        _data_last_dt = recv_dt
                        _data_row_count += 1
                    except Exception:
                        _log_time_override.dt = None
                # data 모드: 첫 틱 처리 시점에 시뮬레이션 시작 시각 기록 (_last_data_dt 설정 후 ts_prefix가 첫 틱 시각 반영)
                if DATA_MODE == "data" and _data_sim_start_time is None:
                    _data_sim_start_time = time.perf_counter()
                    _last_data_progress_ts = time.time()
                    _notify(f"{ts_prefix()} [시뮬레이션 시작] 첫 틱 처리, 시뮬레이션 시작")
                # data 모드: 1초 간격 제자리 진행 출력 (화면만), 1초간 row / 누적 row 표시
                if DATA_MODE == "data" and _data_sim_start_time is not None and (time.time() - _last_data_progress_ts) >= 1.0:
                    rows_last_sec = _data_row_count - _data_row_count_at_last_progress
                    _data_row_count_at_last_progress = _data_row_count
                    _last_data_progress_ts = time.time()
                    ts_str = _last_data_dt.strftime("%H:%M:%S") if _last_data_dt else ""
                    sys.stdout.write(f"\r{ts_prefix()} 진행중 {ts_str}  {rows_last_sec}/{_data_row_count}    ")
                    sys.stdout.flush()
                high_val = None
                low_val = None
                for key in ("stck_hgpr", "high"):
                    if key in row and row.get(key) not in (None, ""):
                        high_val = row.get(key)
                        break
                for key in ("stck_lwpr", "low"):
                    if key in row and row.get(key) not in (None, ""):
                        low_val = row.get(key)
                        break
                try:
                    high_price = float(high_val) if high_val is not None else price
                except Exception:
                    high_price = price
                try:
                    low_price = float(low_val) if low_val is not None else price
                except Exception:
                    low_price = price
                ask_val = None
                bid_val = None
                for key in ("askp1", "askp_1", "ask1"):
                    if key in row and row.get(key) not in (None, ""):
                        ask_val = row.get(key)
                        break
                for key in ("bidp1", "bidp_1", "bid1"):
                    if key in row and row.get(key) not in (None, ""):
                        bid_val = row.get(key)
                        break
                try:
                    askp1 = float(ask_val) if ask_val is not None else None
                except Exception:
                    askp1 = None
                try:
                    bidp1 = float(bid_val) if bid_val is not None else None
                except Exception:
                    bidp1 = None
                acml_val = None
                for key in ("acml_vol", "acml_volume"):
                    if key in row and row.get(key) not in (None, ""):
                        acml_val = row.get(key)
                        break
                try:
                    acml_vol = float(acml_val) if acml_val is not None else None
                except Exception:
                    acml_vol = None
                vol_val = None
                for key in ("cntg_vol", "volume"):
                    if key in row and row.get(key) not in (None, ""):
                        vol_val = row.get(key)
                        break
                try:
                    row_vol = float(vol_val) if vol_val is not None else None
                except Exception:
                    row_vol = None
                open_val = None
                for key in ("stck_oprc", "oprc", "open"):
                    if key in row and row.get(key) not in (None, ""):
                        open_val = row.get(key)
                        break
                try:
                    open_price = float(open_val) if open_val is not None else None
                except Exception:
                    open_price = None
                now_sec = _time_to_sec(now_t)
                preopen_time = now_t < PREOPEN_END or (
                    st.preopen_extend_until is not None and now_t <= st.preopen_extend_until
                )
                if open_price is not None and open_price > 0:
                    st.open_price = open_price
                    st.open_confirmed = True
                    st.post_open_wait_start = None
                if preopen_time:
                    st.last_exp = price
                    st.last_exp_ts = now_t
                elif st.open_confirmed:
                    st.last_real = price
                    st.last_real_ts = now_t
                else:
                    st.last_exp = price
                    st.last_exp_ts = now_t

                if acml_vol is not None:
                    _update_volume_state(st, acml_vol, now_t)

                # DATA 모드 종가 매수: 첫 유효 틱에서 1회 실행
                if _IS_DATA_MODE and pdy_close_buy and not st.initial_buy_done:
                    if st.pdy_close > 0:
                        _buy(st, st.pdy_close, "pdy_close_hold", states, now_t, st.break_pr1, st.break_pr2, cur_price=price)
                        st.initial_buy_done = True

                # 동시호가 예상체결 처리 (recv_ts 기준)
                if preopen_time:
                    _handle_preopen_exp_tick(st, price, now_t, askp1, bidp1, states)
                elif not st.open_confirmed and now_t >= REAL_TRADE_START:
                    if st.post_open_wait_start is None:
                        st.post_open_wait_start = now_sec
                    else:
                        vi_stock = st.preopen_extend_until is not None
                        wait_sec = VI_OPEN_WAIT_SEC if vi_stock else NO_OPEN_WAIT_SEC
                        if (now_sec - st.post_open_wait_start) >= wait_sec:
                            if _IS_DATA_MODE:
                                if not st.preopen_failed:
                                    st.preopen_failed = True
                                    st.preopen_active = False
                                    _log_trade(
                                        f"{ts_prefix()} [pre] cancel buy_order {st.name} no_open_{wait_sec}s"
                                        + (" (VI)" if vi_stock else ""),
                                        states,
                                    )
                            else:
                                if st.last_rest_poll_sec is None or (now_sec - st.last_rest_poll_sec) >= 1:
                                    st.last_rest_poll_sec = now_sec
                                    try:
                                        global _price_client
                                        if _price_client is None:
                                            _price_client = _init_price_client()
                                        output = _rest_submit(_fetch_current_price, _price_client, code)
                                        out_vol = float(output.get("acml_vol") or 0)
                                        if out_vol > 0:
                                            st.open_confirmed = True
                                            st.post_open_wait_start = None
                                            try:
                                                out_oprc = float(output.get("stck_oprc") or 0)
                                            except Exception:
                                                out_oprc = 0.0
                                            if out_oprc > 0:
                                                st.open_price = out_oprc
                                            out_price = float(output.get("stck_prpr") or price)
                                            out_ask = output.get("askp1")
                                            out_bid = output.get("bidp1")
                                            try:
                                                askp1 = float(out_ask) if out_ask not in (None, "") else None
                                            except Exception:
                                                askp1 = None
                                            try:
                                                bidp1 = float(out_bid) if out_bid not in (None, "") else None
                                            except Exception:
                                                bidp1 = None
                                            st.last_real = out_price
                                            st.last_real_ts = now_t
                                            open_pr = st.open_price or 0.0
                                            if open_pr <= 0:
                                                continue
                                            if (
                                                st.preopen_order_pr > 0
                                                and not st.preopen_filled
                                                and not st.preopen_failed
                                                and not pdy_close_buy
                                                and open_pr <= st.preopen_order_pr
                                            ):
                                                st.preopen_filled = True
                                                st.preopen_active = False
                                                tdy_ctrt = (open_pr / st.pdy_close - 1) if st.pdy_close > 0 else 0.0
                                                _log_trade(
                                                    f"{ts_prefix()} [pre] 매수완료 {st.name} 매수가:{open_pr:.0f} 전일종가대비 {tdy_ctrt:.2f}",
                                                    states,
                                                )
                                                _buy(
                                                    st,
                                                    open_pr or out_price,
                                                    "preopen_fill",
                                                    states,
                                                    now_t,
                                                    st.break_pr1,
                                                    st.break_pr2,
                                                    cur_price=open_pr,
                                                )
                                            elif (
                                                st.preopen_order_pr > 0
                                                and not st.preopen_filled
                                                and not st.preopen_failed
                                                and not pdy_close_buy
                                            ):
                                                st.preopen_failed = True
                                                st.preopen_active = False
                                                _log_trade(
                                                    f"{ts_prefix()} [pre] fail buy_order {st.name} open={open_pr:.0f} ord={st.preopen_order_pr:.0f}",
                                                    states,
                                                )
                                            _handle_preopen_real_tick(st, out_price, now_t, out_vol, states)
                                            if pdy_close_buy and not st.initial_buy_done and now_t >= CLOSE_AUC_START:
                                                _buy(
                                                    st,
                                                    out_price,
                                                    "close_buy",
                                                    states,
                                                    now_t,
                                                    st.break_pr1,
                                                    st.break_pr2,
                                                    cur_price=out_price,
                                                )
                                                st.initial_buy_done = True
                                            try:
                                                _trade_tick(st, out_price, out_price, out_price, askp1, bidp1, now_t, states, acml_vol=out_vol if out_vol and out_vol > 0 else None)
                                            except Exception as e:
                                                trade_err_logger.error(
                                                    f"{ts_prefix()} [trade_error] code={code} price={out_price} time={now_t} err={e}"
                                                )
                                                trade_err_logger.error(traceback.format_exc())
                                    except Exception as e:
                                        # ── 한도 초과 시 계정2로 재시도 ──
                                        if _is_rate_limit_error(e) and _price_client_2:
                                            try:
                                                output = _rest_submit(_fetch_current_price, _price_client_2, code)
                                                logger.info(f"{ts_prefix()} [rest] 계정2 보충 {code} pr={output.get('stck_prpr','?')}")
                                            except Exception as e2:
                                                logger.warning(f"{ts_prefix()} [rest] current price failed(계정2) code={code}: {e2}")
                                        else:
                                            logger.warning(f"{ts_prefix()} [rest] current price failed code={code}: {e}")
                elif st.open_confirmed:
                    if (
                        st.preopen_order_pr > 0
                        and not st.preopen_filled
                        and not st.preopen_failed
                        and not pdy_close_buy
                    ):
                        open_pr = st.open_price or 0.0
                        if open_pr <= 0:
                            _log_trade(
                                f"{ts_prefix()} [pre] 대기 {st.name} 시가 미형성 → 매수 판단 보류",
                                states,
                            )
                            continue
                        if open_pr <= st.preopen_order_pr:
                            st.preopen_filled = True
                            st.preopen_active = False
                            tdy_ctrt = (open_pr / st.pdy_close - 1) if st.pdy_close > 0 else 0.0
                            _log_trade(
                                f"{ts_prefix()} [pre] 매수완료 {st.name} 매수가:{open_pr:.0f} 전일종가대비 {tdy_ctrt:.2f}",
                                states,
                            )
                            _buy(
                                st,
                                open_pr or price,
                                "preopen_fill",
                                states,
                                now_t,
                                st.break_pr1,
                                st.break_pr2,
                                cur_price=open_pr,
                            )
                        else:
                            st.preopen_failed = True
                            st.preopen_active = False
                            _log_trade(
                                f"{ts_prefix()} [pre] fail buy_order {st.name} open={open_pr:.0f} ord={st.preopen_order_pr:.0f}",
                                states,
                            )
                    _handle_preopen_real_tick(st, price, now_t, row_vol, states)
                    if pdy_close_buy and not st.initial_buy_done and now_t >= CLOSE_AUC_START:
                        _buy(st, price, "close_buy", states, now_t, st.break_pr1, st.break_pr2, cur_price=price)
                        st.initial_buy_done = True
                    try:
                        _trade_tick(st, price, low_price, high_price, askp1, bidp1, now_t, states, acml_vol=acml_vol)
                    except Exception as e:
                        trade_err_logger.error(
                            f"{ts_prefix()} [trade_error] code={code} price={price} time={now_t} err={e}"
                        )
                        trade_err_logger.error(traceback.format_exc())
                if _IS_DATA_MODE:
                    # ── data2/data3 모드: CODE_TEXT 종목만 틱 출력 & 대화형 대기 ──
                    if DATA_MODE in ("data2", "data3") and code in _code_text_codes:
                        _data2_print_tick(row, code, state=st)
                        if _data2_should_pause(row_t, code):
                            # data3: 프롬프트 직전에 state 변수 표시
                            if DATA_MODE == "data3" and st is not None:
                                try:
                                    _p = float(row["stck_prpr"]) if "stck_prpr" in row else 0.0
                                except (ValueError, TypeError):
                                    _p = 0.0
                                _ts = str(row.get("recv_ts", ""))
                                if not _ts and "recv_ts" in row:
                                    _ts = str(row["recv_ts"])
                                _data3_print_state(st, _p, _ts)
                            _data2_wait_input()
                    _log_time_override.dt = None
                    # data 모드: 모든 거래 종료 시 남은 틱 순회 중단
                    if (
                        DATA_MODE == "data"
                        and states
                        and all(s.stop_trading for s in states.values())
                    ):
                        globals()["_all_trading_stopped"] = True
                        break

        if save and ENABLE_SAVE:
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

        global _last_states
        _last_states = states
        _flush_trade_ledger()

        if (now - _last_print_ts) >= 1.0:
            if print_option == 2:
                _print_status(states)
            elif print_option == 1:
                parts = []
                for code, st in states.items():
                    parts.append(f"{st.name}({_per_sec_counts.get(code,0)}/{_total_counts.get(code,0)})")
                line = f"{ts_prefix()} " + " ".join(parts)
                sys.stdout.write("\r" + line)
                sys.stdout.flush()
            for c in _per_sec_counts:
                _per_sec_counts[c] = 0
            _last_print_ts = now

        if not _all_trading_stopped and states and all(st.stop_trading for st in states.values()):
            globals()["_all_trading_stopped"] = True
            if _IS_DATA_MODE:
                _notify(
                    f"{ts_prefix()} [모든 종목 거래 종료] 현재 모니터링 대상 종목이 없어 거래를 종료합니다."
                    + (" (즉시 요약 생성 후 종료)" if DATA_MODE == "data" else " (15:30까지 가격 모니터링 후 요약 생성)")
                )
            else:
                _notify(
                    f"{ts_prefix()} [모든 종목 거래 종료] 현재 모니터링 대상 종목이 없어 거래를 종료합니다."
                    "(다만 wss 수신은 지속합니다.)"
                )

        # data 모드: 모든 거래 종료 시 즉시 요약 생성 후 종료 (남은 틱 순회 생략)
        if (
            _all_trading_stopped
            and DATA_MODE == "data"
            and not _summary_written
            and _data_sim_start_time is not None
        ):
            t_sim_end = time.perf_counter()
            _data_sim_elapsed = t_sim_end - _data_sim_start_time
            _finish_progress_and_notify(
                f"{ts_prefix()} [시뮬레이션 완료] 시뮬레이션 시작~거래종료: {_data_sim_elapsed:.2f}초"
            )
            _notify(f"{ts_prefix()} [종합집계 생성] 요약 생성 시작")
            # 순수 요약 생성(빌드+저장)만 측정 (_notify/tmsg 블로킹 제외)
            t_write_start = time.perf_counter()
            wrote = _write_summary(states)
            t_write_end = time.perf_counter()
            _data_summary_elapsed = t_write_end - t_write_start
            if wrote:
                _notify(f"{ts_prefix()} 종합집계 저장: {SUMMARY_PATH}")
            _notify(
                f"{ts_prefix()} [시뮬레이션 완료] 요약 생성(빌드+저장): {_data_summary_elapsed:.3f}초"
            )
            _stop_event.set()
            break

        # DATA 모드 종료 판단: DATA_SIM_END 설정 시 해당 시각, 미설정 시 15:30
        # data2/data3: 대화형이므로 종료 판단만 하고 종합집계는 데이터 소진 후 출력
        _data_end_time = DATA_SIM_END if (DATA_SIM_END is not None) else CLOSE_AUC_END
        if (
            _IS_DATA_MODE
            and (not _summary_written)
            and _last_data_dt is not None
            and _last_data_dt.time() >= _data_end_time
            and DATA_MODE not in ("data2", "data3")
        ):
            if _data_sim_start_time is not None:
                t_sim_end = time.perf_counter()
                _data_sim_elapsed = t_sim_end - _data_sim_start_time
                _finish_progress_and_notify(
                    f"{ts_prefix()} [시뮬레이션 완료] 시뮬레이션 시작~거래종료({_data_end_time.strftime('%H:%M')}도달): {_data_sim_elapsed:.2f}초"
                )
            _notify(f"{ts_prefix()} [종합집계 생성] {_data_end_time.strftime('%H:%M')} 도달 → 요약 생성 시작")
            t_write_start = time.perf_counter()
            wrote = _write_summary(states)
            _data_summary_elapsed = time.perf_counter() - t_write_start
            if wrote:
                _notify(f"{ts_prefix()} 종합집계 저장: {SUMMARY_PATH}")
            _notify(
                f"{ts_prefix()} [시뮬레이션 완료] 요약 생성(빌드+저장): {_data_summary_elapsed:.3f}초"
            )
            _stop_event.set()

        if (now - _last_summary_ts) >= SUMMARY_EVERY_SEC:
            _last_summary_ts = now
    logger.info("[모니터링 종료] 수신 처리 루프 종료")


def _shutdown(reason: str):
    if _stop_event.is_set():
        return
    logger.warning(f"[shutdown] {reason}")
    _stop_event.set()
    global _last_states
    if _all_trading_stopped and (not _summary_written) and _last_states:
        if _write_summary(_last_states):
            _notify(f"{ts_prefix()} 종합집계 저장: {SUMMARY_PATH}")
    _flush_trade_ledger(force=True)
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

def _apply_subscriptions(kws, desired_reqs: list, force: bool = False) -> None:
    desired = {req: set(codes) for req in desired_reqs}
    all_reqs = [ccnl_krx, exp_ccnl_krx, overtime_ccnl_krx, overtime_exp_ccnl_krx]  # noqa: F405

    # ── 1단계: 해지 먼저 (MAX SUBSCRIBE OVER 방지) ──────────────
    pending_adds: list[tuple] = []
    for req in all_reqs:
        want = desired.get(req, set())
        have = _subscribed.get(req.__name__, set())
        if force and want:
            to_add = list(want)
            to_del = list(want)
        elif want != have:
            to_add = list(want - have)
            to_del = list(have - want) if want else list(have)
        else:
            to_add = []
            to_del = []
        # 해지를 즉시 실행
        if to_del:
            _send_subscribe(kws, req, to_del, "2")
        if to_add:
            pending_adds.append((req, to_add))
        if to_add or to_del:
            _subscribed[req.__name__] = set(want)

    # ── 2단계: 신규 구독 (해지 완료 후) ──────────────────────
    for req, add_list in pending_adds:
        _send_subscribe(kws, req, add_list, "1")

def _desired_reqs(now: datetime) -> list:
    global _overtime_real_active, _real_start_delay_until
    t = now.time()

    if t < dt_time(8, 50):
        return []
    if dt_time(8, 50) <= t < dt_time(9, 0):
        return [exp_ccnl_krx]  # noqa: F405

    if dt_time(9, 0) <= t < dt_time(15, 10, 10):
        if _real_start_delay_until and now < _real_start_delay_until:
            return [exp_ccnl_krx]  # noqa: F405
        return [ccnl_krx]  # noqa: F405

    if dt_time(15, 10, 10) <= t < dt_time(15, 30):
        return [exp_ccnl_krx]  # noqa: F405

    if dt_time(15, 30) <= t < dt_time(16, 0):
        # 모든 종목 체결가 수신 완료 → 즉시 구독 종료
        if len(_regular_real_seen) >= len(codes):
            return []
        # 15:31:00 이후 → 미수신 종목 무시하고 강제 구독 중단 (WSS 연결 유지)
        global _close_force_stopped
        if t >= CLOSE_STOP_TIME:
            if not _close_force_stopped:
                _close_force_stopped = True
                msg = (
                    f"{ts_prefix()} [close] 15:31 → 강제 구독 중단 "
                    f"(수신 {len(_regular_real_seen)}/{len(codes)}종목, 16:00 시간외 대기)"
                )
                _notify(msg)
            return []
        return [ccnl_krx]  # noqa: F405

    if dt_time(16, 0) <= t < END_TIME:
        return [overtime_ccnl_krx] if _overtime_real_active else [overtime_exp_ccnl_krx]  # noqa: F405

    return []


def _replay_data(states: dict[str, TradeState]) -> None:
    """
    저장된 parquet를 시간순으로 재생.
    - tr_id/is_real_ccnl 등 기록 데이터는 신뢰하지 않음
    - 실제 분류는 ingest_loop에서 각 틱별로 stck_oprc 유무 + 시간으로 판단
    - chunk_size=100 단위로 큐에 넣되, kind='data_replay'로 통일
    """
    src_path = OUT_DIR / f"{DATA_DATE}_wss_data.parquet"
    legacy_path = OUT_DIR / f"{DATA_DATE}_trade_test_wss_data.parquet"
    if not src_path.exists() and legacy_path.exists():
        logger.warning(f"[data] legacy data file detected, using: {legacy_path}")
        src_path = legacy_path
    if not src_path.exists():
        logger.error(f"[data] parquet not found: {src_path}")
        return
    logger.info(f"{ts_prefix()} [data] loading {src_path.name}")

    # ── LazyFrame + predicate pushdown: CODE_TEXT 종목만 디스크에서 읽기 ──
    lf = pl.scan_parquet(src_path)
    # 종목코드 컬럼 탐색
    schema_cols = lf.collect_schema().names()
    code_col_pl = None
    for cand in ("mksc_shrn_iscd", "stck_shrn_iscd", "code"):
        if cand in schema_cols:
            code_col_pl = cand
            break
    if code_col_pl and _code_text_codes:
        target_set = set(_code_text_codes)
        lf = lf.filter(
            pl.col(code_col_pl).cast(pl.Utf8).str.zfill(6).is_in(target_set)
        )
    if "recv_ts" in schema_cols:
        lf = lf.sort("recv_ts")
    df = lf.collect().to_pandas()
    if df.empty:
        logger.warning("[data] empty parquet (필터 후 데이터 없음)")
        return
    _notify(
        f"{ts_prefix()} [data] loaded {len(df):,}행"
        + (f" ({len(_code_text_codes)}종목 필터)" if code_col_pl else "")
    )
    if "recv_ts" in df.columns:
        try:
            df = df.sort_values("recv_ts", kind="mergesort").reset_index(drop=True)
        except Exception as e:
            logger.warning(f"[data] recv_ts sort failed: {e}")

    # NOTE: tr_id/kind 분류를 chunk 단위로 하지 않음.
    # 각 틱(row)의 실제 구분은 ingest_loop 내부에서
    # stck_oprc 유무 + recv_ts 기반 시간으로 개별 판단.
    chunk_size = 100
    for i in range(0, len(df), chunk_size):
        if _stop_event.is_set():
            break
        chunk = df.iloc[i : i + chunk_size].copy()
        recv_ts = (
            str(chunk["recv_ts"].iloc[-1])
            if "recv_ts" in chunk.columns
            else datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S.%f")
        )
        _ingest_queue.put((chunk, "DATA", "data_replay", None, recv_ts))
    logger.info(f"{ts_prefix()} [data] replay complete rows={len(df)}")


def run_ws_forever():
    backoff = 2
    attempt = 0
    while not _stop_event.is_set():
        mode = _get_mode()
        if mode == RunMode.EXIT:
            break
        attempt += 1
        kws = None
        try:
            logger.info(f"{ts_prefix()} [ws] connect attempt={attempt} mode={mode}")

            # open_map 초기화 후 현재 시각에 맞는 구독 등록
            ka.open_map.clear()
            _subscribed.clear()
            desired = _desired_reqs(datetime.now(KST))
            for req in desired:
                ka.KISWebSocket.subscribe(req, list(codes))
                _subscribed[req.__name__] = set(codes)
            logger.info(
                f"{ts_prefix()} [ws] open_map prepared: "
                f"{[r.__name__ for r in desired]} codes={len(codes)}"
            )

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
            update_time_flags()
            logger.info(f"{ts_prefix()} [ws] start on_result (parquet={FINAL_PARQUET_PATH.name})")
            kws.start(on_result=on_result)
            logger.warning(f"{ts_prefix()} [ws] kws.start returned -> reconnecting")
        except SystemExit:
            break
        except Exception as e:
            logger.error(f"{ts_prefix()} [ws] connection exception: {e}")
            logger.error(traceback.format_exc())
        finally:
            with _kws_lock:
                if _active_kws is kws:
                    _active_kws = None
        if _stop_event.is_set():
            break
        logger.info(f"{ts_prefix()} [ws] reconnect in {backoff}s...")
        time.sleep(backoff)
    logger.info(f"{ts_prefix()} [ws] stopped")


def _build_states() -> dict[str, TradeState]:
    name_map = _code_name_map()
    # data 모드: CODE_TEXT 종목만 분석, wss 모드: 전체 codes 분석
    trade_target = list(_code_text_codes) if _IS_DATA_MODE else list(codes)
    per_cash = INITIAL_CAPITAL / max(len(trade_target), 1)
    states: dict[str, TradeState] = {}
    logger.info(
        f"{ts_prefix()} [init] loading state data for {len(trade_target)} codes"
        + (f" (CODE_TEXT 필터)" if _IS_DATA_MODE else "")
    )
    cache = _load_1d_cache()
    for code in trade_target:
        info = cache.get(code, {})
        if not info:
            t0 = time.perf_counter()
            try:
                info = stQueryDB(code)
            except Exception as e:
                logger.warning(f"{ts_prefix()} [init] stQueryDB failed code={code}: {e}")
                info = {}
            elapsed = (time.perf_counter() - t0) * 1000
            logger.info(f"{ts_prefix()} [init] stQueryDB {code} took {elapsed:.1f}ms")
        pdy_close = float(info.get("pdy_close", 0) or 0) or float(info.get("close", 0) or 0)
        pdy_ctrt = float(info.get("pdy_ctrt", 0) or 0)
        r_avr = float(info.get("R_avr", 0) or 0)
        st = TradeState(
            code=code,
            name=name_map.get(code, code),
            pdy_close=pdy_close,
            pdy_ctrt=pdy_ctrt,
            r_avr=r_avr,
            rk=r_avr * K,
            cash=per_cash,
            alloc_cash=per_cash,
        )
        st.rk1 = r_avr * K
        st.rk2 = r_avr * K * K2
        states[code] = st
        logger.info(
            f"{ts_prefix()} [init] {code}:{name_map.get(code, code)} "
            f"pdy_close={pdy_close:.0f} R_avr={r_avr:.2f}"
        )
    logger.info(f"{ts_prefix()} [init] state data loaded")
    return states


if __name__ == "__main__":
    _notify(f"{ts_prefix()} {PROGRAM_NAME} 시작")
    if DATA_MODE == "wss":
        now_kst = datetime.now(KST)
        if _is_weekend(now_kst) or _is_holiday(now_kst):
            reason = "주말" if _is_weekend(now_kst) else "공휴일"
            _notify(f"{ts_prefix()} 휴무일({reason}) -> wss 모드 종료")
            raise SystemExit(0)
        _auth()

    states = _build_states()
    # 시작 시 현재 모드 상태를 바로 출력
    if _IS_DATA_MODE:
        # data 모드: 실제 시각과 무관하게 REGULAR_REAL로 시작 (파일 재생이므로)
        cur_mode = RunMode.REGULAR_REAL
        _current_mode = cur_mode
        _notify(f"{ts_prefix()} data 모드 → parquet 파일 로딩 중(DATE={DATA_DATE})")
        logger.info(f"{ts_prefix()} [mode] data mode, forced={cur_mode}")
    else:
        cur_mode = calc_mode(datetime.now(KST))
        _log_mode_transition(None, cur_mode)
        _current_mode = cur_mode
        logger.info(f"{ts_prefix()} [mode] current={cur_mode}")

    if DATA_MODE == "wss" and datetime.now(KST).time() >= END_TIME:
        _notify(f"{ts_prefix()} 종료 시각 경과 -> 병합 점검")
        _write_summary(states)
        _notify(f"{ts_prefix()} 파일 병합 시작")
        _merge_parts()
        _notify(f"{ts_prefix()} 파일 병합 완료")
        _notify(f"{ts_prefix()} {PROGRAM_NAME} 종료")
        raise SystemExit(0)

    t_writer = threading.Thread(target=writer_loop, daemon=True)
    t_writer.start()

    if DATA_MODE in ("data2", "data3"):
        # data2/data3: ingest_loop을 메인 스레드에서 실행 (stdin input 사용)
        t_replay = threading.Thread(target=lambda: (_replay_data(states), _ingest_queue.put(None)), daemon=True)
        t_replay.start()
        try:
            ingest_loop(states)
        except (KeyboardInterrupt, EOFError):
            _stop_event.set()
        t_replay.join(timeout=2.0)
        t_writer.join(timeout=2.0)
    elif DATA_MODE == "wss":
        t_ingest = threading.Thread(target=ingest_loop, args=(states,), daemon=True)
        t_ingest.start()

        t_print = threading.Thread(target=_print_option_loop, daemon=True)
        t_print.start()

        t_flags = threading.Thread(target=time_flag_loop, daemon=True)
        t_flags.start()

        t_sched = threading.Thread(target=scheduler_loop, daemon=True)
        t_sched.start()

        t_rest = threading.Thread(target=_rest_worker, daemon=True)
        t_rest.start()

        t_top = threading.Thread(target=_top_rank_loop, daemon=True)
        t_top.start()

        t_price_watch = threading.Thread(target=_price_watchdog_loop, daemon=True)
        t_price_watch.start()
        t_price_req = threading.Thread(target=_price_request_loop, daemon=True)
        t_price_req.start()

        try:
            run_ws_forever()
        finally:
            _shutdown("finally")

        try:
            t_writer.join(timeout=2.0)
            t_ingest.join(timeout=2.0)
            t_print.join(timeout=2.0)
            t_flags.join(timeout=2.0)
            t_sched.join(timeout=2.0)
            t_rest.join(timeout=2.0)
            t_top.join(timeout=2.0)
        except Exception:
            pass
    else:
        # data 모드
        t_ingest = threading.Thread(target=ingest_loop, args=(states,), daemon=True)
        t_ingest.start()

        t_print = threading.Thread(target=_print_option_loop, daemon=True)
        t_print.start()

        _replay_data(states)
        _ingest_queue.put(None)  # sentinel: ingest_loop에 데이터 소진을 알림

        try:
            t_ingest.join()
            t_writer.join()
            t_print.join(timeout=2.0)
        except Exception:
            pass
    should_merge = False
    if DATA_MODE == "wss":
        now_t = datetime.now(KST).time()
        should_merge = now_t >= END_TIME or _get_mode() == RunMode.EXIT
    # data2/data3: 종합집계가 중간에 씌여졌더라도 최종 출력 보장
    if DATA_MODE in ("data2", "data3"):
        globals()["_summary_written"] = False
    if _write_summary(states):
        _notify(f"{ts_prefix()} 종합집계 결과 파일 저장: {SUMMARY_PATH}")
    if _IS_DATA_MODE and _data_first_dt and _data_last_dt:
        _notify(
            f"{ts_prefix()} data 범위 {_data_first_dt.strftime('%H:%M:%S')}~"
            f"{_data_last_dt.strftime('%H:%M:%S')} rows={_data_row_count}"
        )
    if DATA_MODE == "data" and _data_sim_start_time is not None:
        total_elapsed = time.perf_counter() - _data_sim_start_time
        if _data_sim_elapsed > 0 or _data_summary_elapsed > 0:
            cleanup_elapsed = total_elapsed - _data_sim_elapsed - _data_summary_elapsed
            _notify(
                f"{ts_prefix()} [시뮬레이션 완료] 총 소요 시각: {total_elapsed:.2f}초 "
                f"(거래={_data_sim_elapsed:.2f}s + 요약={_data_summary_elapsed:.3f}s + 정리={cleanup_elapsed:.2f}s)"
            )
        else:
            _notify(f"{ts_prefix()} [시뮬레이션 완료] 총 소요 시각: {total_elapsed:.2f}초")
    if should_merge:
        _notify(f"{ts_prefix()} 파일 병합 시작: {FINAL_PARQUET_PATH}")
        _merge_parts()
        _notify(f"{ts_prefix()} 파일 병합 완료: {FINAL_PARQUET_PATH}")
    else:
        parts_n = len(list(PART_DIR.glob("*.parquet")))
        _notify(f"{ts_prefix()} 중간 종료 → parts {parts_n}개 유지 (다음 시작 시 병합)")
    _flush_overwrite_log()
    _notify(f"{ts_prefix()} {PROGRAM_NAME} 종료")
    logger.info("=== WSS TRADE TEST END ===")
