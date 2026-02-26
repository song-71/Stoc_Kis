"""
시간대별로 서로 다른 TR 호출코드가 적용됨.
    정규장 실시간 체결가: ccnl_krx
    정규장 예상 체결가: exp_ccnl_krx
    시간외 실시간 체결가: overtime_ccnl_krx
    시간외 예상 체결가: overtime_exp_ccnl_krx

# 국내주식 시간대별 주문구분코드(ORD_DVSN) 참고
| 시간대          | 구분            | 체결방식       | ORD_DVSN                        | 비고           |
| --------------- | --------------- | -------------- | ------------------------------- | -------------- |
| 08:30~08:40     | 시간외 종가     | 즉시 매칭      | 02                              | 전일 종가 고정 |
| 08:30~09:00     | 시초가 동시호가 | 09:00 일괄     | 00(지정가) 01(시장가)           | 시가 결정      |
| 09:00~15:20     | 정규장          | 실시간 체결    | 00(지정가) 01(시장가)           | 일반 매매      |
| 15:20~15:30     | 종가 동시호가   | 15:30 일괄     | 00(지정가) 01(시장가)           | 종가 결정      |
| 15:40~16:00     | 시간외 종가     | 즉시 매칭      | 02                              | 당일 종가 고정 |
| 16:00~18:00     | 시간외 단일가   | 10분 단위 일괄 | 06                              | 시장가 불가    |

스케줄 모드별 구독 구성 현황
    08:29~08:59: exp_ccnl_krx
    08:59~09:00: exp_ccnl_krx + ccnl_krx
    09:00~15:19: ccnl_krx
    15:20~15:30: ccnl_krx + exp_ccnl_krx
    15:59~18:00: overtime_exp_ccnl_krx + overtime_ccnl_krx

=> 현재 지정된 목록과 초기 및 30분 간격 top30리스트로 5개씩 추가한 목록에 대해 최대 41개 범위내에서 구독 지속
=> 이 부분까지 잘 작동됨(지정된 목록 + top30의 5개 종목, 매일 자동화 잘 됨.)
  * 다만 매일 기본 종목 목록은 수기로 수정해야 함.

공통계정(계정1/메인) 사용 : 한도 초과 시 계정2(syw_2)로 재시도
데이터 저장: /home/ubuntu/Stoc_Kis/data/wss_data/

종가매수 대상 기록:
  - 선정/주문/체결 state: data/fetch_top_list/closing_buy_state_{yymmdd}.json
  - top30 추가 리스트: data/fetch_top_list/wss_sub_add_top10_list_{yymmdd}.csv
"""
# =============================================================================
# 시간대 운영(구독 + 저장)
# - 스케줄에 따라 웹소켓 구독을 재구성(연결 재시작)합니다.
# - 저장은 SAVE_MODE/time_flag에 의해 on/off 됩니다.
# =============================================================================
#260223 수신대상 종목(업종) 목록 : 26개
CODE_TEXT = """
광동제약
미래에셋생명
롯데손해보험
비케이홀딩스
한화생명
흥국화재우
보성파워텍
체리부로
흥국화재
다원넥스뷰
퍼스텍
한화손해보험
흥구석유`
TBH글로벌
바이오톡스텍
풍원정밀
"""

# Top5 추가 구독 (False면 CODE_TEXT만 18:00까지 수신, Top50 다운/저장만 수행)
TOP5_ADD = True   # False: Top50 다운·저장 O, Top5 추가/해제/종가매매전환/미수신구독해제 X

# 한도 초과 시 폴백 계정 사용 여부 (True: 한도 초과 시 계정2(syw_2)로 재시도)
USE_FALLBACK_ACCOUNT = False

# 15:20 종가매수 옵션 (True: 15:19 선정 종목에 init_cash 균등분배 시장가=상한가 매수, 선정조건=Select_Tr_target_list)
CLOSE_BUY = True

# Str1 실전 매도 옵션 (True: 종가매수 체결 종목에 대해 다음날 실시간 매도 전략 적용)
STR1_SELL_ENABLED = True

# 재시작 시 미체결 매수주문 전부 취소 (True: 정정취소가능주문조회 후 취소, 주문번호 기록 없어도 가능)
OPEN_BUY_ORDER_CANCEL = True
INIT_CASH = 1_000_000  # 총 매수 한도(원), 종목당 균등분배
# 08:30~08:40 시간외 종가 추가 매수 (True: 전일 미매수 종목에 전일종가 지정가 ORD_DVSN=02 매수)
MORNING_EXTRA_CLOSING_PR_BUY = True

"""
실행/모니터링/종료 명령어

재시작 (기존 종료 후 업데이트 버전 즉시 실행):
  /home/ubuntu/Stoc_Kis/ws_restart_wss_DB1.py
    => restart_wss_DB1.sh을 재시작해줌.

nohup 실행:
  nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/ws_realtime_subscribe_to_DB-1.py > /home/ubuntu/Stoc_Kis/out/wss_realtime_DB-1.out 2>&1 &

로그 모니터링:
  <노협 백그라운드 실행 모니터링>
  tail -f /home/ubuntu/Stoc_Kis/out/wss_realtime_DB-1.out
  <터미널 실행 모니터링>
  tail -f /home/ubuntu/Stoc_Kis/out/logs/wss_DB1_260204.log

프로세스 확인:
  pgrep -af ws_realtime_subscribe_to_DB-1.py

일괄 종료:현재 떠있는 여러 프로세스를 동시에 종료하는 명령어
  pkill -f ws_realtime_subscribe_to_DB-1.py
  
  <정말 안죽으면>
  pkill -9 -f ws_realtime_subscribe_to_DB-1.py

종료 확인:
  pgrep -af ws_realtime_subscribe_to_DB-1.py || echo "no process"



wss 데이터 저장위치 : /home/ubuntu/Stoc_Kis/data/wss_data/
"""

import sys
import time
import shutil
import signal
import threading
import traceback
import queue
import math
import warnings
from collections import deque
from pathlib import Path
from datetime import date, datetime, time as dtime, timedelta
from enum import Enum, auto
from zoneinfo import ZoneInfo

import pandas as pd
import polars as pl
import requests

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

LOG_ID = "DB-1"  # 로그/텔레그램 메시지 식별용 (여러 파일 동시 실행 시 구분)

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
OUT_DIR = SCRIPT_DIR / "data" / "wss_data"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = SCRIPT_DIR / "config.json"
SUB_CFG_PATH = SCRIPT_DIR / "config_wss_1.json"  # 구독 관리 전용 (DB-1)

LOG_DIR = SCRIPT_DIR / "out" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

today_yymmdd = datetime.now(KST).strftime("%y%m%d")
FINAL_PARQUET_PATH = OUT_DIR / f"{today_yymmdd}_wss_data.parquet"
PART_DIR = OUT_DIR / "parts"
PART_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR = OUT_DIR / "backup"
LOG_PATH = LOG_DIR / f"wss_DB1_{today_yymmdd}.log"
CCNL_NOTICE_LOG_PATH = LOG_DIR / f"ccnl_notice_{today_yymmdd}.log"

# 거래장부 CSV (일자별 누적, 하루치 모든 주문/체결 기록)
LEDGER_DIR = SCRIPT_DIR / "data" / "ledger"
LEDGER_DIR.mkdir(parents=True, exist_ok=True)
LEDGER_PATH = LEDGER_DIR / f"trade_ledger_{today_yymmdd}.csv"
LEDGER_COLS = [
    "date", "time", "code", "name",
    "order_type",    # BUY_ORDER / SELL_ORDER / SELL_CANCEL / BUY_FILL / SELL_FILL
    "ord_dvsn",      # 시장가/지정가/시간외종가 등
    "buy_price",     # 매수가 (매수 시점 or 포지션 매수가)
    "sell_price",    # 매도가 (주문가 or 체결가)
    "order_qty",     # 주문 수량
    "fill_qty",      # 체결 수량
    "amount",        # 금액 (price × fill_qty)
    "prdy_ctrt",     # 전일대비율 (%)
    "reason",        # 매도 사유
    "ord_no",        # 주문번호
    "note",
]
_ledger_lock = threading.Lock()

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
    prefix = "wss_DB1_"
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
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import kis_auth_llm as ka  # noqa: E402
sys.modules["kis_auth"] = ka

from domestic_stock_functions_ws import *  # noqa: F403,E402
import json  # noqa: E402
from kis_utils import is_holiday, load_config, load_symbol_master, save_config, calc_limit_up_price  # noqa: E402
from kis_str1_realtime_sell import (  # noqa: E402
    check_premarket_sell, check_premarket_cancel, check_realtime_sell, calc_sell_pnl,
    FEE_RATE_MARKET_SELL,
)
from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig  # noqa: E402
from symulation.Select_Tr_target_list_symulation_pdy_ctrt import get_closing_buy_candidates  # noqa: E402

# ★ import * 이 logger 를 덮어쓰므로 재설정
logger = logging.getLogger("wss")

# =============================================================================
# 인증 — 공통계정(계정1/메인)으로 접속
# =============================================================================
_cfg_json_auth = load_config(str(SCRIPT_DIR / "config.json"))
# 공통계정: config.json 루트의 appkey/appsecret 사용
_main_appkey = _cfg_json_auth.get("appkey")
_main_appsecret = _cfg_json_auth.get("appsecret")
if not _main_appkey or not _main_appsecret:
    raise ValueError("config.json 루트에 appkey/appsecret이 필요합니다.")

# WSS 접속 credential을 공통계정(메인)으로 설정
ka._cfg["my_app"] = _main_appkey
ka._cfg["my_sec"] = _main_appsecret
# 토큰 파일을 공통계정 전용으로 분리 (계정2와 충돌 방지)
_main_token_path = Path(ka.config_root) / f"KIS_main_{datetime.today().strftime('%Y%m%d')}"
ka.token_tmp = str(_main_token_path)
if not _main_token_path.exists():
    _main_token_path.touch()
logger.info(f"[auth] 공통계정(계정1/메인) credential 적용, token_file={ka.token_tmp}")

ka.auth(svr="prod")
ka.auth_ws(svr="prod")
if not getattr(ka, "_base_headers_ws", {}).get("approval_key"):
    raise RuntimeError("auth_ws() 실패: 공통계정(계정1/메인)의 앱키/시크릿을 확인하세요.")

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

# 1분마다 _accumulator 전체를 FINAL_PARQUET_PATH에 스냅샷 저장
SAVE_EVERY_SEC = 60

# ── 기술적 지표 설정 ──
# EMA 기간 리스트 (ma3, ma50, ma200, ma300, ma500, ma2000 컬럼 생성)
INDICATOR_MA_LINE = [3, 50, 200, 300, 500, 2000]
# 볼린저 밴드 기간 및 승수
INDICATOR_BB_PERIOD = 200
INDICATOR_BB_K = 2.0
# 롤링 버퍼 최대 크기 (BB 계산에 필요한 최대 윈도우)
_IND_MAX_WINDOW = max(max(INDICATOR_MA_LINE), INDICATOR_BB_PERIOD)

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
TOP_RANK_OUT_DIR = Path("/home/ubuntu/Stoc_Kis/data/fetch_top_list")
MAX_WSS_SUBSCRIBE = 41                 # WSS 구독 총 상한
# 15:19 종가매매 종목 선정 (Select_Tr_target_list_symulation 조건 사용, 10개 제한 없음)
CLOSING_PERIOD_START = "2026-01-30"    # 1d 조회 시작일 (YYYY-MM-DD)
CLOSING_PERIOD_END = ""                # 종료일, 비우면 오늘
CLOSING_STRATEGY = "str3"              # str1~str5 (Select_Tr_target_list 동일)
CLOSING_TARGET_PDY_CTRT = 0.20         # str1/str3 전일/당일 등락률 기준


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


def _hashkey(base_url: str, appkey: str, appsecret: str, body: dict) -> str:
    url = f"{base_url}/uapi/hashkey"
    r = requests.post(url, headers={"content-type": "application/json", "appkey": appkey, "appsecret": appsecret}, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    return j.get("HASH") or j.get("hash") or ""


def _get_balance_page(client, cano: str, acnt_prdt_cd: str, tr_id: str, ctx_fk: str = "", ctx_nk: str = "") -> tuple:
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
    params = {
        "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd, "AFHR_FLPR_YN": "N",
        "OFL_YN": "", "INQR_DVSN": "02", "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00",
        "CTX_AREA_FK100": ctx_fk, "CTX_AREA_NK100": ctx_nk,
    }
    r = requests.get(url, headers=client._headers(tr_id=tr_id), params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"잔고조회 실패: {j.get('msg1')} raw={j}")
    out1 = j.get("output1") or []
    out2 = j.get("output2") or {}
    if isinstance(out1, dict):
        out1 = [out1]
    return out1, out2, str(j.get("ctx_area_fk100") or out2.get("ctx_area_fk100") or ""), str(j.get("ctx_area_nk100") or out2.get("ctx_area_nk100") or "")


def _buy_order_cash(client, cano: str, acnt_prdt_cd: str, tr_id: str, code: str, qty: int, price: float, ord_dvsn: str = "00") -> dict:
    """ord_dvsn: 00=지정가, 01=시장가, 05=장전시간외종가(08:30~08:40), 06=장후시간외종가(15:40~16:00), 07=시간외단일가(16:00~18:00)"""
    # 시장가(01)는 ORD_UNPR을 0으로 입력해야 함 (KIS API 요구사항)
    ord_unpr = "0" if ord_dvsn == "01" else str(int(price))
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    body = {"CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd, "PDNO": code, "ORD_DVSN": ord_dvsn, "ORD_QTY": str(qty), "ORD_UNPR": ord_unpr}
    headers = client._headers(tr_id=tr_id)
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"매수 주문 실패: {j.get('msg1')} raw={j}")
    return j


def _append_ledger(
    order_type: str,
    code: str,
    name: str = "",
    buy_price: float = 0.0,
    sell_price: float = 0.0,
    order_qty: int = 0,
    fill_qty: int = 0,
    prdy_ctrt: float = 0.0,
    reason: str = "",
    ord_no: str = "",
    ord_dvsn: str = "",
    note: str = "",
) -> None:
    """거래장부 CSV 에 1행 추가 (스레드 안전)."""
    try:
        now = datetime.now(KST)
        amount = sell_price * fill_qty if sell_price > 0 else buy_price * fill_qty
        row = {
            "date":       now.strftime("%Y-%m-%d"),
            "time":       now.strftime("%H:%M:%S"),
            "code":       str(code).zfill(6),
            "name":       name or code_name_map.get(str(code).zfill(6), ""),
            "order_type": order_type,
            "ord_dvsn":   ord_dvsn,
            "buy_price":  round(buy_price, 2),
            "sell_price": round(sell_price, 2),
            "order_qty":  order_qty,
            "fill_qty":   fill_qty,
            "amount":     round(amount, 0),
            "prdy_ctrt":  round(prdy_ctrt, 2),
            "reason":     reason,
            "ord_no":     ord_no,
            "note":       note,
        }
        write_header = not LEDGER_PATH.exists()
        with _ledger_lock:
            # BOM은 신규 파일 최초 작성 시에만 추가 (append 시 BOM 중복 삽입 방지)
            enc = "utf-8-sig" if write_header else "utf-8"
            with open(LEDGER_PATH, "a", encoding=enc, newline="") as f:
                import csv as _csv
                writer = _csv.DictWriter(f, fieldnames=LEDGER_COLS)
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
    except Exception as e:
        logger.warning(f"[ledger] 기록 실패: {e}")


def _sell_order_cash(
    client, cano: str, acnt_prdt_cd: str, code: str, qty: int, ord_dvsn: str = "01"
) -> dict:
    """주식 시장가 매도. TR_ID=TTTC0801U. ord_dvsn: 01=시장가(기본)"""
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    body = {
        "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd,
        "PDNO": code, "ORD_DVSN": ord_dvsn,
        "ORD_QTY": str(qty), "ORD_UNPR": "0",
    }
    headers = client._headers(tr_id="TTTC0801U")
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"매도 주문 실패: {j.get('msg1')} raw={j}")
    return j


def _cancel_order_generic(
    client, cano: str, acnt: str, ordno: str, code: str, qty: int, ord_dvsn: str = "01"
) -> None:
    """주문 취소. ORD_DVSN: 00=지정가, 01=시장가, 02=시간외종가, 06=시간외단일가."""
    if not ordno or qty <= 0:
        return
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-rvsecncl"
    body = {
        "CANO": cano, "ACNT_PRDT_CD": acnt, "KRX_FWDG_ORD_ORGNO": "00000",
        "ORGN_ORDNO": ordno, "ORD_DVSN": ord_dvsn, "RVSE_CNCL_DVSN_CD": "02",
        "ORD_QTY": str(qty), "ORD_UNPR": "0", "PDNO": code,
    }
    headers = client._headers(tr_id="TTTC0803U")
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"주문취소 실패: {j.get('msg1')} raw={j}")


def _cancel_closing_order(client, cano: str, acnt: str, ordno: str, code: str, qty: int) -> None:
    """15:20~15:30 종가매수 시장가(01) 주문 취소."""
    _cancel_order_generic(client, cano, acnt, ordno, code, qty, ord_dvsn="01")


def _inquire_psbl_rvsecncl(client, cano: str, acnt: str, tr_id: str = "TTTC0084R") -> list[dict]:
    """주식정정취소가능주문조회. 매수(SLL_BUY_DVSN_CD=00) 미체결 조회."""
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"
    headers = client._headers(tr_id=tr_id)
    params = {
        "CANO": cano, "ACNT_PRDT_CD": acnt,
        "INQR_DVSN": "00", "SLL_BUY_DVSN_CD": "00",
        "INQR_STRT_DT": "", "INQR_END_DT": "", "PDNO": "", "ORD_GNO_BRNO": "", "ODNO": "",
        "INQR_DVSN_1": "0", "INQR_DVSN_2": "0",
        "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
    }
    out: list[dict] = []
    for _ in range(20):
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        j = r.json()
        if str(j.get("rt_cd")) != "0":
            raise RuntimeError(f"정정취소가능주문조회 실패: {j.get('msg1')} raw={j}")
        output1 = j.get("output1") or []
        if isinstance(output1, dict):
            output1 = [output1]
        out.extend(output1)
        ctx_fk = j.get("ctx_area_fk100") or ""
        ctx_nk = j.get("ctx_area_nk100") or ""
        if not ctx_fk and not ctx_nk:
            break
        params["CTX_AREA_FK100"] = ctx_fk
        params["CTX_AREA_NK100"] = ctx_nk
    return out


def _run_open_buy_order_cancel_on_startup() -> None:
    """재시작 시 미체결 매수주문 전부 취소. 조회 → 현황 출력 → 취소 → 결과 출력."""
    if not OPEN_BUY_ORDER_CANCEL:
        return
    sys.stdout.write("\n")
    _notify(f"{ts_prefix()} [미체결취소] 매수주문 확인 중...")
    try:
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        if not cano:
            _notify(f"{ts_prefix()} [미체결취소] config cano 없음")
            return
        client = _top_client or _init_top_client()
        orders = _inquire_psbl_rvsecncl(client, cano, acnt)
        targets = [o for o in orders if int(float(str(o.get("psbl_qty") or o.get("PSBL_QTY") or 0)).replace(",", "") or 0) > 0]
        if not targets:
            _notify(f"{ts_prefix()} [미체결취소] 미체결 매수주문 없음")
            sys.stdout.write("\n")
            return
        lines = [f"{ts_prefix()} [미체결취소] 현황 {len(targets)}건"]
        for o in targets:
            code = str(o.get("pdno") or o.get("PDNO") or "").strip().zfill(6)
            qty = int(float(str(o.get("psbl_qty") or o.get("PSBL_QTY") or 0)).replace(",", "") or 0)
            name = code_name_map.get(code, code)
            lines.append(f"  {name}({code}) qty={qty}")
        _notify("\n".join(lines))
        _notify(f"{ts_prefix()} [미체결취소] 취소 요청 중...")
        n_ok, n_fail = 0, 0
        for o in targets:
            ordno = str(o.get("odno") or o.get("ODNO") or o.get("ordno") or "").strip()
            code = str(o.get("pdno") or o.get("PDNO") or "").strip().zfill(6)
            qty = int(float(str(o.get("psbl_qty") or o.get("PSBL_QTY") or o.get("ord_qty") or 0)).replace(",", "") or 0)
            if not ordno or not code or qty <= 0:
                continue
            ord_dvsn = str(o.get("ord_dvsn_cd") or o.get("ORD_DVSN_CD") or o.get("ord_dvsn") or "01").strip() or "01"
            try:
                _cancel_order_generic(client, cano, acnt, ordno, code, qty, ord_dvsn)
                n_ok += 1
                time.sleep(0.5)
            except Exception as e:
                n_fail += 1
                logger.warning(f"{ts_prefix()} [미체결취소실패] {code} ordno={ordno}: {e}")
        _notify(f"{ts_prefix()} [미체결취소] 완료: 성공={n_ok} 실패={n_fail}", tele=True)
        sys.stdout.write("\n")
    except Exception as e:
        _notify(f"{ts_prefix()} [미체결취소] 실패: {e}")
        logger.warning(f"{ts_prefix()} [미체결취소] 실패: {e}")


def _run_balance_0858() -> None:
    """08:58 계좌잔고조회 1회 실행, 결과 출력."""
    global _balance_0858_done
    if _balance_0858_done:
        return
    try:
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        if not cano:
            logger.warning(f"{ts_prefix()} [잔고0858] config에 cano 없음")
            return
        client = _init_top_client()
        _, out2, _, _ = _get_balance_page(client, cano, acnt, "TTTC8434R")
        sum_rows = [out2] if isinstance(out2, dict) else (out2 if isinstance(out2, list) and out2 else [])
        if sum_rows:
            item = sum_rows[0]
            dnca = float(str(item.get("dnca_tot_amt", 0) or 0).replace(",", "") or 0)
            ord_cash = float(str(item.get("prvs_rcdl_excc_amt", 0) or 0).replace(",", "") or 0)
            tot_eval = float(str(item.get("tot_evlu_amt", 0) or 0).replace(",", "") or 0)
            out = f"{ts_prefix()} [08:58 잔고조회] 출금가능={dnca:,.0f} 주문가능={ord_cash:,.0f} 총평가={tot_eval:,.0f}"
            sys.stdout.write("\n")
            _notify(out, tele=True)
            sys.stdout.write("\n\n")
            sys.stdout.flush()
        _balance_0858_done = True
    except Exception as e:
        logger.warning(f"{ts_prefix()} [잔고0858] 실패: {e}")


def _print_startup_balance() -> dict[str, int]:
    """
    프로그램 시작 시 계좌 잔고조회 → 상세 출력 → balance_map 반환.

    반환: {종목코드(6자리): 매도가능수량} — _load_str1_sell_state_on_startup에 직접 전달해 재조회 생략.
    실패 시 빈 dict 반환.
    """
    try:
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        if not cano:
            logger.warning(f"{ts_prefix()} [시작잔고] config에 cano 없음")
            return {}
        client = _init_top_client()

        hold_rows: list[dict] = []
        seen_codes: set[str] = set()
        summary: dict = {}
        ctx_fk, ctx_nk = "", ""
        for _ in range(10):
            out1, out2, ctx_fk, ctx_nk = _get_balance_page(client, cano, acnt, "TTTC8434R", ctx_fk, ctx_nk)
            rows = out1 if isinstance(out1, list) else ([out1] if isinstance(out1, dict) else [])
            if not rows:
                break
            for row in rows:
                c = str(row.get("pdno", "")).strip().zfill(6)
                if c and c != "000000" and c not in seen_codes:
                    seen_codes.add(c)
                    hold_rows.append(row)
            if not summary:
                summary = out2 if isinstance(out2, dict) else (out2[0] if isinstance(out2, list) and out2 else {})
            # 공백·빈 문자열 모두 처리
            if not ctx_fk.strip() and not ctx_nk.strip():
                break

        # ── 계좌 요약 ────────────────────────────────────────────────────────
        def _f(key: str) -> float:
            return float(str(summary.get(key, 0) or 0).replace(",", "") or 0)

        dnca      = _f("dnca_tot_amt")
        ord_cash  = _f("prvs_rcdl_excc_amt")
        tot_eval  = _f("tot_evlu_amt")
        buy_amt   = _f("thdt_buy_amt")
        sll_amt   = _f("thdt_sll_amt")
        eval_pfls = _f("evlu_pfls_smtm_amt")

        SEP  = "─" * 60
        now_str = datetime.now(KST).strftime("%H:%M:%S")
        lines: list[str] = [
            "",
            SEP,
            f"[시작 잔고조회] {now_str}",
            SEP,
            f"  출금가능      : {dnca:>15,.0f} 원",
            f"  주문가능      : {ord_cash:>15,.0f} 원",
            f"  총 평가금액   : {tot_eval:>15,.0f} 원",
            f"  평가손익합계  : {eval_pfls:>+15,.0f} 원",
            f"  당일매수금액  : {buy_amt:>15,.0f} 원",
            f"  당일매도금액  : {sll_amt:>15,.0f} 원",
        ]

        # ── 보유 종목 상세 ────────────────────────────────────────────────────
        hold_map: dict[str, int] = {}
        valid_rows = hold_rows  # 이미 pdno 기준 중복 제거됨

        if valid_rows:
            lines.append(SEP)
            lines.append(f"  보유종목 {len(valid_rows)}건")
            hdr = f"  {'코드':^6}  {'종목명':<12}  {'보유':>5}  {'매도가능':>6}  {'매입단가':>9}  {'현재단가':>9}  {'평가금액':>12}  {'평가손익':>12}  {'수익률':>7}"
            lines.append(hdr)
            lines.append("  " + "─" * 88)
            for r in valid_rows:
                code   = str(r.get("pdno", "")).strip().zfill(6)
                _raw_nm = str(r.get("hts_kor_isnm", "") or "").strip()
                if not _raw_nm or _raw_nm == code:
                    # API가 종목명을 반환하지 않으면 로컬 KRX 매핑에서 보완
                    _raw_nm = code_name_map.get(code, code)
                name = _raw_nm[:12]
                hldg   = int(float(str(r.get("hldg_qty", 0) or 0).replace(",", "") or 0))
                psbl   = int(float(str(r.get("ord_psbl_qty", 0) or 0).replace(",", "") or 0))
                pchs   = float(str(r.get("pchs_avg_pric", 0) or 0).replace(",", "") or 0)
                prpr   = float(str(r.get("prpr", 0) or 0).replace(",", "") or 0)
                evlu   = float(str(r.get("evlu_amt", 0) or 0).replace(",", "") or 0)
                pfls   = float(str(r.get("evlu_pfls_amt", 0) or 0).replace(",", "") or 0)
                rt     = float(str(r.get("evlu_pfls_rt", 0) or 0).replace(",", "") or 0)
                # prpr이 0이거나 evlu_amt와 같으면(단가가 아닌 총금액일 경우) evlu÷hldg로 보정
                if prpr == 0 and evlu > 0 and hldg > 0:
                    prpr = evlu / hldg
                elif prpr > 0 and hldg > 1 and abs(prpr - evlu) < 1:
                    prpr = evlu / hldg
                qty    = psbl if psbl > 0 else hldg
                hold_map[code] = qty
                lines.append(
                    f"  {code}  {name:<12}  {hldg:>5,}  {psbl:>6,}  {pchs:>9,.0f}  {prpr:>9,.0f}  {evlu:>12,.0f}  {pfls:>+12,.0f}  {rt:>+7.2f}%"
                )
        else:
            lines.append(SEP)
            lines.append("  보유종목 없음")

        lines.append(SEP)
        lines.append("")
        full_msg = "\n".join(lines)
        sys.stdout.write(full_msg)
        sys.stdout.flush()
        logger.info(full_msg)

        # 텔레그램 전용 요약 (stdout 출력 없이 텔레그램만 전송)
        tele_lines = [
            f"[시작잔고 {now_str}]",
            f"출금가능 {dnca:,.0f}  주문가능 {ord_cash:,.0f}",
            f"총평가 {tot_eval:,.0f}  평가손익 {eval_pfls:+,.0f}",
        ]
        if valid_rows:
            tele_lines.append(f"보유 {len(valid_rows)}종목:")
            for r in valid_rows:
                code  = str(r.get("pdno", "")).strip().zfill(6)
                _tnm  = str(r.get("hts_kor_isnm", "") or "").strip()
                if not _tnm or _tnm == code:
                    _tnm = code_name_map.get(code, code)
                psbl  = int(float(str(r.get("ord_psbl_qty", 0) or 0).replace(",", "") or 0))
                hldg  = int(float(str(r.get("hldg_qty", 0) or 0).replace(",", "") or 0))
                rt    = float(str(r.get("evlu_pfls_rt", 0) or 0).replace(",", "") or 0)
                tele_lines.append(f"  {_tnm}({code}) 보유={hldg} 매도가능={psbl} 수익={rt:+.2f}%")
        # stdout 출력 없이 텔레그램만 전송
        if tmsg is not None:
            try:
                tmsg("\n".join(tele_lines), "-t")
            except Exception:
                pass

        return hold_map

    except Exception as e:
        logger.warning(f"{ts_prefix()} [시작잔고] 조회 실패: {e}")
        _notify(f"{ts_prefix()} [시작잔고] 조회 실패: {e}", tele=True)
        return {}


def _get_ccnl_notice_tr_key() -> str:
    """H0STCNI0 체결통보 구독용 tr_key. 공식: my_htsid (HTS 로그인 ID). kis_devlp.yaml의 my_htsid 확인 필요."""
    trenv = ka.getTREnv()
    h = getattr(trenv, "my_htsid", "") if trenv else ""
    if not h:
        h = ka.getEnv().get("my_htsid", "")
    return (h or "").strip()


def _closing_buy_state_path() -> Path:
    return TOP_RANK_OUT_DIR / f"closing_buy_state_{today_yymmdd}.json"


def _closing_filled_path(yymmdd: str | None = None) -> Path:
    return TOP_RANK_OUT_DIR / f"closing_filled_{yymmdd or today_yymmdd}.json"


def _save_closing_filled() -> None:
    """체결통보 수신 시 _closing_filled_by_code를 파일로 저장 (재시작 복원용)."""
    global _closing_filled_by_code
    if not _closing_filled_by_code:
        return
    path = _closing_filled_path()
    TOP_RANK_OUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({str(k).zfill(6): v for k, v in _closing_filled_by_code.items()}, f)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가매수] 체결 저장 실패: {e}")


def _load_closing_filled(yymmdd: str | None = None) -> dict[str, int]:
    """저장된 체결 수량 로드 (재시작 시 remain 계산용)."""
    path = _closing_filled_path(yymmdd or today_yymmdd)
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {str(k).zfill(6): int(v) for k, v in data.items() if v is not None}
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가매수] 체결 로드 실패: {e}")
        return {}


def _closing_exp_first_tick_path() -> Path:
    """15:20 첫 예상체결가 저장 (재시작 시 모니터링 기준값 복원용)."""
    return TOP_RANK_OUT_DIR / f"closing_exp_first_tick_{today_yymmdd}.json"


def _save_exp_first_tick() -> None:
    """_closing_exp_first_tick을 파일에 저장. 재시작 시 로드하여 모니터링 기준 복원."""
    data = dict(_closing_exp_first_tick)
    if not data:
        return
    path = _closing_exp_first_tick_path()
    TOP_RANK_OUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가모니터] 첫틱 저장 실패: {e}")


def _load_exp_first_tick_from_wss(codes: list[str]) -> dict[str, float]:
    """
    _accumulator (인메모리 누적) 에서 15:20~15:30 예상체결(is_real_ccnl=N) 첫 틱 조회.
    closing_buy_state codes 대상.
    """
    if not codes:
        return {}
    codes_set = {str(c).zfill(6) for c in codes}
    today_ymd = datetime.now(KST).strftime("%Y-%m-%d")
    ts_min = f"{today_ymd} 15:20:00"
    ts_max = f"{today_ymd} 15:30:00"
    result: dict[str, float] = {}
    try:
        with _accumulator_lock:
            if not _accumulator:
                return {}
            df = pl.concat(_accumulator, how="diagonal_relaxed")

        # 코드 컬럼 탐색
        code_col = next(
            (c for c in ("mksc_shrn_iscd", "stck_shrn_iscd", "code") if c in df.columns), None
        )
        # H0STANC0(예상체결) 스키마: stck_prpr 우선
        pr_col = "stck_prpr" if "stck_prpr" in df.columns else "antc_prce"
        if not code_col or pr_col not in df.columns or "recv_ts" not in df.columns:
            return {}

        df = df.with_columns(
            pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias("_code"),
            pl.col("recv_ts").cast(pl.Utf8).alias("_ts"),
            pl.col(pr_col).cast(pl.Float64, strict=False).alias("_pr"),
        ).filter(
            pl.col("_code").is_in(codes_set)
            & (pl.col("_ts").str.slice(0, 19) >= ts_min)
            & (pl.col("_ts").str.slice(0, 19) < ts_max)
        )
        if "is_real_ccnl" in df.columns:
            df = df.filter(pl.col("is_real_ccnl").cast(pl.Utf8).str.strip_chars() == "N")

        if df.is_empty():
            return {}

        df = df.sort("_ts").group_by("_code").first()
        for row in df.iter_rows(named=True):
            c = str(row["_code"]).zfill(6)
            if c in codes_set:
                try:
                    p = float(row["_pr"] or 0)
                    if p > 0:
                        result[c] = p
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가모니터] WSS accumulator 첫틱 조회 실패: {e}")
    return result


def _load_exp_first_tick(codes: list[str] | None = None) -> dict[str, float]:
    """
    재시작 시 15:20 첫 예상체결가 로드.
    1) JSON(즉시저장) → 2) WSS parquet(보조, codes 미충족 시)
    """
    out: dict[str, float] = {}
    path = _closing_exp_first_tick_path()
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            out = {str(k).zfill(6): float(v) for k, v in data.items() if v is not None}
        except Exception as e:
            logger.warning(f"{ts_prefix()} [종가모니터] 첫틱 JSON 로드 실패: {e}")
    if codes:
        need = {str(c).zfill(6) for c in codes} - set(out.keys())
        if need:
            from_wss = _load_exp_first_tick_from_wss(list(need))
            out.update(from_wss)
            if from_wss:
                logger.info(f"{ts_prefix()} [종가모니터] WSS parquet에서 첫틱 {len(from_wss)}건 보완")
    return out


def _save_closing_buy_state(
    codes: list[str],
    code_info: dict[str, dict],
    orders: list[dict] | None = None,
) -> None:
    """15:19 선정 목록 + 주문 결과 저장."""
    path = _closing_buy_state_path()
    TOP_RANK_OUT_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "date": today_yymmdd,
        "selected_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "codes": list(codes),
        "code_info": {k: dict(v) for k, v in code_info.items()},
        "orders": orders or [],
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가매수] state 저장 실패: {e}")


def _save_str1_sell_state() -> None:
    """_str1_sell_state를 closing_buy_state_{yymmdd}.json 의 sell_state 필드에 저장."""
    if not _str1_sell_state:
        return
    path = _closing_buy_state_path()
    if not path.exists():
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        with _str1_sell_state_lock:
            data["sell_state"] = {k: dict(v) for k, v in _str1_sell_state.items()}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [str1_sell] state 저장 실패: {e}")


def _get_balance_holdings() -> dict[str, int]:
    """
    KIS API 잔고조회(TTTC8434R)로 실제 보유 종목·수량 반환.
    반환: {종목코드(6자리): hldg_qty(보유수량)}
    실패 시 빈 dict 반환 (호출부에서 JSON state로 fallback).
    """
    try:
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        if not cano:
            return {}
        client = _top_client or _init_top_client()
        hold_map: dict[str, int] = {}
        ctx_fk, ctx_nk = "", ""
        for _ in range(10):   # 페이지네이션 최대 10페이지
            out1, _, ctx_fk, ctx_nk = _get_balance_page(client, cano, acnt, "TTTC8434R", ctx_fk, ctx_nk)
            rows = out1 if isinstance(out1, list) else ([out1] if isinstance(out1, dict) else [])
            if not rows:
                break
            for row in rows:
                c = str(row.get("pdno", "") or "").strip().zfill(6)
                if not c or c == "000000":
                    continue
                qty = int(float(str(row.get("hldg_qty", 0) or 0).replace(",", "") or 0))
                # 매도가능수량(ord_psbl_qty)이 있으면 우선 사용 (미결제 물량 제외)
                psbl = int(float(str(row.get("ord_psbl_qty", 0) or 0).replace(",", "") or 0))
                if c not in hold_map:   # 중복 방지 (첫 페이지 우선)
                    hold_map[c] = psbl if psbl > 0 else qty
            if not ctx_fk.strip() and not ctx_nk.strip():
                break
        return hold_map
    except Exception as e:
        logger.warning(f"{ts_prefix()} [str1_sell] 잔고조회 실패: {e}")
        return {}


def _load_str1_sell_state_on_startup(balance_map: dict[str, int] | None = None) -> None:
    """
    프로그램 시작 시 어제(또는 당일) closing_buy_state에서 매도 상태 복원.

    1) JSON state의 filled_qty로 1차 수량 산정
    2) KIS API 잔고조회로 실제 보유수량 검증
       - balance_map이 전달된 경우: 재조회 없이 그대로 사용 (시작 시 이미 조회했으므로)
       - balance_map이 None인 경우: _get_balance_holdings() 재호출
       - 잔고에 있는 종목: 실제 보유수량을 사용 (JSON보다 정확)
       - 잔고에 없는 종목(qty=0): 이미 매도됐거나 미체결 → 등록 스킵
    3) 미매도 포지션을 _str1_sell_state에 등록, 구독 목록에도 추가
    """
    global _str1_sell_state
    if not STR1_SELL_ENABLED or not CLOSE_BUY:
        return

    # 당일 state 우선, 없으면 전일 state
    state = _load_closing_buy_state()
    if not state or not state.get("orders"):
        from datetime import timedelta
        yesterday = (datetime.now(KST) - timedelta(days=1)).strftime("%y%m%d")
        state = _load_closing_buy_state_by_date(yesterday)

    filled_map    = _get_closing_filled_for_remain(state) if state and state.get("orders") else {}
    existing_sell = state.get("sell_state") or {} if state else {}
    code_info     = state.get("code_info") or {} if state else {}

    # ── 잔고조회: 시작 시 전달받은 balance_map 우선 사용, 없으면 재조회 ──
    if balance_map is not None:
        logger.info(f"[str1_sell] 시작잔고 재사용: {len(balance_map)}종목")
    else:
        _notify(f"{ts_prefix()} [str1_sell] 잔고조회 시작 (보유수량 검증)…")
        balance_map = _get_balance_holdings()
        if balance_map:
            logger.info(f"[str1_sell] 잔고조회 완료: {len(balance_map)}종목 {balance_map}")
        else:
            logger.warning("[str1_sell] 잔고조회 실패 → JSON state filled_qty로 fallback")

    # 잔고에 아무것도 없으면 (빈 dict) → 포지션 없음 처리
    if not balance_map:
        _notify(f"{ts_prefix()} [str1_sell] 보유종목 없음 → 매도 모니터링 대상 없음")
        return

    # closing_buy_state orders가 없어도, 잔고에 있는 종목은 최소한 등록 시도
    if not state or not state.get("orders"):
        logger.info("[str1_sell] closing_buy_state 없음 → 잔고 보유 종목만으로 sell_state 구성")
        # orders가 없으므로 balance_map 종목 전부를 대상으로 등록
        added_for_sell: list[str] = []
        with _str1_sell_state_lock:
            for code, qty in balance_map.items():
                if qty <= 0:
                    continue
                name = code_name_map.get(code, code)
                _str1_sell_state[code] = {
                    "sold": False, "sell_reason": "", "qty": qty,
                    "sell_price": 0.0, "sell_time": "", "ordno": "",
                    "pnl": 0.0, "ret_pct": 0.0,
                    "buy_price": 0.0,  # 첫 틱 수신 시 prdy_ctrt로 보정
                    "name": name, "premarket_ordered": False,
                    "balance_qty": qty,
                }
                added_for_sell.append(code)
        if added_for_sell:
            added_subs = _ensure_code_structs(added_for_sell)
            with _lock:
                # watchdog/top_rank_loop 에 의한 구독 해제 방지: _base_codes 에 추가
                _base_codes.update(added_for_sell)
                for c in added_for_sell:
                    _top_added_codes.discard(c)
            lines = [f"[str1_sell] 잔고 기반 포지션 등록 {len(added_for_sell)}건"]
            for c in added_for_sell:
                st = _str1_sell_state.get(c, {})
                lines.append(f"  {st.get('name',c)}({c}) 수량={st.get('qty',0)}")
            if added_subs:
                lines.append(f"  구독 추가: {added_subs}")
            msg = "\n".join(lines)
            _notify(msg, tele=True)
            logger.info(msg)
        return

    added_for_sell: list[str] = []
    skipped_zero:   list[str] = []

    with _str1_sell_state_lock:
        for o in state["orders"]:
            code = str(o.get("code", "")).zfill(6)
            if not code:
                continue

            # ① 이미 매도 완료된 종목: 상태 복원만
            if code in existing_sell and existing_sell[code].get("sold"):
                _str1_sell_state[code] = dict(existing_sell[code])
                continue

            # ② 실제 보유수량 결정
            #    우선순위: 잔고조회 > JSON filled_qty
            if balance_map:
                actual_qty = balance_map.get(code, 0)
            else:
                order_qty  = int(o.get("order_qty", o.get("qty", 0)))
                actual_qty = min(order_qty, max(int(o.get("filled_qty", 0)), filled_map.get(code, 0)))

            if actual_qty <= 0:
                # 잔고 없음 → 이미 매도됐거나 미체결
                skipped_zero.append(code)
                continue

            # ③ 미매도 포지션 등록
            info       = code_info.get(code, {})
            prev_close = float(info.get("prev_close") or 0)
            name       = info.get("name") or code_name_map.get(code, code)
            _str1_sell_state[code] = {
                "sold":               False,
                "sell_reason":        "",
                "qty":                actual_qty,
                "sell_price":         0.0,
                "sell_time":          "",
                "ordno":              "",
                "pnl":                0.0,
                "ret_pct":            0.0,
                "buy_price":          prev_close,   # 첫 틱 수신 시 prdy_ctrt로 보정
                "name":               name,
                "premarket_ordered":  False,
                "balance_qty":        actual_qty,   # 잔고조회 원본 수량 (기록용)
            }
            added_for_sell.append(code)

    if skipped_zero:
        logger.info(f"[str1_sell] 잔고 없어 스킵(미체결 or 기매도): {skipped_zero}")

    if added_for_sell:
        added_subs = _ensure_code_structs(added_for_sell)
        with _lock:
            # watchdog/top_rank_loop 에 의한 구독 해제 방지: _base_codes 에 추가
            _base_codes.update(added_for_sell)
            for c in added_for_sell:
                _top_added_codes.discard(c)
        lines = [f"[str1_sell] 미매도 포지션 복원 {len(added_for_sell)}건"]
        for c in added_for_sell:
            st = _str1_sell_state.get(c, {})
            lines.append(f"  {st.get('name',c)}({c}) 수량={st.get('qty',0)}")
        if added_subs:
            lines.append(f"  구독 추가: {added_subs}")
        msg = "\n".join(lines)
        _notify(msg, tele=True)
        logger.info(msg)


def _str1_sell_worker() -> None:
    """
    별도 스레드: _str1_sell_queue에서 매도/취소 요청을 꺼내 KIS API로 처리.
    - action=="sell": 매도 주문
    - action=="cancel": 장전 매도 주문 취소 (09:00 전까지)
    ingest_loop(hot path)과 분리하여 API 지연이 틱 처리를 막지 않도록 함.
    """
    logger.info("[str1_sell] worker started")
    while not _stop_event.is_set() or not _str1_sell_queue.empty():
        try:
            req = _str1_sell_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        if req is None:
            break
        action = req.get("action", "sell")
        code   = req["code"]
        name   = req.get("name", code)

        if action == "cancel":
            # ── 장전 매도 주문 취소 ──
            qty   = int(req.get("qty", 0))
            ordno = str(req.get("ordno", "")).strip()
            reason = req.get("reason", "")
            try:
                if not ordno or qty <= 0:
                    logger.warning(f"[str1_sell] 취소 스킵 {code}: ordno/수량 없음")
                    continue
                cfg = _read_cfg() or load_config(str(CONFIG_PATH))
                cano = str(cfg.get("cano", "")).strip()
                acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
                if not cano:
                    logger.warning(f"[str1_sell] config cano 없음 → {code} 취소 스킵")
                    continue
                client = _top_client or _init_top_client()
                _cancel_order_generic(client, cano, acnt, ordno, code, qty, ord_dvsn="01")
                with _str1_sell_state_lock:
                    st = _str1_sell_state.get(code, {})
                    _str1_sell_state[code] = {
                        **st,
                        "premarket_ordered": False,
                        "premarket_cancel_requested": False,
                        "ordno": "",
                    }
                    _premarket_cancelled_log.append((code, name, reason))
                _save_str1_sell_state()
                msg = f"{ts_prefix()} [str1_sell] 장전매도취소 {name}({code})  사유={reason}"
                _notify(msg, tele=True)
                logger.info(msg)
            except Exception as e:
                logger.error(f"[str1_sell] 장전매도취소 실패 {code}: {e}")
                _notify(f"{ts_prefix()} [str1_sell] 장전매도취소실패 {name}({code}) err={e}", tele=True)
                with _str1_sell_state_lock:
                    if code in _str1_sell_state:
                        _str1_sell_state[code]["premarket_cancel_requested"] = False
            continue

        # ── 매도 주문 ──
        qty       = int(req.get("qty", 0))
        reason    = req.get("reason", "")
        ref_price = req.get("ref_price", 0.0)
        try:
            cfg = _read_cfg() or load_config(str(CONFIG_PATH))
            cano = str(cfg.get("cano", "")).strip()
            acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
            if not cano:
                logger.warning(f"[str1_sell] config cano 없음 → {code} 매도 스킵")
                continue
            client = _top_client or _init_top_client()
            j = _sell_order_cash(client, cano, acnt, code, qty, ord_dvsn="01")
            out = j.get("output") or {}
            ordno = str(out.get("ODNO") or out.get("odno") or "").strip()

            is_premarket = (reason == "str1_시초가하락_예상")
            pnl_info = calc_sell_pnl(ref_price, ref_price, qty) if ref_price > 0 else {}
            with _str1_sell_state_lock:
                st = _str1_sell_state.get(code, {})
                buy_price = st.get("buy_price") or ref_price
                sell_ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
                new_st = {
                    **st,
                    "sell_reason": reason,
                    "sell_price": ref_price,
                    "ordno": ordno,
                }
                if is_premarket:
                    # 장전 매도: 09:00 시초가에 체결 예정 → sold=False, premarket_ordered=True
                    # 이후 예상가 복귀 시 취소 가능
                    new_st["premarket_ordered"] = True
                    new_st["sold"] = False
                else:
                    new_st["sold"] = True
                    new_st["sell_time"] = sell_ts
                    new_st["pnl"] = pnl_info.get("pnl", 0.0)
                    new_st["ret_pct"] = pnl_info.get("ret_pct", 0.0)
                _str1_sell_state[code] = new_st
            _save_str1_sell_state()

            pnl_txt = f"  PNL≈{pnl_info.get('pnl', 0):+,.0f}원  ret≈{pnl_info.get('ret_pct', 0):+.2f}%" if pnl_info else ""
            pm_suffix = " (09:00 시초가 체결 예정, 복귀 시 취소)" if is_premarket else ""
            msg = (
                f"{ts_prefix()} [str1_sell] 매도주문 {name}({code})"
                f"  사유={reason}  수량={qty}  기준가={ref_price:,.0f}{pnl_txt}{pm_suffix}"
            )
            _notify(msg, tele=True)
            logger.info(msg)
            # 거래장부 기록
            with _str1_sell_state_lock:
                bp = _str1_sell_state.get(code, {}).get("buy_price", 0.0)
            _append_ledger(
                order_type="SELL_ORDER",
                code=code, name=name,
                buy_price=float(bp or 0),
                sell_price=float(ref_price or 0),
                order_qty=qty,
                reason=reason,
                ord_no=ordno,
                ord_dvsn="01(시장가)",
                prdy_ctrt=float(_last_prdy_ctrt.get(code, 0.0)),
            )

        except Exception as e:
            logger.error(f"[str1_sell] 매도주문 실패 {code}: {e}")
            _notify(f"{ts_prefix()} [str1_sell] 매도실패 {name}({code}) 사유={reason} err={e}", tele=True)

    logger.info("[str1_sell] worker stopped")


# 15:18 조건부 전체 매도 — 1일 1회 실행 플래그
_sell_1518_done: bool = False


def _run_sell_1518() -> None:
    """
    15:18 조건부 전체 매도.

    - _str1_sell_state 에 등록된 미매도 포지션을 전부 점검
    - 전일대비 28% 미만 (prdy_ctrt < 28) 종목은 즉시 시장가 매도
    - 상한가(prdy_ctrt >= 28) 종목은 유지
    - 하루 1회만 실행
    """
    global _sell_1518_done
    if _sell_1518_done:
        return
    if not STR1_SELL_ENABLED or not _str1_sell_state:
        _sell_1518_done = True
        return

    KEEP_THRESHOLD = 28.0   # 이 값 이상이면 유지
    sell_list: list[str] = []
    keep_list: list[str] = []

    with _str1_sell_state_lock:
        for code, st in _str1_sell_state.items():
            if st.get("sold"):
                continue
            prdy = float(_last_prdy_ctrt.get(code, 0.0))
            if prdy >= KEEP_THRESHOLD:
                keep_list.append(f"{code_name_map.get(code, code)}({code}) {prdy:+.1f}%")
            else:
                sell_list.append((code, prdy))

    lines = [f"[15:18 조건부매도] 전일대비 {KEEP_THRESHOLD}% 미만 → 매도 {len(sell_list)}건 / 유지 {len(keep_list)}건"]
    if keep_list:
        lines.append(f"  유지: {', '.join(keep_list)}")
    _notify("\n".join(lines), tele=True)
    logger.info("\n".join(lines))
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()

    for code, prdy in sell_list:
        reason = f"15:18조건부매도(prdy_ctrt={prdy:+.1f}%)"
        ref = float(_last_prdy_ctrt.get(code, 0.0))
        # 현재가 추정: prdy_ctrt 역산 (없으면 0으로 시장가)
        with _str1_sell_state_lock:
            bp = float(_str1_sell_state.get(code, {}).get("buy_price", 0) or 0)
        ref_price = bp * (1 + prdy / 100) if bp > 0 and abs(prdy) < 100 else 0.0
        _enqueue_str1_sell(code, reason, ref_price)

    _sell_1518_done = True


def _enqueue_str1_sell(code: str, reason: str, ref_price: float) -> None:
    """ingest_loop에서 호출: 매도 조건 충족 시 sell worker에 비동기 주문 요청."""
    with _str1_sell_state_lock:
        st = _str1_sell_state.get(code)
        if not st or st.get("sold"):
            return
        if reason == "str1_시초가하락_예상" and st.get("premarket_ordered"):
            return  # 사전 매도 주문 중복 방지
        qty  = int(st.get("qty") or 0)
        name = st.get("name", code)
        if qty <= 0:
            return
        # 상태 선점 (중복 큐잉 방지)
        if reason == "str1_시초가하락_예상":
            _str1_sell_state[code]["premarket_ordered"] = True
        else:
            _str1_sell_state[code]["sold"] = True  # 낙관적 선점
    _str1_sell_queue.put({
        "action": "sell",
        "code": code, "qty": qty, "reason": reason,
        "ref_price": ref_price, "name": name,
    })


_periodic_sell_check_ts: float = 0.0   # 마지막 주기적 매도 체크 시각
PERIODIC_SELL_CHECK_SEC: float = 15.0  # 체크 주기 (초)


def _check_sell_by_prdy_ctrt_periodic() -> None:
    """
    틱 수신과 무관하게 주기적으로 _last_prdy_ctrt 캐시를 확인해
    전일종가 이하 하락 포지션을 즉시 시장가 매도.

    - _check_str1_sell_conditions 는 틱이 도달해야만 호출됨 → 저거래량 종목 누락 보완
    - scheduler_loop 에서 15초마다 호출
    - 정규장(09:00~15:20) 시간대에만 동작
    """
    global _periodic_sell_check_ts
    if not STR1_SELL_ENABLED or not _str1_sell_state:
        return

    now_t = datetime.now(KST).time()
    if not (dtime(9, 0) <= now_t < dtime(15, 20)):
        return

    if (time.time() - _periodic_sell_check_ts) < PERIODIC_SELL_CHECK_SEC:
        return
    _periodic_sell_check_ts = time.time()

    sold_list: list[str] = []
    with _str1_sell_state_lock:
        targets = [
            (code, dict(st))
            for code, st in _str1_sell_state.items()
            if not st.get("sold") and not st.get("premarket_ordered")
        ]

    for code, st in targets:
        prdy = _last_prdy_ctrt.get(code, 999.0)  # 캐시 없으면 체크 스킵
        if prdy >= 999.0:
            continue
        if prdy < 0:
            bp = float(st.get("buy_price") or 0)
            ref_price = bp * (1 + prdy / 100) if bp > 0 and abs(prdy) < 100 else 0.0
            reason = f"전일종가하락-주기체크(prdy_ctrt={prdy:+.2f}%)"
            _enqueue_str1_sell(code, reason, ref_price)
            sold_list.append(f"{st.get('name', code)}({code}) {prdy:+.1f}%")

    if sold_list:
        msg = f"{ts_prefix()} [주기매도체크] 전일종가하락 매도 {len(sold_list)}건: {', '.join(sold_list)}"
        _notify(msg, tele=True)
        logger.info(msg)


def _enqueue_str1_premarket_cancel(code: str, reason: str) -> None:
    """ingest_loop에서 호출: 장전 매도 주문 후 예상가 복귀 시 취소 요청."""
    with _str1_sell_state_lock:
        st = _str1_sell_state.get(code)
        if not st or st.get("sold"):
            return
        if not st.get("premarket_ordered"):
            return
        ordno = str(st.get("ordno", "")).strip()
        if not ordno:
            return  # worker가 아직 주문 처리 전
        if st.get("premarket_cancel_requested"):
            return  # 중복 취소 방지
        qty = int(st.get("qty") or 0)
        name = st.get("name", code)
        if qty <= 0:
            return
        _str1_sell_state[code]["premarket_cancel_requested"] = True
    _str1_sell_queue.put({
        "action": "cancel",
        "code": code, "qty": qty, "ordno": ordno, "reason": reason, "name": name,
    })


def _write_ccnl_notice_log(df, recv_ts: str) -> None:
    """H0STCNI0 체결통보 수신 데이터를 별도 로그 파일에 저장."""
    try:
        with open(CCNL_NOTICE_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"\n=== {recv_ts} tr_id=H0STCNI0 ===\n")
            f.write(df.to_string())
            f.write("\n")
    except Exception as e:
        logger.warning(f"{ts_prefix()} [체결통보로그] 저장 실패: {e}")


_closing_filled_by_code: dict[str, int] = {}  # code -> 체결 누적 수량 (H0STCNI0 체결 시 갱신)

# ── Str1 실전 매도 상태 ──────────────────────────────────────────────────────
# code → {"sold": bool, "sell_reason": str, "qty": int, "sell_price": float,
#          "sell_time": str, "ordno": str, "pnl": float, "ret_pct": float,
#          "buy_price": float, "premarket_ordered": bool}
_str1_sell_state: dict[str, dict] = {}
_str1_sell_state_lock = threading.Lock()
# 매도 주문 큐: ingest_loop → sell_worker (비동기 처리)
_str1_sell_queue: "queue.Queue[dict]" = queue.Queue()

# ── 장전 동시호가 모니터링 ──────────────────────────────────────────────────
# code → {antc_prce, prdy_ctrt, buy_price, in_sell_cond, first_sell_ts, first_printed}
_premarket_watch: dict[str, dict] = {}
_premarket_last_summary_ts: float = 0.0   # 1분 주기 출력용
PREMARKET_SUMMARY_INTERVAL = 60.0         # 주기적 출력 간격 (초)
_premarket_order_summary_done: bool = False  # 08:59:50 주문현황 정리 1회 출력용
_premarket_cancelled_log: list[tuple[str, str, str]] = []  # (code, name, reason) 장전 취소 완료


def _on_ccnl_notice_filled(df, recv_ts: str) -> None:
    """H0STCNI0 체결통보(CNTG_YN==2) 수신 시 filled_qty 갱신 및 state 저장."""
    col = {str(c).strip().lower(): c for c in df.columns}
    cntg_yn_col = col.get("cntg_yn") or col.get("CNTG_YN")
    code_col = col.get("stck_shrn_iscd") or col.get("STCK_SHRN_ISCD")
    cntg_qty_col = col.get("cntg_qty") or col.get("CNTG_QTY")
    seln_col = col.get("seln_byov_cls") or col.get("SELN_BYOV_CLS")  # 매도/매수 구분
    if not all([cntg_yn_col, code_col, cntg_qty_col]):
        return
    for _, row in df.iterrows():
        try:
            cntg_yn = str(row.get(cntg_yn_col, "")).strip()
            if cntg_yn != "2":  # 2=체결통보
                continue
            code = str(row.get(code_col, "")).strip().zfill(6)
            if not code:
                continue
            qty = int(float(str(row.get(cntg_qty_col, 0) or 0).replace(",", "") or 0))
            if qty <= 0:
                continue
            # 매수/매도 체결 누적
            seln = str(row.get(seln_col, "")).strip()
            # 체결 단가
            fill_pr_col = col.get("cntg_unpr") or col.get("stck_prpr") or col.get("CNTG_UNPR")
            fill_pr = 0.0
            if fill_pr_col:
                try:
                    fill_pr = float(str(row.get(fill_pr_col, 0) or 0).replace(",", "") or 0)
                except Exception:
                    pass
            name_fill = code_name_map.get(code, "") or code

            if seln in ("01", "1"):  # 매도
                _closing_filled_by_code[code] = max(0, _closing_filled_by_code.get(code, 0) - qty)
                # Str1 장전 매도 체결 시 sold=True 전환
                with _str1_sell_state_lock:
                    st = _str1_sell_state.get(code)
                    if st and st.get("premarket_ordered") and not st.get("sold"):
                        _str1_sell_state[code]["sold"] = True
                        _str1_sell_state[code]["sell_reason"] = st.get("sell_reason") or "str1_시초가하락_예상"
                        logger.info(f"{ts_prefix()} [체결통보] Str1 장전매도 체결 {code} 수량={qty}")
                        _save_str1_sell_state()
                        fill_msg = f"{ts_prefix()} [체결통보] Str1 장전매도 체결 {name_fill}({code}) 수량={qty}주"
                        _notify(fill_msg, tele=True)
                        sys.stdout.write(f"\n{fill_msg}\n")
                        sys.stdout.flush()
                # 거래장부: 매도 체결
                with _str1_sell_state_lock:
                    st2 = _str1_sell_state.get(code, {})
                    bp2 = float(st2.get("buy_price") or 0)
                    sr2 = st2.get("sell_reason", "")
                _append_ledger(
                    order_type="SELL_FILL",
                    code=code, name=name_fill,
                    buy_price=bp2, sell_price=fill_pr,
                    fill_qty=qty,
                    reason=sr2,
                    prdy_ctrt=float(_last_prdy_ctrt.get(code, 0.0)),
                    note=f"체결통보 {recv_ts}",
                )
            else:  # 매수
                _closing_filled_by_code[code] = _closing_filled_by_code.get(code, 0) + qty
                # 거래장부: 매수 체결
                _append_ledger(
                    order_type="BUY_FILL",
                    code=code, name=name_fill,
                    buy_price=fill_pr,
                    fill_qty=qty,
                    prdy_ctrt=float(_last_prdy_ctrt.get(code, 0.0)),
                    note=f"체결통보 {recv_ts}",
                )
            # state 갱신
            state = _load_closing_buy_state()
            if state and state.get("orders"):
                orders = []
                for o in state["orders"]:
                    c = str(o.get("code", "")).zfill(6)
                    order_qty = int(o.get("order_qty", o.get("qty", 0)))
                    filled = min(order_qty, max(0, _closing_filled_by_code.get(c, 0)))
                    remain = max(0, order_qty - filled)
                    o2 = dict(o)
                    o2["filled_qty"] = filled
                    o2["remain_qty"] = remain
                    orders.append(o2)
                _save_closing_buy_state(state["codes"], state.get("code_info") or {}, orders)
                _save_closing_filled()
        except Exception as e:
            logger.warning(f"{ts_prefix()} [체결통보] 파싱 오류: {e}")


def _load_closing_buy_state_by_date(yymmdd: str) -> dict | None:
    """지정일 closing_buy_state 로드."""
    path = TOP_RANK_OUT_DIR / f"closing_buy_state_{yymmdd}.json"
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가매수] state 로드 실패 ({yymmdd}): {e}")
        return None


def _load_closing_buy_state() -> dict | None:
    """당일 closing_buy_state 로드."""
    path = _closing_buy_state_path()
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가매수] state 로드 실패: {e}")
        return None


def _run_closing_buy_retry_on_startup() -> None:
    """15:20~15:30 재시작 시 미처리 주문 재시도."""
    if not CLOSE_BUY:
        return
    now = datetime.now(KST).time()
    if not (dtime(15, 20) <= now < dtime(15, 31)):
        return
    state = _load_closing_buy_state()
    if not state or not state.get("codes"):
        return
    orders = state.get("orders") or []
    ordered_codes = {o["code"] for o in orders if o.get("status") == "ordered"}
    pending = [c for c in state["codes"] if c not in ordered_codes]
    if not pending:
        return
    global _closing_codes, _closing_code_info
    _closing_codes.clear()
    _closing_codes.extend(pending)
    _closing_code_info.clear()
    _closing_code_info.update(state.get("code_info") or {})
    # 재시작 시 15:20 첫 틱 복원: JSON → WSS parquet(보조)
    loaded_first = _load_exp_first_tick(codes=state["codes"])
    if loaded_first:
        _closing_exp_first_tick.update(loaded_first)
        logger.info(f"{ts_prefix()} [종가매수] 첫틱 복원 {len(loaded_first)}건")
    _notify(f"{ts_prefix()} [종가매수] 재시작 감지 → 미처리 {len(pending)}건 재주문 시도", tele=True)
    _run_closing_buy_orders()
    # 주문 후 state 갱신 (방금 결과 반영)
    new_placed = [(o["code"], o.get("name", ""), o.get("qty", 0), o.get("limit_up", 0)) for o in _closing_buy_placed]
    merged = {o["code"]: o for o in orders}
    for code, name, qty, limit_up in new_placed:
        merged[code] = {"code": code, "name": name, "order_qty": qty, "limit_up": limit_up, "status": "ordered", "filled_qty": 0, "remain_qty": qty}
    for c in pending:
        if c not in merged or merged[c].get("status") != "ordered":
            nm = (state.get("code_info") or {}).get(c, {}).get("name") or code_name_map.get(c, c)
            merged[c] = {"code": c, "name": nm, "status": "skipped", "reason": "재시도후미체결", "order_qty": 0, "filled_qty": 0, "remain_qty": 0}
    _save_closing_buy_state(state["codes"], state.get("code_info") or {}, list(merged.values()))


def _get_close_from_kis_api(client, code: str, use_today: bool) -> float:
    """KIS API로 당일종가(use_today=True) 또는 전일종가(use_today=False) 조회."""
    try:
        resp = client.inquire_price(code)
        if use_today:
            # 15:40~16:00: 당일 종가 (장 마감 후 stck_clpr 또는 stck_prpr이 당일종가)
            val = resp.get("stck_clpr") or resp.get("stck_prpr") or 0
        else:
            # 08:30~08:40: 전일 종가
            val = resp.get("stck_prdy_clpr") or resp.get("prdy_clpr") or 0
        return float(str(val or 0).replace(",", "") or 0)
    except Exception:
        return 0.0


def _get_prev_close_from_1d(codes: list[str]) -> dict[str, float]:
    """1d parquet에서 전일종가 조회. 최신 날짜=전일 데이터이므로 close 사용. code -> prev_close. (15:20 상한가 산정용)"""
    path = SCRIPT_DIR / "data" / "1d_data" / "kis_1d_unified_parquet_DB.parquet"
    if not path.exists():
        return {}
    try:
        df = pd.read_parquet(str(path), columns=["date", "symbol", "close"])
        if df.empty or "close" not in df.columns:
            return {}
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)
        df["close"] = pd.to_numeric(df["close"], errors="coerce").fillna(0)
        latest = df["date"].max()
        sub = df[(df["date"] == latest) & (df["symbol"].isin(c.zfill(6) for c in codes))]
        return dict(zip(sub["symbol"], sub["close"].astype(float)))
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가매수] 1d parquet 로드 실패: {e}")
        return {}


def _run_closing_buy_orders() -> None:
    """15:20 _closing_codes 종목에 init_cash 균등분배, 상한가(시장가) 매수."""
    global _closing_buy_placed
    if not CLOSE_BUY or not _closing_codes:
        return
    _closing_buy_placed.clear()
    try:
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        if not cano:
            logger.warning(f"{ts_prefix()} [종가매수] config에 cano 없음")
            return
        client = _top_client or _init_top_client()
        tr_id = "TTTC0802U"
        ord_dvsn = "01"  # 15:20 종가 동시호가 시장가
        n = len(_closing_codes)
        cash_per = max(0, INIT_CASH / n) * 0.995  # 수수료 여유 0.5%
        FEE_RATE = 0.00015  # 매수 수수료 추정 0.015%

        # 1d parquet에서 전일종가 우선 로드 (API 실패 대비)
        prev_close_1d = _get_prev_close_from_1d(_closing_codes)
        if prev_close_1d:
            _notify(f"{ts_prefix()} [종가매수] 1d parquet 전일종가 {len(prev_close_1d)}건 로드")

        for code in _closing_codes:
            info = _closing_code_info.get(code, {})
            prev_close = info.get("prev_close") or 0
            if prev_close <= 0 and prev_close_1d:
                prev_close = prev_close_1d.get(code.zfill(6)) or prev_close_1d.get(code) or 0
            if prev_close <= 0:
                try:
                    resp = client.inquire_price(code)
                    prev_close = float(str(resp.get("stck_prdy_clpr") or resp.get("prdy_clpr") or 0).replace(",", "") or 0)
                except Exception:
                    prev_close = 0
            if prev_close <= 0:
                logger.warning(f"{ts_prefix()} [종가매수] {code} 전일종가 없음, 스킵")
                continue
            limit_up = calc_limit_up_price(prev_close)
            qty = int(cash_per // limit_up)
            if qty <= 0:
                continue
            try:
                j = _buy_order_cash(client, cano, acnt, tr_id, code, qty, limit_up, ord_dvsn=ord_dvsn)
                out = j.get("output") or {}
                ordno = str(out.get("ODNO") or out.get("odno") or "").strip()
                name = info.get("name", code_name_map.get(code, code))
                amount = limit_up * qty
                fee_est = int(amount * FEE_RATE)
                amount_net = amount - fee_est
                _closing_buy_placed.append({
                    "name": name, "code": code, "qty": qty, "limit_up": limit_up, "amount_net": amount_net,
                    "ordno": ordno, "cano": cano, "acnt": acnt,
                })
                log_msg = (
                    f"{ts_prefix()} [종가매수주문] 종목명={name} | 주문유형=시장가 | 수량={qty} | "
                    f"금액={amount_net:,} (상한가*수량-수수료)"
                )
                _notify(log_msg)
                # 거래장부 기록
                info2 = _closing_code_info.get(code, {})
                _append_ledger(
                    order_type="BUY_ORDER",
                    code=code, name=name,
                    buy_price=float(limit_up),
                    order_qty=qty,
                    reason="종가매수(15:20)",
                    ord_no=ordno,
                    ord_dvsn="01(시장가)",
                    prdy_ctrt=float(info2["prdy_ctrt"] if "prdy_ctrt" in info2 else _last_prdy_ctrt.get(code, 0.0)),
                    note=f"상한가={limit_up} 전일종가={prev_close}",
                )
            except Exception as e:
                logger.warning(f"{ts_prefix()} [종가매수실패] {code}: {e}")
        if _closing_buy_placed:
            _notify(f"{ts_prefix()} [종가매수주문완료] {len(_closing_buy_placed)}건")
        # 주문 결과 state 저장 (목록, 수량, 주문여부)
        order_records = []
        for o in _closing_buy_placed:
            order_records.append({
                "code": o["code"], "name": o["name"],
                "order_qty": o["qty"], "limit_up": o["limit_up"], "ordno": o.get("ordno", ""),
                "status": "ordered", "filled_qty": 0, "remain_qty": o["qty"]
            })
        for code in _closing_codes:
            if code not in {x["code"] for x in _closing_buy_placed}:
                info = _closing_code_info.get(code, {})
                nm = info.get("name") or code_name_map.get(code, code)
                order_records.append({
                    "code": code, "name": nm,
                    "order_qty": 0, "limit_up": 0, "status": "skipped",
                    "filled_qty": 0, "remain_qty": 0, "reason": "전일종가없음 또는 수량0"
                })
        _save_closing_buy_state(list(_closing_codes), dict(_closing_code_info), order_records)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가매수] 전체 실패: {e}")


def _run_morning_extra_closing_buy() -> None:
    """
    08:30~08:40 시간외 종가: 전일 미체결 종목에 전일종가(ORD_DVSN=02) 지정가 추가 매수.

    remain 계산 우선순위:
      1) 시작잔고(_startup_balance_map): 실제 보유수량으로 filled 역산
         → remain = max(0, order_qty - startup_held)
      2) _get_closing_filled_for_remain(state): 체결통보+잔고조회 병합
      3) JSON state remain_qty 마지막 fallback
    추가 보호:
      - _str1_sell_state sold=True 종목 스킵 (이미 매도 완료)
      - 이미 보유수량 >= 주문수량이면 스킵 (전량 체결 간주)
    """
    global _morning_extra_closing_done
    if not MORNING_EXTRA_CLOSING_PR_BUY or _morning_extra_closing_done:
        return
    try:
        from datetime import timedelta
        yesterday = (datetime.now(KST) - timedelta(days=1)).strftime("%y%m%d")
        state = _load_closing_buy_state_by_date(yesterday)
        if not state or not state.get("orders"):
            _morning_extra_closing_done = True
            logger.info(f"{ts_prefix()} [08:30시간외종가] 전일 closing_buy_state 없음 → 스킵")
            return

        # ── 체결수량 계산: 병합된 filled_map 사용 (체결통보 + 잔고조회) ────────
        filled_map = _get_closing_filled_for_remain(state)

        # ── 시작잔고: 이미 보유 중인 실제 수량 ──────────────────────────────
        # _startup_balance_map은 main 블록에서 이미 취득했으나, 전역 접근 불가
        # → _get_balance_holdings() 재사용 (시간외종가 시점에 다시 조회하는 게 더 정확)
        try:
            live_balance = _get_balance_holdings()
        except Exception:
            live_balance = {}

        # ── Str1 매도 완료 종목 집합 ─────────────────────────────────────────
        with _str1_sell_state_lock:
            sold_codes = {c for c, st in _str1_sell_state.items() if st.get("sold")}

        to_order: list[dict] = []
        skip_lines: list[str] = []
        for o in state["orders"]:
            code = str(o.get("code", "")).zfill(6)
            if not code:
                continue
            name = o.get("name", code_name_map.get(code, code))
            order_qty = int(o.get("order_qty", o.get("qty", 0)))
            if order_qty <= 0:
                continue

            # 이미 매도 완료된 종목 스킵
            if code in sold_codes:
                skip_lines.append(f"  {name}({code}) → 매도완료(sold=True), 매수 스킵")
                continue

            # remain 계산 — 실제 보유수량 우선
            if live_balance:
                held = live_balance.get(code, 0)
                # 이미 전량 보유 중이면 체결 완료로 간주
                if held >= order_qty:
                    skip_lines.append(f"  {name}({code}) → 잔고보유={held} >= 주문={order_qty}, 전량체결 간주")
                    continue
                remain = max(0, order_qty - held)
            else:
                # 잔고조회 실패 시 filled_map 사용
                filled = max(
                    int(o.get("filled_qty", 0)),
                    filled_map.get(code, 0),
                )
                remain = max(0, order_qty - filled)
                if remain <= 0:
                    remain = max(0, order_qty - int(o.get("filled_qty", 0)))

            if remain <= 0:
                skip_lines.append(f"  {name}({code}) → 잔여수량=0, 스킵")
                continue

            o2 = dict(o)
            o2["remain_qty"] = remain
            o2["actual_held"] = live_balance.get(code, 0) if live_balance else -1
            to_order.append(o2)

        # ── 스킵 내역 출력 ────────────────────────────────────────────────────
        if skip_lines:
            _notify(f"{ts_prefix()} [08:30시간외종가] 제외 {len(skip_lines)}건:\n" + "\n".join(skip_lines))

        if not to_order:
            _morning_extra_closing_done = True
            _notify(f"{ts_prefix()} [08:30시간외종가] 추가 매수 대상 없음")
            return

        _morning_extra_closing_done = True  # 1회만 실행 (이중주문 방지)

        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        if not cano:
            logger.warning(f"{ts_prefix()} [08:30시간외종가] config cano 없음")
            return
        client = _top_client or _init_top_client()
        code_info = state.get("code_info") or {}

        _notify(f"{ts_prefix()} [08:30시간외종가] 추가 매수 시작 {len(to_order)}건")
        for o in to_order:
            code    = str(o["code"]).zfill(6)
            remain  = int(o.get("remain_qty", 0))
            held    = int(o.get("actual_held", -1))
            info    = code_info.get(code, {})
            name    = o.get("name", info.get("name", code))
            if remain <= 0:
                continue
            # KIS API에서 전일종가 실시간 조회 (저장값 대신 최신 값 사용)
            prev_close = _get_close_from_kis_api(client, code, use_today=False)
            if prev_close <= 0:
                logger.warning(f"{ts_prefix()} [08:30시간외종가] {code} 전일종가 조회 실패 → 스킵")
                continue
            held_txt = f" (잔고보유={held})" if held >= 0 else ""
            try:
                _buy_order_cash(client, cano, acnt, "TTTC0802U", code, remain, prev_close, ord_dvsn="05")
                _notify(
                    f"{ts_prefix()} [08:30시간외종가_매수] {name}({code})"
                    f"  수량={remain}{held_txt}  전일종가={prev_close:,.0f}  ORD_DVSN=05(장전시간외종가)",
                    tele=True,
                )
            except Exception as e:
                logger.warning(f"{ts_prefix()} [08:30시간외종가실패] {code}: {e}")
                _notify(f"{ts_prefix()} [08:30시간외종가실패] {name}({code}) {e}", tele=True)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [08:30시간외종가] 실패: {e}")


_morning_extra_closing_done = False
_afternoon_extra_closing_done = False
_morning_extra_logged_done = False
_afternoon_extra_logged_done = False


def _get_closing_filled_for_remain(state: dict) -> dict[str, int]:
    """
    체결 수량 병합: in-memory + persisted. 미매수(remain) 계산용.
    재시작 시에도 closing_filled_{yymmdd}.json 복원.
    체결통보 미수신 시 잔고조회로 보완 (filled=min(주문, 보유)).
    """
    persisted = _load_closing_filled()
    merged = dict(persisted)
    for k, v in _closing_filled_by_code.items():
        c = str(k).zfill(6)
        merged[c] = max(merged.get(c, 0), v)
    orders = state.get("orders") or []
    ordered_codes = [(str(o.get("code", "")).zfill(6), int(o.get("order_qty", o.get("qty", 0)))) for o in orders if o.get("status") == "ordered"]
    has_any_filled = any(merged.get(c, 0) > 0 for c, _ in ordered_codes)
    if not has_any_filled and ordered_codes:
        try:
            cfg = _read_cfg() or load_config(str(CONFIG_PATH))
            cano = str(cfg.get("cano", "")).strip()
            acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
            if cano:
                client = _top_client or _init_top_client()
                out1, _, _, _ = _get_balance_page(client, cano, acnt, "TTTC8434R")
                rows = out1 if isinstance(out1, list) else ([out1] if isinstance(out1, dict) else [])
                hold_map = {}
                for row in rows:
                    c = str(row.get("pdno", "") or "").strip().zfill(6)
                    if c:
                        hold_map[c] = int(float(str(row.get("hldg_qty", 0) or 0).replace(",", "") or 0))
                for c, ord_q in ordered_codes:
                    merged[c] = min(ord_q, hold_map.get(c, 0))
                logger.info(f"{ts_prefix()} [종가매수] 체결통보 미반영 → 잔고조회로 remain 계산")
        except Exception as e:
            logger.warning(f"{ts_prefix()} [종가매수] 잔고보완 실패: {e}")
    return merged


def _run_afternoon_extra_closing_buy() -> None:
    """15:40~16:00 시간외 종가: 당일 15:30 미매수 종목에 당일종가(ORD_DVSN=02) 지정가 매수. KIS API 당일종가 사용."""
    global _afternoon_extra_closing_done
    if not CLOSE_BUY or _afternoon_extra_closing_done:
        return
    try:
        state = _load_closing_buy_state()
        if not state or not state.get("orders"):
            _afternoon_extra_closing_done = True
            return
        filled_map = _get_closing_filled_for_remain(state)
        to_order = []
        for o in state["orders"]:
            order_qty = int(o.get("order_qty", o.get("qty", 0)))
            filled = min(order_qty, max(
                int(o.get("filled_qty", 0)),
                filled_map.get(str(o.get("code", "")).zfill(6), 0)
            ))
            remain = max(0, order_qty - filled)
            if remain <= 0:
                remain = max(0, order_qty - filled)
            if remain > 0:
                o2 = dict(o)
                o2["remain_qty"] = remain
                o2["filled_qty"] = filled
                to_order.append(o2)
        if not to_order:
            _afternoon_extra_closing_done = True
            return
        _afternoon_extra_closing_done = True
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        if not cano:
            return
        client = _top_client or _init_top_client()
        tr_id = "TTTC0802U"
        code_info = state.get("code_info") or {}
        for o in to_order:
            code = o["code"]
            remain = int(o.get("remain_qty", 0))
            if remain <= 0:
                continue
            info = code_info.get(code, {})
            today_close = _get_close_from_kis_api(client, code, use_today=True)
            if today_close <= 0:
                continue
            name = o.get("name", info.get("name", code))
            try:
                _buy_order_cash(client, cano, acnt, tr_id, code, remain, today_close, ord_dvsn="06")
                _notify(f"{ts_prefix()} [15:40시간외종가_매수 주문] {name}({code}) 수량={remain} 당일종가={today_close:,.0f} (ORD_DVSN=06 장후시간외종가)")
            except Exception as e:
                logger.warning(f"{ts_prefix()} [15:40시간외종가실패] {code}: {e}")
    except Exception as e:
        logger.warning(f"{ts_prefix()} [15:40시간외종가] 실패: {e}")


def _run_overtime_buy_orders() -> None:
    """16:00~18:00 시간외 단일가: 미매수(remain_qty)만 시장가 매수. 체결통보 병합 데이터 사용."""
    if not CLOSE_BUY:
        return
    state = _load_closing_buy_state()
    if not state or not state.get("orders"):
        return
    filled_map = _get_closing_filled_for_remain(state)
    to_order = []
    for o in state["orders"]:
        if o.get("status") != "ordered":
            continue
        order_qty = int(o.get("order_qty", o.get("qty", 0)))
        filled = min(order_qty, max(
            int(o.get("filled_qty", 0)),
            filled_map.get(str(o.get("code", "")).zfill(6), 0)
        ))
        remain = max(0, order_qty - filled)
        if remain <= 0:
            remain = max(0, order_qty - filled)
        if remain > 0:
            o["remain_qty"] = remain
            o["filled_qty"] = filled
            to_order.append(o)
    if not to_order:
        return
    try:
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        if not cano:
            _notify(f"{ts_prefix()} [시간외매수] config cano 없음 → 추가 매수 스킵")
            return
        client = _top_client or _init_top_client()
        tr_id = "TTTC0802U"
        for o in to_order:
            code = o["code"]
            remain = int(o.get("remain_qty", o.get("order_qty", 0)))
            if remain <= 0:
                continue
            limit_up = o.get("limit_up") or 0
            if limit_up <= 0:
                prev_close = _get_close_from_kis_api(client, code, use_today=False)
                if prev_close <= 0:
                    name = o.get("name", code_name_map.get(code, code))
                    _notify(f"{ts_prefix()} [시간외매수스킵] {name}({code}) 전일종가 조회 실패 → {remain}주 미주문")
                    continue
                limit_up = calc_limit_up_price(prev_close)
            name = o.get("name", code_name_map.get(code, code))
            try:
                _buy_order_cash(client, cano, acnt, tr_id, code, remain, limit_up, ord_dvsn="07")
                _notify(f"{ts_prefix()} [시간외매수] {name}({code}) 수량={remain} (상한가={limit_up:,.0f}, ORD_DVSN=07 시간외단일가)")
            except Exception as e:
                _notify(f"{ts_prefix()} [시간외매수실패] {name}({code}) {remain}주: {e}")
                logger.warning(f"{ts_prefix()} [시간외매수실패] {code}: {e}")
    except Exception as e:
        _notify(f"{ts_prefix()} [시간외매수] 전체 실패: {e}")
        logger.warning(f"{ts_prefix()} [시간외매수] 실패: {e}")


def _log_closing_result_after_window(window_label: str, yymmdd: str | None = None) -> None:
    """시간 종료 후 주문내역·결과를 로그로 출력. filled는 체결통보+persisted 병합 사용."""
    target_date = yymmdd or today_yymmdd
    state = _load_closing_buy_state_by_date(target_date) if target_date != today_yymmdd else _load_closing_buy_state()
    if not state or not state.get("orders"):
        return
    orders = state.get("orders") or []
    code_info = state.get("code_info") or {}
    filled_map = _get_closing_filled_for_remain(state) if target_date == today_yymmdd else _load_closing_filled(target_date)
    lines = [f"{ts_prefix()} [{window_label} 종료] 주문내역·결과"]
    for o in orders:
        code = str(o.get("code", "")).zfill(6)
        name = o.get("name", code_info.get(code, {}).get("name", code))
        ord_q = int(o.get("order_qty", o.get("qty", 0)))
        filled = min(ord_q, max(int(o.get("filled_qty", 0)), filled_map.get(code, 0))) if filled_map else int(o.get("filled_qty", 0))
        remain = max(0, ord_q - filled)
        status = o.get("status", "unknown")
        lines.append(f"  {name}({code}) 주문수량={ord_q} 매수수량={filled} 미매수={remain} status={status}")
    lines.append(f"  (체결통보 기반 _closing_filled_by_code 반영)")
    msg = "\n".join(lines)
    _notify(msg)
    logger.info(msg.replace("\n", " | "))


def _log_closing_order_aggregation(slot_label: str, state: dict | None = None) -> None:
    """주문/매수/미매수 합계(종목수 포함). 개별 종목은 생략, 10분마다 추가주문 결과 요약용."""
    s = state or _load_closing_buy_state()
    if not s or not s.get("orders"):
        return
    orders = s.get("orders") or []
    filled_map = _get_closing_filled_for_remain(s)
    total_ord, total_fill, total_remain = 0, 0, 0
    n_order_items, n_filled_items, n_remain_items = 0, 0, 0
    for o in orders:
        code = str(o.get("code", "")).zfill(6)
        ord_q = int(o.get("order_qty", o.get("qty", 0)))
        filled = min(ord_q, max(int(o.get("filled_qty", 0)), filled_map.get(code, 0)))
        remain = max(0, ord_q - filled)
        total_ord += ord_q
        total_fill += filled
        total_remain += remain
        if ord_q > 0:
            n_order_items += 1
        if filled > 0:
            n_filled_items += 1
        if remain > 0:
            n_remain_items += 1
    agg_title = "매수 집계" if "단일가" in slot_label else "종가매수 집계"
    msg = (
        f"{ts_prefix()} [{slot_label}] {agg_title} "
        f"총 {len(orders)}종목 | 주문 {n_order_items}종목 {total_ord}개 | 매수 {n_filled_items}종목 {total_fill}개 | 미매수 {n_remain_items}종목 {total_remain}개"
    )
    sys.stdout.write("\n")
    _notify(msg)
    logger.info(msg)
    sys.stdout.write("\n")
    sys.stdout.flush()


def _run_closing_buy_filled_notify() -> None:
    """15:30:40 종가매수 결과 통합 통보. 체결통보(H0STCNI0) 기반 주문/체결/미체결."""
    global _closing_buy_filled_done
    if _closing_buy_filled_done or not _closing_buy_placed:
        return
    try:
        total_ord, total_filled, total_remain = 0, 0, 0
        lines = ["[15:30:40 종가매수 결과]"]
        for o in _closing_buy_placed:
            c = str(o.get("code", "")).zfill(6)
            ord_q = int(o.get("qty", 0))
            filled = min(ord_q, _closing_filled_by_code.get(c, 0))
            remain = max(0, ord_q - filled)
            total_ord += ord_q
            total_filled += filled
            total_remain += remain
            lines.append(
                f"  {o.get('name')}({c}) 주문={ord_q} 체결={filled} 미체결={remain}"
            )
        lines.append(f"  합계: 주문={total_ord} 체결={total_filled} 미체결={total_remain}")
        msg = "\n".join(lines)
        sys.stdout.write("\n")
        _notify(msg, tele=True)
        sys.stdout.write("\n\n")
        sys.stdout.flush()
        _closing_buy_filled_done = True
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가매수체결알림] 실패: {e}")


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
_last_overtime_buy_min: int = -1  # 마지막 실행 분(minute) - 10분당 1회
_ccnl_early_add_date: date | None = None  # 08:30 구독 추가한 날짜 (1일 1회)
_overtime_real_seen: set[str] = set()
_overtime_real_active = False
_overtime_real_started_ts: float = 0.0   # 체결가 전환 시점 (타임아웃용)
_overtime_real_last_progress_ts: float = 0.0  # 마지막 진행상황 출력 시각
OVERTIME_REAL_TIMEOUT = 60               # 60초 내 전 종목 미수신 시 강제 복귀
OVERTIME_REAL_PROGRESS_INTERVAL = 10     # 체결가 수신 중 진행상황 출력 간격(초)
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

def _notify_overtime_single_price_final() -> None:
    """18:00 시간외 단일가(16:00~18:00) 마감 시 당일 해당 구분장 거래 총 내역 통보."""
    if not CLOSE_BUY:
        return
    s = _load_closing_buy_state()
    if not s or not s.get("orders"):
        return
    orders = s.get("orders") or []
    filled_map = _get_closing_filled_for_remain(s)
    total_ord, total_fill, total_remain = 0, 0, 0
    n_order_items, n_filled_items, n_remain_items = 0, 0, 0
    remain_detail = []
    for o in orders:
        code = str(o.get("code", "")).zfill(6)
        ord_q = int(o.get("order_qty", o.get("qty", 0)))
        filled = min(ord_q, max(int(o.get("filled_qty", 0)), filled_map.get(code, 0)))
        remain = max(0, ord_q - filled)
        total_ord += ord_q
        total_fill += filled
        total_remain += remain
        if ord_q > 0:
            n_order_items += 1
        if filled > 0:
            n_filled_items += 1
        if remain > 0:
            n_remain_items += 1
            name = o.get("name") or (s.get("code_info") or {}).get(code, {}).get("name") or code_name_map.get(code, code)
            remain_detail.append(f"{name}({code}) {remain}주")
    msg = (
        f"{ts_prefix()} [시간외 단일가 16:00~18:00 마감] 당일 거래 총 내역\n"
        f"총 {len(orders)}종목 | 주문 {n_order_items}종목 {total_ord}개 | 매수 {n_filled_items}종목 {total_fill}개 | 미매수 {n_remain_items}종목 {total_remain}개"
    )
    if remain_detail:
        msg += "\n미매수: " + ", ".join(remain_detail)
    _notify(msg, tele=True)


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
    global _overtime_real_active, _overtime_real_last_progress_ts, _vi_delayed_codes, _vi_delay_until, _last_overtime_real_slot
    global _end_time_reached
    logger.info(f"{ts_prefix()} [scheduler] started")
    while not _stop_event.is_set():
        now = datetime.now(KST)
        # 08:29:59 체결통보 구독: 08:30 이전 연결 시 ccnl_notice 미구독 → 기존 연결에 구독 추가 (1일 1회)
        if dtime(8, 29, 59) <= now.time() < dtime(8, 30, 5):
            global _ccnl_early_add_date
            today = now.date()
            if _ccnl_early_add_date != today:
                with _kws_lock:
                    if _active_kws is not None and "ccnl_notice" not in _subscribed:
                        acc_key = _get_ccnl_notice_tr_key()
                        if acc_key:
                            _ccnl_early_add_date = today
                            _notify(f"{ts_prefix()} [체결통보] 08:29:59 구독 추가", tele=True)
                            _send_subscribe(_active_kws, ccnl_notice, [acc_key], "1")  # noqa: F405
                            _subscribed["ccnl_notice"] = {acc_key}
        # 08:30~08:40 시간외 종가: 전일 미매수 종목 추가 매수 (ORD_DVSN=02)
        if dtime(8, 30) <= now.time() < dtime(8, 41):
            try:
                _run_morning_extra_closing_buy()
            except Exception as e:
                logger.warning(f"{ts_prefix()} [08:30시간외종가] {e}")
        # 08:30~08:40 종료 후 주문내역·결과 로그
        if dtime(8, 41) <= now.time() < dtime(8, 42):
            global _morning_extra_logged_done
            if not _morning_extra_logged_done:
                try:
                    yesterday = (now - timedelta(days=1)).strftime("%y%m%d")
                    _log_closing_result_after_window("08:30~08:40", yesterday)
                    _morning_extra_logged_done = True
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [08:30시간외종가 로그] {e}")
        # 08:58 계좌잔고조회 1회 (lock 밖에서 실행, HTTP 호출 있음)
        if dtime(8, 58) <= now.time() < dtime(8, 59):
            try:
                _run_balance_0858()
            except Exception as e:
                logger.warning(f"{ts_prefix()} [잔고0858] {e}")
        # 08:59:50~09:00:10 장전 주문현황 정리 1회 (주문대기/취소요청/이상없음)
        if dtime(8, 59, 50) <= now.time() < dtime(9, 0, 10):
            try:
                _print_premarket_order_summary()
            except Exception as e:
                logger.warning(f"{ts_prefix()} [장전주문현황] {e}")
        # 09:00~15:20 주기적 매도 조건 체크 (15초마다, 틱 의존 없이 _last_prdy_ctrt 사용)
        if dtime(9, 0) <= now.time() < dtime(15, 20):
            try:
                _check_sell_by_prdy_ctrt_periodic()
            except Exception as e:
                logger.warning(f"{ts_prefix()} [주기매도체크] {e}")
        # 15:18 조건부 전체 매도: 전일대비 28% 미만 종목 전부 시장가 매도
        if dtime(15, 18, 0) <= now.time() <= dtime(15, 18, 10):
            try:
                _run_sell_1518()
            except Exception as e:
                logger.warning(f"{ts_prefix()} [15:18 조건부매도] {e}")
        # 15:29:30 일시 확인: 예상체결가 첫 틱 대비 하락 시 종가매수 취소
        if dtime(15, 29, 28) <= now.time() <= dtime(15, 29, 35):
            try:
                _run_closing_exp_check_152930()
            except Exception as e:
                logger.warning(f"{ts_prefix()} [종가매수-15:29:30] {e}")
        # 15:30:40 실시간 체결 수신 완료 후 잔고조회로 주문 결과 메시지 (tele)
        if dtime(15, 30, 40) <= now.time() < dtime(15, 31):
            try:
                _run_closing_buy_filled_notify()
            except Exception as e:
                logger.warning(f"{ts_prefix()} [종가매수체결알림] {e}")
        # 15:40~16:00 시간외 종가: 당일 미매수 종목에 당일종가(ORD_DVSN=02) 매수
        if dtime(15, 40) <= now.time() < dtime(16, 1):
            try:
                _run_afternoon_extra_closing_buy()
            except Exception as e:
                logger.warning(f"{ts_prefix()} [15:40시간외종가] {e}")
        # 15:40~16:00 종료 후 주문내역·결과 로그
        if dtime(16, 1) <= now.time() < dtime(16, 2):
            global _afternoon_extra_logged_done
            if not _afternoon_extra_logged_done:
                try:
                    _log_closing_result_after_window("15:40~16:00")
                    _afternoon_extra_logged_done = True
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [15:40시간외종가 로그] {e}")
        # 16:00~18:00 시간외 단일가 매수: 10분 단위(16:00, 16:10, ... 17:50) 체결통보 반영 후 주문→직전 결과 출력
        if dtime(16, 0) <= now.time() < END_TIME:
            global _last_overtime_buy_min
            minute = now.minute
            second = now.second
            if minute % 10 == 0 and minute != _last_overtime_buy_min:
                if (minute == 0 and second <= 2) or (minute > 0 and 1 <= second <= 3):  # 16:00:00 / 16:10:01 ...
                    _last_overtime_buy_min = minute
                    # 직전 슬롯 결과 출력 (16:00에는 15:40 종가, 16:10~17:50에는 이전 10분 단일가)
                    prev_label = "15:40종가" if minute == 0 else f"{now.hour:02d}:{(minute - 10) % 60:02d}단일가"
                    _log_closing_order_aggregation(prev_label)
                    try:
                        _run_overtime_buy_orders()
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [시간외매수] {e}")
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
                    # ① 마지막 17:50 단일가 결과 집계 출력
                    _log_closing_order_aggregation("17:50단일가")
                    # ② 시간외 단일가 마감 총 거래 내역 통보 (텔레그램)
                    try:
                        _notify_overtime_single_price_final()
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [시간외단일가 마감통보] {e}")
                    # ③ 구독 종료 + WS 닫기
                    sys.stdout.write("\n")
                    _notify(f"{ts_prefix()} 시간외 체결가 구독 종료")
                    with _kws_lock:
                        if _active_kws is not None:
                            _request_ws_close(_active_kws)
                    time.sleep(1.0)
                    # ② accumulator 즉시 저장
                    _save_accumulator("18:00_shutdown")
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
                    _overtime_real_last_progress_ts = 0.0
                    _overtime_real_seen.clear()
                    _current_mode = RunMode.OVERTIME_REAL
                    _log_mode_transition(RunMode.OVERTIME_EXP, RunMode.OVERTIME_REAL)

            if _overtime_real_active:
                ot_elapsed = time.time() - _overtime_real_started_ts
                seen_n = len(_overtime_real_seen)
                total_n = len(codes)
                if ot_elapsed >= OVERTIME_REAL_PROGRESS_INTERVAL and (
                    time.time() - _overtime_real_last_progress_ts >= OVERTIME_REAL_PROGRESS_INTERVAL
                ):
                    _overtime_real_last_progress_ts = time.time()
                    sys.stdout.write("\n")
                    _notify(f"{ts_prefix()} [시간외] 체결가 수신 중 {seen_n}/{total_n}종목...")
                    sys.stdout.flush()
                if seen_n >= total_n:
                    # 전 종목 수신 완료 → 예상체결가 복귀 + 내 주문 체결 결과 출력
                    _overtime_real_active = False
                    _overtime_real_seen.clear()
                    _current_mode = RunMode.OVERTIME_EXP
                    _log_mode_transition(RunMode.OVERTIME_REAL, RunMode.OVERTIME_EXP)
                    exec_min = ((_last_overtime_real_slot.minute + 1) // 10) * 10 if _last_overtime_real_slot else now.minute
                    slot_label = f"{now.hour:02d}:{exec_min:02d}단일가"
                    _log_closing_order_aggregation(slot_label, _load_closing_buy_state())
                elif ot_elapsed >= OVERTIME_REAL_TIMEOUT:
                    _overtime_real_active = False
                    logger.info(
                        f"{ts_prefix()} [시간외] 체결가 {OVERTIME_REAL_TIMEOUT}초 타임아웃: "
                        f"{seen_n}/{total_n}종목 수신"
                    )
                    _overtime_real_seen.clear()
                    _current_mode = RunMode.OVERTIME_EXP
                    _log_mode_transition(RunMode.OVERTIME_REAL, RunMode.OVERTIME_EXP)
                    exec_min = ((_last_overtime_real_slot.minute + 1) // 10) * 10 if _last_overtime_real_slot else now.minute
                    slot_label = f"{now.hour:02d}:{exec_min:02d}단일가"
                    _log_closing_order_aggregation(slot_label, _load_closing_buy_state())

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
_last_save_time = time.time()   # 마지막 스냅샷 저장 시각
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
        since_active = sum(1 for c in codes if _since_save_counts.get(c, 0) > 0)
        total_codes = len(codes)
    total_rows = _accumulator_rows
    line = (
        f"{prefix} (초당 수신건수 {per_sec_total:03d}건/초), "
        f"(누적 {total_rows}건/수신 종목 {since_active}개/구독 종목 {total_codes}개)"
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
_ingest_queue: "queue.Queue[tuple[pl.DataFrame, str, str, str | None, str]]" = queue.Queue()

_accumulator: list[pl.DataFrame] = []         # 당일 전체 틱 데이터 (메모리 누적)
_accumulator_lock = threading.RLock()          # 스냅샷 저장과 append 동시 처리용
_save_lock = threading.Lock()                  # _save_accumulator 동시 호출 방지 (shutdown 중복 실행)
_accumulator_rows: int = 0                     # 누적 행 수 (표시용)

# ── 종목별 기술적 지표 인메모리 상태 ──
# EMA: {code: {period: float|None}}  – EMA 누적값 (None이면 첫 틱에 가격으로 초기화)
_ema_state: dict[str, dict[int, float | None]] = {
    c: {n: None for n in INDICATOR_MA_LINE} for c in codes
}
# BB용 raw price deque: {code: deque(maxlen=BB_PERIOD)} – 표준편차 계산에 사용
_price_buf: dict[str, deque] = {
    c: deque(maxlen=_IND_MAX_WINDOW) for c in codes
}

_last_recv_ts: dict[str, float] = {c: 0.0 for c in codes}
_code_added_ts: dict[str, float] = {c: time.time() for c in codes}  # 종목별 추가 시점 (watchdog 3분 기준)
_last_summary_ts = time.time()

# ── 장중 미수신 감시 상태 ──
_last_stale_check_ts: dict[str, float] = {}  # 종목별 마지막 REST 상태확인 시각
_halted_codes: set[str] = set()               # 거래정지 확인된 종목
_vi_active_codes: set[str] = set()            # VI 발동 확인된 종목
_vi_exp_sub_ts: dict[str, float] = {}         # VI 종목 예상체결가 구독 시작 시각
_rest_fail_backoff: dict[str, int] = {}       # REST 500 에러 연속 횟수 (백오프용)
_single_price_codes: set[str] = set()          # 57/59 30분 단일가 매매 종목 (58=상장폐지예정은 제외)

_overwrite_events: list[dict[str, str]] = []

_top_client: KisClient | None = None
_price_client: KisClient | None = None
_top_client_2: KisClient | None = None      # 계정2-syw_2 (한도 초과 시 fallback)
_price_client_2: KisClient | None = None     # 계정2-syw_2 (한도 초과 시 fallback)
_price_queue: "queue.Queue[str]" = queue.Queue()
_last_rest_req_ts: dict[str, float] = {c: 0.0 for c in codes}
_rest_req_count = 0
_rest_req_second = int(time.time())
_rest_pending: set[str] = set()
_rest_queue: "queue.Queue[tuple[callable, tuple, dict, float, threading.Event, dict]]" = queue.Queue()
_rest_last_sent_ts = 0.0
_price_req_count = 0
_price_req_second = int(time.time())


# ── PRIMARY: 공통계정(계정1/메인) 클라이언트 ─────────────────────
def _load_main_cfg() -> dict:
    """config.json 루트의 메인 계정 appkey/appsecret을 읽어 반환."""
    cfg = load_config(str(SCRIPT_DIR / "config.json"))
    appkey = cfg.get("appkey")
    appsecret = cfg.get("appsecret")
    if not appkey or not appsecret:
        raise ValueError("config.json 루트에 appkey/appsecret이 필요합니다.")
    return {
        "appkey": appkey,
        "appsecret": appsecret,
        "base_url": cfg.get("base_url") or DEFAULT_BASE_URL,
        "custtype": cfg.get("custtype") or "P",
        "market_div": cfg.get("market_div") or "J",
    }


def _init_top_client() -> KisClient:
    a1 = _load_main_cfg()
    return KisClient(KisConfig(
        appkey=a1["appkey"], appsecret=a1["appsecret"],
        base_url=a1["base_url"], custtype=a1["custtype"],
        market_div=a1.get("market_div") or TOP_RANK_MARKET_DIV,
        token_cache_path=str(SCRIPT_DIR / "kis_token_main.json"),
    ))


def _init_price_client() -> KisClient:
    a1 = _load_main_cfg()
    return KisClient(KisConfig(
        appkey=a1["appkey"], appsecret=a1["appsecret"],
        base_url=a1["base_url"], custtype=a1["custtype"],
        market_div=a1["market_div"],
        token_cache_path=str(SCRIPT_DIR / "kis_token_main.json"),
    ))


# ── FALLBACK: 계정2(syw_2) 클라이언트 (한도 초과 시) ─────────────
def _load_syw2_cfg() -> dict:
    """config.json → accounts.syw_2 키를 읽어 공통 설정과 병합하여 반환."""
    cfg = load_config(str(SCRIPT_DIR / "config.json"))
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
    a2 = _load_syw2_cfg()
    if not a2:
        return None
    return KisClient(KisConfig(
        appkey=a2["appkey"], appsecret=a2["appsecret"],
        base_url=a2["base_url"], custtype=a2["custtype"],
        market_div=a2.get("market_div") or TOP_RANK_MARKET_DIV,
        token_cache_path=str(SCRIPT_DIR / "kis_token_syw2.json"),
    ))


def _init_price_client_2() -> KisClient | None:
    a2 = _load_syw2_cfg()
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


def _single_price_use_antc_now() -> bool:
    """57/59 종목(30분 단일가): 실시간 창(XX:29:29~XX:00:05)이면 False, 그 외 예상체결 창이면 True."""
    t = datetime.now(KST).time()
    m, s = t.minute, t.second
    real_window = (m in (29, 59) and s >= 29) or (m in (0, 30) and s < 6)
    return not real_window


def _enqueue_rest_price_row(code: str, output: dict, use_antc: bool | None = None) -> None:
    # wss 컬럼과 맞춰 저장: 현재가/호가/거래량/상하한가
    # ★ WSS 원본이 소문자이므로 동일하게 소문자 컬럼명 사용 (대소문자 불일치 → 중복 방지)
    # use_antc: 57/59 종목 예상체결가 모드 시 antc_prce→stck_prpr, antc_vol→acml_vol 매핑
    # None이면 _single_price_codes인 경우 현재 시간으로 자동 판단
    if use_antc is None and code in _single_price_codes:
        use_antc = _single_price_use_antc_now()
    elif use_antc is None:
        use_antc = False
    if use_antc:
        stck_prpr = output.get("antc_prce") or output.get("stck_prpr")
        acml_vol = output.get("antc_vol") or output.get("acml_vol")
    else:
        stck_prpr = output.get("stck_prpr")
        acml_vol = output.get("acml_vol")
    row = {
        "mksc_shrn_iscd": str(code).zfill(6),
        "stck_prpr": stck_prpr,
        "askp1": output.get("askp1"),
        "bidp1": output.get("bidp1"),
        "acml_vol": acml_vol,
        "stck_mxpr": output.get("stck_mxpr"),
        "stck_llam": output.get("stck_llam"),
    }
    df = pl.DataFrame([row])
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
    iscd_stat = str(output.get("iscd_stat_cls_code", "")).strip()

    def _stale_notify(msg: str) -> None:
        sys.stdout.write("\n")
        _notify(msg)

    # ── 58(상장폐지 예정) → 거래 대상 제외, 구독 해제 ──
    if iscd_stat == "58":
        _single_price_codes.discard(code)
        if code not in _halted_codes:
            _halted_codes.add(code)
            _stale_notify(
                f"{ts_prefix()} [stale] {name}({code}) 종목상태 58(상장폐지 예정) → 거래 대상 제외, 구독 해제"
            )
        removed = _remove_code_structs([code], force=True)
        if removed:
            _trigger_ws_rebuild()
        return

    # ── 57/59(30분 단일가) → WSS 구독 유지, 실시간↔예상체결 전환 ──
    # 매 XX:29:29 실시간 체결가 전환 → XX:00/30 수신 → XX:05 예상체결가 전환
    # 20초 미수신 시 REST 보강은 기존 stale_check와 동일
    if iscd_stat in ("57", "59"):
        if code not in _single_price_codes:
            _single_price_codes.add(code)
            desc = "57" if iscd_stat == "57" else "59(단기과열)"
            _stale_notify(
                f"{ts_prefix()} [stale] {name}({code}) 종목상태가 {desc}로 "
                "30분 단위 단일가 매매 대상이므로 예상체결가 구독으로 전환합니다.(매 30분 단위 전환)"
            )
        _halted_codes.discard(code)
        _enqueue_rest_price_row(code, output, use_antc=True)
        return

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
    # 계정2(syw_2) fallback 초기화 (실패해도 기본 계정으로 계속)
    if USE_FALLBACK_ACCOUNT:
        try:
            _price_client_2 = _init_price_client_2()
            if _price_client_2:
                logger.info(f"{ts_prefix()} [price] 계정2(syw_2) fallback 초기화 완료")
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
                            logger.info(f"{ts_prefix()} [price] REST보충(계정2) {name}({code}) pr={pr}")
                    except Exception as e2:
                        logger.warning(f"{ts_prefix()} [price] fetch failed(계정2) code={code}: {e2}")
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
            _last_recv_ts[c] = 0.0
            _last_rest_req_ts[c] = 0.0
            _code_added_ts[c] = time.time()  # watchdog 3분 기준: 추가 시점부터 계산
            _init_indicator_buf(c)
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
_closing_codes: list[str] = []       # 15:19 선정된 종가매매 종목 (Select_Tr_target_list 조건)
_closing_code_info: dict[str, dict] = {}  # code -> {prev_close, name} (매수 시 상한가 산정용)
_balance_0858_done = False           # 08:58 잔고조회 1회 실행 플래그
_closing_buy_placed: list[dict] = [] # 15:20 주문 건 (15:30 체결 알림용)
_closing_buy_filled_done = False     # 15:30 체결 메시지 1회 플래그
# 15:20 예상체결가 모니터링: 첫 틱 가격·현재가 캐시, 취소/재주문 상태
_closing_exp_first_tick: dict[str, float] = {}   # code -> 15:20 첫 예상체결가
_closing_last_exp_price: dict[str, float] = {}   # code -> 최근 예상체결가
_closing_last_bidp1: dict[str, float] = {}        # code -> 최근 매수 1호가
_closing_last_askp1: dict[str, float] = {}        # code -> 최근 매도 1호가
_closing_cancelled: set[str] = set()             # 취소된 종목 (재주문 대기)
_closing_cancel_failed: set[str] = set()         # 500 등 서버 에러로 취소 실패한 종목 (재시도 안 함)
_closing_last_action_ts: dict[str, float] = {}   # code -> 마지막 취소/재주문 시각
_closing_152930_done = False                      # 15:29:30 일시 확인 실행 여부
_closing_monitor_lock = threading.Lock()          # 모니터링 락


def _to_float_closing(val) -> float:
    try:
        if isinstance(val, str):
            val = val.replace(",", "")
        return float(val)
    except Exception:
        return 0.0


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
            _last_recv_ts.pop(c, None)
            _last_rest_req_ts.pop(c, None)
            _last_prdy_ctrt.pop(c, None)
            _code_added_ts.pop(c, None)
            _ema_state.pop(c, None)
            _price_buf.pop(c, None)
        if removed:
            _persist_subscription_codes(codes)
    return removed


def _run_closing_exp_monitor_tick(result: pl.DataFrame, code_col: str, col_map: dict) -> None:
    """
    15:20~15:30 예상체결가 모니터링.

    [15:20~15:29:30] 첫 틱·최근가 캐시만. 취소 없음.
    [15:29:30] 처음 취소: 예상가 < 첫틱 시 취소. 단, 첫틱이 매수/매도 1호가 범위 내면 유지.
    [15:29:30~15:30] 지속 모니터링: 예상가 < 첫틱 시 즉시 취소 (쿨다운 없음).
    취소 후 가격 복귀(>=첫틱 또는 첫틱∈1호가) 시 재주문.
    """
    if not CLOSE_BUY or not _closing_codes or _get_mode() != RunMode.CLOSE_EXP:
        return
    # H0STANC0(예상체결) 스키마: stck_prpr만 반환
    pr_col = col_map.get("stck_prpr") or col_map.get("antc_prce")
    bid_col = col_map.get("bidp1")
    ask_col = col_map.get("askp1")
    if not pr_col:
        return

    now_kst = datetime.now(KST).time()
    after_152930 = now_kst >= dtime(15, 29, 30)

    placed_by_code = {o["code"]: o for o in _closing_buy_placed if o.get("qty", 0) > 0}
    result = result.with_columns(
        pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
    )
    now_ts = time.time()
    for df_code in result.partition_by(code_col, maintain_order=True):
        code = str(df_code[code_col][0])
        if code not in _closing_codes or code not in placed_by_code:
            continue
        row = df_code.row(-1, named=True)
        try:
            price = _to_float_closing(row.get(pr_col))
        except Exception:
            continue
        if price <= 0:
            continue
        bidp1 = _to_float_closing(row.get(bid_col)) if bid_col else 0
        askp1 = _to_float_closing(row.get(ask_col)) if ask_col else 0

        with _closing_monitor_lock:
            if code not in _closing_exp_first_tick:
                _closing_exp_first_tick[code] = price
                _save_exp_first_tick()  # 재시작 시 복원용 저장
            first_tick = _closing_exp_first_tick[code]
            _closing_last_exp_price[code] = price
            _closing_last_bidp1[code] = bidp1
            _closing_last_askp1[code] = askp1

            o = placed_by_code.get(code, {})
            ordno = str(o.get("ordno", "")).strip()
            qty = int(o.get("qty", 0))
            limit_up = _to_float_closing(o.get("limit_up", 0))

            # 첫틱이 매수/매도 1호가 범위 내인지 (유지 조건)
            first_tick_in_hoga = False
            if bidp1 > 0 and askp1 > 0:
                lo, hi = min(bidp1, askp1), max(bidp1, askp1)
                first_tick_in_hoga = (lo - 1) <= first_tick <= (hi + 1)
            elif bidp1 > 0 and abs(first_tick - bidp1) < 1:
                first_tick_in_hoga = True
            elif askp1 > 0 and abs(first_tick - askp1) < 1:
                first_tick_in_hoga = True

            # 예상가 < 첫틱 → 취소 조건. 단 첫틱∈1호가 범위면 유지
            should_cancel = (price < first_tick) and not first_tick_in_hoga

            if should_cancel and ordno and code not in _closing_cancelled and code not in _closing_cancel_failed:
                # 15:29:30 이전에는 취소 안 함
                if not after_152930:
                    continue
                # 15:29:30~15:30: 즉시 취소 (쿨다운 없음)
                _closing_last_action_ts[code] = now_ts
                _closing_cancelled.add(code)
                try:
                    cfg = _read_cfg() or load_config(str(CONFIG_PATH))
                    cano = o.get("cano") or str(cfg.get("cano", "")).strip()
                    acnt = o.get("acnt") or str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
                    if not cano:
                        raise RuntimeError("config cano 없음")
                    client = _top_client or _init_top_client()
                    _rest_submit(_cancel_closing_order, min_interval=0.5, timeout=15, client=client, cano=cano, acnt=acnt, ordno=ordno, code=code, qty=qty)
                    o["ordno"] = ""
                    _notify(f"{ts_prefix()} [종가매수취소] {code} {o.get('name','')} 예상체결 {price:.0f} < 첫틱 {first_tick:.0f}")
                except Exception as e:
                    _closing_cancelled.discard(code)
                    if "500" in str(e) or "Internal Server Error" in str(e):
                        _closing_cancel_failed.add(code)
                        logger.warning(f"{ts_prefix()} [종가매수취소실패] {code}: KIS 500 → 재시도 중단")
                    else:
                        logger.warning(f"{ts_prefix()} [종가매수취소실패] {code}: {e}")

            # 취소 후 복귀: 첫틱 이상 또는 첫틱∈1호가 → 재주문
            elif code in _closing_cancelled and (price >= first_tick or first_tick_in_hoga):
                cooldown = 1.0
                last_act = _closing_last_action_ts.get(code, 0)
                if now_ts - last_act < cooldown:
                    continue
                _closing_last_action_ts[code] = now_ts
                _closing_cancelled.discard(code)
                try:
                    cfg = _read_cfg() or load_config(str(CONFIG_PATH))
                    cano = str(cfg.get("cano", "")).strip()
                    acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
                    client = _top_client or _init_top_client()
                    j = _rest_submit(_buy_order_cash, min_interval=1.0, timeout=15, client=client, cano=cano, acnt_prdt_cd=acnt, tr_id="TTTC0802U", code=code, qty=qty, price=limit_up, ord_dvsn="01")
                    out = j.get("output") or {}
                    new_ordno = str(out.get("ODNO") or out.get("odno") or "").strip()
                    o["ordno"] = new_ordno
                    o["cano"] = cano
                    o["acnt"] = acnt
                    _notify(f"{ts_prefix()} [종가매수재주문] {code} {o.get('name','')} 예상체결 {price:.0f} >= 첫틱 {first_tick:.0f} ordno={new_ordno}")
                except Exception as e:
                    _closing_cancelled.add(code)
                    logger.warning(f"{ts_prefix()} [종가매수재주문실패] {code}: {e}")
    return


def _run_closing_exp_check_152930() -> None:
    """
    15:29:30 일시 확인: 이 시점 가격을 첫 틱과 비교.
    - 예상가 < 첫틱 → 취소
    - 첫틱이 매수/매도 1호가 범위 내 → 유지 (취소 안 함)
    """
    global _closing_152930_done
    if not CLOSE_BUY or not _closing_codes or _closing_152930_done:
        return
    with _closing_monitor_lock:
        for o in list(_closing_buy_placed):
            code = o.get("code", "")
            if code not in _closing_codes:
                continue
            ordno = str(o.get("ordno", "")).strip()
            if not ordno or code in _closing_cancelled or code in _closing_cancel_failed:
                continue
            first_tick = _closing_exp_first_tick.get(code, 0)
            last_price = _closing_last_exp_price.get(code, 0)
            bidp1 = _closing_last_bidp1.get(code, 0)
            askp1 = _closing_last_askp1.get(code, 0)
            if first_tick <= 0 or last_price <= 0:
                continue
            # 첫틱이 1호가 범위 내면 유지
            first_tick_in_hoga = False
            if bidp1 > 0 and askp1 > 0:
                lo, hi = min(bidp1, askp1), max(bidp1, askp1)
                first_tick_in_hoga = (lo - 1) <= first_tick <= (hi + 1)
            elif bidp1 > 0 and abs(first_tick - bidp1) < 1:
                first_tick_in_hoga = True
            elif askp1 > 0 and abs(first_tick - askp1) < 1:
                first_tick_in_hoga = True
            if first_tick_in_hoga:
                continue  # 유지
            if last_price < first_tick:
                qty = int(o.get("qty", 0))
                if qty > 0:
                    try:
                        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
                        cano = o.get("cano") or str(cfg.get("cano", "")).strip()
                        acnt = o.get("acnt") or str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
                        if not cano:
                            logger.warning(f"{ts_prefix()} [종가매수취소-15:29:30] {code} config cano 없음, 스킵")
                            continue
                        client = _top_client or _init_top_client()
                        _rest_submit(_cancel_closing_order, min_interval=0.5, timeout=15, client=client, cano=cano, acnt=acnt, ordno=ordno, code=code, qty=qty)
                        o["ordno"] = ""
                        _closing_cancelled.add(code)
                        _notify(f"{ts_prefix()} [종가매수취소-15:29:30] {code} {o.get('name','')} 예상체결 {last_price:.0f} < 첫틱 {first_tick:.0f}")
                    except Exception as e:
                        if "500" in str(e) or "Internal Server Error" in str(e):
                            _closing_cancel_failed.add(code)
                            logger.warning(f"{ts_prefix()} [종가매수취소실패-15:29:30] {code}: KIS 500 → 재시도 중단")
                        else:
                            logger.warning(f"{ts_prefix()} [종가매수취소실패-15:29:30] {code}: {e}")
    _closing_152930_done = True


def _switch_to_closing_codes() -> None:
    """15:20 종가매매 전환: 기존 종목 전부 해제 → _closing_codes 만 남긴다."""
    global _base_codes
    if not _closing_codes:
        logger.warning(f"{ts_prefix()} [top] _closing_codes 비어 있음, 전환 불가")
        return

    # ── 0) CLOSE_BUY 옵션 시 매수 주문을 가장 먼저 실행 (지연 최소화) ──
    if CLOSE_BUY:
        try:
            _run_closing_buy_orders()
        except Exception as e:
            logger.warning(f"{ts_prefix()} [종가매수] {e}")

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
            _last_recv_ts[c] = _last_recv_ts.get(c, 0.0)
            _last_rest_req_ts[c] = _last_rest_req_ts.get(c, 0.0)
            _code_added_ts[c] = now_ts  # watchdog 3분 기준 리셋 (즉시 삭제 방지)
            if c not in _ema_state:
                _init_indicator_buf(c)

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
    # 15:20 예상체결 모니터링 상태 초기화
    _closing_exp_first_tick.clear()
    _closing_last_exp_price.clear()
    _closing_last_bidp1.clear()
    _closing_last_askp1.clear()
    _closing_cancelled.clear()
    _closing_cancel_failed.clear()
    _closing_last_action_ts.clear()
    global _closing_152930_done
    _closing_152930_done = False


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
    # 계정2(syw_2) fallback 초기화 (실패해도 기본 계정으로 계속)
    if USE_FALLBACK_ACCOUNT:
        try:
            _top_client_2 = _init_top_client_2()
            if _top_client_2:
                logger.info(f"{ts_prefix()} [top] 계정2(syw_2) fallback 초기화 완료")
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
        is_closing_select = (next_ts.time() >= dtime(15, 19))   # 15:19 종가매매 선정
        is_closing_switch = (next_ts.time() >= dtime(15, 20))  # 15:20 구독 전환 + 매수
        try:
            # 15:20 구독 전환: 15:19에서 선정된 _closing_codes 사용, fetch 생략
            if is_closing_switch and _closing_codes:
                _switch_to_closing_codes()
                logger.info(f"{ts_prefix()} [top] end time reached (종가매매 전환 완료), stopping")
                return
            # ── 공통계정(메인) → 실패 시 계정2(syw_2)로 재시도 ──
            try:
                rows = _rest_submit(_fetch_fluctuation_top, _top_client, top_n=TOP_RANK_N)
            except Exception as e1:
                logger.warning(f"{ts_prefix()} [top] 공통계정 실패: {type(e1).__name__}: {e1}")
                if USE_FALLBACK_ACCOUNT and _top_client_2:
                    rows = _rest_submit(_fetch_fluctuation_top, _top_client_2, top_n=TOP_RANK_N)
                    logger.info(f"{ts_prefix()} [top] 계정2(syw_2)로 재시도 성공 ({len(rows)}건)")
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
            # ============================================================
            # 15:19 종가매매 종목 선정 — 방금 받은 rows에서 직접 필터
            # 조건: 전일대비 CLOSING_TARGET_PDY_CTRT*100 % 이상 상승(sign=1)
            #       + 현재가/당일고가 >= 0.97 (고가 대비 3% 이내)
            # ============================================================
            if TOP5_ADD and is_closing_select and not _closing_codes:
                closing_candidates = []
                _closing_code_info = {}
                ctrt_threshold = CLOSING_TARGET_PDY_CTRT * 100  # 0.20 → 20.0
                for r in rows:
                    code = str(
                        r.get("stck_shrn_iscd") or r.get("stck_cd") or r.get("code") or ""
                    ).strip().zfill(6)
                    if not code:
                        continue
                    try:
                        prdy_ctrt = float(str(r.get("prdy_ctrt") or 0).replace(",", "") or 0)
                        prdy_sign = str(r.get("prdy_vrss_sign") or "").strip()
                        stck_prpr = float(str(r.get("stck_prpr") or 0).replace(",", "") or 0)
                        stck_hgpr = float(str(r.get("stck_hgpr") or 0).replace(",", "") or 0)
                        name      = str(r.get("hts_kor_isnm") or "").strip()
                    except (TypeError, ValueError):
                        continue
                    if prdy_sign not in ("1", "2"):  # 상한가(1) 또는 상승(2)만
                        continue
                    if prdy_ctrt < ctrt_threshold:  # 전일대비 20% 이상
                        continue
                    if stck_hgpr <= 0:
                        continue
                    if stck_prpr / stck_hgpr < 0.97:  # 현재가 = 고가 대비 3% 이내
                        continue
                    closing_candidates.append(code)
                    _closing_code_info[code] = {"prev_close": 0.0, "name": name, "prdy_ctrt": prdy_ctrt}
                # prev_close: 상한가 계산용, 1d parquet에서 보완
                if closing_candidates:
                    prev_1d = _get_prev_close_from_1d(closing_candidates)
                    for code in closing_candidates:
                        pc = (prev_1d.get(code.zfill(6)) or prev_1d.get(code) or 0) if prev_1d else 0
                        if pc > 0:
                            _closing_code_info[code] = {**_closing_code_info[code], "prev_close": float(pc)}
                _closing_codes.clear()
                _closing_codes.extend(closing_candidates)
                try:
                    code_name_map.update(_code_name_map())
                except Exception:
                    pass
                if _closing_codes:
                    closing_names = [code_name_map.get(c, c) for c in _closing_codes]
                    msg_cl = (
                        f"{ts_prefix()} [top] ★ 종가매매 {len(_closing_codes)}개 선정 ({CLOSING_STRATEGY}): "
                        f"{_closing_codes} / {closing_names}"
                    )
                    sys.stdout.write("\n")
                    _notify(msg_cl)
                    n_sel = len(_closing_codes)
                    per_alloc = INIT_CASH // n_sel if n_sel else 0
                    _notify(
                        f"{ts_prefix()} [종가매매선정] 자원배분 | "
                        f"총액={INIT_CASH:,}원 | 선정종목수={n_sel}개 | 종목당 배정액={per_alloc:,}원"
                    )
                    for i, c in enumerate(_closing_codes):
                        nm = _closing_code_info.get(c, {}).get("name", code_name_map.get(c, c))
                        _notify(f"{ts_prefix()} [종가매매선정] #{i+1} {nm}({c})")
                    _save_closing_buy_state(list(_closing_codes), dict(_closing_code_info), [])

            # ============================================================
            # 15:20 구독 전환: 기존 전부 해제 → 종가매매 종목으로 교체 (TOP5_ADD=False면 스킵)
            # ============================================================
            if TOP5_ADD and is_closing_switch and _closing_codes:
                _switch_to_closing_codes()
                logger.info(f"{ts_prefix()} [top] end time reached (종가매매 전환 완료), stopping")
                return

            # ============================================================
            # 일반 시간대 처리 (15:19/15:20 아닌 경우만, TOP5_ADD=True일 때)
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
    # 종가매매 종목 선정 (15:19) + 구독 전환/매수 (15:20)
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


def _save_accumulator(reason: str = "periodic") -> int:
    """
    _accumulator 전체를 FINAL_PARQUET_PATH 에 스냅샷 저장.

    - 스냅샷 방식: lock 안에서 리스트를 얕은 복사한 뒤 lock 밖에서 IO 수행
      → 저장 중에도 ingest_loop이 계속 append 가능 (lock 경합 최소화)
    - 원자적 저장: .tmp → rename (저장 도중 crash 시 이전 파일 보존)
    - recv_ts 기준 정렬 및 종목명(name) 컬럼 추가
    - _save_lock으로 동시 호출 방지 (signal + finally 중복 실행 시 에러 방지)
    """
    if not _save_lock.acquire(blocking=False):
        logger.info(f"[save] {reason} skipped (already saving)")
        return 0
    try:
        return _save_accumulator_inner(reason)
    finally:
        _save_lock.release()


def _save_accumulator_inner(reason: str) -> int:
    global _written_rows, _last_save_time
    with _accumulator_lock:
        if not _accumulator:
            _last_save_time = time.time()
            return 0
        snapshot = list(_accumulator)   # 얕은 복사 (pl.DataFrame은 불변)

    try:
        t0 = time.time()
        df = pl.concat(snapshot, how="diagonal_relaxed")
        if "recv_ts" in df.columns:
            df = df.sort("recv_ts")

        # 종목명 컬럼 추가
        if "name" not in df.columns:
            code_col = next(
                (c for c in ("mksc_shrn_iscd", "stck_shrn_iscd", "code") if c in df.columns),
                None,
            )
            if code_col:
                try:
                    name_map = _code_name_map()
                    df = df.with_columns(
                        pl.col(code_col)
                        .cast(pl.Utf8)
                        .str.zfill(6)
                        .map_elements(lambda c: name_map.get(c, ""), return_dtype=pl.Utf8)
                        .alias("name")
                    )
                except Exception:
                    pass

        # 원자적 저장: tmp 파일에 쓴 뒤 rename
        tmp_path = FINAL_PARQUET_PATH.with_suffix(".tmp.parquet")
        df.write_parquet(str(tmp_path), compression="zstd", use_pyarrow=True)
        tmp_path.replace(FINAL_PARQUET_PATH)

        n = len(df)
        t1 = time.time()
        with _lock:
            _written_rows = n
        _last_save_time = time.time()
        try:
            size_mb = FINAL_PARQUET_PATH.stat().st_size / (1024 * 1024)
        except Exception:
            size_mb = -1
        logger.info(
            f"[save] {reason}: rows={n} sec={t1-t0:.3f} "
            f"file={FINAL_PARQUET_PATH.name} mb={size_mb:.2f}"
        )
        _emit_save_done(n)
        return n
    except Exception as e:
        logger.error(f"[save] {reason} failed: {e}")
        logger.error(traceback.format_exc())
        return 0

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


def _init_indicator_buf(code: str) -> None:
    """신규 종목 구독 시 EMA·BB 인메모리 상태 초기화."""
    _ema_state[code] = {n: None for n in INDICATOR_MA_LINE}
    _price_buf[code] = deque(maxlen=_IND_MAX_WINDOW)


def _restore_state_on_startup() -> None:
    """
    재시작 시 당일 저장 파일(FINAL_PARQUET_PATH) 및 구 parts/*.parquet 를 읽어
    _accumulator 복원 + EMA·BB 지표 상태 복원.

    흐름:
      1) FINAL_PARQUET_PATH (새 아키텍처 스냅샷) 우선 로드
      2) 없으면 구 아키텍처 호환: PART_DIR/*.parquet 를 통합
      3) recv_ts 기준 오름차순 정렬 후 _accumulator 에 단일 DataFrame 으로 저장
      4) 종목별로 stck_prpr 를 _calc_indicators 에 재투입 → 지표 상태 복원
      5) 구 parts 파일은 backup 으로 이동 (중복 방지)

    호출 시점: codes 초기화·구독 직전 (WSS 연결 전)
    """
    global _accumulator, _accumulator_rows
    try:
        sources: list[str] = []
        parts: list[Path] = []

        # 새 아키텍처: 스냅샷 파일
        if FINAL_PARQUET_PATH.exists() and _is_valid_parquet(FINAL_PARQUET_PATH):
            sources.append(str(FINAL_PARQUET_PATH))
            logger.info(f"[restore] 스냅샷 파일 발견: {FINAL_PARQUET_PATH.name}")

        # 구 아키텍처 호환: parts 파일 수집
        parts = sorted(PART_DIR.glob("*.parquet"))
        if parts:
            sources.extend(str(p) for p in parts)
            logger.info(f"[restore] 구 parts 파일 {len(parts)}개 발견 → 통합")

        if not sources:
            logger.info("[restore] 복원할 파일 없음 → 0부터 시작")
            return

        # 파일마다 스키마(컬럼 구성/타입)가 다를 수 있으므로 개별 로드 후 diagonal_relaxed 병합
        # - extra_columns: 없는 컬럼 → null 채움
        # - 타입 불일치(e.g. Null vs String): supertype 으로 승격
        dfs: list[pl.DataFrame] = []
        for src in sources:
            try:
                dfs.append(pl.read_parquet(src))
            except Exception as _e:
                logger.warning(f"[restore] 파일 읽기 실패, 스킵: {src} → {_e}")
        if not dfs:
            logger.info("[restore] 읽을 수 있는 파일 없음 → 0부터 시작")
            return
        df = pl.concat(dfs, how="diagonal_relaxed")

        # 코드 컬럼 탐색
        code_col = None
        for c in ("mksc_shrn_iscd", "stck_shrn_iscd", "code"):
            if c in df.columns:
                code_col = c
                break
        if not code_col:
            logger.warning("[restore] code 컬럼 없음 → 스킵")
            return

        # recv_ts 기준 정렬 + 코드 정규화
        if "recv_ts" in df.columns:
            df = df.sort("recv_ts")
        df = df.with_columns(
            pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
        )

        total_rows = len(df)

        # _accumulator 복원 (단일 DataFrame으로 저장)
        with _accumulator_lock:
            _accumulator = [df]
            _accumulator_rows = total_rows

        # 지표 상태 복원 (stck_prpr 컬럼이 있을 때만)
        restored_codes: list[str] = []
        if "stck_prpr" in df.columns:
            df_ind = df.with_columns(
                pl.col("stck_prpr").cast(pl.Float64, strict=False).alias("stck_prpr")
            ).drop_nulls("stck_prpr")

            for df_code in df_ind.partition_by(code_col, maintain_order=True):
                code = str(df_code[code_col][0])
                if code not in _ema_state:
                    _init_indicator_buf(code)
                prices = df_code["stck_prpr"].to_list()
                for pr in prices:
                    if pr and pr > 0:
                        _calc_indicators(code, float(pr))
                restored_codes.append(code)

        # 구 parts 파일을 backup 으로 이동 (이미 accumulator 에 흡수됨)
        if parts:
            BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            for p in parts:
                try:
                    p.replace(BACKUP_DIR / p.name)
                except Exception:
                    pass
            logger.info(f"[restore] 구 parts {len(parts)}개 → backup 이동 완료")

        msg = (
            f"[restore] 복원 완료: {len(restored_codes)}종목 / {total_rows}행 "
            f"(parts={len(parts)}개, 스냅샷={'있음' if FINAL_PARQUET_PATH.exists() else '없음'})"
        )
        logger.info(msg)
        sys.stdout.write(f"{msg}\n")
        sys.stdout.flush()

    except Exception as e:
        logger.warning(f"[restore] 복원 실패 (무시하고 계속): {e}\n{traceback.format_exc()}")


def _calc_indicators(code: str, price: float) -> dict:
    """틱 1개 수신 시 EMA + 볼린저 밴드 계산.

    EMA: alpha = 2/(period+1), 첫 틱은 가격 그대로 초기값으로 설정.
    BB:  price_buf 누적값으로 rolling std 계산 (기간 미달 시 None).
    반환: {ma3, ma50, ..., bb_mid, bb_upper, bb_lower, bb_width} dict.
    """
    buf = _price_buf.get(code)
    if buf is None:
        return {}
    buf.append(price)

    state = _ema_state.get(code)
    if state is None:
        return {}

    ema_vals: dict[str, float] = {}
    for n in INDICATOR_MA_LINE:
        alpha = 2.0 / (n + 1)
        prev = state.get(n)
        if prev is None:
            state[n] = price
        else:
            state[n] = prev + alpha * (price - prev)
        ema_vals[f"ma{n}"] = state[n]

    bb_vals: dict[str, float | None] = {}
    if len(buf) >= INDICATOR_BB_PERIOD:
        window = list(buf)[-INDICATOR_BB_PERIOD:]
        mean = sum(window) / INDICATOR_BB_PERIOD
        variance = sum((x - mean) ** 2 for x in window) / (INDICATOR_BB_PERIOD - 1)
        std = math.sqrt(variance)
        bb_vals["bb_mid"]   = mean
        bb_vals["bb_upper"] = mean + INDICATOR_BB_K * std
        bb_vals["bb_lower"] = mean - INDICATOR_BB_K * std
        bb_vals["bb_width"] = INDICATOR_BB_K * 2 * std
    else:
        bb_vals["bb_mid"]   = None
        bb_vals["bb_upper"] = None
        bb_vals["bb_lower"] = None
        bb_vals["bb_width"] = None

    return {**ema_vals, **bb_vals}


def _check_str1_sell_conditions(
    result: pl.DataFrame,
    col_map: dict,
    code_col: str,
    kind: str,
) -> None:
    """
    ingest_loop에서 호출: Str1 실전 매도 조건 판단 → 조건 충족 시 sell 큐에 추가.

    [사전 모니터링 08:29~09:00] kind == "regular_exp"
      stck_prpr(예상체결가)의 prdy_ctrt < 0 → 시초가 하락 예상 → 시장가 매도 주문 선제 등록
    [정규장 09:00~] kind == "regular_real"
      ① bidp1 < wghn_avrg_stck_prc  (가중평균 하락)
      ② ma50  < ma500                (EMA 데드크로스)
    """
    if not _str1_sell_state:
        return

    is_premarket = (kind == "regular_exp")
    is_regular   = (kind == "regular_real")
    if not is_premarket and not is_regular:
        return

    df_tmp = result.with_columns(
        pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
    )

    pr_col      = col_map.get("stck_prpr") or col_map.get("antc_prce")
    prdy_col    = col_map.get("prdy_ctrt")
    wghn_col    = col_map.get("wghn_avrg_stck_prc")
    bidp1_col   = col_map.get("bidp1")
    oprc_col    = col_map.get("stck_oprc")

    global _premarket_last_summary_ts
    now_ts = time.time()
    newly_triggered: list[str] = []   # 이번 틱에서 처음 매도조건 진입한 종목

    for df_code in df_tmp.partition_by(code_col, maintain_order=True):
        code = str(df_code[code_col][0])
        with _str1_sell_state_lock:
            st = _str1_sell_state.get(code)
            if not st or st.get("sold"):
                continue
            # 사전 주문 넣은 상태
            if st.get("premarket_ordered"):
                if is_regular:
                    continue  # 09:00 이후, 시초가 체결 예정/체결됨
                # is_premarket: 예상가 복귀 시 취소 검사 (아래에서 처리)

        row = df_code.row(-1, named=True)  # Polars: 마지막 행을 dict로 반환

        if is_premarket:
            # ── 08:29~09:00: 예상체결가로 시초가 하락 감지 / 주문 취소 검사 ──
            antc = 0.0
            prdy = 0.0
            try:
                if pr_col:   antc = float(str(row.get(pr_col) or 0).replace(",", "") or 0)
                if prdy_col: prdy = float(str(row.get(prdy_col) or 0).replace(",", "") or 0)
            except (ValueError, TypeError):
                continue
            # 첫 틱에서 buy_price(T종가) 갱신
            if antc > 0 and abs(prdy) < 100:
                t_close = antc / (1 + prdy / 100)
                with _str1_sell_state_lock:
                    st2 = _str1_sell_state.get(code, {})
                    if st2.get("buy_price", 0) == 0 and t_close > 0:
                        _str1_sell_state[code]["buy_price"] = t_close
            with _str1_sell_state_lock:
                st2       = _str1_sell_state.get(code, {})
                buy_price = st2.get("buy_price", 0)
                name      = st2.get("name", code)
                qty       = int(st2.get("qty", 0))
                pm_ordered = st2.get("premarket_ordered", False)

            if pm_ordered:
                # 이미 장전 매도 주문 넣음 → 예상가 복귀 시 취소
                should_cancel, cancel_reason = check_premarket_cancel(antc, prdy, buy_price)
                if should_cancel:
                    _enqueue_str1_premarket_cancel(code, cancel_reason)
                # 모니터링은 "주문대기" 상태로 표시 (in_sell_cond=False, 유지)
                prev_watch = _premarket_watch.get(code, {})
                _premarket_watch[code] = {
                    **prev_watch,
                    "antc_prce": antc, "prdy_ctrt": prdy, "buy_price": buy_price,
                    "in_sell_cond": False, "name": name, "qty": qty,
                    "reason": "(주문대기·취소검사중)",
                    "first_sell_ts": prev_watch.get("first_sell_ts"),
                    "first_printed": prev_watch.get("first_printed", False),
                }
                continue

            should_sell, reason = check_premarket_sell(antc, prdy)

            # ── 장전 모니터링 상태 업데이트 ──
            prev_watch = _premarket_watch.get(code, {})
            was_in_cond = prev_watch.get("in_sell_cond", False)
            _premarket_watch[code] = {
                **prev_watch,
                "antc_prce":     antc,
                "prdy_ctrt":     prdy,
                "buy_price":     buy_price,
                "in_sell_cond":  should_sell,
                "name":          name,
                "qty":           qty,
                "reason":        reason if should_sell else "",
                "first_sell_ts": prev_watch.get("first_sell_ts") or (now_ts if should_sell else None),
                "first_printed": prev_watch.get("first_printed", False),
            }
            # 첫 진입 감지: 이번 틱에 처음으로 매도조건 충족
            if should_sell and not was_in_cond:
                newly_triggered.append(code)

            if should_sell:
                _enqueue_str1_sell(code, reason, antc)

        elif is_regular:
            # ── 09:00 이후: 가중평균 하락 / MA 데드크로스 ──
            bidp1 = 0.0; wghn = 0.0; oprc = 0.0
            try:
                if bidp1_col: bidp1 = float(str(row.get(bidp1_col) or 0).replace(",", "") or 0)
                if wghn_col:  wghn  = float(str(row.get(wghn_col)  or 0).replace(",", "") or 0)
                if oprc_col:  oprc  = float(str(row.get(oprc_col)  or 0).replace(",", "") or 0)
            except (ValueError, TypeError):
                continue
            # buy_price 첫 틱 보정 (regular_real에도 prdy_ctrt 있을 수 있음)
            if bidp1 > 0 and prdy_col:
                try:
                    prdy_v = float(str(row.get(prdy_col) or 0).replace(",", "") or 0)
                    if abs(prdy_v) < 100:
                        t_close = bidp1 / (1 + prdy_v / 100)
                        with _str1_sell_state_lock:
                            if _str1_sell_state.get(code, {}).get("buy_price", 0) == 0 and t_close > 0:
                                _str1_sell_state[code]["buy_price"] = t_close
                except (ValueError, TypeError):
                    pass
            # EMA 상태에서 ma50, ma500 조회
            ema = _ema_state.get(code, {})
            ma50  = ema.get(50)  or 0.0
            ma500 = ema.get(500) or 0.0
            should_sell, reason = check_realtime_sell(bidp1, wghn, ma50, ma500, oprc)
            if should_sell:
                _enqueue_str1_sell(code, reason, bidp1)

            # ── 추가 조건: 전일종가 이하 하락 시 즉시 매도 ──
            if not should_sell and prdy_col:
                try:
                    prdy_v = float(str(row.get(prdy_col) or 0).replace(",", "") or 0)
                    if prdy_v < 0:
                        _enqueue_str1_sell(code, f"전일종가하락(prdy_ctrt={prdy_v:.2f}%)", bidp1)
                except (ValueError, TypeError):
                    pass

    # ── 장전 모니터링 출력 ───────────────────────────────────────────────────
    if is_premarket and _premarket_watch:
        _print_premarket_monitor(newly_triggered, now_ts)


def _print_premarket_monitor(newly_triggered: list[str], now_ts: float) -> None:
    """
    장전 동시호가 모니터링 출력.

    ① 이번 틱에서 처음 매도조건 진입한 종목 → 즉시 출력 + 텔레그램
    ② 60초마다 → 전체 감시 종목 요약 출력 (매도조건 / 이상없음 구분)
    """
    global _premarket_last_summary_ts

    # ① 첫 진입 즉시 출력
    for code in newly_triggered:
        w = _premarket_watch.get(code, {})
        _premarket_watch[code]["first_printed"] = True
        msg = _fmt_premarket_alert_line(code, w, prefix="🔴 [장전매도조건 진입]")
        sys.stdout.write(f"\n{msg}\n")
        sys.stdout.flush()
        _notify(msg, tele=True)
        logger.info(msg)

    # ② 1분 주기 요약
    if (now_ts - _premarket_last_summary_ts) < PREMARKET_SUMMARY_INTERVAL:
        return
    _premarket_last_summary_ts = now_ts

    sell_lines:  list[str] = []
    hold_lines:  list[str] = []
    for code, w in sorted(_premarket_watch.items()):
        line = _fmt_premarket_alert_line(code, w)
        if w.get("in_sell_cond"):
            sell_lines.append(line)
        else:
            hold_lines.append(line)

    if not sell_lines and not hold_lines:
        return

    now_str = datetime.now(KST).strftime("%H:%M:%S")
    header  = f"{'='*60}\n[장전 예상체결가 모니터링] {now_str}"
    lines   = [header]
    if sell_lines:
        lines.append(f"  ▼ 매도조건 ({len(sell_lines)}종목) ─────────────────────")
        lines.extend(f"    {l}" for l in sell_lines)
    if hold_lines:
        lines.append(f"  ○ 이상없음 ({len(hold_lines)}종목) ─────────────────────")
        lines.extend(f"    {l}" for l in hold_lines)
    lines.append("=" * 60)
    msg = "\n".join(lines)

    sys.stdout.write(f"\n{msg}\n\n")
    sys.stdout.flush()
    logger.info(msg)
    # 텔레그램은 매도조건 종목이 있을 때만 전송
    if sell_lines:
        tele_lines = [f"[장전모니터 {now_str}]"]
        tele_lines.append(f"▼ 매도조건 {len(sell_lines)}종목")
        tele_lines.extend(sell_lines)
        _notify("\n".join(tele_lines), tele=True)


def _fmt_premarket_alert_line(code: str, w: dict, prefix: str = "") -> str:
    """장전 모니터링 1줄 포맷: 종목명, 보유수량, 예상가, 전일대비, 매수가, 손익, 사유."""
    name      = w.get("name", code)
    qty       = int(w.get("qty", 0))
    antc      = w.get("antc_prce", 0.0)
    prdy      = w.get("prdy_ctrt", 0.0)
    buy_p     = w.get("buy_price", 0.0)
    reason    = w.get("reason", "")
    diff      = antc - buy_p if antc > 0 and buy_p > 0 else 0.0
    diff_pct  = (antc / buy_p - 1) * 100 if buy_p > 0 else 0.0
    first_ts  = w.get("first_sell_ts")
    elapsed   = f" ({int(time.time()-first_ts)}초 경과)" if first_ts else ""

    parts = [name]
    parts.append(f"보유={qty}주")
    parts.append(f"예상가={antc:,.0f}" if antc > 0 else "예상가=N/A")
    parts.append(f"전일대비={prdy:+.2f}%")
    if buy_p > 0:
        parts.append(f"매수가={buy_p:,.0f}  손익≈{diff:+,.0f}({diff_pct:+.2f}%)")
    if reason:
        parts.append(f"사유={reason}{elapsed}")
    line = "  ".join(parts)
    return f"{prefix} {line}".strip() if prefix else line


def _print_premarket_order_summary() -> None:
    """08:59:50~09:00 장전 주문현황 정리 (주문대기/취소요청/이상없음 구분)."""
    global _premarket_order_summary_done
    if _premarket_order_summary_done:
        return
    _premarket_order_summary_done = True

    cancelled_done: list[str] = []   # 취소 완료 (로그에서)
    with _str1_sell_state_lock:
        cancelled_codes = {c[0] for c in _premarket_cancelled_log}
        order_wait: list[str] = []   # 주문대기 (09:00 시초가 체결 예정)
        order_ok: list[str] = []     # 이상없음 (주문 없음)
        cancel_req: list[str] = []   # 취소 요청 중
        for code, name, reason in _premarket_cancelled_log:
            cancelled_done.append(f"  {name}({code})  사유={reason}")
        for code, st in _str1_sell_state.items():
            if st.get("sold"):
                continue
            name = st.get("name", code_name_map.get(code, code))
            qty = int(st.get("qty", 0))
            ordno = str(st.get("ordno", "")).strip()
            if code in cancelled_codes:
                continue  # 취소완료는 위 cancelled_done에 이미 포함
            if st.get("premarket_cancel_requested"):
                cancel_req.append(f"  {name}({code}) 수량={qty}  ordno={ordno or '-'}  [취소요청중]")
            elif st.get("premarket_ordered"):
                order_wait.append(f"  {name}({code}) 수량={qty}주  ordno={ordno or '-'}  [09:00 시초가 체결 예정]")
            else:
                order_ok.append(f"  {name}({code}) 수량={qty}주  [주문 없음]")

    if not order_wait and not cancel_req and not order_ok and not cancelled_done:
        _premarket_order_summary_done = True
        return

    _premarket_order_summary_done = True
    now_str = datetime.now(KST).strftime("%H:%M:%S")
    lines = [f"{'='*60}", f"[장전 주문현황 정리] {now_str} (09:00 전 최종)", "=" * 60]
    if order_wait:
        lines.append(f"  ▼ 주문대기 ({len(order_wait)}종목) ─────────────────────")
        lines.extend(order_wait)
    if cancel_req:
        lines.append(f"  ◐ 취소요청중 ({len(cancel_req)}종목) ─────────────────────")
        lines.extend(cancel_req)
    if cancelled_done:
        lines.append(f"  ● 취소완료 ({len(cancelled_done)}종목) ─────────────────────")
        lines.extend(cancelled_done)
    if order_ok:
        lines.append(f"  ○ 이상없음 ({len(order_ok)}종목) ─────────────────────")
        lines.extend(order_ok)
    lines.append("=" * 60)
    msg = "\n".join(lines)
    sys.stdout.write(f"\n{msg}\n\n")
    sys.stdout.flush()
    logger.info(msg)
    if order_wait:
        _notify(f"[장전주문현황] 주문대기 {len(order_wait)}건 (09:00 시초가 체결 예정)", tele=True)


def writer_loop():
    """1분마다 _accumulator 전체를 FINAL_PARQUET_PATH 에 스냅샷 저장."""
    logger.info("[writer] started")
    while not _stop_event.is_set():
        try:
            time_ok = (time.time() - _last_save_time) >= SAVE_EVERY_SEC
            if time_ok:
                with _accumulator_lock:
                    has_data = bool(_accumulator)
                if has_data:
                    _save_accumulator("periodic")
        except Exception as e:
            logger.error(f"[writer] save error: {e}")
            logger.error(traceback.format_exc())
        time.sleep(0.5)

    # 종료 시 최종 저장
    try:
        _save_accumulator("shutdown")
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
    # 18:00 종료 시 scheduler가 _stop_event.set() + _request_ws_close() 호출.
    # kis_auth_llm.__subscriber는 wait_for(recv(), 60)로 1분마다 _close_requested 체크 → ws.close() 수행.
    # 데이터 수신 시 on_result가 호출되므로, 여기서 _stop_event 감지 시 ws.close() 즉시 스케줄.
    if _stop_event.is_set() and ws is not None:
        try:
            import asyncio
            asyncio.ensure_future(ws.close())
        except Exception:
            pass
        return

    if result is None or getattr(result, "empty", True):
        return

    # 수신 시점 컬럼명 소문자 통일 (ccnl_krx 대문자 vs exp_ccnl_krx 소문자 → concat 시 중복 방지)
    result.columns = [str(c).strip().lower() for c in result.columns]

    # H0STCNI0 체결통보: 별도 로그 저장 + 체결 시 state 갱신 (parquet 미포함)
    trid = str(tr_id)
    if trid == "H0STCNI0":
        recv_ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S.%f")
        _write_ccnl_notice_log(result, recv_ts)
        _on_ccnl_notice_filled(result, recv_ts)
        return
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
    # WSS 파서 단계에서 Polars로 변환 → ingest_loop는 Polars 네이티브로 처리
    result_pl = pl.from_pandas(result)
    _ingest_queue.put((result_pl, trid, kind, is_real, recv_ts))


def ingest_loop():
    global _last_print_ts, _last_summary_ts, _last_full_status_ts, _since_save_rows, _last_any_recv_ts, _accumulator_rows
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

        col_map = {c: c for c in result.columns}  # Polars: columns already lowercase strings
        code_col = col_map.get("mksc_shrn_iscd") or col_map.get("stck_shrn_iscd")
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

        # 모니터링 카운트 (Polars 네이티브)
        try:
            tmp = result.with_columns(
                pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
            )
            with _lock:
                for df_code in tmp.partition_by(code_col, maintain_order=True):
                    code = str(df_code[code_col][0])
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

        # 15:20~15:30 예상체결가 모니터링 (Polars DataFrame 그대로 전달)
        if kind == "regular_exp" and _get_mode() == RunMode.CLOSE_EXP:
            try:
                _run_closing_exp_monitor_tick(result, code_col, col_map)
            except Exception as e:
                logger.warning(f"{ts_prefix()} [종가모니터] tick 처리 오류: {e}")

        # 최근 전일대비율 캐시 (Polars 네이티브)
        try:
            prdy_ctrt_col = col_map.get("prdy_ctrt")
            if prdy_ctrt_col:
                tmp2 = result.with_columns(
                    pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
                )
                for df_code in tmp2.partition_by(code_col, maintain_order=True):
                    code = str(df_code[code_col][0])
                    val = df_code[prdy_ctrt_col][-1]
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
                df_save = result.with_columns([
                    pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col),
                    pl.lit(recv_ts).alias("recv_ts"),
                    pl.lit(trid).alias("tr_id"),
                    pl.lit(is_real).alias("is_real_ccnl"),
                ])
                chunks: list[pl.DataFrame] = []
                with _lock:
                    for df_code in df_save.partition_by(code_col, maintain_order=True):
                        code = str(df_code[code_col][0])
                        # ── 기술적 지표 계산 (EMA + BB) — Polars 네이티브 ──
                        pr_col = "stck_prpr"
                        if pr_col in df_code.columns:
                            ind_rows: list[dict] = []
                            for pr_val in df_code[pr_col]:
                                try:
                                    ind_rows.append(_calc_indicators(code, float(pr_val)))
                                except (TypeError, ValueError):
                                    ind_rows.append({})
                            if ind_rows:
                                ind_df = pl.DataFrame(ind_rows)
                                for col_name in ind_df.columns:
                                    df_code = df_code.with_columns(
                                        ind_df[col_name].alias(col_name)
                                    )
                        chunks.append(df_code)
                # _accumulator 는 _accumulator_lock 으로 보호 (락 분리로 핫패스 지연 최소화)
                if chunks:
                    with _accumulator_lock:
                        for chunk in chunks:
                            _accumulator.append(chunk)
                        total_n = sum(len(c) for c in chunks)
                        _accumulator_rows += total_n
            except Exception:
                logger.error("[ingest] accumulator append failed")
                logger.error(traceback.format_exc())

        # ── Str1 실전 매도 조건 체크 ─────────────────────────────────────────
        if STR1_SELL_ENABLED and _str1_sell_state:
            try:
                _check_str1_sell_conditions(result, col_map, code_col, kind)
            except Exception as _e:
                logger.debug(f"[str1_sell] 조건 체크 예외: {_e}")

        if print_option == 2 and (now - _last_print_ts) >= 1.0:
            _print_counts()
            for c in _per_sec_counts:
                _per_sec_counts[c] = 0
            _last_print_ts = now

        if (now - _last_summary_ts) >= SUMMARY_EVERY_SEC:
            with _lock:
                total_rows = sum(_total_counts.values())
                saved_rows = _written_rows
            accum_rows = _accumulator_rows

            logger.info(
            f"[summary] total_rows={total_rows} saved_rows={saved_rows} "
            f"accum_rows={accum_rows} parquet={FINAL_PARQUET_PATH.name}"
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
    # ingest 큐가 소진될 때까지 최대 8초 대기 (메모리 데이터 유실 방지)
    deadline = time.time() + 8.0
    while not _ingest_queue.empty() and time.time() < deadline:
        time.sleep(0.2)
    # accumulator 즉시 저장
    try:
        _save_accumulator("signal_shutdown")
    except Exception:
        pass
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
        # 08:59:29~09:00:05 57/59(30분 단일가)는 실시간 체결가(09:00 단일가 수신)
        single_codes = _single_price_codes & all_codes
        if single_codes and t >= dtime(8, 59, 29):
            return {
                exp_ccnl_krx: all_codes - single_codes,
                ccnl_krx: single_codes,
            }  # noqa: F405
        return {exp_ccnl_krx: all_codes}  # noqa: F405

    if dtime(9, 0) <= t < dtime(15, 20):
        # ── VI / 57·59(30분 단일가) / 일반 종목 분리 ──
        vi_codes = _vi_delayed_codes & all_codes
        single_codes = _single_price_codes & all_codes
        rest_codes = all_codes - vi_codes - single_codes

        # 57/59: XX:29:29~XX:00:05 / XX:59:29~XX:30:05 = 실시간 체결가, 그 외 예상체결가
        minute, second = t.minute, t.second
        single_real_window = (
            (minute in (29, 59) and second >= 29) or (minute in (0, 30) and second < 6)
        )
        single_real = single_codes if single_real_window else set()
        single_exp = single_codes - single_real

        result = {}
        exp_set = vi_codes | single_exp
        if exp_set:
            result[exp_ccnl_krx] = exp_set
        if rest_codes or single_real:
            result[ccnl_krx] = rest_codes | single_real
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

            # 재연결 시 접속키(approval_key) 갱신 - htsid 만료 방지
            if attempt > 1:
                try:
                    ka.auth_ws(svr="prod")
                    logger.info(f"{ts_prefix()} [ws] auth_ws 재발급 완료")
                except Exception as ae:
                    logger.warning(f"{ts_prefix()} [ws] auth_ws 재발급 실패: {ae}")

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
            # H0STCNI0 체결통보: 08:29:59~18:00 구독 (08:30 시간외종가 매수 대비, 계좌별 주문 결과 확인용)
            now_t = datetime.now(KST).time()
            if dtime(8, 29, 59) <= now_t < END_TIME:
                acc_key = _get_ccnl_notice_tr_key()
                if acc_key:
                    ka.KISWebSocket.subscribe(ccnl_notice, [acc_key])  # noqa: F405
                    _subscribed["ccnl_notice"] = {acc_key}
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
                    if getattr(rsp, "tr_id", "") == "H0STCNI0":
                        _notify(f"{ts_prefix()} [체결통보] H0STCNI0 구독 성공", tele=True)
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
    logger.info(f"[save_mode] {SAVE_MODE}")
    logger.info(f"[save] snapshot every {SAVE_EVERY_SEC}s -> {FINAL_PARQUET_PATH}")
    logger.info(f"[monitor] summary_every={SUMMARY_EVERY_SEC}s stale={STALE_SEC}s")
    logger.info(f"[parquet] -> {FINAL_PARQUET_PATH}")

    if datetime.now(KST).time() >= END_TIME:
        prefix = ts_prefix()
        sys.stdout.write(f"{prefix} 종료 시각 경과 → 저장된 스냅샷 확인 후 종료\n")
        sys.stdout.flush()
        logger.info(f"{prefix} 종료 시각 경과 → 스냅샷={FINAL_PARQUET_PATH}")
        _notify(f"{prefix} {PROGRAM_NAME} 종료 시각 경과로 종료", tele=True)
        raise SystemExit(0)

    # ── 시작 즉시 계좌 잔고조회 → 보유종목 출력 + balance_map 취득 ──────────
    _startup_balance_map = _print_startup_balance()

    # 재시작 시 미체결 매수주문 전부 취소 (OPEN_BUY_ORDER_CANCEL=True 시)
    _run_open_buy_order_cancel_on_startup()

    # 15:20~15:30 재시작 시 미처리 종가매수 주문 재시도 (state 저장 기반)
    _run_closing_buy_retry_on_startup()

    # Str1 매도 상태 복원 — 시작잔고 결과 전달 (중복 API 호출 없이 재사용)
    _load_str1_sell_state_on_startup(_startup_balance_map)

    # 당일 저장 파일 → _accumulator 복원 + EMA·BB 지표 상태 복원 (재시작 연속성)
    _restore_state_on_startup()

    # 워커 시작
    t_writer = threading.Thread(target=writer_loop, daemon=True)
    t_writer.start()

    t_ingest = threading.Thread(target=ingest_loop, daemon=True)
    t_ingest.start()

    # Str1 매도 워커 시작 (ingest → sell_queue → KIS API 주문)
    t_str1_sell = threading.Thread(target=_str1_sell_worker, daemon=True)
    t_str1_sell.start()

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
            # ingest 큐 소진 먼저 → 그 다음 writer flush (순서 중요: ingest→writer)
            t_ingest.join(timeout=10.0)
            t_writer.join(timeout=10.0)
            t_str1_sell.join(timeout=5.0)
            t_flags.join(timeout=3.0)
            t_sched.join(timeout=3.0)
            t_rest.join(timeout=3.0)
            t_price_watch.join(timeout=3.0)
            t_price_req.join(timeout=3.0)
            t_top.join(timeout=3.0)
        except Exception:
            pass
        # 종료 시 최종 스냅샷 저장 (writer_loop 가 이미 저장했을 수 있지만 재확인)
        try:
            with _accumulator_lock:
                has_data = bool(_accumulator)
            if has_data:
                _notify(f"{ts_prefix()} 최종 스냅샷 저장 중: {FINAL_PARQUET_PATH}", tele=False)
                _save_accumulator("final_shutdown")
                _notify(f"{ts_prefix()} 최종 스냅샷 저장 완료: {FINAL_PARQUET_PATH}", tele=True)
        except Exception as e:
            logger.error(f"[finally] 최종 저장 실패: {e}")
        _flush_overwrite_log()
        _notify(f"{ts_prefix()} {PROGRAM_NAME} 종료", tele=True)
        logger.info("=== WSS END ===")
