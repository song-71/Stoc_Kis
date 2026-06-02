"""
[260309] fix: 잔고조회 REST API를 별도 스레드로 분리 → ingest_loop 블로킹 해소
  - _print_opening_call_auction_monitor에서 _query_and_print_balance 동기 호출 → threading.Thread로 비동기 실행
  - 매 60초 잔고조회 시 ingest_loop 2-3초 멈춤 → 수신 데이터 처리/저장 지연 해소

시간대별로 서로 다른 TR 호출코드가 적용됨.
    정규장 실시간 체결가: ccnl_krx
    정규장 예상 체결가: exp_ccnl_krx
    시간외 실시간 체결가: overtime_ccnl_krx
    시간외 예상 체결가: overtime_exp_ccnl_krx

# 국내주식 시간대별 주문구분코드(ORD_DVSN) 참고
| 시간대           | 구분            | 체결방식       | ORD_DVSN                        | 가격(ORD_UNPR)     | 비고           |
| --------------- | -------------- | -------------- | ------------------------------- | ------------------ | -------------- |
| 08:30~08:40     | 장전 시간외 종가  | 즉시 매칭       | 05(장전시간외)                    | 0 (가격 미입력)    | 전일 종가 고정 |
| 08:30~09:00     | 시초가 동시호가   | 09:00 일괄     | 00(지정가) 01(시장가)             | 지정가/0           | 시가 결정      |
| 09:00~15:20     | 정규장           | 실시간 체결     | 00(지정가) 01(시장가)             | 지정가/0           | 일반 매매      |
| 15:20~15:30     | 종가 동시호가     | 15:30 일괄     | 00(지정가) 01(시장가)             | 지정가/0           | 종가 결정      |
| 15:40~16:00     | 장후 시간외 종가  | 즉시 매칭        | 06(장후시간외)                    | 0 (가격 미입력)    | 당일 종가 고정 |
| 16:00~18:00     | 시간외 단일가     | 10분 단위 일괄   | 07(시간외단일가)                  | 가격 입력 필수     | 시장가 불가    |

스케줄 모드별 구독 구성 현황
    08:29~08:59: exp_ccnl_krx
    08:59~09:00: exp_ccnl_krx + ccnl_krx
    09:00~15:19: ccnl_krx
    15:20~15:30: ccnl_krx + exp_ccnl_krx
    15:59~18:00: overtime_exp_ccnl_krx + overtime_ccnl_krx

     KIS API 검토                                                                                                                                                                         
                                                         
  ┌───────────────┬──────────────────────────────┬────────────────────────────────────────────────┬─────────┐                                                                    
  │     TR_ID     │           endpoint           │                        데이터                   │ 적합도   │
  ├───────────────┼──────────────────────────────┼────────────────────────────────────────────────┼─────────┤                                                                    
  │ FHKST01010900 │ inquire-investor             │ 종목별 일별 외인/기관 매매 추이 (수량+금액, 약 30일) │ ⭐ 최적  │
  ├───────────────┼──────────────────────────────┼────────────────────────────────────────────────┼─────────┤
  │ FHPST01710000 │ 외인 순매수 상위 종목 (랭킹) │ 일자별 상위 N개만                                    │ 부분     │                                                                    
  ├───────────────┼──────────────────────────────┼────────────────────────────────────────────────┼─────────┤                                                                    
  │ FHKST03010100 │ 외인기간별 매매 추이         │ 종목별 분기별/장기                                   │ 보조    │                                                                    
  └───────────────┴──────────────────────────────┴────────────────────────────────────────────────┴─────────┘ 


=> 현재 지정된 목록과 초기 및 30분 간격 top30리스트로 5개씩 추가한 목록에 대해 최대 41개 범위내에서 구독 지속
=> 이 부분까지 잘 작동됨(지정된 목록 + 30분 단위 top30의 5개 종목 추가, 매일 자동화 잘 됨.)
  * 다만 매일 기본 종목 목록은 수기로 수정해야 함.

시장가 매수 시 증거금 계산  => 확인 필요
  - 장 중 : 현재가(또는 예상 체결가) 기준으로 증거금 산정
  - (예외) 다음과 같은 경우에는 상한가 기준 증거금 적용 가능:
    * VI 직후 단일가 전환 구간
    * 종가 동시호가 구간
    * 30분 단일가 매매 종목
    * 변동성 매우 큰 종목
    * 내부 리스크 관리 상 보수적 적용 종목

"57(관리종목)" "59(단기과열)" => 이 코드가 맞는지 확인 필요

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
페이퍼코리아
모헨즈
참엔지니어링
"""

# Top5 추가 구독 (False면 CODE_TEXT만 18:00까지 수신, Top50 다운/저장만 수행)
TOP5_ADD = True   # False: Top50 다운·저장 O, Top5 추가/해제/종가매매전환/미수신구독해제 X

# 한도 초과 시 폴백 계정 사용 여부 (True: 한도 초과 시 계정2(syw_2)로 재시도)
USE_FALLBACK_ACCOUNT = False

# 15:20 종가매수 옵션 (True: 15:19 선정 종목에 init_cash 균등분배 시장가=상한가 매수, 선정조건=Select_Tr_target_list)
CLOSE_BUY = True

# Str1 실전 매도 옵션 (True: 종가매수 체결 종목에 대해 다음날 실시간 매도 전략 적용)
STR1_SELL_ENABLED = True
# VI 발동 매매 옵션: "test_mode"=1주만 매수/매도, "run_mode"=정상 매수/매도, "off"=적용 제외
VI_TRADE_MODE = "off"
# 상한가 스톱 연속 이탈 확인 틱 수 (서버 스톱지정가 대신 클라이언트에서 N틱 연속 확인 후 매도)
STOP_LIMIT_CONFIRM_TICKS = 30
# 매수가 손절 N틱 연속 이탈 확인 (단일 틱 fake 가격 차단). 임계 회복 시 카운터 리셋.
LOSS_CONFIRM_TICKS = 30
# 09:00:00 ~ 09:00:00+OPENING_GRACE_SEC 사이는 손절 임계를 OPENING_GRACE_LOSS_PCT 로 강화
# (우선주 등 thin liquidity 첫 틱 1주 체결로 인한 fake 손절 방지).
OPENING_GRACE_SEC = 30
OPENING_GRACE_LOSS_PCT = 0.07

# ─────────────────────────────────────────────────────────────────────────────
# [v_1 신규] 상한가 근접 매수 전략 (문서: docs/01. up_limit_buy_str_260420.md)
#   - 25~28% 구간 다중 필터 AND 통과 시 매수
#   - 단계적 청산: 29% 도달 → 28% 하회 절반, 27% 하회 전량
#   - 손절 -3%, 상한가 도달 후 대폭락 -5% 강제 청산
#   - 가용현금 전량 1종목 집중 (460K 시드 가정, 동시 보유 1종목)
# (주의: 이 섹션은 파일 최상단이라 하단 import 전 — datetime 은 tuple 로 저장)
# [260427] Strategy A-v5 활성화 — v4(28% 5분유지) 폐지, 구독 즉시 ma10 상회 시 시장가 매수
UPLIMIT_V4_ENABLED = True           # (호환) v4 분기 게이트 — 내부 로직은 v5 로 교체됨
UPLIMIT_V5_ENABLED = True           # v5 (구독 즉시 매수) 활성
UPLIMIT_BUY_ENABLED = True          # False: 전략 비활성 (회귀 테스트용)
UPLIMIT_MAX_DAILY_BUYS = 3          # 일일 신규 매수 최대 횟수 (신규 종목, 재매수는 별도)
UPLIMIT_BUY_START_HM = (9, 30)      # 매수 허용 시작 (hour, minute) — 참고용
UPLIMIT_BUY_END_HM = (14, 50)       # 신규 매수 금지
UPLIMIT_CLOSE_TIME_HM = (15, 10)    # 미도달 청산 시각 (Exit 내 처리)
UPLIMIT_MAX_POSITIONS = 3           # [260423] 동시 보유 상한 — 3종목 분산
UPLIMIT_VP_TTL_SEC = 30             # 체결강도 캐시 TTL
UPLIMIT_ENTRY_TICKS = 2             # (호환, v5 시장가에선 미사용)

# [260423] Top30 구독 편입/해제 기준
UPLIMIT_SUBSCRIBE_MIN_CTRT = 25.0   # 이 값 이상이면 Top30 에서 자동 구독 추가
UPLIMIT_UNSUBSCRIBE_CTRT = 20.0     # 미매수 구독 종목이 이 값 미만이면 자동 해제
UPLIMIT_DIVERSIFY_N = 3             # 시드 분산 종목 수 (예: 460K / 3 ≈ 153K/종목)
UPLIMIT_REACH_UPPER_PCT = 29.5      # 상한가 근접 인정 (스톱 지정가 주문 발주 기준)

# [260427] v5 신규 파라미터
UPLIMIT_V5_MIN_TICKS = 10           # ma10 누적 최소 틱 수 (이 미만이면 매수 보류)
UPLIMIT_V5_TRAIL_ARM_CTRT = 29.0    # max_prdy_ctrt 이 값 도달 후 트레일 활성
UPLIMIT_V5_TRAIL_PCT = 0.03         # 트레일스톱 하락률 (highest × 0.97)
UPLIMIT_V5_LOW_LIQ_VALUE = 5_000_000_000  # 저거래 임계 (전일/당일 거래대금 50억)
UPLIMIT_V5_OBSERVE_BAND_LOW = 20.0  # 관찰 구간 하한 (20~25% 는 매수 안 하고 신호만 로그)
UPLIMIT_V5_OBSERVE_BAND_HIGH = 25.0
UPLIMIT_V5_BUY_BAND_HIGH = 30.0     # 매수 상한 (이 값 이상은 limitup_reached)

# [v_1 신규] 종가매수 폭락 방지 필터 (문서: docs/02. Top30_str.md Part B)
CLOSING_CRASH_FILTER_ENABLED = True

# [260423] Strategy B/C — WSS 틱 EMA + BB 기반 보완 전략
# 초기값: 로그만 찍고 매매는 안 함 (백테스트 후 True 전환)
MA_TREND_ENABLED = False       # Strategy B (ma50>ma500 골든크로스, ma10<ma500 데드크로스)
BB_EXPANSION_ENABLED = False   # Strategy C (BB 압축→확장 + 하단 이탈-복귀 반등)

# [260523] Strategy str2 게이트.
#   기본 False. config.json 의 strategy_swap.module == "ws_realtime_tr_str2"
#   또는 config["str2"]["enabled"] == true 일 때만 활성.
#   _refresh_str2_enabled() 가 _check_strategy_swap 폴링 시 갱신.
#   이 값이 False 인 한 _check_str2_from_tick 은 즉시 return → 기존 거동 바이트 동일.
STR2_ENABLED = False
STR2_VI_TRADE_MODE = "run_mode"   # "off"/"test_mode"(1주)/"run_mode"(정상) — str2 매수 수량 정책
# str2 시드: 동시 보유 분산 종목 수 (가용현금 / N 으로 종목당 투자금 결정)
STR2_DIVERSIFY_N = 3

# 직전 영업일 종가매수 선정 종목을 당일 base codes로 자동 사용
AUTO_CODE_FROM_PREV_CLOSING = True

# 재시작 시 미체결 매수주문 전부 취소 (True: 정정취소가능주문조회 후 취소, 주문번호 기록 없어도 가능)
OPEN_BUY_ORDER_CANCEL = True
# 08:30~08:40 시간외 종가 추가 매수 (True: 전일 미매수 종목에 전일종가 지정가 ORD_DVSN=02 매수)
MORNING_EXTRA_CLOSING_PR_BUY = False
# 16:00~18:00 시간외 단일가 매수 (False: 16:01에 조기 종료)
OVERTIME_SINGLE_PRICE_BUY = False

"""
실행/모니터링/종료 명령어

재시작 (기존 종료 후 업데이트 버전 즉시 실행):
  /home/ubuntu/Stoc_Kis/ws_run_restart_wss_trading.py
    => restart_wss_trading.sh을 재시작해줌.

nohup 실행:
  nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/ws_realtime_trading.py >> /home/ubuntu/Stoc_Kis/out/wss_realtime_trading.out 2>&1 &

로그 모니터링:
  <노협 백그라운드 실행 모니터링>
  tail -f /home/ubuntu/Stoc_Kis/out/wss_realtime_trading.out
  tail -n 2000 -f /home/ubuntu/Stoc_Kis/out/wss_realtime_trading.out

  파일이 크면 VS Code에서 직접 여는 게 스크롤하기 편함.(단, 현재 시점 내용만 가져올 수 있음.)
  code /home/ubuntu/Stoc_Kis/out/wss_realtime_trading.out

  <터미널 실행 모니터링>
  tail -f /home/ubuntu/Stoc_Kis/out/logs/wss_TR_{yymmdd}.log

프로세스 확인:
  pgrep -af ws_realtime_trading.py

일괄 종료:현재 떠있는 여러 프로세스를 동시에 종료하는 명령어
  pkill -f ws_realtime_trading.py
  
  <정말 안죽으면>
  pkill -9 -f ws_realtime_trading.py

종료 확인:
  pgrep -af ws_realtime_trading.py || echo "no process"



wss 데이터 저장위치 : /home/ubuntu/Stoc_Kis/data/wss_data/

cd /home/ubuntu/Stoc_Kis
git add ws_realtime_trading.py
git commit -m "feat: 260226_1810, 셧다운시 메모리 파일만 저장토록 개선"
git push

cd /home/ubuntu/Stoc_Kis
git add -A
git commit -m "feat: ws_realtime_trading.py 전체 파일 커밋"
git push
"""

import os
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

LOG_ID = "TR"  # 로그/텔레그램 메시지 식별용 (여러 파일 동시 실행 시 구분)

# ORD_DVSN 시간대별 매핑 상수
# 가격 미입력(ORD_UNPR=0) 대상: 01(시장가), 05(장전시간외), 06(장후시간외)
ORD_DVSN_NO_PRICE = frozenset({"01", "05", "06"})

# 시간대별 ORD_DVSN 매핑
ORD_DVSN_BY_TIME = {
    "pre_market_otc":    "05",   # 08:30~08:40 장전시간외종가, 가격 미입력
    "pre_market_auction":"00",   # 08:30~09:00 시초가 동시호가, 00=지정가/01=시장가
    "regular":           "00",   # 09:00~15:20 정규장, 00=지정가/01=시장가
    "closing_auction":   "00",   # 15:20~15:30 종가 동시호가, 00=지정가/01=시장가
    "post_market_otc":   "06",   # 15:40~16:00 장후시간외종가, 가격 미입력
    "overtime_single":   "07",   # 16:00~18:00 시간외단일가, 가격 입력 필수
}

def ts_prefix() -> str:
    return datetime.now(KST).strftime(f"[%y%m%d_%H%M%S_{LOG_ID}]")

def _get_code_version() -> str:
    """[260427] 기동 시 코드 버전 식별자 반환.
    git 저장소 내라면 'short_hash (commit_date) [+dirty]', 아니면 'unknown'.
    재시작 시 옛 코드 실수로 운영하는 사고 방지용.
    """
    try:
        import subprocess as _sp
        repo_dir = str(Path(__file__).resolve().parent)
        h = _sp.run(
            ["git", "-C", repo_dir, "rev-parse", "--short=10", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        if h.returncode != 0:
            return "unknown"
        sha = h.stdout.strip()
        d = _sp.run(
            ["git", "-C", repo_dir, "log", "-1", "--format=%ci", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        date = d.stdout.strip().split(" ")[0] if d.returncode == 0 else "?"
        # working tree dirty 확인 (커밋 안 된 변경 있음)
        s = _sp.run(
            ["git", "-C", repo_dir, "status", "--porcelain", "ws_realtime_trading.py", "ws_realtime_tr_str1.py", "kis_auth_llm.py"],
            capture_output=True, text=True, timeout=2,
        )
        dirty = " +dirty" if (s.returncode == 0 and s.stdout.strip()) else ""
        return f"{sha} ({date}){dirty}"
    except Exception:
        return "unknown"


def _notify(msg: str, tele: bool = False) -> None:
    logger.info(msg)
    sys.stdout.write("\r\033[2K" + msg + "\n")
    sys.stdout.flush()
    if tele:
        try:
            with open(TELE_LOG_PATH, "a", encoding="utf-8") as _tf:
                _tf.write(f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n")
        except Exception:
            pass
        if tmsg is not None:
            try:
                tmsg(msg, "-t")
            except Exception:
                pass


def _notify_async(msg: str) -> None:
    """[260521] 핫패스 보호용 비동기 텔레그램 통지.

    - 파일 로그(logger) + TELE_LOG_PATH 기록(시간정보 포함)은 **동기**로 즉시 남긴다
      (프로젝트 규칙: 텔레그램 메시지는 자체 로그에 시간정보와 함께 반드시 기록).
    - 실제 텔레그램 HTTP 전송(tmsg)만 데몬 스레드로 fire-and-forget 하여,
      네트워크 지연/장애가 호출 스레드(예: _str1_sell_worker)를 막지 않게 한다.
    - 단발 주문 직후 통지에 사용. 매매 안전성과 무관한 부가 동작 전용.
    """
    logger.info(msg)
    sys.stdout.write("\r\033[2K" + msg + "\n")
    sys.stdout.flush()
    # TELE_LOG_PATH 기록은 동기로 즉시 (누락 방지)
    try:
        with open(TELE_LOG_PATH, "a", encoding="utf-8") as _tf:
            _tf.write(f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n")
    except Exception:
        pass
    # 텔레그램 HTTP 전송만 스레드로 분리 (timeout은 telegMsg.tmsg 내부에서 보장)
    if tmsg is not None:
        def _send() -> None:
            try:
                tmsg(msg, "-t")
            except Exception:
                pass
        try:
            threading.Thread(target=_send, name="tele-notify", daemon=True).start()
        except Exception as _te:
            # 스레드 생성 실패 시 최후수단으로 동기 전송 (드문 케이스)
            logger.warning(f"_notify_async 스레드 생성 실패, 동기 전송 fallback: {_te}")
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
SUB_CFG_PATH = SCRIPT_DIR / "config_wss_1.json"  # 구독 관리 전용 (trading)

LOG_DIR = SCRIPT_DIR / "out" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

today_yymmdd = datetime.now(KST).strftime("%y%m%d")
TELE_LOG_PATH = LOG_DIR / f"{today_yymmdd}_telegram.log"
FINAL_PARQUET_PATH = OUT_DIR / f"{today_yymmdd}_wss_data.parquet"
PART_DIR = OUT_DIR / "parts"
PART_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR = OUT_DIR / "backup"
SNAPSHOT_PATH = OUT_DIR / f"{today_yymmdd}_indicator_snapshot.parquet"
SNAPSHOT_INTERVAL_SEC = 300  # 지표 스냅샷 저장 간격 (5분)
LOG_PATH = LOG_DIR / f"wss_TR_{today_yymmdd}.log"
CCNL_NOTICE_CSV_PATH = LOG_DIR / f"ccnl_notice_{today_yymmdd}.csv"

# 거래장부 CSV (일자별 누적, 하루치 모든 주문/체결 기록)
LEDGER_DIR = SCRIPT_DIR / "data" / "ledger"
LEDGER_DIR.mkdir(parents=True, exist_ok=True)
LEDGER_PATH = LEDGER_DIR / f"trade_ledger_{today_yymmdd}.csv"
LEDGER_COLS = [
    "date", "time", "code", "name", "cano_alias",
    "order_type",    # buy_order / sell_order / buy_fill / sell_fill / buy_cancel
    "ord_dvsn",      # 시장가/지정가/시간외종가 등
    "stck_prpr",     # 현재가 (주문 시 현재가, 체결 시 체결가)
    "buy_price",     # 매수가 (시장가=0, 지정가=주문가)
    "sell_price",    # 매도가 (시장가=0, 지정가=주문가)
    "order_qty",     # 주문 수량
    "fill_qty",      # 체결 수량
    "amount",        # 금액 (price × fill_qty)
    "ret",           # 수익률 (sell_price/buy_price - 1), sell_fill 시 계산
    "profit",        # 수익액 (sell_price*qty - buy_price*qty), sell_fill 시 계산
    "prdy_ctrt",     # 전일대비율 (%)
    "reason",        # 매매 사유 (_주문/_체결 접미사로 구분)
    "ord_no",        # 주문번호
    "note",
]
_ledger_lock = threading.Lock()
_cano_alias_map: dict[str, str] = {}  # cano → alias (체결통보 시 계좌 식별용)

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

# [260513] websockets UTF-8 strict 디코딩 lenient 패치
# ────────────────────────────────────────────────────────────────────────
# KIS WSS 가 깨진 한글 바이트(invalid UTF-8 시퀀스)가 섞인 텍스트 프레임을 송신하면
# python websockets 가 strict 디코딩에 실패 → UnicodeDecodeError → fail_connection
# (close code 1007) → 연결 끊김. 1007 자체는 치명적이진 않으나 직후 SDK 재시도가
# 폭주하면 KIS throttle 모드(invalid approval storm)로 빠짐. 깨진 1글자 손실을
# 감수하고 errors="replace" 로 디코딩하여 1007 발생 자체를 차단한다.
#
# wrap 대상: WebSocketCommonProtocol.read_message
#   - 단일 프레임 경로(라인 1030: frame.data.decode())
#   - fragmentation 경로(라인 1036~1037: codecs.getincrementaldecoder("utf-8", errors="strict"))
#   둘 다 read_message 안에서 발생하므로 한 번에 lenient 화 가능.
#   read_data_frame 은 raw Frame 만 반환하므로 wrap 대상으로 부적합.
#
# site-packages 직접 수정은 금지(패키지 업데이트 시 사라짐) — monkey-patch 로 처리.
try:
    import codecs as _ws_codecs
    import websockets.legacy.protocol as _ws_lp

    _orig_read_message = _ws_lp.WebSocketCommonProtocol.read_message

    async def _lenient_read_message(self):
        try:
            return await _orig_read_message(self)
        except UnicodeDecodeError as e:
            # 단일 프레임 경로에서 strict decode 실패 — 같은 데이터를 lenient 로 재시도 불가
            # (이미 frame 이 소비됨). 빈 문자열 반환하여 상위 루프가 다음 메시지로 진행.
            try:
                logger.warning(
                    f"{ts_prefix()} [ws][utf8-lenient] decode skip "
                    f"({type(e).__name__}: {str(e)[:120]})"
                )
            except Exception:
                pass
            return ""

    _ws_lp.WebSocketCommonProtocol.read_message = _lenient_read_message

    # fragmentation 경로(read_message 내부의 incremental decoder)는 strict 디코더가
    # decoder.decode() 시점에 UnicodeDecodeError 를 raise → 그 예외도 위 wrap 의
    # except 에서 흡수됨(같은 read_message frame 안에서 발생). 추가 패치 불필요.
    #
    # 추가 안전망: getincrementaldecoder 호출을 lenient 로 강제 — fragment 도중 1007
    # 으로 끊기지 않고 ? 치환 후 정상 메시지 일부 회수 가능.
    _orig_get_inc_dec = _ws_codecs.getincrementaldecoder

    def _lenient_get_inc_dec(encoding):
        factory = _orig_get_inc_dec(encoding)
        if encoding.lower().replace("_", "-") in ("utf-8", "utf8"):
            def _factory(errors="strict"):
                # websockets 가 errors="strict" 로 부르더라도 replace 로 강제
                return factory(errors="replace")
            return _factory
        return factory

    # NOTE: codecs.getincrementaldecoder 자체를 글로벌 치환하면 다른 모듈도 영향받음.
    # → websockets.legacy.protocol 모듈 내의 codecs 참조만 치환.
    _ws_lp.codecs.getincrementaldecoder = _lenient_get_inc_dec

    logger.info(f"{ts_prefix()} [ws][utf8-lenient][legacy] monkey-patch applied (read_message — 현 SDK 미사용 경로, 무해 안전망)")
except Exception as _patch_err:
    logger.error(f"{ts_prefix()} [ws][utf8-lenient][legacy] monkey-patch FAILED: {_patch_err}")

# [260520] websockets *새 asyncio API* UTF-8 lenient 패치 (1007 근원 차단 — 실효 버전)
# ────────────────────────────────────────────────────────────────────────
# 위 legacy(read_message) 패치는 KIS SDK 가 실제로 쓰지 않는 경로다. SDK(kis_auth_llm.py:818)
# 는 websockets.connect() → await ws.recv() → websockets.asyncio API 를 사용한다(websockets 15.x).
# 이 경로의 UTF-8 strict 디코드는 Assembler.get() 의 `data.decode()` 에서 발생하며, 깨진 한글
# 바이트에서 UnicodeDecodeError → Connection.recv 가 protocol.fail(CloseCode.INVALID_DATA) 호출
# → "sent 1007 (invalid frame payload data) invalid start byte at position N" 로 연결이 끊긴다.
#   ※ 260513~260520 로그 실측: legacy 패치 0건 catch, 1007 은 9건 발생 → 경로 불일치 확정.
# 정상 프레임은 strict 그대로(동작/성능 불변) 두고, 깨진 프레임에서만 errors="replace" 로
# 1글자만 버려 연결을 유지한다. site-packages 직접 수정 금지 → monkey-patch.
# 버전 가드: Assembler.get 본문을 복제하므로 구조가 다른 버전에서는 적용하지 않는다.
try:
    import codecs as _ws_codecs2
    import websockets as _ws_pkg
    import websockets.asyncio.messages as _ws_msgs

    _ws_ver = getattr(_ws_pkg, "__version__", "0")
    if not _ws_ver.startswith("15."):
        logger.warning(
            f"{ts_prefix()} [ws][utf8-lenient-asyncio] websockets={_ws_ver} (15.x 아님) → "
            f"Assembler.get 패치 skip (본문 구조 불일치 위험)"
        )
    else:
        _last_lenient_skip_ts = [0.0]  # rate-limit 용 (mutable closure)
        # [260602] decode skip 근본원인 진단 로깅 (rate-limit 무시, 최초 N건만 상세)
        #   - 발생이 08:50~08:59 예상체결(H0STANC0) 프레임에만 집중 → CP949 한글 유입 의심.
        #   - 실패 프레임을 버리기 전에 (tr_id / 프레임길이 / 에러offset 주변 hex /
        #     cp949 재디코드 샘플) 을 남겨 truncation vs CP949 인코딩 유입을 확정한다.
        _lenient_diag_count = [0]
        _LENIENT_DIAG_MAX = 40

        async def _lenient_assembler_get(self, decode=None):
            # 원본 websockets.asyncio.messages.Assembler.get(15.0.1) 본문 복제 —
            # 마지막 텍스트 디코드만 lenient 처리. 그 외 로직은 원본과 동일.
            if self.get_in_progress:
                raise _ws_msgs.ConcurrencyError("get() or get_iter() is already running")
            self.get_in_progress = True
            try:
                frame = await self.frames.get(not self.closed)
                self.maybe_resume()
                assert frame.opcode is _ws_msgs.OP_TEXT or frame.opcode is _ws_msgs.OP_BINARY
                if decode is None:
                    decode = frame.opcode is _ws_msgs.OP_TEXT
                frames = [frame]
                while not frame.fin:
                    try:
                        frame = await self.frames.get(not self.closed)
                    except _ws_msgs.asyncio.CancelledError:
                        self.frames.reset(frames)
                        raise
                    self.maybe_resume()
                    assert frame.opcode is _ws_msgs.OP_CONT
                    frames.append(frame)
            finally:
                self.get_in_progress = False
            data = b"".join(frame.data for frame in frames)
            if decode:
                try:
                    return data.decode()                        # 정상 프레임: 기존과 100% 동일
                except UnicodeDecodeError as _e:
                    _now_ts = time.time()
                    # ── [260602] 근본원인 진단: 최초 _LENIENT_DIAG_MAX 건은 rate-limit 무시하고 상세 기록 ──
                    if _lenient_diag_count[0] < _LENIENT_DIAG_MAX:
                        _lenient_diag_count[0] += 1
                        try:
                            _st = _e.start
                            # TR 식별: 정상부 프레임 헤더 "0|H0STANC0|001|..." 는 ASCII → 안전 디코드
                            _hdr = data[:48].decode("ascii", "replace")
                            _trid = _hdr.split("|")[1] if "|" in _hdr else "?"
                            # 에러 offset 주변 ±16바이트 hex
                            _lo = max(0, _st - 16)
                            _win = data[_lo:_st + 16]
                            _hex = " ".join(f"{b:02x}" for b in _win)
                            # 깨진 바이트 자체
                            _bad = data[_st:_st + 4]
                            _bad_hex = " ".join(f"{b:02x}" for b in _bad)
                            # CP949(EUC-KR) 재디코드 시도 → 멀쩡한 한글이 나오면 인코딩 유입 확정
                            _cp949 = data[max(0, _st - 8):_st + 24].decode("cp949", "replace")
                            logger.warning(
                                f"{ts_prefix()} [ws][utf8-diag #{_lenient_diag_count[0]}] "
                                f"tr_id={_trid} len={len(data)} reason='{_e.reason}' offset={_st} "
                                f"bad=[{_bad_hex}] hex@±16=[{_hex}] cp949='{_cp949}'"
                            )
                        except Exception as _diag_e:
                            try:
                                logger.warning(f"{ts_prefix()} [ws][utf8-diag] 진단 로깅 실패: {_diag_e}")
                            except Exception:
                                pass
                        _last_lenient_skip_ts[0] = _now_ts
                    elif _now_ts - _last_lenient_skip_ts[0] >= 30.0:  # 상세 소진 후: 30s 1회 요약
                        _last_lenient_skip_ts[0] = _now_ts
                        try:
                            logger.warning(
                                f"{ts_prefix()} [ws][utf8-lenient-asyncio] decode skip "
                                f"({_e.reason} at {_e.start}) → ?치환 후 연결 유지 (1007 차단)"
                            )
                        except Exception:
                            pass
                    return data.decode("utf-8", "replace")      # 깨진 바이트만 ? — 연결 유지
            else:
                return data

        _ws_msgs.Assembler.get = _lenient_assembler_get

        # get_iter(스트리밍 프래그먼트) 안전망: 모듈 전역 UTF8Decoder 를 lenient factory 로 치환.
        # SDK recv() 는 get 만 쓰지만 완성도 위해 함께 처리.
        def _lenient_utf8_decoder_factory(errors="replace"):
            return _ws_codecs2.getincrementaldecoder("utf-8")(errors="replace")
        _ws_msgs.UTF8Decoder = _lenient_utf8_decoder_factory

        logger.info(
            f"{ts_prefix()} [ws][utf8-lenient-asyncio] monkey-patch applied "
            f"(Assembler.get + UTF8Decoder, websockets={_ws_ver})"
        )
except Exception as _patch_err2:
    logger.error(f"{ts_prefix()} [ws][utf8-lenient-asyncio] monkey-patch FAILED: {_patch_err2}")

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

## 이전 로그 삭제 비활성화 (Phase 3-2: 로그 보존)
# def _cleanup_old_logs() -> None:
#     """당일 로그만 남기고 이전 날짜 로그 파일 삭제."""
#     ...
# _cleanup_old_logs()

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
from kis_utils import is_holiday, load_config, load_symbol_master, save_config, calc_limit_up_price, price_minus_one_tick, price_plus_n_ticks, round_to_tick, print_table, KRX_code_batch, calc_sell_pnl  # noqa: E402  # [260513] uplimit 스톱주문 호가 반올림 누락 복구 / [260522] calc_sell_pnl kis_utils 이전
from ws_realtime_tr_str1 import (  # noqa: E402  (상한가 근접 매수 + 종가 폭락 필터 포함)
    check_opening_call_auction_sell, check_opening_call_auction_cancel, check_realtime_sell,
    vi_buy_strategy, vi_should_cancel, check_vi_sell,
    # 상한가 근접 매수 전략 (docs/01. up_limit_buy_str_260420.md):
    uplimit_approach_buy_signal, uplimit_approach_exit,
    closing_crash_filter_signal, closing_next_day_exit,
    calc_uplimit_qty, calc_entry_price,
    # [260423] 재설계 신규
    uplimit_should_cancel_and_market_sell, uplimit_should_setup_stop_orders,
    UPLIMIT_EXIT_MARKET_SELL_CTRT, UPLIMIT_EXIT_TRIGGER_CTRT,
    # [260423] Strategy B/C 신규
    ma_trend_buy_signal, ma_trend_exit_signal,
    bb_expansion_buy_signal, bb_expansion_exit_signal,
    # [260424] Strategy A-v4 (deprecated, v5 호환용 import 유지)
    check_uplimit_v4_sustain_buy,
    check_uplimit_v4_overnight_hold,
    check_uplimit_v4_nextday_sell,
    UPLIMIT_V4_SUSTAIN_MINUTES, UPLIMIT_V4_HARD_LOSS,
    UPLIMIT_V4_OVERNIGHT_CTRT, UPLIMIT_V4_NEXTDAY_GAP_LOSS,
    # [260427] Strategy A-v5 (구독 즉시 ma10 상회 시 시장가 매수)
    check_uplimit_v5_instant_buy,
    check_uplimit_v5_trail_exit,
    # [260427] v5 분리: 12필터 qualify + 트리거 (ma10 OR BB 반등)
    check_uplimit_v5_qualify,
    check_uplimit_v5_buy_trigger,
    UPLIMIT_V5_QUALIFY_EXPIRY_MIN,
)
# [260523] Strategy str2 (전일 상한가 b1/b2/b3 매수 + 손절/트레일/sell_ready 매도)
#   순수 판단 모듈(per-tick). 서버는 모듈핸들로 들고 strategy_swap reload 시 재바인딩.
#   기본 비활성: config 가 str2 를 지정하지 않으면 이 import 만 존재하고 어떤 거동도 바뀌지 않음.
import ws_realtime_tr_str2 as strat2  # noqa: E402
from ws_realtime_tr_str2 import (  # noqa: E402,F401
    Str2State,
    STOCH_N as STR2_STOCH_N,
    ROLL as STR2_ROLL,
)
from kis_signal_apis import (  # noqa: E402
    fetch_volume_power, fetch_frgn_3day_net,
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

# part 파일 저장: 1500행 수신 시 오래된 1000행 저장·500행 버퍼 유지, 또는 1분마다 flush → 18:00 parts 병합
PART_FLUSH_THRESHOLD = 3000   # 2000/s 폭주 시 I/O 병목 방지: 1500→3000 상향
PART_FLUSH_SAVE = 2000        # 1000→2000: 저장 주기 완화로 WSS 콜백 블로킹 감소
PART_FLUSH_SEC = 60

# ── 기술적 지표 설정 ──
# EMA 기간 리스트 (ma3, ma50, ma200, ma300, ma500, ma2000 컬럼 생성)
INDICATOR_MA_LINE = [3, 10, 50, 200, 300, 500, 2000]  # [260423] ma10 추가 (Strategy B MA trend 용)
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
NO_DATA_RECONNECT_COUNT = 3  # 재구독 N회 연속 실패 시 WSS 강제 재연결 (좀비 소켓 탈출)

# 출력 시 "초당 카운트"는 1000을 넘으면 0부터 다시(999 -> 0)
PER_SEC_ROLLOVER = 1000

# 현재가 REST 보강 (1초 미수신 종목 보강)
REST_MAX_PER_SEC = 10
REST_AFTER_WINDOW_DELAY = 0.1

# 장중 미수신 감시 — REST 거래상태 확인
STALE_CHECK_SEC = 60            # 종목별 미수신 기준(초) → REST 상태 확인 트리거
STALE_CHECK_HALTED_SEC = 300    # 거래중단 확인 후 재확인 간격(초)
STALE_CHECK_VI_SEC = 30         # VI 발동 종목 재확인 간격(초)
STALE_CHECK_INACTIVE_SEC = 20   # 거래비활성 종목 재확인 간격(초) — REST 폴백 빠른 복귀용

# 상승률 상위 종목 추가 구독
TOP_RANK_ENABLED = True
TOP_RANK_END = dtime(15, 20)           # 15:19 종가매매 종목 선정까지 포함
TOP_RANK_N = 50                        # 상승률 상위 다운로드 수
TOP_RANK_ADD_N = 5                     # 랭킹 결과에서 실제 구독 추가할 최대 종목 수
TOP_RANK_REMOVE_THRESHOLD = 20         # 이 순위 밖으로 밀린 종목만 해제
TOP_RANK_MARKET_DIV = "J"
TOP_RANK_OUT_DIR = Path("/home/ubuntu/Stoc_Kis/data/fetch_top_list")
MAX_WSS_SUBSCRIBE = 40                 # WSS 구독 총 상한 (H0STCNI0 체결통보 1슬롯 별도 확보)
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


def _get_balance_page(client, cano: str, acnt_prdt_cd: str, tr_id: str,
                      ctx_fk: str = "", ctx_nk: str = "", tr_cont: str = "") -> tuple:
    """잔고 1페이지 조회. 반환: (out1, out2, body_ctx_fk, body_ctx_nk, resp_tr_cont).
    연속조회 판정은 반드시 응답 헤더 resp_tr_cont('F'/'M'=다음 있음)로만 한다 —
    KIS 는 다음 페이지가 없어도 body 의 ctx_area_fk100 에 'CANO^ACNT^...'+공백 패딩을 채워
    돌려주므로, 그 값으로 재조회하면 500 Internal Server Error 가 난다(미체결취소와 동일 패턴)."""
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
    params = {
        "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd, "AFHR_FLPR_YN": "N",
        "OFL_YN": "", "INQR_DVSN": "02", "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00",
        "CTX_AREA_FK100": ctx_fk, "CTX_AREA_NK100": ctx_nk,
    }
    headers = client._headers(tr_id=tr_id)
    headers["tr_cont"] = tr_cont          # 초기 "" / 연속 "N"
    # [260602] KIS inquire-balance 는 기동 직후/세션 churn 시 첫 호출에 간헐 500(Internal Server Error)을
    #   반환한다(실측: 재시작 3회 모두 a1 첫 잔고호출 500). idempotent GET 이므로 최대 3회 재시도(1.5s 간격).
    r = None
    last_err = None
    for _attempt in range(3):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            r.raise_for_status()
            last_err = None
            break
        except requests.RequestException as e:
            last_err = e
            r = None
            if _attempt < 2:
                time.sleep(1.5)
    if last_err is not None:
        raise last_err
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"잔고조회 실패: {j.get('msg1')} raw={j}")
    out1 = j.get("output1") or []
    out2 = j.get("output2") or {}
    if isinstance(out1, dict):
        out1 = [out1]
    resp_cont = str(r.headers.get("tr_cont") or "").strip().upper()
    return (out1, out2,
            str(j.get("ctx_area_fk100") or out2.get("ctx_area_fk100") or ""),
            str(j.get("ctx_area_nk100") or out2.get("ctx_area_nk100") or ""),
            resp_cont)


def _buy_order_cash(client, cano: str, acnt_prdt_cd: str, tr_id: str, code: str, qty: int, price: float, ord_dvsn: str = "00") -> dict:
    """ord_dvsn: 00=지정가, 01=시장가, 05=장전시간외종가(08:30~08:40), 06=장후시간외종가(15:40~16:00), 07=시간외단일가(16:00~18:00)"""
    # 시장가(01), 장전시간외(05), 장후시간외(06)는 ORD_UNPR=0 (가격 미입력)
    # 시간외단일가(07)는 가격 입력 필수
    ord_unpr = "0" if ord_dvsn in ORD_DVSN_NO_PRICE else str(int(price))
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    body = {"CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd, "PDNO": code, "ORD_DVSN": ord_dvsn, "ORD_QTY": str(qty), "ORD_UNPR": ord_unpr}
    headers = client._headers(tr_id=tr_id)
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        msg1 = j.get("msg1", "")
        # APBK0406: 상한가 초과 → 1틱 낮춘 가격으로 재시도 (최대 3회)
        if "APBK0406" in str(msg1) and ord_dvsn not in ORD_DVSN_NO_PRICE and price > 0:
            retry_price = price
            for attempt in range(3):
                retry_price = price_minus_one_tick(retry_price)
                logger.warning(f"[매수주문] APBK0406 상한가초과 → {attempt+1}차 재시도 price={retry_price:,.0f} (원래={price:,.0f})")
                body["ORD_UNPR"] = str(int(retry_price))
                headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
                r2 = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
                r2.raise_for_status()
                j2 = r2.json()
                if str(j2.get("rt_cd")) == "0":
                    return j2
                if "APBK0406" not in str(j2.get("msg1", "")):
                    raise RuntimeError(f"매수 주문 실패: {j2.get('msg1')} raw={j2}")
            raise RuntimeError(f"매수 주문 실패 (3회 재시도 후): {msg1} raw={j}")
        raise RuntimeError(f"매수 주문 실패: {msg1} raw={j}")
    # 주문 성공 → 해당 종목 H0STCNI0 체결통보 구독 (종목별 등록)
    try:
        _ccnl_notice_sub_add(code)
    except Exception:
        pass
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
    cano_alias: str = "",
    stck_prpr: float = 0.0,
) -> None:
    """거래장부 CSV 에 1행 추가 (스레드 안전)."""
    try:
        now = datetime.now(KST)
        amount = sell_price * fill_qty if sell_price > 0 else buy_price * fill_qty
        # 수익률/수익액: sell_fill 시에만 의미 있음
        ret = round((sell_price / buy_price - 1), 4) if buy_price > 0 and sell_price > 0 else 0.0
        profit = round(sell_price * fill_qty - buy_price * fill_qty, 0) if fill_qty > 0 and sell_price > 0 and buy_price > 0 else 0.0
        row = {
            "date":       now.strftime("%Y-%m-%d"),
            "time":       now.strftime("%H:%M:%S"),
            "code":       str(code).zfill(6),
            "name":       name or code_name_map.get(str(code).zfill(6), ""),
            "cano_alias": cano_alias,
            "order_type": order_type,
            "ord_dvsn":   ord_dvsn,
            "stck_prpr":  round(stck_prpr, 2),
            "buy_price":  round(buy_price, 2),
            "sell_price": round(sell_price, 2),
            "order_qty":  order_qty,
            "fill_qty":   fill_qty,
            "amount":     round(amount, 0),
            "ret":        ret,
            "profit":     profit,
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


def _append_daily_ledger_to_yearly() -> None:
    """당일 거래장부 CSV를 연도별 parquet에 append. 18:00 종료 시 1회 호출."""
    if not LEDGER_PATH.exists():
        logger.info("[ledger] 당일 CSV 없음 → 연도별 parquet 갱신 스킵")
        return
    try:
        year = datetime.now(KST).strftime("%Y")
        yearly_path = LEDGER_DIR / f"trade_ledger_{year}.parquet"
        daily_df = pd.read_csv(LEDGER_PATH, encoding="utf-8-sig")
        if daily_df.empty:
            return
        # recv_ts 통일: date + time → recv_ts
        if "date" in daily_df.columns and "time" in daily_df.columns:
            daily_df["recv_ts"] = daily_df["date"].astype(str) + " " + daily_df["time"].astype(str)
        if yearly_path.exists():
            existing = pd.read_parquet(yearly_path)
            combined = pd.concat([existing, daily_df], ignore_index=True)
            # 중복 제거: date+time+code+order_type+ord_no
            dedup_cols = [c for c in ["date", "time", "code", "order_type", "ord_no"] if c in combined.columns]
            if dedup_cols:
                combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
        else:
            combined = daily_df
        combined.to_parquet(yearly_path, index=False, engine="pyarrow", compression="zstd")
        logger.info(f"[ledger] 연도별 parquet 갱신: {yearly_path.name} ({len(combined):,}행)")
    except Exception as e:
        logger.warning(f"[ledger] 연도별 parquet 갱신 실패: {e}")


def _sell_order_cash(
    client, cano: str, acnt_prdt_cd: str, code: str, qty: int,
    price: float = 0, ord_dvsn: str = "01", cndt_pric: float = 0,
) -> dict:
    """주식 매도. TR_ID=TTTC0801U. ord_dvsn: 01=시장가, 22=스톱지정가(CNDT_PRIC 필수)"""
    # 시장가(01), 장전시간외(05), 장후시간외(06)는 가격 미입력
    ord_unpr = "0" if ord_dvsn in ORD_DVSN_NO_PRICE else str(int(price))
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    body = {
        "CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd,
        "PDNO": code, "ORD_DVSN": ord_dvsn,
        "ORD_QTY": str(qty), "ORD_UNPR": ord_unpr,
    }
    if cndt_pric > 0:
        body["CNDT_PRIC"] = str(int(cndt_pric))
    headers = client._headers(tr_id="TTTC0801U")
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"매도 주문 실패: {j.get('msg1')} raw={j}")
    # 주문 성공 → 해당 종목 H0STCNI0 체결통보 구독 (종목별 등록)
    try:
        _ccnl_notice_sub_add(code)
    except Exception:
        pass
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


def _inquire_psbl_rvsecncl(client, cano: str, acnt: str, tr_id: str = "TTTC0084R",
                            sll_buy_dvsn_cd: str = "00") -> list[dict]:
    """주식정정취소가능주문조회. sll_buy_dvsn_cd: 00=매수, 01=매도, 02=전체."""
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"
    headers = client._headers(tr_id=tr_id)
    params = {
        "CANO": cano, "ACNT_PRDT_CD": acnt,
        "INQR_DVSN": "00", "SLL_BUY_DVSN_CD": sll_buy_dvsn_cd,
        "INQR_STRT_DT": "", "INQR_END_DT": "", "PDNO": "", "ORD_GNO_BRNO": "", "ODNO": "",
        "INQR_DVSN_1": "0", "INQR_DVSN_2": "0",
        "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
    }
    out: list[dict] = []
    # 연속조회는 응답 헤더 tr_cont 로만 판정한다. KIS 는 다음 페이지가 없어도(rt_cd=0)
    # body 의 ctx_area_fk100 에 "CANO^ACNT^" + 공백 패딩 같은 쓰레기 값을 채워 돌려주므로,
    # ctx 값 유무로 루프를 돌리면 그 가짜 연속키로 재조회 → 500 Internal Server Error 가 난다.
    # 초기 요청 tr_cont="", 연속 요청 tr_cont="N". 응답 tr_cont 가 F/M 일 때만 다음 페이지가 있다.
    headers["tr_cont"] = ""
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
        tr_cont = str(r.headers.get("tr_cont") or "").strip().upper()
        if tr_cont not in ("F", "M"):   # D/E/공백 = 마지막 페이지(또는 데이터 없음)
            break
        headers["tr_cont"] = "N"
        params["CTX_AREA_FK100"] = j.get("ctx_area_fk100") or ""
        params["CTX_AREA_NK100"] = j.get("ctx_area_nk100") or ""
    return out


def _cancel_open_sell_orders_for_code(client, cano: str, acnt: str, code: str) -> int:
    """특정 종목의 미체결 매도주문을 모두 취소. 취소 건수 반환."""
    orders = _inquire_psbl_rvsecncl(client, cano, acnt, sll_buy_dvsn_cd="01")
    cancelled = 0
    for o in orders:
        o_code = str(o.get("pdno") or o.get("PDNO") or "").strip().zfill(6)
        if o_code != code:
            continue
        ordno = str(o.get("odno") or o.get("ODNO") or o.get("ordno") or "").strip()
        psbl_qty = int(float(str(o.get("psbl_qty") or o.get("PSBL_QTY") or 0).replace(",", "") or 0))
        ord_qty = int(float(str(o.get("ord_qty") or o.get("ORD_QTY") or 0).replace(",", "") or 0))
        ord_unpr = str(o.get("ord_unpr") or o.get("ORD_UNPR") or "0").replace(",", "")
        ord_dvsn = str(o.get("ord_dvsn_cd") or o.get("ORD_DVSN_CD") or o.get("ord_dvsn") or "01").strip() or "01"
        ord_dvsn_name = {"00": "지정가", "01": "시장가", "02": "시간외종가", "05": "장전시간외", "06": "장후시간외", "07": "시간외단일가"}.get(ord_dvsn, ord_dvsn)
        name = code_name_map.get(code, code)
        logger.info(
            f"[str1_sell] 미체결매도 발견 {name}({code})"
            f"  주문번호={ordno}  주문수량={ord_qty}  취소가능수량={psbl_qty}"
            f"  주문가={ord_unpr}  주문방식={ord_dvsn_name}({ord_dvsn})"
        )
        if not ordno or psbl_qty <= 0:
            continue
        try:
            _cancel_order_generic(client, cano, acnt, ordno, code, psbl_qty, ord_dvsn)
            cancelled += 1
            logger.info(f"[str1_sell] 미체결매도 취소 성공 {name}({code}) ordno={ordno} qty={psbl_qty}")
            time.sleep(0.3)
        except Exception as e:
            logger.error(f"[str1_sell] 미체결매도 취소 실패 {name}({code}) ordno={ordno}: {e}")
    return cancelled


def _run_open_buy_order_cancel_on_startup() -> None:
    """재시작 시 미체결 매수주문 전부 취소. 조회 → 현황 출력 → 취소 → 결과 출력.
    단, 15:20~18:00 종가/시간외 시간대에는 기존 주문을 유지해야 하므로 취소 스킵
    (재시작 후 _restore_orders_from_server_on_startup()에서 codes/체결통보 복원)."""
    if not OPEN_BUY_ORDER_CANCEL:
        return
    # 15:20~18:00: 종가매수/장후시간외/시간외단일가 시간대 → 기존 주문 유지
    nt = datetime.now(KST).time()
    if dtime(15, 20) <= nt < dtime(18, 0):
        _notify(f"{ts_prefix()} [미체결취소] {nt.strftime('%H:%M')} 종가/시간외 시간대 → 기존 주문 유지, 취소 스킵")
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


def _tele_balance_summary(title: str, now_str: str,
                          per_account: list[dict]) -> None:
    """텔레그램 전용 잔고 요약 전송 (stdout 출력 없음).

    per_account 항목: {"alias", "cano", "dnca", "ord_cash",
                       "tot_eval", "eval_pfls", "valid_rows"}
    """
    tele_lines = [f"[{title} {now_str}]"]
    for acc in per_account:
        tele_lines.append(f"◆ [{acc['alias']} ({acc['cano']})]")
        tele_lines.append(f"출금가능 {acc['dnca']:,.0f}  주문가능 {acc['ord_cash']:,.0f}")
        tele_lines.append(f"총평가 {acc['tot_eval']:,.0f}  평가손익 {acc['eval_pfls']:+,.0f}")
        valid_rows = acc.get("valid_rows") or []
        if valid_rows:
            tele_lines.append(f"<보유종목 {len(valid_rows)}건>")
            for r in valid_rows:
                code = str(r.get("pdno", "")).strip().zfill(6)
                _tnm = str(r.get("hts_kor_isnm", "") or "").strip()
                if not _tnm or _tnm == code:
                    _tnm = code_name_map.get(code, code)
                psbl = int(float(str(r.get("ord_psbl_qty", 0) or 0).replace(",", "") or 0))
                hldg = int(float(str(r.get("hldg_qty", 0) or 0).replace(",", "") or 0))
                pchs = float(str(r.get("pchs_avg_pric", 0) or 0).replace(",", "") or 0)
                rt   = float(str(r.get("evlu_pfls_rt", 0) or 0).replace(",", "") or 0)
                tele_lines.append(f"  {_tnm}({code}) 보유={hldg} 매도가능={psbl} 매입단가={pchs:,.0f} 수익={rt:+.2f}%")
        else:
            tele_lines.append("<보유종목 없음>")
    if tmsg is not None:
        try:
            tmsg("\n".join(tele_lines), "-t")
        except Exception:
            pass


def _query_and_print_balance(label: str, *, trade_only: bool = True,
                              tele_label: str | None = None,
                              hold_map_trade_only: bool = False) -> dict[str, dict] | None:
    """
    REST API(TTTC8434R) 잔고조회 → 상세 테이블 출력 → hold_map 반환.

    반환:
      - dict: 조회 성공 (보유종목 없으면 빈 dict, 있으면 {종목코드: {qty, buy_price}})
      - None: 조회 실패 (예외/설정누락)
    tele_label: 텔레그램 전송 시 사용할 라벨. None이면 전송 안 함.
    hold_map_trade_only: True 시 trade_enabled=False 계좌는 화면/텔레 표시만 하고
      hold_map / _code_account_map 에는 포함하지 않음 (str1_sell 등 거래 로직 보호).
    """
    global _code_account_map
    try:
        accounts = _iter_enabled_accounts(trade_only=trade_only)
        if not accounts:
            cfg = _read_cfg() or load_config(str(CONFIG_PATH))
            cano = str(cfg.get("cano", "")).strip()
            acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
            if not cano:
                logger.warning(f"{ts_prefix()} [{label}] config에 cano 없음")
                return None
            accounts = [{"account_id": "main", "alias": "a1", "cano": cano, "acnt_prdt_cd": acnt}]

        # 계좌별 데이터 수집
        per_account: list[dict] = []
        hold_map: dict[str, dict] = {}
        for acct in accounts:
            cano = acct["cano"]
            acnt = acct.get("acnt_prdt_cd", "01") or "01"
            alias = acct.get("alias", acct.get("account_id", "?"))
            # hold_map_trade_only=True 시 trade_enabled=False 계좌는 표시 전용 (거래 로직 제외)
            acct_trade_enabled = acct.get("trade_enabled", True) is not False
            include_in_hold_map = (not hold_map_trade_only) or acct_trade_enabled
            try:
                client = _init_account_client(acct) if "appkey" in acct else (_top_client or _init_top_client())
            except Exception as e:
                logger.warning(f"{ts_prefix()} [{label}] {alias}({cano}) 클라이언트 초기화 실패: {e}")
                continue
            acct_summary: dict = {}
            acct_rows: list[dict] = []
            seen_codes_local: set[str] = set()
            ctx_fk, ctx_nk, tr_cont = "", "", ""
            for _ in range(10):
                out1, out2, ctx_fk, ctx_nk, resp_cont = _get_balance_page(client, cano, acnt, "TTTC8434R", ctx_fk, ctx_nk, tr_cont)
                if not acct_summary:
                    acct_summary = out2 if isinstance(out2, dict) else (out2[0] if isinstance(out2, list) and out2 else {})
                rows = out1 if isinstance(out1, list) else ([out1] if isinstance(out1, dict) else [])
                if not rows:
                    break
                for row in rows:
                    c = str(row.get("pdno", "")).strip().zfill(6)
                    if c and c != "000000" and c not in seen_codes_local:
                        seen_codes_local.add(c)
                        row["_acct_alias"] = f"{alias}({cano})"
                        acct_rows.append(row)
                        if include_in_hold_map and c not in _code_account_map:
                            _code_account_map[c] = acct
                if resp_cont not in ("F", "M"):   # 응답 tr_cont 로만 연속 판정 (body ctx 패딩 신뢰 금지)
                    break
                tr_cont = "N"
            valid_rows = [
                r for r in acct_rows
                if int(float(str(r.get("hldg_qty", 0) or 0).replace(",", "") or 0)) > 0
            ]
            per_account.append({
                "alias": alias, "cano": cano,
                "summary": acct_summary, "valid_rows": valid_rows,
                "include_in_hold_map": include_in_hold_map,
            })
            logger.info(f"[{label}] {alias}({cano}) 조회 완료 (보유 {len(valid_rows)}건)")

        # ── 출력 라인 구성 ─────────────────────────────────────────────────────
        SEP = "─" * 66
        BIG_SEP = "═" * 66
        ACCT_LINE_TAIL = "═" * 54
        now_str = datetime.now(KST).strftime("%H:%M:%S")
        sum_cols = ["출금가능", "주문가능", "총 평가금액", "평가손익합계", "당일매수금액", "당일매도금액"]
        sum_align = {c: "right" for c in sum_cols}
        hold_cols = ["코드", "종목명", "보유", "매도가능", "매입단가", "현재단가", "평가금액", "평가손익", "수익률"]
        hold_align = {"코드": "left", "종목명": "left", "보유": "right", "매도가능": "right",
                      "매입단가": "right", "현재단가": "right", "평가금액": "right", "평가손익": "right", "수익률": "right"}

        lines: list[str] = [SEP, f"[{label}] {now_str}"]
        tele_payload: list[dict] = []

        for acc in per_account:
            alias = acc["alias"]
            cano = acc["cano"]
            summary = acc["summary"] or {}
            valid_rows = acc["valid_rows"]
            acc_include = acc.get("include_in_hold_map", True)

            def _f(key: str, _s=summary) -> float:
                return float(str(_s.get(key, 0) or 0).replace(",", "") or 0)

            dnca      = _f("dnca_tot_amt")
            ord_cash  = _f("prvs_rcdl_excc_amt")
            tot_eval  = _f("tot_evlu_amt")
            buy_amt   = _f("thdt_buy_amt")
            sll_amt   = _f("thdt_sll_amt")
            eval_pfls = _f("evlu_pfls_smtm_amt")

            sum_row = {
                "출금가능": f"{dnca:,.0f}", "주문가능": f"{ord_cash:,.0f}",
                "총 평가금액": f"{tot_eval:,.0f}", "평가손익합계": f"{eval_pfls:+,.0f}",
                "당일매수금액": f"{buy_amt:,.0f}", "당일매도금액": f"{sll_amt:,.0f}",
            }
            sum_tbl = print_table([sum_row], sum_cols, sum_align,
                                  no_print=True, header_sep=False)

            lines.append(f"◆ [{alias} ({cano})]   {ACCT_LINE_TAIL}")
            lines.append(sum_tbl)

            if valid_rows:
                tbl_rows = []
                for r in valid_rows:
                    code   = str(r.get("pdno", "")).strip().zfill(6)
                    _raw_nm = str(r.get("hts_kor_isnm", "") or "").strip()
                    if not _raw_nm or _raw_nm == code:
                        _raw_nm = code_name_map.get(code, code)
                    if code in _no_sell_codes:
                        _raw_nm = f"{_raw_nm}[NS]"
                    hldg   = int(float(str(r.get("hldg_qty", 0) or 0).replace(",", "") or 0))
                    psbl   = int(float(str(r.get("ord_psbl_qty", 0) or 0).replace(",", "") or 0))
                    pchs   = float(str(r.get("pchs_avg_pric", 0) or 0).replace(",", "") or 0)
                    prpr   = float(str(r.get("prpr", 0) or 0).replace(",", "") or 0)
                    evlu   = float(str(r.get("evlu_amt", 0) or 0).replace(",", "") or 0)
                    pfls   = float(str(r.get("evlu_pfls_amt", 0) or 0).replace(",", "") or 0)
                    rt     = float(str(r.get("evlu_pfls_rt", 0) or 0).replace(",", "") or 0)
                    if prpr == 0 and evlu > 0 and hldg > 0:
                        prpr = evlu / hldg
                    elif prpr > 0 and hldg > 1 and abs(prpr - evlu) < 1:
                        prpr = evlu / hldg
                    qty    = psbl if psbl > 0 else hldg
                    if acc_include and code not in hold_map:
                        hold_map[code] = {"qty": qty, "buy_price": pchs}
                    tbl_rows.append({
                        "코드": code, "종목명": _raw_nm,
                        "보유": f"{hldg:,}", "매도가능": f"{psbl:,}",
                        "매입단가": f"{pchs:,.0f}", "현재단가": f"{prpr:,.0f}",
                        "평가금액": f"{evlu:,.0f}", "평가손익": f"{pfls:+,.0f}",
                        "수익률": f"{rt:+.2f}%(▲)" if rt >= 29.5 else f"{rt:+.2f}%",
                    })
                hold_tbl = print_table(tbl_rows, hold_cols, hold_align,
                                       no_print=True, header_sep=False)
                lines.append(f"<보유종목 {len(valid_rows)}건>")
                lines.append(hold_tbl)
            else:
                lines.append("<보유종목 없음>")

            tele_payload.append({
                "alias": alias, "cano": cano,
                "dnca": dnca, "ord_cash": ord_cash,
                "tot_eval": tot_eval, "eval_pfls": eval_pfls,
                "valid_rows": valid_rows,
            })

        lines.append(BIG_SEP)
        full_msg = "\n".join(lines)
        sys.stdout.write(full_msg + "\n")
        sys.stdout.flush()
        logger.info(full_msg)
        if tele_label:
            _tele_balance_summary(tele_label, now_str, tele_payload)

        return hold_map

    except Exception as e:
        logger.warning(f"{ts_prefix()} [{label}] 조회 실패: {e}")
        return None


def _run_balance_0858() -> None:
    """08:58 계좌잔고조회 1회 실행, 결과 출력. 모든 활성 계좌 순회."""
    global _balance_0858_done
    if _balance_0858_done:
        return
    _query_and_print_balance("08:58 잔고조회", trade_only=False, tele_label="08:58 잔고")
    _balance_0858_done = True


def _print_startup_balance() -> dict[str, dict]:
    """
    프로그램 시작 시 계좌 잔고조회 → 상세 출력 → balance_map 반환.

    반환: {종목코드(6자리): {"qty": 매도가능수량, "buy_price": 매입평균가}}
    실패 시 빈 dict 반환.

    표시: 모든 활성 계좌(trade_enabled=False 포함) — 08:58 잔고조회와 동일
    hold_map: trade_enabled=True 계좌만 — str1_sell 등 거래 로직이 비거래 계좌를 건드리지 않도록 보호
    """
    hold_map = _query_and_print_balance(
        "시작 잔고조회",
        trade_only=False,
        tele_label="시작잔고",
        hold_map_trade_only=True,
    )
    if hold_map is None:
        _notify(f"{ts_prefix()} [시작잔고] 조회 실패", tele=True)
        return {}
    if not hold_map:
        _notify(f"{ts_prefix()} [시작잔고] 보유종목 없음", tele=True)
    return hold_map


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
    저장된 parquet 파일에서 15:20~15:30 예상체결(is_real_ccnl=N) 첫 틱 조회.
    closing_buy_state codes 대상. parts 및 FINAL parquet 파일에서 조회.
    """
    if not codes:
        return {}
    codes_set = {str(c).zfill(6) for c in codes}
    today_ymd = datetime.now(KST).strftime("%Y-%m-%d")
    ts_min = f"{today_ymd} 15:20:00"
    ts_max = f"{today_ymd} 15:30:00"
    result: dict[str, float] = {}
    try:
        # parquet 우선 (parts 백업용)
        sources: list[Path] = []
        if FINAL_PARQUET_PATH.exists() and _is_valid_parquet(FINAL_PARQUET_PATH):
            sources.append(FINAL_PARQUET_PATH)
        for p in sorted(PART_DIR.glob("*.parquet")):
            if _is_valid_parquet(p):
                sources.append(p)
        if not sources:
            return {}

        dfs: list[pl.DataFrame] = []
        for p in sources:
            try:
                dfs.append(pl.read_parquet(str(p)))
            except Exception:
                continue
        if not dfs:
            return {}
        df = pl.concat(dfs, how="diagonal_relaxed")

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
        logger.warning(f"{ts_prefix()} [종가모니터] parquet 첫틱 조회 실패: {e}")
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


def _get_balance_holdings() -> dict[str, dict]:
    """
    KIS API 잔고조회(TTTC8434R)로 실제 보유 종목·수량·매입단가 반환.
    반환: {종목코드(6자리): {"qty": 보유수량, "buy_price": 매입평균가}}
    모든 활성 계좌를 순회하여 합산. 실패 시 빈 dict 반환.
    """
    hold_map: dict[str, dict] = {}
    # _code_account_map: 종목별 매도 시 사용할 계좌 정보 저장
    global _code_account_map
    accounts = _iter_enabled_accounts(trade_only=True)
    if not accounts:
        # V1 fallback
        try:
            cfg = _read_cfg() or load_config(str(CONFIG_PATH))
            cano = str(cfg.get("cano", "")).strip()
            acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
            if not cano:
                return {}
            accounts = [{"account_id": "main", "alias": "a1", "cano": cano, "acnt_prdt_cd": acnt}]
        except Exception:
            return {}
    for acct in accounts:
        cano = acct["cano"]
        acnt = acct.get("acnt_prdt_cd", "01") or "01"
        alias = acct.get("alias", acct.get("account_id", "?"))
        try:
            client = _init_account_client(acct) if "appkey" in acct else (_top_client or _init_top_client())
            ctx_fk, ctx_nk, tr_cont = "", "", ""
            for _ in range(10):
                out1, _, ctx_fk, ctx_nk, resp_cont = _get_balance_page(client, cano, acnt, "TTTC8434R", ctx_fk, ctx_nk, tr_cont)
                rows = out1 if isinstance(out1, list) else ([out1] if isinstance(out1, dict) else [])
                if not rows:
                    break
                for row in rows:
                    c = str(row.get("pdno", "") or "").strip().zfill(6)
                    if not c or c == "000000":
                        continue
                    qty = int(float(str(row.get("hldg_qty", 0) or 0).replace(",", "") or 0))
                    psbl = int(float(str(row.get("ord_psbl_qty", 0) or 0).replace(",", "") or 0))
                    pchs = float(str(row.get("pchs_avg_pric", 0) or 0).replace(",", "") or 0)
                    # ord_psbl_qty(매도가능수량) 우선 사용. hldg_qty 폴백 금지
                    # (hldg_qty는 미결제 매수주문 포함 총량이라 수량초과 매도 유발)
                    if psbl > 0:
                        effective_qty = psbl
                    elif qty > 0 and psbl == 0:
                        # API 불안정 시 ord_psbl_qty=0 반환 가능 → hldg_qty 사용하되 경고
                        effective_qty = qty
                        logger.warning(
                            f"{ts_prefix()} [잔고] {c} ord_psbl_qty=0, hldg_qty={qty}"
                            f" → hldg_qty 사용 (매도 시 수량 재검증 필요)"
                        )
                    else:
                        effective_qty = qty
                    if c not in hold_map:
                        hold_map[c] = {"qty": effective_qty, "buy_price": pchs}
                        _code_account_map[c] = acct  # 해당 종목을 보유한 계좌 기록
                    else:
                        # 다른 계좌에도 동일 종목 보유 시 수량 합산
                        hold_map[c]["qty"] += effective_qty
                if resp_cont not in ("F", "M"):   # 응답 tr_cont 로만 연속 판정 (body ctx 패딩 신뢰 금지)
                    break
                tr_cont = "N"
            logger.info(f"[str1_sell] 잔고조회 {alias}({cano}): {sum(1 for c, v in hold_map.items() if _code_account_map.get(c) == acct and v.get('qty', 0) > 0)}종목")
        except Exception as e:
            logger.warning(f"{ts_prefix()} [str1_sell] 잔고조회 실패 {alias}({cano}): {e}")
    return hold_map


def _load_str1_sell_state_on_startup(balance_map: dict[str, dict] | None = None) -> None:
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
            for code, bm_val in balance_map.items():
                if isinstance(bm_val, dict):
                    qty = bm_val.get("qty", 0)
                    buy_price = bm_val.get("buy_price", 0.0)
                else:
                    qty = bm_val
                    buy_price = 0.0
                if qty <= 0:
                    continue
                name = code_name_map.get(code, code)
                _str1_sell_state[code] = {
                    "sold": False, "sell_reason": "", "qty": qty,
                    "sell_price": 0.0, "sell_time": "", "ordno": "",
                    "pnl": 0.0, "ret_pct": 0.0,
                    "buy_price": buy_price, "highest": 0.0,
                    "name": name, "opening_call_auction_ordered": False,
                    "balance_qty": qty,
                    "loss_below_count": 0,
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
            # H0STMKO0 장운영정보 구독 (보유종목 VI 감지)
            _mkstatus_sub_add(set(added_for_sell))
            # WSS 구독 재구성: 잔고 종목이 open_map 구성 이후에 추가된 경우 대비
            _trigger_ws_rebuild()
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
            balance_buy_price = 0.0
            if balance_map:
                bm_val = balance_map.get(code, {})
                if isinstance(bm_val, dict):
                    actual_qty = bm_val.get("qty", 0)
                    balance_buy_price = bm_val.get("buy_price", 0.0)
                else:
                    actual_qty = bm_val
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
            # buy_price: 잔고조회 매입단가 > prev_close(전일종가) 순으로 사용
            effective_buy_price = balance_buy_price if balance_buy_price > 0 else prev_close
            _str1_sell_state[code] = {
                "sold":               False,
                "sell_reason":        "",
                "qty":                actual_qty,
                "sell_price":         0.0,
                "sell_time":          "",
                "ordno":              "",
                "pnl":                0.0,
                "ret_pct":            0.0,
                "buy_price":          effective_buy_price,
                "highest":            0.0,
                "name":               name,
                "opening_call_auction_ordered":  False,
                "balance_qty":        actual_qty,   # 잔고조회 원본 수량 (기록용)
                "loss_below_count":   0,             # 매수가 손절 N틱 연속 이탈 카운터
            }
            added_for_sell.append(code)

        # ── 잔고에 있지만 orders에 없는 종목 보충 등록 (전일 이전 매수 보유 종목) ──
        if balance_map:
            orders_codes = {str(o.get("code","")).zfill(6) for o in state["orders"]}
            for code, bm_val in balance_map.items():
                if code in orders_codes or code in _str1_sell_state:
                    continue  # 이미 처리됨
                if isinstance(bm_val, dict):
                    qty = bm_val.get("qty", 0)
                    buy_price = bm_val.get("buy_price", 0.0)
                else:
                    qty = bm_val
                    buy_price = 0.0
                if qty <= 0:
                    continue
                name = code_name_map.get(code, code)
                _str1_sell_state[code] = {
                    "sold": False, "sell_reason": "", "qty": qty,
                    "sell_price": 0.0, "sell_time": "", "ordno": "",
                    "pnl": 0.0, "ret_pct": 0.0,
                    "buy_price": buy_price, "highest": 0.0,
                    "name": name, "opening_call_auction_ordered": False,
                    "balance_qty": qty,
                    "loss_below_count": 0,
                }
                added_for_sell.append(code)
                logger.info(f"[str1_sell] 잔고 보충 등록: {name}({code}) qty={qty} buy_price={buy_price}")

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
        # H0STMKO0 장운영정보 구독 (보유종목 VI 감지)
        _mkstatus_sub_add(set(added_for_sell))
        # WSS 구독 재구성: 잔고 종목이 open_map 구성 이후에 추가된 경우 대비
        _trigger_ws_rebuild()


# ── no_sell_codes 로드/갱신/추가 ─────────────────────────────────────────────

def _load_no_sell_codes() -> None:
    """시작 시 config.json에서 no_sell_codes 로드."""
    global _no_sell_codes
    cfg = _read_cfg()
    if not cfg:
        return
    raw = cfg.get("no_sell_codes", [])
    if isinstance(raw, list):
        _no_sell_codes = {str(c).strip().zfill(6) for c in raw if str(c).strip()}
    if _no_sell_codes:
        msg = f"{ts_prefix()} [no_sell] 매도금지 종목 로드: {sorted(_no_sell_codes)}"
        _notify(msg, tele=True)
        logger.info(msg)


def _refresh_no_sell_from_cfg(cfg: dict) -> None:
    """_check_external_orders() 에서 매 사이클 호출 → 변경 감지 시 갱신."""
    global _no_sell_codes
    raw = cfg.get("no_sell_codes", [])
    new_set: set[str] = set()
    if isinstance(raw, list):
        new_set = {str(c).strip().zfill(6) for c in raw if str(c).strip()}
    if new_set != _no_sell_codes:
        added = new_set - _no_sell_codes
        removed = _no_sell_codes - new_set
        _no_sell_codes = new_set
        parts = []
        if added:
            parts.append(f"추가={sorted(added)}")
        if removed:
            parts.append(f"해제={sorted(removed)}")
        msg = f"{ts_prefix()} [no_sell] 매도금지 변경: {', '.join(parts)}"
        _notify(msg, tele=True)
        logger.info(msg)


def _add_no_sell_code(code: str, source: str = "") -> None:
    """런타임 추가 + config.json에 영구 저장."""
    global _no_sell_codes
    code = str(code).strip().zfill(6)
    if code in _no_sell_codes:
        return
    _no_sell_codes.add(code)
    # config.json에 저장
    try:
        cfg = _read_cfg()
        if cfg is not None:
            ns_list = cfg.get("no_sell_codes", [])
            if not isinstance(ns_list, list):
                ns_list = []
            if code not in ns_list:
                ns_list.append(code)
            cfg["no_sell_codes"] = ns_list
            save_config(cfg, str(CONFIG_PATH))
    except Exception as e:
        logger.warning(f"[no_sell] config 저장 실패: {e}")
    src_label = f" (source={source})" if source else ""
    msg = f"{ts_prefix()} [no_sell] 매도금지 추가: {code}{src_label}"
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
            # ── 동시호가 매도 주문 취소 ──
            qty   = int(req.get("qty", 0))
            ordno = str(req.get("ordno", "")).strip()
            reason = req.get("reason", "")
            try:
                if not ordno or qty <= 0:
                    logger.warning(f"[str1_sell] 취소 스킵 {code}: ordno/수량 없음")
                    continue
                # 종목별 보유 계좌로 취소 (syw_2 등 서브계좌 지원)
                acct = _code_account_map.get(code)
                if acct:
                    cano = acct["cano"]
                    acnt = acct.get("acnt_prdt_cd", "01") or "01"
                    client = _init_account_client(acct)
                else:
                    cfg = _read_cfg() or load_config(str(CONFIG_PATH))
                    cano = str(cfg.get("cano", "")).strip()
                    acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
                    client = _top_client or _init_top_client()
                if not cano:
                    logger.warning(f"[str1_sell] config cano 없음 → {code} 취소 스킵")
                    continue
                logger.info(
                    f"{ts_prefix()} [str1_sell] ■ 동시호가매도 취소 API호출 {name}({code})"
                    f"  tr_id=TTTC0803U  원주문번호={ordno}  수량={qty}  사유={reason}"
                )
                _cancel_order_generic(client, cano, acnt, ordno, code, qty, ord_dvsn="01")
                with _str1_sell_state_lock:
                    st = _str1_sell_state.get(code, {})
                    _str1_sell_state[code] = {
                        **st,
                        "opening_call_auction_ordered": False,
                        "opening_call_auction_cancel_requested": False,
                        "cancel_retry_cnt": 0,
                        "ordno": "",
                    }
                    _opening_call_auction_cancelled_log.append((code, name, reason))
                _save_str1_sell_state()
                msg = (
                    f"{ts_prefix()} [str1_sell] ✔ 동시호가매도 취소완료 {name}({code})"
                    f"  tr_id=TTTC0803U  원주문번호={ordno}  수량={qty}  사유={reason}"
                )
                _notify(msg, tele=True)
                logger.info(msg)
            except Exception as e:
                with _str1_sell_state_lock:
                    if code in _str1_sell_state:
                        retry_cnt = _str1_sell_state[code].get("cancel_retry_cnt", 0) + 1
                        _str1_sell_state[code]["cancel_retry_cnt"] = retry_cnt
                        if retry_cnt >= 5:
                            # 5회 실패 → 재시도 포기 (플래그 True로 설정하여 재큐잉 차단)
                            _str1_sell_state[code]["opening_call_auction_cancel_requested"] = True
                            logger.error(f"[str1_sell] 동시호가매도 취소 {retry_cnt}회 실패 → 재시도 중단 {name}({code}) err={e}")
                            _notify(f"{ts_prefix()} [str1_sell] 동시호가매도 취소 {retry_cnt}회 실패→포기 {name}({code})", tele=True)
                        else:
                            # 재시도 허용 (플래그 해제)
                            _str1_sell_state[code]["opening_call_auction_cancel_requested"] = False
                            logger.error(f"[str1_sell] 동시호가매도 취소실패 ({retry_cnt}/5) {code}: {e}")
                            _notify(f"{ts_prefix()} [str1_sell] 동시호가매도 취소실패 ({retry_cnt}/5) {name}({code}) err={e}", tele=True)
            continue

        # ── 매도 주문 ──
        qty       = int(req.get("qty", 0))
        reason    = req.get("reason", "")
        ref_price = req.get("ref_price", 0.0)
        ord_dvsn  = req.get("ord_dvsn", "01")
        sell_price = req.get("price", 0.0)
        cndt_pric  = req.get("cndt_pric", 0.0)
        ord_dvsn_name = {"00": "지정가", "01": "시장가", "02": "시간외종가", "05": "장전시간외", "06": "장후시간외", "07": "시간외단일가", "22": "스톱지정가"}.get(ord_dvsn, ord_dvsn)
        retry_after_cancel = False   # 미체결 취소 후 재주문 플래그
        try:
            # 종목별 보유 계좌로 매도 (syw_2 등 서브계좌 지원)
            acct = _code_account_map.get(code)
            if acct:
                cano = acct["cano"]
                acnt = acct.get("acnt_prdt_cd", "01") or "01"
                client = _init_account_client(acct)
                alias = acct.get("alias", acct.get("account_id", "?"))
                logger.info(f"[str1_sell] {name}({code}) → {alias}({cano}) 계좌로 매도")
            else:
                cfg = _read_cfg() or load_config(str(CONFIG_PATH))
                cano = str(cfg.get("cano", "")).strip()
                acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
                client = _top_client or _init_top_client()
            if not cano:
                logger.warning(f"[str1_sell] config cano 없음 → {code} 매도 스킵")
                continue

            # [260521] 핫패스 지연 제거: 매도 직전 REST 잔고 재조회(_get_balance_holdings,
            # TTTC8434R 페이지네이션 ×활성계좌, ~1.5초)를 삭제. 매도 수량은 이미
            # req["qty"](= _str1_sell_state/시작 balance_map 시드값)로 알고 있으므로 그대로 사용.
            # 수량초과(APBK0400) 안전망은 아래 except 경로(미체결 매도주문 취소 후 재주문)가
            # 그대로 담당한다.
            is_stop_limit = (ord_dvsn == "22")
            is_opening_call_auction = reason.startswith("str1_시초가하락_예상")
            sell_label = "스톱지정가매도" if is_stop_limit else ("동시호가매도" if is_opening_call_auction else "매도")
            if is_stop_limit:
                price_info = f"  조건가={cndt_pric:,.0f}  지정가={sell_price:,.0f}"
            elif ord_dvsn not in ORD_DVSN_NO_PRICE:
                price_info = f"  주문가={ref_price:,.0f}"
            else:
                price_info = ""
            api_call_msg = (
                f"{ts_prefix()} [str1_sell] ■ {sell_label}주문 API호출 {name}({code})"
                f"  tr_id=TTTC0801U  수량={qty}  주문방식={ord_dvsn_name}({ord_dvsn}){price_info}"
                f"  사유={reason}"
            )
            # [260521] 핫패스 보호: 주문 직전에는 빠른 파일 로그만 남긴다.
            # (텔레그램 동기 전송 ~1.0초 제거 + 중복 logger.info 제거 →
            #  발동→주문 지연 단축. 텔레그램 통지는 주문 후 _notify_async로 비동기 전송.)
            logger.info(api_call_msg)
            # [260521] 발동→주문 내부 지연 계측: 큐등록(enq_ts)부터 KIS 주문 전송 직전까지.
            # 핫패스 블로킹 회귀(REST/텔레그램 등) 감시용. 목표 수~수십 ms.
            _enq_ts = req.get("enq_ts")
            if _enq_ts:
                logger.info(
                    f"{ts_prefix()} [sell_latency] {name}({code}) "
                    f"발동→주문 {(time.time() - _enq_ts) * 1000:.1f}ms"
                )
            j = _sell_order_cash(
                client, cano, acnt, code, qty,
                price=sell_price, ord_dvsn=ord_dvsn, cndt_pric=cndt_pric,
            )
            out = j.get("output") or {}
            ordno = str(out.get("ODNO") or out.get("odno") or "").strip()

            with _str1_sell_state_lock:
                st = _str1_sell_state.get(code, {})
                buy_price = st.get("buy_price") or ref_price
            pnl_info = calc_sell_pnl(buy_price, ref_price, qty) if buy_price > 0 else {}
            with _str1_sell_state_lock:
                st = _str1_sell_state.get(code, {})
                sell_ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
                new_st = {
                    **st,
                    "sell_reason": reason,
                    "sell_price": ref_price,
                    "ordno": ordno,
                }
                if is_stop_limit:
                    # 스톱지정가: 조건 미충족 시 미체결 → sold=False 유지, 일반 매도 계속 가능
                    new_st["stop_limit_ordered"] = True
                    new_st["sold"] = False
                elif is_opening_call_auction:
                    # 동시호가 매도: 09:00 시초가에 체결 예정 → sold=False, opening_call_auction_ordered=True
                    # 이후 예상가 복귀 시 취소 가능
                    new_st["opening_call_auction_ordered"] = True
                    new_st["sold"] = False
                else:
                    new_st["sold"] = True
                    new_st["sell_time"] = sell_ts
                    new_st["pnl"] = pnl_info.get("pnl", 0.0)
                    new_st["ret_pct"] = pnl_info.get("ret_pct", 0.0)
                _str1_sell_state[code] = new_st
            _save_str1_sell_state()

            pnl_txt = f"  PNL≈{pnl_info.get('pnl', 0):+,.0f}원  ret≈{pnl_info.get('ret_pct', 0):+.2f}%" if pnl_info else ""
            opening_call_auction_suffix = " → 09:00 시초가 체결대기(복귀 시 취소가능)" if is_opening_call_auction else ""
            msg = (
                f"{ts_prefix()} [str1_sell] ✔ {sell_label}주문 접수완료 {name}({code})"
                f"  tr_id=TTTC0801U  주문번호={ordno}  수량={qty}"
                f"  주문방식={ord_dvsn_name}({ord_dvsn}){price_info}"
                f"  사유={reason}{pnl_txt}{opening_call_auction_suffix}"
            )
            # [260521] 주문 후 통지는 비동기로 (텔레그램 HTTP가 워커를 막아 다음 매도 처리를
            # 지연시키지 않게). _notify_async 가 파일 로그 + TELE_LOG_PATH 기록은 동기로 즉시
            # 남기므로 별도 logger.info 중복 호출은 불필요.
            _notify_async(msg)
            # 매도 성공 시 EMA 데드크로스 상태 해제
            _ema_sell_cond.pop(code, None)
            _up_trend_state.pop(code, None)
            # VI 포지션 정리
            if code in _vi_positions:
                _vi_positions[code]["sold"] = True
            # 매도 완료 시 H0STMKO0 장운영정보 구독 해제
            if not is_opening_call_auction:
                try:
                    _mkstatus_sub_remove({code})
                except Exception as e_mk:
                    logger.warning(f"[str1_sell] H0STMKO0 해제 실패 {code}: {e_mk}")
            # 거래장부 기록
            with _str1_sell_state_lock:
                bp = _str1_sell_state.get(code, {}).get("buy_price", 0.0)
            _is_market = ord_dvsn == "01"
            _append_ledger(
                order_type="sell_order",
                code=code, name=name,
                buy_price=float(bp or 0),
                sell_price=0.0 if _is_market else float(ref_price or 0),
                order_qty=qty,
                reason=f"{reason}_주문",
                ord_no=ordno,
                ord_dvsn=f"{ord_dvsn}({ord_dvsn_name})",
                prdy_ctrt=float(_last_prdy_ctrt.get(code, 0.0)),
                stck_prpr=float(ref_price or 0),
            )

        except Exception as e:
            err_str = str(e)
            logger.error(
                f"[str1_sell] 매도주문 실패 {name}({code})"
                f"  수량={qty}  기준가={ref_price:,.0f}  주문방식={ord_dvsn_name}({ord_dvsn})"
                f"  사유={reason}  에러={e}"
            )

            # ── 수량 초과(APBK0400) → 미체결 매도주문 취소 후 재주문 ──
            if "APBK0400" in err_str or "수량을 초과" in err_str:
                try:
                    logger.info(f"[str1_sell] 수량초과 → {name}({code}) 미체결 매도주문 조회/취소 시도")
                    n_cancelled = _cancel_open_sell_orders_for_code(client, cano, acnt, code)
                    if n_cancelled > 0:
                        logger.info(f"[str1_sell] {name}({code}) 미체결 매도 {n_cancelled}건 취소 완료 → 재주문 예정")
                        retry_after_cancel = True
                    else:
                        logger.warning(f"[str1_sell] {name}({code}) 취소할 미체결 매도주문 없음")
                except Exception as ce:
                    logger.error(f"[str1_sell] {name}({code}) 미체결 매도 취소 중 에러: {ce}")

            if not retry_after_cancel:
                _notify(
                    f"{ts_prefix()} [str1_sell] 매도실패 {name}({code})"
                    f"  수량={qty}  기준가={ref_price:,.0f}  주문방식={ord_dvsn_name}({ord_dvsn})"
                    f"  사유={reason} err={e}",
                    tele=True,
                )
                # 주문 실패 시 sell_ordered/sold 복구 → 재시도 가능
                with _str1_sell_state_lock:
                    if code in _str1_sell_state:
                        _str1_sell_state[code]["sell_ordered"] = False
                        _str1_sell_state[code]["sold"] = False

        # ── 미체결 취소 후 재주문 ──
        if retry_after_cancel:
            time.sleep(0.5)  # 취소 반영 대기
            try:
                logger.info(
                    f"[str1_sell] 재주문 시도 {name}({code})"
                    f"  수량={qty}  기준가={ref_price:,.0f}  주문방식={ord_dvsn_name}({ord_dvsn})"
                )
                j2 = _sell_order_cash(client, cano, acnt, code, qty, ord_dvsn=ord_dvsn)
                out2 = j2.get("output") or {}
                ordno2 = str(out2.get("ODNO") or out2.get("odno") or "").strip()
                is_opening_call_auction = reason.startswith("str1_시초가하락_예상")
                with _str1_sell_state_lock:
                    st = _str1_sell_state.get(code, {})
                    buy_price = st.get("buy_price") or ref_price
                pnl_info = calc_sell_pnl(buy_price, ref_price, qty) if buy_price > 0 else {}
                with _str1_sell_state_lock:
                    st = _str1_sell_state.get(code, {})
                    sell_ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
                    new_st = {
                        **st,
                        "sell_reason": reason,
                        "sell_price": ref_price,
                        "ordno": ordno2,
                    }
                    if is_opening_call_auction:
                        new_st["opening_call_auction_ordered"] = True
                        new_st["sold"] = False
                    else:
                        new_st["sold"] = True
                        new_st["sell_time"] = sell_ts
                        new_st["pnl"] = pnl_info.get("pnl", 0.0)
                        new_st["ret_pct"] = pnl_info.get("ret_pct", 0.0)
                    _str1_sell_state[code] = new_st
                _save_str1_sell_state()

                pnl_txt = f"  PNL≈{pnl_info.get('pnl', 0):+,.0f}원  ret≈{pnl_info.get('ret_pct', 0):+.2f}%" if pnl_info else ""
                msg = (
                    f"{ts_prefix()} [str1_sell] 재주문 성공 {name}({code})"
                    f"  주문번호={ordno2}  수량={qty}  기준가={ref_price:,.0f}"
                    f"  주문방식={ord_dvsn_name}({ord_dvsn})  사유={reason}{pnl_txt}"
                )
                _notify(msg, tele=True)
                logger.info(msg)
                if not is_opening_call_auction:
                    try:
                        _mkstatus_sub_remove({code})
                    except Exception:
                        pass
                with _str1_sell_state_lock:
                    bp = _str1_sell_state.get(code, {}).get("buy_price", 0.0)
                _is_market2 = ord_dvsn == "01"
                _append_ledger(
                    order_type="sell_order",
                    code=code, name=name,
                    buy_price=float(bp or 0),
                    sell_price=0.0 if _is_market2 else float(ref_price or 0),
                    order_qty=qty,
                    reason=f"{reason}_주문(미체결취소후재주문)",
                    ord_no=ordno2,
                    ord_dvsn=f"{ord_dvsn}({ord_dvsn_name})",
                    prdy_ctrt=float(_last_prdy_ctrt.get(code, 0.0)),
                    stck_prpr=float(ref_price or 0),
                )
            except Exception as e2:
                logger.error(
                    f"[str1_sell] 재주문도 실패 {name}({code})"
                    f"  수량={qty}  기준가={ref_price:,.0f}  에러={e2}"
                )
                _notify(
                    f"{ts_prefix()} [str1_sell] 재주문실패 {name}({code})"
                    f"  수량={qty}  기준가={ref_price:,.0f}  주문방식={ord_dvsn_name}({ord_dvsn})"
                    f"  사유={reason} err={e2}",
                    tele=True,
                )
                with _str1_sell_state_lock:
                    if code in _str1_sell_state:
                        _str1_sell_state[code]["sell_ordered"] = False
                        _str1_sell_state[code]["sold"] = False

    logger.info("[str1_sell] worker stopped")


# 15:19:30 일괄 매도 — 1일 1회 실행 플래그
_sell_1518_done: bool = False


def _run_sell_1518() -> None:
    """
    15:19:30 조건부 전체 매도.

    - _str1_sell_state 에 등록된 미매도 포지션을 전부 점검
    - 보유 조건: 전일대비 20% 이상 AND 당일 최고가 대비 현재가 -5% 이내
    - 위 조건 미충족 종목은 즉시 시장가 매도
    - 하루 1회만 실행
    """
    global _sell_1518_done
    if _sell_1518_done:
        return

    KEEP_THRESHOLD = 20.0   # 전일대비 이 값 이상이면 보유 후보
    HIGH_MARGIN = 0.05      # 당일 최고가 대비 5% 이내

    if not STR1_SELL_ENABLED:
        _sell_1518_done = True
        if not _str1_sell_state:
            _notify(f"{ts_prefix()} [일괄매도] 15:19:30 시작 | 보유종목 0건 | 매도대상 없음", tele=True)
        return

    if not _str1_sell_state:
        _sell_1518_done = True
        _notify(f"{ts_prefix()} [일괄매도] 15:19:30 시작 | 보유종목 0건 | 매도대상 없음", tele=True)
        return

    sell_list: list[tuple] = []
    keep_list: list[str] = []

    # 현재가/고가 조회용 클라이언트
    client = _top_client or _init_top_client()

    with _str1_sell_state_lock:
        active_positions = {
            code: dict(st)
            for code, st in _str1_sell_state.items()
            if not st.get("sold") and not st.get("sell_ordered")
        }

    for code, st in active_positions.items():
        prdy = float(_last_prdy_ctrt.get(code, 0.0))
        name = code_name_map.get(code, code)

        # 고가 대비 현재가 확인 (REST API)
        high_ratio = 1.0  # 기본값: 고가 = 현재가
        try:
            resp = client.inquire_price(code)
            stck_prpr = float(str(resp.get("stck_prpr") or 0).replace(",", "") or 0)
            stck_hgpr = float(str(resp.get("stck_hgpr") or 0).replace(",", "") or 0)
            if stck_hgpr > 0 and stck_prpr > 0:
                high_ratio = stck_prpr / stck_hgpr
        except Exception:
            pass

        # 보유 조건: 전일대비 20%+ AND 고가 대비 -5% 이내
        keep = (prdy >= KEEP_THRESHOLD) and (high_ratio >= (1 - HIGH_MARGIN))
        if keep:
            keep_list.append(f"{name}({code}) {prdy:+.1f}% 고가비={high_ratio:.1%}")
        else:
            sell_list.append((code, prdy, high_ratio))

    total = len(active_positions)
    lines = [
        f"[일괄매도] 15:19:30 시작 | 보유종목 {total}건 | "
        f"매도 {len(sell_list)}건 / 유지 {len(keep_list)}건 "
        f"(기준: 전일대비 {KEEP_THRESHOLD}%+ AND 고가대비 -{HIGH_MARGIN*100:.0f}% 이내)"
    ]
    if keep_list:
        lines.append(f"  유지: {', '.join(keep_list)}")
    if sell_list:
        sell_desc = [f"{code_name_map.get(c,c)}({c}) {p:+.1f}% 고가비={h:.1%}" for c, p, h in sell_list]
        lines.append(f"  매도: {', '.join(sell_desc)}")
    _notify("\n".join(lines), tele=True)
    logger.info("\n".join(lines))

    for code, prdy, _ in sell_list:
        reason = f"일괄매도(prdy_ctrt={prdy:+.1f}%)"
        with _str1_sell_state_lock:
            bp = float(_str1_sell_state.get(code, {}).get("buy_price", 0) or 0)
        ref_price = bp * (1 + prdy / 100) if bp > 0 and abs(prdy) < 100 else 0.0
        _enqueue_str1_sell(code, reason, ref_price)

    _sell_1518_done = True


def _enqueue_str1_sell(
    code: str, reason: str, ref_price: float,
    ord_dvsn: str = "01", price: float = 0.0, cndt_pric: float = 0.0,
) -> None:
    """ingest_loop에서 호출: 매도 조건 충족 시 sell worker에 비동기 주문 요청."""
    if code in _no_sell_codes:
        logger.debug(f"{ts_prefix()} [no_sell] 매도 차단: {code} 사유={reason}")
        return
    is_stop_limit = (ord_dvsn == "22")
    with _str1_sell_state_lock:
        st = _str1_sell_state.get(code)
        if not st or st.get("sold"):
            return
        if st.get("sell_ordered"):
            return  # 이미 매도 주문 진행 중 → 중복 방지
        if is_stop_limit and st.get("stop_limit_ordered"):
            return  # 스톱지정가 중복 방지
        if reason.startswith("str1_시초가하락_예상") and st.get("opening_call_auction_ordered"):
            return  # 사전 매도 주문 중복 방지
        qty  = int(st.get("qty") or 0)
        name = st.get("name", code)
        if qty <= 0:
            return
        # 상태 선점 (중복 큐잉 방지)
        is_opening_call_auction = reason.startswith("str1_시초가하락_예상")
        if is_stop_limit:
            _str1_sell_state[code]["stop_limit_ordered"] = True
            # sold=True 하지 않음 — 조건 미충족 시 미체결이므로 일반 매도 계속 가능
        elif is_opening_call_auction:
            _str1_sell_state[code]["sell_ordered"] = True
            _str1_sell_state[code]["opening_call_auction_ordered"] = True
        else:
            _str1_sell_state[code]["sell_ordered"] = True
            _str1_sell_state[code]["sold"] = True  # 낙관적 선점
    sell_type = "스톱지정가매도" if is_stop_limit else ("동시호가매도" if is_opening_call_auction else "매도")
    logger.info(
        f"{ts_prefix()} [str1_sell] ▶ {sell_type} 큐등록 {name}({code})"
        f"  수량={qty}  기준가={ref_price:,.0f}  사유={reason}"
    )
    _str1_sell_queue.put({
        "action": "sell",
        "code": code, "qty": qty, "reason": reason,
        "ref_price": ref_price, "name": name,
        "ord_dvsn": ord_dvsn, "price": price, "cndt_pric": cndt_pric,
        # [260521] 발동(큐등록)→주문 전송 내부 지연 계측용 (sell_latency 로그)
        "enq_ts": time.time(),
    })


_periodic_sell_check_ts: float = 0.0   # 마지막 주기적 매도 체크 시각
PERIODIC_SELL_CHECK_SEC: float = 15.0  # 주기 체크 (초) — 메인은 틱 기반


def _check_sell_by_prdy_ctrt_periodic() -> None:
    """
    틱 수신과 무관하게 주기적으로 _last_prdy_ctrt 캐시를 확인해
    전일종가 이하 하락 포지션을 즉시 시장가 매도.

    - 메인 매도 판단은 ingest_loop의 _check_str1_sell_conditions (틱 기반)
    - 이 함수는 저거래량 종목(틱 미수신) 누락 보완용 주기 체크 (15초 간격)
    - scheduler_loop 에서 호출
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
            if not st.get("sold") and not st.get("sell_ordered") and not st.get("opening_call_auction_ordered")
            and code not in _no_sell_codes
            and code not in _uplimit_positions  # [v_1] 상한가 근접 매수 종목은 EMERGENCY/3%손절 제외 (별도 Exit 로직)
        ]

    for code, st in targets:
        bp = float(st.get("buy_price") or 0)
        if bp <= 0:
            continue
        cur_price = _last_stck_prpr.get(code, 0.0)
        if cur_price <= 0:
            continue
        if cur_price < bp * 0.97:
            reason = f"3%손절-주기체크(cur={cur_price:.0f}/buy={bp:.0f}={cur_price/bp*100-100:+.2f}%)"
            _enqueue_str1_sell(code, reason, cur_price)
            sold_list.append(f"{st.get('name', code)}({code}) {cur_price/bp*100-100:+.1f}%")

    if sold_list:
        msg = f"{ts_prefix()} [주기매도체크] 틱기반매도 미실행 → 캐시확인 매도 {len(sold_list)}건: {', '.join(sold_list)}"
        _notify(msg, tele=True)
        logger.info(msg)


def _enqueue_str1_opening_call_auction_cancel(code: str, reason: str) -> None:
    """ingest_loop에서 호출: 장전 매도 주문 후 예상가 복귀 시 취소 요청."""
    with _str1_sell_state_lock:
        st = _str1_sell_state.get(code)
        if not st or st.get("sold"):
            return
        if not st.get("opening_call_auction_ordered"):
            return
        ordno = str(st.get("ordno", "")).strip()
        if not ordno:
            return  # worker가 아직 주문 처리 전
        if st.get("opening_call_auction_cancel_requested"):
            return  # 중복 취소 방지
        if st.get("cancel_retry_cnt", 0) >= 5:
            return  # 5회 실패 → 재시도 차단
        qty = int(st.get("qty") or 0)
        name = st.get("name", code)
        if qty <= 0:
            return
        _str1_sell_state[code]["opening_call_auction_cancel_requested"] = True
    logger.info(
        f"{ts_prefix()} [str1_sell] ▶ 동시호가매도 취소 큐등록 {name}({code})"
        f"  수량={qty}  원주문번호={ordno}  사유={reason}"
    )
    _str1_sell_queue.put({
        "action": "cancel",
        "code": code, "qty": qty, "ordno": ordno, "reason": reason, "name": name,
    })


_ccnl_notice_header_written = False

_CCNL_CSV_COLUMNS = [
    "date", "time", "cust_id", "acnt_no", "stck_shrn_iscd", "cntg_isnm40",
    "oder_no", "cntg_qty", "cntg_unpr", "stck_cntg_hour",
    "cntg_yn", "acpt_yn", "acnt_no2", "oder_qty", "seln_byov_cls",
    "rctf_cls", "oder_kind", "oder_cond", "rfus_yn", "brnc_no",
    "acnt_name", "ord_cond_prc", "ord_exg_gb", "popup_yn", "filler",
    "crdt_cls", "crdt_loan_date", "oder_prc",
]

def _write_ccnl_notice_log(df, recv_ts: str) -> None:
    """H0STCNI0 체결통보 수신 데이터를 CSV 파일에 저장."""
    global _ccnl_notice_header_written
    try:
        parts = recv_ts.split(" ", 1)
        date_str = parts[0]
        time_str = parts[1] if len(parts) > 1 else ""

        out = df.with_columns([
            pl.lit(date_str).alias("date"),
            pl.lit(time_str).alias("time"),
        ])

        # 없는 컬럼은 빈 값으로 추가
        for c in _CCNL_CSV_COLUMNS:
            if c not in out.columns:
                out = out.with_columns(pl.lit("").alias(c))
        out = out.select(_CCNL_CSV_COLUMNS)

        with open(CCNL_NOTICE_CSV_PATH, "a", encoding="utf-8") as f:
            if not _ccnl_notice_header_written:
                f.write(",".join(_CCNL_CSV_COLUMNS) + "\n")
                _ccnl_notice_header_written = True
            for row in out.to_dicts():
                f.write(",".join(str(row[c]) for c in _CCNL_CSV_COLUMNS) + "\n")
    except Exception as e:
        logger.warning(f"{ts_prefix()} [체결통보로그] 저장 실패: {e}")


_closing_filled_by_code: dict[str, int] = {}  # code -> 체결 누적 수량 (H0STCNI0 체결 시 갱신)

# ── H0STCNI0 복호화 실패 의심 경고 1회 발송 플래그 (260512) ──
# cust_id 가 평문 HTS ID 가 아닌 Base64 형태이면 복호화가 실패한 정황으로 보고
# 텔레그램으로 1회만 알림 (스팸 방지).
_h0stcni0_decrypt_warn_sent: bool = False


# ── H0STCNI0 종목별 구독 관리 ──────────────────────────────────────────────────
_ccnl_notice_sub_codes: set[str] = set()  # 종목별 H0STCNI0 구독 중인 종목코드
_ccnl_notice_order_ts: dict[str, float] = {}  # 주문 후 체결확인용 타임스탬프


def _ccnl_notice_sub_add(code: str) -> None:
    """주문 발생 시 H0STCNI0 체결통보 구독 등록. tr_key는 htsid(종목코드 아님)."""
    if code in _ccnl_notice_sub_codes:
        return
    # [260428] SKIP_CCNI0_ON_INIT=1 일 때는 종목별 sub_add 도 건너뜀 (storm 재발 방지)
    if os.environ.get("SKIP_CCNI0_ON_INIT") == "1":
        logger.info(f"{ts_prefix()} [체결통보] H0STCNI0 sub_add SKIP ({code}, env toggle)")
        return
    htsid = _get_ccnl_notice_tr_key()
    if not htsid:
        logger.warning(f"{ts_prefix()} [체결통보] htsid 미설정 → H0STCNI0 구독 불가: {code}")
        return
    with _kws_lock:
        if _active_kws is not None:
            try:
                _send_subscribe(_active_kws, ccnl_notice, [htsid], "1")  # noqa: F405
                _ccnl_notice_sub_codes.add(code)
                _ccnl_notice_order_ts[code] = time.time()
                name = code_name_map.get(code, code)
                logger.info(f"{ts_prefix()} [체결통보] H0STCNI0 구독 추가: {name}({code}), tr_key={htsid}")
            except Exception as e:
                logger.warning(f"{ts_prefix()} [체결통보] H0STCNI0 구독 실패: {code} {e}")


def _ccnl_notice_sub_remove(code: str) -> None:
    """전량 체결 시 종목 추적만 제거. WSS 해제는 보내지 않음 (htsid 단위이므로 타 종목 영향)."""
    if code not in _ccnl_notice_sub_codes:
        return
    _ccnl_notice_sub_codes.discard(code)
    _ccnl_notice_order_ts.pop(code, None)
    name = code_name_map.get(code, code)
    logger.info(f"{ts_prefix()} [체결통보] 종목 추적 해제: {name}({code}), WSS 구독은 유지")


# ── Str1 실전 매도 상태 ──────────────────────────────────────────────────────
# code → {"sold": bool, "sell_reason": str, "qty": int, "sell_price": float,
#          "sell_time": str, "ordno": str, "pnl": float, "ret_pct": float,
#          "buy_price": float, "opening_call_auction_ordered": bool}
_str1_sell_state: dict[str, dict] = {}
_str1_sell_state_lock = threading.Lock()
# 매도 주문 큐: ingest_loop → sell_worker (비동기 처리)
_str1_sell_queue: "queue.Queue[dict]" = queue.Queue()
# 종목별 보유 계좌 매핑: _get_balance_holdings()에서 구축, 매도 시 올바른 계좌로 주문
_code_account_map: dict[str, dict] = {}
# ── 매도 금지 종목 (no_sell_codes) ─────────────────────────────────────────────
# config.json의 no_sell_codes 배열에서 로드, 런타임 추가 가능
_no_sell_codes: set[str] = set()
# ── EMA 데드크로스 상태 기반 매도 ──────────────────────────────────────────────
# up_trend 지속 상태: 정배열(ma50>ma200>ma300>ma500>ma2000>wghn) 유지 중이면 True
_up_trend_state: dict[str, bool] = {}
_oprc_cache: dict[str, float] = {}        # {code: stck_oprc} 당일 시가 캐시 (불변값)
# EMA 매도 조건: up_trend→ma500<ma2000 데드크로스 발생 시 True, 매도 성공까지 유지
_ema_sell_cond: dict[str, bool] = {}

# ── VI 발동 매수 포지션 추적 ──────────────────────────────────────────────────
# code → {buy_price, buy_ts, qty, highest, sold, name, ordno}
_vi_positions: dict[str, dict] = {}

# ── [v_1 신규] 상한가 근접 매수 포지션 추적 ────────────────────────────────
# code → {buy_price, buy_ts, qty, ordno, max_prdy_ctrt, highest_since_buy,
#         half_sold, name, status}
_uplimit_positions: dict[str, dict] = {}
_uplimit_buy_pending: dict[str, str] = {}   # code → ordno (매수 주문 송신 후 체결 대기)
_uplimit_exit_pending: dict[str, str] = {}  # code → ordno (매도 주문 송신 후 체결 대기)
_uplimit_today_buy_count: int = 0
_uplimit_blacklist: set[str] = set()        # 당일 재매수 금지 (정상 단계청산 후만 추가; 25% 하회 시장가 매도 후는 재매수 허용)
_uplimit_state_lock = threading.Lock()
_uplimit_state_last_save_ts: float = 0.0
_uplimit_seed_base = 0.0                    # [260423] float 시드 베이스 (세션 시작 시 INIT_CASH / UPLIMIT_DIVERSIFY_N 으로 계산, annotation 제거 — global 선언 호환)

# Signal Strength 보조 캐시
_uplimit_day_vol_bucket: dict[str, list] = {}        # code → [(minute_ts_int, minute_vol), ...] (slide 5분)
_uplimit_prev_min_vol: dict[str, tuple[int, float]] = {}  # code → (cur_minute_ts, cur_minute_acml_vol) — 분봉 경계 계산용
_uplimit_25pct_cross_ts: dict[str, datetime] = {}    # code → 25% 최초 돌파 시각

# [260424] Strategy A-v4 상태 (호환 보존 — 일부 dict 는 v5 에서 재사용)
_uplimit_v4_28pct_first_ts: dict[str, datetime] = {}   # (deprecated v4 sustain — v5 에서 미사용)
_uplimit_v4_holdover: dict[str, dict] = {}             # code → {buy_price, buy_ts, qty, buy_date, hold_reason, buy_source}
                                                        # 14:55 에 29.5%+ 이었던 종목 (익일까지 보유) — v5 에서도 사용
_uplimit_v4_daily_bought_codes: set[str] = set()        # 당일 매수 이력 (중복 방지) — v5 에서도 사용
_uplimit_v4_last_sustain_log: dict[str, float] = {}     # (deprecated)
_uplimit_vola_10d: dict[str, float] = {}             # code → 10일 로그수익률 std (장 시작 전 계산)
_uplimit_prev_day_ctrt: dict[str, float] = {}        # code → 전일 등락률
_uplimit_volume_power: dict[str, tuple[float, float]] = {}  # code → (value, fetched_ts)
_uplimit_frgn_3d: dict[str, float] = {}              # code → 외국인 3일 순매수
_uplimit_precache_done: bool = False

# [260427] Strategy A-v5 상태
_uplimit_v5_evaluated: set[str] = set()             # 당일 1회 매수 평가 완료 종목 (재평가 방지)
_uplimit_v5_limitup_reached: set[str] = set()       # 30% 도달 이력 — 매수 차단 대상
_uplimit_v5_observe_log: set[str] = set()           # 20~25% 관찰 로그 1회 기록 종목
_uplimit_prev_day_amount: dict[str, float] = {}     # code → 전일 거래대금 (저거래 판별)
_uplimit_v5_last_acml: dict[str, float] = {}        # code → 직전 수신 acml_tr_pbmn (저거래 fallback)
# [260427] v5 종목 선정/트리거 분리 (12필터 통과 후 ma10/BB 반등 트리거 대기)
_uplimit_v5_qualified_ts: dict[str, datetime] = {}  # code → 12필터 통과 시각 (10분 만료)
_uplimit_v5_last_bidp1: dict[str, float] = {}       # code → 직전 틱 bidp1 (BB 반등 판정)
_uplimit_v5_lower_breached: set[str] = set()        # qualified 이후 bb_lower 이탈 이력
# [260427] Phase 5: 외인 데이터 lazy fetch 시도 이력 (종목당 1회만 시도)
_uplimit_frgn_3d_fetch_attempted: set[str] = set()

# [260423] Strategy B (MA trend) 신호 관찰 상태
_ma_trend_prev_ma50: dict[str, float] = {}    # code → 직전 틱 ma50 (골든크로스 edge 판정)
_ma_trend_prev_ma500: dict[str, float] = {}   # code → 직전 틱 ma500
_ma_trend_last_signal_log: dict[str, float] = {}  # code → 마지막 시그널 로그 시각 (쿨다운)

# [260423] Strategy C (BB expansion) 신호 관찰 상태
_bb_width_history: dict[str, deque] = {}      # code → 최근 100틱 bb_width deque
_bb_lower_cross_history: dict[str, deque] = {}  # code → 최근 30틱 (bidp1 < bb_lower) bool deque
_bb_exp_last_signal_log: dict[str, float] = {}   # 쿨다운
_bb_last_bidp1: dict[str, float] = {}            # 직전 bidp1 (상승 첫 양틱 판정)

# ── [260523] Strategy str2 상태 (str1 sell_state 와 완전 분리) ──────────────────
# 종목별 전략 상태 (Str2State). 호출자가 매 틱 갱신하며 strat2 판단 함수에 전달.
_str2_state: dict[str, "Str2State"] = {}
_str2_state_lock = threading.Lock()
_str2_state_last_save_ts: float = 0.0
_str2_session_setup_done: set[str] = set()   # 08:58 분류 1회 완료 종목
_str2_premarket_0855: dict[str, float] = {}  # code → 08:55 직전 현재가 스냅샷
# 히스토리 파생 버퍼 (회신 §3 line 51: 직전 ROLL(50)틱 min(bidp1)/max(askp1))
_str2_bidp_buf: dict[str, deque] = {}        # code → 직전 50틱 bidp1 deque
_str2_askp_buf: dict[str, deque] = {}        # code → 직전 50틱 askp1 deque
# bb_hi_max = rolling_max(bb_upper, 600). baked bb_upper(ddof=0) 를 매 틱 push.
#   ※ ddof 결정: 로컬 회신 §2/§3 은 σ ddof=1 명시하나, 서버 파리티 기준은
#     _calc_indicators 가 산출하는 baked BB(모집단분산 ddof=0)이므로 ddof=0 채택.
#     bb_lo/bb_hi/bb_hi_max 전부 동일 baked bb(ddof=0) 기반 → str2 판단이 쓰는 bb 값과
#     bb_hi_max 소스가 동일. 로컬 백테스트가 baked BB(ddof=0)를 그대로 써야 backtest=live 성립.
_str2_bb_upper_hist: dict[str, deque] = {}   # code → 최근 600틱 baked bb_upper(ddof=0) deque
_str2_last_acml_amt: dict[str, float] = {}   # code → 직전 acml_tr_pbmn (참고)

# ── 장전 동시호가 모니터링 ──────────────────────────────────────────────────
# code → {antc_prce, prdy_ctrt, buy_price, in_sell_cond, first_sell_ts, first_printed}
_opening_call_auction_watch: dict[str, dict] = {}
_opening_call_auction_last_summary_ts: float = 0.0   # 1분 주기 출력용
OPENING_CALL_AUCTION_SUMMARY_INTERVAL = 60.0         # 주기적 출력 간격 (초)
_opening_call_auction_order_summary_done: bool = False  # 08:59:50 주문현황 정리 1회 출력용
_opening_call_auction_cancelled_log: list[tuple[str, str, str]] = []  # (code, name, reason) 장전 취소 완료


def _on_ccnl_notice_filled(df, recv_ts: str) -> None:
    """H0STCNI0 체결통보(CNTG_YN==2) 수신 시 filled_qty 갱신 및 state 저장."""
    # 컬럼명은 on_result에서 이미 소문자 통일됨
    cols = set(df.columns)
    if not all(c in cols for c in ("cntg_yn", "stck_shrn_iscd", "cntg_qty")):
        return
    for row in df.to_dicts():
        try:
            cntg_yn = str(row.get("cntg_yn", "")).strip()
            if cntg_yn != "2":  # 2=체결통보
                continue
            code = str(row.get("stck_shrn_iscd", "")).strip().zfill(6)
            if not code:
                continue
            qty = int(float(str(row.get("cntg_qty", 0) or 0).replace(",", "") or 0))
            if qty <= 0:
                continue
            # 매수/매도 체결 누적
            seln = str(row.get("seln_byov_cls", "")).strip()
            # 체결 단가
            fill_pr_col = "cntg_unpr" if "cntg_unpr" in cols else ("stck_prpr" if "stck_prpr" in cols else None)
            fill_pr = 0.0
            if fill_pr_col:
                try:
                    fill_pr = float(str(row.get(fill_pr_col, 0) or 0).replace(",", "") or 0)
                except Exception:
                    pass
            name_fill = code_name_map.get(code, "") or code

            # [v_1] uplimit 체결 처리 우선 (매수/매도 양쪽)
            if code in _uplimit_positions or code in _uplimit_buy_pending or code in _uplimit_exit_pending:
                try:
                    _handle_uplimit_ccnl_fill(code, seln, qty, fill_pr, name_fill, recv_ts)
                except Exception as _e:
                    logger.warning(f"{ts_prefix()} [uplimit_ccnl] {name_fill}({code}) 처리 오류: {_e}")

            # [260523] str2 체결 처리 — str2 보유/주문 종목이면 str2 가 단독 소유 (str1 자동등록 차단)
            _is_str2_code = False
            if STR2_ENABLED and code in _str2_state:
                _is_str2_code = True
                try:
                    _handle_str2_ccnl_fill(code, seln, qty, fill_pr, name_fill, recv_ts)
                except Exception as _e:
                    logger.warning(f"{ts_prefix()} [str2_ccnl] {name_fill}({code}) 처리 오류: {_e}")

            # 외부주문 체결 매칭 (ordno 가 _external_order_track 에 있을 때만 알림)
            try:
                _ext_ordno = str(row.get("oder_no") or row.get("odno") or row.get("ord_no") or "").strip()
                if _ext_ordno:
                    with _external_order_track_lock:
                        _ext_info = _external_order_track.get(_ext_ordno)
                    if _ext_info:
                        _ext_amt = qty * fill_pr
                        _ext_msg = (
                            f"{ts_prefix()} [외부주문] #{_ext_info['no']} [{_ext_info['alias']}] 체결: "
                            f"{_ext_info['action']} {_ext_info['name']}({_ext_info['code']}) "
                            f"{qty}주 × {fill_pr:,.0f}원 = {_ext_amt:,.0f}원 ordno={_ext_ordno}"
                        )
                        _notify(_ext_msg, tele=True)
                        logger.info(_ext_msg)
            except Exception as _e:
                logger.warning(f"{ts_prefix()} [외부주문] 체결 매칭 오류: {_e}")

            if seln in ("01", "1"):  # 매도
                _closing_filled_by_code[code] = max(0, _closing_filled_by_code.get(code, 0) - qty)
                # Str1 장전 매도 체결 시 sold=True 전환
                with _str1_sell_state_lock:
                    st = _str1_sell_state.get(code)
                    if st and st.get("opening_call_auction_ordered") and not st.get("sold"):
                        _str1_sell_state[code]["sold"] = True
                        _str1_sell_state[code]["sell_reason"] = st.get("sell_reason") or "str1_시초가하락_예상"
                        logger.info(f"{ts_prefix()} [체결통보] Str1 동시호가매도 체결 {code} 수량={qty}")
                        _save_str1_sell_state()
                        fill_msg = f"{ts_prefix()} [체결통보] Str1 동시호가매도 체결 {name_fill}({code}) 수량={qty}주"
                        logger.info(fill_msg)
                        sys.stdout.write(f"\n{fill_msg}\n")
                        sys.stdout.flush()
                # 거래장부: 매도 체결
                with _str1_sell_state_lock:
                    st2 = _str1_sell_state.get(code, {})
                    bp2 = float(st2.get("buy_price") or 0)
                    sr2 = st2.get("sell_reason", "")
                _append_ledger(
                    order_type="sell_fill",
                    code=code, name=name_fill,
                    buy_price=bp2, sell_price=fill_pr,
                    fill_qty=qty,
                    reason=f"{sr2}_체결" if sr2 else "",
                    prdy_ctrt=float(_last_prdy_ctrt.get(code, 0.0)),
                    note=f"체결통보 {recv_ts}",
                    stck_prpr=fill_pr,
                )
            else:  # 매수
                _closing_filled_by_code[code] = _closing_filled_by_code.get(code, 0) + qty
                # 거래장부: 매수 체결 — reason 결정
                _buy_fill_reason = ""
                if any(p.get("code") == code for p in _closing_buy_prepared):
                    _buy_fill_reason = "종가매수_체결(15:20)"
                elif code in _vi_buy_pending:
                    _buy_fill_reason = "VI매수_체결"
                else:
                    _buy_fill_reason = "매수_체결"
                _append_ledger(
                    order_type="buy_fill",
                    code=code, name=name_fill,
                    buy_price=fill_pr,
                    fill_qty=qty,
                    prdy_ctrt=float(_last_prdy_ctrt.get(code, 0.0)),
                    note=f"체결통보 {recv_ts}",
                    stck_prpr=fill_pr,
                    reason=_buy_fill_reason,
                )
                # ── 매수 체결 시 매도전략 자동 등록 + 실시간 구독 확보 ──
                # [260523] str2 종목은 str2 가 단독 소유 → str1 sell_state 자동등록 차단 (이중 매도 방지)
                _need_sub = False
                if not _is_str2_code:
                    with _str1_sell_state_lock:
                        if code not in _str1_sell_state:
                            _str1_sell_state[code] = {
                                "sold": False, "sell_reason": "", "qty": qty,
                                "sell_price": 0.0, "sell_time": "", "ordno": "",
                                "pnl": 0.0, "ret_pct": 0.0,
                                "buy_price": fill_pr, "name": name_fill,
                                "opening_call_auction_ordered": False, "balance_qty": qty,
                                "source": _buy_fill_reason,
                                "loss_below_count": 0,
                            }
                            _need_sub = True
                            logger.info(f"{ts_prefix()} [체결통보] _str1_sell_state 자동등록: {name_fill}({code}) buy={fill_pr:,.0f} qty={qty}")
                        else:
                            # 이미 등록된 경우: 수량 누적, buy_price 보정 (부분체결 대응)
                            _st = _str1_sell_state[code]
                            _st["qty"] = _st.get("qty", 0) + qty
                            _st["balance_qty"] = _st.get("balance_qty", 0) + qty
                            if _st.get("buy_price", 0) <= 0:
                                _st["buy_price"] = fill_pr
                if _need_sub:
                    _ensure_code_structs([code])
                    with _lock:
                        _base_codes.add(code)
                    _trigger_ws_rebuild()
                    logger.info(f"{ts_prefix()} [체결통보] 실시간 구독 추가: {name_fill}({code})")
            # 전량 체결 시 H0STCNI0 구독 해제 (슬롯 반환)
            if seln in ("01", "1"):
                # 매도: sold=True인 경우 구독 해제
                with _str1_sell_state_lock:
                    st_chk = _str1_sell_state.get(code, {})
                    if st_chk.get("sold"):
                        _ccnl_notice_sub_remove(code)
            else:
                # 매수: 체결 누적이 주문수량 이상이면 구독 해제
                state_chk = _load_closing_buy_state()
                if state_chk and state_chk.get("orders"):
                    for o_chk in state_chk["orders"]:
                        c_chk = str(o_chk.get("code", "")).zfill(6)
                        if c_chk == code:
                            oq = int(o_chk.get("order_qty", o_chk.get("qty", 0)))
                            fq = _closing_filled_by_code.get(code, 0)
                            if fq >= oq:
                                _ccnl_notice_sub_remove(code)
                            break

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


def _restore_orders_from_server_on_startup() -> None:
    """15:20~18:00 재시작 시 KIS 서버에서 미체결 매수주문 조회 → codes/체결통보/tracking 복원.

    기존 _closing_buy_retry (15:20~15:30 재주문)와 보완적:
    - 이 함수는 재주문을 하지 않음. 이미 서버에 접수된 주문을 그대로 두고
      로컬 트래킹(codes 구독, H0STCNI0 체결통보 구독, _today_closing_target_codes)만 복원.
    """
    global _today_closing_target_codes, _today_closing_target_done, _overtime_order_done

    if not CLOSE_BUY:
        return
    now_t = datetime.now(KST).time()
    if not (dtime(15, 20) <= now_t < dtime(18, 0)):
        return

    try:
        accounts = _iter_enabled_accounts(trade_only=True)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [restart] 계좌 조회 실패: {e}")
        return

    all_orders: list[dict] = []   # {code, name, ordno, ord_qty, psbl_qty, filled, ord_dvsn, sll_buy, acct_alias}
    for acct in accounts:
        cano = str(acct.get("cano", "")).strip()
        acnt = str(acct.get("acnt_prdt_cd", "01")).strip() or "01"
        alias = acct.get("alias", acct.get("account_id", ""))
        if not cano:
            continue
        try:
            client = _init_account_client(acct)
            raw_orders = _inquire_psbl_rvsecncl(client, cano, acnt, sll_buy_dvsn_cd="02")  # 전체
        except Exception as e:
            logger.warning(f"{ts_prefix()} [restart] {alias} 주문조회 실패: {e}")
            continue

        for o in raw_orders:
            code = str(o.get("pdno") or o.get("PDNO") or "").strip().zfill(6)
            if not code or code == "000000":
                continue
            ordno = str(o.get("odno") or o.get("ODNO") or "").strip()
            ord_qty = int(float(str(o.get("ord_qty") or o.get("ORD_QTY") or 0).replace(",", "") or 0))
            psbl_qty = int(float(str(o.get("psbl_qty") or o.get("PSBL_QTY") or 0).replace(",", "") or 0))
            ord_dvsn = str(o.get("ord_dvsn_cd") or o.get("ORD_DVSN_CD") or "").strip()
            sll_buy = str(o.get("sll_buy_dvsn_cd") or o.get("SLL_BUY_DVSN_CD") or "").strip()
            # 매수만 복원 (매도 미체결은 _load_str1_sell_state_on_startup이 담당)
            if sll_buy != "02":
                continue
            if psbl_qty <= 0:
                continue
            all_orders.append({
                "code": code,
                "name": code_name_map.get(code, code),
                "ordno": ordno,
                "ord_qty": ord_qty,
                "psbl_qty": psbl_qty,
                "filled": max(0, ord_qty - psbl_qty),
                "ord_dvsn": ord_dvsn,
                "acct_alias": alias,
                "cano": cano,
            })

    if not all_orders:
        logger.info(f"{ts_prefix()} [restart] 미체결 매수주문 없음 → 복원 스킵")
        return

    # 종목별 요약
    restore_codes: set[str] = set()
    for o in all_orders:
        restore_codes.add(o["code"])

    # 1) codes 추가 (WSS 구독 대상)
    try:
        added = _ensure_code_structs(list(restore_codes))
        if added:
            logger.info(f"{ts_prefix()} [restart] codes 복원: {len(added)}종목")
    except Exception as e:
        logger.warning(f"{ts_prefix()} [restart] codes 복원 실패: {e}")

    # 2) H0STCNI0 체결통보 대상 등록 (WSS 연결 전이므로 set에만 등록, 실제 구독은 run_ws_forever)
    now_ts = time.time()
    for code in restore_codes:
        _ccnl_notice_sub_codes.add(code)
        _ccnl_notice_order_ts[code] = now_ts

    # 3) 시간대별 복원: 16:00~18:00이면 _today_closing_target_codes로, 그 전이면 _closing_codes로
    if dtime(16, 0) <= now_t < dtime(18, 0):
        _today_closing_target_codes = list(restore_codes)
        _today_closing_target_done = True   # 재선정 스크립트 재실행 방지
        _overtime_order_done = True         # 16:00 주문 중복 방지
        logger.info(
            f"{ts_prefix()} [restart] 16:00~18:00 시간외단일가 미체결 복원: "
            f"{len(restore_codes)}종목 → _today_closing_target_codes"
        )
    else:
        global _closing_codes, _closing_code_info
        for code in restore_codes:
            if code not in _closing_codes:
                _closing_codes.append(code)
            if code not in _closing_code_info:
                _closing_code_info[code] = {"name": code_name_map.get(code, code)}
        logger.info(
            f"{ts_prefix()} [restart] 15:20~16:00 종가/장후시간외 미체결 복원: "
            f"{len(restore_codes)}종목 → _closing_codes"
        )

    # 4) _closing_buy_state 재구성 (체결 추적 로직과 호환)
    try:
        orders_json = []
        for o in all_orders:
            orders_json.append({
                "code": o["code"], "name": o["name"],
                "order_qty": o["ord_qty"],
                "limit_up": 0,
                "ordno": o["ordno"],
                "status": "ordered",
                "filled_qty": o["filled"],
                "remain_qty": o["psbl_qty"],
                "acct_alias": o["acct_alias"],
            })
        code_info = {o["code"]: {"name": o["name"]} for o in all_orders}
        _save_closing_buy_state(list(restore_codes), code_info, orders_json)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [restart] state 저장 실패: {e}")

    # 5) 요약 로그 + 텔레그램
    by_acct: dict[str, list[str]] = {}
    for o in all_orders:
        alias = o["acct_alias"] or "main"
        by_acct.setdefault(alias, []).append(
            f"{o['name']}({o['code']}) 미체결={o['psbl_qty']}/{o['ord_qty']} ordno={o['ordno']}"
        )
    lines = [f"{ts_prefix()} [restart] 서버 미체결 매수주문 복원: 총 {len(all_orders)}건 ({len(restore_codes)}종목)"]
    for alias, items in by_acct.items():
        lines.append(f"  [{alias}] {len(items)}건")
        for item in items:
            lines.append(f"    {item}")
    _notify("\n".join(lines), tele=True)


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


_closing_buy_prepared: list[dict] = []   # 15:19 사전 준비된 주문 데이터
_closing_buy_prepare_done: bool = False  # 사전 준비 완료 플래그
_closing_acct_clients: dict[str, "KisClient"] = {}  # 15:19 사전 초기화된 계좌 클라이언트


def _prepare_closing_buy_orders() -> None:
    """15:19 종가매수 사전 준비: 종목선정 + 계좌별 수량계산 + 주문 데이터 생성 (주문 발송 안 함).

    Phase 4-4: 다중 계좌 순회 — 모든 활성 계좌에 대해 종목별 주문 준비.
    각 계좌의 가용 현금(주문가능금액 - exclude_cash)을 기준으로 수량 산출.
    """
    global _closing_buy_prepare_done
    if not CLOSE_BUY or not _closing_codes or _closing_buy_prepare_done:
        return
    _closing_buy_prepare_done = True
    _closing_buy_prepared.clear()
    try:
        accounts = _iter_enabled_accounts(trade_only=True)
        if not accounts:
            logger.warning(f"{ts_prefix()} [종가매수준비] 활성 계좌 없음")
            return

        # 가격 조회용 클라이언트 (메인 계좌)
        client = _top_client or _init_top_client()
        n = len(_closing_codes)
        FEE_RATE = 0.00015

        prev_close_1d = _get_prev_close_from_1d(_closing_codes)
        if prev_close_1d:
            _notify(f"{ts_prefix()} [종가매수준비] 1d parquet 전일종가 {len(prev_close_1d)}건 로드")

        # 종목별 전일종가·상한가 사전 조회
        code_prices: dict[str, dict] = {}
        _filtered_out: list[tuple[str, str]] = []  # [v_1] 폭락 필터로 제외된 종목 (name, reason)
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
                logger.warning(f"{ts_prefix()} [종가매수준비] {code} 전일종가 없음, 스킵")
                continue

            name = info.get("name", code_name_map.get(code, code))

            # [v_1] 폭락 방지 필터 적용 (CLOSING_CRASH_FILTER_ENABLED)
            if CLOSING_CRASH_FILTER_ENABLED:
                try:
                    resp = client.inquire_price(code) or {}
                    prdy_sign = str(resp.get("prdy_vrss_sign") or "").strip()
                    prdy_ctrt = float(str(resp.get("prdy_ctrt") or 0).replace(",", "") or 0)
                    stck_prpr = float(str(resp.get("stck_prpr") or 0).replace(",", "") or 0)
                    stck_hgpr = float(str(resp.get("stck_hgpr") or 0).replace(",", "") or 0)
                    acml_tr_pbmn = float(str(resp.get("acml_tr_pbmn") or 0).replace(",", "") or 0)
                    volume_power = None
                    try:
                        vp_raw = resp.get("tday_rltv") or resp.get("prdy_vrss_rltv_rate")
                        if vp_raw is not None:
                            volume_power = float(str(vp_raw).replace(",", ""))
                    except Exception:
                        pass
                    # 보조 데이터
                    frgn_3d = fetch_frgn_3day_net(client, code)
                    vola_10d = _uplimit_vola_10d.get(code)
                    passed, freason, fstr = closing_crash_filter_signal(
                        prdy_ctrt=prdy_ctrt, stck_prpr=stck_prpr, stck_hgpr=stck_hgpr,
                        prdy_sign=prdy_sign, volume_power=volume_power,
                        acml_tr_pbmn=acml_tr_pbmn, uplimit_bid_ratio=None,
                        frgn_3d_net=frgn_3d, vola_10d=vola_10d,
                        institution_net_today=None,
                    )
                    if not passed:
                        _filtered_out.append((f"{name}({code})", freason))
                        logger.info(f"{ts_prefix()} [종가매수필터] 제외 {name}({code}) {freason}")
                        continue
                    else:
                        logger.info(f"{ts_prefix()} [종가매수필터] 통과 {name}({code}) {freason}")
                except Exception as _fe:
                    logger.warning(f"{ts_prefix()} [종가매수필터] {name}({code}) 필터 오류(통과 처리): {_fe}")

            # [260423] 거래대금을 정렬 기준으로 보관 (자원배분 우선순위)
            _acml_tr_pbmn_for_sort = 0.0
            if CLOSING_CRASH_FILTER_ENABLED:
                try:
                    _acml_tr_pbmn_for_sort = float(str((resp or {}).get("acml_tr_pbmn") or 0).replace(",", "") or 0)
                except Exception:
                    _acml_tr_pbmn_for_sort = 0.0
            code_prices[code] = {
                "prev_close": prev_close,
                "limit_up": calc_limit_up_price(prev_close),
                "name": name,
                "prdy_ctrt": float(info.get("prdy_ctrt", _last_prdy_ctrt.get(code, 0.0))),
                "acml_tr_pbmn": _acml_tr_pbmn_for_sort,   # [260423] 자원배분 정렬 기준
            }

        if _filtered_out:
            _notify(
                f"{ts_prefix()} [종가매수필터] 폭락방지 제외 {len(_filtered_out)}건: "
                + ", ".join(f"{n}[{r}]" for n, r in _filtered_out[:10]),
                tele=True,
            )

        # [260423] 선정된 종목을 거래대금 큰 순으로 정렬 (자원 부족 시 큰 종목 우선 배정)
        if code_prices:
            code_prices = dict(
                sorted(code_prices.items(),
                       key=lambda kv: kv[1].get("acml_tr_pbmn", 0.0),
                       reverse=True)
            )
            _sorted_log = ", ".join(
                f"{v['name']}({k},{int(v.get('acml_tr_pbmn',0)/1e8)}억)"
                for k, v in list(code_prices.items())[:10]
            )
            logger.info(f"{ts_prefix()} [종가매수준비] 거래대금 정렬: {_sorted_log}")

        # 계좌별 주문 준비
        for acct in accounts:
            cano = acct["cano"]
            acnt = acct.get("acnt_prdt_cd", "01") or "01"
            alias = acct.get("alias", acct["account_id"])

            # 계좌별 가용 현금 조회
            try:
                acct_client = _init_account_client(acct)
                avail_cash = _get_account_available_cash(acct_client, acct)
            except Exception as e:
                logger.warning(f"{ts_prefix()} [종가매수준비] {alias} 잔고조회 실패: {e}")
                # 폴백: max_invest 사용
                max_inv = float(str(acct.get("max_invest", "0") or 0).replace("_", "") or 0)
                avail_cash = max(0, max_inv) if max_inv > 0 else 0

            _acct_max_inv = float(str(acct.get("max_invest", "0") or 0).replace("_", "") or 0)
            _notify(
                f"{ts_prefix()} [종가매수준비] {alias} 가용현금={int(avail_cash):,}원 "
                f"(max_invest={int(_acct_max_inv):,}, 적용액={int(avail_cash):,})"
            )

            if avail_cash <= 0:
                logger.info(f"{ts_prefix()} [종가매수준비] {alias} 가용현금=0 → 스킵")
                continue

            cash_per = max(0, avail_cash / n) * 0.995

            for code, cp in code_prices.items():
                limit_up = cp["limit_up"]
                qty = int(cash_per // limit_up)
                if qty <= 0:
                    logger.info(f"{ts_prefix()} [종가매수준비] {cp['name']}({code}) 제외: 상한가({limit_up:,.0f}) > 배정액({cash_per:,.0f})")
                    continue
                _closing_buy_prepared.append({
                    "code": code, "name": cp["name"], "qty": qty,
                    "limit_up": limit_up, "prev_close": cp["prev_close"],
                    "cano": cano, "acnt": acnt,
                    "account_id": acct["account_id"], "alias": alias,
                    "prdy_ctrt": cp["prdy_ctrt"],
                })

        # 클라이언트 사전 초기화 (15:20 주문 시 즉시 사용)
        _closing_acct_clients.clear()
        for acct in accounts:
            try:
                _closing_acct_clients[acct["cano"]] = _init_account_client(acct)
            except Exception as e:
                logger.warning(f"{ts_prefix()} [종가매수준비] {acct.get('alias','')} 클라이언트 생성 실패: {e}")

        _notify(
            f"{ts_prefix()} [종가매수준비] {len(_closing_buy_prepared)}건 사전 준비 완료 "
            f"({len(accounts)}계좌 × {len(code_prices)}종목, 클라이언트 {len(_closing_acct_clients)}개 캐시, 15:20:00 정시 일괄 발송 대기)",
            tele=True,
        )
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가매수준비] 전체 실패: {e}")


def _run_closing_buy_orders() -> None:
    """15:20:00 사전 준비된 주문을 즉시 일괄 발송.

    Phase 4-4: 계좌별 클라이언트를 생성하여 주문. prep에 account_id/cano 포함.
    """
    global _closing_buy_placed
    if not CLOSE_BUY or not _closing_codes:
        return
    _closing_buy_placed.clear()

    # 사전 준비 안 된 경우 즉시 준비 (폴백)
    if not _closing_buy_prepared:
        _prepare_closing_buy_orders()

    if not _closing_buy_prepared:
        _notify(f"{ts_prefix()} [종가매수] 사전 준비 데이터 없음 → 스킵")
        return

    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # 사전 캐시된 클라이언트 사용, 없으면 폴백
        acct_clients = _closing_acct_clients if _closing_acct_clients else {}
        if not acct_clients:
            # 폴백: 즉석 생성 (사전 준비 안 된 경우)
            for acct in _iter_enabled_accounts():
                try:
                    acct_clients[acct["cano"]] = _init_account_client(acct)
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [종가매수] {acct.get('alias','')} 클라이언트 생성 실패: {e}")
        fallback_client = _top_client or _init_top_client()

        tr_id = "TTTC0802U"
        ord_dvsn = "01"  # 15:20 종가 동시호가 시장가
        FEE_RATE = 0.00015

        def _send_one(prep):
            code = prep["code"]
            client = acct_clients.get(prep["cano"], fallback_client)
            ord_unpr = "0"  # 시장가
            body = {"CANO": prep["cano"], "ACNT_PRDT_CD": prep["acnt"], "PDNO": code,
                    "ORD_DVSN": ord_dvsn, "ORD_QTY": str(prep["qty"]), "ORD_UNPR": ord_unpr}
            url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-cash"
            headers = client._headers(tr_id=tr_id)
            headers["content-type"] = "application/json"
            # hashkey 생략 (KIS 공식: 선택사항)
            t0 = time.time()
            r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
            r.raise_for_status()
            j = r.json()
            elapsed = time.time() - t0
            if str(j.get("rt_cd")) != "0":
                raise RuntimeError(f"종가매수 실패: {j.get('msg1')} raw={j}")
            try:
                _ccnl_notice_sub_add(code)
            except Exception:
                pass
            return prep, j, elapsed

        # 20건씩 배치 (KIS REST 20건/초 한도)
        batches = [_closing_buy_prepared[i:i+20] for i in range(0, len(_closing_buy_prepared), 20)]
        ledger_records = []  # ledger 후처리용
        _t_total_start = time.perf_counter()  # 총 주문 처리 시간 측정 시작

        for batch_idx, batch in enumerate(batches):
            if batch_idx > 0:
                time.sleep(1.0)  # 다음 배치는 1초 후
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = {executor.submit(_send_one, p): p for p in batch}
                for fut in as_completed(futures):
                    prep = futures[fut]
                    code = prep["code"]
                    alias = prep.get("alias", prep.get("account_id", ""))
                    try:
                        prep, j, elapsed = fut.result()
                        out = j.get("output") or {}
                        ordno = str(out.get("ODNO") or out.get("odno") or "").strip()
                        qty = prep["qty"]
                        limit_up = prep["limit_up"]
                        name = prep["name"]
                        cano = prep["cano"]
                        acnt = prep["acnt"]
                        amount = limit_up * qty
                        fee_est = int(amount * FEE_RATE)
                        amount_net = amount - fee_est
                        _closing_buy_placed.append({
                            "name": name, "code": code, "qty": qty, "limit_up": limit_up, "amount_net": amount_net,
                            "ordno": ordno, "cano": cano, "acnt": acnt,
                            "account_id": prep.get("account_id", "main"), "alias": alias,
                        })
                        acct_tag = f"[{alias}]" if alias else ""
                        # 주문 완료 즉시 로그 (체결통보보다 먼저 기록되도록)
                        _notify(
                            f"{ts_prefix()} [종가매수주문]{acct_tag} 종목명={name} | 주문유형=시장가 | 수량={qty} | "
                            f"금액={amount_net:,} (상한가*수량-수수료) | {elapsed:.3f}s"
                        )
                        ledger_records.append({
                            "code": code, "name": name, "limit_up": float(limit_up),
                            "qty": qty, "ordno": ordno, "acct_tag": acct_tag,
                            "prdy_ctrt": float(prep.get("prdy_ctrt", 0.0)),
                            "prev_close": prep.get("prev_close", 0), "alias": alias,
                        })
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [종가매수실패] {code} {alias}: {e}")

        # ledger 기록 (주문 완료 후 일괄)
        for rec in ledger_records:
            _append_ledger(
                order_type="buy_order",
                code=rec["code"], name=rec["name"],
                buy_price=0,
                order_qty=rec["qty"],
                reason="종가매수_주문(15:20)",
                ord_no=rec["ordno"],
                ord_dvsn="01(시장가)",
                prdy_ctrt=rec["prdy_ctrt"],
                note=f"상한가={rec['limit_up']} 전일종가={rec['prev_close']} 계좌={rec['alias']}",
                cano_alias=rec["alias"],
                stck_prpr=_last_stck_prpr.get(rec["code"], 0.0),
            )
        if _closing_buy_placed:
            _t_total_elapsed = time.perf_counter() - _t_total_start
            _notify(f"{ts_prefix()} [종가매수주문완료] {len(_closing_buy_placed)}건 | 총 소요시간 {_t_total_elapsed:.3f}s")
        # 주문 결과 state 저장
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
    08:30~08:40 시간외 종가 추가 매수.

    소스: 오늘 아침 새로 리스트업된 _morning_target_codes (당일 전일 상한가 종목 리스트).
          (※ 어제 15:20 closing_buy_state 는 사용하지 않음 — 사용자 지시.)
    fallback: _morning_target_codes 비면 모듈 전역 codes 사용. 둘 다 비면 _notify 후 return.

    수량 산출 (15:20 종가매수 _prepare_closing_buy_orders 패턴 차용, 단일계좌):
      - main cfg 의 cano/acnt
      - 가용현금 = _get_account_available_cash(client, cfg). 실패 시 max_invest 폴백
      - n = len(target_codes), cash_per = (avail_cash / n) * 0.995
      - 종목별 prev_close → limit_up = calc_limit_up_price(prev_close)
      - qty_target = int(cash_per // limit_up)
      - held = live_balance.get(code,{}).get("qty",0) 이미 보유분 차감
      - remain = max(0, qty_target - held). remain<=0 이면 skip
    추가 보호:
      - _str1_sell_state sold=True 종목 스킵
    추적: 주문 성공 시 _morning_extra_placed 에 append (08:41 print 함수가 사용).
    """
    global _morning_extra_closing_done, _morning_extra_placed
    if not MORNING_EXTRA_CLOSING_PR_BUY or _morning_extra_closing_done:
        return
    try:
        # ── 1) 대상 종목 산출: _morning_target_codes 우선, 폴백 codes ─────────
        target_codes: list[str] = []
        if _morning_target_codes:
            target_codes = [str(c).zfill(6) for c in _morning_target_codes]
            src_label = "morning_target_codes"
        elif codes:
            target_codes = [str(c).zfill(6) for c in codes]
            src_label = "module.codes(fallback)"
        else:
            _morning_extra_closing_done = True
            _notify(f"{ts_prefix()} [08:30시간외종가] 대상 종목 없음 → 스킵", tele=True)
            return

        logger.info(
            f"{ts_prefix()} [08:30시간외종가] 진입 source={src_label} target={len(target_codes)}건"
        )

        # ── 2) 잔고/매도완료 종목 ───────────────────────────────────────────
        try:
            live_balance = _get_balance_holdings()
        except Exception:
            live_balance = {}

        with _str1_sell_state_lock:
            sold_codes = {c for c, st in _str1_sell_state.items() if st.get("sold")}

        # ── 3) 단일계좌 cfg + 가용현금 ────────────────────────────────────────
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        if not cano:
            _morning_extra_closing_done = True
            logger.warning(f"{ts_prefix()} [08:30시간외종가] config cano 없음 → 스킵")
            return
        client = _top_client or _init_top_client()

        # 가용현금 — 단일계좌이므로 main cfg 자체를 acct dict 로 활용
        try:
            avail_cash = _get_account_available_cash(client, cfg)
        except Exception as _ce:
            logger.warning(f"{ts_prefix()} [08:30시간외종가] 가용현금 조회 실패: {_ce}")
            max_inv = float(str(cfg.get("max_invest", "0") or 0).replace("_", "") or 0)
            avail_cash = max(0.0, max_inv)

        if avail_cash <= 0:
            _morning_extra_closing_done = True
            _notify(f"{ts_prefix()} [08:30시간외종가] 가용현금=0 → 스킵", tele=True)
            return

        n = len(target_codes)
        cash_per = (avail_cash / n) * 0.995
        _notify(
            f"{ts_prefix()} [08:30시간외종가] 가용현금={int(avail_cash):,}원, "
            f"종목당 배분={int(cash_per):,}원 (n={n})"
        )

        # ── 4) 종목별 전일종가 사전 조회 (병렬) ──────────────────────────────
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _resolve_prev_close(code: str) -> float:
            pc = 0.0
            for _attempt in range(3):
                pc = _get_close_from_kis_api(client, code, use_today=False)
                if pc > 0:
                    return pc
                time.sleep(0.3)
            return _get_prev_close_from_1d([code]).get(code, 0) or 0.0

        pc_map: dict[str, float] = {}
        with ThreadPoolExecutor(max_workers=min(20, max(1, n))) as ex:
            fmap = {ex.submit(_resolve_prev_close, c): c for c in target_codes}
            for fut in as_completed(fmap):
                c = fmap[fut]
                try:
                    pc_map[c] = fut.result() or 0.0
                except Exception:
                    pc_map[c] = 0.0

        # ── 5) 종목별 수량 산출 + 스킵 판정 ───────────────────────────────────
        to_order: list[dict] = []
        skip_lines: list[str] = []
        for code in target_codes:
            name = code_name_map.get(code, code)

            # 매도완료 스킵
            if code in sold_codes:
                skip_lines.append(f"  {name}({code}) → 매도완료(sold=True), 매수 스킵")
                continue

            prev_close = pc_map.get(code, 0.0)
            if prev_close <= 0:
                skip_lines.append(f"  {name}({code}) → 전일종가 없음, 스킵")
                continue

            limit_up = calc_limit_up_price(prev_close)
            if limit_up <= 0:
                skip_lines.append(f"  {name}({code}) → 상한가 계산 실패, 스킵")
                continue

            qty_target = int(cash_per // limit_up)
            if qty_target <= 0:
                skip_lines.append(
                    f"  {name}({code}) → 상한가({limit_up:,.0f}) > 배분액({cash_per:,.0f}), 수량=0 스킵"
                )
                continue

            lb_val = live_balance.get(code, {}) if live_balance else {}
            held = lb_val.get("qty", 0) if isinstance(lb_val, dict) else (lb_val or 0)
            remain = max(0, qty_target - int(held or 0))
            if remain <= 0:
                skip_lines.append(
                    f"  {name}({code}) → 잔고보유={held} >= 목표={qty_target}, 추가매수 0 스킵"
                )
                continue

            to_order.append({
                "code": code,
                "name": name,
                "remain": remain,
                "qty_target": qty_target,
                "held": int(held or 0),
                "prev_close": prev_close,
                "limit_up": limit_up,
            })

        if skip_lines:
            _notify(f"{ts_prefix()} [08:30시간외종가] 제외 {len(skip_lines)}건:\n" + "\n".join(skip_lines))

        if not to_order:
            _morning_extra_closing_done = True
            _notify(
                f"{ts_prefix()} [08:30시간외종가] 완료 placed=0건 skip={len(skip_lines)}건 (대상 없음)",
                tele=True,
            )
            return

        _morning_extra_closing_done = True  # 1회만 실행 (이중주문 방지)
        _notify(f"{ts_prefix()} [08:30시간외종가] 추가 매수 시작 {len(to_order)}건")

        # ── 6) 주문 발사 (20건/sec 배치 병렬) ─────────────────────────────────
        def _send_one_830(o: dict):
            code = o["code"]
            remain = int(o["remain"])
            held = int(o["held"])
            name = o["name"]
            prev_close = float(o["prev_close"])
            if remain <= 0 or prev_close <= 0:
                return code, name, remain, held, prev_close, False, "전일종가없음" if prev_close <= 0 else "수량0"
            try:
                _buy_order_cash(client, cano, acnt, "TTTC0802U", code, remain, prev_close, ord_dvsn="05")
                try:
                    _ccnl_notice_sub_add(code)
                except Exception:
                    pass
                return code, name, remain, held, prev_close, True, ""
            except Exception as e:
                return code, name, remain, held, prev_close, False, str(e)

        batches = [to_order[i:i+20] for i in range(0, len(to_order), 20)]
        for bi, batch in enumerate(batches):
            if bi > 0:
                time.sleep(1.0)
            with ThreadPoolExecutor(max_workers=len(batch)) as ex:
                futs = [ex.submit(_send_one_830, o) for o in batch]
                for fut in as_completed(futs):
                    try:
                        code, name, remain, held, prev_close, ok, err = fut.result()
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [08:30시간외종가] future 실패: {e}")
                        continue
                    if ok:
                        held_txt = f" (잔고보유={held})" if held >= 0 else ""
                        _notify(
                            f"{ts_prefix()} [08:30시간외종가_매수] {name}({code})"
                            f"  수량={remain}{held_txt}  전일종가={prev_close:,.0f}  ORD_DVSN=05(장전시간외종가)",
                            tele=True,
                        )
                        _morning_extra_placed.append({
                            "code": code,
                            "name": name,
                            "qty": remain,
                            "prev_close": prev_close,
                            "ts": time.time(),
                        })
                    else:
                        logger.warning(f"{ts_prefix()} [08:30시간외종가실패] {code}: {err}")
                        _notify(f"{ts_prefix()} [08:30시간외종가실패] {name}({code}) {err}", tele=True)

        _notify(
            f"{ts_prefix()} [08:30시간외종가] 완료 placed={len(_morning_extra_placed)}건 "
            f"skip={len(skip_lines)}건",
            tele=True,
        )
    except Exception as e:
        logger.warning(f"{ts_prefix()} [08:30시간외종가] 실패: {e}")


def _log_morning_extra_result() -> None:
    """08:41 — _run_morning_extra_closing_buy 가 당일 발사한 주문에 한해 결과 표 출력.

    소스: _morning_extra_placed (당일 주문 성공 항목).
    체결수량: min(주문수량, _closing_filled_by_code.get(code, 0)).
    """
    if not _morning_extra_placed:
        _notify(f"{ts_prefix()} [08:30~08:40 종료] 추가 매수 주문 없음", tele=True)
        return

    table_rows: list[dict] = []
    total_ord, total_filled, total_remain = 0, 0, 0
    for o in _morning_extra_placed:
        code = str(o.get("code", "")).zfill(6)
        name = o.get("name", code_name_map.get(code, code))
        ord_q = int(o.get("qty", 0))
        filled = min(ord_q, max(0, int(_closing_filled_by_code.get(code, 0))))
        remain = max(0, ord_q - filled)
        total_ord += ord_q
        total_filled += filled
        total_remain += remain
        table_rows.append({
            "종목명": name,
            "코드": code,
            "주문": str(ord_q),
            "체결": str(filled),
            "미체결": str(remain),
        })

    title = f"{ts_prefix()} [08:30~08:40 종료] 추가 매수 결과 (당일 주문 {len(table_rows)}건)"
    print(f"\n{title}")
    print_table(
        table_rows,
        columns=["종목명", "코드", "주문", "체결", "미체결"],
        align={"종목명": "left", "코드": "left", "주문": "right", "체결": "right", "미체결": "right"},
    )
    print(f"  합계: 주문={total_ord} 체결={total_filled} 미체결={total_remain}\n")
    logger.info(f"{title} | 합계: 주문={total_ord} 체결={total_filled} 미체결={total_remain}")

    # 텔레그램 (stdout 이중 출력 방지: tmsg 직접 호출)
    tele_lines = [f"[08:30~08:40 종료] 추가 매수 결과 (당일 주문 {len(table_rows)}건)"]
    for r in table_rows:
        tele_lines.append(
            f"  {r['종목명']}({r['코드']}) 주문={r['주문']} 체결={r['체결']} 미체결={r['미체결']}"
        )
    tele_lines.append(f"  합계: 주문={total_ord} 체결={total_filled} 미체결={total_remain}")
    if tmsg is not None:
        try:
            tmsg(f"{ts_prefix()}\n" + "\n".join(tele_lines), "-t")
        except Exception:
            pass


_morning_extra_closing_done = False
_afternoon_extra_closing_done = False
_morning_extra_logged_done = False
_afternoon_extra_logged_done = False
_sell_state_supplement_done = False
# [260602] 08:45 진단로그 확인 리마인더 (decode skip 근본원인 진단 — 1일 1회)
_diag_reminder_done = False
# 08:30~08:40 시간외 종가 추가매수 — 당일 실제 발사한 주문 기록 (08:41 결과 표 소스)
_morning_extra_placed: list[dict] = []

# ── 잔고조회 스케줄러 (Phase 2-2) ──
_closing_balance_1531_done = False
_closing_balance_1558_done = False
_overtime_balance_checked: set[str] = set()
# 장중 매수/매도 주문 후 체결 대기 중 1분마다 잔고조회
_pending_order_balance_last_ts: float = 0.0


def _run_closing_balance_verification(label: str) -> None:
    """REST API 잔고조회로 체결 결과 검증 + _closing_filled_by_code 갱신."""
    state = _load_closing_buy_state()
    if not state or not state.get("orders"):
        return
    try:
        for acct in _iter_enabled_accounts(trade_only=True):
            cano = str(acct.get("cano", "")).strip()
            acnt = str(acct.get("acnt_prdt_cd", "01")).strip() or "01"
            if not cano:
                continue
            client = _top_client or _init_top_client()
            out1, _, _, _, _ = _get_balance_page(client, cano, acnt, "TTTC8434R")
            rows = out1 if isinstance(out1, list) else ([out1] if isinstance(out1, dict) else [])
            hold_map = {}
            for row in rows:
                c = str(row.get("pdno", "") or "").strip().zfill(6)
                if c:
                    hold_map[c] = int(float(str(row.get("hldg_qty", 0) or 0).replace(",", "") or 0))

            changes = []
            for o in state["orders"]:
                code = str(o.get("code", "")).zfill(6)
                order_qty = int(o.get("order_qty", o.get("qty", 0)))
                old_filled = _closing_filled_by_code.get(code, 0)
                held = hold_map.get(code, 0)
                new_filled = min(order_qty, held)
                if new_filled > old_filled:
                    _closing_filled_by_code[code] = new_filled
                    name = o.get("name", code_name_map.get(code, code))
                    changes.append(f"{name}({code}) {old_filled}→{new_filled}주")
            _save_closing_filled()

            if changes:
                msg = f"{ts_prefix()} [{label}] 잔고검증 체결 반영({cano[-4:]}): {', '.join(changes)}"
                _notify(msg, tele=True)
            else:
                logger.info(f"{ts_prefix()} [{label}] 잔고검증 완료({cano[-4:]}) — 변동 없음")

            # ── 잔고에 있지만 미구독/미등록 종목 자동 감지 + 구독 추가 ──
            _new_codes_for_sub = []
            for row in rows:
                c = str(row.get("pdno", "") or "").strip().zfill(6)
                hq = int(float(str(row.get("hldg_qty", 0) or 0).replace(",", "") or 0))
                if not c or hq <= 0:
                    continue
                if c not in codes:
                    bp = float(str(row.get("pchs_avg_pric", 0) or 0).replace(",", "") or 0)
                    nm = str(row.get("prdt_name", "") or code_name_map.get(c, c)).strip()
                    _new_codes_for_sub.append(c)
                    with _str1_sell_state_lock:
                        if c not in _str1_sell_state:
                            _str1_sell_state[c] = {
                                "sold": False, "sell_reason": "", "qty": hq,
                                "sell_price": 0.0, "sell_time": "", "ordno": "",
                                "pnl": 0.0, "ret_pct": 0.0,
                                "buy_price": bp, "name": nm,
                                "opening_call_auction_ordered": False, "balance_qty": hq,
                                "source": f"잔고검증_{label}",
                            }
                    logger.info(f"{ts_prefix()} [{label}] 미구독 보유종목 감지: {nm}({c}) qty={hq} bp={bp:,.0f}")
            if _new_codes_for_sub:
                _ensure_code_structs(_new_codes_for_sub)
                with _lock:
                    _base_codes.update(_new_codes_for_sub)
                _trigger_ws_rebuild()
                _notify(f"{ts_prefix()} [{label}] 미구독 보유종목 {len(_new_codes_for_sub)}건 구독 추가: "
                        f"{', '.join(_new_codes_for_sub)}", tele=True)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [{label}] 잔고검증 실패: {e}")


def _get_closing_filled_for_remain(state: dict) -> dict[str, int]:
    """
    체결 수량 병합: in-memory + persisted. 미매수(remain) 계산용.
    순수 읽기 함수 — 잔고조회는 _run_closing_balance_verification 스케줄러에서 수행.
    """
    persisted = _load_closing_filled()
    merged = dict(persisted)
    for k, v in _closing_filled_by_code.items():
        c = str(k).zfill(6)
        merged[c] = max(merged.get(c, 0), v)
    # 즉시 잔고조회 제거 — 이 함수는 _closing_filled_by_code를 읽기만 하는 순수 함수.
    # 잔고조회는 별도 스케줄러(_run_closing_balance_verification)에서 지정 시간에만 실행.
    return merged


def _run_afternoon_extra_closing_buy() -> None:
    """15:40~16:00 시간외 종가: 당일 15:30 미매수 종목에 ORD_DVSN=06(장후시간외종가) 매수. 가격 미입력(당일종가 자동매칭)."""
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
        from concurrent.futures import ThreadPoolExecutor, as_completed
        client = _top_client or _init_top_client()
        tr_id = "TTTC0802U"
        code_info = state.get("code_info") or {}

        # 1) 당일종가 병렬 조회 (REST, 20건/초 한도)
        def _fetch_close(code):
            try:
                return code, _get_close_from_kis_api(client, code, use_today=True)
            except Exception as e:
                logger.warning(f"{ts_prefix()} [15:40당일종가조회실패] {code}: {e}")
                return code, 0

        codes_to_fetch = [o["code"] for o in to_order]
        close_map: dict[str, float] = {}
        with ThreadPoolExecutor(max_workers=min(20, len(codes_to_fetch) or 1)) as executor:
            for fut in as_completed([executor.submit(_fetch_close, c) for c in codes_to_fetch]):
                code, close_price = fut.result()
                close_map[code] = close_price

        # 2) 계좌별 주문 병렬 발송 (20건 배치, KIS REST 20건/초 한도)
        for acct in _iter_enabled_accounts(trade_only=True):
            cano = str(acct.get("cano", "")).strip()
            acnt = str(acct.get("acnt_prdt_cd", "01")).strip() or "01"
            if not cano:
                continue
            alias = acct.get("alias", acct.get("account_id", ""))

            order_items = []
            for o in to_order:
                code = o["code"]
                remain = int(o.get("remain_qty", 0))
                if remain <= 0:
                    continue
                today_close = close_map.get(code, 0)
                if today_close <= 0:
                    continue
                info = code_info.get(code, {})
                name = o.get("name", info.get("name", code))
                order_items.append({
                    "code": code, "name": name, "remain": remain, "today_close": today_close,
                })

            def _send_one(item):
                t0 = time.time()
                try:
                    _buy_order_cash(client, cano, acnt, tr_id,
                                    item["code"], item["remain"], item["today_close"], ord_dvsn="06")
                    return item, None, time.time() - t0
                except Exception as e:
                    return item, e, time.time() - t0

            batches = [order_items[i:i+20] for i in range(0, len(order_items), 20)]
            ok_list, fail_list = [], []
            for batch_idx, batch in enumerate(batches):
                if batch_idx > 0:
                    time.sleep(1.0)  # KIS REST 20건/초 한도 준수
                with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                    for fut in as_completed([executor.submit(_send_one, it) for it in batch]):
                        item, err, elapsed = fut.result()
                        if err is None:
                            ok_list.append((item, elapsed))
                            _notify(
                                f"{ts_prefix()} [15:40시간외종가_매수 주문] {item['name']}({item['code']}) "
                                f"수량={item['remain']} 당일종가={item['today_close']:,.0f} "
                                f"(ORD_DVSN=06 장후시간외종가) 계좌={cano[-4:]} {elapsed:.3f}s"
                            )
                        else:
                            fail_list.append((item, err))
                            logger.warning(f"{ts_prefix()} [15:40시간외종가실패] {item['code']}: {err}")
            _notify(
                f"{ts_prefix()} [15:40시간외종가주문완료] {alias} {len(ok_list)}건 성공 / {len(fail_list)}건 실패",
                tele=True,
            )
    except Exception as e:
        logger.warning(f"{ts_prefix()} [15:40시간외종가] 실패: {e}")


_overtime_order_done: bool = False  # 16:00 1회 주문 완료 플래그


def _run_overtime_buy_orders() -> None:
    """
    16:00~18:00 시간외 단일가.
    - 16:00:00: 15:58 재선정 종목(_today_closing_target_codes)에 ORD_DVSN=07 주문 1회 발송
    - 15:20 closing_buy_state는 시간외에서 **무시** (top30 기반이라 상한가 가능성 예측, 15:58은 장 마감 후 실제 데이터라 더 정확)
    - 16:10~17:50: 체결 확인만 수행 (추가 주문 없음)
    """
    global _overtime_order_done
    if not CLOSE_BUY:
        return

    now_t = datetime.now(KST).time()
    target_codes = list(_today_closing_target_codes)

    # 16:00:00 — 1회만 주문 발송
    if not _overtime_order_done and now_t < dtime(16, 1):
        _overtime_order_done = True
        if not target_codes:
            _notify(f"{ts_prefix()} [시간외단일가] 16:00 15:58 재선정 종목 없음 → 주문 스킵")
            return
        try:
            client = _top_client or _init_top_client()
            tr_id = "TTTC0802U"

            # 1) 각 계좌별 가용현금 조회 + 종목별 자원배분 (균등분배)
            for acct in _iter_enabled_accounts(trade_only=True):
                cano = str(acct.get("cano", "")).strip()
                acnt = str(acct.get("acnt_prdt_cd", "01")).strip() or "01"
                if not cano:
                    continue
                try:
                    acct_client = _init_account_client(acct)
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [시간외단일가] 계좌 클라이언트 실패 {cano}: {e}")
                    continue
                try:
                    avail_cash = _get_account_available_cash(acct_client, acct)
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [시간외단일가] 가용현금 조회 실패 {cano}: {e}")
                    continue

                # 종목당 배정액 (균등분배)
                n = len(target_codes)
                cash_per = max(0, avail_cash / max(n, 1)) * 0.995   # 수수료 버퍼
                order_items = []
                for code in target_codes:
                    try:
                        prev_close = _get_close_from_kis_api(acct_client, code, use_today=False)
                        if prev_close <= 0:
                            continue
                        limit_up = calc_limit_up_price(prev_close)
                        qty = int(cash_per // max(limit_up, 1))
                        if qty <= 0:
                            continue
                        name = code_name_map.get(code, code)
                        order_items.append({
                            "code": code, "name": name, "qty": qty,
                            "limit_up": limit_up, "prev_close": prev_close,
                        })
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [시간외단일가_준비실패] {code}: {e}")

                alias = acct.get("alias", acct.get("account_id", ""))
                _notify(
                    f"{ts_prefix()} [시간외단일가준비] {alias} 가용현금={int(avail_cash):,}원 / "
                    f"{n}종목 → 종목당≈{int(cash_per):,}원 / 주문가능={len(order_items)}건"
                )

                # 2) 20건 배치 병렬 발송 (KIS REST 20건/초 한도)
                from concurrent.futures import ThreadPoolExecutor, as_completed
                def _send_one(item):
                    t0 = time.time()
                    try:
                        _buy_order_cash(acct_client, cano, acnt, tr_id,
                                        item["code"], item["qty"], item["limit_up"], ord_dvsn="07")
                        # 거래장부 기록
                        try:
                            _append_ledger(
                                order_type="buy_order",
                                code=item["code"], name=item["name"],
                                buy_price=0,
                                order_qty=item["qty"],
                                reason="시간외단일가_주문(16:00, 15:58재선정)",
                                ord_dvsn="07(시간외단일가)",
                                prdy_ctrt=0.0,
                                note=f"상한가={item['limit_up']} 전일종가={item['prev_close']}",
                                cano_alias=alias,
                                stck_prpr=_last_stck_prpr.get(item["code"], 0.0),
                            )
                        except Exception:
                            pass
                        return item, None, time.time() - t0
                    except Exception as e:
                        return item, e, time.time() - t0

                batches = [order_items[i:i+20] for i in range(0, len(order_items), 20)]
                ok_list, fail_list = [], []
                for batch_idx, batch in enumerate(batches):
                    if batch_idx > 0:
                        time.sleep(1.0)
                    with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                        for fut in as_completed([executor.submit(_send_one, it) for it in batch]):
                            item, err, elapsed = fut.result()
                            if err is None:
                                ok_list.append(item)
                            else:
                                fail_list.append((item, str(err)))
                                logger.warning(f"{ts_prefix()} [시간외단일가실패] {item['code']}: {err}")

                # 3) 통합 결과 출력
                lines = [f"{ts_prefix()} [시간외단일가주문] {alias} {len(ok_list)}건 발송완료 ({len(order_items)}건 중)"]
                for it in ok_list:
                    lines.append(f"  {it['name']}({it['code']}) 수량={it['qty']} 상한가={it['limit_up']:,.0f} (ORD_DVSN=07)")
                for it, err in fail_list:
                    lines.append(f"  [실패] {it['name']}({it['code']}) {it['qty']}주: {err}")
                _notify("\n".join(lines), tele=True)
        except Exception as e:
            _notify(f"{ts_prefix()} [시간외단일가] 전체 실패: {e}")
            logger.warning(f"{ts_prefix()} [시간외단일가] 실패: {e}")


def _log_closing_result_after_window(window_label: str, yymmdd: str | None = None) -> None:
    """시간 종료 후 주문내역·결과를 로그로 출력. filled는 체결통보+persisted 병합 사용."""
    target_date = yymmdd or today_yymmdd
    state = _load_closing_buy_state_by_date(target_date) if target_date != today_yymmdd else _load_closing_buy_state()
    if not state or not state.get("orders"):
        return
    orders = state.get("orders") or []
    code_info = state.get("code_info") or {}
    filled_map = _get_closing_filled_for_remain(state) if target_date == today_yymmdd else _load_closing_filled(target_date)
    title = f"{ts_prefix()} [{window_label} 종료] 주문내역·결과"
    table_rows: list[dict] = []
    total_ord, total_filled, total_remain = 0, 0, 0
    for o in orders:
        code = str(o.get("code", "")).zfill(6)
        name = o.get("name", code_info.get(code, {}).get("name", code))
        ord_q = int(o.get("order_qty", o.get("qty", 0)))
        filled = min(ord_q, max(int(o.get("filled_qty", 0)), filled_map.get(code, 0))) if filled_map else int(o.get("filled_qty", 0))
        remain = max(0, ord_q - filled)
        total_ord += ord_q
        total_filled += filled
        total_remain += remain
        table_rows.append({
            "종목명": name,
            "코드": code,
            "주문": str(ord_q),
            "매수": str(filled),
            "미매수": str(remain),
            "status": o.get("status", "unknown"),
        })
    print(f"\n{title}")
    if table_rows:
        print_table(
            table_rows,
            columns=["종목명", "코드", "주문", "매수", "미매수", "status"],
            align={"종목명": "left", "코드": "left", "주문": "right", "매수": "right", "미매수": "right", "status": "left"},
        )
    print(f"  합계: 주문={total_ord} 매수={total_filled} 미매수={total_remain}")
    print(f"  (체결통보 기반 _closing_filled_by_code 반영)\n")
    # 텔레그램
    tele_lines = [f"[{window_label} 종료] 주문내역·결과"]
    for r in table_rows:
        tele_lines.append(f"  {r['종목명']}({r['코드']}) 주문={r['주문']} 매수={r['매수']} 미매수={r['미매수']}")
    tele_lines.append(f"  합계: 주문={total_ord} 매수={total_filled} 미매수={total_remain}")
    if tmsg is not None:
        try:
            tmsg("\n".join(tele_lines), "-t")
        except Exception:
            pass
    logger.info(f"{title} | 합계: 주문={total_ord} 매수={total_filled} 미매수={total_remain}")


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
        table_rows: list[dict] = []
        for o in _closing_buy_placed:
            c = str(o.get("code", "")).zfill(6)
            ord_q = int(o.get("qty", 0))
            filled = min(ord_q, _closing_filled_by_code.get(c, 0))
            remain = max(0, ord_q - filled)
            total_ord += ord_q
            total_filled += filled
            total_remain += remain
            table_rows.append({
                "종목명": o.get("name", c),
                "코드": c,
                "주문": str(ord_q),
                "체결": str(filled),
                "미체결": str(remain),
            })
        print(f"\n{ts_prefix()} [15:30:40 종가매수 결과]")
        if table_rows:
            print_table(
                table_rows,
                columns=["종목명", "코드", "주문", "체결", "미체결"],
                align={"종목명": "left", "코드": "left", "주문": "right", "체결": "right", "미체결": "right"},
            )
        print(f"  합계: 주문={total_ord} 체결={total_filled} 미체결={total_remain}\n")
        # 텔레그램 전송 (stdout 이중 출력 방지: tmsg 직접 호출)
        tele_lines = [f"[15:30:40 종가매수 결과]"]
        for r in table_rows:
            tele_lines.append(f"  {r['종목명']}({r['코드']}) 주문={r['주문']} 체결={r['체결']} 미체결={r['미체결']}")
        tele_lines.append(f"  합계: 주문={total_ord} 체결={total_filled} 미체결={total_remain}")
        if tmsg is not None:
            try:
                tmsg("\n".join(tele_lines), "-t")
            except Exception:
                pass
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


def _load_prev_closing_codes() -> list[str]:
    """연도별 거래장부 parquet에서 직전 종가매수 종목코드 로드.
    reason에 '종가매수'를 포함하는 행 중 가장 마지막 날짜의 code를 추출.
    없으면 빈 리스트 → CODE_TEXT 폴백.
    """
    if not AUTO_CODE_FROM_PREV_CLOSING:
        return []
    try:
        now = datetime.now(KST)
        for year in [now.strftime("%Y"), str(int(now.strftime("%Y")) - 1)]:
            path = LEDGER_DIR / f"trade_ledger_{year}.parquet"
            if not path.exists():
                continue
            df = pd.read_parquet(path)
            closing = df[df["reason"].astype(str).str.contains("종가매수", na=False)]
            if closing.empty:
                continue
            last_date = closing["date"].max()
            last_day = closing[closing["date"] == last_date]
            codes = last_day["code"].astype(str).str.zfill(6).unique().tolist()
            if codes:
                logger.info(
                    f"[codes] 거래장부({last_date}) 종가매수 {len(codes)}개 로드: {codes}"
                )
                return codes
    except Exception as e:
        logger.warning(f"[codes] 거래장부 parquet 로드 실패: {e}")
    return []


def _load_morning_target_codes() -> list[str]:
    """당일 아침 전일 상한가 종목을 정확하게 선정.
    1) Select_Tr_target_list_symulation_pdy_ctrt.py 실행 → CSV 갱신
    2) Select_Tr_target_list.csv에서 마지막 날짜 + group=ST 종목 추출
    3) 실패 시 _load_prev_closing_codes() fallback
    """
    import subprocess as _sp

    csv_path = Path("/home/ubuntu/Stoc_Kis/symulation/Select_Tr_target_list.csv")
    script_path = Path("/home/ubuntu/Stoc_Kis/symulation/Select_Tr_target_list_symulation_pdy_ctrt.py")

    # ── 1) CSV 갱신: 스크립트 실행 ──
    try:
        logger.info(f"{ts_prefix()} [morning_target] Select_Tr_target_list 스크립트 실행 중...")
        result = _sp.run(
            [sys.executable, str(script_path)],
            capture_output=True, text=True, timeout=120,
            cwd=str(script_path.parent),
        )
        if result.returncode != 0:
            logger.warning(f"[morning_target] 스크립트 실행 실패 (rc={result.returncode}): {result.stderr[:300]}")
        else:
            logger.info(f"{ts_prefix()} [morning_target] 스크립트 실행 완료")
    except Exception as e:
        logger.warning(f"[morning_target] 스크립트 실행 예외: {e}")

    # ── 2) CSV 읽기 → 마지막 날짜 + group=ST 필터 ──
    try:
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV 없음: {csv_path}")
        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
        if df.empty or "date" not in df.columns or "symbol" not in df.columns:
            raise ValueError("CSV가 비어 있거나 필수 컬럼 누락")

        last_date = df["date"].max()
        last_day = df[df["date"] == last_date]
        raw_codes = last_day["symbol"].str.strip().str.zfill(6).unique().tolist()

        # group=ST 필터링 (모듈 레벨 초기화 전이므로 직접 KRX_code.csv 로드)
        krx_path = Path("/home/ubuntu/Stoc_Kis/data/admin/symbol_master/KRX_code.csv")
        st_set = set()
        if krx_path.exists():
            krx_df = pd.read_csv(krx_path, dtype=str, usecols=["code", "group"])
            krx_df["code"] = krx_df["code"].str.strip().str.zfill(6)
            krx_df["group"] = krx_df["group"].str.strip().str.upper()
            st_set = set(krx_df.loc[krx_df["group"] == "ST", "code"])
        if st_set:
            st_codes = [c for c in raw_codes if c in st_set]
        else:
            st_codes = raw_codes  # KRX 데이터 없으면 전체 통과
        # 종목명 매핑 (code_name_map이 아직 없을 수 있으므로 CSV name 사용)
        name_map = dict(zip(last_day["symbol"].str.strip().str.zfill(6), last_day["name"].str.strip())) if "name" in last_day.columns else {}
        names = [name_map.get(c, c) for c in st_codes]
        logger.info(
            f"{ts_prefix()} [morning_target] CSV({last_date}) {len(raw_codes)}종목 중 ST={len(st_codes)}종목: {names}"
        )
        if st_codes:
            return st_codes
    except Exception as e:
        logger.warning(f"[morning_target] CSV 읽기 실패: {e}")

    # ── 3) fallback: 기존 거래장부 기반 ──
    logger.info("[morning_target] fallback → _load_prev_closing_codes()")
    return _load_prev_closing_codes()


_loaded_top_added: list[str] = []  # config에서 복원한 top_added (초기화 전 임시 저장)

# 15:35 당일 상한가 종목 재선정 결과 (시간외 매매용)
_today_closing_target_codes: list[str] = []    # 15:35 재선정 결과 code 리스트
_today_closing_target_done: bool = False       # 15:35 재선정 1회 실행 플래그


def _run_today_target_reselect() -> list[str]:
    """15:30 장 종료 후 15:35경 실행: 당일 상한가 종목 재선정.

    1) Select_Tr_target_list_symulation_pdy_ctrt.py 실행 → CSV 갱신 (1m 데이터 기반)
    2) Select_Tr_target_list.csv에서 **당일 날짜** + group=ST 종목 추출
    3) 시간외 종가매매/단일가 매매 대상으로 사용 (_today_closing_target_codes)
    """
    import subprocess as _sp
    global _today_closing_target_codes, _today_closing_target_done

    if _today_closing_target_done:
        return _today_closing_target_codes
    _today_closing_target_done = True

    csv_path = Path("/home/ubuntu/Stoc_Kis/symulation/Select_Tr_target_list.csv")
    script_path = Path("/home/ubuntu/Stoc_Kis/symulation/Select_Tr_target_list_symulation_pdy_ctrt.py")
    today_ymd = datetime.now(KST).strftime("%Y-%m-%d")

    # 1) 스크립트 실행
    try:
        logger.info(f"{ts_prefix()} [today_target] 15:35 당일 상한가 재선정 스크립트 실행 중...")
        result = _sp.run(
            [sys.executable, str(script_path)],
            capture_output=True, text=True, timeout=180,
            cwd=str(script_path.parent),
        )
        if result.returncode != 0:
            logger.warning(f"[today_target] 스크립트 실행 실패 (rc={result.returncode}): {result.stderr[:300]}")
        else:
            logger.info(f"{ts_prefix()} [today_target] 스크립트 실행 완료")
    except Exception as e:
        logger.warning(f"[today_target] 스크립트 실행 예외: {e}")

    # 2) CSV 읽기 → 당일 날짜 + group=ST 필터
    codes_result: list[str] = []
    try:
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV 없음: {csv_path}")
        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
        if df.empty or "date" not in df.columns or "symbol" not in df.columns:
            raise ValueError("CSV가 비어 있거나 필수 컬럼 누락")
        today_df = df[df["date"] == today_ymd]
        if today_df.empty:
            logger.warning(f"[today_target] CSV에 {today_ymd} 데이터 없음 (last_date={df['date'].max()})")
            # fallback: 최신 날짜 사용
            today_df = df[df["date"] == df["date"].max()]
        raw_codes = today_df["symbol"].str.strip().str.zfill(6).unique().tolist()

        # group=ST 필터
        st_codes = [c for c in raw_codes if _is_stock_group(c)]
        name_map = dict(zip(today_df["symbol"].str.strip().str.zfill(6), today_df["name"].str.strip())) if "name" in today_df.columns else {}
        names = [name_map.get(c, c) for c in st_codes]
        codes_result = st_codes
        msg = (
            f"{ts_prefix()} [today_target] 15:35 재선정 완료: "
            f"{today_ymd} {len(raw_codes)}종목 중 ST={len(st_codes)}개 {names}"
        )
        logger.info(msg)
        _notify(msg, tele=True)
    except Exception as e:
        logger.warning(f"[today_target] CSV 읽기 실패: {e}")

    _today_closing_target_codes = codes_result
    return codes_result

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


def _print_morning_target_table(codes_in: list[str]) -> None:
    """장 시작 전 전일 상한가 모니터링 대상 종목을 표로 출력 + 텔레그램 전송.

    소스: symulation/Select_Tr_target_list.csv 의 last_date 행에서 codes_in 필터링.
    컬럼: 종목명 / 코드 / 어제종가 / 등락율(%) (tdy_ctrt 가 0.xxx 형태이므로 ×100).
    """
    csv_path = Path("/home/ubuntu/Stoc_Kis/symulation/Select_Tr_target_list.csv")
    if not csv_path.exists():
        logger.warning(f"{ts_prefix()} [morning_target_table] CSV 없음: {csv_path}")
        return
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    if df.empty or "date" not in df.columns or "symbol" not in df.columns:
        logger.warning(f"{ts_prefix()} [morning_target_table] CSV 비어있음/필수컬럼 누락")
        return
    last_date = df["date"].max()
    csv_df = df[df["date"] == last_date].copy()
    csv_df["symbol"] = csv_df["symbol"].str.strip().str.zfill(6)
    codes_norm = [str(c).zfill(6) for c in codes_in]

    rows: list[dict] = []
    for c in codes_norm:
        r = csv_df[csv_df["symbol"] == c]
        if r.empty:
            continue
        row = r.iloc[0]
        try:
            close_v = float(str(row.get("close", "0") or 0).replace(",", "") or 0)
        except (ValueError, TypeError):
            close_v = 0.0
        try:
            ctrt_v = float(str(row.get("tdy_ctrt", "0") or 0).replace(",", "") or 0)
        except (ValueError, TypeError):
            ctrt_v = 0.0
        rows.append({
            "종목명": str(row.get("name", c)),
            "코드": c,
            "어제종가": f"{close_v:,.0f}",
            "등락율(%)": f"{ctrt_v * 100:+.2f}",
        })

    if not rows:
        logger.info(f"{ts_prefix()} [전일 상한가 모니터링 대상 종목] CSV({last_date}) 매칭 종목 없음")
        return

    title = (f"{ts_prefix()} [전일 상한가 모니터링 대상 종목] {len(rows)}건 "
             f"(소스: Select_Tr_target_list.csv {last_date})")
    print(f"\n{title}")
    print_table(
        rows,
        columns=["종목명", "코드", "어제종가", "등락율(%)"],
        align={"종목명": "left", "코드": "left", "어제종가": "right", "등락율(%)": "right"},
    )
    logger.info(title)

    # 텔레그램 전송 (stdout 이중 출력 방지: tmsg 직접 호출)
    # 시간 가드: 09:00 이전에만 발송 (장중 재시작 시 노이즈 방지)
    if datetime.now(KST).time() >= dtime(9, 0):
        logger.info(f"{ts_prefix()} [morning_target_table] 09:00 이후 → 텔레그램 발송 스킵")
        return
    tele_lines = [f"[전일 상한가 모니터링 대상 종목] {len(rows)}건 (CSV {last_date})"]
    for r in rows:
        tele_lines.append(f"  {r['종목명']}({r['코드']}) 종가={r['어제종가']} 등락율={r['등락율(%)']}%")
    if tmsg is not None:
        try:
            tmsg(f"{ts_prefix()}\n" + "\n".join(tele_lines), "-t")
        except Exception:
            pass


_morning_target_codes = _load_morning_target_codes()
if _morning_target_codes:
    try:
        _print_morning_target_table(_morning_target_codes)
    except Exception as _e:
        logger.warning(f"[morning_target_table] {_e}")
    _code_text_codes = _morning_target_codes
else:
    _code_text_codes = _parse_codes(CODE_TEXT)
    logger.info(f"[codes] morning_target 없음 → CODE_TEXT 폴백({len(_code_text_codes)}개)")
codes = _load_codes_from_cfg(_code_text_codes)
if not codes:
    raise RuntimeError("CODE_TEXT에서 종목코드를 찾지 못했습니다.")
code_name_map = _code_name_map()
logger.info(f"[codes] count={len(codes)} codes={codes}")
_codes_lock = threading.RLock()
# base_codes = morning_target 종목 (하루 종일 보호, top30에 의한 해제 방지)
_base_codes = set(_morning_target_codes) if _morning_target_codes else set(_code_text_codes)
logger.info(f"[codes] base={len(_base_codes)}개 (보호, morning_target), top_added(복원)={len(_loaded_top_added)}개")
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

# [260428] main WSS 보호 — appkey ALREADY IN USE 시 long backoff
# (storm self-feeding loop 차단; close frame 송신 실패로 KIS 측 stale session 잔존 시 자연 회복 대기)
_main_last_already_in_use_ts: float = 0.0
# [260506] 핸드셰이크 실패(dwell<15s) 다발 시점. ALREADY IN USE 와는 별도로 추적해서
# 로그/원인 진단을 정확히 분리한다. (예전엔 같은 변수 재사용해서 오인 진단 발생)
_main_last_handshake_fail_ts: float = 0.0
# [260513] invalid approval 감지 시점. 1007 frame error 직후 KIS 측이 동일 appkey 에
# 같은 approval_key 를 짧게 재발급(throttle)하는 패턴 대응 — 감지 시 60s backoff 강제.
_main_last_invalid_approval_ts: float = 0.0
MAIN_WSS_BACKOFF_ALREADY_IN_USE_SEC: float = 90.0
MAIN_WSS_BACKOFF_INVALID_APPROVAL_SEC: float = 60.0
# [260520] 개장 직전/직후(시초) 핸드셰이크 실패 시 90s long backoff 가 09:00 시초 구간을
# 통째로 날리는 문제(260520 09:00 사건: 08:59:46 dwell<15s → 90s backoff → 09:01:16 까지 무수신)
# → near-open 구간(08:55~09:02)에 한해 backoff 단축. ALREADY IN USE 는 KIS 측 stale session
# 정리에 실제 시간이 필요(단축 시 재유발)하므로 단축 대상이 아니다.
MAIN_WSS_BACKOFF_NEAR_OPEN_SEC: float = 15.0

# [260506] WSS 자기보호 자동중단 — 핸드셰이크 N회 연속 실패 시 reconnect 중단 + 프로세스 종료
# (KIS WSS 가 IP 단위 throttle 누적되기 전 자기측에서 끊어 IP 차단 자초 방지)
# KRX 사이드카/서킷브레이커와는 무관 — KRX 정지 시 핸드셰이크는 정상이라 카운터 증가 안 함.
# [260511] 임계 10 → 2 로 단축. SDK retry 10회 = 1 burst → dwell<15s 면 1 카운트.
# 2 burst (약 22초) 안에 즉시 자체 종료 → ALREADY IN USE storm 의 origin 차단.
# [260513] 2 → 4 완화. 260513 15:26 사건: 1007 frame error → invalid approval throttle 폭주로
# 2 burst 안에 회복 불가 → 장 중반 사망. 4 burst (약 60초+) 안에 invalid approval backoff 가
# 동작할 시간을 확보.
HANDSHAKE_FAIL_SELF_STOP_LIMIT: int = 4

# [260427] v5 진단 카운터 (5분 주기 INFO 로그) — 매수가 안 일어난 원인 추적
_v5_diag_counters: dict[str, int] = {
    "fn_called": 0,             # _check_uplimit_v4_from_tick 함수 호출 횟수
    "kind_not_regular_real": 0, # 첫 분기 return
    "rows_processed": 0,        # 종목 단위 처리 횟수
    "code_invalid": 0,          # code 유효성 fail
    "in_holdover": 0,           # _uplimit_v4_holdover 차단
    "in_daily_bought": 0,       # _uplimit_v4_daily_bought_codes 차단
    "in_buy_pending": 0,        # _uplimit_buy_pending 차단
    "in_evaluated": 0,          # _uplimit_v5_evaluated 차단
    "limitup_reached": 0,       # 30% 도달 이력 차단
    "low_liquidity": 0,         # 저거래 차단
    "diversify_full": 0,        # 시드 슬롯 가득 참
    "ctrt_below_25": 0,         # prdy_ctrt < 25%
    "ctrt_at_or_above_30": 0,   # prdy_ctrt >= 30% (limitup_reached 추가 직전)
    "ctrt_in_observe_band": 0,  # 20~25% 관찰 구간
    "ctrt_in_buy_band": 0,      # 25~30% 매수 구간 진입
    "qualify_called": 0,        # 12필터 호출
    "qualify_passed": 0,        # 12필터 통과
    "qualify_failed": 0,        # 12필터 차단
    "trigger_evaluated": 0,     # 매수 트리거 평가
    "trigger_buy_fired": 0,     # 매수 트리거 발동
    "in_position_path": 0,      # 보유 포지션 path 진입
}
_v5_diag_last_dump_ts: float = 0.0
_V5_DIAG_DUMP_INTERVAL_SEC: float = 300.0   # 5분 주기 dump


def _v5_diag_dump_if_due() -> None:
    """5분 주기로 진단 카운터를 INFO 로그로 출력 + 리셋."""
    global _v5_diag_last_dump_ts
    now_ts = time.time()
    if now_ts - _v5_diag_last_dump_ts < _V5_DIAG_DUMP_INTERVAL_SEC:
        return
    _v5_diag_last_dump_ts = now_ts
    parts = [f"{k}={v}" for k, v in _v5_diag_counters.items() if v > 0]
    if not parts:
        logger.info(f"{ts_prefix()} [v5_diag] (5분 누적) 호출/처리 모두 0건 — v5 path 미호출 가능성")
    else:
        logger.info(f"{ts_prefix()} [v5_diag] (5분 누적) {' | '.join(parts)}")
    # 카운터 리셋
    for k in _v5_diag_counters:
        _v5_diag_counters[k] = 0


def _trigger_ws_rebuild():
    """종목 변경 시 즉시 동적 구독/해제 적용 (재연결 없이).

    [260526] 연속 timeout 시 os._exit(2) 에스컬레이션 추가.
    배경: 5/26 09:03~09:59 동안 동일 timeout 이 16회 반복되며 runner 재시작이 발동하지 못함.
          원인은 dead socket 이 아니라 scheduler_loop 가 _kws_lock 을 점유한 채 멈춰 있던 것.
          어느 쪽이든 임계(_REBUILD_FAIL_EXIT_LIMIT=5) 초과 시 프로세스를 종료해 runner 가 재기동.
    """
    global _rebuild_consecutive_fail_count

    def _do():
        with _kws_lock:
            if _active_kws is not None:
                desired = _desired_subscription_map(datetime.now(KST))
                _apply_subscriptions(_active_kws, desired)

    t = threading.Thread(target=_do, daemon=True)
    t.start()
    t.join(timeout=5.0)
    if t.is_alive():
        # timeout → _do 스레드가 _kws_lock 들고 dead socket에 send() 중 블로킹 가능
        # dead socket 강제 종료 → send() 에러로 _kws_lock 해제 → scheduler_loop 교착 방지
        _rebuild_consecutive_fail_count += 1
        logger.warning(
            f"{ts_prefix()} [ws] _trigger_ws_rebuild timeout(5s) "
            f"({_rebuild_consecutive_fail_count}/{_REBUILD_FAIL_EXIT_LIMIT}) — "
            f"dead socket/락점유 의심, 강제 종료"
        )
        try:
            if _active_kws is not None:
                _request_ws_close(_active_kws)
        except Exception:
            pass
        if _rebuild_consecutive_fail_count >= _REBUILD_FAIL_EXIT_LIMIT:
            try:
                _notify(
                    f"{ts_prefix()} [ws] _trigger_ws_rebuild "
                    f"{_rebuild_consecutive_fail_count}회 연속 실패 → "
                    f"os._exit(2) 하드종료 (runner 재시작 유도)",
                    tele=True,
                )
            except Exception:
                pass
            try:
                logger.error(
                    f"{ts_prefix()} [ws] rebuild "
                    f"{_rebuild_consecutive_fail_count}회 연속 실패 → os._exit(2)"
                )
            except Exception:
                pass
            try:
                _set_runtime_status(RUNTIME_STATUS_STOPPED)
            except Exception:
                pass
            time.sleep(0.5)
            os._exit(2)
    else:
        if _rebuild_consecutive_fail_count > 0:
            logger.info(
                f"{ts_prefix()} [ws] _trigger_ws_rebuild 성공 → 연속실패 카운터 리셋 "
                f"(이전={_rebuild_consecutive_fail_count})"
            )
        _rebuild_consecutive_fail_count = 0

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


END_TIME = dtime(18, 0) if OVERTIME_SINGLE_PRICE_BUY else dtime(16, 1)
_end_time_reached = False   # 18:00 종료 시도 시 True (WS 지연 종료 시 병합 판단용)

def _backup_log_file():
    """종료 시 로그 파일을 날짜별 백업 (wss_realtime_trading_YYMMDD.out)"""
    import shutil
    _base = os.path.dirname(os.path.abspath(__file__))
    src = os.path.join(_base, "out", "wss_realtime_trading.out")
    if not os.path.exists(src):
        return
    today_str = datetime.now(KST).strftime("%y%m%d")
    dst = os.path.join(_base, "out", f"wss_realtime_trading_{today_str}.out")
    try:
        shutil.copy2(src, dst)
        logger.info(f"{ts_prefix()} [로그백업] {dst}")
    except Exception as e:
        logger.warning(f"{ts_prefix()} [로그백업 실패] {e}")
_regular_real_seen: set[str] = set()
_close_force_stopped = False          # 15:30 유예시간 후 강제 구독 중단 1회 로그용
_last_overtime_buy_min: int = -1  # 마지막 실행 분(minute) - 10분당 1회
_ccnl_early_add_date: date | None = None  # 08:30 구독 추가한 날짜 (1일 1회)
_overtime_real_seen: set[str] = set()
_overtime_real_active = False
_overtime_real_started_ts: float = 0.0   # 체결가 전환 시점 (타임아웃용)
_overtime_real_last_progress_ts: float = 0.0  # 마지막 진행상황 출력 시각
OVERTIME_REAL_TIMEOUT = 180              # 180초 내 전 종목 미수신 시 강제 복귀
OVERTIME_REAL_PROGRESS_INTERVAL = 10     # 체결가 수신 중 진행상황 출력 간격(초)
_last_overtime_real_slot: datetime | None = None
_vi_delayed_codes: set[str] = set()   # VI 발동 의심 종목 (09:00~09:02 예상체결 유지)
_vi_delay_until: datetime | None = None  # VI 지연 종료 시각 (09:02:00)
# [260602 관찰] 08:59 prdy>9.8% 종목 H0STMKO0 관찰구독 → vi_cls_code 로그(연장기준 정합성 검증용, 동작 미변경)
_vi_observe_codes: set[str] = set()
_vi_observe_done: bool = False
_last_prdy_ctrt: dict[str, float] = {c: 0.0 for c in codes}
_last_mkop_cls_code: dict[str, str] = {}  # code -> new_mkop_cls_code (종목상태, WS 수신값)
_last_trid_per_code: dict[str, str] = {}  # code → 마지막 수신 tr_id
_last_stck_prpr: dict[str, float] = {}  # code -> 최근 체결가 (모니터링용)
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
        _sub_names = [code_name_map.get(c, c) for c in codes]
        _notify(f"{ts_prefix()} 08:50 예상체결가 구독 시작: {len(codes)}종목 {_sub_names}", tele=True)
    elif cur == RunMode.REGULAR_REAL:
        _sub_names = [code_name_map.get(c, c) for c in codes]
        _notify(f"{ts_prefix()} 09:00 예상체결가 구독 중지 -> 실시간 체결가 구독 시작: {len(codes)}종목 {_sub_names}", tele=True)
    elif cur == RunMode.CLOSE_EXP:
        pass  # _switch_to_closing_codes()에서 메시지 출력 완료 (tele 포함)
    elif cur == RunMode.CLOSE_REAL:
        _notify(f"{ts_prefix()} 15:30 종가 체결가 구독 전환")
    elif cur == RunMode.OVERTIME_EXP:
        if prev == RunMode.OVERTIME_REAL:
            _notify(f"{ts_prefix()} [시간외] 10분단위 실시간체결가 구독 종료 → 예상체결가 복귀")
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

_last_strategy_swap_ts: float = 0.0


def _refresh_str2_enabled() -> None:
    """config 기반 STR2_ENABLED 게이트 갱신 (기본 False).

    활성 조건 (OR):
      · config["str2"]["enabled"] == true
      · config["strategy_swap"]["module"] == "ws_realtime_tr_str2" 이고 status in (requested, applied)
    이 함수가 STR2_ENABLED 를 True 로 올리기 전까지 str2 관련 코드는 어떤 부작용도 내지 않는다.
    """
    global STR2_ENABLED
    try:
        cfg = _read_cfg()
        if not cfg:
            return
        enabled = False
        s2 = cfg.get("str2")
        if isinstance(s2, dict) and s2.get("enabled") is True:
            enabled = True
        swap = cfg.get("strategy_swap")
        if isinstance(swap, dict) and swap.get("module") == "ws_realtime_tr_str2" \
                and str(swap.get("status", "")) in ("requested", "applied"):
            enabled = True
        if enabled != STR2_ENABLED:
            STR2_ENABLED = enabled
            msg = f"{ts_prefix()} [str2] 게이트 {'활성' if enabled else '비활성'} (config 반영)"
            _notify(msg, tele=True)
            logger.info(msg)
    except Exception as e:
        logger.debug(f"[str2] 게이트 갱신 예외: {e}")


def _check_strategy_swap() -> None:
    """config.json의 strategy_swap.status == 'requested' 이면 전략 모듈 리로드."""
    global _last_strategy_swap_ts
    now_ts = time.time()
    if now_ts - _last_strategy_swap_ts < 1.0:  # 최소 1초 간격
        return
    _last_strategy_swap_ts = now_ts
    # str2 게이트는 swap 요청과 무관하게 매 폴링 갱신 (config["str2"]["enabled"] 반영)
    _refresh_str2_enabled()
    try:
        cfg = _read_cfg()
        if not cfg:
            return
        swap = cfg.get("strategy_swap")
        if not swap or not isinstance(swap, dict):
            return
        if swap.get("status") != "requested":
            return
        module_name = swap.get("module", "ws_realtime_tr_str1")
        import importlib
        mod = importlib.import_module(module_name)
        importlib.reload(mod)
        # str2 모듈 스왑: 모듈핸들 strat2 만 재바인딩 (판단 함수는 strat2.* 로 호출하므로 자동 반영)
        if module_name == "ws_realtime_tr_str2":
            global strat2
            strat2 = mod
            logger.info(f"{ts_prefix()} [전략교체] ws_realtime_tr_str2 모듈핸들 재바인딩")
            cfg["strategy_swap"] = {
                "status": "applied", "module": module_name,
                "timestamp": datetime.now(KST).isoformat(),
            }
            save_config(cfg, str(CONFIG_PATH))
            _refresh_str2_enabled()
            return
        # 리로드된 함수를 글로벌에 반영
        # calc_sell_pnl 은 kis_utils 공용 유틸로 이전 — 전략 스왑 대상 아님 (재바인딩 제외)
        global check_opening_call_auction_sell, check_opening_call_auction_cancel, check_realtime_sell
        check_opening_call_auction_sell = getattr(mod, "check_opening_call_auction_sell", check_opening_call_auction_sell)
        check_opening_call_auction_cancel = getattr(mod, "check_opening_call_auction_cancel", check_opening_call_auction_cancel)
        check_realtime_sell = getattr(mod, "check_realtime_sell", check_realtime_sell)
        # config에 적용 완료 기록
        cfg["strategy_swap"] = {
            "status": "applied",
            "module": module_name,
            "timestamp": datetime.now(KST).isoformat(),
        }
        save_config(cfg, str(CONFIG_PATH))
        msg = f"{ts_prefix()} [전략교체] {module_name} 리로드 적용 완료"
        _notify(msg, tele=True)
        logger.info(msg)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [전략교체] 실패: {e}")
        try:
            cfg2 = _read_cfg()
            if cfg2:
                cfg2["strategy_swap"] = {
                    "status": f"failed: {e}",
                    "module": cfg.get("strategy_swap", {}).get("module", ""),
                    "timestamp": datetime.now(KST).isoformat(),
                }
                save_config(cfg2, str(CONFIG_PATH))
        except Exception:
            pass


# ── 외부 주문 주입 (Phase 4-3) ───────────────────────────────────────────────
_last_external_order_ts: float = 0.0

# 외부주문 ordno → 매칭 정보 (체결통보 수신 시 식별용)
# {ordno: {"no": int, "alias": str, "code": str, "name": str,
#          "action": "매수"|"매도", "qty": int, "ord_dvsn": str, "price": float, "ts": float}}
# TODO: 24시간 이상 오래된 항목은 다음 거래일 첫 사이클에 cleanup
_external_order_track: dict[str, dict] = {}
_external_order_track_lock = threading.Lock()


def _format_external_order_intake_msg(order: dict, idx: int) -> str:
    """외부주문 1차 접수 텔레그램 메시지 구성 (종목명/계좌/수량/가격 포함)."""
    no = order.get("no", idx + 1)
    sym = str(order.get("symbol", "")).strip()
    order_type = int(order.get("order", 0) or 0)
    action = "매수" if order_type == 1 else ("매도" if order_type == 2 else "?")
    target = order.get("target", "all")
    if isinstance(target, list):
        target_desc = ",".join(str(t) for t in target)
    else:
        target_desc = str(target or "all")

    # 종목명 lookup — 실패해도 절대 죽지 않음
    name_part = sym
    try:
        parsed = _parse_codes(sym)
        if parsed:
            code = parsed[0]
            nm = code_name_map.get(code, "") if isinstance(code_name_map, dict) else ""
            if not nm:
                try:
                    nm = _code_name_map().get(code, "") or ""
                except Exception:
                    nm = ""
            name_part = f"{nm}({code})" if nm else code
    except Exception:
        name_part = sym

    # qty / amount
    qty = int(order.get("qty", 0) or 0)
    amount = float(order.get("amount", 0) or 0)
    if qty > 0:
        qty_part = f" qty={qty}"
    elif amount > 0:
        qty_part = f" amt={amount:,.0f}"
    else:
        qty_part = ""

    # target_price
    raw_tp = order.get("target_price", 0)
    if isinstance(raw_tp, str):
        _tp_str = raw_tp.strip()
        if _tp_str in ("상한가", "하한가"):
            price_part = f" price={_tp_str}"
        else:
            try:
                _tp_num = float(_tp_str or 0)
                price_part = " price=시장가" if _tp_num == 0 else f" price={_tp_num:,.0f}"
            except Exception:
                price_part = f" price={_tp_str}" if _tp_str else " price=시장가"
    else:
        try:
            _tp_num = float(raw_tp or 0)
            price_part = " price=시장가" if _tp_num == 0 else f" price={_tp_num:,.0f}"
        except Exception:
            price_part = " price=시장가"

    # ord_type — 빈값/auto 면 생략
    ot = str(order.get("ord_type", "")).strip()
    ot_part = "" if ot == "" or ot.lower() == "auto" else f" ord_type={ot}"

    return (f"{ts_prefix()} [외부주문] #{no} 접수: [{target_desc}] "
            f"{action} {name_part}{qty_part}{price_part}{ot_part}")


def _cleanup_failed_external_orders_at_startup() -> None:
    """프로그램 시작 직후(08:30 이전) 1회 호출: config.json 의 failed 외부주문 정리.

    - `external_orders` 배열에서 result.startswith("failed") 항목을 모두 삭제
    - 각 건마다 로그, 합계 로그 1회 (0건이면 무로그)
    - 08:30 이후엔 동작 안 함 (장 시작 후 발생한 failed 는 알림/수동 처리 대상)
    """
    try:
        if datetime.now(KST).time() >= dtime(8, 30):
            return
        cfg = _read_cfg()
        if not cfg:
            return
        ext_orders = cfg.get("external_orders")
        if not ext_orders or not isinstance(ext_orders, list):
            return

        removed = 0
        kept: list = []
        for order in ext_orders:
            if not isinstance(order, dict):
                kept.append(order)
                continue
            _result = str(order.get("result", "")).strip()
            if not _result.startswith("failed"):
                kept.append(order)
                continue
            # failed 건 → 로그 후 제외
            try:
                _no = order.get("no", "?")
                _sym_raw = str(order.get("symbol", "")).strip()
                _code_resolved = (_parse_codes(_sym_raw) or [_sym_raw])[0]
                _name = _code_name_map().get(_code_resolved, _sym_raw)
                _qty = int(order.get("target_qty") or order.get("qty") or 0)
                _amt = float(order.get("amount", 0) or 0)
                logger.info(
                    f"{ts_prefix()} [외부주문][시작cleanup] 삭제: "
                    f"종목={_name}({_code_resolved}) 수량={_qty} 금액={_amt:,.0f} result={_result}"
                )
            except Exception as _e:
                logger.info(
                    f"{ts_prefix()} [외부주문][시작cleanup] 삭제: order={order} "
                    f"(로그 구성 오류: {_e})"
                )
            removed += 1

        if removed > 0:
            if kept:
                cfg["external_orders"] = kept
            else:
                cfg.pop("external_orders", None)
            save_config(cfg, str(CONFIG_PATH))
            logger.info(f"{ts_prefix()} [외부주문][시작cleanup] {removed}건 정리 완료")
    except Exception as e:
        logger.warning(f"{ts_prefix()} [외부주문][시작cleanup] 처리 오류: {e}")


def _check_external_orders() -> None:
    """config.json의 external_orders 배열 모니터링 → 주문 실행.

    external_orders 각 항목 구조:
      no, target, order(1=매수/2=매도), symbol, qty, amount,
      target_price(0=시장가, "상한가"/"하한가" 키워드 가능), ord_type, result, target_qty, remain_qty

    ord_type (생략 시 auto):
      auto/""    : 현재 시각 기준 자동 매핑
                    <08:55           : 대기 (08:55까지)
                    08:55~15:30      : 시장가(01) 또는 지정가(00, target_price>0)
                    15:30~15:40      : 대기 (15:40까지)
                    15:40~16:00      : 장후시간외종가(06), 가격 미입력
                    16:00~18:00      : 시간외단일가(07),
                                       target_price 없으면 REST 로 상한가 자동 조회
                    >=18:00          : 대기 (다음날 08:55까지)
      pre_market : 장전시간외종가(05), 08:30~08:40, 전일종가 자동체결
      post_market: 장후시간외종가(06), 15:40~16:00, 당일종가 자동체결
      overtime   : 시간외단일가(07), 16:00~18:00, target_price 필수

    result 진행: "" → "접수" → "주문완료" → 삭제
                          → "failed: ..." → 1회 텔레그램 알림 후 유지 (수동 처리)
    """
    global _last_external_order_ts
    now_ts = time.time()
    if now_ts - _last_external_order_ts < 1.0:
        return
    _last_external_order_ts = now_ts

    try:
        cfg = _read_cfg()
        if not cfg:
            return
        # no_sell_codes 변경 감지 (매 사이클)
        _refresh_no_sell_from_cfg(cfg)
        ext_orders = cfg.get("external_orders")
        if not ext_orders or not isinstance(ext_orders, list):
            return

        changed = False
        completed_indices: list[int] = []

        for idx, order in enumerate(ext_orders):
            if not isinstance(order, dict):
                continue
            result = str(order.get("result", "")).strip()

            # ── 접수 단계: 빈 result → "접수" ──
            if result == "":
                # 재주문 진입: 이전 실패/대기 플래그 reset (재실패 시 다시 알림 받기 위함)
                order.pop("_failed_notified", None)
                order.pop("_time_wait_logged", None)
                order["result"] = "접수"
                changed = True
                logger.info(f"{ts_prefix()} [외부주문] config raw: {order}")
                try:
                    _intake_msg = _format_external_order_intake_msg(order, idx)
                except Exception as _e:
                    sym = str(order.get("symbol", "")).strip()
                    _intake_msg = (f"{ts_prefix()} [외부주문] #{order.get('no',idx+1)} 접수: "
                                   f"{'매수' if order.get('order')==1 else '매도'} {sym}")
                    logger.warning(f"{ts_prefix()} [외부주문] intake msg 구성 오류: {_e}")
                _notify(_intake_msg, tele=True)
                continue  # 다음 루프에서 실행

            # ── 실행 단계: "접수" → 주문 실행 → "주문완료" ──
            if result == "접수":
                _exec_external_order(cfg, order, idx)
                changed = True
                continue

            # ── 실패 처리: "failed: ..." → 1회 텔레그램 알림 후 config 유지 ──
            if result.startswith("failed"):
                if not order.get("_failed_notified"):
                    try:
                        _no = order.get("no", idx + 1)
                        _sym_raw = str(order.get("symbol", "")).strip()
                        _code_resolved = (_parse_codes(_sym_raw) or [_sym_raw])[0]
                        _name = _code_name_map().get(_code_resolved, _sym_raw)
                        _qty = int(order.get("target_qty") or order.get("qty") or 0)
                        _amt = float(order.get("amount", 0) or 0)
                        _fail_msg = (
                            f"{ts_prefix()} [외부주문] #{_no} 실패: 종목={_name}({_code_resolved}) "
                            f"수량={_qty} 금액={_amt:,.0f} result={result} — "
                            f"config 에서 result 를 빈값(\"\")으로 수정 시 재주문"
                        )
                    except Exception as _e:
                        _fail_msg = (
                            f"{ts_prefix()} [외부주문] #{order.get('no', idx + 1)} 실패: result={result} "
                            f"— config 에서 result 를 빈값(\"\")으로 수정 시 재주문 (메시지 구성 오류: {_e})"
                        )
                    _notify(_fail_msg, tele=True)
                    logger.warning(_fail_msg)
                    order["_failed_notified"] = True
                    changed = True
                continue

            # ── 완료 정리: "주문완료" → 결과 로그 후 삭제 대기 ──
            if result == "주문완료":
                try:
                    _no = order.get("no", idx + 1)
                    _sym_raw = str(order.get("symbol", "")).strip()
                    _code_resolved = (_parse_codes(_sym_raw) or [_sym_raw])[0]
                    _name = _code_name_map().get(_code_resolved, _sym_raw)
                    _t_qty = int(order.get("target_qty") or order.get("qty") or 0)
                    logger.info(
                        f"{ts_prefix()} [외부주문] #{_no} 완료 정리: "
                        f"종목={_name}({_code_resolved}) 수량={_t_qty} 결과={result} → config 삭제"
                    )
                except Exception as _e:
                    logger.info(
                        f"{ts_prefix()} [외부주문] #{order.get('no', idx + 1)} 완료 정리: "
                        f"결과={result} → config 삭제 (로그 구성 오류: {_e})"
                    )
                completed_indices.append(idx)

        # 완료 항목 삭제 (역순)
        if completed_indices:
            for i in sorted(completed_indices, reverse=True):
                ext_orders.pop(i)
            changed = True

        # 빈 배열이면 키 자체 제거
        if not ext_orders:
            if "external_orders" in cfg:
                del cfg["external_orders"]  # type: ignore[arg-type]
                changed = True

        if changed:
            save_config(cfg, str(CONFIG_PATH))

    except Exception as e:
        logger.warning(f"{ts_prefix()} [외부주문] 처리 오류: {e}")


def _exec_external_order(cfg: dict, order: dict, idx: int) -> None:
    """external_orders 단일 항목 실행.

    Phase 4-4: target 필드로 대상 계좌 지정. "all"이면 전체 계좌에 주문.

    ord_type 옵션 (생략 시 auto):
      auto / ""  : 현재 시각 기준 자동 매핑
                    <08:55       : 대기 (08:55까지)
                    08:55~15:30  : 시장가(01) (target_price=0) / 지정가(00) (target_price>0)
                    15:30~15:40  : 대기 (15:40까지)
                    15:40~16:00  : 장후시간외종가(06), target_price 무시
                    16:00~18:00  : 시간외단일가(07),
                                   target_price 없으면 REST 상한가(stck_mxpr) 자동 조회
                    >=18:00      : 대기 (다음날 08:55까지)
      pre_market : 장전시간외종가(05), 08:30~08:40, 전일종가 자동체결, 가격 미입력
      post_market: 장후시간외종가(06), 15:40~16:00, 당일종가 자동체결, 가격 미입력
      overtime   : 시간외단일가(07), 16:00~18:00, target_price 필수

    target_price 특수 키워드:
      "상한가" : REST API로 해당 종목 상한가 조회 → 지정가(00) 주문
      "하한가" : REST API로 해당 종목 하한가 조회 → 지정가(00) 주문
    """
    logger.info(f"{ts_prefix()} [외부주문] 실행 시작 #{order.get('no',idx+1)}: {order}")
    sym = str(order.get("symbol", "")).strip()
    order_type = int(order.get("order", 0))  # 1=매수, 2=매도
    qty = int(order.get("qty", 0) or 0)
    amount = float(order.get("amount", 0) or 0)
    raw_target_price = order.get("target_price", 0)
    # "상한가"/"하한가" 키워드 지원 — 종목코드 확정 후 실제 가격 조회
    _price_keyword = str(raw_target_price).strip() if isinstance(raw_target_price, str) else ""
    target_price = 0.0 if _price_keyword in ("상한가", "하한가") else float(raw_target_price or 0)
    target = order.get("target", "all")
    ord_type = str(order.get("ord_type", "")).strip().lower()

    if order_type not in (1, 2):
        order["result"] = f"failed: order={order_type} 잘못된 주문유형"
        return

    # ── 종목코드 변환 ──
    codes_resolved = _parse_codes(sym)
    if not codes_resolved:
        order["result"] = f"failed: '{sym}' 종목 못찾음"
        logger.warning(f"{ts_prefix()} [외부주문] 종목 못찾음: {sym}")
        return
    code = codes_resolved[0]

    # ── "상한가"/"하한가" 키워드 → 실제 가격 조회 ──
    if _price_keyword in ("상한가", "하한가"):
        try:
            _any_acct = next(iter(_iter_enabled_accounts()), None)
            if _any_acct:
                _pc = _init_account_client(_any_acct)
                _resp = _pc.inquire_price(code)
                if _price_keyword == "상한가":
                    target_price = float(str(_resp.get("stck_mxpr") or 0).replace(",", "") or 0)
                else:
                    target_price = float(str(_resp.get("stck_llam") or 0).replace(",", "") or 0)
                if target_price <= 0:
                    order["result"] = f"failed: {_price_keyword} 조회 실패"
                    return
                logger.info(f"{ts_prefix()} [외부주문] {code} {_price_keyword}={target_price:,.0f}")
        except Exception as e:
            order["result"] = f"failed: {_price_keyword} 조회 오류: {e}"
            return

    # ── 수량 계산 (amount 지정 시 현재가 기준으로 qty 산출) ──
    if qty <= 0 and amount > 0:
        price_ref = _last_stck_prpr.get(code, 0)
        if price_ref <= 0:
            ema = _ema_state.get(code, {})
            price_ref = ema.get(3, 0) or 0
        if price_ref > 0:
            qty = int(amount / price_ref)
        if qty <= 0:
            order["result"] = f"failed: 수량 산출 불가 (price_ref={price_ref:.0f})"
            return

    if qty <= 0:
        order["result"] = "failed: qty=0"
        return

    order["target_qty"] = qty
    order["remain_qty"] = qty

    # ── 대상 계좌 결정 (Phase 4-4) ──
    all_accounts = _iter_enabled_accounts()
    target_accounts = _resolve_target_accounts(target, all_accounts)
    if not target_accounts:
        order["result"] = "failed: 대상 계좌 없음"
        return

    # ── ord_dvsn / price_val 결정 ──
    _ORD_TYPE_MAP = {
        "pre_market": ("05", "장전시간외종가"),
        "post_market": ("06", "장후시간외종가"),
        "overtime": ("07", "시간외단일가"),
    }
    _now_dt = datetime.now(KST)
    _now_t = _now_dt.time()
    _now_hm = _now_dt.strftime("%H%M")
    _no_for_log = order.get("no", idx + 1)

    if ord_type in _ORD_TYPE_MAP:
        # ── 명시 ord_type: 기존 동작 그대로 유지 ──
        ord_dvsn, ord_label = _ORD_TYPE_MAP[ord_type]
        # 05/06은 가격 미입력, 07은 target_price 필수
        if ord_dvsn == "07" and target_price <= 0:
            order["result"] = f"failed: 시간외단일가(07)는 target_price 필수"
            return
        price_val = target_price if ord_dvsn == "07" else 0

        # ── 명시 ord_type 시간 윈도우 체크 (대기) ──
        _ORD_TIME_WINDOW = {
            "05": ("0830", "0840", "장전시간외종가(05)는 08:30~08:40"),
            "06": ("1540", "1600", "장후시간외종가(06)는 15:40~16:00"),
            "07": ("1600", "1800", "시간외단일가(07)는 16:00~18:00"),
        }
        if ord_dvsn in _ORD_TIME_WINDOW:
            _t_start, _t_end, _t_desc = _ORD_TIME_WINDOW[ord_dvsn]
            if _now_hm < _t_start or _now_hm >= _t_end:
                # 아직 시간이 안 됐으면 "접수" 상태 유지 → 다음 사이클에서 재시도
                order["result"] = "접수"
                if not order.get("_time_wait_logged"):
                    logger.info(f"{ts_prefix()} [외부주문] #{_no_for_log} 시간 대기 중: {_t_desc} (현재={_now_hm})")
                    order["_time_wait_logged"] = True
                return
    else:
        # ── auto (기본) / 빈값: 현재 시각 기준 자동 매핑 ──
        if ord_type and ord_type != "auto":
            logger.warning(f"{ts_prefix()} [외부주문] 미인식 ord_type='{ord_type}' → auto 처리")

        # 시각별 분기:
        #   < 08:55          : 대기 (08:55 까지)
        #   08:55 ~ 15:30    : 시장가(01) 또는 지정가(00, target_price>0)
        #   15:30 ~ 15:40    : 대기 (15:40 까지)
        #   15:40 ~ 16:00    : 장후시간외종가(06), 가격 미입력
        #   16:00 ~ 18:00    : 시간외단일가(07), target_price 없으면 상한가 자동 조회
        #   >= 18:00         : 대기 (다음날 08:55 까지)
        _wait_reason: str | None = None
        if _now_t < dtime(8, 55):
            _wait_reason = "08:55까지"
        elif _now_t < dtime(15, 30):
            # 08:55 ~ 15:30 : 동시호가(08:55~09:00) 시장가 + 장중(09:00~15:30) 시장가/지정가
            ord_dvsn = "01" if target_price == 0 else "00"
            ord_label = "시장가" if ord_dvsn == "01" else "지정가"
            price_val = target_price if target_price > 0 else 0
        elif _now_t < dtime(15, 40):
            _wait_reason = "15:40까지"
        elif _now_t < dtime(16, 0):
            # 15:40 ~ 16:00 : 장후시간외종가(06), target_price 무시
            ord_dvsn = "06"
            ord_label = "장후시간외종가"
            price_val = 0
        elif _now_t < dtime(18, 0):
            # 16:00 ~ 18:00 : 시간외단일가(07)
            ord_dvsn = "07"
            ord_label = "시간외단일가"
            if target_price > 0:
                price_val = target_price
            else:
                # target_price 미지정 → REST inquire_price 로 상한가(stck_mxpr) 조회
                try:
                    _any_acct = next(iter(_iter_enabled_accounts()), None)
                    if _any_acct:
                        _pc = _init_account_client(_any_acct)
                        _resp = _pc.inquire_price(code)
                        _mxpr = float(str(_resp.get("stck_mxpr") or 0).replace(",", "") or 0)
                        if _mxpr <= 0:
                            order["result"] = "failed: 시간외단일가(07) 상한가 조회 실패"
                            return
                        price_val = _mxpr
                        logger.info(f"{ts_prefix()} [외부주문] #{_no_for_log} 시간외단일가 상한가 자동조회: {code} = {price_val:,.0f}")
                    else:
                        order["result"] = "failed: 시간외단일가(07) 상한가 조회용 계좌 없음"
                        return
                except Exception as _e:
                    order["result"] = f"failed: 시간외단일가(07) 상한가 조회 오류: {_e}"
                    return
        else:
            _wait_reason = "다음날 08:55까지"

        if _wait_reason is not None:
            # 아직 시간이 안 됐으면 "접수" 상태 유지 → 다음 사이클에서 재시도
            order["result"] = "접수"
            if not order.get("_time_wait_logged"):
                logger.info(f"{ts_prefix()} [외부주문] #{_no_for_log} 시간 대기 중: {_wait_reason} (현재={_now_hm})")
                order["_time_wait_logged"] = True
            return

    name = _code_name_map().get(code, code)
    action = "매수" if order_type == 1 else "매도"
    success_count = 0
    total_ordered_qty = 0

    for acct in target_accounts:
        cano = acct["cano"]
        acnt = acct.get("acnt_prdt_cd", "01") or "01"
        alias = acct.get("alias", acct["account_id"])
        try:
            client = _init_account_client(acct)

            if order_type == 1:  # 매수
                tr_id = "TTTC0802U"
                j = _buy_order_cash(client, cano, acnt, tr_id, code, qty, price_val, ord_dvsn)
            else:  # 매도
                j = _sell_order_cash(client, cano, acnt, code, qty, price_val, ord_dvsn)

            ordno = j.get("output", {}).get("ODNO", "")
            success_count += 1
            total_ordered_qty += qty

            price_desc = ord_label if price_val == 0 else f"{price_val:,.0f}"
            msg = (f"{ts_prefix()} [외부주문] #{order.get('no',idx+1)} [{alias}] 접수 완료: "
                   f"{action} {name}({code}) qty={qty} {ord_label}({ord_dvsn}) price={price_desc} ordno={ordno}")
            _notify(msg, tele=True)
            logger.info(msg)

            try:
                _ext_is_market = ord_dvsn == "01"
                _ext_order_type = "buy_order" if order_type == 1 else "sell_order"
                _ext_reason = f"external_{action}주문"
                _append_ledger(
                    order_type=_ext_order_type,
                    code=code, name=name,
                    buy_price=0 if (_ext_is_market and order_type == 1) else (price_val if order_type == 1 else 0),
                    sell_price=0 if (_ext_is_market and order_type == 2) else (price_val if order_type == 2 else 0),
                    order_qty=qty,
                    ord_no=ordno,
                    ord_dvsn=f"{ord_dvsn}({ord_label})",
                    note=f"계좌={alias}",
                    cano_alias=alias,
                    stck_prpr=_last_stck_prpr.get(code, 0.0),
                    reason=_ext_reason,
                )
            except Exception:
                pass

            # 외부주문 ordno 추적 (체결통보 매칭용)
            if ordno:
                try:
                    with _external_order_track_lock:
                        _external_order_track[str(ordno).strip()] = {
                            "no": order.get("no", idx + 1),
                            "alias": alias,
                            "code": code,
                            "name": name,
                            "action": action,
                            "qty": qty,
                            "ord_dvsn": ord_dvsn,
                            "price": price_val,
                            "ts": time.time(),
                        }
                except Exception as _e:
                    logger.warning(f"{ts_prefix()} [외부주문] ordno 추적 등록 실패: {_e}")

        except Exception as e:
            _last_err_msg = str(e)
            order["_last_error"] = f"[{alias}] {_last_err_msg}"
            logger.warning(f"{ts_prefix()} [외부주문] #{order.get('no',idx+1)} [{alias}] 실패: {e}")

    if success_count > 0:
        order["result"] = "주문완료"
        order["remain_qty"] = 0
        # no_sell 옵션: 매수 성공 시 매도 금지 목록에 자동 추가
        if order_type == 1 and order.get("no_sell"):
            _add_no_sell_code(sym.zfill(6), source="external_order")
    else:
        _err_tail = order.get("_last_error", "")
        order["result"] = f"failed: {len(target_accounts)}계좌 전부 실패" + (f" ({_err_tail})" if _err_tail else "")


# [260526] WSS 무수신 watchdog — 독립 데몬 스레드.
# 배경: 종전 watchdog 은 scheduler_loop while 안에 있어 scheduler 가 _kws_lock 무한대기로
#       정지하면 watchdog 도 동반 정지(5/26 사고). 이를 분리해 scheduler 와 무관하게 자체
#       sleep 루프로 _last_any_recv_ts 만 보고 판정 → 임계 도달 시 os._exit(2).
WSS_NO_RECV_EXIT_SEC = 180.0     # 180초 무수신 시 os._exit(2)
WSS_NO_RECV_ALERT_SEC = 90.0     # 90초 무수신 시 텔레그램 경고 1회
WSS_WATCHDOG_GRACE_SEC = 120.0   # 프로세스 시작 직후 120초는 유예


def _wss_norecv_watchdog_loop():
    """[260526] WSS 무수신 watchdog (독립 데몬).
    - 시간창: 09:00 ~ 15:20 (종전 09:30 → 09:00 으로 확대: 개장 직후 구독 분리 사각지대 제거).
    - 임계: 90초 알림 1회 → 180초 os._exit(2).
    - _last_any_recv_ts 만 읽으므로 lock 불요(단일 float 갱신은 GIL 보호).
    """
    _process_start_ts = time.time()
    _alert_sent = False
    while not _stop_event.is_set():
        try:
            _now_t = datetime.now(KST).time()
            if (dtime(9, 0) <= _now_t < dtime(15, 20)
                and (time.time() - _process_start_ts) > WSS_WATCHDOG_GRACE_SEC):
                _idle_sec = time.time() - _last_any_recv_ts if _last_any_recv_ts > 0 else 0
                if _idle_sec >= WSS_NO_RECV_ALERT_SEC and not _alert_sent:
                    try:
                        _notify(
                            f"{ts_prefix()} [wss_watchdog] ★ WSS {int(_idle_sec)}초 무수신 감지 "
                            f"({int(WSS_NO_RECV_EXIT_SEC - _idle_sec)}초 후 프로세스 강제 종료 예정)",
                            tele=True,
                        )
                    except Exception:
                        pass
                    _alert_sent = True
                if _idle_sec >= WSS_NO_RECV_EXIT_SEC:
                    try:
                        _notify(
                            f"{ts_prefix()} [wss_watchdog] ★★ WSS {int(_idle_sec)}초 무수신 "
                            f"→ os._exit(2) 로 프로세스 종료 (runner 재시작 유도)",
                            tele=True,
                        )
                    except Exception:
                        pass
                    try:
                        logger.error(f"{ts_prefix()} [wss_watchdog] idle={int(_idle_sec)}s → os._exit(2)")
                    except Exception:
                        pass
                    try:
                        _set_runtime_status(RUNTIME_STATUS_STOPPED)
                    except Exception:
                        pass
                    time.sleep(1.0)
                    os._exit(2)
                # 수신 재개 시 alert 플래그 리셋
                if _idle_sec < 10 and _alert_sent:
                    _alert_sent = False
                    try:
                        logger.info(f"{ts_prefix()} [wss_watchdog] 수신 재개, alert 리셋")
                    except Exception:
                        pass
        except Exception as _we:
            try:
                logger.debug(f"[wss_watchdog] 예외: {_we}")
            except Exception:
                pass
        time.sleep(1.0)


def scheduler_loop():
    global _current_mode, _last_rebuild_ts, _last_no_data_warn_ts, _no_data_rebuild_count
    global _overtime_real_active, _overtime_real_last_progress_ts, _vi_delayed_codes, _vi_delay_until, _last_overtime_real_slot
    global _end_time_reached, _active_kws, _vi_observe_codes, _vi_observe_done
    logger.info(f"{ts_prefix()} [scheduler] started")
    # ── 시작 시 cleanup: 08:30 이전이면 config 의 failed 외부주문 정리 (1회) ──
    _cleanup_failed_external_orders_at_startup()
    # [260526] WSS 무수신 watchdog 은 독립 데몬 스레드(_wss_norecv_watchdog_loop)로 분리됨.
    _last_per_code_log_ts = 0.0      # [260423] 종목별 수신 분포 주기 로그
    while not _stop_event.is_set():
        try:
            now = datetime.now(KST)

            # [260423] 종목별 WSS 수신 분포 주기 로그 (5분) — 수신 감소 원인 진단용
            try:
                _now_sec = time.time()
                if _now_sec - _last_per_code_log_ts >= 300 and _last_wss_recv_ts:
                    _last_per_code_log_ts = _now_sec
                    # 구독 중인 종목 기준
                    with _lock:
                        _sub_codes = set(codes)
                    _active = []
                    _stale = []
                    for c in _sub_codes:
                        last_ts = _last_wss_recv_ts.get(c, 0.0)
                        idle = _now_sec - last_ts if last_ts > 0 else -1
                        nm = code_name_map.get(c, c)
                        if last_ts == 0:
                            _stale.append(f"{nm}({c}:미수신)")
                        elif idle > 60:
                            _stale.append(f"{nm}({c}:{int(idle)}s)")
                        else:
                            _active.append(f"{nm}({c}:{int(idle)}s)")
                    logger.info(
                        f"{ts_prefix()} [recv_분포] 구독={len(_sub_codes)}/"
                        f"활성(≤60s)={len(_active)}/stale(>60s)={len(_stale)}"
                    )
                    if _stale:
                        logger.info(f"{ts_prefix()} [recv_분포] stale: {_stale[:20]}")
            except Exception as _pe:
                logger.debug(f"[recv_분포] 예외: {_pe}")
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
                                _notify(f"{ts_prefix()} [체결통보] 08:29:59 구독 추가: tr_key={acc_key!r}", tele=True)
                                _send_subscribe(_active_kws, ccnl_notice, [acc_key], "1")  # noqa: F405
                                _subscribed["ccnl_notice"] = {acc_key}
                            else:
                                warn_msg = f"{ts_prefix()} [체결통보] 08:29:59 구독 스킵: my_htsid 미설정!"
                                logger.warning(warn_msg)
                                _notify(warn_msg, tele=True)
            # 08:30~08:40 시간외 종가: 당일 _morning_target_codes 기준 추가 매수 (ORD_DVSN=05)
            # MORNING_EXTRA_CLOSING_PR_BUY=False 이면 매수/결과출력 모두 스킵
            if MORNING_EXTRA_CLOSING_PR_BUY:
                if dtime(8, 30) <= now.time() < dtime(8, 41):
                    try:
                        _run_morning_extra_closing_buy()
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [08:30시간외종가] {e}")
                # 08:30~08:40 종료 후 — 당일 발사한 주문에 한해 결과 표 출력
                if dtime(8, 41) <= now.time() < dtime(8, 42):
                    global _morning_extra_logged_done
                    if not _morning_extra_logged_done:
                        try:
                            _log_morning_extra_result()
                            _morning_extra_logged_done = True
                        except Exception as e:
                            logger.warning(f"{ts_prefix()} [08:30시간외종가 로그] {e}")
            # 08:42~08:57 매도 상태 보충: 시작잔고에서 누락된 포지션 재탐색 (1회)
            if dtime(8, 42) <= now.time() < dtime(8, 58):
                global _sell_state_supplement_done
                if (not _sell_state_supplement_done
                        and _morning_extra_closing_done
                        and not _str1_sell_state
                        and STR1_SELL_ENABLED and CLOSE_BUY):
                    try:
                        fresh_balance = _get_balance_holdings()
                        if fresh_balance:
                            _fb_detail = ", ".join(f"{code_name_map.get(c,c)}({c})" for c in sorted(fresh_balance.keys()))
                            logger.info(f"{ts_prefix()} [str1_sell] 시작잔고 누락 보충: "
                                        f"{len(fresh_balance)}종목 발견 → [{_fb_detail}]")
                            _notify(f"{ts_prefix()} [str1_sell] 시작잔고 누락 보충: "
                                    f"{len(fresh_balance)}종목 [{_fb_detail}]", tele=True)
                            _load_str1_sell_state_on_startup(fresh_balance)
                            _trigger_ws_rebuild()
                        _sell_state_supplement_done = True
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [str1_sell] 매도 상태 보충 실패: {e}")
            # 08:58 계좌잔고조회 1회 (lock 밖에서 실행, HTTP 호출 있음)
            if dtime(8, 58) <= now.time() < dtime(8, 59):
                try:
                    _run_balance_0858()
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [잔고0858] {e}")
            # 08:59:50~09:00:10 동시호가 주문현황 정리 1회 (체결대기/취소요청/이상없음)
            if dtime(8, 59, 50) <= now.time() < dtime(9, 0, 10):
                try:
                    _print_opening_call_auction_order_summary()
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [장전주문현황] {e}")
            # 09:00~15:20 주기적 매도 조건 체크 (15초마다, 틱 의존 없이 _last_prdy_ctrt 사용)
            if dtime(9, 0) <= now.time() < dtime(15, 20):
                try:
                    _check_sell_by_prdy_ctrt_periodic()
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [주기매도체크] {e}")
            # 09:00~15:20 장중 잔고조회: 체결 대기 종목이 있으면 1분마다 잔고조회
            if dtime(9, 0) <= now.time() < dtime(15, 20):
                global _pending_order_balance_last_ts
                if _ccnl_notice_sub_codes:
                    now_ts_bal = time.time()
                    if now_ts_bal - _pending_order_balance_last_ts >= 60.0:
                        _pending_order_balance_last_ts = now_ts_bal
                        try:
                            _run_closing_balance_verification("장중_체결대기")
                        except Exception as e:
                            logger.warning(f"{ts_prefix()} [장중잔고조회] {e}")
            # 15:19:30 일괄매도: 전일대비 20% 미만 또는 고가대비 -5% 초과 하락 종목 매도
            if dtime(15, 19, 28) <= now.time() <= dtime(15, 19, 35):
                try:
                    _run_sell_1518()
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [일괄매도] {e}")
            # 15:19:00 종가매수 사전 준비 (종목선정 + 수량계산, 주문 미발송)
            if dtime(15, 19, 0) <= now.time() <= dtime(15, 19, 5):
                try:
                    _prepare_closing_buy_orders()
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [종가매수준비] {e}")
            # 15:19:10 사전 구독 해제: 기존 ccnl_krx 전종목 해제 (H0STCNI0/H0STMKO0 유지)
            if dtime(15, 19, 10) <= now.time() <= dtime(15, 19, 15):
                global _pre_unsub_closing_done
                if not _pre_unsub_closing_done and _closing_codes:
                    _pre_unsub_closing_done = True
                    try:
                        with _kws_lock:
                            if _active_kws is not None:
                                have_real = _subscribed.get("ccnl_krx", set())
                                to_unsub = list(have_real)
                                if to_unsub:
                                    for i in range(0, len(to_unsub), 10):
                                        batch = to_unsub[i:i+10]
                                        _send_subscribe(_active_kws, ccnl_krx, batch, "2")  # noqa: F405
                                    _subscribed["ccnl_krx"] = set()
                                    _notify(f"{ts_prefix()} [사전구독해제] 15:19:10 정규장 {len(to_unsub)}종목 전체 해제 완료")
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [사전구독해제] {e}")
            # 15:21:00 예상체결가 구독 시작 (WSS 유지 상태에서 subscribe만)
            if dtime(15, 21, 0) <= now.time() <= dtime(15, 21, 5):
                if not getattr(_switch_to_closing_codes, '_exp_sub_done', False) and _closing_codes:
                    try:
                        with _kws_lock:
                            if _active_kws is not None:
                                _send_subscribe(_active_kws, exp_ccnl_krx, list(_closing_codes), "1")  # noqa: F405
                                _subscribed["exp_ccnl_krx"] = set(_closing_codes)
                                _switch_to_closing_codes._exp_sub_done = True
                                _notify(f"{ts_prefix()} [예상체결가구독] 15:21 {len(_closing_codes)}종목 구독 완료", tele=True)
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [예상체결가구독] {e}")
            # 15:29:30 일시 확인: 예상체결가 첫 틱 대비 하락 시 종가매수 취소
            if dtime(15, 29, 28) <= now.time() <= dtime(15, 29, 35):
                try:
                    _run_closing_exp_check_152930()
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [종가매수-15:29:30] {e}")
            # 15:29:50 취소 금액 재분배
            if dtime(15, 29, 50) <= now.time() <= dtime(15, 29, 55):
                try:
                    _run_closing_cancel_redistribute()
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [종가재분배] {e}")
            # 15:30:40 실시간 체결 수신 완료 후 잔고조회로 주문 결과 메시지 (tele)
            if dtime(15, 30, 40) <= now.time() < dtime(15, 31):
                try:
                    _run_closing_buy_filled_notify()
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [종가매수체결알림] {e}")
            # ── 잔고조회 스케줄러: 체결통보 누락분 보완 ──
            # 15:32 — 종가 체결 결과 검증 (15:31 → 15:32 지연: 15:30:00 구독전환 후 hot path 안정화 대기)
            if dtime(15, 32, 0) <= now.time() <= dtime(15, 32, 5):
                global _closing_balance_1531_done
                if not _closing_balance_1531_done:
                    _closing_balance_1531_done = True
                    try:
                        _run_closing_balance_verification("15:32_종가체결")
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [잔고검증15:32] {e}")
            # 15:58 — 시간외 종가 체결 결과 검증
            if dtime(15, 58, 0) <= now.time() <= dtime(15, 58, 5):
                global _closing_balance_1558_done
                if not _closing_balance_1558_done:
                    _closing_balance_1558_done = True
                    try:
                        _run_closing_balance_verification("15:58_시간외종가")
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [잔고검증15:58] {e}")
            # 16:11, 16:21, ..., 18:01 — 시간외 단일가 10분 단위 체결 확인
            if OVERTIME_SINGLE_PRICE_BUY and dtime(16, 11) <= now.time() < END_TIME:
                global _overtime_balance_checked
                minute = now.minute
                if minute % 10 == 1 and now.second <= 5:
                    slot_key = f"{now.hour:02d}:{minute:02d}"
                    if slot_key not in _overtime_balance_checked:
                        _overtime_balance_checked.add(slot_key)
                        try:
                            _run_closing_balance_verification(f"{slot_key}_시간외단일가")
                        except Exception as e:
                            logger.warning(f"{ts_prefix()} [잔고검증{slot_key}] {e}")
            # 15:58 당일 상한가 재선정 (1m 데이터 기반 → 16:00~18:00 시간외 단일가 대상)
            # 15:20 top30 기반 선정보다 장 마감 후 데이터가 더 정확 → 시간외는 이걸 사용
            if OVERTIME_SINGLE_PRICE_BUY and dtime(15, 58) <= now.time() < dtime(16, 0) and not _today_closing_target_done:
                try:
                    _run_today_target_reselect()
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [today_target] 실행 실패: {e}")
            # 15:40~16:00 시간외 종가: 당일 미매수 종목에 당일종가(ORD_DVSN=02) 매수
            if dtime(15, 40) <= now.time() < dtime(16, 1):
                try:
                    _run_afternoon_extra_closing_buy()
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [15:40시간외종가] {e}")
            # 15:40~16:00 종료 후 주문내역·결과 로그 (15:55에 출력 → 16:00 시간외단일가 진입 전 완료)
            if dtime(15, 55) <= now.time() < dtime(15, 56):
                global _afternoon_extra_logged_done
                if not _afternoon_extra_logged_done:
                    try:
                        _log_closing_result_after_window("15:40~16:00")
                        _afternoon_extra_logged_done = True
                    except Exception as e:
                        logger.warning(f"{ts_prefix()} [15:40시간외종가 로그] {e}")
            # 16:00~18:00 시간외 단일가 매수: 10분 단위(16:00, 16:10, ... 17:50) 체결통보 반영 후 주문→직전 결과 출력
            if OVERTIME_SINGLE_PRICE_BUY and dtime(16, 0) <= now.time() < END_TIME:
                global _last_overtime_buy_min
                minute = now.minute
                second = now.second
                if minute % 10 == 0 and minute != _last_overtime_buy_min:
                    if (minute == 0 and second <= 2) or (minute > 0 and 1 <= second <= 3):  # 16:00:00 / 16:10:01 ...
                        _last_overtime_buy_min = minute
                        # 직전 슬롯 결과 출력 (16:00에는 이미 15:55에 출력했으므로 스킵)
                        if minute > 0:
                            prev_label = f"{now.hour:02d}:{(minute - 10) % 60:02d}단일가"
                            _log_closing_order_aggregation(prev_label)
                        try:
                            _run_overtime_buy_orders()
                        except Exception as e:
                            logger.warning(f"{ts_prefix()} [시간외매수] {e}")
            # ── [260602] 08:45 진단로그 확인 TODO 리마인더 (decode skip 근본원인) ──
            # 08:50 예상체결(H0STANC0) 구독 직전에 1회 알림. 장전 08:50~08:59 에 [ws][utf8-diag #N]
            # 로그가 찍히면 cp949='...' 필드로 CP949 한글 유입 여부를 확정한다.
            global _diag_reminder_done
            if (not _diag_reminder_done) and dtime(8, 45) <= now.time() < dtime(8, 50):
                _diag_reminder_done = True
                _notify(
                    f"{ts_prefix()} [TODO][진단] decode skip 근본원인 진단로그 확인 — "
                    f"08:50~08:59 장전 예상체결(H0STANC0) 구간에서 '[ws][utf8-diag #N]' 라인의 "
                    f"cp949='...' 값 확인(멀쩡한 한글=CP949 인코딩 유입 확정). "
                    f"로그: out/logs/wss_TR_{datetime.now(KST).strftime('%y%m%d')}.log",
                    tele=True,
                )

            new_mode = calc_mode(now)
            with _mode_lock:
                old_mode = _current_mode
                if old_mode != new_mode:
                    _current_mode = new_mode
                    _log_mode_transition(old_mode, new_mode)
                    if new_mode == RunMode.CLOSE_EXP and old_mode == RunMode.REGULAR_REAL:
                        # 15:20:00 정시 전환 시 종가매수 즉시 발송 (top_rank_loop 타이밍 놓침 방지)
                        if CLOSE_BUY and _closing_codes:
                            try:
                                logger.info(f"{ts_prefix()} [종가매수] CLOSE_EXP 진입 → _switch_to_closing_codes 즉시 호출")
                                _switch_to_closing_codes()
                            except Exception as e:
                                logger.warning(f"{ts_prefix()} [종가매수] CLOSE_EXP 전환 처리 실패: {e}")
                    if new_mode == RunMode.CLOSE_REAL:
                        _regular_real_seen.clear()
                        global _close_force_stopped
                        _close_force_stopped = False
                    if new_mode == RunMode.OVERTIME_REAL:
                        _overtime_real_seen.clear()
                    if new_mode == RunMode.EXIT:
                        if OVERTIME_SINGLE_PRICE_BUY:
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
                        # ② 버퍼 flush + parts 병합 (18:00 장 종료 시에만 병합)
                        _flush_part_buffer(f"{END_TIME.strftime('%H:%M')}_shutdown", force_full=True)
                        _merge_parts_to_final()
                        # ②-1 거래장부 연도별 parquet 갱신
                        _append_daily_ledger_to_yearly()
                        time.sleep(1.0)
                        # ③ 최종 요약
                        _notify(_final_summary())
                        time.sleep(1.0)
                        # ④ 프로그램 종료 안내 (텔레그램)
                        if OVERTIME_SINGLE_PRICE_BUY:
                            _notify(f"{ts_prefix()} 18:00 이후 시간외 단일가 종료로 프로그램 종료", tele=True)
                        else:
                            _notify(f"{ts_prefix()} 16:01 시간외 단일가 미사용으로 프로그램 종료", tele=True)
                        # ⑤ 로그 파일 날짜별 백업
                        _backup_log_file()
                        _end_time_reached = True
                        _stop_event.set()
                        # [260506] kws.start() 의 자동 reconnect 루프가 _stop_event 를 못 보고
                        # 좀비로 살아남는 것을 막기 위한 강제 종료. 모든 저장/통보/백업 끝.
                        _set_runtime_status(RUNTIME_STATUS_STOPPED)
                        logger.info(f"{ts_prefix()} [scheduler] EXIT 모드 정리 완료 → os._exit(0)")
                        os._exit(0)

                # ── [260602 관찰] 08:59 prdy>9.8% 종목 H0STMKO0 관찰구독 → vi_cls_code 로그 ──
                # 목적: 장 오픈 연장(VI 예측) 기준을 H0STMKO0 vi_cls_code 로 전환하기 전, 09:00 전후
                #   그 값이 어떻게 오는지 정합성 검증. 관찰 전용 — 실제 연장 동작은 기존 09:00 타이머 유지.
                if (not _vi_observe_done) and dtime(8, 59) <= now.time() <= dtime(8, 59, 5):
                    _vi_observe_done = True
                    observe = {c for c, v in _last_prdy_ctrt.items() if v > 9.8 and c in set(codes)}
                    if observe:
                        _vi_observe_codes |= observe
                        try:
                            _mkstatus_sub_add(observe)
                        except Exception as e:
                            logger.warning(f"{ts_prefix()} [VI관찰-0859] H0STMKO0 관찰구독 실패: {e}")
                        obs_names = [f"{code_name_map.get(c, c)}({c})={_last_prdy_ctrt.get(c, 0.0):.1f}%" for c in observe]
                        _notify(
                            f"{ts_prefix()} [VI관찰-0859] {len(observe)}종목 H0STMKO0 관찰구독 시작 "
                            f"→ vi_cls_code 로그 (대상: {', '.join(obs_names)})", tele=True)
                    else:
                        logger.info(f"{ts_prefix()} [VI관찰-0859] prdy>9.8% 종목 없음 → 관찰 스킵")
                # 09:05 관찰 종료: 관찰용 H0STMKO0 해제(보유/keepalive/VI활성 제외) + 셋 비움
                if _vi_observe_codes and now.time() >= dtime(9, 5):
                    _keep = set(_mkt_keepalive_current.values()) | _vi_active_codes
                    _to_rm = _vi_observe_codes - _keep
                    if _to_rm:
                        try:
                            _mkstatus_sub_remove(_to_rm)
                        except Exception:
                            pass
                    logger.info(f"{ts_prefix()} [VI관찰-0859] 관찰 종료 → H0STMKO0 {len(_to_rm)}종목 해제")
                    _vi_observe_codes.clear()

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
                        with _lock:
                            per_sec_total = sum(_per_sec_counts.values())
                            since_active = sum(1 for c in codes if _since_save_counts.get(c, 0) > 0)
                        avg = _per_sec_sum_since_save // max(1, _per_sec_n_since_save)
                        _notify(
                            f"{ts_prefix()} (초당 최대 {_per_sec_max_since_save}건/평균 {avg}건/현재 {per_sec_total}건 수신), "
                            f"(버퍼 {_part_buffer_rows}건/수신 종목 {since_active}개/구독 종목 {total_n}개)"
                        )
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
                                    f"체결가 구독 해제, WS 연결은 유지 (체결통보 수신 지속)",
                                    tele=True,
                                )
                                # WS를 닫지 않음 — 체결가 구독은 _apply_subscriptions(desired={})에서 해제됨
                                # H0STCNI0 구독만 살아 있어 15:40 시간외종가 체결통보 수신 가능
                        # 15:31 타임아웃 → 미수신 종목 있어도 구독 해제 (WS는 유지)
                        elif (new_mode == RunMode.CLOSE_REAL
                                and not desired
                                and now.time() >= dtime(15, 31)):
                            if not _close_force_stopped:
                                _close_force_stopped = True
                                _notify(
                                    f"{ts_prefix()} 15:31 타임아웃: 종가 체결 "
                                    f"{len(_regular_real_seen)}/{len(codes)}종목 수신, "
                                    f"체결가 구독 해제, WS 연결은 유지 (체결통보 수신 지속)",
                                    tele=True,
                                )

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
                        _desired = _desired_subscription_map(now)
                        _sub_info = ", ".join(
                            f"{k.__name__}:{len(v)}" for k, v in _desired.items() if v
                        )
                        _notify(
                            f"{ts_prefix()} {int(idle_sec)}초 데이터 미수신 → 재구독 시도 ({_sub_info})"
                        )
                        _last_no_data_warn_ts = now_ts
                    if idle_sec >= NO_DATA_REBUILD_SEC and (now_ts - _last_rebuild_ts) >= NO_DATA_REBUILD_SEC:
                        if _stop_event.is_set():
                            continue  # 셧다운 중 재구독 시도 방지
                        _no_data_rebuild_count += 1
                        if _no_data_rebuild_count >= NO_DATA_RECONNECT_COUNT:
                            # 재구독 N회 연속 실패 → WSS 강제 재연결 (좀비 소켓 탈출)
                            logger.warning(
                                f"{ts_prefix()} [ws] 재구독 {_no_data_rebuild_count}회 연속 실패"
                                f" → WSS 강제 재연결 (zombie socket 의심)"
                            )
                            _notify(
                                f"{ts_prefix()} [ws] 재구독 {_no_data_rebuild_count}회 실패 → WSS 강제 재연결",
                                tele=True,
                            )
                            _no_data_rebuild_count = 0
                            with _kws_lock:
                                if _active_kws is not None:
                                    _request_ws_close(_active_kws)
                                    _active_kws = None
                            _ws_rebuild_event.set()
                            _last_rebuild_ts = now_ts
                        else:
                            logger.warning(
                                f"{ts_prefix()} [ws] no data for {int(idle_sec)}s"
                                f" -> resubscribe ({_no_data_rebuild_count}/{NO_DATA_RECONNECT_COUNT})"
                            )
                            with _kws_lock:
                                if _active_kws is not None:
                                    _apply_subscriptions(_active_kws, _desired_subscription_map(now), force=True)
                            _last_rebuild_ts = now_ts
            # ── 전략 모듈 핫스왑 체크 (0.5초 간격) ──
            _check_strategy_swap()
            # ── 외부 주문 주입 체크 (0.5초 간격) ──
            _check_external_orders()
            # ── 단일가 관찰 타임아웃 (15초 경과 시 확정) ──
            if _single_price_observe:
                _obs_now = time.time()
                for _obs_code in list(_single_price_observe.keys()):
                    _obs = _single_price_observe[_obs_code]
                    _obs_elapsed = _obs_now - _obs["first_ts"]
                    if _obs_elapsed >= 15:
                        _single_price_observe.pop(_obs_code)
                        _single_price_codes[_obs_code] = _obs["stat"]
                        _obs_name = code_name_map.get(_obs_code, _obs_code)
                        _obs_desc = "57(관리종목)" if _obs["stat"] == "57" else "59(단기과열)"
                        _obs_msg = f"{ts_prefix()} [KRX조회] 30분 단일가 확정 {_obs_name}({_obs_code}) {_obs_desc} ({_obs['tick_count']}틱/{_obs_elapsed:.0f}초 관찰)"
                        logger.info(_obs_msg)
                        _notify(_obs_msg, tele=True)
        except Exception as e:
            logger.error(f"{ts_prefix()} [scheduler] unexpected error: {e}")
            logger.error(traceback.format_exc())
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
_no_data_rebuild_count = 0   # 재구독 연속 실패 횟수 (데이터 수신 시 리셋)
# [260526] _trigger_ws_rebuild 연속 timeout 카운터 (성공 시 0 리셋).
# 임계 초과 시 os._exit(2) 로 runner 재시작 유도 (rebuild 무한루프 방지).
_rebuild_consecutive_fail_count = 0
_REBUILD_FAIL_EXIT_LIMIT = 5

# 직전 parquet 저장 이후 초당 수신건수 통계 (최대/평균/현재 표기용)
_per_sec_max_since_save = 0
_per_sec_sum_since_save = 0
_per_sec_n_since_save = 0


def _reset_per_sec_stats() -> None:
    global _per_sec_max_since_save, _per_sec_sum_since_save, _per_sec_n_since_save
    _per_sec_max_since_save = 0
    _per_sec_sum_since_save = 0
    _per_sec_n_since_save = 0

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
    global _per_sec_max_since_save, _per_sec_sum_since_save, _per_sec_n_since_save
    prefix = _fmt_now_prefix()
    with _lock:
        per_sec_total = sum(_per_sec_counts.values())
        since_active = sum(1 for c in codes if _since_save_counts.get(c, 0) > 0)
        total_codes = len(codes)

    # 직전 저장 이후 누적 통계 갱신
    if per_sec_total > _per_sec_max_since_save:
        _per_sec_max_since_save = per_sec_total
    _per_sec_sum_since_save += per_sec_total
    _per_sec_n_since_save += 1
    avg = _per_sec_sum_since_save // max(1, _per_sec_n_since_save)

    line = (
        f"{prefix} (초당 최대 {_per_sec_max_since_save}건/평균 {avg}건/현재 {per_sec_total}건 수신), "
        f"(버퍼 {_part_buffer_rows}건/수신 종목 {since_active}개/구독 종목 {total_codes}개)"
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
    sys.stdout.write(f"=> [{LOG_ID}] [save] {rows} rows save done\n")
    sys.stdout.flush()
    # 로그: 마지막 수신통계와 save 정보를 한 줄로 기록 (스크립트 로그에서도 수신상태 추적 가능)
    if _last_status_line:
        logger.info(f"{_last_status_line} => [save] {rows} rows save done")
    else:
        logger.info(f"{_fmt_now_prefix()} [save] {rows} rows save done")

## Phase 3-7: 수신건수 디버그 로그 제거 (광동(0/1255) 등)
def _format_full_entries() -> list[str]:
    return []  # 비활성화

def _log_full_progress():
    pass  # 비활성화

# =============================================================================
# 버퍼/Writer (flush는 워커에서만)
# =============================================================================
_lock = threading.RLock()
_stop_event = threading.Event()
_ingest_queue: "queue.Queue[tuple[pl.DataFrame, str, str, str | None, str]]" = queue.Queue()

_part_buffer: list[pl.DataFrame] = []         # flush 전 임시 버퍼 (1000행 또는 1분마다 part로 저장)
_part_buffer_lock = threading.RLock()          # flush와 append 동시 처리용
_save_lock = threading.Lock()                  # _flush_part_buffer 동시 호출 방지
_part_buffer_rows: int = 0                      # 버퍼 내 행 수 (표시용)
_part_seq: int = 0                              # 당일 part 파일 순번 (파일명 중복 방지)

# ── 종목별 기술적 지표 인메모리 상태 ──
# EMA: {code: {period: float|None}}  – EMA 누적값 (None이면 첫 틱에 가격으로 초기화)
_ema_state: dict[str, dict[int, float | None]] = {
    c: {n: None for n in INDICATOR_MA_LINE} for c in codes
}
# BB용 raw price list: {code: list} – 최대 _IND_MAX_WINDOW개 유지, deque 대신 list로 복사 없이 직접 계산
_price_buf: dict[str, list] = {
    c: [] for c in codes
}
# BB incremental 계산용 캐시: sum, sum_of_squares (슬라이딩 윈도우)
_bb_sum: dict[str, float] = {}
_bb_sq_sum: dict[str, float] = {}


_last_recv_ts: dict[str, float] = {c: 0.0 for c in codes}
_code_added_ts: dict[str, float] = {c: time.time() for c in codes}  # 종목별 추가 시점 (watchdog 3분 기준)
_last_summary_ts = time.time()

# ── ingest_loop 처리 지연 측정 (30s 주기 1회 로그) ──
_ingest_lag_max_ms: float = 0.0
_ingest_lag_sum_ms: float = 0.0
_ingest_lag_count: int = 0
_ingest_lag_last_print_ts: float = 0.0

# ── _calc_indicators 자체 소요시간 측정 (60s 주기, 토글로 끌 수 있음) ──
_ind_calc_t_sum: float = 0.0
_ind_calc_t_max: float = 0.0
_ind_calc_t_n: int = 0
_ind_calc_last_print_ts: float = 0.0
_ind_calc_measure_enabled: bool = True   # 사용자가 직접 False 로 끌 수 있음

# ── 장중 미수신 감시 상태 ──
_last_stale_check_ts: dict[str, float] = {}  # 종목별 마지막 REST 상태확인 시각
_halted_codes: set[str] = set()               # 거래정지 확인된 종목
_recent_tick_ts: dict[str, deque] = {}        # 종목별 최근 5개 틱 타임스탬프 (갑자기 끊김 감지용)
_vi_trigger_info: dict[str, dict] = {}        # VI REST 조회로 확인된 발동 정보 {code: {vi_time, vi_cls_code}}
_vi_active_codes: set[str] = set()            # VI 발동 확인된 종목
_last_wss_recv_ts: dict[str, float] = {}     # WSS 전용 수신 시각 (REST 제외)
_vi_cls_cache: dict[str, str] = {}            # code → 마지막 vi_cls_code (H0STMKO0)
_vi_exp_sub_ts: dict[str, float] = {}         # VI 종목 예상체결가 구독 시작 시각
_vi_stnd_prc: dict[str, float] = {}           # 종목별 VI기준가 (실시간체결 VI_STND_PRC)
_vi_trigger_count: dict[str, int] = {}        # 종목별 VI 발동 횟수 (당일 누적)
_vi_history: list[dict] = []                  # VI 발동 이력 [{code, name, vi_type, stnd_prc, price, time}, ...]
_vi_start_ts: dict[str, str] = {}             # code → VI 발동 ISO timestamp
_vi_end_ts: dict[str, str] = {}               # code → VI 해제 ISO timestamp
# 장운영 전체 이벤트 (CB/사이드카)
_last_mkop_event: dict[str, str] = {}         # code → 마지막 mkop_cls_code
_market_event: dict[str, str] = {}            # code → 장운영 이벤트명 (VI/사이드카 등)
_market_wide_event: dict[str, str] = {}       # "KOSPI"/"KOSDAQ" → CB/사이드카 이벤트명
_market_wide_mkop: dict[str, str] = {}        # "KOSPI"/"KOSDAQ" → 마지막 mkop_cls_code
_market_wide_start_ts: dict[str, str] = {}    # "KOSPI"/"KOSDAQ" → 발동 ISO timestamp
_code_market_map: dict[str, str] = {}         # code → "KOSPI"/"KOSDAQ"
_last_cb_replay_ts: dict[str, float] = {}     # CB 중 마지막 복제 가격 저장 시각 (5초 간격)
# H0STMKO0 keep-alive 순환 (마켓당 1개 유지)
_MKT_KEEPALIVE_INITIAL = {
    "KOSPI":  "005930",  # 삼성전자
    "KOSDAQ": "293490",  # 카카오게임즈
}
_mkt_keepalive_current: dict[str, str] = {}   # market → 현재 keep-alive 중인 code
# MKOP_CLS_CODE 정의 (H0STMKO0 문서)
_MKOP_SIDECAR_CODES = {"187", "388", "397", "398"}
_MKOP_CIRCUIT_CODES = {"174", "175", "182", "184", "185"}
_MKOP_EVENT_NAMES = {
    "187": "사이드카발동", "388": "사이드카해제",
    "397": "사이드카매수발동", "398": "사이드카매수해제",
    "174": "서킷브레이크발동", "175": "서킷브레이크해제",
    "182": "서킷브레이크장종동시마감", "184": "서킷브레이크개시", "185": "서킷브레이크해제",
    "164": "시장임시정지",
}
_CB_ACTIVE_EVENTS = {"서킷브레이크발동", "서킷브레이크개시", "서킷브레이크동시호가"}
_rest_fail_backoff: dict[str, int] = {}       # REST 500 에러 연속 횟수 (백오프용)
_price_block_until: dict[str, float] = {}     # [v_1] code → TTL 만료 unix_ts (500 에러 3회+ 종목 30분 조회 차단)
PRICE_BLOCK_FAIL_THRESHOLD = 3                # 500 에러 N회 연속 시 차단 시작
PRICE_BLOCK_TTL_SEC = 1800                    # 차단 유지 시간 (30분)
_single_price_codes: dict[str, str] = {}       # code → iscd_stat ("57" or "59") 30분 단일가 매매 종목
_single_price_pending: dict[str, str] = {}     # code → iscd_stat, 틱 확인 전 대기 종목
_single_price_observe: dict[str, dict] = {}    # code → {"stat", "first_ts", "tick_count"} 정시 윈도우 관찰 중


def _check_iscd_stat_krx(target_codes: list[str]) -> None:
    """KRX_code_batch로 종목의 iscd_stat_cls_code를 조회하여 _single_price_pending 갱신.
    57=관리종목, 59=단기과열 → 30분 단일가 후보 (틱 수신 후 확정)."""
    if not target_codes:
        return
    try:
        status_map = KRX_code_batch(target_codes)
        for code, stat in status_map.items():
            if stat in ("57", "59"):
                if code not in _single_price_codes and code not in _single_price_pending:
                    _single_price_pending[code] = stat
                    name = code_name_map.get(code, code)
                    desc = "57(관리종목)" if stat == "57" else "59(단기과열)"
                    logger.info(f"[KRX조회] {name}({code}) iscd_stat={stat} ({desc}) → 30분 단일가 후보 (틱 확인 대기)")
    except Exception as e:
        logger.warning(f"[KRX조회] iscd_stat 조회 실패: {e}")


# [v_1] REST 500 진단용 iscd_stat 설명표 (KRX DB 기준)
_KRX_ISCD_STAT_DESC = {
    "00": "정상", "51": "투자유의", "52": "투자경고", "53": "투자위험예고",
    "54": "투자위험", "55": "투자주의", "57": "관리종목", "58": "정리매매",
    "59": "단기과열", "60": "매매거래정지", "61": "거래정지", "62": "투자주의환기종목",
    "99": "상장폐지",
}


def _diagnose_price_500(code: str, name: str) -> dict:
    """
    REST 500 에러 3회 도달 시 종목 상태 진단.
    KRX_code_batch 로 iscd_stat 조회 → 상태별 적절한 조치.

    Returns: {
        "iscd_stat": str | None,   # KRX 상태 코드
        "desc": str,                # 한글 설명
        "action": str,              # "block_30min" | "single_price" | "halt_remove" | "temp_block"
        "reason": str,
    }
    """
    result = {"iscd_stat": None, "desc": "조회실패", "action": "block_30min", "reason": "KRX 조회 실패 → 보수적 차단"}
    try:
        status_map = KRX_code_batch([code])
        stat = status_map.get(code) if status_map else None
        if stat is None:
            result["reason"] = "KRX 응답에 종목 없음 → 상장폐지 의심, 30분 차단"
            return result
        result["iscd_stat"] = stat
        result["desc"] = _KRX_ISCD_STAT_DESC.get(stat, f"미상({stat})")

        if stat == "00":
            result["action"] = "temp_block"
            result["reason"] = "KRX상 정상 → KIS 서버 일시 장애 추정, 30분 차단 후 재시도"
        elif stat in ("57", "59"):
            # 30분 단일가 매매 대상
            if code not in _single_price_codes and code not in _single_price_pending:
                _single_price_pending[code] = stat
            result["action"] = "single_price"
            result["reason"] = f"{result['desc']} → 30분 단일가 매매 워크플로로 이관 (차단 없음)"
        elif stat in ("58", "60", "61", "99"):
            _halted_codes.add(code)
            result["action"] = "halt_remove"
            result["reason"] = f"{result['desc']} → 거래불가, _halted_codes 등록 + 완전 차단"
        else:
            # 투자유의/경고 등 — 거래는 가능하나 주의 필요
            result["action"] = "block_30min"
            result["reason"] = f"{result['desc']} → 보수적 30분 차단"
        return result
    except Exception as e:
        result["reason"] = f"KRX 조회 예외: {e} → 보수적 30분 차단"
        return result


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


# ── 다중 계좌 순회 인프라 (Phase 4-4) ─────────────────────────────────────────
from kis_utils import ConfigProxy, _detect_config_version, get_user_config, get_account_config  # noqa: E402


def _iter_enabled_accounts(trade_only: bool = False) -> list[dict]:
    """V2 config의 모든 활성 계좌를 순회하여 주문에 필요한 정보 반환.

    반환 리스트 각 항목:
      {
        "account_id": "main" | "syw_2",
        "alias": "a1" | "a2",
        "appkey": ..., "appsecret": ..., "cano": ..., "acnt_prdt_cd": ...,
        "exclude_cash": ..., "base_url": ...,
        "token_cache_path": ..., "trade_enabled": bool,
      }
    trade_only=True 시 trade_enabled=False인 계좌 제외 (잔고조회/매수/매도 전용).
    V1 config인 경우 메인 계좌 1개만 반환.
    """
    cfg = _read_cfg()
    if not cfg:
        return []
    raw = cfg.get_raw() if isinstance(cfg, ConfigProxy) else cfg

    # V1 → 메인 1건
    if _detect_config_version(raw) < 2:
        appkey = cfg.get("appkey", "")
        if not appkey:
            return []
        return [{
            "account_id": "main", "alias": "a1",
            "appkey": appkey,
            "appsecret": cfg.get("appsecret", ""),
            "cano": cfg.get("cano", ""),
            "acnt_prdt_cd": cfg.get("acnt_prdt_cd", "01"),
            "exclude_cash": cfg.get("exclude_cash", "0"),
            "max_invest": cfg.get("max_invest", "0"),
            "base_url": cfg.get("base_url") or DEFAULT_BASE_URL,
            "token_cache_path": str(SCRIPT_DIR / "kis_token_main.json"),
        }]

    # V2 → 모든 사용자의 모든 계좌
    accounts: list[dict] = []
    alias_idx = 0
    for uid, user in raw.get("users", {}).items():
        base_url = raw.get("base_url") or DEFAULT_BASE_URL
        for aid, acfg in user.get("accounts", {}).items():
            appkey = acfg.get("appkey", "")
            cano = acfg.get("cano", "")
            if not appkey or not cano:
                continue
            if acfg.get("enabled") is False:
                continue
            te = acfg.get("trade_enabled", True)
            if trade_only and te is False:
                continue
            alias_idx += 1
            token_name = "kis_token_main.json" if aid == "main" else f"kis_token_{aid}.json"
            accounts.append({
                "account_id": aid,
                "alias": acfg.get("cano_alias") or f"a{alias_idx}",
                "appkey": appkey,
                "appsecret": acfg.get("appsecret", ""),
                "cano": cano,
                "acnt_prdt_cd": acfg.get("acnt_prdt_cd", "") or "01",
                "exclude_cash": acfg.get("exclude_cash", "0"),
                "max_invest": acfg.get("max_invest", "0"),
                "base_url": base_url,
                "token_cache_path": str(SCRIPT_DIR / token_name),
                "trade_enabled": te,
            })
    # cano → alias 매핑 갱신 (체결통보 시 계좌 식별용)
    for a in accounts:
        _cano_alias_map[a["cano"]] = a["alias"]
    return accounts


def _init_account_client(acct: dict) -> KisClient:
    """계좌 정보 dict로 KisClient 생성 + 토큰 확보."""
    client = KisClient(KisConfig(
        appkey=acct["appkey"],
        appsecret=acct["appsecret"],
        base_url=acct.get("base_url") or DEFAULT_BASE_URL,
        custtype=acct.get("custtype") or "P",
        market_div=acct.get("market_div") or "J",
        token_cache_path=acct.get("token_cache_path", ""),
    ))
    client.ensure_token()
    return client


def _get_account_available_cash(client: KisClient, acct: dict) -> float:
    """계좌의 투자가능금액 조회.
    max_invest 설정 시: min(총평가-exclude, max_invest) - 보유평가
    미설정 시: 주문가능금액 - exclude_cash (기존방식)
    """
    cano = acct["cano"]
    acnt = acct.get("acnt_prdt_cd", "01") or "01"
    exclude = float(str(acct.get("exclude_cash", "0") or 0).replace("_", "") or 0)
    max_inv = float(str(acct.get("max_invest", "0") or 0).replace("_", "") or 0)
    try:
        hold_rows, out2, _, _, _ = _get_balance_page(client, cano, acnt, "TTTC8434R")
        items = [out2] if isinstance(out2, dict) else (out2 if isinstance(out2, list) else [])
        if not items:
            return 0.0
        ord_cash = float(str(items[0].get("prvs_rcdl_excc_amt", 0) or 0).replace(",", "") or 0)

        if max_inv > 0:
            tot_eval = float(str(items[0].get("tot_evlu_amt", 0) or 0).replace(",", "") or 0)
            stock_eval = sum(
                float(str(r.get("evlu_amt", 0) or 0).replace(",", "") or 0)
                for r in (hold_rows or [])
                if int(float(str(r.get("hldg_qty", 0) or 0).replace(",", "") or 0)) > 0
            )
            budget = min(tot_eval - exclude, max_inv)
            investable = max(0.0, budget - stock_eval)
            return min(investable, ord_cash)
        else:
            return max(0.0, ord_cash - exclude)
    except Exception as e:
        logger.warning(f"[multi-acct] {acct['account_id']} 잔고조회 실패: {e}")
    return 0.0


def _resolve_target_accounts(target, all_accounts: list[dict]) -> list[dict]:
    """external_orders의 target 필드를 실제 계좌 리스트로 변환.

    target:
      "all"          → 전체 계좌
      "a1"           → alias 매칭
      ["a1", "a2"]   → 다중 alias 매칭
      "main"         → account_id 매칭
    """
    if not all_accounts:
        return []
    if target is None or target == "all" or target == "":
        return all_accounts
    if isinstance(target, str):
        target = [target]
    if isinstance(target, list):
        matched = []
        for t in target:
            t = str(t).strip()
            for a in all_accounts:
                if a["alias"] == t or a["account_id"] == t:
                    if a not in matched:
                        matched.append(a)
        if not matched:
            logger.warning(f"{ts_prefix()} [외부주문] target={target} 매칭 실패 → 주문 건너뜀")
        return matched
    return all_accounts


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
        "bidp1": output.get("bidp1") or stck_prpr,   # REST에 bidp1 없으면 현재가 대체
        "acml_vol": acml_vol,
        "stck_mxpr": output.get("stck_mxpr"),
        "stck_llam": output.get("stck_llam"),
        # ── 매도 판단에 필요한 필드 추가 ──
        "stck_oprc": output.get("stck_oprc"),
        "stck_hgpr": output.get("stck_hgpr"),
        "wghn_avrg_stck_prc": output.get("wghn_avrg_stck_prc"),
        "prdy_ctrt": output.get("prdy_ctrt"),
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

def _vi_exp_sub_switch(code: str) -> None:
    """VI 발동: 실시간체결가 → 예상체결가 swap (단일 main WSS).
    1) ccnl_krx 해제, 2) exp_ccnl_krx 구독, 3) H0STMKO0 동적 추가.
    """
    if code in _vi_exp_sub_ts:
        return  # 이미 전환됨
    _vi_exp_sub_ts[code] = time.time()
    _vi_active_codes.add(code)
    name = code_name_map.get(code, code)
    with _kws_lock:
        if _active_kws is not None:
            try:
                _send_subscribe(_active_kws, ccnl_krx, [code], "2")  # noqa: F405  실시간 해제
                _send_subscribe(_active_kws, exp_ccnl_krx, [code], "1")  # noqa: F405  예상체결 구독
                logger.info(f"{ts_prefix()} [VI전환] {name}({code}) ccnl→exp swap 완료")
            except Exception as e:
                logger.warning(f"{ts_prefix()} [VI전환] {name}({code}) swap 실패: {e}")
    try:
        _mkstatus_sub_add({code})
    except Exception as e:
        logger.warning(f"{ts_prefix()} [VI전환] H0STMKO0 추가 실패 {code}: {e}")


def _vi_exp_sub_restore(code: str) -> None:
    """VI 해제: 예상체결가 → 실시간체결가 복귀 + H0STMKO0 keep-alive 순환 (단일 main WSS)."""
    _vi_exp_sub_ts.pop(code, None)
    _vi_active_codes.discard(code)
    _vi_exp_last_notify.pop(code, None)
    name = code_name_map.get(code, code)
    with _kws_lock:
        if _active_kws is not None:
            try:
                _send_subscribe(_active_kws, exp_ccnl_krx, [code], "2")  # noqa: F405  예상체결 해제
                _send_subscribe(_active_kws, ccnl_krx, [code], "1")      # noqa: F405  실시간 복귀
                logger.info(f"{ts_prefix()} [VI복구] {name}({code}) exp→ccnl 복귀 완료")
            except Exception as e:
                logger.warning(f"{ts_prefix()} [VI복구] {name}({code}) 복귀 실패: {e}")
    # ── H0STMKO0 keep-alive 순환 (마켓당 1개 유지) ──
    market = _code_market_map.get(code, "")
    if market:
        prev_keepalive = _mkt_keepalive_current.get(market, "")
        if prev_keepalive != code:
            _mkt_keepalive_current[market] = code
            held_codes = set()
            try:
                with _str1_sell_state_lock:
                    held_codes = {c for c, st in _str1_sell_state.items() if not st.get("sold")}
            except Exception:
                pass
            if (prev_keepalive
                and prev_keepalive not in _vi_active_codes
                and prev_keepalive not in held_codes
                and prev_keepalive != code):
                try:
                    _mkstatus_sub_remove({prev_keepalive})
                    logger.info(f"{ts_prefix()} [VI복구] H0STMKO0 keep-alive 순환: {prev_keepalive}→{code} ({market})")
                except Exception as e:
                    logger.warning(f"{ts_prefix()} [VI복구] 이전 keep-alive 해제 실패 {prev_keepalive}: {e}")


# 하위 호환 alias (기존 호출부 유지)
_vi_exp_sub_add = _vi_exp_sub_switch
_vi_exp_sub_unsub = _vi_exp_sub_restore


# ── VI 발동 중 예상체결가 모니터링 ──
_vi_exp_last_notify: dict[str, float] = {}  # 종목별 마지막 알림 시각 (초단위 스팸 방지)
_VI_EXP_NOTIFY_INTERVAL = 10.0              # 같은 종목 알림 최소 간격(초)


def _monitor_vi_exp_price(result, col_map: dict, code_col: str) -> None:
    """
    VI 발동 종목의 예상체결가 수신 시 가격 변동 모니터링.
    VI기준가 대비 상승/하락 방향과 등락률을 알림.
    """
    if not _vi_active_codes or not code_col:
        return

    pr_col = col_map.get("stck_prpr") or col_map.get("antc_prce")
    prdy_col = col_map.get("prdy_ctrt")
    if not pr_col:
        return

    now_ts = time.time()
    tmp = result.with_columns(
        pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
    )

    for df_code in tmp.partition_by(code_col, maintain_order=True):
        code = str(df_code[code_col][0])
        if code not in _vi_active_codes:
            continue

        # 스팸 방지: 일정 간격 이내면 스킵
        last_t = _vi_exp_last_notify.get(code, 0.0)
        if (now_ts - last_t) < _VI_EXP_NOTIFY_INTERVAL:
            continue

        try:
            antc_prc = float(str(df_code[pr_col][-1]).replace(",", "") or 0)
        except (ValueError, TypeError):
            continue
        if antc_prc <= 0:
            continue

        prdy_rt = 0.0
        if prdy_col:
            try:
                prdy_rt = float(str(df_code[prdy_col][-1]).replace(",", "") or 0)
            except (ValueError, TypeError):
                pass

        stnd_prc = _vi_stnd_prc.get(code, 0.0)
        name = code_name_map.get(code, code)
        cnt = _vi_trigger_count.get(code, 0)

        # VI기준가 대비 변동률 계산
        if stnd_prc > 0:
            vi_diff_pct = (antc_prc - stnd_prc) / stnd_prc * 100
            direction = "상승" if vi_diff_pct > 0 else "하락"
            msg = (
                f"{ts_prefix()} [VI모니터] {name}({code}) 예상체결가={antc_prc:,.0f}"
                f"  VI기준가={stnd_prc:,.0f}({vi_diff_pct:+.2f}% {direction})"
                f"  등락률={prdy_rt:+.2f}%  ({cnt}회차VI)"
            )
        else:
            msg = (
                f"{ts_prefix()} [VI모니터] {name}({code}) 예상체결가={antc_prc:,.0f}"
                f"  등락률={prdy_rt:+.2f}%  ({cnt}회차VI)"
            )
        logger.info(msg)
        _vi_exp_last_notify[code] = now_ts

        # ── VI 매수/취소 판단 (예상체결가 vs 현재가) ──
        cur_prc = _last_stck_prpr.get(code, 0.0)
        if code in _vi_buy_pending:
            _try_vi_buy_cancel(code, name, antc_prc, cur_prc)
        else:
            _try_vi_buy(code, name, antc_prc, cur_prc)


# ── VI 발동 매수 (전략 판단: str1, 주문 실행: 여기) ─────────────────────────────
# VI 매수 주문이 나간 종목 추적 (취소 로직용) — code → ordno
_vi_buy_pending: dict[str, str] = {}

def _try_vi_buy(code: str, name: str, antc_prce: float, stck_prpr: float) -> None:
    """예상체결가 수신 시 VI 매수 시도 — 전략 판단은 str1에 위임."""
    if code in _vi_buy_pending:
        return  # 이미 주문 나간 상태
    already_holding = code in _vi_positions and not _vi_positions[code].get("sold")
    should_buy, reason = vi_buy_strategy(
        antc_prce, stck_prpr, VI_TRADE_MODE, already_holding,
    )
    if not should_buy:
        return
    try:
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        if not cano:
            logger.warning(f"[VI매수] config cano 없음 → {code} 매수 스킵")
            return
        client = _top_client or _init_top_client()
        buy_price = stck_prpr if stck_prpr > 0 else antc_prce
        limit_up = calc_limit_up_price(buy_price)
        # test_mode: 1주, run_mode: max_invest 기반 수량
        accts = _iter_enabled_accounts(trade_only=True)
        main_acct = accts[0] if accts else {}
        max_inv = float(str(main_acct.get("max_invest", "0") or 0).replace("_", "") or 0)
        if VI_TRADE_MODE == "test_mode":
            qty = 1
        else:
            try:
                vi_avail = _get_account_available_cash(client, main_acct)
            except Exception:
                vi_avail = max_inv
            qty = max(1, int(vi_avail / buy_price)) if vi_avail > 0 else 1
        j = _buy_order_cash(client, cano, acnt, "TTTC0802U", code, qty, limit_up, ord_dvsn="01")
        out = j.get("output") or {}
        ordno = str(out.get("ODNO") or out.get("odno") or "").strip()
        now_ts = datetime.now(KST)
        _vi_buy_pending[code] = ordno
        _vi_positions[code] = {
            "buy_price": buy_price, "buy_ts": now_ts,
            "qty": qty, "highest": buy_price, "sold": False,
            "name": name, "ordno": ordno,
        }
        with _str1_sell_state_lock:
            _str1_sell_state[code] = {
                "sold": False, "sell_reason": "", "qty": qty,
                "sell_price": 0.0, "sell_time": "", "ordno": "",
                "pnl": 0.0, "ret_pct": 0.0,
                "buy_price": buy_price, "name": name,
                "opening_call_auction_ordered": False, "balance_qty": qty,
                "source": "vi",
                "loss_below_count": 0,
            }
        _ensure_code_structs([code])
        with _lock:
            _base_codes.add(code)
        mode_label = "테스트(1주)" if VI_TRADE_MODE == "test_mode" else "정상"
        msg = (
            f"{ts_prefix()} [VI매수] {name}({code}) 상한가 매수주문"
            f"  수량={qty}  현재가={stck_prpr:,.0f}  예상체결가={antc_prce:,.0f}"
            f"  상한가={limit_up:,.0f}  주문번호={ordno}  모드={mode_label}"
        )
        _notify(msg, tele=True)
        logger.info(msg)
        _append_ledger(
            order_type="buy_order", code=code, name=name,
            buy_price=buy_price, order_qty=qty, reason=f"{reason}_주문",
            ord_no=ordno, stck_prpr=stck_prpr,
        )
    except Exception as e:
        logger.error(f"[VI매수] {name}({code}) 매수 실패: {e}")


def _try_vi_buy_cancel(code: str, name: str, antc_prce: float, stck_prpr: float) -> None:
    """예상체결가 모니터링 중 취소 조건 충족 시 VI 매수 주문 취소."""
    ordno = _vi_buy_pending.get(code)
    if not ordno:
        return
    should_cancel, reason = vi_should_cancel(antc_prce, stck_prpr)
    if not should_cancel:
        return
    try:
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        client = _top_client or _init_top_client()
        pos = _vi_positions.get(code, {})
        qty = pos.get("qty", 0)
        _cancel_order_generic(client, cano, acnt, ordno, code, qty, ord_dvsn="01")
        _vi_buy_pending.pop(code, None)
        _vi_positions.pop(code, None)
        with _str1_sell_state_lock:
            _str1_sell_state.pop(code, None)
        msg = (
            f"{ts_prefix()} [VI매수취소] {name}({code})"
            f"  예상체결가={antc_prce:,.0f} ≤ 현재가={stck_prpr:,.0f}"
            f"  주문번호={ordno}  사유={reason}"
        )
        _notify(msg, tele=True)
        logger.info(msg)
        _append_ledger(
            order_type="buy_cancel", code=code, name=name,
            buy_price=stck_prpr, order_qty=qty, reason=reason,
            ord_no=ordno, stck_prpr=stck_prpr,
        )
    except Exception as e:
        logger.error(f"[VI매수취소] {name}({code}) 취소 실패: {e}")


# =============================================================================
# [v_1 신규] 상한가 근접 매수 — 헬퍼 / 매수 / 청산
# =============================================================================

def _uplimit_state_path(yymmdd: str | None = None):
    """상태 파일 경로: data/fetch_top_list/uplimit_buy_state_{yymmdd}.json."""
    if yymmdd is None:
        yymmdd = datetime.now(KST).strftime("%y%m%d")
    return TOP_RANK_OUT_DIR / f"uplimit_buy_state_{yymmdd}.json"


def _save_uplimit_state(force: bool = False) -> None:
    """uplimit 전략 상태를 JSON 으로 저장 (throttled 5s)."""
    global _uplimit_state_last_save_ts
    now_ts = time.time()
    if not force and (now_ts - _uplimit_state_last_save_ts) < 5.0:
        return
    try:
        path = _uplimit_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with _uplimit_state_lock:
            data = {
                "date": datetime.now(KST).strftime("%y%m%d"),
                "updated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                "positions": {
                    c: {k: (v.isoformat() if isinstance(v, datetime) else v)
                        for k, v in p.items()}
                    for c, p in _uplimit_positions.items()
                },
                "buy_pending": dict(_uplimit_buy_pending),
                "exit_pending": dict(_uplimit_exit_pending),
                "today_buy_count": _uplimit_today_buy_count,
                "blacklist": sorted(_uplimit_blacklist),
                # [260424] v4 overnight 홀드 포지션 (익일 매도 대상)
                "v4_holdover": {
                    c: {k: (v.isoformat() if isinstance(v, datetime) else v)
                        for k, v in h.items()}
                    for c, h in _uplimit_v4_holdover.items()
                },
                "v4_daily_bought": sorted(_uplimit_v4_daily_bought_codes),
            }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        _uplimit_state_last_save_ts = now_ts
    except Exception as e:
        logger.warning(f"{ts_prefix()} [uplimit] 상태 저장 실패: {e}")


def _restore_uplimit_state_on_startup() -> None:
    """당일 uplimit_buy_state_*.json 을 메모리로 복원."""
    global _uplimit_today_buy_count
    try:
        path = _uplimit_state_path()
        if not path.exists():
            return
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        with _uplimit_state_lock:
            for c, p in (data.get("positions") or {}).items():
                # buy_ts 복원
                bt = p.get("buy_ts")
                if isinstance(bt, str):
                    try:
                        p["buy_ts"] = datetime.fromisoformat(bt)
                    except Exception:
                        p["buy_ts"] = datetime.now(KST)
                _uplimit_positions[str(c).zfill(6)] = p
            for c, o in (data.get("buy_pending") or {}).items():
                _uplimit_buy_pending[str(c).zfill(6)] = str(o)
            for c, o in (data.get("exit_pending") or {}).items():
                _uplimit_exit_pending[str(c).zfill(6)] = str(o)
            _uplimit_today_buy_count = int(data.get("today_buy_count") or 0)
            for c in (data.get("blacklist") or []):
                _uplimit_blacklist.add(str(c).zfill(6))
            # [260424] v4 holdover 복원 (어제 14:55 에 29.5%+ 이었던 종목)
            for c, h in (data.get("v4_holdover") or {}).items():
                bt = h.get("buy_ts")
                if isinstance(bt, str):
                    try:
                        h["buy_ts"] = datetime.fromisoformat(bt)
                    except Exception:
                        pass
                _uplimit_v4_holdover[str(c).zfill(6)] = h
            for c in (data.get("v4_daily_bought") or []):
                # 당일 중복매수 방지 플래그는 날짜 다르면 리셋
                _saved_date = data.get("date", "")
                if _saved_date == datetime.now(KST).strftime("%y%m%d"):
                    _uplimit_v4_daily_bought_codes.add(str(c).zfill(6))
        logger.info(
            f"{ts_prefix()} [uplimit] 상태 복원: positions={len(_uplimit_positions)}, "
            f"buy_pending={len(_uplimit_buy_pending)}, today_buy_count={_uplimit_today_buy_count}, "
            f"v4_holdover={len(_uplimit_v4_holdover)}, "
            f"daily_bought={len(_uplimit_v4_daily_bought_codes)}"
        )
        # [260427] v5 진단: startup 시 차단 후보 종목 명시
        if _uplimit_v4_daily_bought_codes:
            logger.info(
                f"{ts_prefix()} [v5_diag][startup] daily_bought 차단 종목: "
                f"{sorted(_uplimit_v4_daily_bought_codes)}"
            )
        if _uplimit_v4_holdover:
            for c, h in _uplimit_v4_holdover.items():
                logger.info(
                    f"{ts_prefix()} [v4_holdover복원] {h.get('name', c)}({c}) "
                    f"buy={int(h.get('buy_price', 0)):,} qty={h.get('qty')} "
                    f"hold_date={h.get('buy_date')}"
                )
    except Exception as e:
        logger.warning(f"{ts_prefix()} [uplimit] 상태 복원 실패: {e}")


# ── [260523] Strategy str2 상태 저장/복원 (str2 활성 시에만) ────────────────────
def _str2_state_path(yymmdd: str | None = None):
    """str2 상태 파일 경로: data/fetch_top_list/str2_state_{yymmdd}.json."""
    if yymmdd is None:
        yymmdd = datetime.now(KST).strftime("%y%m%d")
    return TOP_RANK_OUT_DIR / f"str2_state_{yymmdd}.json"


def _save_str2_state(force: bool = False) -> None:
    """_str2_state(Str2State) 를 JSON 으로 저장 (throttled 5s). str2 비활성 시 무동작."""
    global _str2_state_last_save_ts
    if not STR2_ENABLED:
        return
    now_ts = time.time()
    if not force and (now_ts - _str2_state_last_save_ts) < 5.0:
        return
    try:
        from dataclasses import asdict as _asdict
        path = _str2_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with _str2_state_lock:
            data = {
                "date": datetime.now(KST).strftime("%y%m%d"),
                "updated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                "state": {c: _asdict(st) for c, st in _str2_state.items()},
                "session_setup_done": sorted(_str2_session_setup_done),
            }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        _str2_state_last_save_ts = now_ts
    except Exception as e:
        logger.warning(f"{ts_prefix()} [str2] 상태 저장 실패: {e}")


def _restore_str2_state_on_startup() -> None:
    """당일 str2_state_*.json 을 메모리로 복원 (재시작 연속성). str2 비활성 시 무동작."""
    if not STR2_ENABLED:
        return
    try:
        path = _str2_state_path()
        if not path.exists():
            return
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if str(data.get("date", "")) != datetime.now(KST).strftime("%y%m%d"):
            return  # 당일 파일이 아니면 무시
        with _str2_state_lock:
            for c, d in (data.get("state") or {}).items():
                try:
                    _str2_state[c] = Str2State(**d)
                except Exception:
                    continue
            for c in (data.get("session_setup_done") or []):
                _str2_session_setup_done.add(c)
        n_pos = sum(1 for st in _str2_state.values() if st.position)
        logger.info(f"{ts_prefix()} [str2] 상태 복원: {len(_str2_state)}종목 (보유 {n_pos})")
    except Exception as e:
        logger.warning(f"{ts_prefix()} [str2] 상태 복원 실패: {e}")


def _precache_uplimit_signals() -> None:
    """장 시작 전 10일 변동성/전일 등락률/외국인 캐시 일괄 구축.
    감시 대상 = 현재 구독 중인 codes ∪ 전일 closing_buy_state.codes.
    """
    global _uplimit_precache_done
    if _uplimit_precache_done:
        return
    try:
        # [260427] Phase 5: 당일 외인 캐시 parquet → 메모리 적재 (있다면)
        _load_frgn_3d_cache_today()

        target_codes: set[str] = set()
        with _lock:
            target_codes.update(c.zfill(6) for c in codes)
        # 전일 closing_buy 후보 추가
        try:
            prev_state = _load_closing_buy_state()
            if prev_state:
                for c in prev_state.get("codes") or []:
                    target_codes.add(str(c).zfill(6))
        except Exception:
            pass
        if not target_codes:
            _uplimit_precache_done = True
            return

        logger.info(f"{ts_prefix()} [uplimit_precache] 대상 {len(target_codes)} 종목 사전 캐시 시작")

        # 10일 일봉 변동성 + 전일 등락률 — kis_1d parquet 재사용 시도
        try:
            import polars as _pl
            from pathlib import Path as _Path
            daily_path = _Path("/home/ubuntu/Stoc_Kis/data/fetch_daily_KRX_ohlcv/kis_1d_parquet")
            if daily_path.exists():
                # 최근 20일 일봉 (buffered) 로드
                import glob
                files = sorted(glob.glob(str(daily_path / "*.parquet")))[-20:]
                if files:
                    df = _pl.concat([_pl.read_parquet(f) for f in files])
                    if "code" in df.columns and "close" in df.columns:
                        import math
                        for code in target_codes:
                            sub = df.filter(_pl.col("code") == code).sort("date")
                            if sub.height < 11:
                                continue
                            closes = sub.tail(11).get_column("close").to_list()
                            if len(closes) < 11:
                                continue
                            rets = []
                            for i in range(1, len(closes)):
                                if closes[i-1] > 0 and closes[i] > 0:
                                    rets.append(math.log(closes[i] / closes[i-1]))
                            if len(rets) >= 5:
                                mean = sum(rets) / len(rets)
                                var = sum((x - mean) ** 2 for x in rets) / len(rets)
                                _uplimit_vola_10d[code] = math.sqrt(var)
                            # 전일 등락률
                            if len(closes) >= 2 and closes[-2] > 0:
                                _uplimit_prev_day_ctrt[code] = (closes[-1] / closes[-2] - 1) * 100
        except Exception as e:
            logger.warning(f"{ts_prefix()} [uplimit_precache] 일봉 기반 변동성 계산 실패: {e}")

        # [260427] 외인 일괄 fetch 제거 — v5 는 25%+ 도달 종목에만 외인 필터 적용.
        # 보유/전일 후보 등 일반 구독 종목은 어차피 F2(보유) 또는 25% 미만으로 차단.
        # 외인 데이터는 (a) parquet load (b) Top30 신규 추가 시 fetch (c) lazy fetch 로 충분.

        # [260427] v5 저거래 판별 캐시 — 전일 거래대금 일괄 적재
        try:
            _populate_prev_day_amount(list(target_codes))
        except Exception as _ee:
            logger.warning(f"{ts_prefix()} [v5_저거래캐시_초기] 실패: {_ee}")

        # [260427] Phase 5: 외인 캐시 parquet 저장 (precache 결과 누적분 모두)
        try:
            saved = _save_frgn_3d_cache_today()
            if saved > 0:
                logger.info(f"{ts_prefix()} [v5_frgn_save] {saved}종목 → 당일 parquet 저장")
        except Exception as _es:
            logger.warning(f"{ts_prefix()} [v5_frgn_save] 실패: {_es}")

        logger.info(
            f"{ts_prefix()} [uplimit_precache] 완료: "
            f"vola_10d={len(_uplimit_vola_10d)}, "
            f"prev_ctrt={len(_uplimit_prev_day_ctrt)}, "
            f"prev_amount={len(_uplimit_prev_day_amount)}, "
            f"frgn_3d={len(_uplimit_frgn_3d)}"
        )
        _uplimit_precache_done = True
    except Exception as e:
        logger.error(f"{ts_prefix()} [uplimit_precache] 실패: {e}")


def _precache_uplimit_for_new_codes(new_codes: list[str]) -> None:
    """[v_1] Top30 장중 동적 추가 종목에 대한 uplimit 필터용 캐시 구축.
    - vola_10d (10일 일봉 std) : kis_1d parquet 에서 즉시 계산
    - prev_day_prdy_ctrt       : 일봉 최근 2개 비교
    - frgn_3d_net              : REST 호출 (코드당 1회, 짧은 sleep 포함)
    이미 캐시된 코드는 skip.
    """
    if not new_codes:
        return
    target = [c.zfill(6) for c in new_codes
              if c.zfill(6) not in _uplimit_vola_10d]
    if not target:
        return

    # 일봉 parquet 기반 vola_10d + prev_day_ctrt
    try:
        import polars as _pl
        import glob
        import math
        from pathlib import Path as _Path
        daily_path = _Path("/home/ubuntu/Stoc_Kis/data/fetch_daily_KRX_ohlcv/kis_1d_parquet")
        if daily_path.exists():
            files = sorted(glob.glob(str(daily_path / "*.parquet")))[-20:]
            if files:
                df = _pl.concat([_pl.read_parquet(f) for f in files])
                if "code" in df.columns and "close" in df.columns:
                    for code in target:
                        sub = df.filter(_pl.col("code") == code).sort("date")
                        if sub.height < 11:
                            continue
                        closes = sub.tail(11).get_column("close").to_list()
                        if len(closes) < 11:
                            continue
                        rets = [math.log(closes[i]/closes[i-1])
                                for i in range(1, len(closes))
                                if closes[i-1] > 0 and closes[i] > 0]
                        if len(rets) >= 5:
                            mean = sum(rets)/len(rets)
                            var = sum((x-mean)**2 for x in rets)/len(rets)
                            _uplimit_vola_10d[code] = math.sqrt(var)
                        if len(closes) >= 2 and closes[-2] > 0:
                            _uplimit_prev_day_ctrt[code] = (closes[-1]/closes[-2] - 1) * 100
    except Exception as e:
        logger.warning(f"{ts_prefix()} [uplimit_precache_top] 일봉 계산 실패: {e}")

    # 외국인 3일 순매수 (REST, 비동기 제출로 블로킹 최소화)
    try:
        client = _top_client or _init_top_client()
        frgn_fetched = 0
        for code in target[:10]:  # 한 번에 최대 10개만
            if code in _uplimit_frgn_3d:
                continue
            try:
                v = fetch_frgn_3day_net(client, code)
                if v is not None:
                    _uplimit_frgn_3d[code] = v
                    frgn_fetched += 1
                time.sleep(0.05)
            except Exception:
                pass
        # [260427] Phase 5: 신규 fetch 결과 즉시 parquet 갱신
        if frgn_fetched > 0:
            try:
                _save_frgn_3d_cache_today()
            except Exception:
                pass
    except Exception:
        pass

    logger.info(
        f"{ts_prefix()} [uplimit_precache_top] Top30 신규 {len(target)}종목 캐시 "
        f"vola={sum(1 for c in target if c in _uplimit_vola_10d)}, "
        f"prev_ctrt={sum(1 for c in target if c in _uplimit_prev_day_ctrt)}"
    )

    # [260427] v5 저거래 판별: 전일 거래대금 캐시 (unified parquet)
    _populate_prev_day_amount(target)


def _frgn_3d_cache_path() -> "Path":
    """외인 3일 순매수 캐시 일별 parquet 경로."""
    today = datetime.now(KST).strftime("%y%m%d")
    cache_dir = SCRIPT_DIR / "data" / "frgn_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{today}.parquet"


def _load_frgn_3d_cache_today() -> int:
    """당일 외인 3일 순매수 parquet → _uplimit_frgn_3d 적재.
    Returns: 로드된 종목 수.
    """
    try:
        path = _frgn_3d_cache_path()
        if not path.exists():
            return 0
        df = pd.read_parquet(str(path))
        if df.empty or "code" not in df.columns or "frgn_3d_net" not in df.columns:
            return 0
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["frgn_3d_net"] = pd.to_numeric(df["frgn_3d_net"], errors="coerce")
        loaded = 0
        for c, v in zip(df["code"], df["frgn_3d_net"]):
            if pd.notna(v):
                _uplimit_frgn_3d[c] = float(v)
                loaded += 1
        logger.info(f"{ts_prefix()} [v5_frgn_load] {path.name} → {loaded}종목 메모리 적재")
        return loaded
    except Exception as e:
        logger.warning(f"{ts_prefix()} [v5_frgn_load] 실패: {e}")
        return 0


def _save_frgn_3d_cache_today() -> int:
    """_uplimit_frgn_3d → 당일 외인 parquet 저장 (덮어쓰기).
    Returns: 저장된 종목 수.
    """
    try:
        if not _uplimit_frgn_3d:
            return 0
        path = _frgn_3d_cache_path()
        now_iso = datetime.now(KST).strftime("%Y-%m-%dT%H:%M:%S")
        rows = [
            {"code": c, "frgn_3d_net": float(v), "fetched_ts": now_iso}
            for c, v in _uplimit_frgn_3d.items()
        ]
        df = pd.DataFrame(rows)
        df.to_parquet(str(path), index=False)
        return len(rows)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [v5_frgn_save] 실패: {e}")
        return 0


def _lazy_fetch_frgn_3d(code: str) -> float | None:
    """v5 매수 판정 시 캐시 miss 종목에 대해 외인 데이터 동기 fetch.
    종목당 1회만 시도 (_uplimit_frgn_3d_fetch_attempted 으로 추적).
    성공 시 메모리 캐시 + parquet 동시 갱신.
    """
    if code in _uplimit_frgn_3d:
        return _uplimit_frgn_3d[code]
    if code in _uplimit_frgn_3d_fetch_attempted:
        return None  # 이미 시도해서 실패함
    _uplimit_frgn_3d_fetch_attempted.add(code)
    try:
        client = _top_client or _init_top_client()
        v = fetch_frgn_3day_net(client, code)
        if v is not None:
            _uplimit_frgn_3d[code] = v
            try:
                _save_frgn_3d_cache_today()
            except Exception:
                pass
            logger.info(f"{ts_prefix()} [v5_frgn_lazy] {code} → {int(v):,} (캐시 miss → 동기 fetch)")
            return v
    except Exception as e:
        logger.debug(f"[v5_frgn_lazy] {code} fetch 실패: {e}")
    return None


def _populate_prev_day_amount(codes: list[str]) -> None:
    """unified 1d parquet 의 최근 날짜 'value' 컬럼을 _uplimit_prev_day_amount 에 적재."""
    targets = [c.zfill(6) for c in codes if c.zfill(6) not in _uplimit_prev_day_amount]
    if not targets:
        return
    try:
        path = SCRIPT_DIR / "data" / "1d_data" / "kis_1d_unified_parquet_DB.parquet"
        if not path.exists():
            return
        df = pd.read_parquet(str(path), columns=["date", "symbol", "value"])
        if df.empty or "value" not in df.columns:
            return
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)
        df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)
        latest = df["date"].max()
        sub = df[(df["date"] == latest) & (df["symbol"].isin(targets))]
        for sym, val in zip(sub["symbol"], sub["value"].astype(float)):
            _uplimit_prev_day_amount[sym] = val
    except Exception as e:
        logger.warning(f"{ts_prefix()} [v5_저거래캐시] 실패: {e}")


def _get_bb_lower_v5(code: str) -> float | None:
    """v5 BB 반등 트리거용 — 현재 캐시 기준 bb_lower 반환.
    BB 미형성 (price_buf 길이 < 200) 또는 캐시 미초기화 시 None.
    _calc_indicators 가 매 틱 _bb_sum/_bb_sq_sum 갱신하므로 직전 틱 기준값.
    """
    import math as _math
    buf = _price_buf.get(code)
    if buf is None or len(buf) < INDICATOR_BB_PERIOD:
        return None
    s = _bb_sum.get(code)
    sq = _bb_sq_sum.get(code)
    if s is None or sq is None:
        return None
    n = INDICATOR_BB_PERIOD
    mean = s / n
    var = max(0.0, sq / n - mean * mean)
    std = _math.sqrt(var)
    return mean - INDICATOR_BB_K * std


# =============================================================================
# [260523] Strategy str2 — 지표 갭 보강 (틱당 증분 계산)
#   서버는 ma3~ma2000 + bb_mid/upper/lower/width 를 _calc_indicators 가 baked 로
#   산출 중(회신 §2 (A)군). str2 라이브 갭은 stoch_k + bb_hi_max 2종.
#   ※ bb_hi/bb_lo/bb_hi_max 전부 동일 baked BB(ddof=0) 기반 — 회신 §2/§3 의 ddof=1
#     명시와 다르나, 서버 파리티 기준이 _calc_indicators 의 모집단분산(ddof=0)이므로
#     ddof=0 채택. 로컬 백테스트가 baked BB(ddof=0)를 그대로 써야 backtest=live 성립.
# =============================================================================
def _str2_compute_stoch_k(code: str) -> float:
    """stoch_k(raw %K, n=1500) = 100·(c−min)/(max−min).

    소스 = _price_buf[code] (= stck_prpr 누적). 회신 §2/§3 정본:
      · diff(max−min)==0 또는 warmup(데이터 1개 미만) → NaN
      · clip 0..100, M(100) 평활 미적용 (stoch_s 는 별도 컬럼이므로 여기선 미산출)
    ※ _price_buf 는 _IND_MAX_WINDOW(=max(MA,BB)=2000) 까지만 유지하므로 1500틱 확보됨.
    """
    buf = _price_buf.get(code)
    if not buf:
        return float("nan")
    window = buf[-STR2_STOCH_N:]
    if len(window) < 1:
        return float("nan")
    c = window[-1]
    lo = min(window)
    hi = max(window)
    diff = hi - lo
    if diff <= 0:
        return float("nan")
    k = 100.0 * (c - lo) / diff
    return max(0.0, min(100.0, k))


def _str2_update_bb_hi_max(code: str, bb_upper: float | None) -> float:
    """bb_hi_max = rolling_max(bb_upper, 600). baked bb_upper(ddof=0)를 매 틱 push.

    bb_upper 가 None(워밍업, buf<200)이면 push 하지 않고 현재까지의 max 반환.
    히스토리 없으면 NaN.
    """
    hist = _str2_bb_upper_hist.get(code)
    if hist is None:
        hist = deque(maxlen=600)
        _str2_bb_upper_hist[code] = hist
    if bb_upper is not None and bb_upper == bb_upper and bb_upper > 0:  # not NaN
        hist.append(bb_upper)
    if not hist:
        return float("nan")
    return max(hist)


def _str2_baked_bb(code: str) -> tuple[float | None, float | None]:
    """현재 baked bb_upper/bb_lower(ddof=0) 반환 (_calc_indicators 캐시 재사용).

    _get_bb_lower_v5 와 동일 소스(_bb_sum/_bb_sq_sum, 모집단분산). bb_hi=upper, bb_lo=lower.
    워밍업(buf<200 또는 캐시 미초기화) 시 (None, None).
    """
    buf = _price_buf.get(code)
    if buf is None or len(buf) < INDICATOR_BB_PERIOD:
        return None, None
    s = _bb_sum.get(code)
    sq = _bb_sq_sum.get(code)
    if s is None or sq is None:
        return None, None
    n = INDICATOR_BB_PERIOD
    mean = s / n
    var = max(0.0, sq / n - mean * mean)
    std = math.sqrt(var)
    return mean + INDICATOR_BB_K * std, mean - INDICATOR_BB_K * std


def _is_low_liquidity_v5(code: str, cur_acml_tr_pbmn: float = 0.0) -> tuple[bool, str]:
    """v5 저거래 판별.
    - 전일 거래대금(`_uplimit_prev_day_amount[code]`) 이 있으면 그것 기준
    - 없으면 현재 수신된 당일 acml_tr_pbmn 기준
    - 둘 다 없으면 판정 불가 → False (통과)
    """
    prev_amt = _uplimit_prev_day_amount.get(code, 0.0)
    if prev_amt > 0:
        if prev_amt < UPLIMIT_V5_LOW_LIQ_VALUE:
            return True, f"전일거래대금={int(prev_amt/1e8)}억<50억"
        return False, ""
    if cur_acml_tr_pbmn > 0:
        if cur_acml_tr_pbmn < UPLIMIT_V5_LOW_LIQ_VALUE:
            return True, f"당일acml={int(cur_acml_tr_pbmn/1e8)}억<50억"
        return False, ""
    return False, ""   # 판정 불가 시 통과 (보수적 차단 안 함)


def _update_uplimit_minute_vol(code: str, acml_vol: float, now_dt: datetime) -> None:
    """틱마다 분봉 경계 감지하여 직전 분의 거래량을 5분 버킷에 누적."""
    if acml_vol <= 0:
        return
    try:
        cur_min = int(now_dt.timestamp() // 60)
        prev_key, prev_vol = _uplimit_prev_min_vol.get(code, (cur_min, acml_vol))
        if cur_min != prev_key:
            # 분이 바뀜 → 직전 분 거래량 확정 (delta)
            delta = max(0.0, prev_vol - (_uplimit_prev_min_vol.get(code, (0, 0))[1] if False else 0.0))
            # 간단하게: 직전 분 '시작 acml_vol' 대비 현재 acml_vol 차 = 분봉 vol 근사
            # 정확도보다는 서지 감지가 목적
            bucket = _uplimit_day_vol_bucket.setdefault(code, [])
            bucket.append((prev_key, prev_vol))
            # 최근 6분만 유지
            if len(bucket) > 6:
                del bucket[:-6]
            _uplimit_prev_min_vol[code] = (cur_min, acml_vol)
        else:
            _uplimit_prev_min_vol[code] = (cur_min, acml_vol)
    except Exception:
        pass


def _calc_uplimit_last5m_avg_vol_per_min(code: str) -> float:
    """최근 5분 구간의 분당 평균 거래량 (당일 분봉 근사)."""
    bucket = _uplimit_day_vol_bucket.get(code, [])
    if len(bucket) < 2:
        return 0.0
    # 가장 최근 5개 분의 acml_vol 변화량 합 / 분 수
    recent = bucket[-6:]
    diffs = []
    for i in range(1, len(recent)):
        d = recent[i][1] - recent[i-1][1]
        if d > 0:
            diffs.append(d)
    if not diffs:
        return 0.0
    return sum(diffs) / len(diffs)


def _calc_uplimit_day_avg_vol_per_min(code: str, acml_vol: float, now_dt: datetime) -> float:
    """09:00 이후 경과 분 기준 분당 평균 누적 거래량."""
    open_dt = now_dt.replace(hour=9, minute=0, second=0, microsecond=0)
    mins = max(1.0, (now_dt - open_dt).total_seconds() / 60.0)
    return acml_vol / mins


UPLIMIT_VP_FAIL_TTL_SEC = 10  # VP 조회 실패 시 재시도 억제 기간


def _get_uplimit_volume_power(client, code: str) -> float | None:
    """체결강도 조회 (성공 30초 / 실패 10초 TTL 캐시 + 500 block 존중).

    실패도 캐시하여 매 틱 재호출로 인한 inquire_price retry 루프 폭주를 방지.
    """
    # 500 차단 중인 종목은 inquire_price 자체가 실패할 것이므로 skip
    if _price_block_until.get(code, 0.0) > time.time():
        return None
    cached = _uplimit_volume_power.get(code)
    if cached is not None:
        cached_val, cached_ts = cached
        age = time.time() - cached_ts
        ttl = UPLIMIT_VP_TTL_SEC if cached_val is not None else UPLIMIT_VP_FAIL_TTL_SEC
        if age < ttl:
            return cached_val
    try:
        v = fetch_volume_power(client, code)
    except Exception:
        v = None
    _uplimit_volume_power[code] = (v, time.time())
    return v


def _try_uplimit_buy(
    code: str,
    name: str,
    prdy_ctrt: float,
    stck_prpr: float,
    stck_oprc: float,
    prev_close: float,
    acml_vol: float,
    bid_sum_top5: float,
    ask_sum_top5: float,
    now_dt: datetime,
    prev_ctrt: float = 0.0,   # [260423] 28% edge-trigger 용 직전 틱 등락률
) -> None:
    """[260423] 28% 상향 돌파 순간 매수 판단 → 13필터 AND 통과 시 주문.
    시드 분산: UPLIMIT_DIVERSIFY_N(3) 종목 한도, 각 매수금액 = INIT_CASH / N.
    """
    global _uplimit_today_buy_count
    if not UPLIMIT_BUY_ENABLED:
        return
    if code in _uplimit_positions or code in _uplimit_buy_pending:
        return
    with _uplimit_state_lock:
        if code in _uplimit_blacklist:
            return
        # [260423] 분산 한도 체크 — 보유 + pending 합산
        active_n = len(_uplimit_positions) + len(_uplimit_buy_pending)
        if active_n >= UPLIMIT_DIVERSIFY_N:
            return
        if _uplimit_today_buy_count >= UPLIMIT_MAX_DAILY_BUYS:
            return

    if code in _vi_positions and not _vi_positions[code].get("sold"):
        return

    client = _top_client or _init_top_client()
    volume_power = _get_uplimit_volume_power(client, code)
    frgn_3d = _uplimit_frgn_3d.get(code)

    day_avg = _calc_uplimit_day_avg_vol_per_min(code, acml_vol, now_dt)
    last5m_avg = _calc_uplimit_last5m_avg_vol_per_min(code)

    cross_ts = _uplimit_25pct_cross_ts.get(code)
    min_since_cross = (now_dt - cross_ts).total_seconds() / 60.0 if cross_ts else None
    open_dt = now_dt.replace(hour=9, minute=0, second=0, microsecond=0)
    min_since_open = (now_dt - open_dt).total_seconds() / 60.0

    should_buy, reason, _ = uplimit_approach_buy_signal(
        prdy_ctrt=prdy_ctrt,
        prev_close=prev_close,
        stck_oprc=stck_oprc,
        acml_vol=acml_vol,
        day_avg_vol_per_min=day_avg,
        last_5m_avg_vol_per_min=last5m_avg,
        min_since_open=min_since_open,
        min_since_25pct_cross=min_since_cross,
        vola_10d=_uplimit_vola_10d.get(code),
        bid_sum_top5=bid_sum_top5,
        ask_sum_top5=ask_sum_top5,
        volume_power=volume_power,
        frgn_3d_net=frgn_3d,
        prev_day_prdy_ctrt=_uplimit_prev_day_ctrt.get(code, 0.0),
        already_holding=False,
        now_hm=(now_dt.hour, now_dt.minute),
        today_buy_count=_uplimit_today_buy_count,
        max_daily_buys=UPLIMIT_MAX_DAILY_BUYS,
        prev_ctrt=prev_ctrt,     # [260423]
    )

    if not should_buy:
        # INFO 로그 (종목당 10초 쿨다운 — skip 사유 가시화)
        last_log_ts = getattr(_try_uplimit_buy, "_last_log_ts", {})
        if time.time() - last_log_ts.get(code, 0) > 10:
            logger.info(f"{ts_prefix()} [uplimit_skip] {name}({code}) {reason}")
            last_log_ts[code] = time.time()
            setattr(_try_uplimit_buy, "_last_log_ts", last_log_ts)
        return

    # ── 주문 실행 ──
    try:
        accts = _iter_enabled_accounts(trade_only=True)
        if not accts:
            logger.warning(f"{ts_prefix()} [uplimit] {name}({code}) 활성 계좌 없음 → 매수 스킵")
            return
        acct = accts[0]  # a1
        cano = acct["cano"]
        acnt = acct.get("acnt_prdt_cd", "01") or "01"
        acct_client = _init_account_client(acct)
        avail_cash = _get_account_available_cash(acct_client, acct)
        if avail_cash <= 0:
            logger.info(f"{ts_prefix()} [uplimit] {name}({code}) 가용현금=0 → 매수 스킵")
            return

        # [260423] 시드 베이스 = INIT_CASH / UPLIMIT_DIVERSIFY_N
        # 세션 시작 시 _uplimit_seed_base 에 저장됨. 미설정이면 avail_cash/N 로 fallback.
        seed_base = _uplimit_seed_base if _uplimit_seed_base > 0 else (avail_cash / UPLIMIT_DIVERSIFY_N)
        # 투자 금액은 min(seed_base, avail_cash) — 이미 다른 종목에 투입된 상태면 잔액이 더 작음
        invest_amount = min(seed_base, avail_cash)

        entry_price = calc_entry_price(stck_prpr, "KOSPI", UPLIMIT_ENTRY_TICKS)
        qty = calc_uplimit_qty(invest_amount, entry_price, "KOSPI")
        if qty <= 0:
            msg = (
                f"{ts_prefix()} [uplimit] {name}({code}) 수량=0 "
                f"(seed={int(seed_base):,}원, 잔고={int(avail_cash):,}원, 단가={int(entry_price):,}원)"
            )
            _notify(msg, tele=True)
            logger.info(msg)
            return

        j = _buy_order_cash(acct_client, cano, acnt, "TTTC0802U",
                            code, qty, entry_price, ord_dvsn="00")
        out = j.get("output") or {}
        ordno = str(out.get("ODNO") or out.get("odno") or "").strip()

        with _uplimit_state_lock:
            _uplimit_buy_pending[code] = ordno
            _uplimit_positions[code] = {
                "name": name, "qty": qty, "buy_price": entry_price,
                "buy_ts": now_dt, "ordno": ordno,
                "max_prdy_ctrt": prdy_ctrt, "highest_since_buy": entry_price,
                "half_sold": False, "reason": reason,
                "source": "uplimit",
                "prev_close": prev_close,
                "stop_orders_placed": False,
                "stop_order_nos": [],  # [260423] 스톱 지정가 주문번호 리스트 (취소용)
                "invest_amount": invest_amount,
            }
            _uplimit_today_buy_count += 1
        _save_uplimit_state(force=True)

        msg = (
            f"{ts_prefix()} [uplimit매수] {name}({code}) ctrt={prdy_ctrt:.2f}%"
            f"  수량={qty}  단가={int(entry_price):,}  주문번호={ordno}"
            f"  사유={reason}"
        )
        _notify(msg, tele=True)
        logger.info(msg)
        _append_ledger(
            order_type="buy_order", code=code, name=name,
            buy_price=entry_price, order_qty=qty, reason=f"uplimit_{reason}",
            ord_no=ordno, stck_prpr=stck_prpr,
        )
    except Exception as e:
        logger.error(f"{ts_prefix()} [uplimit매수] {name}({code}) 주문 실패: {e}")


# =============================================================================
# [260423 재설계] Exit 관련 신규 함수 3종
#   - _setup_stop_limit_orders(): 상한가 근접 29.5% 도달 시 스톱 지정가 주문 2건 발주
#   - _try_uplimit_market_sell(): 25% 하회 시 기존 스톱 취소 + 시장가 전량 매도
#   - _auto_unsub_code(): 미매수 구독 종목 20% 하회 시 자동 해제
# =============================================================================

def _setup_stop_limit_orders(code: str, pos: dict) -> None:
    """
    상한가(29.5%+) 도달 시 KIS 서버 스톱 지정가 주문 2건 발주.
    ① CNDT_PRIC = 29%가격, ORD_UNPR = 28%가격, qty = 총수량 / 2
    ② CNDT_PRIC = 28%가격, ORD_UNPR = 27%가격, qty = 나머지
    서버가 조건가 도달 시 자동으로 지정가 매도 주문 전환.
    체결은 지정가 이상 (28% 이상)에서 발생.
    """
    prev_close = float(pos.get("prev_close") or 0)
    qty_total = int(pos.get("qty") or 0)
    name = pos.get("name", code)
    if prev_close <= 0 or qty_total <= 0:
        logger.warning(f"{ts_prefix()} [uplimit_stop] {name}({code}) 스킵: prev_close={prev_close}, qty={qty_total}")
        return

    qty_half = max(1, qty_total // 2)
    qty_rest = qty_total - qty_half

    # 가격 계산 (전일종가 기준 + 호가단위 반올림)
    cndt_29 = round_to_tick(prev_close * 1.29, "KOSPI")   # 29% 가격
    unpr_28 = round_to_tick(prev_close * 1.28, "KOSPI")   # 28% 가격
    cndt_28 = round_to_tick(prev_close * 1.28, "KOSPI")
    unpr_27 = round_to_tick(prev_close * 1.27, "KOSPI")

    accts = _iter_enabled_accounts(trade_only=True)
    if not accts:
        logger.warning(f"{ts_prefix()} [uplimit_stop] {name}({code}) 활성 계좌 없음")
        return
    acct = accts[0]
    cano = acct["cano"]
    acnt = acct.get("acnt_prdt_cd", "01") or "01"
    client = _init_account_client(acct)

    order_nos: list[str] = []

    # ① 절반 — 29% 조건가 / 28% 지정가
    try:
        j1 = _sell_order_cash(
            client, cano, acnt, code, qty_half,
            price=unpr_28, ord_dvsn="22", cndt_pric=cndt_29,
        )
        ordno1 = str((j1.get("output") or {}).get("ODNO") or "").strip()
        order_nos.append(ordno1)
        logger.info(
            f"{ts_prefix()} [uplimit_stop1] {name}({code}) 1차 절반 스톱지정가 "
            f"qty={qty_half} CNDT={int(cndt_29):,}(29%) UNPR={int(unpr_28):,}(28%) ord={ordno1}"
        )
    except Exception as e:
        logger.error(f"{ts_prefix()} [uplimit_stop1] {name}({code}) 발주 실패: {e}")

    # ② 나머지 절반 — 28% 조건가 / 27% 지정가
    if qty_rest > 0:
        try:
            j2 = _sell_order_cash(
                client, cano, acnt, code, qty_rest,
                price=unpr_27, ord_dvsn="22", cndt_pric=cndt_28,
            )
            ordno2 = str((j2.get("output") or {}).get("ODNO") or "").strip()
            order_nos.append(ordno2)
            logger.info(
                f"{ts_prefix()} [uplimit_stop2] {name}({code}) 2차 나머지 스톱지정가 "
                f"qty={qty_rest} CNDT={int(cndt_28):,}(28%) UNPR={int(unpr_27):,}(27%) ord={ordno2}"
            )
        except Exception as e:
            logger.error(f"{ts_prefix()} [uplimit_stop2] {name}({code}) 발주 실패: {e}")

    with _uplimit_state_lock:
        p = _uplimit_positions.get(code)
        if p:
            p["stop_orders_placed"] = True
            p["stop_order_nos"] = order_nos
            p["stop_prices"] = {
                "half_cndt": int(cndt_29), "half_unpr": int(unpr_28),
                "rest_cndt": int(cndt_28), "rest_unpr": int(unpr_27),
            }
    _save_uplimit_state(force=True)

    msg = (
        f"{ts_prefix()} [uplimit_stop] ★ {name}({code}) 상한가근접(29.5%+) → 스톱지정가 {len(order_nos)}건 발주. "
        f"[1차] 29%→28% {qty_half}주, [2차] 28%→27% {qty_rest}주"
    )
    _notify(msg, tele=True)


def _try_uplimit_market_sell(code: str, reason: str) -> None:
    """
    25% 하회 시: 기존 스톱 지정가 주문 전부 취소 + 시장가 전량 매도.
    재매수 허용을 위해 _uplimit_blacklist 에는 추가하지 않음.
    """
    with _uplimit_state_lock:
        pos = _uplimit_positions.get(code)
        if pos is None:
            return
        if pos.get("market_sell_fired"):
            return   # 중복 방지
        pos["market_sell_fired"] = True
        qty_remaining = int(pos.get("qty") or 0)
        stop_nos = list(pos.get("stop_order_nos") or [])
        name = pos.get("name", code)

    accts = _iter_enabled_accounts(trade_only=True)
    if not accts:
        logger.warning(f"{ts_prefix()} [uplimit_mkt] {name}({code}) 활성 계좌 없음")
        return
    acct = accts[0]
    cano = acct["cano"]
    acnt = acct.get("acnt_prdt_cd", "01") or "01"
    client = _init_account_client(acct)

    # ① 기존 스톱 지정가 주문 취소
    for ordno in stop_nos:
        if not ordno:
            continue
        try:
            _cancel_order_generic(client, cano, acnt, ordno, code, qty_remaining, ord_dvsn="01")
        except Exception as e:
            logger.warning(f"{ts_prefix()} [uplimit_mkt] {name}({code}) 스톱취소 실패 ord={ordno}: {e}")

    # ② 시장가 전량 매도
    if qty_remaining > 0:
        try:
            j = _sell_order_cash(client, cano, acnt, code, qty_remaining, ord_dvsn="01")
            ordno = str((j.get("output") or {}).get("ODNO") or "").strip()
            with _uplimit_state_lock:
                _uplimit_exit_pending[code] = ordno
                p = _uplimit_positions.get(code)
                if p:
                    p["last_exit_reason"] = reason
                    p["last_exit_ordno"] = ordno
            _save_uplimit_state(force=True)
            msg = (
                f"{ts_prefix()} [uplimit매도] ★ {name}({code}) 시장가 전량 {qty_remaining}주 "
                f"주문번호={ordno} 사유={reason} (29% 재상회 시 재매수 허용)"
            )
            _notify(msg, tele=True)
            logger.info(msg)
        except Exception as e:
            logger.error(f"{ts_prefix()} [uplimit_mkt] {name}({code}) 시장가매도 실패: {e}")


def _auto_unsub_code(code: str, reason: str) -> None:
    """미매수 구독 종목이 20% 하회했을 때 자동 해제 (쿨다운 포함)."""
    # 쿨다운: 동일 종목 30초 내 재호출 방지
    last_ts = getattr(_auto_unsub_code, "_last_ts", {})
    if time.time() - last_ts.get(code, 0) < 30:
        return
    last_ts[code] = time.time()
    setattr(_auto_unsub_code, "_last_ts", last_ts)

    if code not in _top_added_codes:
        return
    name = code_name_map.get(code, code)
    try:
        removed = _remove_code_structs([code])
        if code in removed:
            _top_added_codes.discard(code)
            _top_added_ts.pop(code, None)
            _log_top_sub_event(code, name, "해제")
            _notify(f"{ts_prefix()} [top] 자동해제 {name}({code}) {reason}")
            _trigger_ws_rebuild()
    except Exception as e:
        logger.warning(f"{ts_prefix()} [uplimit_unsub] {name}({code}) 해제 실패: {e}")


def _try_uplimit_exit(
    code: str,
    action: str,
    reason: str,
    sell_ratio: float,
    now_dt: datetime,
) -> None:
    """@deprecated [260423] — 단계적 청산 로직은 스톱 지정가 주문이 대체.
    25% 하회 시는 _try_uplimit_market_sell() 사용.
    (호출측 호환용 유지, 기존 단계적 sell_half/sell_all 동작)
    """
    with _uplimit_state_lock:
        pos = _uplimit_positions.get(code)
        if pos is None:
            return
        if code in _uplimit_exit_pending:
            return  # 이미 주문 송신
        qty_total = int(pos.get("qty") or 0)
        if qty_total <= 0:
            return
        if action == "sell_half":
            if pos.get("half_sold"):
                return
            sell_qty = max(1, qty_total // 2)
        else:
            sell_qty = qty_total
        name = pos.get("name", code)

    try:
        accts = _iter_enabled_accounts(trade_only=True)
        if not accts:
            logger.warning(f"{ts_prefix()} [uplimit_exit] {name}({code}) 활성 계좌 없음")
            return
        acct = accts[0]
        cano = acct["cano"]
        acnt = acct.get("acnt_prdt_cd", "01") or "01"
        client = _init_account_client(acct)
        # 시장가 매도
        j = _sell_order_cash(client, cano, acnt, code, sell_qty, ord_dvsn="01")
        out = j.get("output") or {}
        ordno = str(out.get("ODNO") or out.get("odno") or "").strip()

        with _uplimit_state_lock:
            _uplimit_exit_pending[code] = ordno
            pos = _uplimit_positions.get(code)
            if pos:
                pos["last_exit_reason"] = reason
                pos["last_exit_ordno"] = ordno
                if action == "sell_half":
                    pos["half_sold"] = True
                    pos["qty_after_half"] = qty_total - sell_qty
        _save_uplimit_state(force=True)

        msg = (
            f"{ts_prefix()} [uplimit매도] {name}({code}) {action}"
            f"  수량={sell_qty}/{qty_total}  주문번호={ordno}  사유={reason}"
        )
        _notify(msg, tele=True)
        logger.info(msg)
    except Exception as e:
        logger.error(f"{ts_prefix()} [uplimit_exit] {name}({code}) 주문 실패: {e}")


def _handle_uplimit_ccnl_fill(
    code: str,
    seln: str,
    qty: int,
    fill_pr: float,
    name: str,
    recv_ts: str,
) -> None:
    """H0STCNI0 체결통보 처리: uplimit 매수/매도 체결 반영."""
    is_sell = seln in ("01", "1")
    with _uplimit_state_lock:
        pos = _uplimit_positions.get(code)
        if is_sell:
            # 매도 체결
            ordno = _uplimit_exit_pending.pop(code, "")
            reason = (pos or {}).get("last_exit_reason", "")
            if pos:
                remaining = max(0, int(pos.get("qty", 0)) - qty)
                pos["qty"] = remaining
                pos["last_sell_fill_price"] = fill_pr
                if remaining <= 0:
                    # 전량 매도 완료 → 포지션 제거 + 블랙리스트 추가 (당일 재매수 금지)
                    _uplimit_positions.pop(code, None)
                    _uplimit_blacklist.add(code)
            log_msg = (
                f"{ts_prefix()} [uplimit체결] 매도 {name}({code}) 수량={qty} "
                f"단가={int(fill_pr):,} 주문번호={ordno} 사유={reason}"
            )
        else:
            # 매수 체결
            ordno = _uplimit_buy_pending.pop(code, "")
            if pos:
                pos["buy_price"] = fill_pr if fill_pr > 0 else pos.get("buy_price")
                pos["qty"] = qty  # 체결수량 확정 (부분체결은 합산으로 추후 확장)
                pos["highest_since_buy"] = max(pos.get("highest_since_buy", 0), fill_pr)
                # [260427] v5 시장가 매수 시 임시 buy_price 로 초기화된 post_buy_low 를
                # 실제 체결가로 재설정 (하드손절-5% 가 실 체결가 기준으로 동작하도록)
                if fill_pr > 0:
                    pos["post_buy_low"] = fill_pr
            log_msg = (
                f"{ts_prefix()} [uplimit체결] 매수 {name}({code}) 수량={qty} "
                f"단가={int(fill_pr):,} 주문번호={ordno}"
            )
    _save_uplimit_state(force=True)
    _notify(log_msg, tele=True)
    logger.info(log_msg)


def _check_uplimit_exit_for_tick(
    code: str,
    prdy_ctrt: float,
    bidp1: float,
    now_dt: datetime,
) -> None:
    """틱 수신 시마다 보유 포지션 Exit 조건 확인."""
    if code not in _uplimit_positions:
        return
    with _uplimit_state_lock:
        pos = _uplimit_positions.get(code)
        if pos is None:
            return
        # 최고가/최고 등락률 갱신
        if prdy_ctrt > pos.get("max_prdy_ctrt", 0):
            pos["max_prdy_ctrt"] = prdy_ctrt
        if bidp1 > pos.get("highest_since_buy", 0):
            pos["highest_since_buy"] = bidp1

    mins_since = (now_dt - pos["buy_ts"]).total_seconds() / 60.0
    action, reason, sell_ratio = uplimit_approach_exit(
        prdy_ctrt=prdy_ctrt,
        bidp1=bidp1,
        buy_price=float(pos.get("buy_price") or 0),
        max_prdy_ctrt=float(pos.get("max_prdy_ctrt") or 0),
        highest_since_buy=float(pos.get("highest_since_buy") or 0),
        minutes_since_buy=mins_since,
        half_sold=bool(pos.get("half_sold", False)),
        now_hm=(now_dt.hour, now_dt.minute),
    )
    if action in ("sell_half", "sell_all"):
        _try_uplimit_exit(code, action, reason, sell_ratio, now_dt)


# ── H0STMKO0 장운영정보 수신 처리 (VI 감지) ──
_mkstatus_sub_codes: set[str] = set()  # H0STMKO0 구독 중인 종목
_mkstatus_pending: set[str] = set()    # WSS 미연결 시점에 요청된 H0STMKO0 구독 보류분 (연결 후 흡수)

def _on_market_status_krx(result) -> None:
    """H0STMKO0 장운영정보 수신 → 종목상태 갱신."""
    if result is None or len(result) == 0:
        return
    for row in result.to_dicts():
        code = str(row.get("mksc_shrn_iscd", "")).strip()
        if not code:
            continue
        name = code_name_map.get(code, code)

        # ── [260602 관찰] 08:59 prdy>9.8% 관찰 종목: vi_cls_code 로그만 남기고 실제 로직은 건너뜀 ──
        # (연장기준 정합성 검증용. 실제 VI 전환/연장은 기존 경로가 처리하므로 동작 변경 없음.)
        if code in _vi_observe_codes:
            logger.warning(
                f"{ts_prefix()} [VI관찰-0859] {name}({code}) "
                f"vi_cls_code={str(row.get('vi_cls_code', '')).strip()!r} "
                f"ovtm_vi={str(row.get('ovtm_vi_cls_code', '')).strip()!r} "
                f"mkop={str(row.get('mkop_cls_code', '')).strip()!r} "
                f"antc_mkop={str(row.get('antc_mkop_cls_code', '')).strip()!r} "
                f"trht={str(row.get('trht_yn', '')).strip()!r} "
                f"iscd_stat={str(row.get('iscd_stat_cls_code', '')).strip()!r}"
            )
            continue

        trht_yn = str(row.get("trht_yn", "N")).strip().upper()
        iscd_stat = str(row.get("iscd_stat_cls_code", "")).strip()

        # 거래정지 감지
        if trht_yn == "Y" and code not in _halted_codes:
            _halted_codes.add(code)
            _notify(
                f"{ts_prefix()} [종목상태] {name}({code}) 거래정지 (H0STMKO0, trht_yn=Y)",
                tele=True,
            )

        # iscd_stat 갱신 — H0STMKO0의 값은 KRX DB와 다를 수 있으므로 교차검증
        if iscd_stat in ("57", "59") and code not in _single_price_codes and code not in _single_price_pending:
            try:
                krx_map = KRX_code_batch([code])
                krx_stat = krx_map.get(code)
                if krx_stat in ("57", "59"):
                    _single_price_pending[code] = krx_stat
                    desc = "57(관리종목)" if krx_stat == "57" else "59(단기과열)"
                    logger.info(f"{ts_prefix()} [종목상태] {name}({code}) {desc} (H0STMKO0→KRX확인) → 30분 단일가 후보")
                else:
                    logger.info(
                        f"{ts_prefix()} [종목상태] {name}({code}) "
                        f"H0STMKO0 iscd_stat={iscd_stat} ≠ KRX({krx_stat}) → 무시"
                    )
            except Exception as e:
                logger.warning(f"{ts_prefix()} [종목상태] {name}({code}) KRX 교차검증 실패: {e}")

        # ── 장운영구분코드 (mkop_cls_code) — 사이드카/서킷브레이크 감지 ──
        mkop = str(row.get("mkop_cls_code", "")).strip()
        market = _code_market_map.get(code, "")
        if mkop:
            prev_mkop = _last_mkop_event.get(code, "")
            if mkop != prev_mkop:
                _last_mkop_event[code] = mkop
                event_name = _MKOP_EVENT_NAMES.get(mkop)
                if event_name:
                    _market_event[code] = event_name
                    msg = f"{ts_prefix()} [장운영] {name}({code}) mkop={mkop} ({event_name})"
                    logger.warning(msg)
                    _notify(msg, tele=True)
                    # 사이드카(187/397)=프로그램매매 호가효력정지로 개인거래와 무관 → 위 정보성
                    # 로깅/알림만 남기고 시장 전파(REST 폴링 생략)는 발동시키지 않는다.
                    # 시장 전파(REST 폴링 생략)는 서킷브레이커류(174=발동/184=개시/164=시장임시정지)만.
                    if mkop in ("174", "184", "164") and market:
                        now_iso = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
                        _market_wide_event[market] = event_name
                        _market_wide_mkop[market] = mkop
                        _market_wide_start_ts[market] = now_iso
                        logger.warning(f"{ts_prefix()} [장운영] 마켓전파: {market} ← {event_name} (발동시각={now_iso})")
                        _notify(f"{ts_prefix()} [장운영] {market} {event_name} — REST 폴링 생략 시작", tele=True)
                # 사이드카/서킷 해제 → 종목 이벤트 클리어 (사이드카 해제 388/398 포함)
                if mkop in ("175", "185", "388", "398"):
                    _market_event.pop(code, None)
                # 시장 전파 해제(REST 폴링 재개)는 서킷브레이커 해제(175/185)만.
                # 사이드카 해제(388/398)는 애초에 시장 전파를 안 했으므로 진입 금지.
                if mkop in ("175", "185"):
                    if market and _market_wide_event.get(market):
                        prev_evt = _market_wide_event.pop(market, "")
                        _market_wide_mkop.pop(market, None)
                        _market_wide_start_ts.pop(market, None)
                        logger.warning(f"{ts_prefix()} [장운영] 마켓전파 해제: {market} ({prev_evt} → mkop={mkop})")
                        _notify(f"{ts_prefix()} [장운영] {market} 해제 — REST 폴링 재개", tele=True)

        # VI 발동/해제 감지 (vi_cls_code)
        # [260602 수정] 실측상 KIS vi_cls_code 는 발동='Y' / 해제='N' 으로 온다(현재가조회·H0STMKO0 동일).
        #   과거 코드는 0/1/2/3 숫자를 가정해 `!= "0"` 으로 발동을 판정 → 해제값 'N' 도 발동으로 오판,
        #   [VI해제]/restore 가 한 번도 안 타고 5분 fallback 으로만 복귀하던 버그(260529 로그: 발동4/해제0).
        #   → 발동상태값 = 'N'/'0'/'' 이 아닌 값. 단순 문자열변화 대신 '상태전이'로 판정해
        #     keepalive 선캐싱('0')·반복 프레임에서의 허위 전이를 방지하고, 재발동도 자연히 처리한다.
        vi_cls = str(row.get("vi_cls_code", "")).strip()
        if vi_cls:
            prev_vi = _vi_cls_cache.get(code, "")
            _vi_cls_cache[code] = vi_cls
            is_active = vi_cls not in ("N", "0", "")     # 발동 상태
            was_active = prev_vi not in ("N", "0", "")    # 직전 발동 상태
            if is_active and not was_active:              # 비활성 → 발동/재발동
                cnt = _vi_trigger_count.get(code, 0) + 1
                _vi_trigger_count[code] = cnt
                _vi_exp_sub_switch(code)
                # VI 발동 시각 기록 (parquet 저장용)
                _vi_start_ts[code] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
                _vi_end_ts.pop(code, None)
                _market_event[code] = "VI발동"
                msg = (f"{ts_prefix()} [VI발동] {name}({code}) "
                       f"vi_cls={vi_cls} ({cnt}회차)")
                logger.warning(msg)
                _notify(msg, tele=True)
            elif was_active and not is_active:            # 발동 → 해제
                _vi_exp_sub_restore(code)
                _vi_end_ts[code] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
                _market_event.pop(code, None)
                _vi_trigger_info.pop(code, None)
                msg = (f"{ts_prefix()} [VI해제] {name}({code}) "
                       f"vi_cls={vi_cls}")
                logger.info(msg)
                _notify(msg, tele=True)
                # H0STMKO0 동적 구독 해제 (keepalive/보유 종목 제외)
                keepalive_set = set(_mkt_keepalive_current.values())
                if code not in keepalive_set:
                    held_codes_vi = set()
                    try:
                        with _str1_sell_state_lock:
                            held_codes_vi = {c for c, st in _str1_sell_state.items() if not st.get("sold")}
                    except Exception:
                        pass
                    if code not in held_codes_vi:
                        try:
                            _mkstatus_sub_remove({code})
                        except Exception as e:
                            logger.warning(f"{ts_prefix()} [VI해제] H0STMKO0 해제 실패 {code}: {e}")


H0STMKO0_MAX_SLOTS: int = 5  # 데이터(ccnl/exp) 슬롯 ≥35 보장 위한 보수적 cap


def _mkstatus_sub_add(codes_to_add: set[str]) -> None:
    """보유 / VI 종목의 H0STMKO0 장운영정보 구독 추가 (main WSS 직결).
    cap 초과 시 보유 > VI > 기타 우선순위로 핵심 종목만 보호.
    WSS 미연결 시점 호출은 _mkstatus_pending 에 보류했다가 open_map 빌드 시 흡수.
    """
    global _mkstatus_sub_codes, _mkstatus_pending
    # 이미 구독 중 / 보류 중인 종목은 중복 제외
    new_codes = set(codes_to_add) - _mkstatus_sub_codes - _mkstatus_pending
    if not new_codes:
        return
    # WSS 미연결 → 보류 후 연결 시 open_map 빌드에서 (재)구독 (이전엔 조용히 no-op 되어
    # 구독이 영영 안 됐던 버그 — 보유/VI H0STMKO0 가 장중 0이 되는 원인이었음)
    with _kws_lock:
        _is_connected = _active_kws is not None
    if not _is_connected:
        _mkstatus_pending |= new_codes
        logger.info(
            f"{ts_prefix()} [H0STMKO0] WSS 미연결 → {len(new_codes)}건 보류(연결 후 구독)"
        )
        return
    available = max(0, H0STMKO0_MAX_SLOTS - len(_mkstatus_sub_codes))
    if available < len(new_codes):
        held: set[str] = set()
        try:
            with _str1_sell_state_lock:
                held = {c for c, st in _str1_sell_state.items() if not st.get("sold")}
        except Exception:
            pass
        priority_held = new_codes & held
        priority_vi = (new_codes & _vi_active_codes) - priority_held
        priority_rest = new_codes - priority_held - priority_vi
        ordered = list(priority_held) + list(priority_vi) + list(priority_rest)
        new_codes = set(ordered[:available])
        logger.warning(
            f"{ts_prefix()} [H0STMKO0] 슬롯 cap({H0STMKO0_MAX_SLOTS}) 초과 → 보유/VI 우선만 구독"
        )
    if not new_codes:
        return
    with _kws_lock:
        if _active_kws is not None:
            try:
                _send_subscribe(_active_kws, market_status_krx, list(new_codes), "1")  # noqa: F405
                _mkstatus_sub_codes.update(new_codes)
                names = [f"{code_name_map.get(c, c)}({c})" for c in new_codes]
                logger.info(f"{ts_prefix()} [H0STMKO0] 구독 추가: {', '.join(names)}")
            except Exception as e:
                logger.warning(f"{ts_prefix()} [H0STMKO0] 구독 추가 실패: {e}")


def _mkstatus_sub_remove(codes_to_remove: set[str]) -> None:
    """매도 완료 / VI 해제 종목의 H0STMKO0 구독 해제 (main WSS 직결)."""
    global _mkstatus_sub_codes
    active = set(codes_to_remove) & _mkstatus_sub_codes
    if not active:
        return
    with _kws_lock:
        if _active_kws is not None:
            try:
                _send_subscribe(_active_kws, market_status_krx, list(active), "2")  # noqa: F405
                _mkstatus_sub_codes -= active
                names = [f"{code_name_map.get(c, c)}({c})" for c in active]
                logger.info(f"{ts_prefix()} [H0STMKO0] 구독 해제: {', '.join(names)}")
            except Exception as e:
                logger.warning(f"{ts_prefix()} [H0STMKO0] 구독 해제 실패: {e}")


def _avg_tick_interval(code: str) -> float:
    """종목의 최근 틱 간격 평균(초) 반환. 데이터 부족 시 999.0."""
    dq = _recent_tick_ts.get(code)
    if not dq or len(dq) < 2:
        return 999.0
    intervals = [dq[i] - dq[i - 1] for i in range(1, len(dq))]
    return sum(intervals) / len(intervals)


def _inquire_vi_status_single(client: KisClient, code: str) -> tuple[bool, str, str]:
    """단일 종목 VI 상태 REST 조회.

    Returns:
        (is_vi, vi_time, vi_cls_code)
        - is_vi: VI 발동 중이면 True
        - vi_time: 발동시각 (HHMMSS)
        - vi_cls_code: "1"=정적VI, "2"=동적VI, "3"=정적&동적
    """
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/quotations/inquire-vi-status"
    headers = client._headers(tr_id="FHPST01390000")
    today = datetime.now(KST).strftime("%Y%m%d")
    params = {
        "FID_COND_SCR_DIV_CODE": "20139",
        "FID_INPUT_ISCD": code,
        "FID_MRKT_CLS_CODE": "0",
        "FID_DIV_CLS_CODE": "0",      # 전체(상승+하락)
        "FID_RANK_SORT_CLS_CODE": "0", # 전체 VI 종류
        "FID_INPUT_DATE_1": today,
        "FID_TRGT_CLS_CODE": "",
        "FID_TRGT_EXLS_CLS_CODE": "",
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        j = r.json()
        if str(j.get("rt_cd")) != "0":
            logger.warning(f"{ts_prefix()} [VI조회] {code} 실패: {j.get('msg1', '')}")
            return (False, "", "")
        rows = j.get("output") or []
        # 해당 종목의 가장 최근 VI 발동 중인 레코드 찾기
        for row in rows:
            row_code = str(row.get("mksc_shrn_iscd", "")).strip().zfill(6)
            if row_code != code:
                continue
            vi_cls = str(row.get("vi_cls_code", "")).strip()
            vi_cancel = str(row.get("vi_cncl_hour", "")).strip()
            vi_time = str(row.get("cntg_vi_hour", "")).strip()
            # vi_cncl_hour 가 비어있거나 "000000" 이면 아직 발동 중
            if vi_cls and vi_cls != "N" and (not vi_cancel or vi_cancel == "000000"):
                return (True, vi_time, vi_cls)
        return (False, "", "")
    except Exception as e:
        logger.warning(f"{ts_prefix()} [VI조회] {code} 오류: {e}")
        return (False, "", "")


def _handle_stale_check_result(code: str, output: dict) -> None:
    """REST 상태확인(FHKST01010100) 결과로 종목 상태 판단 및 조치.

    주의: REST API(FHKST01010100)의 iscd_stat_cls_code는 KRX code DB의 값과
    **전혀 다른 의미**이므로 종목상태(상장폐지/관리종목 등) 판단에 사용하면 안 됨.
    종목상태 판단은 KRX_code_batch (daily_KRX_code_DB) 기준으로만 수행할 것.
    여기서는 temp_stop_yn, sltr_yn 등 명확한 필드만 사용.
    """
    name = code_name_map.get(code, code)
    pr = output.get("stck_prpr", "")
    vol = output.get("acml_vol", "")
    temp_stop = str(output.get("temp_stop_yn", "N")).strip().upper()
    vi_cls_rt = str(output.get("vi_cls_code", "")).strip()   # 현재가조회 VI구분 (발동='Y'/해제='N')
    sltr = str(output.get("sltr_yn", "N")).strip().upper()
    mrkt_warn = str(output.get("mrkt_warn_cls_code", "")).strip()
    invt_caful = str(output.get("invt_caful_yn", "N")).strip().upper()

    def _status_notify(msg: str) -> None:
        sys.stdout.write("\n")
        _notify(msg)

    # ── VI 발동 감지 (vi_cls_code 기준) ──
    # [260602] 실측: VI 동시호가 중엔 temp_stop_yn='N' 인데 vi_cls_code='Y' 로 온다.
    #   → temp_stop_yn 으로는 VI 를 못 잡으므로 현재가조회의 vi_cls_code 로 감지한다.
    #   발동시각은 감지시각(now)으로 기록한다 — 정확 발동시각/VI종류(정적·동적)는 vi_status CSV
    #   (Daily_vi, 260602부터 양방향)에 권위있게 남으므로, 여기서 별도 VI현황조회(REST 추가호출)는
    #   하지 않는다(로그 장식용 호출 제거 → 변동성 장세 REST 절약). 매매 영향 없음.
    #   _vi_exp_sub_switch 가 H0STMKO0 도 구독하므로 해제는 경로 A(H0STMKO0 vi_cls='N')가 처리.
    if vi_cls_rt not in ("N", "0", "") and code not in _vi_exp_sub_ts:
        detected_at = datetime.now(KST).strftime("%H:%M:%S")
        msg = (f"{ts_prefix()} [VI감지-REST] {name}({code}) "
               f"vi_cls_code={vi_cls_rt} 감지시각={detected_at} → 예상체결 전환")
        logger.warning(msg)
        _notify(msg, tele=True)
        _vi_exp_sub_switch(code)
        _enqueue_rest_price_row(code, output)
        return

    # ── 장중 일시정지 (거래정지 등 — temp_stop_yn=Y) ──
    if temp_stop == "Y":
        if code not in _halted_codes:
            _halted_codes.add(code)
            _status_notify(f"{ts_prefix()} [종목상태] {name}({code}) 일시정지 중 (temp_stop_yn=Y, vi_cls_code={vi_cls_rt or '-'})")
        _enqueue_rest_price_row(code, output)
        return

    # ── 정리매매 → 해제 ──
    if sltr == "Y":
        if code not in _halted_codes:
            _halted_codes.add(code)
            _status_notify(f"{ts_prefix()} [종목상태] {name}({code}) 정리매매 (sltr_yn=Y) → 구독 해제")
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
            _status_notify(f"{ts_prefix()} [종목상태] {name}({code}) 거래 없음 (pr={pr_val}, vol={vol_val}) → 구독 해제")
            removed = _remove_code_structs([code], force=True)
            if removed:
                _trigger_ws_rebuild()
        return

    # ── 투자위험/투자주의 → 유지 (1회 알람) ──
    if mrkt_warn == "03" or invt_caful == "Y":
        if code not in _halted_codes:
            _halted_codes.add(code)
            tag = "투자위험" if mrkt_warn == "03" else "투자주의"
            _status_notify(f"{ts_prefix()} [종목상태] {name}({code}) {tag}")
        _enqueue_rest_price_row(code, output)
        return

    # ── 정상: REST 보충만 수행 (57/59 자동 판정 없음, KIS API 조회결과 기반만 사용) ──
    _halted_codes.discard(code)
    _enqueue_rest_price_row(code, output)
    pr_fmt = f"{float(str(pr).replace(',', '') or 0):,.0f}" if pr else "0"
    logger.info(f"{ts_prefix()} [REST보충] {name}({code}) 현재가={pr_fmt} temp_stop_yn={temp_stop}")


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
                        if last > 0 and c not in _single_price_codes:
                            continue  # 이미 WSS 체결 수신(=open 가격 확보) → skip (57/59는 장전 예상가일 수 있으므로 제외)
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
        #     - WSS 전용 시각(_last_wss_recv_ts)으로 stale 판단
        #       (REST가 _last_recv_ts를 갱신해 WSS 미수신을 마스킹하는 문제 방지)
        #     - WSS 한 번도 미수신 종목: STALE_CHECK_SEC(60초) 간격
        #     - WSS 수신 이력 있는 종목: STALE_CHECK_INACTIVE_SEC(20초) 미수신 시만
        #     - 거래정지 종목: STALE_CHECK_HALTED_SEC(300초) 간격
        # ================================================================
        if dtime(9, 10) <= now_t <= dtime(15, 30):
            with _lock:
                for c in list(codes):
                    if c in _rest_pending:
                        continue
                    last_wss = _last_wss_recv_ts.get(c, 0.0)
                    idle = (now - last_wss) if last_wss > 0 else (now - start_ts)

                    # ── "갑자기 끊김" 감지: 활발히 거래되던 종목이 갑자기 멈춤 → VI 의심 ──
                    avg_itv = _avg_tick_interval(c)
                    if (avg_itv < 5.0
                            and last_wss > 0
                            and idle >= max(10.0, avg_itv * 3)
                            and c not in _vi_exp_sub_ts
                            and c not in _halted_codes):
                        last_check = _last_stale_check_ts.get(c, 0.0)
                        if (now - last_check) >= 10.0:
                            _last_stale_check_ts[c] = now
                            _rest_pending.add(c)
                            try:
                                _price_queue.put_nowait(("stale_check", c))
                            except Exception:
                                pass
                            continue

                    # 상태별 재확인 간격 적용
                    if c in _halted_codes:
                        check_interval = STALE_CHECK_HALTED_SEC    # 300초
                    elif last_wss > 0:
                        # WSS 수신 이력 있음 → 비활성 구간
                        check_interval = STALE_CHECK_INACTIVE_SEC  # 20초
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

            # VI 예상체결가 구독 자동 해제 (300초 경과 — H0STMKO0 감지 실패 시 fallback)
            for c in list(_vi_exp_sub_ts):
                if (now - _vi_exp_sub_ts[c]) >= 300:
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

        # [v_1] 500 에러 3회+ 차단 종목은 건너뜀 (TTL 만료 시 자동 복구)
        _blk_until = _price_block_until.get(code, 0.0)
        if _blk_until > time.time():
            _rest_pending.discard(code)
            continue
        elif _blk_until > 0:
            # TTL 만료 → 차단 해제 + 카운터 초기화 (한 번 더 시도)
            _price_block_until.pop(code, None)
            _rest_fail_backoff.pop(code, None)
            name = code_name_map.get(code, code)
            logger.info(f"{ts_prefix()} [price_block] {name}({code}) 30분 경과 → 차단 해제, 재시도")

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
            prev_fc = _rest_fail_backoff.pop(code, None)  # 성공 → 거래정지 의심 해제
            _price_block_until.pop(code, None)             # [v_1] 성공 시 차단 상태도 해제
            if prev_fc:
                name = code_name_map.get(code, code)
                logger.info(f"{ts_prefix()} [price] REST 복구: {name}({code}) fc={prev_fc}→0")
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
                # [v_1] 3회 이상 연속 500 → KRX 상태 진단 후 맞춤 조치
                if fc == PRICE_BLOCK_FAIL_THRESHOLD:
                    diag = _diagnose_price_500(code, name)
                    stat, desc, action, dreason = diag["iscd_stat"], diag["desc"], diag["action"], diag["reason"]

                    if action == "single_price":
                        # 57/59 관리/단기과열 → 단일가 매매로 이관, 짧은 차단 (10분) 후 재개
                        _price_block_until[code] = time.time() + 600
                        _notify(
                            f"{ts_prefix()} [price_diag] {name}({code}) REST 500×{fc} "
                            f"→ KRX iscd_stat={stat}({desc}) → 단일가 매매 이관",
                            tele=True,
                        )
                    elif action == "halt_remove":
                        # 정지/상폐 등 → 완전 차단 (12시간)
                        _price_block_until[code] = time.time() + 43200
                        _notify(
                            f"{ts_prefix()} [price_diag] ★ {name}({code}) REST 500×{fc} "
                            f"→ KRX iscd_stat={stat}({desc}) → 거래불가 확정, 구독 해제 권장",
                            tele=True,
                        )
                        logger.warning(f"{ts_prefix()} [price_diag] {dreason}")
                    elif action == "temp_block":
                        # KRX 정상 → KIS 서버 일시 장애 추정, 30분 차단
                        _price_block_until[code] = time.time() + PRICE_BLOCK_TTL_SEC
                        _notify(
                            f"{ts_prefix()} [price_diag] {name}({code}) REST 500×{fc} "
                            f"→ KRX 정상(00) → KIS 서버 일시 장애 추정, {PRICE_BLOCK_TTL_SEC//60}분 차단",
                            tele=True,
                        )
                    else:  # block_30min (조회 실패 / 투자유의 등)
                        _price_block_until[code] = time.time() + PRICE_BLOCK_TTL_SEC
                        _notify(
                            f"{ts_prefix()} [price_diag] {name}({code}) REST 500×{fc} "
                            f"→ iscd_stat={stat}({desc}) → {PRICE_BLOCK_TTL_SEC//60}분 차단",
                            tele=True,
                        )

                    logger.warning(
                        f"{ts_prefix()} [price_diag] {name}({code}) stat={stat} "
                        f"action={action} reason={dreason} "
                        f"until={datetime.fromtimestamp(_price_block_until[code], KST).strftime('%H:%M:%S')}"
                    )
                elif fc > PRICE_BLOCK_FAIL_THRESHOLD:
                    # 진단 이후 block 상태가 아니면(복구 후 재실패) 짧게 유지 차단
                    if code not in _price_block_until or _price_block_until[code] < time.time():
                        _price_block_until[code] = time.time() + PRICE_BLOCK_TTL_SEC
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
            # ── 새 종목 iscd_stat 조회 (KRX_code) → 57/59 단일가 판별 ──
            _check_iscd_stat_krx(added)
    return added


# KRX group 필터 캐시 (group == "ST" 인 종목만 구독 대상)
_krx_group_cache: dict[str, str] = {}


def _load_krx_group_cache() -> None:
    """KRX_code.csv에서 code→group 매핑을 로드한다. code→market 매핑도 구성."""
    global _krx_group_cache
    try:
        krx_path = Path("/home/ubuntu/Stoc_Kis/data/admin/symbol_master/KRX_code.csv")
        if not krx_path.exists():
            logger.warning("[top] KRX_code.csv not found, group filter disabled")
            return
        # group 로드
        df = pd.read_csv(krx_path, dtype=str, usecols=["code", "group"])
        df["code"] = df["code"].str.strip().str.zfill(6)
        df["group"] = df["group"].str.strip().str.upper()
        _krx_group_cache = dict(zip(df["code"], df["group"]))
        logger.info(f"[top] KRX group cache loaded: {len(_krx_group_cache)} entries")
        # market 매핑 (컬럼명 자동 감지)
        try:
            df_mkt = pd.read_csv(krx_path, dtype=str)
            df_mkt.columns = [str(c).strip().lower() for c in df_mkt.columns]
            mkt_col = next((c for c in ("market", "mkt", "market_name", "exchange") if c in df_mkt.columns), None)
            if "code" in df_mkt.columns and mkt_col:
                df_mkt["code"] = df_mkt["code"].astype(str).str.strip().str.zfill(6)
                df_mkt[mkt_col] = df_mkt[mkt_col].astype(str).str.strip().str.upper()
                for c, m in zip(df_mkt["code"], df_mkt[mkt_col]):
                    if "KOSPI" in m or m in ("1", "K", "STK"):
                        _code_market_map[c] = "KOSPI"
                    elif "KOSDAQ" in m or m in ("2", "Q", "KSQ"):
                        _code_market_map[c] = "KOSDAQ"
                logger.info(f"[top] code_market_map 구성: {len(_code_market_map)}건 (mkt_col={mkt_col})")
            else:
                logger.info("[top] KRX_code.csv market 컬럼 없음 → code_market_map 미구성")
        except Exception as e_mkt:
            logger.warning(f"[top] code_market_map 구성 실패: {e_mkt}")
    except Exception as e:
        logger.warning(f"[top] KRX group cache load failed: {e}")


def _is_stock_group(code: str) -> bool:
    """KRX group이 'ST'(일반 주식)인지 확인. 캐시 없으면 통과."""
    if not _krx_group_cache:
        return True  # 캐시 없으면 필터 미적용
    return _krx_group_cache.get(code, "") == "ST"


# 탑10 구독 추가/해제 이력 관리
_top_added_codes: set[str] = set(_loaded_top_added)  # config에서 복원 또는 빈 set
# [260423] LRU 관리용 — code → 추가된 unix_ts (슬롯 초과 시 오래된 것부터 해제)
_top_added_ts: dict[str, float] = {c: time.time() for c in _top_added_codes}
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
_closing_cancel_failed: set[str] = set()         # 3회 재시도 후 최종 실패 종목
_closing_cancel_retry: dict[str, int] = {}       # code -> 취소 재시도 횟수
_closing_last_action_ts: dict[str, float] = {}   # code -> 마지막 취소/재주문 시각
_closing_152930_done = False                      # 15:29:30 일시 확인 실행 여부
_pre_unsub_closing_done = False                   # 15:19:55 사전 구독 해제 실행 여부
_closing_monitor_lock = threading.Lock()          # 모니터링 락


def _to_float_closing(val) -> float:
    try:
        if isinstance(val, str):
            val = val.replace(",", "")
        return float(val)
    except Exception:
        return 0.0


def _sub_monthly_log_path() -> Path:
    """월별 구독 이력 CSV 경로: wss_subscription_log_YYMM.csv"""
    yymm = datetime.now(KST).strftime("%y%m")
    return TOP_RANK_OUT_DIR / f"wss_subscription_log_{yymm}.csv"


def _log_subscription_event(code: str, name: str, sub_type: str, action: str) -> None:
    """월별 구독 이력 CSV에 1행 추가.
    sub_type: base / top30 / vi / str1_sell / closing 등
    action: 시작 / 추가 / 해제
    """
    TOP_RANK_OUT_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(KST)
    log_path = _sub_monthly_log_path()
    write_header = not log_path.exists()
    try:
        with open(log_path, "a", encoding="utf-8-sig") as f:
            if write_header:
                f.write("date,time,type,code,name,action\n")
            f.write(f"{now.strftime('%Y-%m-%d')},{now.strftime('%H:%M:%S')},{sub_type},{code},{name},{action}\n")
    except Exception as e:
        logger.warning(f"[sub_log] monthly write failed: {e}")


def _log_base_codes_on_startup() -> None:
    """프로그램 시작 시 base_codes(morning target)를 월별 CSV에 기록."""
    for c in sorted(_base_codes):
        name = code_name_map.get(c, c)
        _log_subscription_event(c, name, "base", "시작")
    logger.info(f"[sub_log] base_codes {len(_base_codes)}종목 월별 CSV 기록 완료")


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
    # 월별 CSV에도 기록
    _log_subscription_event(code, name, "top30", "추가" if action == "추가" else "해제")


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
            # _ema_state, _price_buf는 당일 모든 종목 유지 (구독 해제해도 pop 안 함)
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
                    retry_cnt = _closing_cancel_retry.get(code, 0) + 1
                    _closing_cancel_retry[code] = retry_cnt
                    if retry_cnt >= 3:
                        _closing_cancel_failed.add(code)
                        logger.warning(f"{ts_prefix()} [종가매수취소실패] {code}: {retry_cnt}회 재시도 후 중단 ({e})")
                    else:
                        logger.warning(f"{ts_prefix()} [종가매수취소실패] {code}: {e} (재시도 {retry_cnt}/3)")

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


_closing_redistribute_done = False


def _run_closing_cancel_redistribute() -> None:
    """
    15:29:50 취소 금액 재분배.
    취소 완료된 종목의 금액을 거래량 5000 이상 종목에 추가 시장가 매수.
    """
    global _closing_redistribute_done
    if _closing_redistribute_done or not CLOSE_BUY or not _closing_cancelled:
        return
    _closing_redistribute_done = True

    # 취소된 종목의 금액 합산
    cancel_amount = 0.0
    for o in _closing_buy_placed:
        code = o.get("code", "")
        if code in _closing_cancelled:
            cancel_amount += float(o.get("amount_net", 0))
    if cancel_amount <= 0:
        logger.info(f"{ts_prefix()} [종가재분배] 취소 금액 없음, 재분배 스킵")
        return

    # 거래량 5000 이상 종목 중 미취소·미보유 종목 선별
    placed_codes = {o.get("code") for o in _closing_buy_placed}
    redist_candidates: list[dict] = []
    state = _load_closing_buy_state()
    if state and state.get("code_info"):
        for code, info in state["code_info"].items():
            if code in placed_codes:
                continue
            vol = int(info.get("acml_vol", 0) or 0)
            if vol >= 5000:
                pr = float(info.get("stck_prpr", 0) or 0)
                if pr > 0:
                    redist_candidates.append({"code": code, "price": pr, "vol": vol, "name": info.get("name", code)})
    redist_candidates.sort(key=lambda x: x["vol"], reverse=True)

    if not redist_candidates:
        logger.info(f"{ts_prefix()} [종가재분배] 거래량≥5000 후보 없음 (취소금액={cancel_amount:,.0f})")
        return

    # 재분배: 상위 종목부터 시장가 매수
    try:
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        client = _top_client or _init_top_client()
        remain_amount = cancel_amount
        ordered: list[str] = []
        for cand in redist_candidates:
            if remain_amount < cand["price"]:
                break
            qty = int(remain_amount / cand["price"])
            if qty <= 0:
                continue
            code = cand["code"]
            limit_up = calc_limit_up_price(cand["price"])
            try:
                j = _buy_order_cash(client, cano, acnt, "TTTC0802U", code, qty, limit_up, ord_dvsn="01")
                out = j.get("output") or {}
                ordno = str(out.get("ODNO") or out.get("odno") or "").strip()
                cost = cand["price"] * qty
                remain_amount -= cost
                ordered.append(f"{cand['name']}({code}) {qty}주 ≈{cost:,.0f}원")
                _closing_buy_placed.append({
                    "name": cand["name"], "code": code, "qty": qty, "limit_up": limit_up,
                    "amount_net": cost, "ordno": ordno, "cano": cano, "acnt": acnt,
                })
            except Exception as e:
                logger.warning(f"{ts_prefix()} [종가재분배주문실패] {code}: {e}")
        if ordered:
            msg = f"{ts_prefix()} [종가재분배] 취소금액={cancel_amount:,.0f}원 → {len(ordered)}종목 추가매수: {', '.join(ordered)}"
            _notify(msg, tele=True)
            logger.info(msg)
        else:
            logger.info(f"{ts_prefix()} [종가재분배] 추가매수 주문 없음 (취소금액={cancel_amount:,.0f})")
    except Exception as e:
        logger.warning(f"{ts_prefix()} [종가재분배] 실패: {e}")


_closing_switch_done = False  # 15:20 종가 전환 중복 실행 방지

def _switch_to_closing_codes() -> None:
    """15:20 종가매매 전환: 기존 종목 전부 해제 → _closing_codes 만 남긴다."""
    global _base_codes, _closing_switch_done
    if _closing_switch_done:
        logger.info(f"{ts_prefix()} [top] 종가매매 전환 이미 완료 → 스킵")
        return
    if not _closing_codes:
        logger.warning(f"{ts_prefix()} [top] _closing_codes 비어 있음, 전환 불가")
        return
    _closing_switch_done = True  # 선점 (매수 실패해도 재진입 방지)

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

    # ── 2) WSS 유지 (구독은 15:19:10 해제 완료, 15:21 예상체결가 구독 예정) ──

    # ── 3) 메시지 출력 ──
    sys.stdout.write("\n")  # 제자리 출력 줄바꿈
    _notify(f"{ts_prefix()} [종가매매전환] WSS 유지, 15:21 예상체결가 구독 예정", tele=True)
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
    logger.info(f"{ts_prefix()} [top] started (top{TOP_RANK_N}조회, {TOP_RANK_ADD_N}개 추가, sub={len(codes)}/{MAX_WSS_SUBSCRIBE}, until={TOP_RANK_END})")
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
        remain = (next_ts - now).total_seconds()
        if remain <= 2.0:
            time.sleep(max(0.005, remain * 0.5))  # 2초 이내: 정밀 폴링
        else:
            time.sleep(min(remain, 1.0))
        if datetime.now(KST) < next_ts:
            continue
        logger.info(f"{ts_prefix()} [top] 랭킹 조회 시작 (schedule #{idx})")
        is_closing_select = (next_ts.time() >= dtime(15, 18))   # 15:18 종가매매 선정
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
            _fn = os.path.basename(str(out_path)).replace("Fetch_fluctuation_top_", "...")
            msg = f"{ts_prefix()} 상승률 상위 Top{TOP_RANK_N} 다운로드, 저장 완료({_fn})"
            sys.stdout.write("\n")  # 제자리 출력 줄바꿈
            _notify(msg)

            # --- 현재 랭킹 종목코드 추출 ---
            current_top_codes: list[str] = []
            current_top_ctrt: dict[str, float] = {}   # [260423] code → prdy_ctrt (25% 필터용)
            for row in rows:
                code = str(
                    row.get("stck_shrn_iscd")
                    or row.get("stck_cd")
                    or row.get("code")
                    or ""
                ).zfill(6)
                if code:
                    current_top_codes.append(code)
                    try:
                        _ctrt = float(str(row.get("prdy_ctrt") or 0).replace(",", "") or 0)
                        current_top_ctrt[code] = _ctrt
                    except Exception:
                        current_top_ctrt[code] = 0.0

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
                _cfg_limit_up_only = (_read_cfg() or load_config(str(CONFIG_PATH))).get("closing_buy_limit_up_only", False)
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
                    # closing_buy_limit_up_only=true 시 상한가(sign=1)만 허용
                    if _cfg_limit_up_only and prdy_sign != "1":
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
                    _notify(msg_cl, tele=True)
                    n_sel = len(_closing_codes)
                    _accts = _iter_enabled_accounts(trade_only=True)
                    _max_inv = float(str((_accts[0] if _accts else {}).get("max_invest", "0") or 0).replace("_", "") or 0)
                    per_alloc = int(_max_inv) // n_sel if n_sel else 0
                    _notify(
                        f"{ts_prefix()} [종가매매선정] 자원배분 | "
                        f"총액(한도)={int(_max_inv):,}원 | 선정종목수={n_sel}개 | 종목당 배정액={per_alloc:,}원"
                    )
                    # 상세 선정 로그 (Phase 3-8)
                    for i, c in enumerate(_closing_codes):
                        info = _closing_code_info.get(c, {})
                        nm = info.get("name", code_name_map.get(c, c))
                        prdy = info.get("prdy_ctrt", 0.0)
                        pc = info.get("prev_close", 0.0)
                        is_limit = "▲상한가" if prdy >= 29.5 else "▲"
                        cur_pr = 0.0
                        hg_diff = 0
                        hg_ratio = 0.0
                        for r in rows:
                            rc = str(r.get("stck_shrn_iscd") or r.get("code") or "").strip().zfill(6)
                            if rc == c:
                                cur_pr = float(str(r.get("stck_prpr") or 0).replace(",", "") or 0)
                                hp = float(str(r.get("stck_hgpr") or 0).replace(",", "") or 0)
                                if hp > 0 and cur_pr > 0:
                                    hg_diff = int(cur_pr - hp)
                                    hg_ratio = (cur_pr / hp - 1) * 100
                                break
                        # 예상 수량/금액 계산 (상한가 기준)
                        _pc = info.get("prev_close") or 0
                        _lu = calc_limit_up_price(_pc) if _pc > 0 else 0
                        _qty = int(per_alloc // _lu) if _lu > 0 else 0
                        _amt = int(_qty * _lu) if _lu > 0 else 0
                        _notify(
                            f"{ts_prefix()} [종가매매선정] #{i+1} {nm}({c}), "
                            f"pr={cur_pr:,.0f}, prdy_ctrt={prdy:+.2f}({is_limit}), "
                            f"hg-pr={hg_diff:,}, pr/hg={hg_ratio:+.2f}%, "
                            f"qty={_qty}, amt={_amt:,}"
                        )
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

                # [260423 재설계]
                # --- 1) 구독 해제 대상: 20% 미만으로 떨어진 미매수 종목 + Top 랭킹 완전 이탈 종목
                #       (매수/매도 pending 중인 종목은 보호)
                codes_to_remove: list[str] = []
                for c in list(_top_added_codes):
                    # 보호: 매수 완료/대기 또는 매도 대기 중이면 해제 금지
                    if c in _uplimit_positions or c in _uplimit_buy_pending or c in _uplimit_exit_pending:
                        continue
                    cur_ctrt = current_top_ctrt.get(c)
                    if cur_ctrt is None:
                        # Top 응답에서 사라짐 → 완전 이탈, 해제
                        codes_to_remove.append(c)
                        continue
                    # 20% 미만으로 떨어진 미매수 종목 → 해제
                    if cur_ctrt < UPLIMIT_UNSUBSCRIBE_CTRT:
                        codes_to_remove.append(c)

                if codes_to_remove:
                    removed = _remove_code_structs(codes_to_remove)
                    for c in removed:
                        _top_added_codes.discard(c)
                        _top_added_ts.pop(c, None)
                        name = code_name_map.get(c, c)
                        _log_top_sub_event(c, name, "해제")
                    if removed:
                        removed_names = [code_name_map.get(c, c) for c in removed]
                        removed_ctrts = [f"{current_top_ctrt.get(c, 0.0):.2f}%" for c in removed]
                        msg_rm = (
                            f"{ts_prefix()} [top] 구독 해제 (<{UPLIMIT_UNSUBSCRIBE_CTRT}% 또는 이탈): "
                            f"{list(zip(removed, removed_names, removed_ctrts))}"
                        )
                        sys.stdout.write("\n")
                        _notify(msg_rm)
                        changed = True

                # --- 2) 25% 이상 모든 Top 종목 → 구독 추가 (신규만)
                candidates = []
                for code in current_top_codes:
                    _ctrt_cur = current_top_ctrt.get(code, 0.0)
                    if _ctrt_cur < UPLIMIT_SUBSCRIBE_MIN_CTRT:
                        continue   # [260423] 25% 미만은 편입 대상 아님
                    # [260423] 상한가 30% 초과 이상치 제외 (SPAC/재상장/오류 데이터)
                    if _ctrt_cur > 30.0:
                        continue
                    # [260423] SPAC(스팩) 제외 — 상한가 규정 무의미, 변동성 예측 불가
                    _nm = code_name_map.get(code, "")
                    if "스팩" in _nm or "SPAC" in _nm.upper():
                        continue
                    if code in _base_codes:
                        continue
                    if code in codes:
                        continue  # 이미 구독 중
                    if code in _watchdog_removed_codes:
                        continue
                    if not _is_stock_group(code):
                        continue
                    candidates.append(code)

                # 구독 상한 체크 + LRU 해제 (이미 구독 중인 _top_added 중 가장 오래된 것부터)
                # H0STMKO0(보유/VI 동적 구독) 도 동일 40 슬롯에서 차감
                current_count = len(codes)
                available_slots = max(0, MAX_WSS_SUBSCRIBE - current_count - len(_mkstatus_sub_codes))
                if available_slots < len(candidates):
                    # LRU 해제 대상: _top_added_codes 중 매수/매도 pending 없는 종목을 추가 시각 오름차순 정렬
                    evictable = [
                        (ts, c) for c, ts in _top_added_ts.items()
                        if c in _top_added_codes
                        and c not in _uplimit_positions
                        and c not in _uplimit_buy_pending
                        and c not in _uplimit_exit_pending
                    ]
                    evictable.sort()   # 오래된 것부터
                    need = len(candidates) - available_slots
                    evicted_codes = [c for _, c in evictable[:need]]
                    if evicted_codes:
                        _removed_lru = _remove_code_structs(evicted_codes)
                        for c in _removed_lru:
                            _top_added_codes.discard(c)
                            _top_added_ts.pop(c, None)
                            _log_top_sub_event(c, code_name_map.get(c, c), "해제")
                        if _removed_lru:
                            msg_lru = f"{ts_prefix()} [top] LRU 해제 (슬롯 확보 {len(_removed_lru)}건): {_removed_lru}"
                            _notify(msg_lru)
                            changed = True
                        available_slots = max(0, MAX_WSS_SUBSCRIBE - len(codes) - len(_mkstatus_sub_codes))
                    candidates = candidates[:available_slots]

                added = _ensure_code_structs(candidates)
                _now_ts = time.time()
                for c in added:
                    _top_added_codes.add(c)
                    _top_added_ts[c] = _now_ts
                    name = code_name_map.get(c, c)
                    _log_top_sub_event(c, name, "추가")
                # [v_1] Top30 신규 추가 종목에 uplimit 프리캐시 (vola_10d, prev_day_ctrt, frgn_3d)
                if UPLIMIT_BUY_ENABLED and added:
                    try:
                        _precache_uplimit_for_new_codes(added)
                    except Exception as _e:
                        logger.warning(f"{ts_prefix()} [uplimit_precache_top] 실패: {_e}")
                added_names = [code_name_map.get(c, c) for c in added]
                added_ctrts = [f"{current_top_ctrt.get(c, 0.0):.2f}%" for c in added]
                if added:
                    msg2 = (
                        f"{ts_prefix()} [top] 구독 추가 (≥{UPLIMIT_SUBSCRIBE_MIN_CTRT}%): "
                        f"{list(zip(added, added_names, added_ctrts))} "
                        f"(총 {len(codes)}/{MAX_WSS_SUBSCRIBE})"
                    )
                    changed = True
                else:
                    if available_slots <= 0:
                        reason = f"상한도달({current_count}/{MAX_WSS_SUBSCRIBE})"
                    elif not candidates:
                        reason = f"후보없음(25%+ Top 중 base/구독중 제외)"
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
    """[260423 재설계]
    Phase 1 (09:00:10~09:05:00): 10초 주기 — 장 시작 직후 급등 포착
    Phase 2 (09:05:00~14:50:00): 1분 주기 — 정규장 상한가 근접 감시
    + 15:18, 15:20 종가매매 전환 스케줄 유지
    """
    base = now.date()
    times: list[datetime] = []

    # Phase 1: 09:00:10 ~ 09:05:00 (10초 주기)
    ph1 = datetime.combine(base, dtime(9, 0, 10), tzinfo=KST)
    ph1_end = datetime.combine(base, dtime(9, 5, 0), tzinfo=KST)
    while ph1 <= ph1_end:
        times.append(ph1)
        ph1 += timedelta(seconds=10)

    # Phase 2: 09:06:00 ~ 14:50:00 (1분 주기, UPLIMIT_BUY_END 까지)
    ph2 = datetime.combine(base, dtime(9, 6, 0), tzinfo=KST)
    ph2_end = datetime.combine(base, dtime(14, 50, 0), tzinfo=KST)
    while ph2 <= ph2_end:
        times.append(ph2)
        ph2 += timedelta(minutes=1)

    # 종가매매 종목 선정 (15:18) + 구독 전환/매수 (15:20)
    times.append(datetime.combine(base, dtime(15, 18), tzinfo=KST))
    times.append(datetime.combine(base, dtime(15, 20), tzinfo=KST))
    return sorted(set(times))

# TR별 "실체결(Y) / 예상체결(N)" 구분
TRID_TO_IS_REAL: dict[str, str] = {}  # tr_id -> "Y"/"N"
TRID_TO_KIND: dict[str, str] = {}     # tr_id -> "regular_real"/"regular_exp"/"overtime_real"/"overtime_exp"

# TRID fallback 매핑 (extract 실패 대비)
KNOWN_TRID_MAP: dict[str, tuple[str, str]] = {
    "H0STCNT0": ("regular_real", "Y"),   # 국내주식 실시간체결가 (KRX) — ccnl_krx
    "H0UNCNT0": ("regular_real", "Y"),   # 국내주식 실시간체결가 (통합) — ccnl_total
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


def _flush_part_buffer(reason: str = "periodic", force_full: bool = False) -> int:
    """
    _part_buffer 를 part 파일로 저장.
    - force_full=False: 1500행 이상 시 1000행 저장·500행 버퍼 유지
    - force_full=True (periodic/shutdown): 남은 데이터 전부 저장
    """
    if not _save_lock.acquire(blocking=False):
        logger.info(f"[part] {reason} skipped (already flushing)")
        return 0
    try:
        return _flush_part_buffer_inner(reason, force_full)
    finally:
        _save_lock.release()


def _flush_part_buffer_inner(reason: str, force_full: bool) -> int:
    """force_full이면 전부 저장, 아니면 1500행 이상 시 1000행 저장·500행 버퍼 유지."""
    global _written_rows, _last_save_time, _part_seq, _part_buffer_rows
    with _part_buffer_lock:
        if not _part_buffer:
            _last_save_time = time.time()
            return 0
        if not force_full and _part_buffer_rows < PART_FLUSH_THRESHOLD:
            return 0
        snapshot = list(_part_buffer)

    try:
        t0 = time.time()
        df = pl.concat(snapshot, how="diagonal_relaxed")
        if "recv_ts" in df.columns:
            df = df.sort("recv_ts")

        total_n = len(df)
        if force_full:
            df_save = df
            df_keep = pl.DataFrame()
        else:
            if total_n < PART_FLUSH_THRESHOLD:
                return 0
            df_save = df.head(PART_FLUSH_SAVE)
            df_keep = df.tail(total_n - PART_FLUSH_SAVE)

        # 종목명 컬럼 추가
        if "name" not in df_save.columns:
            code_col = next(
                (c for c in ("mksc_shrn_iscd", "stck_shrn_iscd", "code") if c in df_save.columns),
                None,
            )
            if code_col:
                try:
                    name_map = _code_name_map()
                    df_save = df_save.with_columns(
                        pl.col(code_col)
                        .cast(pl.Utf8)
                        .str.zfill(6)
                        .map_elements(lambda c: name_map.get(c, ""), return_dtype=pl.Utf8)
                        .alias("name")
                    )
                except Exception:
                    pass

        n = len(df_save)
        yymmdd = datetime.now(KST).strftime("%y%m%d")
        hms = datetime.now(KST).strftime("%H%M%S")
        _part_seq += 1
        part_name = f"{yymmdd}_{hms}_{_part_seq:04d}.parquet"
        part_path = PART_DIR / part_name
        df_save.write_parquet(str(part_path), compression="zstd", use_pyarrow=True)

        # 유지할 데이터를 버퍼에 되돌림 (force_full이면 비움)
        with _part_buffer_lock:
            _part_buffer.clear()
            if len(df_keep) > 0:
                _part_buffer.append(df_keep)
            _part_buffer_rows = len(df_keep)

        t1 = time.time()
        with _lock:
            _written_rows += n
        _last_save_time = time.time()
        _reset_per_sec_stats()
        try:
            size_mb = part_path.stat().st_size / (1024 * 1024)
        except Exception:
            size_mb = -1
        logger.info(
            f"[part] {reason}: rows={n} sec={t1-t0:.3f} "
            f"file={part_name} mb={size_mb:.2f}"
        )
        _emit_save_done(n)
        return n
    except Exception as e:
        logger.error(f"[part] {reason} failed: {e}")
        logger.error(traceback.format_exc())
        return 0


def _merge_parts_to_final() -> int:
    """
    당일 데이터 병합: FINAL(덮어쓰기 형식) + parts(청크 형식) → 중복 제거 후 FINAL 저장.
    FINAL(YYMMDD_wss_data.parquet)는 기존 덮어쓰기 저장 형식으로 오늘 이미 있을 수 있음.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds

    global _written_rows
    yymmdd = datetime.now(KST).strftime("%y%m%d")
    date_parts = sorted(PART_DIR.glob(f"{yymmdd}_*.parquet"))

    tables: list[pa.Table] = []
    # 1) 덮어쓰기 형식 파일 (FINAL) 우선 포함
    if FINAL_PARQUET_PATH.exists() and _is_valid_parquet(FINAL_PARQUET_PATH):
        try:
            tables.append(pq.read_table(str(FINAL_PARQUET_PATH)))
            logger.info(f"[merge] 기존 FINAL 포함: {FINAL_PARQUET_PATH.name}")
        except Exception as e:
            logger.warning(f"[merge] FINAL 읽기 실패: {e}")

    # 2) parts(청크) 파일 개별 로드 — 스키마 불일치 방지
    for p in date_parts:
        try:
            tables.append(pq.read_table(str(p)))
        except Exception as e:
            logger.warning(f"[merge] part 읽기 실패 {p.name}: {e}")
    if date_parts:
        logger.info(f"[merge] parts {len(date_parts)}개 포함")

    if not tables:
        logger.info(f"[merge] 병합할 데이터 없음 (날짜={yymmdd})")
        return 0

    # 스키마 통합: null/large_string→utf8, 테이블 간 string↔numeric 충돌 해소
    def _unify_schema(tbl: pa.Table) -> pa.Table:
        new_fields = []
        for i, field in enumerate(tbl.schema):
            if pa.types.is_null(field.type):
                new_fields.append((i, field.name, pa.utf8()))
            elif pa.types.is_large_string(field.type):
                new_fields.append((i, field.name, pa.utf8()))
        if new_fields:
            for idx, name, new_type in new_fields:
                col = tbl.column(idx).cast(new_type)
                tbl = tbl.set_column(idx, name, col)
        return tbl

    tables = [_unify_schema(t) for t in tables]

    # 테이블 간 동일 컬럼의 타입 충돌 해소 (예: bb_mid가 string vs double)
    if len(tables) > 1:
        # 컬럼별 최종 타입 결정: numeric 타입이 하나라도 있으면 float64로 통일
        col_types: dict[str, set] = {}
        for t in tables:
            for field in t.schema:
                col_types.setdefault(field.name, set()).add(field.type)
        conflict_cols = {name for name, types in col_types.items() if len(types) > 1}
        if conflict_cols:
            logger.info(f"[merge] 타입 충돌 컬럼 통일: {conflict_cols}")
            unified = []
            for t in tables:
                for col_name in conflict_cols:
                    if col_name in t.column_names:
                        idx = t.schema.get_field_index(col_name)
                        ftype = t.schema.field(idx).type
                        has_numeric = any(
                            pa.types.is_floating(tp) or pa.types.is_integer(tp)
                            for tp in col_types[col_name]
                        )
                        if has_numeric and (pa.types.is_string(ftype) or pa.types.is_large_string(ftype)):
                            # string → float64 변환 (변환 불가 값은 null)
                            col = t.column(idx)
                            col = pa.compute.cast(col, pa.float64(), safe=False)
                            t = t.set_column(idx, col_name, col)
                        elif has_numeric and pa.types.is_null(ftype):
                            col = pa.nulls(len(t), type=pa.float64())
                            t = t.set_column(idx, col_name, col)
                unified.append(t)
            tables = unified

    combined = pa.concat_tables(tables, promote_options="permissive") if len(tables) > 1 else tables[0]
    df = combined.to_pandas()

    # 3) code + recv_ts 기준 중복 제거 (덮어쓰기·parts 겹침 방지)
    code_col = next((c for c in ("mksc_shrn_iscd", "stck_shrn_iscd", "code") if c in df.columns), None)
    if code_col and "recv_ts" in df.columns:
        before_n = len(df)
        df = df.drop_duplicates(subset=[code_col, "recv_ts"], keep="last")
        if len(df) < before_n:
            logger.info(f"[merge] 중복 제거: {before_n - len(df)}행")

    if "recv_ts" in df.columns:
        try:
            df = df.sort_values("recv_ts", kind="mergesort").reset_index(drop=True)
        except Exception:
            pass
    combined = pa.Table.from_pandas(df, preserve_index=False)
    tmp_path = FINAL_PARQUET_PATH.with_suffix(".tmp.parquet")
    pq.write_table(combined, tmp_path, compression="zstd", use_dictionary=True)
    tmp_path.replace(FINAL_PARQUET_PATH)

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    for p in date_parts:
        try:
            p.replace(BACKUP_DIR / p.name)
        except Exception as e:
            logger.warning(f"[merge] backup 이동 실패 {p.name}: {e}")

    n = combined.num_rows
    src_desc = f"FINAL+{len(date_parts)}parts" if date_parts else "FINAL만"
    logger.info(f"[merge] 완료: {src_desc} → {FINAL_PARQUET_PATH.name} ({n:,}행)")
    with _lock:
        _written_rows = n
    return n

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
    _price_buf[code] = []
    _bb_sum.pop(code, None)
    _bb_sq_sum.pop(code, None)


def _save_indicator_snapshot() -> None:
    """EMA·BB 인메모리 지표 + 트레일스톱 highest 상태를 스냅샷 파일로 저장.

    저장 내용: 종목별 EMA 값, price_buf (BB 계산용 deque), last_price, tick_count,
              highest (트레일스톱용 최고가).
    복원 시 가격 리플레이 없이 즉시 지표 상태 재구성 가능.
    """
    try:
        # highest 수집: _str1_sell_state에서 미매도 종목의 highest
        highest_map: dict[str, float] = {}
        with _str1_sell_state_lock:
            for code, st in _str1_sell_state.items():
                if not st.get("sold") and st.get("highest", 0) > 0:
                    highest_map[code] = st["highest"]

        # 스냅샷 대상 종목: _ema_state 키 + highest 보유 종목
        all_codes = set(_ema_state.keys()) | set(highest_map.keys())
        rows: list[dict] = []
        for code in all_codes:
            ema = _ema_state.get(code, {})
            buf = _price_buf.get(code)
            buf_list = buf if buf else []
            rows.append({
                "code": code,
                "ema_3": ema.get(3),
                "ema_50": ema.get(50),
                "ema_200": ema.get(200),
                "ema_300": ema.get(300),
                "ema_500": ema.get(500),
                "ema_2000": ema.get(2000),
                "price_buf": buf_list,
                "last_price": buf_list[-1] if buf_list else 0.0,
                "tick_count": len(buf_list),
                "highest": highest_map.get(code, 0.0),
                "snapshot_ts": datetime.now(KST).isoformat(),
            })
        if not rows:
            return
        df = pl.DataFrame(rows)
        tmp = SNAPSHOT_PATH.with_suffix(".tmp.parquet")
        df.write_parquet(str(tmp), compression="zstd", use_pyarrow=True)
        tmp.replace(SNAPSHOT_PATH)
        n_highest = sum(1 for v in highest_map.values() if v > 0)
        msg = f"{ts_prefix()} [snapshot] 저장: {len(rows)}종목 (highest={n_highest}건) → {SNAPSHOT_PATH.name}"
        logger.info(msg)
        sys.stdout.write(f"\n{msg}\n")
        sys.stdout.flush()
    except Exception as e:
        logger.warning(f"[snapshot] 저장 실패: {e}")


def _restore_from_snapshot() -> bool:
    """스냅샷 파일에서 EMA·BB 지표 + highest 상태 직접 복원. 성공 시 True.

    가격 리플레이 없이 직접 복원하므로 1초 이내 완료.
    스냅샷이 없거나 당일 장중(09:00 이후) 것이 아니면 False → 새로 시작.
    """
    if not SNAPSHOT_PATH.exists() or not _is_valid_parquet(SNAPSHOT_PATH):
        return False
    try:
        df = pl.read_parquet(str(SNAPSHOT_PATH))
        if "code" not in df.columns or "price_buf" not in df.columns:
            return False

        # ── 스냅샷 시각 검증: 당일 09:00 이후에 저장된 것만 유효 ──
        if "snapshot_ts" in df.columns and len(df) > 0:
            snap_ts_str = str(df["snapshot_ts"][0])
            try:
                snap_dt = datetime.fromisoformat(snap_ts_str)
                market_open = datetime.now(KST).replace(
                    hour=9, minute=0, second=0, microsecond=0
                )
                if snap_dt < market_open:
                    msg = (f"{ts_prefix()} [restore] 스냅샷이 당일 장 시작 전 데이터 → 스킵 "
                           f"(snapshot_ts={snap_ts_str})")
                    logger.info(msg)
                    sys.stdout.write(f"{msg}\n")
                    sys.stdout.flush()
                    return False
            except (ValueError, TypeError) as e:
                logger.warning(f"[restore] snapshot_ts 파싱 실패: {e}")
                return False

        restored = 0
        highest_restored = 0
        for row in df.iter_rows(named=True):
            code = str(row["code"]).zfill(6)
            if code not in _ema_state:
                _init_indicator_buf(code)
            # EMA 상태 복원
            ema = _ema_state[code]
            for n in INDICATOR_MA_LINE:
                key = f"ema_{n}"
                val = row.get(key)
                if val is not None:
                    ema[n] = float(val)
            # price_buf 복원 (BB 계산용)
            buf_data = row.get("price_buf", [])
            if buf_data:
                buf_list = [float(x) for x in buf_data[-_IND_MAX_WINDOW:]]
                _price_buf[code] = buf_list
                # BB incremental 캐시 초기화
                bb_window = buf_list[-INDICATOR_BB_PERIOD:]
                if len(bb_window) >= INDICATOR_BB_PERIOD:
                    _bb_sum[code] = sum(bb_window)
                    _bb_sq_sum[code] = sum(x * x for x in bb_window)
            # highest 복원 (트레일스톱용)
            h_val = row.get("highest", 0.0)
            if h_val and float(h_val) > 0:
                with _str1_sell_state_lock:
                    if code in _str1_sell_state and not _str1_sell_state[code].get("sold"):
                        cur = _str1_sell_state[code].get("highest", 0.0)
                        if float(h_val) > cur:
                            _str1_sell_state[code]["highest"] = float(h_val)
                            highest_restored += 1
            restored += 1

        msg = f"{ts_prefix()} [restore] 스냅샷 즉시 복원: {restored}종목, highest={highest_restored}건 ({SNAPSHOT_PATH.name})"
        logger.info(msg)
        sys.stdout.write(f"{msg}\n")
        sys.stdout.flush()
        return restored > 0
    except Exception as e:
        logger.warning(f"[restore] 스냅샷 복원 실패: {e}")
        return False


def _restore_state_on_startup() -> None:
    """
    재시작 시 지표 상태 복원. 스냅샷 우선 → parts 리플레이 폴백.

    1순위: 스냅샷 파일 (_indicator_snapshot.parquet) → 직접 복원 (1초 이내)
    2순위: FINAL + parts 파일 → 가격 리플레이 복원 (기존 방식)

    스냅샷 복원 후에도 스냅샷 이후에 생성된 parts가 있으면 추가 리플레이.
    """
    try:
        # 장 시작 전(09:00 이전)에는 당일 데이터 없으므로 복원 불필요
        if datetime.now(KST).time() < dtime(9, 0):
            logger.info("[restore] 장 시작 전 → 복원 스킵")
            sys.stdout.write(f"{ts_prefix()} [restore] 장 시작 전 → 복원 스킵\n")
            sys.stdout.flush()
            return

        # 1순위: 스냅샷 직접 복원
        snapshot_restored = _restore_from_snapshot()
        snapshot_mtime = 0.0
        if snapshot_restored and SNAPSHOT_PATH.exists():
            snapshot_mtime = SNAPSHOT_PATH.stat().st_mtime
            # 스냅샷 이후 생성된 parts만 추가 리플레이
            new_parts = [p for p in sorted(PART_DIR.glob("*.parquet"))
                         if p.stat().st_mtime > snapshot_mtime]
            if new_parts:
                logger.info(f"[restore] 스냅샷 이후 parts {len(new_parts)}개 추가 리플레이")
                _replay_prices_from_files([str(p) for p in new_parts])
            return

        # 2순위: 기존 방식 (FINAL + parts 리플레이)
        sources: list[str] = []
        parts: list[Path] = []

        if FINAL_PARQUET_PATH.exists() and _is_valid_parquet(FINAL_PARQUET_PATH):
            sources.append(str(FINAL_PARQUET_PATH))
            logger.info(f"[restore] FINAL 파일 발견: {FINAL_PARQUET_PATH.name}")

        parts = sorted(PART_DIR.glob("*.parquet"))
        if parts:
            sources.extend(str(p) for p in parts)
            logger.info(f"[restore] parts 파일 {len(parts)}개 발견 → 리플레이")

        if not sources:
            logger.info("[restore] 복원할 파일 없음 → 0부터 시작")
            return

        _replay_prices_from_files(sources)

        msg = (
            f"[restore] 복원 완료: parts={len(parts)}개, "
            f"FINAL={'있음' if FINAL_PARQUET_PATH.exists() else '없음'}"
        )
        logger.info(msg)
        sys.stdout.write(f"{msg}\n")
        sys.stdout.flush()

    except Exception as e:
        logger.warning(f"[restore] 복원 실패 (무시하고 계속): {e}\n{traceback.format_exc()}")


def _replay_prices_from_files(sources: list[str]) -> None:
    """파일 목록에서 가격 데이터를 읽어 _calc_indicators 리플레이."""
    dfs: list[pl.DataFrame] = []
    for src in sources:
        try:
            dfs.append(pl.read_parquet(src))
        except Exception as _e:
            logger.warning(f"[restore] 파일 읽기 실패, 스킵: {src} → {_e}")
    if not dfs:
        return
    df = pl.concat(dfs, how="diagonal_relaxed")

    code_col = None
    for c in ("mksc_shrn_iscd", "stck_shrn_iscd", "code"):
        if c in df.columns:
            code_col = c
            break
    if not code_col:
        return

    if "recv_ts" in df.columns:
        df = df.sort("recv_ts")
    df = df.with_columns(
        pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
    )

    restored_codes: list[str] = []
    if "stck_prpr" in df.columns:
        df_ind = df.with_columns(
            pl.col("stck_prpr").cast(pl.Float64, strict=False).alias("stck_prpr")
        ).drop_nulls("stck_prpr")
        for df_code in df_ind.partition_by(code_col, maintain_order=True):
            code = str(df_code[code_col][0])
            if code not in _ema_state:
                _init_indicator_buf(code)
            for pr in df_code["stck_prpr"].to_list():
                if pr and pr > 0:
                    _calc_indicators(code, float(pr))
            restored_codes.append(code)

    logger.info(f"[restore] 리플레이 완료: {len(restored_codes)}종목 / {len(df)}행")


def _calc_indicators(code: str, price: float) -> dict:
    """틱 1개 수신 시 EMA + 볼린저 밴드 계산.

    EMA: alpha = 2/(period+1), 첫 틱은 가격 그대로 초기값으로 설정.
    BB:  price_buf 누적값으로 rolling std 계산 (기간 미달 시 None).
    반환: {ma3, ma50, ..., bb_mid, bb_upper, bb_lower, bb_width} dict.

    ※ 1회용 성능측정: _ind_calc_measure_enabled=True 시 60s 주기 max/avg/n 로그.
       측정이 필요 없어지면 `_ind_calc_measure_enabled = False` 로 끌 수 있음.

    ※ CLOSE_REAL(15:30~) 이후 모드에서는 매매 판단에 지표 불필요 → 계산 스킵.
       hot path 부담 감소 + 15:30 구독전환 시점 ingest_lag spike 완화.
    """
    # 15:30 이후 모드: 지표 계산 불필요 → 즉시 반환 (hot path 비용 제거)
    if _current_mode in (RunMode.CLOSE_REAL, RunMode.OVERTIME_EXP,
                         RunMode.OVERTIME_REAL, RunMode.STOP, RunMode.EXIT):
        return {}

    _t0 = time.perf_counter() if _ind_calc_measure_enabled else 0.0

    buf = _price_buf.get(code)
    if buf is None:
        return {}

    state = _ema_state.get(code)
    if state is None:
        return {}

    # ── 첫 틱: EMA 초기화 ──
    if len(buf) == 0:
        for n in INDICATOR_MA_LINE:
            state[n] = price

    # price_buf 축적 (pre-fill 없이 자연 축적)
    buf.append(price)
    if len(buf) > _IND_MAX_WINDOW:
        buf[:] = buf[-_IND_MAX_WINDOW:]

    # ── EMA 계산 ──
    ema_vals: dict[str, float] = {}
    for n in INDICATOR_MA_LINE:
        alpha = 2.0 / (n + 1)
        prev = state.get(n)
        if prev is None:
            state[n] = price
        else:
            state[n] = prev + alpha * (price - prev)
        ema_vals[f"ma{n}"] = state[n]

    # ── BB incremental 계산 ──
    bb_vals: dict[str, float | None] = {}
    bb_n = INDICATOR_BB_PERIOD
    s = _bb_sum.get(code)
    sq = _bb_sq_sum.get(code)

    if s is None or sq is None:
        # 캐시 미초기화 → 현재 buf 기준 1회 계산
        window = buf[-bb_n:]
        s = sum(window)
        sq = sum(x * x for x in window)
    elif len(buf) > bb_n:
        # 슬라이딩: 새 값 추가 + 오래된 값 제거
        old_price = buf[-bb_n - 1]
        s += price - old_price
        sq += price * price - old_price * old_price
    else:
        # 축적 중 (< 200개): 추가만
        s += price
        sq += price * price

    _bb_sum[code] = s
    _bb_sq_sum[code] = sq

    count = min(len(buf), bb_n)
    if count >= 2:
        mean = s / count
        variance = max(0.0, sq / count - mean * mean)
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

    # ── [측정] 1tick 처리 소요시간 — 60s 주기 max/avg/n 로그 ──
    # 측정 자체가 본함수 반환을 방해하지 않도록 try/except 로 감쌈
    if _ind_calc_measure_enabled:
        try:
            _dt = (time.perf_counter() - _t0) * 1e6   # 마이크로초
            global _ind_calc_t_sum, _ind_calc_t_max, _ind_calc_t_n, _ind_calc_last_print_ts
            _ind_calc_t_sum += _dt
            if _dt > _ind_calc_t_max:
                _ind_calc_t_max = _dt
            _ind_calc_t_n += 1
            now_t = time.time()
            if now_t - _ind_calc_last_print_ts >= 60.0:
                avg = _ind_calc_t_sum / max(1, _ind_calc_t_n)
                logger.info(
                    f"{ts_prefix()} [ind_calc] 1tick 소요 max={_ind_calc_t_max:.1f}us "
                    f"avg={avg:.2f}us n={_ind_calc_t_n} (직전 60s)"
                )
                _ind_calc_t_sum = 0.0
                _ind_calc_t_max = 0.0
                _ind_calc_t_n = 0
                _ind_calc_last_print_ts = now_t
        except Exception:
            pass

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
      ② up_trend 지속 상태에서 ma500 < ma2000 데드크로스 (상태 기반, 매도 성공까지 유지)
    """
    if not _str1_sell_state:
        return

    is_opening_call_auction = (kind == "regular_exp")
    is_regular   = (kind == "regular_real")
    if not is_opening_call_auction and not is_regular:
        return

    df_tmp = result.with_columns(
        pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
    )

    pr_col      = col_map.get("stck_prpr") or col_map.get("antc_prce")
    prdy_col    = col_map.get("prdy_ctrt")
    wghn_col    = col_map.get("wghn_avrg_stck_prc")
    bidp1_col   = col_map.get("bidp1")
    hgpr_col    = col_map.get("stck_hgpr")
    oprc_col    = col_map.get("stck_oprc")

    global _opening_call_auction_last_summary_ts
    now_ts = time.time()
    newly_triggered: list[str] = []   # 이번 틱에서 처음 매도조건 진입한 종목

    for df_code in df_tmp.partition_by(code_col, maintain_order=True):
        code = str(df_code[code_col][0])
        with _str1_sell_state_lock:
            st = _str1_sell_state.get(code)
            if not st or st.get("sold"):
                _opening_call_auction_watch.pop(code, None)       # 매도 완료 → 모니터링 캐시 제거
                continue
            # 사전 주문 넣은 상태
            if st.get("opening_call_auction_ordered"):
                if is_regular:
                    continue  # 09:00 이후, 시초가 체결 예정/체결됨
                # is_opening_call_auction: 예상가 복귀 시 취소 검사 (아래에서 처리)

        row = df_code.row(-1, named=True)  # Polars: 마지막 행을 dict로 반환

        if is_opening_call_auction:
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
                opening_call_auction_ordered = st2.get("opening_call_auction_ordered", False)

            if opening_call_auction_ordered:
                # 이미 장전 매도 주문 넣음 → 예상가 복귀 시 취소
                should_cancel, cancel_reason = check_opening_call_auction_cancel(antc, prdy, buy_price)
                if should_cancel:
                    _enqueue_str1_opening_call_auction_cancel(code, cancel_reason)
                # 모니터링은 "체결대기" 상태로 표시 (in_sell_cond=False, 유지)
                prev_watch = _opening_call_auction_watch.get(code, {})
                _opening_call_auction_watch[code] = {
                    **prev_watch,
                    "antc_prce": antc, "prdy_ctrt": prdy, "buy_price": buy_price,
                    "in_sell_cond": False, "name": name, "qty": qty,
                    "reason": "(체결대기·취소검사중)",
                    "first_sell_ts": prev_watch.get("first_sell_ts"),
                    "first_printed": prev_watch.get("first_printed", False),
                }
                continue

            # no_sell 종목은 OCA 매도 스킵, 모니터링 캐시만 갱신
            if code in _no_sell_codes:
                prev_watch = _opening_call_auction_watch.get(code, {})
                _opening_call_auction_watch[code] = {
                    **prev_watch,
                    "antc_prce": antc, "prdy_ctrt": prdy, "buy_price": buy_price,
                    "in_sell_cond": False, "name": name, "qty": qty,
                    "reason": "[no_sell]",
                    "first_sell_ts": prev_watch.get("first_sell_ts"),
                    "first_printed": prev_watch.get("first_printed", False),
                }
                continue

            should_sell, reason = check_opening_call_auction_sell(antc, prdy)

            # ── 장전 모니터링 상태 업데이트 ──
            prev_watch = _opening_call_auction_watch.get(code, {})
            was_in_cond = prev_watch.get("in_sell_cond", False)
            _opening_call_auction_watch[code] = {
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
            bidp1 = 0.0; wghn = 0.0; oprc = 0.0; prdy_v = 0.0; stck_prpr = 0.0; hgpr = 0.0
            try:
                if bidp1_col: bidp1 = float(str(row.get(bidp1_col) or 0).replace(",", "") or 0)
                if wghn_col:  wghn  = float(str(row.get(wghn_col)  or 0).replace(",", "") or 0)
                if oprc_col:  oprc  = float(str(row.get(oprc_col)  or 0).replace(",", "") or 0)
                # 시가 캐시: 값이 있으면 저장, 없으면 캐시에서 복원
                if oprc > 0:
                    _oprc_cache[code] = oprc
                elif code in _oprc_cache:
                    oprc = _oprc_cache[code]
                if prdy_col:  prdy_v = float(str(row.get(prdy_col) or 0).replace(",", "") or 0)
                if pr_col:    stck_prpr = float(str(row.get(pr_col) or 0).replace(",", "") or 0)
                if hgpr_col:  hgpr  = float(str(row.get(hgpr_col)  or 0).replace(",", "") or 0)
            except (ValueError, TypeError):
                continue
            # buy_price 첫 틱 보정 (regular_real에도 prdy_ctrt 있을 수 있음)
            if bidp1 > 0 and abs(prdy_v) < 100:
                try:
                    t_close = bidp1 / (1 + prdy_v / 100)
                    with _str1_sell_state_lock:
                        if _str1_sell_state.get(code, {}).get("buy_price", 0) == 0 and t_close > 0:
                            _str1_sell_state[code]["buy_price"] = t_close
                except (ValueError, TypeError):
                    pass
            # EMA 상태에서 ma50, ma200, ma300, ma500, ma2000 조회
            ema = _ema_state.get(code, {})
            ma50   = ema.get(50)   or 0.0
            ma200  = ema.get(200)  or 0.0
            ma300  = ema.get(300)  or 0.0
            ma500  = ema.get(500)  or 0.0
            ma2000 = ema.get(2000) or 0.0

            # ── 상한가 근접(29%+) 클라이언트 스톱 매도 (연속 N틱 이탈 확인) ──
            if code not in _no_sell_codes:
                with _str1_sell_state_lock:
                    _st_sl = _str1_sell_state.get(code, {})
                    _sl_ordered = _st_sl.get("stop_limit_ordered", False)
                    _sl_sold = _st_sl.get("sold", False)
                    _sl_monitoring = _st_sl.get("sl_monitoring", False)
                    _sl_cndt = _st_sl.get("sl_cndt_price", 0)
                    _sl_sell = _st_sl.get("sl_sell_price", 0)
                    _sl_below = _st_sl.get("sl_below_count", 0)
                if not _sl_ordered and not _sl_sold and bidp1 > 0:
                    if prdy_v >= 29.0 and not _sl_monitoring:
                        # ▶ 첫 29%+ 감지 → 모니터링 시작 (주문 안 함)
                        prev_close = bidp1 / (1 + prdy_v / 100)
                        if prev_close > 0:
                            _cndt = int(prev_close * 1.29)
                            _sell = int(prev_close * 1.28)
                            with _str1_sell_state_lock:
                                if code in _str1_sell_state:
                                    _str1_sell_state[code]["sl_monitoring"] = True
                                    _str1_sell_state[code]["sl_cndt_price"] = _cndt
                                    _str1_sell_state[code]["sl_sell_price"] = _sell
                                    _str1_sell_state[code]["sl_below_count"] = 0
                            logger.info(
                                f"{ts_prefix()} [스톱] {code} 29%+ 모니터링 시작"
                                f" (조건={_cndt:,}, 지정={_sell:,})"
                            )
                    elif _sl_monitoring and _sl_cndt > 0:
                        # ▶ 모니터링 중 → 이탈 확인
                        if bidp1 < _sl_cndt:
                            new_count = _sl_below + 1
                            with _str1_sell_state_lock:
                                if code in _str1_sell_state:
                                    _str1_sell_state[code]["sl_below_count"] = new_count
                            if new_count >= STOP_LIMIT_CONFIRM_TICKS:
                                # N틱 연속 이탈 확인 → 지정가 매도
                                _enqueue_str1_sell(
                                    code,
                                    f"str1_상한가_스톱({new_count}틱이탈,"
                                    f"조건={_sl_cndt:,},지정={_sl_sell:,})",
                                    ref_price=_sl_sell, ord_dvsn="00",
                                    price=_sl_sell,
                                )
                            else:
                                logger.debug(
                                    f"{ts_prefix()} [스톱] {code} 이탈 {new_count}/{STOP_LIMIT_CONFIRM_TICKS}"
                                    f" bidp1={bidp1:,} < 조건={_sl_cndt:,}"
                                )
                        else:
                            # 29% 이상 복귀 → 카운트 리셋
                            if _sl_below > 0:
                                with _str1_sell_state_lock:
                                    if code in _str1_sell_state:
                                        _str1_sell_state[code]["sl_below_count"] = 0

            # ── VI 포지션 매도 판단 ──
            vi_pos = _vi_positions.get(code)
            with _str1_sell_state_lock:
                _src = _str1_sell_state.get(code, {}).get("source", "")
            if vi_pos and not vi_pos.get("sold") and _src == "vi":
                # VI 최고가 갱신 (VI 매수 시점 이후 bidp1 기준, 일중최고가 사용 안 함)
                if bidp1 > vi_pos.get("highest", 0):
                    vi_pos["highest"] = bidp1
                if code not in _no_sell_codes:
                    vi_buy_price = vi_pos.get("buy_price", 0)
                    vi_buy_ts = vi_pos.get("buy_ts")
                    minutes_since = (datetime.now(KST) - vi_buy_ts).total_seconds() / 60.0 if vi_buy_ts else 999.0
                    vi_sell, vi_reason = check_vi_sell(
                        bidp1, wghn, ma50, ma200, ma300, ma500, ma2000, oprc,
                        highest_since_buy=vi_pos.get("highest", 0),
                        buy_price=vi_buy_price,
                        minutes_since_buy=minutes_since,
                    )
                    if vi_sell:
                        vi_pos["sold"] = True
                        vi_msg = f"{ts_prefix()} [VI매도] {name}({code}) 매도결정  사유={vi_reason}  매수가={vi_buy_price:,.0f}  현재가={bidp1:,.0f}"
                        logger.info(vi_msg)
                        _notify(vi_msg, tele=True)
                        _enqueue_str1_sell(code, vi_reason, bidp1)
            else:
                # ── Str1 일반 매도 판단 (트레일스톱 포함) ──
                with _str1_sell_state_lock:
                    _st = _str1_sell_state.get(code, {})
                    _buy_p = _st.get("buy_price", 0.0)
                    _highest = _st.get("highest", 0.0)
                # Str1 최고가 갱신 (일중최고가 vs 매수호가 중 큰 값) — no_sell이어도 유지
                tick_high = max(bidp1, _highest)
                if tick_high > _highest:
                    _highest = tick_high
                    with _str1_sell_state_lock:
                        if code in _str1_sell_state:
                            _str1_sell_state[code]["highest"] = _highest
                if code not in _no_sell_codes:
                    should_sell, reason, cur_up = check_realtime_sell(
                        bidp1, wghn, ma50, ma200, ma300, ma500, ma2000, oprc,
                        was_up_trend=_up_trend_state.get(code, False),
                        highest_since_buy=_highest,
                        buy_price=_buy_p,
                    )
                    _up_trend_state[code] = cur_up

                    if should_sell:
                        if "데드크로스" in reason:
                            _ema_sell_cond[code] = True     # 매도 성공까지 유지용
                        _enqueue_str1_sell(code, reason, bidp1)
                    elif _buy_p > 0 and bidp1 > 0:
                        # ── 매수가 손절 가드 (단일 틱 fake 차단) ──
                        # 09:00:00 ~ 09:00:00+OPENING_GRACE_SEC: 임계 강화 (-7%, 우선주 첫 틱 1주 보호)
                        # 그 외: 기존 -3% 임계
                        # 두 경우 모두 LOSS_CONFIRM_TICKS 연속 이탈 시에만 매도 발동.
                        # 임계 위로 회복하면 카운터 리셋.
                        _now_kst_t = datetime.now(KST).time()
                        in_opening_grace = (
                            dtime(9, 0, 0) <= _now_kst_t < dtime(9, 0, OPENING_GRACE_SEC)
                        )
                        loss_threshold_pct = OPENING_GRACE_LOSS_PCT if in_opening_grace else 0.03
                        loss_threshold_price = _buy_p * (1 - loss_threshold_pct)
                        if bidp1 < loss_threshold_price:
                            with _str1_sell_state_lock:
                                _prev_cnt = _str1_sell_state.get(code, {}).get("loss_below_count", 0)
                                _new_cnt = _prev_cnt + 1
                                if code in _str1_sell_state:
                                    _str1_sell_state[code]["loss_below_count"] = _new_cnt
                            if _new_cnt >= LOSS_CONFIRM_TICKS:
                                loss_pct = (bidp1 / _buy_p - 1) * 100
                                _grace_tag = ",grace" if in_opening_grace else ""
                                _enqueue_str1_sell(
                                    code,
                                    f"매수가손절({loss_pct:.2f}%,매수={int(_buy_p):,},{_new_cnt}틱{_grace_tag})",
                                    bidp1,
                                )
                            else:
                                logger.debug(
                                    f"{ts_prefix()} [매수가손절_보류] {code} bidp1={bidp1:,} "
                                    f"< 임계{loss_threshold_pct*100:.0f}%={int(loss_threshold_price):,} "
                                    f"({_new_cnt}/{LOSS_CONFIRM_TICKS}틱"
                                    f"{',grace' if in_opening_grace else ''})"
                                )
                            # loss-pending 중에는 ema 데드크로스 재시도도 보류 (원본 elif 체인의 'loss 우선' 의미 유지)
                        else:
                            # 임계 위로 회복 → 카운터 리셋 후, ema 데드크로스 재시도 평가
                            with _str1_sell_state_lock:
                                if (
                                    code in _str1_sell_state
                                    and _str1_sell_state[code].get("loss_below_count", 0) > 0
                                ):
                                    _str1_sell_state[code]["loss_below_count"] = 0
                            if _ema_sell_cond.get(code, False):
                                # 데드크로스 재시도는 전일종가 위일 때만
                                _enqueue_str1_sell(code, "str1_Up_trend_ma500_ma2000_데드크로스", bidp1)
                    elif _ema_sell_cond.get(code, False):
                        # 데드크로스 재시도는 전일종가 위일 때만 (buy_price/bidp1 미확보 fallback)
                        _enqueue_str1_sell(code, "str1_Up_trend_ma500_ma2000_데드크로스", bidp1)

            # ── 정규장 실시간 체결 시 모니터링 캐시 갱신 (stale 방지) ──
            with _str1_sell_state_lock:
                st2 = _str1_sell_state.get(code, {})
                buy_price = st2.get("buy_price", 0)
                name = st2.get("name", code)
                qty = int(st2.get("qty", 0))
            price = stck_prpr if stck_prpr > 0 else (bidp1 if bidp1 > 0 else _last_stck_prpr.get(code, 0))
            prev_watch = _opening_call_auction_watch.get(code, {})
            _opening_call_auction_watch[code] = {
                **prev_watch,
                "antc_prce": price,
                "prdy_ctrt": prdy_v,
                "buy_price": buy_price,
                "in_sell_cond": prdy_v < 0,
                "name": name,
                "qty": qty,
                "reason": f"전일종가하락({prdy_v:.2f}%)" if prdy_v < 0 else "",
            }

    # ── Str1 모니터링 출력 (장전 08:29~09:00 / 장중 09:00~15:20 / 장마감 15:20~15:30) ──
    if (is_opening_call_auction or is_regular) and _opening_call_auction_watch:
        _print_opening_call_auction_monitor(newly_triggered, now_ts, is_opening_call_auction=is_opening_call_auction)


# =============================================================================
# [v_1 신규] 상한가 근접 매수 — 틱 기반 25~28% 감지 + Exit 모니터링
# =============================================================================
def _check_uplimit_conditions_from_tick(
    result: "pl.DataFrame",
    col_map: dict,
    code_col: str,
    kind: str,
) -> None:
    """
    ingest_loop 에서 호출: 정규장 체결 틱마다 [260423 재설계]
      1) 25% 최초 돌파 기록 (_uplimit_25pct_cross_ts)
      2) **28% 상향 돌파 순간** edge-trigger → _try_uplimit_buy (매수 판단)
      3) 29.5% 도달 감지 → 스톱 지정가 주문 세트 발주 (_setup_stop_limit_orders)
      4) 25% 하회 감지 → 기존 스톱 취소 + 시장가 전량 매도 (_try_uplimit_market_sell)
      5) 20% 하회 + 미매수 종목 → 구독 해제 (_top_added_codes 에서 제거)
    """
    if not UPLIMIT_BUY_ENABLED:
        return
    if kind != "regular_real":
        return

    # [260424] v4 분기 — UPLIMIT_V4_ENABLED=True 면 v4 로직만 수행, 기존 edge-trigger 건너뜀
    if UPLIMIT_V4_ENABLED:
        try:
            _check_uplimit_v4_from_tick(result, col_map, code_col, kind)
        except Exception as _ev4:
            logger.debug(f"[uplimit_v4] 예외: {_ev4}")
        return

    pr_col    = col_map.get("stck_prpr")
    prdy_col  = col_map.get("prdy_ctrt")
    oprc_col  = col_map.get("stck_oprc")
    vol_col   = col_map.get("acml_vol")
    bidp1_col = col_map.get("bidp1")
    ask_sum_cols = [col_map.get(f"askp_rsqn{i}") for i in range(1, 6)]
    bid_sum_cols = [col_map.get(f"bidp_rsqn{i}") for i in range(1, 6)]

    df_tmp = result.with_columns(
        pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
    )
    now_dt = datetime.now(KST)
    h, m = now_dt.hour, now_dt.minute
    in_window = (UPLIMIT_BUY_START_HM <= (h, m) < UPLIMIT_BUY_END_HM)

    for df_code in df_tmp.partition_by(code_col, maintain_order=True):
        try:
            code = str(df_code[code_col][0])
            if not code or code in ("None", "000000"):
                continue
            row = df_code.row(-1, named=True)

            def _f(col):
                if not col:
                    return 0.0
                try:
                    return float(str(row.get(col) or 0).replace(",", "") or 0)
                except Exception:
                    return 0.0

            prdy_ctrt = _f(prdy_col)
            stck_prpr = _f(pr_col)
            stck_oprc = _f(oprc_col) or _oprc_cache.get(code, 0.0)
            acml_vol  = _f(vol_col)
            bidp1     = _f(bidp1_col)

            # prev_ctrt (edge-trigger 용): 반드시 _last_prdy_ctrt 갱신 전 값 사용
            prev_ctrt = _last_prdy_ctrt.get(code, 0.0)

            _update_uplimit_minute_vol(code, acml_vol, now_dt)

            # 25% 최초 돌파 시각 기록
            if code not in _uplimit_25pct_cross_ts and 25.0 <= prdy_ctrt < 29.0:
                _uplimit_25pct_cross_ts[code] = now_dt

            # ── 보유 포지션 관리 ──────────────────────────────────────────
            if code in _uplimit_positions:
                pos = _uplimit_positions[code]
                if prdy_ctrt > pos.get("max_prdy_ctrt", 0):
                    pos["max_prdy_ctrt"] = prdy_ctrt

                # (3) 상한가 근접 29.5% 도달 시 스톱 지정가 주문 세트 발주
                if (pos.get("max_prdy_ctrt", 0) >= UPLIMIT_REACH_UPPER_PCT
                    and not pos.get("stop_orders_placed", False)):
                    try:
                        _setup_stop_limit_orders(code, pos)
                    except Exception as _se:
                        logger.warning(f"{ts_prefix()} [uplimit_stop] {code} 스톱주문 실패: {_se}")

                # (4) 25% 하회 감지 시 기존 스톱 취소 + 시장가 전량 매도
                if prdy_ctrt < UPLIMIT_EXIT_MARKET_SELL_CTRT:
                    try:
                        _try_uplimit_market_sell(code, reason=f"uplimit_25%하회({prdy_ctrt:.2f}%)")
                    except Exception as _me:
                        logger.warning(f"{ts_prefix()} [uplimit_mkt] {code} 시장가매도 실패: {_me}")

            # ── 미매수 구독 종목 20% 하회 → 자동 해제 ────────────────────
            elif (code in _top_added_codes
                  and code not in _uplimit_buy_pending
                  and code not in _uplimit_exit_pending
                  and prdy_ctrt < UPLIMIT_UNSUBSCRIBE_CTRT):
                try:
                    _auto_unsub_code(code, reason=f"20%하회({prdy_ctrt:.2f}%)")
                except Exception as _ue:
                    logger.warning(f"{ts_prefix()} [uplimit_unsub] {code} 해제 실패: {_ue}")

            # ── 매수 판단 (edge-trigger: 28% 상향 돌파 순간만) ─────────────
            if not in_window:
                continue
            # prev < 28 <= cur < 29.5 인 경우만 signal 호출 (성능)
            if not (prev_ctrt < UPLIMIT_EXIT_TRIGGER_CTRT <= prdy_ctrt < UPLIMIT_REACH_UPPER_PCT):
                continue

            name = code_name_map.get(code, code)
            prev_close = 0.0
            if stck_prpr > 0 and abs(prdy_ctrt) < 100:
                prev_close = stck_prpr / (1 + prdy_ctrt / 100.0)

            bid_sum = sum(_f(c) for c in bid_sum_cols)
            ask_sum = sum(_f(c) for c in ask_sum_cols)

            _try_uplimit_buy(
                code=code, name=name,
                prdy_ctrt=prdy_ctrt, stck_prpr=stck_prpr,
                stck_oprc=stck_oprc, prev_close=prev_close,
                acml_vol=acml_vol,
                bid_sum_top5=bid_sum, ask_sum_top5=ask_sum,
                now_dt=now_dt,
                prev_ctrt=prev_ctrt,
            )
        except Exception as _e:
            logger.debug(f"[uplimit_tick] 예외 {code if 'code' in locals() else '?'}: {_e}")


# =============================================================================
# [260427] Strategy A-v5 — 구독 즉시 ma10 상회 시 시장가 매수
#   v4 (28% 5분유지) 폐지. 호환을 위해 함수명 _check_uplimit_v4_from_tick 유지.
#   매수: 25%≤prdy_ctrt<30%, 10틱 누적 후 stck_prpr>ma10 첫 시점 시장가
#   Exit: 트레일 3% (max_prdy_ctrt≥29 도달 후) > 하드 5% > 14:55
#   상한가(≥30%) 도달 이력 종목은 _uplimit_v5_limitup_reached 에 기록 → 매수 차단
#   저거래(전일/당일 거래대금 50억 미만) 차단
#   20%~25% 구간은 _uplimit_v5_observe_log 에 1회 기록 (관찰 모드, 매수 안 함)
# =============================================================================
def _check_uplimit_v4_from_tick(
    result: "pl.DataFrame",
    col_map: dict,
    code_col: str,
    kind: str,
) -> None:
    """v5 매수/Exit 로직 (함수명은 호환 위해 v4 유지)."""
    _v5_diag_counters["fn_called"] += 1
    _v5_diag_dump_if_due()
    if kind != "regular_real":
        _v5_diag_counters["kind_not_regular_real"] += 1
        return

    pr_col    = col_map.get("stck_prpr")
    prdy_col  = col_map.get("prdy_ctrt")
    bidp1_col = col_map.get("bidp1")
    acml_col  = col_map.get("acml_tr_pbmn")
    oprc_col  = col_map.get("stck_oprc")
    acml_vol_col = col_map.get("acml_vol")
    # [260429] F11: 전체 호가 잔량 (top5 합산 → TOTAL 사용)
    bid_total_col = col_map.get("total_bidp_rsqn")
    ask_total_col = col_map.get("total_askp_rsqn")
    # [260429] F12: 체결강도 CTTR 직접 사용 (REST volume_power 호출 제거)
    cttr_col = col_map.get("cttr")

    df_tmp = result.with_columns(
        pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
    )
    now_dt = datetime.now(KST)
    h, m = now_dt.hour, now_dt.minute

    for df_code in df_tmp.partition_by(code_col, maintain_order=True):
        try:
            _v5_diag_counters["rows_processed"] += 1
            code = str(df_code[code_col][0])
            if not code or code in ("None", "000000"):
                _v5_diag_counters["code_invalid"] += 1
                continue
            row = df_code.row(-1, named=True)

            def _f(col):
                if not col:
                    return 0.0
                try:
                    return float(str(row.get(col) or 0).replace(",", "") or 0)
                except Exception:
                    return 0.0

            prdy_ctrt = _f(prdy_col)
            stck_prpr = _f(pr_col)
            bidp1     = _f(bidp1_col) or stck_prpr
            stck_oprc = _f(oprc_col)
            acml_vol  = _f(acml_vol_col)
            acml_tr_pbmn = _f(acml_col)
            # [260429] F11: 전체 호가 잔량 / F12: WSS CTTR 체결강도
            bid_total = _f(bid_total_col)
            ask_total = _f(ask_total_col)
            cttr_wss  = _f(cttr_col)
            if acml_tr_pbmn > 0:
                _uplimit_v5_last_acml[code] = acml_tr_pbmn

            # ── 상한가(≥30%) 도달 이력 기록 ─────────────────────────────
            if prdy_ctrt >= UPLIMIT_V5_BUY_BAND_HIGH:
                _v5_diag_counters["ctrt_at_or_above_30"] += 1
                _uplimit_v5_limitup_reached.add(code)

            # ── 보유 포지션 관리 (source=v5; v4 호환 유지) ────────────────
            if code in _uplimit_positions and _uplimit_positions[code].get("source") in ("v5", "v4"):
                _v5_diag_counters["in_position_path"] += 1
                pos = _uplimit_positions[code]
                if bidp1 > pos.get("highest_since_buy", 0):
                    pos["highest_since_buy"] = bidp1
                if prdy_ctrt > pos.get("max_prdy_ctrt", 0):
                    pos["max_prdy_ctrt"] = prdy_ctrt
                cur_price = bidp1 if bidp1 > 0 else stck_prpr
                if cur_price > 0:
                    prev_pbl = pos.get("post_buy_low", cur_price)
                    pos["post_buy_low"] = min(prev_pbl, cur_price) if prev_pbl > 0 else cur_price

                exit_reason = None
                # (a) 트레일 3% (max_prdy_ctrt≥29 도달 후만 활성)
                trail_hit, trail_msg = check_uplimit_v5_trail_exit(
                    buy_price=pos.get("buy_price", 0),
                    highest_since_buy=pos.get("highest_since_buy", 0),
                    max_prdy_ctrt=pos.get("max_prdy_ctrt", 0),
                    cur_price=cur_price,
                )
                if trail_hit:
                    exit_reason = trail_msg
                # (b) 하드손절 -5% (post_buy_low 기준)
                if not exit_reason:
                    pbl = pos.get("post_buy_low", 0)
                    if pbl > 0 and pbl <= pos["buy_price"] * (1 - UPLIMIT_V4_HARD_LOSS):
                        exit_reason = f"v5_하드손절-{int(UPLIMIT_V4_HARD_LOSS*100)}%"
                # (c) 14:55 판정 (v4 holdover 로직 그대로 재사용)
                if not exit_reason and (h, m) >= (14, 55):
                    should_hold, hr = check_uplimit_v4_overnight_hold(prdy_ctrt, (h, m))
                    if should_hold:
                        _move_v4_to_holdover(code, pos, reason=hr)
                        continue
                    else:
                        exit_reason = hr or f"v5_당일청산({prdy_ctrt:.2f}%)"

                if exit_reason:
                    try:
                        _try_v4_market_sell(code, reason=exit_reason)
                    except Exception as _ex:
                        logger.warning(f"{ts_prefix()} [v5_exit] {code} 매도 실패: {_ex}")
                continue  # 보유 종목 처리 완료

            # ── 매수 후보 사전 컷 ──────────────────────────────────────────
            if code in _uplimit_v4_holdover:
                _v5_diag_counters["in_holdover"] += 1
                continue
            if code in _uplimit_v4_daily_bought_codes:
                _v5_diag_counters["in_daily_bought"] += 1
                continue
            if code in _uplimit_buy_pending:
                _v5_diag_counters["in_buy_pending"] += 1
                continue
            if code in _uplimit_v5_evaluated:
                _v5_diag_counters["in_evaluated"] += 1
                continue   # 당일 1회 평가만 (구독 후 첫 ma10 상회 시점)

            # ── 20%~25% 관찰 구간: 1회만 로그 기록 (매수 안 함) ────────────
            if (UPLIMIT_V5_OBSERVE_BAND_LOW <= prdy_ctrt < UPLIMIT_V5_OBSERVE_BAND_HIGH):
                _v5_diag_counters["ctrt_in_observe_band"] += 1
            if (UPLIMIT_V5_OBSERVE_BAND_LOW <= prdy_ctrt < UPLIMIT_V5_OBSERVE_BAND_HIGH
                and code not in _uplimit_v5_observe_log):
                _uplimit_v5_observe_log.add(code)
                logger.info(
                    f"{ts_prefix()} [v5_observe] "
                    f"{code_name_map.get(code, code)}({code}) "
                    f"ctrt={prdy_ctrt:.2f}% — 관찰 (매수 대상 아님)"
                )

            # ── 매수 판정: 25%~30% 구간만 ───────────────────────────────
            if not (UPLIMIT_V5_OBSERVE_BAND_HIGH <= prdy_ctrt < UPLIMIT_V5_BUY_BAND_HIGH):
                if prdy_ctrt < UPLIMIT_V5_OBSERVE_BAND_HIGH:
                    _v5_diag_counters["ctrt_below_25"] += 1
                continue
            _v5_diag_counters["ctrt_in_buy_band"] += 1

            # 상한가 도달 이력 차단
            if code in _uplimit_v5_limitup_reached:
                _v5_diag_counters["limitup_reached"] += 1
                continue

            # 저거래 차단
            is_low, low_reason = _is_low_liquidity_v5(code, _uplimit_v5_last_acml.get(code, 0.0))
            if is_low:
                _v5_diag_counters["low_liquidity"] += 1
                _uplimit_v5_evaluated.add(code)   # 1회 기록 (재평가 방지)
                logger.info(
                    f"{ts_prefix()} [v5_skip_저거래] "
                    f"{code_name_map.get(code, code)}({code}) {low_reason}"
                )
                continue

            # 시드 분산 슬롯 체크
            active_n = sum(
                1 for c, p in _uplimit_positions.items()
                if p.get("source") in ("v5", "v4") and not p.get("sold", False)
            ) + len(_uplimit_buy_pending)
            if active_n >= UPLIMIT_DIVERSIFY_N:
                _v5_diag_counters["diversify_full"] += 1
                continue

            # ── 1단계: 12필터 qualify (아직 통과 안 한 종목만) ──────────────
            if code not in _uplimit_v5_qualified_ts:
                _v5_diag_counters["qualify_called"] += 1
                # 12필터 데이터 수집
                prev_close = stck_prpr / (1 + prdy_ctrt / 100.0) if prdy_ctrt > -100 and stck_prpr > 0 else 0
                client = _top_client or _init_top_client()
                try:
                    volume_power = _get_uplimit_volume_power(client, code)
                except Exception:
                    volume_power = None
                # [260427] Phase 5: 외인 데이터 캐시 miss 시 lazy fetch (종목당 1회)
                frgn_3d = _uplimit_frgn_3d.get(code)
                if frgn_3d is None:
                    frgn_3d = _lazy_fetch_frgn_3d(code)
                day_avg = _calc_uplimit_day_avg_vol_per_min(code, acml_vol, now_dt)
                last5m_avg = _calc_uplimit_last5m_avg_vol_per_min(code)
                cross_ts = _uplimit_25pct_cross_ts.get(code)
                min_since_cross = (now_dt - cross_ts).total_seconds() / 60.0 if cross_ts else None

                qualified, q_reason = check_uplimit_v5_qualify(
                    prdy_ctrt=prdy_ctrt,
                    prev_close=prev_close,
                    stck_oprc=stck_oprc,
                    acml_vol=acml_vol,
                    day_avg_vol_per_min=day_avg,
                    last_5m_avg_vol_per_min=last5m_avg,
                    min_since_25pct_cross=min_since_cross,
                    vola_10d=_uplimit_vola_10d.get(code),
                    bid_total=bid_total,           # [260513] NameError(bid_sum) 복구 → 함수 시그니처(bid_total) 와 일치
                    ask_total=ask_total,           # [260513] NameError(ask_sum) 복구 → 함수 시그니처(ask_total) 와 일치
                    volume_power=volume_power,
                    frgn_3d_net=frgn_3d,
                    prev_day_prdy_ctrt=_uplimit_prev_day_ctrt.get(code, 0.0),
                    already_holding=False,
                    now_hm=(h, m),
                    today_buy_count=_uplimit_today_buy_count,
                    max_daily_buys=UPLIMIT_MAX_DAILY_BUYS,
                )
                if not qualified:
                    _v5_diag_counters["qualify_failed"] += 1
                    # skip 로그 종목당 10초 쿨다운
                    _last_log = _uplimit_v4_last_sustain_log.get(code, 0.0)
                    if time.time() - _last_log > 10:
                        _uplimit_v4_last_sustain_log[code] = time.time()
                        logger.info(f"{ts_prefix()} [v5_skip] {code_name_map.get(code, code)}({code}) {q_reason}")
                    continue
                # 12필터 통과 → qualified 등록
                _v5_diag_counters["qualify_passed"] += 1
                _uplimit_v5_qualified_ts[code] = now_dt
                logger.info(
                    f"{ts_prefix()} [v5_qualify] {code_name_map.get(code, code)}({code}) "
                    f"{q_reason} → 10분 내 매수 트리거 대기"
                )
                # 첫 qualify 직후엔 트리거 평가는 다음 틱부터 (last_bidp1 누적 후)
                _uplimit_v5_last_bidp1[code] = bidp1
                continue

            # ── 2단계: 매수 트리거 (qualified 종목만) ──────────────────────
            _v5_diag_counters["trigger_evaluated"] += 1
            qualified_ts = _uplimit_v5_qualified_ts[code]
            qualified_min = (now_dt - qualified_ts).total_seconds() / 60.0

            ema = _ema_state.get(code) or {}
            ma10 = float(ema.get(10) or 0)
            tick_count = int(_total_counts.get(code, 0))
            bb_lower = _get_bb_lower_v5(code)
            prev_bidp1 = _uplimit_v5_last_bidp1.get(code, bidp1)

            # bb_lower 이탈 이력 추적 (qualified 이후 어느 시점이라도 발생하면 set 추가)
            if bb_lower is not None and bidp1 > 0 and bidp1 < bb_lower:
                _uplimit_v5_lower_breached.add(code)
            breach_flag = code in _uplimit_v5_lower_breached

            should_buy, t_reason = check_uplimit_v5_buy_trigger(
                stck_prpr=stck_prpr,
                bidp1=bidp1,
                prev_bidp1=prev_bidp1,
                ma10=ma10,
                bb_lower=bb_lower,
                breach_flag=breach_flag,
                tick_count=tick_count,
                qualified_min_ago=qualified_min,
            )

            # bidp1 history 업데이트 (다음 틱 prev 로 사용)
            _uplimit_v5_last_bidp1[code] = bidp1

            if not should_buy:
                if t_reason:
                    # 만료 등 명시 사유만 1회 로그
                    if not hasattr(_check_uplimit_v4_from_tick, "_v5_expire_log"):
                        _check_uplimit_v4_from_tick._v5_expire_log = set()
                    if code not in _check_uplimit_v4_from_tick._v5_expire_log:
                        _check_uplimit_v4_from_tick._v5_expire_log.add(code)
                        logger.info(f"{ts_prefix()} [v5_skip] {code_name_map.get(code, code)}({code}) {t_reason}")
                continue

            # 매수 실행 (시장가)
            _v5_diag_counters["trigger_buy_fired"] += 1
            _uplimit_v5_evaluated.add(code)
            try:
                _try_v5_buy(code, prdy_ctrt, stck_prpr, now_dt, t_reason)
            except Exception as _eb:
                logger.warning(f"{ts_prefix()} [v5_buy] {code} 실패: {_eb}")
        except Exception as _e:
            logger.debug(f"[v5_tick] 예외 {code if 'code' in locals() else '?'}: {_e}")


def _try_v5_buy(code: str, prdy_ctrt: float, stck_prpr: float, now_dt, reason: str):
    """v5 매수 주문 (시장가, ord_dvsn='01').
    - 가격 0 으로 발주 → KIS 가 시장가 처리
    - 체결가는 _process_uplimit_notice 가 fill_pr 로 pos['buy_price'] 자동 갱신
    - source='v5' (v4 holdover/state 호환을 위해 dict 이름은 v4 그대로 사용)
    """
    name = code_name_map.get(code, code)
    accts = _iter_enabled_accounts(trade_only=True)
    if not accts:
        return
    acct = accts[0]
    client = _init_account_client(acct)
    avail_cash = _get_account_available_cash(client, acct)
    if avail_cash <= 0:
        logger.info(f"{ts_prefix()} [v5_buy] {name}({code}) 잔고 0 → 스킵")
        return
    # 시드 베이스 분할
    seed_base = _uplimit_seed_base if _uplimit_seed_base > 0 else (avail_cash / UPLIMIT_DIVERSIFY_N)
    invest = min(seed_base, avail_cash)
    # 시장가: 현재가 기준 수량 계산 (체결가는 시장가로 결정됨)
    qty = calc_uplimit_qty(invest, stck_prpr, "KOSPI")
    if qty <= 0:
        logger.info(
            f"{ts_prefix()} [v5_buy] {name}({code}) 수량=0 "
            f"(seed={int(seed_base):,}원, 현재가={int(stck_prpr):,}원)"
        )
        return

    # 시장가 발주: ord_dvsn='01', 가격 0
    j = _buy_order_cash(
        client, acct["cano"], acct.get("acnt_prdt_cd", "01") or "01",
        "TTTC0802U", code, qty, 0, ord_dvsn="01"
    )
    out = j.get("output") or {}
    ordno = str(out.get("ODNO") or out.get("odno") or "").strip()
    rt_cd = str(j.get("rt_cd") or "").strip()
    if rt_cd != "0":
        with _uplimit_state_lock:
            _uplimit_v4_daily_bought_codes.add(code)
        logger.warning(
            f"{ts_prefix()} [v5_buy] {name}({code}) 실패 rt_cd={rt_cd} "
            f"msg={j.get('msg1','')} → 당일 재시도 차단"
        )
        return

    prev_close = stck_prpr / (1 + prdy_ctrt / 100.0) if prdy_ctrt > -100 else 0

    with _uplimit_state_lock:
        _uplimit_buy_pending[code] = ordno
        _uplimit_positions[code] = {
            "name": name, "qty": qty,
            # 임시값: 체결통보 수신 시 fill_pr 로 갱신됨
            "buy_price": stck_prpr,
            "buy_ts": now_dt, "ordno": ordno,
            "max_prdy_ctrt": prdy_ctrt, "highest_since_buy": stck_prpr,
            "source": "v5", "reason": reason,
            "prev_close": prev_close,
            "invest_amount": invest,
            "post_buy_low": stck_prpr,
        }
        _uplimit_v4_daily_bought_codes.add(code)
    _save_uplimit_state(force=True)

    msg = (
        f"{ts_prefix()} [v5_매수] {name}({code}) ctrt={prdy_ctrt:.2f}%  "
        f"수량={qty}  현재가={int(stck_prpr):,}  ord={ordno}  사유={reason}  주문=시장가"
    )
    _notify(msg, tele=True)
    logger.info(msg)
    _append_ledger(
        order_type="buy_order", code=code, name=name,
        buy_price=stck_prpr, order_qty=qty, reason=f"v5_{reason}",
        ord_no=ordno, stck_prpr=stck_prpr,
    )


# [260427] _try_v4_buy 는 v5 시장가 호출로 위임 (호환 alias)
def _try_v4_buy(code: str, prdy_ctrt: float, stck_prpr: float, now_dt, reason: str):
    return _try_v5_buy(code, prdy_ctrt, stck_prpr, now_dt, reason)


def _try_v4_market_sell(code: str, reason: str):
    """v4 당일 포지션 시장가 매도."""
    with _uplimit_state_lock:
        pos = _uplimit_positions.get(code)
        if pos is None or pos.get("source") != "v4":
            return
        if code in _uplimit_exit_pending:
            return
        if pos.get("market_sell_fired"):
            return
        pos["market_sell_fired"] = True
        qty = int(pos.get("qty") or 0)
        name = pos.get("name", code)

    if qty <= 0:
        return
    accts = _iter_enabled_accounts(trade_only=True)
    if not accts:
        return
    acct = accts[0]
    client = _init_account_client(acct)
    try:
        j = _sell_order_cash(client, acct["cano"], acct.get("acnt_prdt_cd", "01") or "01",
                             code, qty, ord_dvsn="01")
        out = j.get("output") or {}
        ordno = str(out.get("ODNO") or out.get("odno") or "").strip()
        with _uplimit_state_lock:
            _uplimit_exit_pending[code] = ordno
            p = _uplimit_positions.get(code)
            if p:
                p["last_exit_reason"] = reason
                p["last_exit_ordno"] = ordno
        _save_uplimit_state(force=True)
        msg = f"{ts_prefix()} [v4_매도] {name}({code}) 시장가 qty={qty} ord={ordno} 사유={reason}"
        _notify(msg, tele=True)
        logger.info(msg)
    except Exception as e:
        logger.error(f"{ts_prefix()} [v4_exit] {name}({code}) 매도주문 실패: {e}")


def _move_v4_to_holdover(code: str, pos: dict, reason: str):
    """14:55 상한가 근처 → overnight 로 이전."""
    name = pos.get("name", code)
    buy_date = pos.get("buy_ts")
    if isinstance(buy_date, datetime):
        buy_date = buy_date.strftime("%Y-%m-%d")
    with _uplimit_state_lock:
        _uplimit_v4_holdover[code] = {
            "name": name,
            "buy_price": pos.get("buy_price"),
            "buy_ts": pos.get("buy_ts"),
            "buy_date": buy_date,
            "qty": pos.get("qty"),
            "ordno": pos.get("ordno"),
            "max_prdy_ctrt": pos.get("max_prdy_ctrt"),
            "highest_since_buy": pos.get("highest_since_buy"),
            "hold_reason": reason,
        }
        # _uplimit_positions 에서는 제거 (오늘은 더이상 당일 포지션 아님)
        _uplimit_positions.pop(code, None)
    _save_uplimit_state(force=True)
    msg = (
        f"{ts_prefix()} [v4_익일홀드] ★ {name}({code}) {reason} → 익일 매도 예정 "
        f"(buy={int(pos.get('buy_price', 0)):,}, qty={pos.get('qty')})"
    )
    _notify(msg, tele=True)
    logger.info(msg)


def _check_v4_nextday_exit(result: "pl.DataFrame", col_map: dict, code_col: str, kind: str):
    """익일 장 시작 후 overnight 포지션에 대해 매도 판정.
    ingest_loop 에서 매 틱 호출.
    """
    if kind != "regular_real":
        return
    if not _uplimit_v4_holdover:
        return
    pr_col    = col_map.get("stck_prpr")
    bidp1_col = col_map.get("bidp1")
    low_col   = col_map.get("stck_lwpr") or col_map.get("low")
    df_tmp = result.with_columns(
        pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
    )
    now_dt = datetime.now(KST)
    h, m = now_dt.hour, now_dt.minute

    held_codes = set(_uplimit_v4_holdover.keys())
    for df_code in df_tmp.partition_by(code_col, maintain_order=True):
        try:
            code = str(df_code[code_col][0])
            if code not in held_codes:
                continue
            if code in _uplimit_exit_pending:
                continue
            row = df_code.row(-1, named=True)
            def _f(col):
                if not col: return 0.0
                try:
                    return float(str(row.get(col) or 0).replace(",", "") or 0)
                except Exception:
                    return 0.0
            stck_prpr = _f(pr_col)
            bidp1     = _f(bidp1_col) or stck_prpr
            low       = _f(low_col) or stck_prpr

            # 틱 기반 EMA 재사용 — ma500 ≈ 1m MA5, ma2000 ≈ 1m MA20 근사
            ema = _ema_state.get(code) or {}
            ma_fast = float(ema.get(500) or 0)
            ma_slow = float(ema.get(2000) or 0)

            hold = _uplimit_v4_holdover.get(code)
            if hold is None:
                continue
            buy = float(hold.get("buy_price") or 0)

            should_sell, reason = check_uplimit_v4_nextday_sell(
                bidp1=bidp1, low_bar=low, buy_price=buy,
                ma_fast=ma_fast, ma_slow=ma_slow, now_hm=(h, m),
            )
            if should_sell:
                _try_v4_holdover_sell(code, reason)
        except Exception as _e:
            logger.debug(f"[v4_nextday] 예외: {_e}")


def _try_v4_holdover_sell(code: str, reason: str):
    """익일 overnight 포지션 시장가 매도."""
    with _uplimit_state_lock:
        hold = _uplimit_v4_holdover.get(code)
        if hold is None:
            return
        if code in _uplimit_exit_pending:
            return
        qty = int(hold.get("qty") or 0)
        name = hold.get("name", code)

    if qty <= 0:
        with _uplimit_state_lock:
            _uplimit_v4_holdover.pop(code, None)
        return

    accts = _iter_enabled_accounts(trade_only=True)
    if not accts:
        return
    acct = accts[0]
    client = _init_account_client(acct)
    try:
        j = _sell_order_cash(client, acct["cano"], acct.get("acnt_prdt_cd", "01") or "01",
                             code, qty, ord_dvsn="01")
        out = j.get("output") or {}
        ordno = str(out.get("ODNO") or out.get("odno") or "").strip()
        with _uplimit_state_lock:
            _uplimit_exit_pending[code] = ordno
            hold["last_exit_ordno"] = ordno
            hold["last_exit_reason"] = reason
        _save_uplimit_state(force=True)
        msg = f"{ts_prefix()} [v4_익일매도] ★ {name}({code}) 시장가 qty={qty} ord={ordno} 사유={reason}"
        _notify(msg, tele=True)
        logger.info(msg)
    except Exception as e:
        logger.error(f"{ts_prefix()} [v4_nextday_sell] {name}({code}) 주문 실패: {e}")


# =============================================================================
# [260523] Strategy str2 — 전용 동기 주문 함수 (str1 큐/worker 미사용)
#   _buy_order_cash/_sell_order_cash/_cancel_order_generic 프리미티브 직접 호출.
#   v5 _try_v5_buy 패턴(잔고조회→수량→발주→state→ledger→notify) 동일.
#   ※ STR2_VI_TRADE_MODE: "test_mode" → 1주, "run_mode" → 정상 수량, "off" → 발주 안 함.
# =============================================================================
def _str2_qty(stck_prpr: float, market: str) -> int:
    """str2 매수 수량. test_mode=1주, run_mode=가용현금/STR2_DIVERSIFY_N 기준."""
    if STR2_VI_TRADE_MODE == "test_mode":
        return 1
    accts = _iter_enabled_accounts(trade_only=True)
    if not accts:
        return 0
    client = _init_account_client(accts[0])
    avail = _get_account_available_cash(client, accts[0])
    if avail <= 0:
        return 0
    seed_base = avail / max(1, STR2_DIVERSIFY_N)
    return calc_uplimit_qty(min(seed_base, avail), stck_prpr, market or "KOSPI")


def _str2_place_buy(code: str, st: "Str2State", price: float, stck_prpr: float,
                    use_market: bool, origin: str, reason: str) -> None:
    """str2 매수 주문 (use_market=True 시장가 ord_dvsn='01', 아니면 지정가 '00').

    지정가 가격(price)은 회신 §3 의 '직전50틱 min(bidp1)' 기준가(호출자 계산).
    체결가는 H0STCNI0 체결통보가 갱신하므로 여기선 임시 buy_price=stck_prpr.
    """
    if STR2_VI_TRADE_MODE == "off":
        return
    name = code_name_map.get(code, code)
    market = _code_market_map.get(code, "KOSPI") or "KOSPI"
    accts = _iter_enabled_accounts(trade_only=True)
    if not accts:
        return
    acct = accts[0]
    client = _init_account_client(acct)
    qty = _str2_qty(stck_prpr, market)
    if qty <= 0:
        logger.info(f"{ts_prefix()} [str2_buy] {name}({code}) 수량=0 → 스킵 (현재가={int(stck_prpr):,})")
        return
    ord_dvsn = "01" if use_market else "00"
    ord_price = 0 if use_market else round_to_tick(price, market)
    if not use_market and ord_price <= 0:
        logger.info(f"{ts_prefix()} [str2_buy] {name}({code}) 지정가 0 → 스킵")
        return
    try:
        j = _buy_order_cash(
            client, acct["cano"], acct.get("acnt_prdt_cd", "01") or "01",
            "TTTC0802U", code, qty, ord_price, ord_dvsn=ord_dvsn,
        )
    except Exception as e:
        logger.error(f"{ts_prefix()} [str2_buy] {name}({code}) 주문 실패: {e}")
        return
    out = j.get("output") or {}
    ordno = str(out.get("ODNO") or out.get("odno") or "").strip()
    with _str2_state_lock:
        st.buy_origin = origin
        st.last_buy_ordno = ordno
        if use_market:
            # 시장가: 즉시 체결 가정 → 보유 잠정 표시 (체결통보가 buy_price/qty 확정)
            st.buy_order_active = False
            st.position = True
            st.buy_price = float(stck_prpr)    # 임시값 — _handle_str2_ccnl_fill 가 fill_pr 로 갱신
            st.highest = float(stck_prpr)
        else:
            # 지정가: 미체결 상태 → position 은 체결통보 수신 시에만 True
            st.buy_order_active = True
            st.buy_order_price = float(ord_price)
            st.buy_order_qty = qty   # 미체결 취소 시 사용 (st.qty 는 아직 0)
    _save_str2_state(force=True)
    dvsn_label = "시장가" if use_market else f"지정가({int(ord_price):,})"
    msg = (f"{ts_prefix()} [str2_매수] {name}({code}) {origin} {dvsn_label} qty={qty} "
           f"현재가={int(stck_prpr):,} ord={ordno} 사유={reason}")
    _notify(msg, tele=True)
    logger.info(msg)
    _append_ledger(order_type="buy_order", code=code, name=name, buy_price=stck_prpr,
                   order_qty=qty, reason=f"str2_{reason}", ord_no=ordno,
                   ord_dvsn=ord_dvsn, stck_prpr=stck_prpr)


def _str2_place_sell(code: str, st: "Str2State", qty: int, price: float,
                     use_limit: bool, reason: str, stck_prpr: float) -> None:
    """str2 매도 주문 (use_limit=True 지정가 '00' price 기준, 아니면 시장가 '01').

    지정가 매도 기준가(price)는 회신 §3 의 '직전50틱 max(askp1)'(호출자 계산).
    """
    if STR2_VI_TRADE_MODE == "off":
        return
    name = code_name_map.get(code, code)
    market = _code_market_map.get(code, "KOSPI") or "KOSPI"
    if qty <= 0:
        return
    accts = _iter_enabled_accounts(trade_only=True)
    if not accts:
        return
    acct = accts[0]
    client = _init_account_client(acct)
    ord_dvsn = "00" if use_limit else "01"
    ord_price = round_to_tick(price, market) if use_limit else 0
    if use_limit and ord_price <= 0:
        logger.info(f"{ts_prefix()} [str2_sell] {name}({code}) 지정가 0 → 시장가 전환")
        ord_dvsn, ord_price = "01", 0
    try:
        j = _sell_order_cash(client, acct["cano"], acct.get("acnt_prdt_cd", "01") or "01",
                             code, qty, price=ord_price, ord_dvsn=ord_dvsn)
    except Exception as e:
        logger.error(f"{ts_prefix()} [str2_sell] {name}({code}) 주문 실패: {e}")
        return
    out = j.get("output") or {}
    ordno = str(out.get("ODNO") or out.get("odno") or "").strip()
    with _str2_state_lock:
        if ord_dvsn == "00":
            st.sell_order_active = True
            st.sell_order_price = float(ord_price)
        else:
            st.sell_order_active = False
        st.last_sell_ordno = ordno
        st.last_sell_reason = reason
    _save_str2_state(force=True)
    dvsn_label = f"지정가({int(ord_price):,})" if ord_dvsn == "00" else "시장가"
    msg = (f"{ts_prefix()} [str2_매도] {name}({code}) {dvsn_label} qty={qty} "
           f"현재가={int(stck_prpr):,} ord={ordno} 사유={reason}")
    _notify(msg, tele=True)
    logger.info(msg)
    _append_ledger(order_type="sell_order", code=code, name=name,
                   buy_price=st.buy_price, sell_price=stck_prpr, order_qty=qty,
                   reason=f"str2_{reason}", ord_no=ordno, ord_dvsn=ord_dvsn,
                   stck_prpr=stck_prpr)


def _str2_cancel(code: str, st: "Str2State", ordno: str, qty: int, ord_dvsn: str, reason: str) -> None:
    """str2 미체결 주문 취소 (지정가 매수/매도 취소)."""
    if not ordno or qty <= 0:
        return
    name = code_name_map.get(code, code)
    accts = _iter_enabled_accounts(trade_only=True)
    if not accts:
        return
    acct = accts[0]
    client = _init_account_client(acct)
    try:
        _cancel_order_generic(client, acct["cano"], acct.get("acnt_prdt_cd", "01") or "01",
                              ordno, code, qty, ord_dvsn=ord_dvsn)
        msg = f"{ts_prefix()} [str2_취소] {name}({code}) ord={ordno} 사유={reason}"
        _notify(msg, tele=True)
        logger.info(msg)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [str2_cancel] {name}({code}) 취소 실패: {e}")


def _handle_str2_ccnl_fill(code: str, seln: str, qty: int, fill_pr: float,
                           name: str, recv_ts: str) -> None:
    """str2 종목 체결통보 처리 (매수/매도). _on_ccnl_notice_filled 에서 호출.

    매수 체결: 실 체결가로 buy_price 확정, qty 누적, 매수 지정가 active 해제.
    매도 체결: qty 차감, 전량 매도 시 position 해제 (보유 종료).
    """
    with _str2_state_lock:
        st = _str2_state.get(code)
        if st is None:
            return
        is_sell = seln in ("01", "1")
        if is_sell:
            st.qty = max(0, st.qty - qty)
            if st.qty <= 0:
                st.position = False
                st.sell_order_active = False
                st.sell_ready = False
            _append_ledger(order_type="sell_fill", code=code, name=name,
                           buy_price=st.buy_price, sell_price=fill_pr, fill_qty=qty,
                           reason=f"str2_{st.last_sell_reason}_체결" if st.last_sell_reason else "str2_매도체결",
                           note=f"체결통보 {recv_ts}", stck_prpr=fill_pr)
            log_msg = (f"{ts_prefix()} [str2_체결] 매도 {name}({code}) {qty}주 × {int(fill_pr):,} "
                       f"(잔여 {st.qty})")
        else:
            # 첫 체결이면 buy_price = 실 체결가, 이후 부분체결은 가중평균
            if st.qty <= 0:
                st.buy_price = fill_pr
            elif fill_pr > 0:
                st.buy_price = (st.buy_price * st.qty + fill_pr * qty) / (st.qty + qty)
            st.qty += qty
            st.position = True
            st.buy_order_active = False   # 매수 지정가 체결 완료
            if st.highest < fill_pr:
                st.highest = fill_pr
            _append_ledger(order_type="buy_fill", code=code, name=name,
                           buy_price=fill_pr, fill_qty=qty, reason="str2_매수체결",
                           note=f"체결통보 {recv_ts}", stck_prpr=fill_pr)
            log_msg = (f"{ts_prefix()} [str2_체결] 매수 {name}({code}) {qty}주 × {int(fill_pr):,} "
                       f"(누적 {st.qty}, 평단 {int(st.buy_price):,})")
    _save_str2_state(force=True)
    _notify(log_msg, tele=True)
    logger.info(log_msg)


# =============================================================================
# [260523] Strategy str2 — per-tick 드라이버 (ingest_loop 에서 호출)
#   STR2_ENABLED=False 면 즉시 return → 기존 거동 바이트 동일.
#   strat2.* 순수 판단 함수를 호출하고, 동기 주문(_str2_place_*)을 발주한다.
#   지표/히스토리(stoch_k·bb_hi_max·직전50틱 min(bidp1)/max(askp1)·ma10_prev/ma500_prev·
#   running_low)는 여기서 _price_buf/_ema_state/baked bb 를 읽어 산출/유지한다.
# =============================================================================
def _check_str2_from_tick(result: "pl.DataFrame", col_map: dict, code_col: str, kind: str) -> None:
    """str2 매수/매도 per-tick 판단 + 주문. 회신 §3/§8(6) 입력계약 준수."""
    if not STR2_ENABLED:
        return
    if kind != "regular_real":
        return

    pr_col    = col_map.get("stck_prpr")
    prdy_col  = col_map.get("prdy_ctrt")
    wghn_col  = col_map.get("wghn_avrg_stck_prc")
    bidp1_col = col_map.get("bidp1")
    # askp1: 매도 지정가 기준가(직전50틱 max). col_map 에 없으면 fallback.
    #   라이브 H0STCNT0 엔 askp1 존재(domestic_stock_functions_ws schema) — 방어용 폴백.
    askp1_col = col_map.get("askp1")

    df_tmp = result.with_columns(
        pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
    )
    now_dt = datetime.now(KST)
    now_sec = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second

    for df_code in df_tmp.partition_by(code_col, maintain_order=True):
        try:
            code = str(df_code[code_col][0])
            if not code or code in ("None", "000000"):
                continue
            row = df_code.row(-1, named=True)

            def _f(col):
                if not col:
                    return 0.0
                try:
                    return float(str(row.get(col) or 0).replace(",", "") or 0)
                except Exception:
                    return 0.0

            stck_prpr = _f(pr_col)
            prdy_ctrt = _f(prdy_col)
            wghn      = _f(wghn_col)
            bidp1     = _f(bidp1_col) or stck_prpr
            # askp1 fallback: askp1 → bidp1 → stck_prpr
            askp1 = _f(askp1_col) if askp1_col else 0.0
            if askp1 <= 0:
                askp1 = bidp1 if bidp1 > 0 else stck_prpr
            if stck_prpr <= 0:
                continue

            st = _str2_state.get(code)
            if st is None:
                st = Str2State()
                _str2_state[code] = st
            st.tick_idx += 1
            tick_idx = st.tick_idx

            # ── 히스토리 버퍼 갱신 (직전 ROLL(50)틱 min(bidp1)/max(askp1)) ──
            bbuf = _str2_bidp_buf.get(code)
            if bbuf is None:
                bbuf = deque(maxlen=STR2_ROLL); _str2_bidp_buf[code] = bbuf
            abuf = _str2_askp_buf.get(code)
            if abuf is None:
                abuf = deque(maxlen=STR2_ROLL); _str2_askp_buf[code] = abuf
            if bidp1 > 0:
                bbuf.append(bidp1)
            if askp1 > 0:
                abuf.append(askp1)
            roll_min_bidp = min(bbuf) if bbuf else 0.0
            roll_max_askp = max(abuf) if abuf else 0.0

            # running_low (당일 정규장 저점)
            if st.running_low <= 0 or (bidp1 > 0 and bidp1 < st.running_low):
                st.running_low = bidp1 if bidp1 > 0 else stck_prpr

            # ── 지표: baked MA(ddof 무관) + baked BB(ddof=0) + stoch_k + bb_hi_max ──
            ema = _ema_state.get(code) or {}
            ma10  = float(ema.get(10) or 0)
            ma50  = float(ema.get(50) or 0)
            ma200 = float(ema.get(200) or 0)
            ma300 = float(ema.get(300) or 0)
            ma500 = float(ema.get(500) or 0)
            ma2000 = float(ema.get(2000) or 0)
            bb_hi, bb_lo = _str2_baked_bb(code)          # ddof=0 baked
            # bb_hi_max 윈도우는 매 틱 유지 필수 (포지션 진입 전부터 rolling_max(600) 누적).
            bb_hi_max = _str2_update_bb_hi_max(code, bb_hi)
            bb_hi_v   = bb_hi if bb_hi is not None else 0.0
            bb_lo_v   = bb_lo if bb_lo is not None else 0.0
            bb_himax_v = bb_hi_max if bb_hi_max == bb_hi_max else 0.0   # NaN→0
            # stoch_k(1500틱 min/max) 는 매도(sell_ready)에서만 사용 → 보유 종목만 lazy 계산.

            ma500_up, ma2000_up = strat2.compute_trends(
                ma50, ma200, ma300, ma500, ma2000, wghn, tick_idx)

            # ── 세션 셋업: 08:58 분류 1회 (b1_enabled/b2_pending + limit_up) ──
            if code not in _str2_session_setup_done:
                # 08:55 직전가 스냅샷 (08:55~08:58 사이 첫 도달 시 기록)
                if strat2.T_0855 <= now_sec < strat2.T_0858 and code not in _str2_premarket_0855:
                    _str2_premarket_0855[code] = stck_prpr
                if now_sec >= strat2.T_0858:
                    p0855 = _str2_premarket_0855.get(code)
                    b1, b2 = strat2.classify_premarket(p0855, stck_prpr, ma50)
                    prev_close = stck_prpr / (1 + prdy_ctrt / 100.0) if prdy_ctrt > -100 else 0.0
                    market = _code_market_map.get(code, "KOSPI") or "KOSPI"
                    st.b1_enabled = b1
                    st.b2_pending = b2
                    st.prev_close = prev_close
                    st.limit_up = calc_limit_up_price(prev_close, market) if prev_close > 0 else 0.0
                    _str2_session_setup_done.add(code)

            # ── b2: 09:00 시초 시장가 (장전 상승 분류) ──
            if (st.b2_pending and not st.b2_used and not st.position
                    and now_sec >= strat2.T_0900):
                st.b2_used = True
                _str2_place_buy(code, st, price=0.0, stck_prpr=stck_prpr,
                                use_market=True, origin="b2", reason="시초시장가(장전상승)")
                st.ma10_prev, st.ma500_prev = ma10, ma500
                continue

            # ── 보유 포지션: 매도 판단 (손절/트레일은 strat2.sell_stop_trail 로만) ──
            if st.position:
                # 트레일 기준 최고가 갱신
                if bidp1 > st.highest:
                    st.highest = bidp1
                pr_for_stop = bidp1 if bidp1 > 0 else stck_prpr

                # (a) 손절/트레일 (전략파일 소유 — 메인 중복 금지)
                do_sell, sreason = strat2.sell_stop_trail(st.buy_price, pr_for_stop, st.highest)
                if do_sell:
                    _str2_place_sell(code, st, st.qty, price=0.0, use_limit=False,
                                     reason=sreason, stck_prpr=stck_prpr)
                    st.ma10_prev, st.ma500_prev = ma10, ma500
                    continue

                ma500gap = strat2.compute_ma500gap(bidp1, ma500)

                # (b) sell_ready set/release — stoch_k(1500틱) 는 여기서만 lazy 계산
                stoch_k = _str2_compute_stoch_k(code)
                if strat2.sell_ready_set(stoch_k, bb_himax_v, bb_hi_v, ma10):
                    st.sell_ready = True
                if strat2.sell_ready_release(ma50, ma200, st.buy_order_active):
                    st.sell_ready = False

                # (c) 지정가 매도주문 발주 (sell_ready & ma500gap & ma10<ma50)
                if strat2.sell_limit_place_ok(st.sell_ready, st.sell_order_active, ma500gap, ma10, ma50):
                    if roll_max_askp > 0:
                        st.sell_order_tick = tick_idx
                        _str2_place_sell(code, st, st.qty, price=roll_max_askp, use_limit=True,
                                         reason="sell_ready_지정가(직전50틱max askp)", stck_prpr=stck_prpr)
                        st.ma10_prev, st.ma500_prev = ma10, ma500
                        continue

                # (d) 지정가 매도주문 처리 (미체결 강제 / 지정가 체결)
                if st.sell_order_active:
                    can_fill = (tick_idx > st.sell_order_tick and stck_prpr >= st.sell_order_price > 0)
                    do2, r2, use_limit = strat2.sell_order_resolve(
                        st.sell_order_active, ma50, ma200, can_fill)
                    if do2:
                        if use_limit:
                            # 지정가 체결 — 별도 발주 없이 active 해제 (체결통보가 처리)
                            with _str2_state_lock:
                                st.sell_order_active = False
                            logger.info(f"{ts_prefix()} [str2_sell] {code} {r2}")
                        else:
                            # 미체결 강제 시장가: 기존 지정가 취소 후 시장가
                            if st.last_sell_ordno:
                                _str2_cancel(code, st, st.last_sell_ordno, st.qty, "00",
                                             "미체결강제_지정가취소")
                            _str2_place_sell(code, st, st.qty, price=0.0, use_limit=False,
                                             reason=r2, stck_prpr=stck_prpr)
                        st.ma10_prev, st.ma500_prev = ma10, ma500
                        continue

                # (e) ma10/ma500 데드크로스 (ma500gap 해제 시)
                do3, r3 = strat2.sell_deadcross(ma500gap, st.ma10_prev, st.ma500_prev, ma10, ma500)
                if do3:
                    _str2_place_sell(code, st, st.qty, price=0.0, use_limit=False,
                                     reason=r3, stck_prpr=stck_prpr)
                    st.ma10_prev, st.ma500_prev = ma10, ma500
                    continue

                # (f) 15:18 비상한가 청산 / 상한가 이월(carry_hold)
                if (not st.closing_checked) and now_sec >= strat2.T_1518:
                    st.closing_checked = True
                    if strat2.is_limit_up(stck_prpr, prdy_ctrt, st.limit_up):
                        st.carry_hold = True   # 익일 carry_nextday_premarket_sell 로 처리
                        logger.info(f"{ts_prefix()} [str2] {code} 15:18 상한가 → carry_hold(익일이월)")
                    else:
                        _str2_place_sell(code, st, st.qty, price=0.0, use_limit=False,
                                         reason="15:18_비상한가청산", stck_prpr=stck_prpr)
                        st.ma10_prev, st.ma500_prev = ma10, ma500
                        continue

                st.ma10_prev, st.ma500_prev = ma10, ma500
                continue   # 보유 종목 처리 완료

            # ── 미보유: 매수 판단 (b1/b3 지정가 → 시장가 전환) ──
            if now_sec < strat2.T_0900:
                st.ma10_prev, st.ma500_prev = ma10, ma500
                continue

            # 매수필터(토글)
            allowed = True
            if strat2.USE_BUY_FILTER:
                # ma2000[i-500], 윈도우 고점 — 라이브 미사전계산 시 None(워밍업) 전달 → 통과
                allowed = strat2.buy_filter_allowed(
                    tick_idx=tick_idx, ma2000=ma2000, ma2000_lookback=0.0,
                    running_low=st.running_low, prpr=stck_prpr,
                    recent_hi_w=None, prev_hi_w=None,
                )

            # 지정가 매수주문 대기 중이면 시장가 전환 판정
            if st.buy_order_active:
                esc = strat2.buy_escalation(st.buy_origin, ma50, ma200, ma300, ma500,
                                            ma500_up, ma2000_up)
                if esc:
                    if st.last_buy_ordno and st.buy_order_qty > 0:
                        _str2_cancel(code, st, st.last_buy_ordno, st.buy_order_qty, "00",
                                     "시장가전환_지정가취소")
                        with _str2_state_lock:
                            st.buy_order_active = False
                            st.buy_order_qty = 0
                    _str2_place_buy(code, st, price=0.0, stck_prpr=stck_prpr,
                                    use_market=True, origin=st.buy_origin, reason=esc)
                st.ma10_prev, st.ma500_prev = ma10, ma500
                continue

            # 신규 매수 발주 (b1/b3)
            origin = strat2.buy_b1b3_origin(
                st.b1_enabled, allowed, bidp1, bb_lo_v, ma10, ma50, ma300, ma500,
                ma500_up, ma2000_up)
            if origin and roll_min_bidp > 0:
                st.buy_order_tick = tick_idx
                _str2_place_buy(code, st, price=roll_min_bidp, stck_prpr=stck_prpr,
                                use_market=False, origin=origin,
                                reason=f"{origin}_지정가(직전50틱min bidp)")

            st.ma10_prev, st.ma500_prev = ma10, ma500
        except Exception as _e:
            logger.debug(f"[str2_tick] 예외 {code if 'code' in locals() else '?'}: {_e}")

    _save_str2_state(force=False)


def _check_str2_carry_premarket(result: "pl.DataFrame", col_map: dict, code_col: str, kind: str) -> None:
    """carry_hold(15:18 상한가 이월) 포지션의 익일 장전 매도 판정 (회신 §6).

    str1 식 — 익일 개장 전(예상체결가 antc_prce / 현재가)이 wghn 하락 이탈 시 개장 전 시장가 매도.
    정규장 진입(09:00) 후에는 _check_str2_from_tick 의 일반 매도(sell_*)가 재개된다.
    ※ str2 슬롯 배타 소유 — v4 overnight/nextday 와 혼용 금지.
    """
    if not STR2_ENABLED:
        return
    if kind != "regular_exp":
        return
    # carry_hold 보유 종목이 없으면 즉시 종료
    carry_codes = {c for c, st in _str2_state.items() if st.carry_hold and st.position}
    if not carry_codes:
        return
    pr_col   = col_map.get("antc_prce") or col_map.get("stck_prpr")
    wghn_col = col_map.get("wghn_avrg_stck_prc")
    df_tmp = result.with_columns(
        pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
    )
    for df_code in df_tmp.partition_by(code_col, maintain_order=True):
        try:
            code = str(df_code[code_col][0])
            if code not in carry_codes:
                continue
            st = _str2_state.get(code)
            if st is None or not st.carry_hold:
                continue
            row = df_code.row(-1, named=True)

            def _f(col):
                if not col:
                    return 0.0
                try:
                    return float(str(row.get(col) or 0).replace(",", "") or 0)
                except Exception:
                    return 0.0

            antc_or_pr = _f(pr_col)
            wghn       = _f(wghn_col)
            do_sell, reason = strat2.carry_nextday_premarket_sell(antc_or_pr, wghn)
            if do_sell:
                _str2_place_sell(code, st, st.qty, price=0.0, use_limit=False,
                                 reason=reason, stck_prpr=antc_or_pr)
                with _str2_state_lock:
                    st.carry_hold = False
        except Exception as _e:
            logger.debug(f"[str2_carry] 예외: {_e}")


# =============================================================================
# [260423] Strategy B (MA trend) + C (BB expansion) — 신호 관찰 (로그만)
# =============================================================================
def _check_strategy_bc_from_tick(
    result: "pl.DataFrame",
    col_map: dict,
    code_col: str,
    kind: str,
) -> None:
    """ingest_loop 에서 호출. _ema_state 와 _price_buf 를 읽어 ma50/500, bb 계산.
    신호 발생 시 **로그만** 찍음 (MA_TREND_ENABLED / BB_EXPANSION_ENABLED 플래그 확인).
    백테스트 전 실운영 데이터 관찰 목적.
    """
    if kind != "regular_real":
        return
    if not (MA_TREND_ENABLED or BB_EXPANSION_ENABLED):
        return

    pr_col   = col_map.get("stck_prpr")
    prdy_col = col_map.get("prdy_ctrt")
    vol_col  = col_map.get("acml_vol")
    bidp1_col = col_map.get("bidp1")

    df_tmp = result.with_columns(
        pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
    )
    now_dt = datetime.now(KST)
    h, m = now_dt.hour, now_dt.minute

    for df_code in df_tmp.partition_by(code_col, maintain_order=True):
        try:
            code = str(df_code[code_col][0])
            if not code or code in ("None", "000000"):
                continue
            row = df_code.row(-1, named=True)

            def _f(col):
                if not col:
                    return 0.0
                try:
                    return float(str(row.get(col) or 0).replace(",", "") or 0)
                except Exception:
                    return 0.0

            prdy_ctrt = _f(prdy_col)
            stck_prpr = _f(pr_col)
            acml_vol  = _f(vol_col)
            bidp1     = _f(bidp1_col) or stck_prpr

            # _ema_state 에서 MA 읽기
            ema = _ema_state.get(code) or {}
            ma10_cur  = float(ema.get(10) or 0)
            ma50_cur  = float(ema.get(50) or 0)
            ma500_cur = float(ema.get(500) or 0)

            # Strategy B: ma50>ma500 골든크로스 관찰
            if MA_TREND_ENABLED and ma50_cur > 0 and ma500_cur > 0:
                ma50_prev = _ma_trend_prev_ma50.get(code, ma50_cur)
                ma500_prev = _ma_trend_prev_ma500.get(code, ma500_cur)
                # 매수 신호 판정 (보유 여부: _uplimit_positions 공유)
                already_holding = (code in _uplimit_positions) or (code in _uplimit_buy_pending)
                ok, reason = ma_trend_buy_signal(
                    prdy_ctrt=prdy_ctrt,
                    prev_day_prdy_ctrt=_uplimit_prev_day_ctrt.get(code, 0.0),
                    acml_vol=acml_vol,
                    ma50_cur=ma50_cur, ma500_cur=ma500_cur,
                    ma50_prev=ma50_prev, ma500_prev=ma500_prev,
                    already_holding=already_holding,
                    now_hm=(h, m),
                )
                if ok:
                    # 쿨다운 10초
                    _last_ts = _ma_trend_last_signal_log.get(code, 0.0)
                    if time.time() - _last_ts > 10:
                        name = code_name_map.get(code, code)
                        logger.info(f"{ts_prefix()} [ma_trend_signal] ★ {name}({code}) {reason}")
                        _ma_trend_last_signal_log[code] = time.time()
                # 현재 값 저장 (다음 틱의 prev 가 됨)
                _ma_trend_prev_ma50[code] = ma50_cur
                _ma_trend_prev_ma500[code] = ma500_cur

            # Strategy C: BB 압축→확장 + 하단 이탈-복귀 관찰
            if BB_EXPANSION_ENABLED:
                # price_buf 있고 BB 계산된 종목만 (buf 길이 ≥ BB_PERIOD)
                buf = _price_buf.get(code)
                if buf and len(buf) >= INDICATOR_BB_PERIOD:
                    # 현재 BB 계산 (재계산 — 캐시가 있다면 거기서 읽는 게 나음)
                    try:
                        import statistics as _st
                        bb_window = list(buf)[-INDICATOR_BB_PERIOD:]
                        bb_mean = sum(bb_window) / len(bb_window)
                        bb_std = _st.pstdev(bb_window)
                        bb_width_cur = INDICATOR_BB_K * 2 * bb_std
                        bb_upper_cur = bb_mean + INDICATOR_BB_K * bb_std
                        bb_lower_cur = bb_mean - INDICATOR_BB_K * bb_std
                    except Exception:
                        continue

                    # bb_width 히스토리 업데이트 (최근 100틱)
                    wh = _bb_width_history.setdefault(code, deque(maxlen=100))
                    wh.append(bb_width_cur)
                    bb_width_min_recent = min(wh) if wh else bb_width_cur

                    # 하단 이탈 히스토리 (직전 30틱)
                    ch = _bb_lower_cross_history.setdefault(code, deque(maxlen=30))
                    is_crossed_now = (bidp1 < bb_lower_cur)
                    prev_tick_crossed = ch[-1] if ch else False
                    bb_lower_crossed_recent = any(ch) if ch else False
                    ch.append(is_crossed_now)

                    bidp1_prev = _bb_last_bidp1.get(code, bidp1)

                    already_holding = (code in _uplimit_positions) or (code in _uplimit_buy_pending)
                    ok, reason = bb_expansion_buy_signal(
                        prdy_ctrt=prdy_ctrt, bidp1=bidp1, bidp1_prev=bidp1_prev,
                        bb_lower_cur=bb_lower_cur, bb_width_cur=bb_width_cur,
                        bb_width_min_recent=bb_width_min_recent,
                        bb_lower_crossed_recent=bb_lower_crossed_recent,
                        bb_lower_crossed_prev_tick=prev_tick_crossed,
                        already_holding=already_holding,
                        now_hm=(h, m),
                    )
                    if ok:
                        _last_ts = _bb_exp_last_signal_log.get(code, 0.0)
                        if time.time() - _last_ts > 10:
                            name = code_name_map.get(code, code)
                            logger.info(f"{ts_prefix()} [bb_exp_signal] ★ {name}({code}) {reason}")
                            _bb_exp_last_signal_log[code] = time.time()
                    _bb_last_bidp1[code] = bidp1
        except Exception as _e:
            logger.debug(f"[strategy_bc_tick] 예외 {code if 'code' in locals() else '?'}: {_e}")


def _run_balance_monitor_bg(sell_lines: list[str]) -> None:
    """별도 스레드에서 REST 잔고조회 + 결과 출력. ingest_loop 블로킹 방지."""
    try:
        nt = datetime.now(KST).time()
        if dtime(8, 29) <= nt < dtime(9, 0):
            bal_label = "장전 예상체결가 모니터링"
        elif dtime(15, 20) <= nt < dtime(15, 30):
            bal_label = "장마감 예상체결가 모니터링"
        else:
            bal_label = "보유종목 모니터링"

        hold_map = _query_and_print_balance(bal_label)

        # ── 잔고 기반 매도 완료 감지: sell_ordered인데 잔고에서 사라진 종목 → sold=True ──
        if hold_map is not None:
            with _str1_sell_state_lock:
                for code, st in list(_str1_sell_state.items()):
                    if st.get("sold") or not st.get("sell_ordered"):
                        continue
                    # 잔고에 없거나 수량 0 → 체결 완료로 간주
                    bm = hold_map.get(code, 0)
                    bal_qty = bm.get("qty", 0) if isinstance(bm, dict) else (bm or 0)
                    if bal_qty <= 0:
                        st["sold"] = True
                        st["sell_reason"] = st.get("sell_reason") or "잔고확인_체결완료"
                        _opening_call_auction_watch.pop(code, None)
                        name = st.get("name", code)
                        logger.info(f"{ts_prefix()} [잔고확인] 매도 체결 확인: {name}({code}) — 잔고에서 소멸")
                        _notify(f"[잔고확인] 매도 체결 확인: {name}({code})", tele=True)
                _save_str1_sell_state()

        if sell_lines:
            now_str = datetime.now(KST).strftime("%H:%M:%S")
            sell_header = f"  ▼ 매도조건 ({len(sell_lines)}종목) ─────────────────────"
            sell_msg = "\n".join([sell_header] + [f"    {l}" for l in sell_lines])
            sys.stdout.write(f"{sell_msg}\n")
            sys.stdout.flush()
            logger.info(sell_msg)
            tele_lines = [f"[매도조건 {now_str}]"]
            tele_lines.append(f"▼ 매도조건 {len(sell_lines)}종목")
            tele_lines.extend(sell_lines)
            _notify("\n".join(tele_lines), tele=True)
    except Exception as e:
        logger.warning(f"{ts_prefix()} [balance_monitor_bg] {e}")


def _print_opening_call_auction_monitor(newly_triggered: list[str], now_ts: float, is_opening_call_auction: bool = True) -> None:
    """
    Str1 포지션 모니터링 출력.

    - 08:29~09:00: 장전 예상체결가
    - 09:00~15:20: 실시간 체결가 (정규장)
    - 15:20~15:30: 장마감 예상체결가

    ① 이번 틱에서 처음 매도조건 진입한 종목 → 즉시 출력 + 텔레그램
    ② 60초마다 → REST 잔고조회를 별도 스레드에서 실행 (ingest_loop 블로킹 방지)
    """
    global _opening_call_auction_last_summary_ts

    # ① 첫 진입 즉시 출력
    for code in newly_triggered:
        w = _opening_call_auction_watch.get(code, {})
        _opening_call_auction_watch[code]["first_printed"] = True
        prefix = "🔴 [동시호가매도조건 진입]" if is_opening_call_auction else "🔴 [매도조건 진입]"
        msg = _fmt_opening_call_auction_alert_line(code, w, prefix=prefix)
        _notify(msg, tele=True)

    # ② 1분 주기 요약 — REST API 잔고조회를 별도 스레드에서 실행
    if (now_ts - _opening_call_auction_last_summary_ts) < OPENING_CALL_AUCTION_SUMMARY_INTERVAL:
        return
    _opening_call_auction_last_summary_ts = now_ts

    # 매도조건 종목 먼저 확인 (WSS 캐시 기반 — 빠름)
    sell_lines: list[str] = []
    for code, w in sorted(_opening_call_auction_watch.items()):
        if w.get("in_sell_cond"):
            sell_lines.append(_fmt_opening_call_auction_alert_line(code, w))

    # REST 잔고조회 + 출력을 별도 스레드에서 실행 (ingest_loop 블로킹 방지)
    threading.Thread(
        target=_run_balance_monitor_bg,
        args=(list(sell_lines),),
        daemon=True,
    ).start()


def _get_str1_status_label(code: str) -> str:
    """종목상태 라벨: 57/59(30분 단일가), mkop(시장구분) 등."""
    stat = _single_price_codes.get(code)
    if stat:
        desc = "관리" if stat == "57" else "과열" if stat == "59" else stat
        return f"{stat}({desc})"
    mkop = _last_mkop_cls_code.get(code, "")
    if mkop in ("57", "59"):
        return f"{mkop}(단일가)"
    if mkop:
        return mkop
    return "-"


def _fmt_opening_call_auction_alert_line(code: str, w: dict, prefix: str = "") -> str:
    """Str1 모니터링 1줄: 종목명, 종목상태, 보유수량, 가격, 전일대비, 매수가, 손익, 사유."""
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
    stat      = _get_str1_status_label(code)

    price_label = "예상가" if code in _single_price_codes else "현재가"
    parts = [name]
    if code in _no_sell_codes:
        parts.append("[no_sell]")
    parts.append(f"[{stat}]")
    parts.append(f"보유={qty}주")
    parts.append(f"{price_label}={antc:,.0f}" if antc > 0 else f"{price_label}=N/A")
    parts.append(f"전일대비={prdy:+.2f}%")
    if buy_p > 0:
        parts.append(f"매입단가={buy_p:,.0f}  손익≈{diff:+,.0f}({diff_pct:+.2f}%)")
    if reason:
        parts.append(f"사유={reason}{elapsed}")
    line = "  ".join(parts)
    return f"{prefix} {line}".strip() if prefix else line


def _print_opening_call_auction_order_summary() -> None:
    """08:59:50~09:00 동시호가 주문현황 정리 (체결대기/취소요청/이상없음 구분)."""
    global _opening_call_auction_order_summary_done
    if _opening_call_auction_order_summary_done:
        return
    _opening_call_auction_order_summary_done = True

    cancelled_done: list[str] = []   # 취소 완료 (로그에서)
    with _str1_sell_state_lock:
        cancelled_codes = {c[0] for c in _opening_call_auction_cancelled_log}
        fill_wait: list[str] = []    # 체결대기 (주문접수완료, 09:00 시초가 체결 예정)
        order_ok: list[str] = []     # 이상없음 (주문 없음)
        cancel_req: list[str] = []   # 취소 요청 중
        for code, name, reason in _opening_call_auction_cancelled_log:
            cancelled_done.append(f"  {name}({code})  사유={reason}")
        for code, st in _str1_sell_state.items():
            if st.get("sold"):
                continue
            name = st.get("name", code_name_map.get(code, code))
            qty = int(st.get("qty", 0))
            ordno = str(st.get("ordno", "")).strip()
            if code in cancelled_codes:
                continue  # 취소완료는 위 cancelled_done에 이미 포함
            if st.get("opening_call_auction_cancel_requested"):
                cancel_req.append(f"  {name}({code}) 수량={qty}주  주문번호={ordno or '-'}  [취소요청중]")
            elif st.get("opening_call_auction_ordered"):
                fill_wait.append(f"  {name}({code}) 수량={qty}주  주문번호={ordno or '-'}  [매도주문접수완료→시초가 체결대기]")
            else:
                order_ok.append(f"  {name}({code}) 수량={qty}주  [매도주문 없음]")

    if not fill_wait and not cancel_req and not order_ok and not cancelled_done:
        _opening_call_auction_order_summary_done = True
        return

    _opening_call_auction_order_summary_done = True
    now_str = datetime.now(KST).strftime("%H:%M:%S")
    lines = [f"{'='*60}", f"[동시호가 주문현황 정리] {now_str} (09:00 전 최종)", "=" * 60]
    if fill_wait:
        lines.append(f"  ▼ 체결대기 ({len(fill_wait)}종목) ─────────────────────")
        lines.extend(fill_wait)
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
    if fill_wait:
        _notify(f"[동시호가현황] 체결대기 {len(fill_wait)}건 (주문접수완료→09:00 시초가 체결 예정)", tele=True)


_last_snapshot_ts: float = 0.0


def writer_loop():
    """1분마다 _part_buffer 를 part 파일로 flush. 5분마다 지표 스냅샷 저장."""
    global _last_snapshot_ts
    logger.info("[writer] started")
    while not _stop_event.is_set():
        try:
            time_ok = (time.time() - _last_save_time) >= PART_FLUSH_SEC
            if time_ok:
                with _part_buffer_lock:
                    has_data = bool(_part_buffer)
                if has_data:
                    # 15:30:00~15:31:00: 구독전환 hot path 보호 → periodic flush 가드
                    _now_t = datetime.now(KST).time()
                    if not (dtime(15, 30, 0) <= _now_t < dtime(15, 31, 0)):
                        _flush_part_buffer("periodic", force_full=True)
            # ── 지표 스냅샷 주기 저장 (5분 간격) ──
            now_ts = time.time()
            if now_ts - _last_snapshot_ts >= SNAPSHOT_INTERVAL_SEC:
                _last_snapshot_ts = now_ts
                if _ema_state and datetime.now(KST).time() >= dtime(9, 0):
                    _save_indicator_snapshot()
        except Exception as e:
            logger.error(f"[writer] save error: {e}")
            logger.error(traceback.format_exc())
        time.sleep(0.5)

    # 종료 시 최종 저장
    try:
        _flush_part_buffer("shutdown", force_full=True)
        _save_indicator_snapshot()  # 종료 시 최종 스냅샷
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

    if result is None or len(result) == 0:
        return

    # 수신 시점 컬럼명 소문자 통일 (ccnl_krx 대문자 vs exp_ccnl_krx 소문자 → concat 시 중복 방지)
    result = result.rename({c: c.strip().lower() for c in result.columns})

    # H0STCNI0 체결통보: 별도 로그 저장 + 체결 시 state 갱신 (parquet 미포함)
    trid = str(tr_id)
    if trid == "H0STCNI0":
        recv_ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S.%f")

        # ── [260512] 복호화 실패 가드 (1회 텔레그램 알림) ──
        # 정상: cust_id = 평문 HTS ID (영숫자 8~12자, 예: 'sywems12')
        # 비정상: cust_id 가 Base64 암호문 (길이 >20 + '/', '+', '=' 포함)
        # → 체결통보 미수신 위험을 사용자에게 즉시 통지
        global _h0stcni0_decrypt_warn_sent
        if not _h0stcni0_decrypt_warn_sent and len(result) > 0:
            try:
                sample_cust_id = str(result.row(0, named=True).get("cust_id") or "")
                if len(sample_cust_id) > 20 or any(c in sample_cust_id for c in "/+="):
                    _h0stcni0_decrypt_warn_sent = True
                    warn_msg = (
                        f"{ts_prefix()} ★★ [체결통보] H0STCNI0 복호화 실패 의심: "
                        f"cust_id 가 평문 HTS ID 가 아닌 Base64 형태 — 체결통보 미수신 위험"
                    )
                    logger.error(warn_msg)
                    _notify(warn_msg, tele=True)
            except Exception:
                pass

        _write_ccnl_notice_log(result, recv_ts)
        # ── 가독성 로그 ──
        for row in result.to_dicts():
            try:
                cntg_yn = str(row.get("cntg_yn", "")).strip()
                if cntg_yn not in ("1", "2"):
                    continue
                code = str(row.get("stck_shrn_iscd", "")).strip().zfill(6)
                seln = str(row.get("seln_byov_cls", "")).strip()
                side = "매도" if seln in ("01", "1") else "매수"
                name = code_name_map.get(code, "") or code
                label = f"{name}({code})" if name != code else code
                oder_no = str(row.get("oder_no", "")).strip()
                if cntg_yn == "1":  # 접수
                    _raw_qty = row.get("cntg_qty", 0)
                    try:
                        qty = int(float(str(_raw_qty).replace(",", "").strip() or "0"))
                    except (ValueError, TypeError):
                        qty = 0
                    qty_s = f" {qty}주" if qty else ""
                    _notify(f"{ts_prefix()} [체결통보-접수] {side} {label}{qty_s} | 주문번호={oder_no}", tele=True)
                else:  # 체결
                    qty = int(float(str(row.get("cntg_qty", 0) or 0).replace(",", "") or 0))
                    price = int(float(str(row.get("cntg_unpr", 0) or 0).replace(",", "") or 0))
                    rmn = int(float(str(row.get("rmn_qty", 0) or 0).replace(",", "") or 0))
                    pnl_txt = ""
                    reason_txt = ""
                    buy_txt = ""
                    if seln in ("01", "1") and price > 0:  # 매도체결 → PNL/사유/매수가 첨부
                        # [260526] 매도 통보에 사유/매수가 누락되어 있어 부가 정보 추가
                        # (str1_sell 이 _str1_sell_state[code]["sell_reason"]/["buy_price"] 에 기록함)
                        with _str1_sell_state_lock:
                            st = _str1_sell_state.get(code, {})
                            bp = float(st.get("buy_price") or 0)
                            sell_reason = str(st.get("sell_reason") or "").strip()
                        if bp > 0:
                            pnl_info = calc_sell_pnl(bp, price, qty)
                            pnl_txt = f" | PNL≈{pnl_info['pnl']:+,.0f}원({pnl_info['ret_pct']:+.2f}%)"
                            buy_txt = f" | 매수가={int(bp):,}원"
                        if sell_reason:
                            reason_txt = f" | 사유={sell_reason}"
                    _notify(f"{ts_prefix()} [체결통보-체결] {side} {label} {qty}주 x {price:,}원 | 잔여={rmn} | 주문번호={oder_no}{buy_txt}{pnl_txt}{reason_txt}", tele=True)
            except Exception:
                pass
        _on_ccnl_notice_filled(result, recv_ts)
        return

    # H0STMKO0 장운영정보: VI 발동/해제 감지 (parquet 미포함)
    if trid == "H0STMKO0":
        _on_market_status_krx(result)
        return
    # polars는 중복 컬럼명을 자동으로 suffix 처리하므로 별도 제거 불필요

    trid = str(tr_id)
    if trid not in TRID_TO_KIND and trid in KNOWN_TRID_MAP:
        kind, is_real = KNOWN_TRID_MAP[trid]
        TRID_TO_KIND[trid] = kind
        TRID_TO_IS_REAL[trid] = is_real
    else:
        kind = TRID_TO_KIND.get(trid, "unknown")
        is_real = TRID_TO_IS_REAL.get(trid, None)

    recv_ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S.%f")
    # result는 이미 Polars DataFrame → ingest_loop는 Polars 네이티브로 처리
    _ingest_queue.put((result, trid, kind, is_real, recv_ts))


def ingest_loop():
    global _last_print_ts, _last_summary_ts, _last_full_status_ts, _since_save_rows, _last_any_recv_ts, _part_buffer_rows, _no_data_rebuild_count
    logger.info("[ingest] started")
    while not _stop_event.is_set() or not _ingest_queue.empty():
        try:
            item = _ingest_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        if item is None:
            break
        # ── [측정] item 1건 처리 시작 시각 (30s 주기 1회 max/avg/n 로그) ──
        _proc_t0 = time.perf_counter()
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

        # ── 통합 zfill + partition_by (1회) ──────────────────────────────
        prdy_col = col_map.get("prdy_ctrt")
        pr_col = col_map.get("stck_prpr")
        mkop_col = col_map.get("new_mkop_cls_code")
        vi_prc_col = col_map.get("vi_stnd_prc")
        oprc_col_check = col_map.get("stck_oprc") if (_vi_delayed_codes and kind == "regular_exp") else None

        try:
            tmp = result.with_columns(
                pl.col(code_col).cast(pl.Utf8).str.zfill(6).alias(code_col)
            )

            # 저장용 추가 컬럼 (save=True일 때만)
            if save:
                if is_real is None:
                    is_real = "N" if "exp" in kind else "Y"
                tmp = tmp.with_columns([
                    pl.lit(recv_ts).alias("recv_ts"),
                    pl.lit(trid).alias("tr_id"),
                    pl.lit(is_real).alias("is_real_ccnl"),
                ])

            chunks: list[pl.DataFrame] = []
            _vi_delayed_changed = False

            for df_code in tmp.partition_by(code_col, maintain_order=True):
                code = str(df_code[code_col][0])

                # ── (1) 모니터링 카운트 ──
                if code in _per_sec_counts:
                    n = len(df_code)
                    _per_sec_counts[code] = (_per_sec_counts[code] + n) % PER_SEC_ROLLOVER
                    _total_counts[code] += n
                    _since_save_rows += n
                    _since_save_counts[code] += n
                    _last_recv_ts[code] = now
                    _last_any_recv_ts = now
                    _no_data_rebuild_count = 0  # 데이터 수신 → 재구독 실패 카운터 리셋
                    # 종목별 구독 전환 후 첫 실시간 시세 정상 수신 로깅
                    # (시장 시세 stck_prpr=현재가. 내 주문 체결[체결통보-체결]과 무관 — 혼동 방지 위해 용어 분리)
                    prev_trid = _last_trid_per_code.get(code)
                    if prev_trid and prev_trid != trid:
                        name = code_name_map.get(code, code)
                        _pr_col = col_map.get("stck_prpr")
                        _price = ""
                        if _pr_col and _pr_col in df_code.columns:
                            try:
                                _price = f" 현재가={int(float(str(df_code[_pr_col][-1]).replace(',', '') or 0)):,}"
                            except Exception:
                                pass
                        logger.info(
                            f"{ts_prefix()} [WSS전환후 첫 시세 정상수신] {name}({code}){_price}"
                        )
                    _last_trid_per_code[code] = trid
                    # 57/59 종목이 H0STCNT0(실시간체결) 수신 → 30분 단일가 아님 → 일반종목 전환
                    if code in _single_price_codes and trid == "H0STCNT0":
                        stat = _single_price_codes.pop(code)
                        name = code_name_map.get(code, code)
                        msg = (f"{ts_prefix()} [단일가해제] {name}({code}) "
                               f"iscd_stat={stat} 실시간체결(H0STCNT0) 수신 → 일반종목 전환")
                        logger.info(msg)
                        _notify(msg, tele=True)
                    # 57/59 후보 종목: 정시 윈도우 관찰 후 30분 단일가 확정/해제
                    if code in _single_price_pending and trid != "FHKST01010100":
                        t_now = datetime.now(KST).time()
                        m, s = t_now.minute, t_now.second
                        in_single_window = (m in (0, 30) and s < 30) or (m in (29, 59) and s >= 30)
                        if in_single_window:
                            # 정시 윈도우 내 첫 틱 → 관찰 시작 (즉시 확정하지 않음)
                            stat = _single_price_pending.pop(code)
                            _single_price_observe[code] = {"stat": stat, "first_ts": now, "tick_count": 1}
                            name = code_name_map.get(code, code)
                            desc = "57(관리종목)" if stat == "57" else "59(단기과열)"
                            logger.info(f"{ts_prefix()} [KRX조회] {name}({code}) {desc} 정시 윈도우 첫 틱 → 관찰 시작 (10초간 추가 틱 확인)")
                        else:
                            stat = _single_price_pending.pop(code)
                            name = code_name_map.get(code, code)
                            msg = f"{ts_prefix()} [단일가해제] {name}({code}) iscd_stat={stat} 비단일가 시간대 틱 수신 → 일반종목"
                            logger.info(msg)
                            _notify(msg, tele=True)
                    # 관찰 중인 종목: 추가 틱으로 연속 거래 여부 판정
                    if code in _single_price_observe and trid != "FHKST01010100":
                        obs = _single_price_observe[code]
                        obs["tick_count"] += 1
                        elapsed = (now - obs["first_ts"]).total_seconds()
                        name = code_name_map.get(code, code)
                        if obs["tick_count"] >= 3 and elapsed < 15:
                            # 15초 내 3건 이상 → 연속 거래 (단일가 아님)
                            _single_price_observe.pop(code)
                            msg = f"{ts_prefix()} [단일가해제] {name}({code}) iscd_stat={obs['stat']} 정시 윈도우 내 {obs['tick_count']}틱/{elapsed:.0f}초 → 연속거래 판정 (일반종목)"
                            logger.info(msg)
                            _notify(msg, tele=True)
                        elif elapsed >= 15:
                            # 15초 경과 + 3건 미만 → 단일가 확정
                            _single_price_observe.pop(code)
                            _single_price_codes[code] = obs["stat"]
                            desc = "57(관리종목)" if obs["stat"] == "57" else "59(단기과열)"
                            msg = f"{ts_prefix()} [KRX조회] 30분 단일가 확정 {name}({code}) {desc} ({obs['tick_count']}틱/{elapsed:.0f}초 관찰)"
                            logger.info(msg)
                            _notify(msg, tele=True)
                    # WSS 전용 수신 시각 (REST 제외)
                    if trid != "FHKST01010100":
                        _last_wss_recv_ts[code] = now
                        # 틱 간격 추적 (갑자기 끊김 감지용)
                        if code not in _recent_tick_ts:
                            _recent_tick_ts[code] = deque(maxlen=5)
                        _recent_tick_ts[code].append(now)
                    if kind == "regular_real" and _get_mode() == RunMode.CLOSE_REAL:
                        _regular_real_seen.add(code)
                    if kind == "overtime_real":
                        _overtime_real_seen.add(code)

                # ── (2) 캐시 업데이트 ──
                if code and code != "None" and code != "000000":
                    if prdy_col:
                        try:
                            _last_prdy_ctrt[code] = float(df_code[prdy_col][-1])
                        except Exception:
                            pass
                    if pr_col:
                        try:
                            _last_stck_prpr[code] = float(str(df_code[pr_col][-1]).replace(",", "") or 0)
                        except Exception:
                            pass
                    if mkop_col:
                        try:
                            v = str(df_code[mkop_col][-1] or "").strip()
                            if v:
                                prev_mkop = _last_mkop_cls_code.get(code, "")
                                if prev_mkop and prev_mkop != v:
                                    name = code_name_map.get(code, code)
                                    _mkop_names = {"00": "장중", "10": "동시호가", "20": "장마감", "30": "시간외단일가"}
                                    prev_label = _mkop_names.get(prev_mkop, prev_mkop)
                                    cur_label = _mkop_names.get(v, v)
                                    logger.info(
                                        f"{ts_prefix()} [실시간체결통보] {name}({code}) "
                                        f"new_mkop_cls_code: {prev_mkop}({prev_label}) → {v}({cur_label})"
                                    )
                                _last_mkop_cls_code[code] = v
                        except Exception:
                            pass
                    # VI기준가 캐시 (실시간체결에만 존재)
                    if vi_prc_col:
                        try:
                            vp = float(str(df_code[vi_prc_col][-1]).replace(",", "") or 0)
                            if vp > 0:
                                _vi_stnd_prc[code] = vp
                        except Exception:
                            pass

                # ── (3) VI 지연 해제 ──
                if oprc_col_check and code in _vi_delayed_codes:
                    try:
                        oprc_val = float(str(df_code[oprc_col_check][-1]).replace(",", "") or 0)
                        if oprc_val > 0:
                            _vi_delayed_codes.discard(code)
                            _vi_delayed_changed = True
                            name = code_name_map.get(code, code)
                            logger.info(
                                f"{ts_prefix()} [VI지연해제] {name}({code}) "
                                f"stck_oprc={oprc_val:,.0f} → 시가형성 확인, 실시간체결 전환"
                            )
                    except (ValueError, TypeError):
                        pass

                # ── (4) 저장 + 지표 계산 ──
                if save:
                    # ── 기술적 지표 계산 (EMA + BB) — Polars 네이티브 ──
                    _pr_col_save = "stck_prpr"
                    if _pr_col_save in df_code.columns:
                        ind_rows: list[dict] = []
                        for pr_val in df_code[_pr_col_save]:
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
                    # ── VI / 장운영 이벤트 컬럼 추가 ──
                    # vi_start_ts/vi_end_ts는 VI 활성 중에만 기록 (해제 후 계속 기록 방지)
                    _is_vi = code in _vi_active_codes
                    _mkt = _code_market_map.get(code, "")
                    _mkt_evt = _market_wide_event.get(_mkt, "") or _market_event.get(code, "")
                    df_code = df_code.with_columns([
                        pl.lit("Y" if _is_vi else "").alias("vi_yn"),
                        # [260602] H0STMKO0 마지막 vi_cls_code 원값 저장 (발동='Y'/해제='N', H0STMKO0 미구독 종목은 "")
                        pl.lit(_vi_cls_cache.get(code, "")).alias("vi_cls_code"),
                        pl.lit(_vi_start_ts.get(code, "") if _is_vi else "").alias("vi_start_ts"),
                        pl.lit(_mkt_evt).alias("market_event"),
                    ])
                    chunks.append(df_code)

            # VI 지연 해제 후 rebuild (루프 밖)
            if _vi_delayed_changed and not _vi_delayed_codes:
                _trigger_ws_rebuild()

        except Exception:
            pass

        # 15:20~15:30 예상체결가 모니터링 (Polars DataFrame 그대로 전달)
        if kind == "regular_exp" and _get_mode() == RunMode.CLOSE_EXP:
            try:
                _run_closing_exp_monitor_tick(result, code_col, col_map)
            except Exception as e:
                logger.warning(f"{ts_prefix()} [종가모니터] tick 처리 오류: {e}")

        # _part_buffer 에 추가 (1000행 또는 1분마다 part 파일로 flush)
        if save and chunks:
            try:
                with _part_buffer_lock:
                    for chunk in chunks:
                        _part_buffer.append(chunk)
                    total_n = sum(len(c) for c in chunks)
                    _part_buffer_rows += total_n
                # 1500행 도달 시 오래된 1000행 flush
                # 15:30:00~15:31:00 구간은 구독전환과 동시 발생 시 hot path 차단 → flush 가드
                if _part_buffer_rows >= PART_FLUSH_THRESHOLD:
                    _now_t = datetime.now(KST).time()
                    if not (dtime(15, 30, 0) <= _now_t < dtime(15, 31, 0)):
                        _flush_part_buffer("1000rows")
            except Exception:
                logger.error("[ingest] part buffer append failed")
                logger.error(traceback.format_exc())

        # ── Str1 실전 매도 조건 체크 ─────────────────────────────────────────
        if STR1_SELL_ENABLED and _str1_sell_state:
            try:
                _check_str1_sell_conditions(result, col_map, code_col, kind)
            except Exception as _e:
                logger.debug(f"[str1_sell] 조건 체크 예외: {_e}")

        # ── [v_1] 상한가 근접 매수: 25~28% 감지 + Exit 모니터링 ───────────────
        if UPLIMIT_BUY_ENABLED:
            try:
                _check_uplimit_conditions_from_tick(result, col_map, code_col, kind)
            except Exception as _e:
                logger.debug(f"[uplimit] 조건 체크 예외: {_e}")

        # ── [260424] Strategy A-v4 익일 overnight 포지션 매도 판정 ──────────
        if UPLIMIT_V4_ENABLED and _uplimit_v4_holdover:
            try:
                _check_v4_nextday_exit(result, col_map, code_col, kind)
            except Exception as _e:
                logger.debug(f"[v4_nextday] 조건 체크 예외: {_e}")

        # ── [260423] Strategy B/C 신호 관찰 (로그만, 매매 안 함) ───────────────
        if MA_TREND_ENABLED or BB_EXPANSION_ENABLED:
            try:
                _check_strategy_bc_from_tick(result, col_map, code_col, kind)
            except Exception as _e:
                logger.debug(f"[strategy_bc] 조건 체크 예외: {_e}")

        # ── [260523] Strategy str2 매수/매도 (STR2_ENABLED 게이트, 기본 False) ──
        if STR2_ENABLED:
            try:
                _check_str2_from_tick(result, col_map, code_col, kind)
            except Exception as _e:
                logger.debug(f"[str2] 조건 체크 예외: {_e}")
            try:
                # carry_hold 익일 장전 매도 (regular_exp 틱에서만 동작)
                _check_str2_carry_premarket(result, col_map, code_col, kind)
            except Exception as _e:
                logger.debug(f"[str2_carry] 조건 체크 예외: {_e}")

        # ── VI 발동 종목 예상체결가 모니터링 ──────────────────────────────────
        if _vi_active_codes and "exp" in kind:
            try:
                _monitor_vi_exp_price(result, col_map, code_col)
            except Exception as _e:
                logger.debug(f"[VI모니터] 예외: {_e}")

        if print_option == 2 and (now - _last_print_ts) >= 1.0:
            _print_counts()
            for c in _per_sec_counts:
                _per_sec_counts[c] = 0
            _last_print_ts = now

        if (now - _last_summary_ts) >= SUMMARY_EVERY_SEC:
            total_rows = sum(_total_counts.values())
            saved_rows = _written_rows
            accum_rows = _part_buffer_rows

            logger.info(
            f"[summary] total_rows={total_rows} saved_rows={saved_rows} "
            f"buf={accum_rows} saved={saved_rows} -> parts"
            )
            _last_summary_ts = now

        if (now - _last_full_status_ts) >= FULL_STATUS_EVERY_SEC:
            _log_full_progress()
            _last_full_status_ts = now

        # ── [측정] item 1건 처리 종료 — 30s 주기 max/avg/n 로그 ──
        # 측정 자체가 hot path 를 부수지 않도록 try/except 로 감쌈
        try:
            _proc_dt_ms = (time.perf_counter() - _proc_t0) * 1000.0
            global _ingest_lag_max_ms, _ingest_lag_sum_ms, _ingest_lag_count, _ingest_lag_last_print_ts
            if _proc_dt_ms > _ingest_lag_max_ms:
                _ingest_lag_max_ms = _proc_dt_ms
            _ingest_lag_sum_ms += _proc_dt_ms
            _ingest_lag_count += 1
            if now - _ingest_lag_last_print_ts >= 30.0:
                qsz = _ingest_queue.qsize()
                avg = _ingest_lag_sum_ms / max(1, _ingest_lag_count)
                logger.info(
                    f"{ts_prefix()} [ingest_lag] qsize={qsz} 처리시간 max={_ingest_lag_max_ms:.1f}ms "
                    f"avg={avg:.2f}ms n={_ingest_lag_count} (직전 30s 기준)"
                )
                _ingest_lag_max_ms = 0.0
                _ingest_lag_sum_ms = 0.0
                _ingest_lag_count = 0
                _ingest_lag_last_print_ts = now
        except Exception:
            pass
    logger.info("[ingest] stopped")

# =============================================================================
# 런타임 상태 기록 (config.json 의 root key)
# =============================================================================
# [260506] watchdog 협업용 — 시작 시 1, 정상 종료 시 2
# 워치독은 status=2 인데 프로세스가 살아있으면 강제 종료한다.
RUNTIME_STATUS_KEY = "ws_realtime_trading_status"
RUNTIME_STATUS_RUNNING = 1
RUNTIME_STATUS_STOPPED = 2


def _set_runtime_status(status: int) -> None:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            _cfg = json.load(f)
        _cfg[RUNTIME_STATUS_KEY] = status
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(_cfg, f, ensure_ascii=False, indent=2)
        logger.info(f"[runtime_status] {RUNTIME_STATUS_KEY}={status}")
    except Exception as e:
        logger.warning(f"[runtime_status] 기록 실패: status={status} err={e}")


# [260506] WSS 자기보호 자동중단 — 핸드셰이크 실패 N회 연속 시 호출
def _wss_self_stop(reason: str, count: int) -> None:
    # [260513] exit=0 → exit=2 변경. runner.sh 는 exit=0 을 "정상 종료"로 인식해
    # 재시작하지 않았으므로 (260513 15:28 사건: 장 중반 사망 후 부활 불가),
    # 비정상 종료(exit=2, watchdog 강제종료 분기)로 통일해 runner 가 재시작하도록 한다.
    msg = (
        f"{ts_prefix()} [ws][자기보호중단] KIS WSS 핸드셰이크 {count}회 연속 실패 → "
        f"IP throttle 회피를 위해 reconnect 중단 + 프로세스 종료 (exit=2, runner 재시작 유도).\n"
        f"  사유: {reason}\n"
        f"  ※ 시장 사이드카/서킷브레이커와 무관 — 자기측 connection 보호용"
    )
    try:
        logger.warning(msg)
    except Exception:
        pass
    try:
        _notify(msg, tele=True)
    except Exception as e:
        try:
            logger.warning(f"[자기보호중단] 텔레그램 통보 실패: {e}")
        except Exception:
            pass
    try:
        with _kws_lock:
            if _active_kws is not None:
                _request_ws_close(_active_kws)
    except Exception:
        pass
    _set_runtime_status(RUNTIME_STATUS_STOPPED)
    time.sleep(0.5)
    os._exit(2)


# =============================================================================
# 종료 처리
# =============================================================================
def _shutdown(reason: str):
    global _active_kws
    if _stop_event.is_set():
        return
    _ts_now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    logger.warning(f"\n{'=' * 66}\n[shutdown] {reason}  @ {_ts_now}\n{'=' * 66}")
    _stop_event.set()
    _ws_rebuild_event.set()
    t0 = time.time()  # [260423] 단계별 경과시간 측정
    # WSS 종료 (새 데이터 유입 차단) — snapshot pattern
    # [260511] lock 안에서 _request_ws_close 호출하면 좀비 hang 시 close 자체가 hang
    #         → KIS 측 ALREADY IN USE 잔재 유발. snapshot 으로 lock 즉시 풀고 lock 밖에서 close
    with _kws_lock:
        kws_snap = _active_kws
        _active_kws = None  # 다른 스레드의 추가 send 방지
    if kws_snap is not None:
        # close 는 정상 소켓이면 즉시 끝난다. 타임아웃은 _request_ws_close 내부에서 단계별로 관리하며,
        # 여기 외곽은 6s 안전망(내부 UNSUBSCRIBE 예산 2s + close 여유).
        _close_t = threading.Thread(target=_request_ws_close, args=(kws_snap,), daemon=True)
        _close_t.start()
        _close_t.join(timeout=6.0)
        if _close_t.is_alive():
            logger.warning(f"[shutdown] _request_ws_close 6s timeout — 계속 진행 (self-heal 로 복구)")
    t_wss = time.time()
    logger.info(f"[shutdown] (1/4) WSS 종료 완료 (+{t_wss-t0:.2f}s)")
    # ── 메모리 데이터 저장 (WSS 종료 후, 큐 소진 대기 → flush → snapshot) ──
    deadline = time.time() + 8.0
    try:
        while not _ingest_queue.empty() and time.time() < deadline:
            time.sleep(0.2)
        t_ingest = time.time()
        logger.info(f"[shutdown] (2/4) ingest 큐 소진 완료 (남은={_ingest_queue.qsize()}건, +{t_ingest-t_wss:.2f}s)")
    except Exception as e:
        logger.warning(f"[shutdown] ingest 큐 대기 예외: {e}")
        t_ingest = time.time()
    try:
        n_saved = _flush_part_buffer(f"shutdown_{reason}", force_full=True)
        t_flush = time.time()
        logger.info(f"[shutdown] (3/4) part buffer flush 완료: {n_saved}행 저장 (+{t_flush-t_ingest:.2f}s)")
    except Exception as e:
        logger.warning(f"[shutdown] part buffer flush 실패: {e}")
        t_flush = time.time()
    try:
        _save_indicator_snapshot()
        t_snap = time.time()
        logger.info(f"[shutdown] (4/4) 지표 스냅샷 저장 완료 (+{t_snap-t_flush:.2f}s, 총 {t_snap-t0:.2f}s)")
    except Exception as e:
        logger.warning(f"[shutdown] 지표 스냅샷 저장 실패: {e}")
    time.sleep(0.3)
    # [260506] 좀비 reconnect 루프 차단 — kws.start() 가 KIS 라이브러리 내부에서
    # 자동 reconnect 루프를 도는 경우 _stop_event 만으로는 빠져나오지 않아
    # 47시간 좀비 thread 가 매 새벽 KIS 핸드셰이크를 시도하며 appkey 잔재를 남겼다.
    # 모든 정리 단계가 끝났으므로 process 자체를 강제 종료한다 (runner 는 정상 종료로 인식).
    _set_runtime_status(RUNTIME_STATUS_STOPPED)
    logger.info("[shutdown] os._exit(0) — 좀비 thread 차단을 위한 강제 종료")
    os._exit(0)

def _handle_signal(signum, frame):
    _shutdown(f"signal={signum}")
    raise SystemExit(0)

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

def _request_ws_close(kws) -> None:
    # [260428] close frame 송신 전 active 구독 명시적 UNSUBSCRIBE 송신 → KIS 가 즉시 정리
    # → 재시작 시 ALREADY IN USE storm 차단. (이전: close 만 호출 → KIS 측 정리 지연)
    # 부모 main 의 _subscribed 만 대상 (a2 자식은 자체 cmd_queue 의 stop 으로 정리됨).
    # [260511] kws is _active_kws 체크 제거 — _shutdown 이 _active_kws=None 으로 먼저
    #         설정 후 호출하므로, 항상 _subscribed 기반으로 UNSUBSCRIBE 시도.
    #         각 send 는 1초 thread timeout 으로 dead socket hang 방지.
    # [260602] 단계별 소요시간 측정 + UNSUBSCRIBE 총예산 cap.
    #   기존: UNSUBSCRIBE(최대 5타입 × 1s) 가 외곽 예산(5s)을 다 먹으면 close() 가 호출조차 안 됨
    #   → close frame 미전송 → KIS 세션 잔재 → 재시작 직후 새 접속 1006. close 를 항상 실행하도록
    #   UNSUBSCRIBE 총예산을 2s 로 묶고, 어느 단계가 시간을 먹는지 로그로 남겨 원인을 확정한다.
    _t0 = time.time()
    _UNSUB_BUDGET = 2.0
    try:
        _name_to_req = {
            "ccnl_krx": ccnl_krx,                  # noqa: F405
            "exp_ccnl_krx": exp_ccnl_krx,          # noqa: F405
            "ccnl_notice": ccnl_notice,            # noqa: F405
            "overtime_ccnl_krx": overtime_ccnl_krx,        # noqa: F405
            "overtime_exp_ccnl_krx": overtime_exp_ccnl_krx,# noqa: F405
        }
        for _name, _codes in list(_subscribed.items()):
            if time.time() - _t0 >= _UNSUB_BUDGET:
                logger.warning(f"{ts_prefix()} [shutdown] UNSUBSCRIBE 예산({_UNSUB_BUDGET}s) 소진 → 남은 종목 skip, close 진행")
                break
            _req = _name_to_req.get(_name)
            if not _req or not _codes:
                continue
            _st = time.time()
            _ut = threading.Thread(
                target=_send_subscribe,
                args=(kws, _req, list(_codes), "2"),
                daemon=True,
            )
            _ut.start()
            _ut.join(timeout=1.0)
            if _ut.is_alive():
                logger.warning(f"{ts_prefix()} [shutdown] UNSUBSCRIBE timeout ({_name}, +{time.time()-_st:.2f}s)")
            else:
                logger.info(f"{ts_prefix()} [shutdown] UNSUBSCRIBE ok ({_name}, +{time.time()-_st:.2f}s)")
    except Exception as _e:
        logger.debug(f"{ts_prefix()} [shutdown] UNSUBSCRIBE 단계 skip: {_e}")

    # close frame 송신 (반드시 실행 — KISWebSocket.close 내부 timeout 보유)
    logger.info(f"{ts_prefix()} [shutdown] UNSUBSCRIBE 단계 종료(+{time.time()-_t0:.2f}s) → close frame 송신 시작")
    _tc = time.time()
    for attr in ("close", "shutdown"):
        try:
            fn = getattr(kws, attr, None)
            if callable(fn):
                fn()
                logger.info(f"{ts_prefix()} [shutdown] close() 반환 (+{time.time()-_tc:.2f}s)")
                return
        except Exception as _ce:
            logger.warning(f"{ts_prefix()} [shutdown] close({attr}) 예외(+{time.time()-_tc:.2f}s): {_ce}")
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
    action = "구독 추가" if tr_type == "1" else "구독 해제"
    for idx, chunk in enumerate(_chunks(data, MAX_CODES_PER_SESSION), start=1):
        try:
            kws.send_request(request=req, tr_type=tr_type, data=chunk)
            code_names = [f"{code_name_map.get(c, c)}({c})" for c in chunk]
            logger.info(
                f"{ts_prefix()} [{action} 결과] TR_ID={rid or 'unknown'} "
                f"종목={', '.join(sorted(code_names))}"
            )
        except Exception as e:
            code_names = [f"{code_name_map.get(c, c)}({c})" for c in chunk]
            logger.warning(
                f"{ts_prefix()} [{action} 실패] TR_ID={rid or 'unknown'} "
                f"종목={', '.join(sorted(code_names))} err={e}"
            )

def _apply_subscriptions(kws, desired: dict, force: bool = False) -> None:
    """
    desired: {request_func: set(종목코드)} — _desired_subscription_map()의 반환값
    종목별로 올바른 구독 타입을 적용합니다.
    구독 슬롯 초과 방지: 해제 완료 후 대기 → 신규 구독 (batch 10개씩)
    """
    all_reqs = [ccnl_krx, exp_ccnl_krx, overtime_ccnl_krx, overtime_exp_ccnl_krx]  # noqa: F405

    if force:
        # ── force 재구독: ALREADY IN SUBSCRIBE 방지 위해 UNSUBSCRIBE 후 SUBSCRIBE ──
        for req in all_reqs:
            want = desired.get(req, set())
            if want:
                _cnm = code_name_map if "code_name_map" in globals() else {}
                _names = [_cnm.get(c, c) for c in sorted(want)]
                logger.info(f"{ts_prefix()} [ws] force 재구독: {req.__name__} {len(want)}종목 {_names}")
                _send_subscribe(kws, req, list(want), "2")  # UNSUBSCRIBE 먼저
                time.sleep(0.05)
                _send_subscribe(kws, req, list(want), "1")  # SUBSCRIBE
                _subscribed[req.__name__] = set(want)
            else:
                _subscribed.pop(req.__name__, None)
        return

    # ── 1단계: 모든 해제 대상을 먼저 일괄 UNSUBSCRIBE (슬롯 확보) ──
    pending_adds: list[tuple] = []
    any_unsub = False
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
            # batch 단위 해제 (10개씩)
            for i in range(0, len(to_del), 10):
                batch = to_del[i:i+10]
                _send_subscribe(kws, req, batch, "2")
            any_unsub = True
        if to_add:
            pending_adds.append((req, to_add))

    # ── 해제 → 신규 구독 사이 서버 처리 대기 ──
    if any_unsub and pending_adds:
        time.sleep(0.1)

    # ── 2단계: 신규 구독 (batch 10개씩) ────
    for req, add_list in pending_adds:
        for i in range(0, len(add_list), 10):
            batch = add_list[i:i+10]
            _send_subscribe(kws, req, batch, "1")  # SUBSCRIBE

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
        single_codes = (_single_price_codes.keys() | _single_price_pending.keys()) & all_codes
        if single_codes and t >= dtime(8, 59, 29):
            return {
                exp_ccnl_krx: all_codes - single_codes,
                ccnl_krx: single_codes,
            }  # noqa: F405
        return {exp_ccnl_krx: all_codes}  # noqa: F405

    if dtime(9, 0) <= t < dtime(15, 20):
        # ── VI / 57·59(30분 단일가) / 일반 종목 분리 ──
        vi_codes = _vi_delayed_codes & all_codes
        single_codes = (_single_price_codes.keys() | _single_price_pending.keys()) & all_codes
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

    if dtime(15, 20) <= t < dtime(15, 21):
        return {}  # 15:20~15:21: 주문 발송만, 예상체결가 구독은 15:21에 시작
    if dtime(15, 21) <= t < dtime(15, 30):
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
    global _close_force_stopped, _main_last_already_in_use_ts, _main_last_handshake_fail_ts
    global _main_last_invalid_approval_ts
    global _mkstatus_sub_codes, _mkstatus_pending, _active_kws
    backoff = 2
    attempt = 0

    while not _stop_event.is_set():
        mode = _get_mode()
        if mode == RunMode.EXIT:
            break

        # ── 보유종목 없으면 WSS 구독 불필요 → 18:00까지 대기 ──
        with _str1_sell_state_lock:
            _has_held = any(not st.get("sold") for st in _str1_sell_state.values())
        if not _has_held and not _base_codes:
            now_t = datetime.now(KST).time()
            if now_t < END_TIME:
                logger.info(f"{ts_prefix()} [ws] 보유종목·base codes 모두 없음 → {END_TIME.strftime('%H:%M')}까지 대기")
                while not _stop_event.is_set():
                    if datetime.now(KST).time() >= END_TIME:
                        break
                    if _base_codes:
                        logger.info(f"{ts_prefix()} [ws] base codes 추가됨 → WSS 연결 진행")
                        break
                    time.sleep(30.0)
                else:
                    break
                if datetime.now(KST).time() >= END_TIME:
                    logger.info(f"{ts_prefix()} [ws] {END_TIME.strftime('%H:%M')} 도달 → 마무리 진행")
                    break
            else:
                break

        attempt += 1
        kws = None
        try:
            logger.info(f"{ts_prefix()} [ws] connect attempt={attempt} mode={mode.name}")

            # 재연결 시 접속키(approval_key) 갱신 - htsid 만료 방지
            if attempt > 1:
                # [260513] invalid approval 직후엔 KIS 가 같은 approval_key 재발급(throttle)함.
                # auth_ws 호출 전 충분히 대기하여 새 approval_key 가 나오도록 한다.
                _elapsed_inv_appr = time.time() - _main_last_invalid_approval_ts
                if _elapsed_inv_appr < MAIN_WSS_BACKOFF_INVALID_APPROVAL_SEC:
                    _wait_inv = MAIN_WSS_BACKOFF_INVALID_APPROVAL_SEC - _elapsed_inv_appr
                    logger.warning(
                        f"{ts_prefix()} [ws] invalid approval 감지 후 "
                        f"{_wait_inv:.1f}s backoff → throttle 해소 대기 (auth_ws 전)"
                    )
                    # 단계적 sleep (stop event 빠른 응답)
                    for _ in range(int(_wait_inv * 10)):
                        if _stop_event.is_set():
                            break
                        time.sleep(0.1)
                    if _stop_event.is_set():
                        break
                try:
                    # [260601][C] 직전 연결이 dwell<15s(무효 approval_key 의심)였으면 강제 재발급.
                    # auth_ws(force_new=True)는 재발급 전 파일을 다시 읽어 다른 주체가 이미
                    # 갱신했으면 그걸 채택(핑퐁 방지)하고, 동일할 때만 새 키를 발급한다.
                    _force_new_ak = bool(getattr(run_ws_forever, "_force_new_approval", False))
                    ka.auth_ws(svr="prod", force_new=_force_new_ak)
                    run_ws_forever._force_new_approval = False
                    logger.info(
                        f"{ts_prefix()} [ws] auth_ws {'강제 재발급' if _force_new_ak else '재발급'} 완료"
                    )
                except Exception as ae:
                    logger.warning(f"{ts_prefix()} [ws] auth_ws 재발급 실패: {ae}")

                # [260423] auth_ws 실패 감지 — approval_key 가 없거나 빈 상태면 연속 실패 카운터 증가
                _ak = (getattr(ka, "_base_headers_ws", {}) or {}).get("approval_key")
                if not _ak:
                    _auth_fail = getattr(run_ws_forever, "_auth_fail_count", 0) + 1
                    run_ws_forever._auth_fail_count = _auth_fail
                    _notify(
                        f"{ts_prefix()} [ws] ★ approval_key 재발급 실패 ({_auth_fail}회 연속) — "
                        f"KIS 토큰 서버 응답 없음",
                        tele=True,
                    )
                    # 3회 연속 실패 시 프로세스 강제 종료 → runner 가 자동 재시작 (fresh approval_key)
                    if _auth_fail >= 3:
                        _notify(
                            f"{ts_prefix()} [ws] ★★ approval_key 3회 연속 실패 → "
                            f"프로세스 종료 (runner 재시작 유도)",
                            tele=True,
                        )
                        logger.error(f"{ts_prefix()} [ws] auth_ws 3회 연속 실패 → sys.exit(1)")
                        # 짧은 대기 후 강제 종료 (정상 shutdown 경로 생략 — 이미 WSS 끊긴 상태)
                        time.sleep(1.0)
                        os._exit(1)  # noqa — runner 가 감지하여 재시작
                    # 다음 재시도 전 대기 (60초 후 재시도)
                    time.sleep(60)
                    continue
                else:
                    run_ws_forever._auth_fail_count = 0   # 성공 시 리셋

            # open_map 초기화 후 현재 시각에 맞는 구독 등록 (종목별 매핑)
            ka.open_map.clear()
            _subscribed.clear()
            # H0STCNI0 체결통보: 다른 구독보다 먼저 등록 (슬롯 우선 확보)
            now_t = datetime.now(KST).time()
            acc_key = _get_ccnl_notice_tr_key()
            # [260428 진단용 toggle] H0STCNI0 + main approval_key 충돌 의심 검증
            # SKIP_CCNI0_ON_INIT=1 이면 init 구독 건너뜀 (storm 사라지면 확정)
            if os.environ.get("SKIP_CCNI0_ON_INIT") == "1":
                logger.warning(
                    f"{ts_prefix()} [체결통보] H0STCNI0 init 구독 SKIP "
                    f"(SKIP_CCNI0_ON_INIT=1, 진단용 — 매수/매도 체결 알림 못 받음)"
                )
            elif acc_key:
                ka.KISWebSocket.subscribe(ccnl_notice, [acc_key])  # noqa: F405
                _subscribed["ccnl_notice"] = {acc_key}
                logger.info(f"{ts_prefix()} [체결통보] H0STCNI0 구독 요청: tr_key={acc_key!r}")
            else:
                warn_msg = f"{ts_prefix()} [체결통보] H0STCNI0 구독 스킵: my_htsid 미설정!"
                logger.warning(warn_msg)
                _notify(warn_msg, tele=True)

            # ── H0STMKO0(장운영정보) 대상 확정: keepalive + 보류분 + 기존 동적코드 ──
            # H0STMKO0 는 _desired_subscription_map 에 포함되지 않는 순수 동적 구독이라,
            # 여기서 명시 (재)구독하지 않으면 부팅·재연결 후 keepalive/보유/VI 종목의
            # 장운영정보(서킷브레이커 판별 채널)가 0이 되는 잠재버그가 있다.
            # keepalive 종목(KOSPI/KOSDAQ 각 1)은 매 연결마다 상시 구독해 CB 판별을 보장.
            keepalive_codes = set(_MKT_KEEPALIVE_INITIAL.values())
            # WSS 미연결 시점에 보류된 _mkstatus_sub_add 요청 흡수
            if _mkstatus_pending:
                _mkstatus_sub_codes |= _mkstatus_pending
                _mkstatus_pending.clear()
            mkstatus_target = set(_mkstatus_sub_codes) | keepalive_codes
            # H0STMKO0_MAX_SLOTS cap: 초과 시 보유 > VI > keepalive 우선
            # (_mkstatus_sub_add 의 cap 우선순위와 동일 패턴)
            if len(mkstatus_target) > H0STMKO0_MAX_SLOTS:
                held_mk: set[str] = set()
                try:
                    with _str1_sell_state_lock:
                        held_mk = {c for c, st in _str1_sell_state.items() if not st.get("sold")}
                except Exception:
                    pass
                priority_held = mkstatus_target & held_mk
                priority_vi = (mkstatus_target & _vi_active_codes) - priority_held
                priority_ka = (mkstatus_target & keepalive_codes) - priority_held - priority_vi
                priority_rest = mkstatus_target - priority_held - priority_vi - priority_ka
                ordered = (list(priority_held) + list(priority_vi)
                           + list(priority_ka) + list(priority_rest))
                mkstatus_target = set(ordered[:H0STMKO0_MAX_SLOTS])
                logger.warning(
                    f"{ts_prefix()} [H0STMKO0] open_map cap({H0STMKO0_MAX_SLOTS}) 초과 "
                    f"→ 보유/VI/keepalive 우선만 구독"
                )
            _mkstatus_sub_codes = mkstatus_target
            # keepalive 종목 부작용 방지:
            #  - 마켓 매핑 보강 (CB 마켓전파 시 _code_market_map 필요)
            #  - 허위 [VI해제] 방지: 첫 프레임 vi_cls="0" 이고 prev_vi="" 면 transition 으로 잡혀
            #    _vi_exp_sub_restore + [VI해제] 텔레그램이 잘못 나가므로 미리 "0" 캐싱
            for _mkt, _kc in _MKT_KEEPALIVE_INITIAL.items():
                _code_market_map.setdefault(_kc, _mkt)
                _mkt_keepalive_current[_mkt] = _kc
                _vi_cls_cache.setdefault(_kc, "0")
                _last_mkop_event.setdefault(_kc, "")

            # ── 슬롯 상한 준수: H0STCNI0(1) + H0STMKO0(N) + 종목데이터(M) <= 40 ──
            ccnl_notice_slots = 1  # H0STCNI0 이미 구독됨
            mkstatus_slots = len(_mkstatus_sub_codes)  # H0STMKO0 (keepalive + 보유/VI)
            desired_map = _desired_subscription_map(datetime.now(KST))
            data_slots = sum(len(v) for v in desired_map.values())
            total_sub = ccnl_notice_slots + mkstatus_slots + data_slots

            if total_sub > MAX_WSS_SUBSCRIBE:
                max_data = MAX_WSS_SUBSCRIBE - ccnl_notice_slots - mkstatus_slots
                logger.warning(
                    f"{ts_prefix()} [ws] 구독 상한 초과: {total_sub} > {MAX_WSS_SUBSCRIBE}"
                    f" → 종목데이터 {data_slots} → {max_data}로 축소"
                )
                over = data_slots - max_data
                for req in list(desired_map.keys()):
                    if over <= 0:
                        break
                    cs = desired_map[req]
                    if len(cs) > over:
                        removable = cs - _base_codes
                        remove_n = min(len(removable), over)
                        for c in list(removable)[:remove_n]:
                            cs.discard(c)
                            over -= 1
                    elif len(cs) <= over:
                        over -= len(cs)
                        desired_map[req] = set()
                data_slots = sum(len(v) for v in desired_map.values())
                total_sub = ccnl_notice_slots + mkstatus_slots + data_slots

            for req, code_set in desired_map.items():
                if code_set:
                    ka.KISWebSocket.subscribe(req, list(code_set))
                _subscribed[req.__name__] = set(code_set)

            # ── H0STMKO0 (재)구독: 부팅·재연결마다 open_map 에 실어 복원 보장 ──
            if _mkstatus_sub_codes:
                ka.KISWebSocket.subscribe(market_status_krx, list(_mkstatus_sub_codes))  # noqa: F405
                _subscribed["market_status_krx"] = set(_mkstatus_sub_codes)

            sub_desc = ", ".join(f"{r.__name__}={len(c)}종목" for r, c in desired_map.items() if c)
            sub_desc += f", H0STCNI0=1, H0STMKO0={mkstatus_slots}"
            logger.info(
                f"{ts_prefix()} [ws] open_map prepared: {sub_desc} total={total_sub}/{MAX_WSS_SUBSCRIBE}"
            )
            # 재연결 도중 쌓인 이벤트 클리어
            # (open_map에 최신 codes가 이미 반영되었으므로 이전 이벤트는 무효)
            _ws_rebuild_event.clear()

            # [260513] max_retries=10 → 1 축소.
            # SDK __runner 의 while self.retry_count < self.max_retries (kis_auth_llm_v0415.py:827)
            # 은 connection exception 발생 시 1초 sleep 후 같은 approval_key 로 재연결을 반복함.
            # 1007 frame error 후 이 루프가 10번 폭주(=10초 안에 8~10회) → KIS throttle(invalid
            # approval storm) → 슬롯 잠금. retry_count 는 인스턴스 수명 동안 리셋되지 않으므로
            # max_retries=1 이면 SDK 내부 재시도 없이 즉시 kws.start() 반환 → outer run_ws_forever
            # 가 새 approval_key 재발급 후 재연결(이 outer loop 는 backoff + auth_ws 재호출 보유).
            kws = ka.KISWebSocket(api_url="", max_retries=1)
            with _kws_lock:
                _active_kws = kws

            def _on_system(rsp):
                global _main_last_already_in_use_ts, _main_last_invalid_approval_ts
                if rsp.isOk and rsp.tr_msg and "SUCCESS" in rsp.tr_msg:
                    if getattr(rsp, "tr_id", "") == "H0STCNI0" and attempt == 2:
                        _notify(f"{ts_prefix()} [체결통보] 재접속 완료", tele=True)
                    return
                if rsp.tr_msg:
                    msg = str(rsp.tr_msg)
                    # 구독 전환 시 서버 타이밍에 따른 무해한 메시지는 DEBUG로
                    if "not found" in msg.lower() or "ALREADY IN SUBSCRIBE" in msg:
                        logger.debug(
                            f"{ts_prefix()} [ws][system] tr_id={rsp.tr_id} tr_key={rsp.tr_key} msg={msg}"
                        )
                    elif "htsid" in msg.lower():
                        cur_htsid = _get_ccnl_notice_tr_key()
                        warn_msg = (
                            f"{ts_prefix()} [ws][system] htsid 에러! "
                            f"현재설정={cur_htsid!r}, "
                            f"tr_id={rsp.tr_id}, tr_key={rsp.tr_key}, msg={msg}"
                        )
                        logger.warning(warn_msg)
                        _notify(warn_msg, tele=True)
                    else:
                        logger.warning(
                            f"{ts_prefix()} [ws][system] tr_id={rsp.tr_id} tr_key={rsp.tr_key} msg={msg}"
                        )
                        # [260428] ALREADY IN USE 감지 → 장기 backoff 트리거 (a2 와 동일 패턴)
                        if "ALREADY IN USE" in msg:
                            _main_last_already_in_use_ts = time.time()
                        # [260513] invalid approval 감지 → 60s backoff 트리거.
                        # 1007 frame error 직후 KIS 가 동일 appkey 에 같은 approval_key 를
                        # 짧은 시간 내 반복 발급(throttle)하므로, 충분히 대기 후 재발급해야
                        # 새 approval_key 가 나옴. 스팸 방지: 직전 30초 내 알림은 생략.
                        elif "invalid approval" in msg.lower():
                            _now_ts = time.time()
                            _spam_gap = _now_ts - _main_last_invalid_approval_ts
                            _main_last_invalid_approval_ts = _now_ts
                            if _spam_gap > 30.0:
                                _notify(
                                    f"{ts_prefix()} [ws] ★ invalid approval 감지 "
                                    f"(tr_id={rsp.tr_id}) → "
                                    f"{MAIN_WSS_BACKOFF_INVALID_APPROVAL_SEC:.0f}s backoff 후 "
                                    f"approval_key 재발급 예정",
                                    tele=True,
                                )
                            # [260513] SDK 내부 reconnect 루프 즉시 중단.
                            # max_retries=1 로 줄였더라도, 한 번이라도 invalid approval 응답을
                            # 받는 순간 같은 approval_key 로의 추가 시도는 KIS throttle 을 악화시킴.
                            # kws.close() → _close_requested=True + ws.close() → __runner while
                            # 루프 종료 → kws.start() 반환 → outer run_ws_forever 의 backoff 진입.
                            try:
                                kws.close()
                                logger.warning(
                                    f"{ts_prefix()} [ws] invalid approval → kws.close() 호출 "
                                    f"(SDK 내부 reconnect 루프 즉시 중단)"
                                )
                            except Exception as _e_close:
                                logger.warning(
                                    f"{ts_prefix()} [ws] invalid approval kws.close 실패: {_e_close}"
                                )

            kws.on_system = _on_system

            # 시간 플래그 즉시 1회 갱신
            update_time_flags()

            logger.info(f"{ts_prefix()} [ws] start on_result (parquet={FINAL_PARQUET_PATH.name})")
            _kws_start_ts = time.time()
            kws.start(on_result=on_result)

            _kws_dwell = time.time() - _kws_start_ts
            logger.info(f"{ts_prefix()} [ws] kws.start returned (mode={mode.name}, dwell={_kws_dwell:.1f}s)")
            # [260506] dwell 이 짧으면 핸드셰이크 실패가 다발한 상태로 추정 → long backoff
            # 실제 원인은 보통 직전 세션의 좀비 reconnect 잔재(같은 appkey 재사용) 또는
            # 자기측 연결 lifecycle 미정리. 서버 거부로 단정하지 말 것.
            # [260513] max_retries=1 로 축소: SDK 내부 재시도 없이 즉시 반환 → outer loop 가
            # 새 approval_key 재발급 후 재연결. 핸드셰이크 실패 다발 패턴은 outer dwell<15s 로 측정.
            if _kws_dwell < 15.0:
                # [260601][C] dwell<15s = 무효 approval_key(invalid approval) 의심 → 다음
                # 재접속에서 approval_key 강제 재발급(force_new). 260601 재현: 무효 캐시 키를
                # 계속 재사용하면 invalid approval→bare 1006 영구 반복 → 이 플래그로 self-heal.
                run_ws_forever._force_new_approval = True
                if (time.time() - _main_last_handshake_fail_ts) >= 30.0:
                    _main_last_handshake_fail_ts = time.time()
                    logger.warning(
                        f"{ts_prefix()} [ws] kws.start dwell={_kws_dwell:.2f}s < 15s "
                        f"→ 핸드셰이크 실패 다발 추정 (invalid approval_key 의심 → 다음 재접속 "
                        f"강제 재발급, 좀비 reconnect 잔재 또는 자기측 lifecycle 미정리 가능성)"
                    )
                # [260506] 자기보호 자동중단 카운터 — 연속 N회 핸드셰이크 실패 시 reconnect 중단
                run_ws_forever._handshake_fail_count = (
                    getattr(run_ws_forever, "_handshake_fail_count", 0) + 1
                )
                _hs_n = run_ws_forever._handshake_fail_count
                logger.warning(
                    f"{ts_prefix()} [ws] handshake_fail_count={_hs_n}/"
                    f"{HANDSHAKE_FAIL_SELF_STOP_LIMIT} (dwell={_kws_dwell:.2f}s)"
                )
                if _hs_n >= HANDSHAKE_FAIL_SELF_STOP_LIMIT:
                    _wss_self_stop(
                        reason=f"dwell<15s 연속 (마지막 dwell={_kws_dwell:.2f}s)",
                        count=_hs_n,
                    )
            else:
                # 정상 connect — 카운터 리셋
                if getattr(run_ws_forever, "_handshake_fail_count", 0) > 0:
                    logger.info(
                        f"{ts_prefix()} [ws] handshake_fail_count 리셋 "
                        f"(이전={run_ws_forever._handshake_fail_count}, dwell={_kws_dwell:.1f}s)"
                    )
                    run_ws_forever._handshake_fail_count = 0

        except SystemExit:
            break
        except Exception as e:
            logger.error(f"{ts_prefix()} [ws] connection exception: {e}")
            logger.error(traceback.format_exc())
            # [260506] kws.start 자체가 예외로 끝난 경우 — 핸드셰이크 단계 실패 패턴이면 카운터 증가
            _exn = type(e).__name__
            _emsg = str(e)
            _is_handshake_err = (
                _exn in ("InvalidMessage", "ConnectionResetError", "TimeoutError", "OSError")
                or "did not receive a valid HTTP response" in _emsg
                or "during opening handshake" in _emsg
                or "Connection reset" in _emsg
            )
            if _is_handshake_err:
                run_ws_forever._handshake_fail_count = (
                    getattr(run_ws_forever, "_handshake_fail_count", 0) + 1
                )
                _hs_n = run_ws_forever._handshake_fail_count
                logger.warning(
                    f"{ts_prefix()} [ws] handshake_fail_count={_hs_n}/"
                    f"{HANDSHAKE_FAIL_SELF_STOP_LIMIT} (예외={_exn})"
                )
                if _hs_n >= HANDSHAKE_FAIL_SELF_STOP_LIMIT:
                    _wss_self_stop(reason=f"{_exn}: {_emsg[:120]}", count=_hs_n)
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
            # [260427] wait 진입 직후 강제: 종가매수 결과 통보 + 잔고검증 (체결통보 누락 보완)
            try:
                _run_closing_buy_filled_notify()
            except Exception as _ex:
                logger.warning(f"{ts_prefix()} [wait_init] closing 결과 통보 실패: {_ex}")
            try:
                global _closing_balance_1531_done
                if not _closing_balance_1531_done:
                    _closing_balance_1531_done = True
                    _run_closing_balance_verification("15:31_종가체결")
                else:
                    # 이미 실행됐어도 wait 진입 시점에 한 번 더 (강제 재검증)
                    _run_closing_balance_verification("15:31_종가체결_재검증")
            except Exception as _ex:
                logger.warning(f"{ts_prefix()} [wait_init] 잔고검증 실패: {_ex}")
            # 15:40 시간외 종가 매수까지 대기 알림 (텔레그램)
            try:
                _notify(
                    f"{ts_prefix()} [wait] 종가매수 정리 완료 → 15:40 시간외 종가 매수까지 대기",
                    tele=True,
                )
            except Exception:
                pass
            # [260427] idle 구간도 1분 주기로 heartbeat 로그 (멎었는지 알 수 있도록)
            _last_hb = time.time()
            while not _stop_event.is_set():
                _now = datetime.now(KST)
                if _now.time() >= dtime(16, 0):
                    break
                if time.time() - _last_hb >= 60.0:
                    _remain_sec = (dtime(16, 0).hour * 3600 + dtime(16, 0).minute * 60) \
                                  - (_now.hour * 3600 + _now.minute * 60 + _now.second)
                    _remain_min = max(0, _remain_sec) // 60
                    logger.info(
                        f"{ts_prefix()} [ws] idle 중 — 16:00 시간외까지 {_remain_min}분 남음"
                    )
                    _last_hb = time.time()
                time.sleep(1.0)
            logger.info(f"{ts_prefix()} [ws] 16:00 도달 → 시간외 단일가 연결 시작")
        else:
            # [260506] 직전 30초 내 핸드셰이크 실패 다발 또는 ALREADY IN USE 감지 시
            # → long backoff (자기측 잔재 정리 + 동일 appkey 재시도 storm 차단)
            elapsed_fail = time.time() - _main_last_handshake_fail_ts
            elapsed_already = time.time() - _main_last_already_in_use_ts
            if elapsed_already < 30.0 or elapsed_fail < 30.0:
                if elapsed_already < 30.0:
                    # ALREADY IN USE: KIS 측 stale session 정리에 실제 시간 필요 → 단축 안 함
                    wait = MAIN_WSS_BACKOFF_ALREADY_IN_USE_SEC
                    cause = "ALREADY IN USE 감지"
                else:
                    # 핸드셰이크 실패 다발(dwell<15s): 개장 직전/직후면 시초 손실 방지 위해 단축
                    cause = "핸드셰이크 실패 다발 (dwell<15s)"
                    _now_open = datetime.now(KST).time()
                    if dtime(8, 55) <= _now_open < dtime(9, 2):
                        wait = MAIN_WSS_BACKOFF_NEAR_OPEN_SEC
                        cause += " · near-open 단축"
                    else:
                        wait = MAIN_WSS_BACKOFF_ALREADY_IN_USE_SEC
                logger.warning(
                    f"{ts_prefix()} [ws] {cause} → "
                    f"long backoff {wait:.0f}s (storm 차단)"
                )
            else:
                wait = backoff
                logger.info(f"{ts_prefix()} [ws] reconnect in {wait}s...")
            # 단계적 sleep (stop event 빠른 응답)
            for _ in range(int(wait * 10)):
                if _stop_event.is_set():
                    break
                time.sleep(0.1)

    logger.info(f"{ts_prefix()} [ws] stopped")

# =============================================================================
# main
# =============================================================================
if __name__ == "__main__":
    # [260427] 기동 시 코드 버전 표시 — 옛 코드로 실수 운영 방지
    _CODE_VERSION = _get_code_version()
    if is_holiday():
        msg = f"{ts_prefix()} {PROGRAM_NAME} [v={_CODE_VERSION}] => 오늘은 휴일이므로 프로그램을 종료합니다."
        _notify(msg, tele=True)
        # [260506] 휴일 즉시 종료도 watchdog 협업용 status=2 기록 (실행 흔적 남김)
        _set_runtime_status(RUNTIME_STATUS_STOPPED)
        sys.exit(0)
    else:
        msg = f"{ts_prefix()} {PROGRAM_NAME} [v={_CODE_VERSION}] => 오늘은 개장일이므로 프로그램을 시작합니다."
        _notify(msg, tele=True)
        # [260506] 영업일 시작 — watchdog 가 살아있는 프로세스로 인식하도록 status=1 기록
        _set_runtime_status(RUNTIME_STATUS_RUNNING)
    logger.info(f"[save_mode] {SAVE_MODE}")
    logger.info(f"[part] flush {PART_FLUSH_THRESHOLD}행 시 {PART_FLUSH_SAVE}행 저장/{PART_FLUSH_SEC}s -> parts/ -> 18:00 merge")
    logger.info(f"[monitor] summary_every={SUMMARY_EVERY_SEC}s stale={STALE_SEC}s")
    logger.info(f"[parquet] -> {FINAL_PARQUET_PATH}")

    if datetime.now(KST).time() >= END_TIME:
        prefix = ts_prefix()
        sys.stdout.write(f"{prefix} 종료 시각 경과 → 저장된 스냅샷 확인 후 종료\n")
        sys.stdout.flush()
        logger.info(f"{prefix} 종료 시각 경과 → 스냅샷={FINAL_PARQUET_PATH}")
        _notify(f"{prefix} {PROGRAM_NAME} 종료 시각 경과로 종료", tele=True)
        _set_runtime_status(RUNTIME_STATUS_STOPPED)
        raise SystemExit(0)

    # ── KRX_code로 57/59(30분 단일가) 종목 사전 초기화 ──────────
    _check_iscd_stat_krx(list(codes))

    # ── 시작 즉시 계좌 잔고조회 → 보유종목 출력 + balance_map 취득 ──────────
    _startup_balance_map = _print_startup_balance()

    # 재시작 시 미체결 매수주문 전부 취소 (OPEN_BUY_ORDER_CANCEL=True 시, 16:00~18:00 제외)
    _run_open_buy_order_cancel_on_startup()

    # 16:00 이후 재시작: 시간외 단일가 주문은 서버에 유지되므로 중복 주문 방지
    if datetime.now(KST).time() >= dtime(16, 0):
        _overtime_order_done = True  # noqa: F841 — 모듈 레벨 변수 직접 갱신
        logger.info(f"{ts_prefix()} [시간외단일가] 16:00 이후 재시작 → 기존 주문 유지, 중복 주문 방지")

    # 15:20~15:30 재시작 시 미처리 종가매수 주문 재시도 (state 저장 기반)
    _run_closing_buy_retry_on_startup()

    # 15:20~18:00 재시작 시 KIS 서버에서 미체결 매수주문 조회 → codes/체결통보/tracking 복원
    _restore_orders_from_server_on_startup()

    # Str1 매도 상태 복원 — 시작잔고 결과 전달 (중복 API 호출 없이 재사용)
    _load_str1_sell_state_on_startup(_startup_balance_map)

    # [260523] str2 게이트 갱신 + 활성 시 상태 복원 (재시작 연속성)
    try:
        _refresh_str2_enabled()
        if STR2_ENABLED:
            _restore_str2_state_on_startup()
    except Exception as _e:
        logger.warning(f"{ts_prefix()} [str2] 시작 복원 실패: {_e}")

    # [v_1] uplimit 전략 상태 복원 + 사전 캐시 구축
    if UPLIMIT_BUY_ENABLED:
        try:
            _restore_uplimit_state_on_startup()
        except Exception as _e:
            logger.warning(f"{ts_prefix()} [uplimit] 시작 복원 실패: {_e}")
        try:
            _precache_uplimit_signals()
        except Exception as _e:
            logger.warning(f"{ts_prefix()} [uplimit] 사전 캐시 실패: {_e}")
        # [260423] 시드 베이스 계산: INIT_CASH / UPLIMIT_DIVERSIFY_N
        # (module-level — global 선언 불필요, 직접 할당)
        try:
            _accts = _iter_enabled_accounts(trade_only=True)
            if _accts:
                _acct = _accts[0]
                _client = _init_account_client(_acct)
                _init_cash = _get_account_available_cash(_client, _acct)
                _uplimit_seed_base = _init_cash / UPLIMIT_DIVERSIFY_N
                logger.info(
                    f"{ts_prefix()} [uplimit_seed] INIT_CASH={int(_init_cash):,}원, "
                    f"DIVERSIFY_N={UPLIMIT_DIVERSIFY_N}, seed_base={int(_uplimit_seed_base):,}원/종목"
                )
            else:
                logger.warning(f"{ts_prefix()} [uplimit_seed] 활성 계좌 없음 → seed_base=0 (매수 불가)")
        except Exception as _e:
            logger.warning(f"{ts_prefix()} [uplimit_seed] 초기화 실패: {_e}")

    # 매도 금지 종목 로드
    _load_no_sell_codes()

    # 당일 parts/FINAL → _ema_state, _price_buf 지표 복원 (재시작 연속성)
    _restore_state_on_startup()

    # 시작 시 base_codes를 월별 구독 이력 CSV에 기록
    _log_base_codes_on_startup()

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

    # [260526] WSS 무수신 watchdog 독립 데몬 (scheduler 정지와 무관하게 os._exit(2) 보장)
    t_wss_wd = threading.Thread(target=_wss_norecv_watchdog_loop, name="wss-norecv-watchdog", daemon=True)
    t_wss_wd.start()

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
        _shutdown("finally")  # _stop_event.is_set()면 즉시 return (중복 호출 안전)
        # [260423] thread join 대폭 단축: 전부 daemon=True 라 main 종료 시 자동 사멸.
        # _shutdown() 에서 이미 큐 소진 + flush + snapshot 완료됨 → 길게 기다릴 필요 없음.
        _t_join_start = time.time()
        try:
            t_ingest.join(timeout=1.0)
            t_writer.join(timeout=1.0)
            t_str1_sell.join(timeout=0.5)
            t_flags.join(timeout=0.3)
            t_sched.join(timeout=0.3)
            t_rest.join(timeout=0.3)
            t_price_watch.join(timeout=0.3)
            t_price_req.join(timeout=0.3)
            t_top.join(timeout=0.3)
        except Exception:
            pass
        logger.info(f"[finally] thread join 완료 (+{time.time()-_t_join_start:.2f}s)")
        # [260423] 중복 flush 제거 — _shutdown() 에서 이미 force_full=True 로 저장 완료됨.
        # 혹시 남은 데이터가 있을 경우 대비 조건부 flush (남은 버퍼 있을 때만, 로그만)
        try:
            with _part_buffer_lock:
                has_data = bool(_part_buffer)
            if has_data:
                # shutdown 이후 추가 도착 데이터 — 빠르게 저장
                _flush_part_buffer("final_shutdown", force_full=True)
        except Exception as e:
            logger.error(f"[finally] 최종 flush 실패: {e}")
        _flush_overwrite_log()
        # 연도별 거래장부 parquet → CSV 변환 저장
        try:
            year = datetime.now(KST).strftime("%Y")
            _yearly_pq = LEDGER_DIR / f"trade_ledger_{year}.parquet"
            if _yearly_pq.exists():
                pd.read_parquet(_yearly_pq).to_csv(
                    _yearly_pq.with_suffix(".csv"), index=False, encoding="utf-8-sig"
                )
                logger.info(f"[ledger] CSV 변환 완료: {_yearly_pq.with_suffix('.csv')}")
        except Exception as e:
            logger.error(f"[ledger] CSV 변환 실패: {e}")
        # [260427] telegram 비동기화 — Telegram API 응답 지연이 shutdown 흐름을 막지 않음
        try:
            threading.Thread(
                target=_notify,
                args=(f"{ts_prefix()} {PROGRAM_NAME} 종료",),
                kwargs={"tele": True},
                daemon=True,
            ).start()
        except Exception:
            pass
        logger.info("=== WSS END ===")
