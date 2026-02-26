import argparse, json, os, signal, sys, threading, time, select
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
import pandas as pd
import requests

from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig
from kis_utils import LogManager, TeeStdout, calc_limit_up_price, load_config, load_kis_data_layout, print_table, resolve_account_config, save_config, symbol_master_path, stQueryDB

# 시스템 시간대를 KST로 고정
os.environ["TZ"] = "Asia/Seoul"
time.tzset()

"""
  =============================================================================
<<<  실행 옵션 >>>
  1: 계좌 잔고 조회(10초 간격 재 조회)
  2: open id 조회/정리
  23: 쓰레드 보유종목 조회(1초 간격)
  21: 주문내역조회(inquire-order) - 주문/체결 내역 표시
  22: open_order 조회(미체결 주문 정보 조회)
  3: 종목 선정
  4: Top_Trading + 모니터링(순차)
  5: 웹소켓 테스트(전략 시뮬레이션)
  6: Top_Trading + WebSocket
============================================================================="""
RUN_MODE = 1
CANO_KEY = "cano"  # config.json의 계좌키 (예: cano)

DISTRIB_BUY = False         # 실행초기 분산매수 실행 여부, True : 분산매수, False : 분산매수 미실행
AFTER_HOURS_TEST = True  # True면 장 마감 후에도 모드4 테스트 실행
Closing_Sell = True        # 종가 매도 실행 여부, True : 종가 매도, False : 종가 매도 미실행
Closing_Buy = True         # 종가 매수 실행 여부, True : 종가 매수, False : 종가 매수 미실행

# 테스트용: 스레드 동작만 보기 위해 09:00 대기/초기 매수 로직 스킵 (기본 False)
THREAD_TEST_MODE = True  # True면 초기 매수 로직을 건너뛰고 스레드 동작만 확인
MODE3_THREAD_TEST = False  # 모드3에 추가로 쓰레드 기능을 추가하여 쓰레드 정상 작동여부 테스트하는 기능

# WebSocket 시뮬레이션 옵션 (mode5)
TRAIL_STEP = 0.05
TRAIL_STEPS = [0.02, 0.03, 0.04, 0.05, 0.60, 0.10, 0.15, 0.20, 0.25, 0.29]
STOP_LOSS = 0.0
K = 0.3
SLIPPAGE = 0.005
CODE_TEXT = """
삼성전자
젬백스
비보존 제약
메지온
쎄노텍
지니틱스
아이티아이즈
유디엠텍
리브스메드
"""

# Top_Trading 옵션
TOP_TRADING_RET = 0.25  # 종목선정 조건 : 상승률 >= TOP_TRADING_RET, < 0.33, 전일종가 또는 현재종가 >= 500원, 보유종목 제외.
SELL_HOLD_RET = 0.25    # 종가 수익률 25% 이상이면 보유, 미만이면 매도
MAX_SYMBOLS_SCAN = 0    # 종목 선정 한도 수량 지정, 0이면 전체 스캔, 20 : 20개만 스캔
ENABLE_ADD_BUY_30M = False  # 30분 단위 추가매수 비활성화
ENABLE_TRADE_LEDGER = True  # 거래장부 기록 사용 여부 (mode4에서 적용)
TRADE_LEDGER_PATH = "/home/ubuntu/Stoc_Kis/data/trades/trades_all.parquet"
ORD_DVSN_IOC_BEST = "15"  # IOC 최유리 (즉시체결, 잔량취소)
ORD_DVSN_IOC_MKT = "14"  # IOC 시장가 (즉시체결, 잔량취소)
CURRENT_RUN_MODE: Optional[int] = None
CURRENT_TR_ID_DAILY: Optional[str] = None
CURRENT_TR_ID_CANCEL: Optional[str] = None
CURRENT_TR_ID_BALANCE: Optional[str] = None
CURRENT_TR_ID_OPEN: Optional[str] = None
CURRENT_TR_ID_ORDER: Optional[str] = None
ORDER_RESP_BY_CODE: Dict[str, Dict[str, Any]] = {}

# 모니터링 상태 추적용
MONITOR_THREAD_ID: Optional[int] = None

# buy 옵션(위에서 buy 옵션을 선택시 실행할 옵션을 지정)
BUY_SYMBOL = ""
BUY_QTY = 0
BUY_PRICE = "m"  # 숫자 또는 "m"(시장가)

# 휴일 수기 관리 (YYYY-MM-DD)
HOLIDAYS = {
    # "2026-01-01",
}

# 웹소켓/모니터링 옵션
WS_SYMBOL = "005930"
MONITOR_INTERVAL_SEC = 1.0
MONITOR_POLL_SEC = 15.0
INQUIRE_RETRY_MAX = 3
INQUIRE_RETRY_DELAY = 0.6
WS_URL = "wss://openapi.koreainvestment.com:9443/websocket"
WS_TR_ID = "H0STCNT0"
WS_PRICE_IDX = 2  # 실시간 체결 필드 index (기본값)
WS_VOLUME_IDX = 7
WS_TIME_IDX = 1

# 분산매수 기록 파일 (JSON)
DISPERSE_RECORD_PATH = "top_trading_disperse_dates.json"

_LOG_MANAGER: LogManager | None = None
_TRADE_LOG_PATH: str | None = None
_BUY_TODAY: dict[str, dict[str, Any]] = {}

""" 
=============================================================================
<<< 4/6 모드 운용 매뉴얼 (08:00 실행 기준) >>>
- 08:00 실행 시점: KST 기준, 거래일이 아니면 종료.
- 08:55 사전 계좌조회:
  - 보유종목 스냅샷 저장(코드/수량).
  - D+2 주문가능금액(prvs_rcdl_excc_amt)에서 exclude_cash 차감.
- 09:00~09:04 갭하락 매도:
  - 09:00~09:04 사이 첫 거래가 발생한 경우만 시가로 간주.
  - 첫 거래가 없으면(0값) 갭 판단 제외.
  - 시가 < 전일종가면 갭하락으로 시장가 매도.
  - 그 외는(갭하락 아님) 이후 트레일링 모니터링/매도 로직 수행.
  - 1분 대기 후 잔고 재조회(D+2 cash 재확인).
- 09:05 상승률 상위 50 매수:
  - 등락률 순위 API(FHPST01700000) 기반.
  - 조건: 상승률 >= TOP_TRADING_RET, < 0.33, 전일종가 또는 현재종가 >= 500원,
          보유종목 제외.
  - 분산매수 로직(종목별 최대한도 적용)으로 시장가 매수.
  - 이 단계에서는 거래대금(value) 필터를 적용하지 않음.
- 09:05 이후 즉시 분산매수 (DISTRIB_BUY=True):
  - 종목선정 기준: 전일 대비 금일(Parquet_DB 마지막 수신일의 전일 대비, mode=1).
  - 필터: 현재가>=1000원, 상승률<0.33, 거래대금>=10억.
  - 분산매수 일자 기록(당일 1회).
  - DISTRIB_BUY=False면 대기 모드 진입.
- 14:50 종가 매도:
  - Closing_Sell=True이면 수익률 < SELL_HOLD_RET 종목을 시장가 매도.
  - Closing_Sell=False이면 매도 스킵 후 재매수 단계로 진행.
- 14:50 이후 재매수:
  - 종목선정 기준: 전일 대비 금일(등락률 순위 API, mode=3).
  - 필터: 현재가>=1000원, 상승률<0.33, 거래대금>=10억.
  - 분산매수 후 다음 거래일로 분산매수 기록 저장.
- 15:00~15:30:
  - 상한가 미체결 주문 취소 후, 다음 후보로 재매수 시도.

<<< 종목선정(옵션 3) 기준 >>>
- 1번: 전일 대비 금일 (Parquet_DB 마지막 수신일의 전일 대비, tdy_ctrt)
  - 필터: 현재가>=1000원, 상승률<0.33, 거래대금>=10억
- 2번: 전전일 대비 금일 (Parquet_DB 마지막 수신일의 전전일 대비, pdy_ctrt)
  - 필터: 현재가>=1000원, 상승률<0.33, 거래대금>=10억
- 3번: 전일 대비 금일 (API 실시간, prdy_ctrt)
  - 필터: 현재가>=1000원, 상승률<0.33, 거래대금>=10억

※ 4/6 모드의 시간대별 종목선정은 위 기준을 시간대에 맞춰 적용:
  - 09:05 매수: 등락률 상위 API + 시가==저가 + 거래대금 필터 미적용
  - 오전 분산매수: parquet 기반 mode=1
  - 14:50 이후 재매수: 등락률 상위 API mode=3
 
mode2 → /inquire-psbl-rvsecncl 사용 (정정/취소 진행)
mode21 → /inquire-order (TR-ID: TTTC8001R)
mode22 → /inquire-psbl-rvsecncl (open order 조회만)
RUN_MODE 숫자 오류(2-1) → 21로 정상화

모드 요약:
- 4: Top_Trading + 모니터링(순차)
- 5: WebSocket OHLCV 출력(단독)
- 6: Top_Trading + WebSocket
"""


def _log(msg: str) -> None:
    if _LOG_MANAGER is None:
        print(msg)
        return
    _LOG_MANAGER.log(msg)


def _log_tm(msg: str) -> None:
    if _LOG_MANAGER is None:
        print(msg)
        return
    _LOG_MANAGER.log_tm(msg)


def _append_trade_log(
    action: str,
    code: str,
    name: str,
    qty: int,
    price: float,
    amount: float,
    pnl: float | None = None,
    ret: float | None = None,
    reason: str = "",
    ordno: str = "",
) -> None:
    if not _TRADE_LOG_PATH:
        return
    row = {
        "ts": _now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "action": action,
        "code": code,
        "name": name,
        "qty": qty,
        "price": f"{price:.0f}",
        "amount": f"{amount:.0f}",
        "pnl": "" if pnl is None else f"{pnl:.0f}",
        "ret": "" if ret is None else f"{ret:.4f}",
        "reason": reason,
        "ordno": ordno,
    }
    try:
        write_header = not os.path.exists(_TRADE_LOG_PATH)
        df = pd.DataFrame([row])
        df.to_csv(
            _TRADE_LOG_PATH,
            mode="a",
            header=write_header,
            index=False,
            encoding="utf-8-sig",
        )
    except Exception:
        pass


def _trade_ledger_enabled() -> bool:
    return ENABLE_TRADE_LEDGER and CURRENT_RUN_MODE == 4


def _append_trade_ledger_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    try:
        out_dir = os.path.dirname(TRADE_LEDGER_PATH)
        os.makedirs(out_dir, exist_ok=True)
        df_new = pd.DataFrame(rows)
        if os.path.exists(TRADE_LEDGER_PATH):
            try:
                df_old = pd.read_parquet(TRADE_LEDGER_PATH)
                df_all = pd.concat([df_old, df_new], ignore_index=True)
            except Exception:
                df_all = df_new
        else:
            df_all = df_new
        df_all.to_parquet(TRADE_LEDGER_PATH, index=False)
    except Exception as e:
        _log(f"[거래장부] 저장 실패: {e}")


def _record_trade_ledger(stage: str, **fields: Any) -> None:
    if not _trade_ledger_enabled():
        return
    row = {
        "ts": _now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "stage": stage,
    }
    row.update(fields)
    _append_trade_ledger_rows([row])


def _query_date_for_ccld() -> str:
    now = _now_kst()
    if now.hour < 9:
        return (now - timedelta(days=1)).strftime("%Y%m%d")
    return now.strftime("%Y%m%d")


def _find_ccld_by_ordno(
    client: KisClient, cano: str, acnt_prdt_cd: str, ord_no: str
) -> Optional[Dict[str, Any]]:
    if not CURRENT_TR_ID_DAILY:
        return None
    query_date = _query_date_for_ccld()
    try:
        orders, _ = _inquire_daily_ccld(
            client, cano, acnt_prdt_cd, CURRENT_TR_ID_DAILY, query_date, query_date, ord_no=ord_no
        )
    except Exception as e:
        _log(f"[거래장부] 체결조회 실패 ordno={ord_no} error={e}")
        return None
    for o in orders:
        odno = _to_str(_pick(o, "ordno", "odno", "ord_no", default=""))
        if odno == ord_no:
            return o
    return None


def _get_holding_qty(
    client: KisClient, cano: str, acnt_prdt_cd: str, tr_id_balance: str, code: str
) -> int:
    try:
        holdings, _ = _collect_holdings_snapshot(
            client,
            cano,
            acnt_prdt_cd,
            tr_id_balance,
            max_pages=1,
            page_sleep_sec=0.0,
            timeout_sec=2.0,
        )
    except Exception:
        return 0
    for h in holdings:
        if _to_str(h.get("code", "")) == code:
            return _to_int(h.get("qty", 0))
    return 0


def _get_orderable_cash(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_balance: str,
    exclude_cash: float,
) -> float:
    try:
        rows, summary, _, _ = _get_balance_page(client, cano, acnt_prdt_cd, tr_id_balance)
    except Exception:
        return 0.0
    summary_rows = summary if isinstance(summary, list) else [summary]
    if not summary_rows:
        return 0.0
    item = summary_rows[0]
    ord_cash = _to_float(_pick(item, "ord_psbl_cash", "prvs_rcdl_excc_amt", default=0))
    return max(0.0, ord_cash - exclude_cash)


def _schedule_trade_fill_check(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_balance: str,
    ord_no: str,
    code: str,
    side: str,
    req_qty: int,
    req_price: float,
    pre_qty: int,
) -> None:
    if not _trade_ledger_enabled() or not ord_no:
        return

    def _attempt_check(delay_sec: float) -> Optional[Dict[str, Any]]:
        time.sleep(delay_sec)
        return _find_ccld_by_ordno(client, cano, acnt_prdt_cd, ord_no)

    def _record_fill(ccld: Dict[str, Any]) -> None:
        fill_qty = _to_int(_pick(ccld, "ccld_qty", "tot_ccld_qty", "stck_cnt", default=0))
        fill_price = _to_float(_pick(ccld, "ccld_unpr", "stck_prpr", "ccld_prc", default=0))
        fill_amt = _to_float(_pick(ccld, "tot_ccld_amt", "ccld_amt", default=fill_qty * fill_price))
        post_qty = _get_holding_qty(client, cano, acnt_prdt_cd, tr_id_balance, code)
        _log(
            f"[체결확인] {side} {code} ordno={ord_no} qty={fill_qty} price={fill_price:,.0f}"
        )
        _record_trade_ledger(
            "fill",
            side=side,
            code=code,
            ord_no=ord_no,
            fill_qty=fill_qty,
            fill_price=fill_price,
            fill_amt=fill_amt,
            pre_qty=pre_qty,
            post_qty=post_qty,
        )

    def _worker() -> None:
        ccld = _attempt_check(0.5)
        if ccld:
            _record_fill(ccld)
            return
        ccld = _attempt_check(0.7)
        if ccld:
            _record_fill(ccld)
            return
        time.sleep(300)
        ccld = _find_ccld_by_ordno(client, cano, acnt_prdt_cd, ord_no)
        if ccld:
            _record_fill(ccld)
            return
        _log(f"[체결미확인] {side} {code} ordno={ord_no} 5분 경과")
        if CURRENT_TR_ID_CANCEL and CURRENT_TR_ID_OPEN:
            try:
                orders = _inquire_open_orders(client, cano, acnt_prdt_cd, CURRENT_TR_ID_OPEN)
            except Exception:
                orders = []
            for o in orders:
                odno = _to_str(_pick(o, "odno", "ordno", default=""))
                if odno == ord_no:
                    qty = _to_int(_pick(o, "ord_qty", default=req_qty))
                    code_o = _to_str(_pick(o, "pdno", default=code))
                    try:
                        _log(f"[미체결취소시도] {side} {code_o} ordno={ord_no} qty={qty}")
                        _cancel_order(client, cano, acnt_prdt_cd, CURRENT_TR_ID_CANCEL, ord_no, code_o, qty)
                        _record_trade_ledger(
                            "cancel",
                            side=side,
                            code=code_o,
                            ord_no=ord_no,
                            cancel_qty=qty,
                        )
                    except Exception as e:
                        _log(f"[거래장부] 취소 실패 ordno={ord_no} error={e}")
                    break

    th = threading.Thread(target=_worker, daemon=True)
    th.start()

def _record_buy_today(code: str, name: str, qty: int, price: float) -> None:
    if qty <= 0 or price <= 0:
        return
    info = _BUY_TODAY.get(code)
    if info:
        prev_qty = _to_int(info.get("qty", 0))
        prev_price = _to_float(info.get("buy_price", 0))
        new_qty = prev_qty + qty
        if new_qty > 0:
            avg_price = (prev_price * prev_qty + price * qty) / new_qty
        else:
            avg_price = price
        info["qty"] = new_qty
        info["buy_price"] = avg_price
        if name:
            info["name"] = name
    else:
        _BUY_TODAY[code] = {
            "name": name or "",
            "qty": qty,
            "buy_price": price,
            "ts": _now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        }


def _pick(row: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return default


def _to_int(val: Any) -> int:
    try:
        return int(float(val))
    except Exception:
        return 0


def _to_float(val: Any) -> float:
    try:
        if isinstance(val, str):
            val = val.replace(",", "")
        return float(val)
    except Exception:
        return 0.0


def _to_str(val: Any) -> str:
    try:
        if val is None:
            return ""
        return str(val).strip()
    except Exception:
        return ""


def _rate_to_ratio(val: Any) -> float:
    """
    등락률 값을 비율로 정규화
    - 29.5 => 0.295
    - 0.295 => 0.295
    """
    rate = _to_float(val)
    if abs(rate) > 1.5:
        return rate / 100.0
    return rate


def _round_to_tick(price: float) -> float:
    if price < 1000:
        tick = 1
    elif price < 5000:
        tick = 5
    elif price < 10000:
        tick = 10
    elif price < 50000:
        tick = 50
    elif price < 100000:
        tick = 100
    elif price < 500000:
        tick = 500
    else:
        tick = 1000
    return round(price / tick) * tick


def _max_buy_limit(cur_price: float, value: float) -> float:
    """
    종목별 1회 분산매수 최대 한도
    1) 현재가 기준: min(현재가 * 100000, 500,000,000)
    2) value 기준: max(50,000,000, value / 150)
    3) min(현재가 기준, value 기준)
    """
    price_limit = min(cur_price * 100000, 500000000)
    value_limit = max(50000000, value / 150.0)
    return min(price_limit, value_limit)


def _allocate_cash_by_max(total_cash: float, max_limits: List[float]) -> List[float]:
    """
    최대한도 기준 분산매수 금액 배분
    - 최대한도 오름차순으로 배분
    - max < 평균이면 max만 배정하고 잔여금액/잔여종목수로 평균 재계산
    """
    n = len(max_limits)
    if n == 0 or total_cash <= 0:
        return [0.0] * n
    indexed = list(enumerate(max_limits))
    indexed.sort(key=lambda x: x[1])
    remaining_cash = total_cash
    remaining_count = n
    alloc = [0.0] * n
    for idx, max_limit in indexed:
        if remaining_count <= 0:
            break
        avg = remaining_cash / remaining_count if remaining_count > 0 else 0.0
        use = max_limit if max_limit < avg else avg
        use = max(0.0, min(use, remaining_cash))
        alloc[idx] = use
        remaining_cash -= use
        remaining_count -= 1
    return alloc


def _print_table(rows: List[Dict[str, Any]], columns: List[str], align: Dict[str, str]) -> None:
    print_table(rows, columns, align)


def _now_kst() -> datetime:
    return datetime.now(timezone(timedelta(hours=9)))


def _is_trading_day(dt: datetime) -> bool:
    kst_dt = dt.astimezone(timezone(timedelta(hours=9)))
    if kst_dt.weekday() >= 5:
        return False
    return kst_dt.strftime("%Y-%m-%d") not in HOLIDAYS


def _next_trading_day(dt: datetime) -> datetime:
    d = dt.astimezone(timezone(timedelta(hours=9)))
    while True:
        d = d + timedelta(days=1)
        if _is_trading_day(d):
            return d


def _wait_until(target: datetime) -> None:
    while True:
        now = _now_kst()
        if now >= target:
            return
        time.sleep(min(30, (target - now).total_seconds()))


def _load_disperse_record(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(str(x) for x in data)
    except Exception:
        return set()
    return set()


def _save_disperse_record(path: str, dates: set[str]) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sorted(dates), f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _hashkey(base_url: str, appkey: str, appsecret: str, body: Dict[str, Any]) -> str:
    url = f"{base_url}/uapi/hashkey"
    headers = {
        "content-type": "application/json",
        "appkey": appkey,
        "appsecret": appsecret,
    }
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    hk = j.get("HASH") or j.get("hash")
    if not hk:
        raise RuntimeError(f"hashkey 실패: {j}")
    return hk


def _issue_websocket_approval_key(appkey: str, appsecret: str, base_url: str) -> str:
    url = f"{base_url}/oauth2/Approval"
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": appkey, "secretkey": appsecret}
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    key = j.get("approval_key")
    if not key:
        raise RuntimeError(f"approval_key 발급 실패: {j}")
    return key


def _parse_ws_tick(message: str) -> tuple[str | None, float | None, float | None]:
    """
    KIS 실시간 메시지 기본 파싱 (|, ^ 구분자)
    반환: (time_str, price, volume)
    """
    try:
        if "|" not in message or "^" not in message:
            return None, None, None
        parts = message.split("|")
        if len(parts) < 4:
            return None, None, None
        body = parts[-1]
        fields = body.split("^")
        time_str = fields[WS_TIME_IDX] if WS_TIME_IDX < len(fields) else None
        price = float(fields[WS_PRICE_IDX]) if WS_PRICE_IDX < len(fields) else None
        volume = float(fields[WS_VOLUME_IDX]) if WS_VOLUME_IDX < len(fields) else None
        return time_str, price, volume
    except Exception:
        return None, None, None


def _run_ws_ohlcv(appkey: str, appsecret: str, base_url: str, custtype: str) -> None:
    print("[mode5] 웹소켓 테스트: 실시간체결가 기반 전략 시뮬레이션")
    try:
        import logging
        logging.getLogger().setLevel(logging.WARNING)
    except Exception:
        pass
    try:
        from pathlib import Path
        import kis_auth_llm as ka  # type: ignore
        OPEN_API_WS_DIR = Path.home() / "open-trading-api" / "examples_user" / "domestic_stock"
        sys.path.append(str(OPEN_API_WS_DIR))
        sys.modules["kis_auth"] = ka
        from domestic_stock_functions_ws import ccnl_total  # type: ignore
    except Exception as e:
        print(f"websocket 모듈 로드 실패: {e}")
        return

    svr = "prod" if "openapi.koreainvestment.com" in base_url else "vps"
    ka.auth(svr=svr)
    ka.auth_ws(svr=svr)
    if not getattr(ka, "_base_headers_ws", {}).get("approval_key"):
        print("auth_ws 실패: 승인키 확인 필요")
        return

    symbols_path = symbol_master_path(load_kis_data_layout())
    name_map: Dict[str, str] = {}
    name_to_code: Dict[str, str] = {}
    if os.path.exists(symbols_path):
        sdf = pd.read_parquet(symbols_path, columns=["code", "name"])
        name_map = dict(zip(sdf["code"].astype(str), sdf["name"].astype(str)))
        name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"].astype(str)))

    def _parse_codes(text: str) -> List[str]:
        codes: List[str] = []
        for line in text.splitlines():
            raw = line.strip()
            if not raw:
                continue
            if raw.isdigit():
                codes.append(raw.zfill(6))
                continue
            code = name_to_code.get(raw)
            if code:
                codes.append(code.zfill(6))
        return list(dict.fromkeys(codes))

    codes = _parse_codes(CODE_TEXT)
    if not codes and WS_SYMBOL:
        codes = [c.strip().zfill(6) for c in WS_SYMBOL.split(",") if c.strip()]
    if not codes:
        codes = ["005930"]

    info_map = _load_latest_1d_info(codes)
    today = _now_kst().strftime("%Y%m%d")
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "symulation")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{today}_trading_result.csv")

    def _append_ws_row(row: Dict[str, Any]) -> None:
        write_header = not os.path.exists(out_path)
        df = pd.DataFrame([row])
        df.to_csv(out_path, mode="a", header=write_header, index=False, encoding="utf-8-sig")

    def _append_ws_start_marker() -> None:
        marker = f"[Mode 5] _ 웹소켓 시뮬레이션 시작============ { _now_kst().strftime('%Y-%m-%d %H:%M:%S') }"
        with open(out_path, "a", encoding="utf-8-sig") as f:
            f.write(marker + "\n")

    state: Dict[str, Dict[str, Any]] = {}
    for code in codes:
        info = info_map.get(code, {})
        r_avr = _to_float(info.get("R_avr", 0))
        state[code] = {
            "name": name_map.get(code, ""),
            "r_avr": r_avr,
            "pdy_close": _to_float(info.get("pdy_close", 0)),
            "pdy_ctrt": _to_float(info.get("pdy_ctrt", 0)),
            "open": None,
            "min_low": None,
            "peak": None,
            "last_price": None,
            "buy_pr": 0.0,
            "in_position": False,
            "trail_step": 0.0,
            "trail_stop": 0.0,
            "total_ret": 1.0,
            "tot_ret": 0.0,
            "sell_cnt": 0,
        }

    def _log_event(
        code: str,
        event: str,
        price: float,
        time_str: str,
        buy_pr: float | None = None,
        sell_pr: float | None = None,
        trail_step: float | None = None,
        min_low: float | None = None,
        rk: float | None = None,
        breakout: float | None = None,
        ret: float | None = None,
    ) -> None:
        s = state[code]
        name = s.get("name", "")
        tot_ret = s.get("tot_ret", 0)
        date_str = _now_kst().strftime("%Y%m%d")
        if event == "BUY":
            msg = (
                f"[{date_str} {time_str}] {code} {name} "
                f"buy: {buy_pr:.0f} (low: {min_low:.0f}, RK: {rk:.0f}, breakout: {breakout:.0f})"
            )
        elif event == "TRAIL_UP":
            msg = (
                f"[{date_str} {time_str}] {code} {name} "
                f"pr: {price:.0f}  trail: {trail_step:.2f}"
            )
        elif event == "SELL":
            diff = (sell_pr - buy_pr) if buy_pr and sell_pr else 0.0
            msg = (
                f"[{date_str} {time_str}] {code} {name} "
                f"buy_pr: {buy_pr:.0f} sell_pr {sell_pr:.0f} diff {diff:.0f} "
                f"ret {'' if ret is None else f'{ret:.4f}'}"
            )
        else:
            msg = (
                f"[{date_str} {time_str}] {code} {name} {event} "
                f"price={price:.0f} tot_ret={tot_ret:.4f}"
            )
        # ensure status line doesn't overlap event logs
        sys.stdout.write("\n")
        sys.stdout.flush()
        print(msg)
        _append_ws_row(
            {
                "date": date_str,
                "time": time_str,
                "code": code,
                "name": name,
                "price": f"{price:.0f}",
                "min_low": "" if min_low is None else f"{min_low:.0f}",
                "RK": "" if rk is None else f"{rk:.0f}",
                "breakout": "" if breakout is None else f"{breakout:.0f}",
                "buy_pr": "" if buy_pr is None else f"{buy_pr:.0f}",
                "trail_stop": f"{s.get('trail_stop', 0):.0f}",
                "sell_pr": "" if sell_pr is None else f"{sell_pr:.0f}",
                "ret": "" if ret is None else f"{ret:.4f}",
                "tot_ret": f"{tot_ret:.4f}",
            }
        )

    _append_ws_start_marker()
    print(f"subscribe codes: {', '.join(codes)}")
    names_line = ", ".join(name_map.get(code, "") for code in codes)
    print(f"subscribe names: {names_line}")
    last_tick_ts: float | None = None
    warned_missing = False
    last_summary_ts: float | None = None
    status_code = codes[0] if codes else ""
    tick_sec = int(time.time())
    tick_cnt = 0
    tick_rate = 0

    def _watch_no_ticks() -> None:
        nonlocal last_tick_ts
        while True:
            time.sleep(20)
            if last_tick_ts is None:
                ts = _now_kst().strftime("%y%m%d_%H:%M:%S")
                print(f"[{ts}] 아직 체결가가 수신되지 않습니다. 종목/시간대 확인 필요.")
            else:
                return

    def _pick_field(row: Any, keys: List[str], default: Any = None) -> Any:
        for key in keys:
            try:
                if key in row:
                    return row.get(key)
            except Exception:
                pass
        return default

    def on_result(ws, tr_id, result, data_info):  # type: ignore[no-untyped-def]
        nonlocal last_tick_ts, warned_missing, last_summary_ts, tick_sec, tick_cnt, tick_rate
        if result is None or result.empty:
            return
        try:
            row = result.iloc[0]
            time_str = str(
                _pick_field(row, ["STCK_CNTG_HOUR", "CNTG_HOUR", "HOUR", "TIME", "time"], "")
            ).zfill(6)
            price = float(
                _pick_field(row, ["STCK_PRPR", "PRPR", "CNTG_PRIC", "LAST", "PRICE"], 0) or 0
            )
            code = str(
                _pick_field(
                    row,
                    [
                        "MKSC_SHRN_ISCD",
                        "STCK_CODE",
                        "STCK_SHRN_ISCD",
                        "SHRN_ISCD",
                        "PDNO",
                        "ISCD",
                        "CODE",
                    ],
                    "",
                )
            ).strip().zfill(6)
        except Exception:
            return
        if not time_str or price <= 0:
            if not warned_missing:
                warned_missing = True
                print("tick parse 실패: 필드명이 다르거나 가격/시간이 비어있습니다.")
            return
        if code not in state:
            if len(state) == 1:
                code = list(state.keys())[0]
            else:
                return

        last_tick_ts = time.time()
        now_sec = int(last_tick_ts)
        if now_sec == tick_sec:
            tick_cnt += 1
        else:
            tick_rate = tick_cnt
            tick_sec = now_sec
            tick_cnt = 1

        s = state[code]
        if s["open"] is None:
            s["open"] = price
            s["min_low"] = price
            s["peak"] = price
            _log_event(code, "START", price, time_str)
            return

        s["min_low"] = min(s["min_low"], price)
        s["peak"] = max(s["peak"], price)
        s["last_price"] = price

        if code == status_code and s["min_low"] and s["min_low"] > 0:
            cur_ret = price / s["min_low"] - 1
            status = (
                f"[{today} {time_str}] {code} {s.get('name','')} "
                f"price={price:.0f} min_low={s['min_low']:.0f} "
                f"low_ret={cur_ret:+.4f} ({tick_rate}/sec)"
            )
            sys.stdout.write("\r" + status.ljust(120))
            sys.stdout.flush()

        if last_summary_ts is None or time.time() - last_summary_ts >= 300:
            rows: List[Dict[str, str]] = []
            tot_ret_vals: List[float] = []
            for code_key in codes:
                st = state.get(code_key, {})
                last_price = _to_float(st.get("last_price", 0))
                pdy_close = _to_float(st.get("pdy_close", 0))
                tdy_ctrt = (last_price / pdy_close - 1) if pdy_close > 0 else 0.0
                tot_ret_val = _to_float(st.get("tot_ret", 0))
                tot_ret_vals.append(tot_ret_val)
                rows.append(
                    {
                        "date": today,
                        "code": code_key,
                        "name": st.get("name", ""),
                        "pdy_close": f"{pdy_close:.0f}" if pdy_close else "",
                        "open": f"{_to_float(st.get('open', 0)):.0f}" if st.get("open") else "",
                        "close": f"{last_price:.0f}" if last_price else "",
                        "prdy_ctrt": f"{_to_float(st.get('pdy_ctrt', 0)):.6f}" if st.get("pdy_ctrt") else "",
                        "tdy_ctrt": f"{tdy_ctrt:.6f}" if last_price and pdy_close else "",
                        "sell_cnt": f"{_to_int(st.get('sell_cnt', 0))}",
                        "tot_ret": f"{tot_ret_val:.6f}",
                    }
                )
            if tot_ret_vals:
                avg_tot_ret = sum(tot_ret_vals) / len(tot_ret_vals)
                rows.append(
                    {
                        "date": "avg_tot_ret",
                        "code": "",
                        "name": "",
                        "pdy_close": "",
                        "open": "",
                        "close": "",
                        "prdy_ctrt": "",
                        "tdy_ctrt": "",
                        "sell_cnt": "",
                        "tot_ret": f"{avg_tot_ret:.6f}",
                    }
                )
            columns = [
                "date",
                "code",
                "name",
                "pdy_close",
                "open",
                "close",
                "prdy_ctrt",
                "tdy_ctrt",
                "sell_cnt",
                "tot_ret",
            ]
            align = {
                "date": "left",
                "code": "right",
                "name": "left",
                "pdy_close": "right",
                "open": "right",
                "close": "right",
                "prdy_ctrt": "right",
                "tdy_ctrt": "right",
                "sell_cnt": "right",
                "tot_ret": "right",
            }
            sys.stdout.write("\n")
            sys.stdout.flush()
            print_table(rows, columns, align)
            last_summary_ts = time.time()

        if not s["in_position"]:
            breakout = s["min_low"] + (s["r_avr"] * K)
            if breakout > 0 and price >= breakout:
                s["buy_pr"] = breakout
                s["in_position"] = True
                s["trail_step"] = 0.0
                s["trail_stop"] = 0.0
                _log_event(
                    code,
                    "BUY",
                    price,
                    time_str,
                    buy_pr=breakout,
                    min_low=s["min_low"],
                    rk=s["r_avr"] * K,
                    breakout=breakout,
                )
            return

        buy_pr = s["buy_pr"]
        if buy_pr <= 0:
            return

        if price < buy_pr:
            sell_pr = buy_pr * (1 - SLIPPAGE)
            trade_ret = (sell_pr / buy_pr - 1) if buy_pr > 0 else 0.0
            s["total_ret"] *= (1 + trade_ret)
            s["tot_ret"] = s["total_ret"] - 1
            s["sell_cnt"] += 1
            s["in_position"] = False
            s["buy_pr"] = 0.0
            s["trail_step"] = 0.0
            s["trail_stop"] = 0.0
            s["min_low"] = sell_pr
            s["peak"] = sell_pr
            _log_event(code, "SELL", price, time_str, buy_pr=buy_pr, sell_pr=sell_pr, ret=trade_ret)
            return

        peak_ret = (s["peak"] / buy_pr - 1) if buy_pr > 0 else 0.0
        trigger_price = buy_pr * (1 + TRAIL_STEP + STOP_LOSS)
        if price >= trigger_price:
            reached_steps = [st for st in TRAIL_STEPS if peak_ret >= st]
            if reached_steps:
                new_step = max(reached_steps)
                if new_step > s["trail_step"]:
                    s["trail_step"] = new_step
                    s["trail_stop"] = buy_pr * (1 + new_step)
                    _log_event(code, "TRAIL_UP", price, time_str, trail_step=new_step)

        if s["trail_stop"] > 0 and price <= s["trail_stop"]:
            sell_pr = s["trail_stop"] * (1 - SLIPPAGE)
            trade_ret = (sell_pr / buy_pr - 1) if buy_pr > 0 else 0.0
            s["total_ret"] *= (1 + trade_ret)
            s["tot_ret"] = s["total_ret"] - 1
            s["sell_cnt"] += 1
            s["in_position"] = False
            s["buy_pr"] = 0.0
            s["trail_step"] = 0.0
            s["trail_stop"] = 0.0
            s["min_low"] = sell_pr
            s["peak"] = sell_pr
            _log_event(code, "SELL", price, time_str, buy_pr=buy_pr, sell_pr=sell_pr, ret=trade_ret)

    kws = ka.KISWebSocket(api_url="")
    kws.subscribe(request=ccnl_total, data=codes)
    threading.Thread(target=_watch_no_ticks, daemon=True).start()
    kws.start(on_result=on_result)


def _reset_token(client: KisClient) -> None:
    client.access_token = None
    client.token_expire_dt = None
    try:
        if os.path.exists(client.cfg.token_cache_path):
            os.remove(client.cfg.token_cache_path)
    except Exception:
        pass


def _get_balance_page(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    ctx_fk: str = "",
    ctx_nk: str = "",
    return_raw: bool = False,
    timeout_sec: float = 10.0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str, str] | Tuple[List[Dict[str, Any]], Dict[str, Any], str, str, Dict[str, Any]]:
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
    headers = client._headers(tr_id=tr_id)
    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "00",
        "CTX_AREA_FK100": ctx_fk,
        "CTX_AREA_NK100": ctx_nk,
    }

    last_err: Optional[Exception] = None
    retried_for_token = False
    retried_for_rate = 0
    retried_for_conn = 0
    while True:
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout_sec)
            if r.status_code != 200:
                try:
                    j = r.json()
                except Exception:
                    raise RuntimeError(f"잔고조회 HTTP {r.status_code}: {r.text}")
                if str(j.get("msg_cd")) == "EGW00201" and retried_for_rate < 3:
                    retried_for_rate += 1
                    time.sleep(1.5 * retried_for_rate)
                    continue
                if (
                    str(j.get("msg_cd")) == "EGW00123"
                    and "token" in str(j.get("msg1", "")).lower()
                    and not retried_for_token
                ):
                    _reset_token(client)
                    headers = client._headers(tr_id=tr_id)
                    retried_for_token = True
                    continue
                raise RuntimeError(f"잔고조회 HTTP {r.status_code}: {j}")

            j = r.json()
            if str(j.get("rt_cd")) != "0":
                if str(j.get("msg_cd")) == "EGW00201" and retried_for_rate < 3:
                    retried_for_rate += 1
                    time.sleep(1.5 * retried_for_rate)
                    continue
                if (
                    str(j.get("msg_cd")) == "EGW00123"
                    and "token" in str(j.get("msg1", "")).lower()
                    and not retried_for_token
                ):
                    _reset_token(client)
                    headers = client._headers(tr_id=tr_id)
                    retried_for_token = True
                    continue
                msg = j.get("msg1")
                if str(j.get("msg_cd")) == "OPSQ2000" and "INVALID_CHECK_ACNO" in str(msg or ""):
                    raise RuntimeError(f"[잔고조회실패] 계좌번호 오류 CANO={cano} ACNT_PRDT_CD={acnt_prdt_cd}: {msg}")
                raise RuntimeError(f"잔고조회 실패: {msg} raw={j}")
        except requests.exceptions.RequestException as e:
            last_err = e
            if retried_for_conn < 3:
                retried_for_conn += 1
                time.sleep(1.0 * retried_for_conn)
                continue
            raise
        except Exception as e:
            last_err = e
            raise
        break

    if last_err:
        raise last_err

    output1 = j.get("output1") or []
    output2 = j.get("output2") or {}
    if isinstance(output1, dict):
        output1 = [output1]
    if isinstance(output2, list):
        pass
    elif not isinstance(output2, dict):
        output2 = {}

    next_fk = j.get("ctx_area_fk100")
    next_nk = j.get("ctx_area_nk100")
    if not next_fk and isinstance(output2, dict):
        next_fk = output2.get("ctx_area_fk100")
    if not next_nk and isinstance(output2, dict):
        next_nk = output2.get("ctx_area_nk100")
    if return_raw:
        return output1, output2, str(next_fk or ""), str(next_nk or ""), j
    return output1, output2, str(next_fk or ""), str(next_nk or "")


def _sell_market(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    code: str,
    qty: int,
) -> Dict[str, Any]:
    pre_qty = 0
    if _trade_ledger_enabled() and CURRENT_TR_ID_BALANCE:
        pre_qty = _get_holding_qty(client, cano, acnt_prdt_cd, CURRENT_TR_ID_BALANCE, code)
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    body = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "PDNO": code,
        "ORD_DVSN": ORD_DVSN_IOC_MKT,  # IOC 시장가
        "ORD_QTY": str(qty),
        "ORD_UNPR": "0",
    }
    headers = client._headers(tr_id=tr_id)
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"매도 주문 실패: {j.get('msg1')} raw={j}")
    ord_no = _to_str(_pick(j.get("output", {}) or {}, "ODNO", "odno", "ord_no", default=""))
    if ord_no:
        ORDER_RESP_BY_CODE[code] = j
    _record_trade_ledger(
        "order",
        side="SELL",
        code=code,
        ord_no=ord_no,
        req_qty=qty,
        ord_dvsn="01",
        req_price=0.0,
        pre_qty=pre_qty,
    )
    if _trade_ledger_enabled():
        _schedule_trade_fill_check(
            client,
            cano,
            acnt_prdt_cd,
            CURRENT_TR_ID_BALANCE or tr_id,
            ord_no,
            code,
            "SELL",
            qty,
            0.0,
            pre_qty,
        )
    return j


def _sell_order(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    code: str,
    qty: int,
    price: float,
) -> Dict[str, Any]:
    pre_qty = 0
    if _trade_ledger_enabled() and CURRENT_TR_ID_BALANCE:
        pre_qty = _get_holding_qty(client, cano, acnt_prdt_cd, CURRENT_TR_ID_BALANCE, code)
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    body = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "PDNO": code,
        "ORD_DVSN": "00",  # 지정가
        "ORD_QTY": str(qty),
        "ORD_UNPR": str(int(price)),
    }
    headers = client._headers(tr_id=tr_id)
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"매도 주문 실패: {j.get('msg1')} raw={j}")
    ord_no = _to_str(_pick(j.get("output", {}) or {}, "ODNO", "odno", "ord_no", default=""))
    if ord_no:
        ORDER_RESP_BY_CODE[code] = j
    _record_trade_ledger(
        "order",
        side="SELL",
        code=code,
        ord_no=ord_no,
        req_qty=qty,
        ord_dvsn="00",
        req_price=float(price),
        pre_qty=pre_qty,
    )
    if _trade_ledger_enabled():
        _schedule_trade_fill_check(
            client,
            cano,
            acnt_prdt_cd,
            CURRENT_TR_ID_BALANCE or tr_id,
            ord_no,
            code,
            "SELL",
            qty,
            float(price),
            pre_qty,
        )
    return j


def _buy_order(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    code: str,
    qty: int,
    price: float | None,
) -> Dict[str, Any]:
    pre_qty = 0
    if _trade_ledger_enabled() and CURRENT_TR_ID_BALANCE:
        pre_qty = _get_holding_qty(client, cano, acnt_prdt_cd, CURRENT_TR_ID_BALANCE, code)
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    ord_dvsn = ORD_DVSN_IOC_BEST if price is None else "00"
    body = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "PDNO": code,
        "ORD_DVSN": ord_dvsn,
        "ORD_QTY": str(qty),
        "ORD_UNPR": "0" if price is None else str(int(price)),
    }
    headers = client._headers(tr_id=tr_id)
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"매수 주문 실패: {j.get('msg1')} raw={j}")
    ord_no = _to_str(_pick(j.get("output", {}) or {}, "ODNO", "odno", "ord_no", default=""))
    if ord_no:
        ORDER_RESP_BY_CODE[code] = j
    _record_trade_ledger(
        "order",
        side="BUY",
        code=code,
        ord_no=ord_no,
        req_qty=qty,
        ord_dvsn=ord_dvsn,
        req_price=0.0 if price is None else float(price),
        pre_qty=pre_qty,
    )
    if _trade_ledger_enabled():
        _schedule_trade_fill_check(
            client,
            cano,
            acnt_prdt_cd,
            CURRENT_TR_ID_BALANCE or tr_id,
            ord_no,
            code,
            "BUY",
            qty,
            0.0 if price is None else float(price),
            pre_qty,
        )
    return j


def _buy_order_after_hours(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    code: str,
    qty: int,
    price: float,
) -> Dict[str, Any]:
    pre_qty = 0
    if _trade_ledger_enabled() and CURRENT_TR_ID_BALANCE:
        pre_qty = _get_holding_qty(client, cano, acnt_prdt_cd, CURRENT_TR_ID_BALANCE, code)
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    body = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "PDNO": code,
        "ORD_DVSN": "07",  # 시간외 단일가
        "ORD_QTY": str(qty),
        "ORD_UNPR": str(int(price)),
    }
    headers = client._headers(tr_id=tr_id)
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"시간외 매수 주문 실패: {j.get('msg1')} raw={j}")
    ord_no = _to_str(_pick(j.get("output", {}) or {}, "ODNO", "odno", "ord_no", default=""))
    if ord_no:
        ORDER_RESP_BY_CODE[code] = j
    _record_trade_ledger(
        "order",
        side="BUY",
        code=code,
        ord_no=ord_no,
        req_qty=qty,
        ord_dvsn="07",
        req_price=float(price),
        pre_qty=pre_qty,
    )
    return j


def _cancel_order(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    ordno: str,
    code: str,
    qty: int,
) -> Dict[str, Any]:
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-rvsecncl"
    body = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "KRX_FWDG_ORD_ORGNO": "00000",
        "ORGN_ORDNO": ordno,
        "ORD_DVSN": "00",
        "RVSE_CNCL_DVSN_CD": "02",
        "ORD_QTY": str(qty),
        "ORD_UNPR": "0",
        "PDNO": code,
    }
    headers = client._headers(tr_id=tr_id)
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"주문취소 실패: {j.get('msg1')} raw={j}")
    return j


def _latest_parquet_candidates(
    parquet_path: str, min_ret: float, mode: int
) -> pd.DataFrame:
    cols = ["date", "symbol", "close", "value", "volume"]
    df = pd.read_parquet(
        parquet_path,
        columns=cols + ["pdy_ctrt", "tdy_ctrt", "p2dy_ctrt", "pdy_close", "p2dy_close", "pdy_uc", "pdy_h", "pdy_L"],
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    if "pdy_close" not in df.columns or df["pdy_close"].isna().all():
        df["pdy_close"] = df.groupby("symbol")["close"].shift(1)
    if "p2dy_close" not in df.columns or df["p2dy_close"].isna().all():
        df["p2dy_close"] = df.groupby("symbol")["close"].shift(2)
    if "tdy_ctrt" not in df.columns or df["tdy_ctrt"].isna().all():
        df["tdy_ctrt"] = (df["close"] / df["pdy_close"]) - 1
    if "pdy_ctrt" not in df.columns or df["pdy_ctrt"].isna().all():
        df["pdy_ctrt"] = (df["pdy_close"] / df["p2dy_close"]) - 1
    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy()
    if mode == 1:
        latest = latest[latest["tdy_ctrt"] >= min_ret].copy()
    else:
        latest = latest[latest["pdy_ctrt"] >= min_ret].copy()
    return latest.sort_values("value", ascending=False)


def _fetch_fluctuation_top(
    client: KisClient, top_n: int = 50, market_div: str = "J"
) -> List[Dict[str, Any]]:
    """
    국내주식 등락률 순위 API 호출 (상위 n개)
    """
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/ranking/fluctuation"
    headers = client._headers(tr_id="FHPST01700000")
    params = {
        "fid_cond_mrkt_div_code": market_div,
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
    last_err: Exception | None = None
    for attempt in range(1, client.cfg.max_http_retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=client.cfg.timeout_sec)
            if r.status_code >= 500:
                last_err = RuntimeError(
                    f"[등락률순위] 서버 오류 status={r.status_code} body={r.text[:500]}"
                )
                sleep = client.cfg.http_backoff_sec * (2 ** (attempt - 1))
                time.sleep(sleep)
                continue
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
        except Exception as e:
            last_err = e
            sleep = client.cfg.http_backoff_sec * (2 ** (attempt - 1))
            print(f"[등락률순위] attempt={attempt}/{client.cfg.max_http_retries} failed: {e} (sleep {sleep:.1f}s)")
            time.sleep(sleep)

    raise RuntimeError(f"[등락률순위] 최종 실패: {last_err}")


def _top_gainers_candidates(
    client: KisClient,
    exclude_codes: set[str],
    name_map: Dict[str, str],
    top_n: int = 50,
    apply_filters: bool = True,
    min_ret: Optional[float] = None,
    max_ret: float = 0.33,
    min_price: float = 500.0,
    require_open_low: bool = False,
) -> pd.DataFrame:
    rows = _fetch_fluctuation_top(client, top_n=top_n, market_div=client.cfg.market_div)
    items: List[Dict[str, Any]] = []
    for row in rows:
        code = _to_str(_pick(row, "stck_shrn_iscd", "stck_cd", "code", default=""))
        if not code or code in exclude_codes:
            continue
        name = _to_str(_pick(row, "hts_kor_isnm", "stck_name", "name", default=""))
        ret_ratio = _rate_to_ratio(_pick(row, "prdy_ctrt", "ctrt", "rate", default=0))
        if apply_filters:
            if min_ret is not None and ret_ratio < min_ret:
                continue
            if ret_ratio >= max_ret:
                continue
        cur_price = _to_float(_pick(row, "stck_prpr", "prpr", "price", default=0))
        prev_close = _to_float(_pick(row, "prdy_clpr", "prev_close", default=0))
        if prev_close <= 0 and cur_price > 0 and ret_ratio > -0.999:
            prev_close = cur_price / (1 + ret_ratio)
        if apply_filters and min_price > 0 and max(prev_close, cur_price) < min_price:
            continue
        if cur_price <= 0:
            continue
        value = _to_float(_pick(row, "acml_tr_pbmn", "acml_tr_pbmn_amt", "value", default=0))
        items.append(
            {
                "symbol": code,
                "name": name or name_map.get(code, ""),
                "ret": ret_ratio,
                "prev_close": prev_close,
                "cur_price": cur_price,
                "value": value,
            }
        )
    if not items:
        return pd.DataFrame()
    return pd.DataFrame(items).sort_values("ret", ascending=False)


def _select_candidates(
    client: KisClient,
    parquet_path: str,
    name_map: Dict[str, str],
    mode: int,
    min_ret: float,
    top_n: int = 50,
    emit: bool = True,
) -> pd.DataFrame:
    """
    mode=1: 전일 대비 금일 (Parquet_DB tdy_ctrt)
    mode=2: 전전일 대비 금일 (Parquet_DB pdy_ctrt)
    mode=3: 전일 대비 금일 (등락률 순위 API 상위 top_n)
    """
    if mode in (1, 2):
        candidates = _latest_parquet_candidates(parquet_path, min_ret, mode)
        if candidates.empty:
            if emit:
                label = (
                    "전일 대비 금일(Parquet_DB 마지막 수신일의 전일 대비)"
                    if mode == 1
                    else "전전일 대비 금일(Parquet_DB 마지막 수신일의 전전일 대비)"
                )
                print(f"[종목선정] 기준={label}, 기준값>={min_ret:.2f} 없음")
            return candidates
        candidates = candidates[candidates["close"] >= 500].copy()
        candidates = candidates[candidates["value"] >= 1_000_000_000].copy()
        show = candidates.copy()
        show["name"] = show["symbol"].map(name_map).fillna("")
        if mode == 1:
            show["tdy_ctrt(%)"] = (show["tdy_ctrt"] * 100).round(2)
        else:
            show["pdy_ctrt(%)"] = (show["pdy_ctrt"] * 100).round(2)
        show["p2dy_close"] = show["p2dy_close"].apply(_round_to_tick)
        show["pdy_close"] = show["pdy_close"].apply(_round_to_tick)
        show["close"] = show["close"].apply(_round_to_tick)
        show["value"] = show["value"].where(show["value"] > 0, show.get("volume"))
        show["date"] = show["date"].dt.strftime("%Y-%m-%d")
        if mode == 1:
            show = show.sort_values(["tdy_ctrt", "symbol"], ascending=[False, False])
        else:
            show = show.sort_values(["pdy_ctrt", "symbol"], ascending=[False, False])
        if emit:
            if mode == 1:
                print("[종목선정] 기준=전일 대비 금일(Parquet_DB 마지막 수신일의 전일 대비)")
            else:
                print("[종목선정] 기준=전전일 대비 금일(Parquet_DB 마지막 수신일의 전전일 대비)")
            table = (
                show[
                    [
                        "date",
                        "symbol",
                        "name",
                        "tdy_ctrt(%)" if mode == 1 else "pdy_ctrt(%)",
                        "p2dy_close",
                        "pdy_close",
                        "close",
                        "value",
                    ]
                ]
                .rename(columns={"symbol": "code"})
            )
            table_rows = table.to_dict("records")
            for r in table_rows:
                key = "tdy_ctrt(%)" if mode == 1 else "pdy_ctrt(%)"
                r[key] = f"{_to_float(r.get(key)):,.2f}"
                r["p2dy_close"] = f"{_to_float(r.get('p2dy_close')):,.0f}"
                r["pdy_close"] = f"{_to_float(r.get('pdy_close')):,.0f}"
                r["close"] = f"{_to_float(r.get('close')):,.0f}"
                r["value"] = f"{_to_float(r.get('value')):,.0f}"
            _print_table(
                table_rows,
                [
                    "date",
                    "code",
                    "name",
                    "tdy_ctrt(%)" if mode == 1 else "pdy_ctrt(%)",
                    "p2dy_close",
                    "pdy_close",
                    "close",
                    "value",
                ],
                {
                    ("tdy_ctrt(%)" if mode == 1 else "pdy_ctrt(%)"): "right",
                    "p2dy_close": "right",
                    "pdy_close": "right",
                    "close": "right",
                    "value": "right",
                },
            )
            print(f"[종목선정] 총 {len(table)}건")
        return candidates
    # mode 3: 등락률 순위 API
    rows = _fetch_fluctuation_top(client, top_n=top_n, market_div=client.cfg.market_div)  # 등락률 순위 API 상위 top_n(=50)개를 API를 통해 가져오는 함수
    items: List[Dict[str, Any]] = []
    for row in rows:
        code = _to_str(_pick(row, "stck_shrn_iscd", "stck_cd", "code", default=""))
        name = _to_str(_pick(row, "hts_kor_isnm", "stck_name", "name", default=""))
        ret_ratio = _rate_to_ratio(_pick(row, "prdy_ctrt", "ctrt", "rate", default=0))
        if ret_ratio < min_ret:
            continue
        cur_price = _to_float(_pick(row, "stck_prpr", "prpr", "price", default=0))
        if cur_price < 500:
            continue
        prev_close = _to_float(_pick(row, "prdy_clpr", "prev_close", default=0))
        if prev_close <= 0 and cur_price > 0 and ret_ratio > -0.999:
            prev_close = cur_price / (1 + ret_ratio)
        value = _to_float(_pick(row, "acml_tr_pbmn", "acml_tr_pbmn_amt", "value", default=0))
        volume = _to_float(_pick(row, "acml_vol", "acml_vol_cnt", "volume", default=0))
        if value <= 0 and volume > 0 and cur_price > 0:
            value = cur_price * volume
        if value < 1_000_000_000:
            continue
        items.append(
            {
                "symbol": code,
                "name": name or name_map.get(code, ""),
                "ret": ret_ratio,
                "prev_close": prev_close,
                "cur_price": cur_price,
                "value": value,
                "volume": volume,
            }
        )
    if not items:
        if emit:
            print(f"[종목선정] 기준=전일 대비 금일(API실시간, 금일/전일), 기준값>={min_ret:.2f} 없음")
        return pd.DataFrame()
    df = pd.DataFrame(items).sort_values(["ret", "symbol"], ascending=[False, False])
    if not df.empty:
        df["date"] = _now_kst().strftime("%Y-%m-%d")
    df["ret(%)"] = (df["ret"] * 100).round(2)
    df["prev_close"] = df["prev_close"].apply(_round_to_tick)
    df["cur_price"] = df["cur_price"].apply(_round_to_tick)
    df["value"] = df["value"].where(df["value"] > 0, df["volume"])
    if emit:
        print("[종목선정] 기준=전일 대비 금일(API실시간, 금일/전일)")
        table = (
            df[
                ["date", "symbol", "name", "ret(%)", "prev_close", "cur_price", "value"]
            ]
            .rename(columns={"symbol": "code"})
        )
        table_rows = table.to_dict("records")
        for r in table_rows:
            r["ret(%)"] = f"{_to_float(r.get('ret(%)')):,.2f}"
            r["prev_close"] = f"{_to_float(r.get('prev_close')):,.0f}"
            r["cur_price"] = f"{_to_float(r.get('cur_price')):,.0f}"
            r["value"] = f"{_to_float(r.get('value')):,.0f}"
        _print_table(
            table_rows,
            ["date", "code", "name", "ret(%)", "prev_close", "cur_price", "value"],
            {
                "ret(%)": "right",
                "prev_close": "right",
                "cur_price": "right",
                "value": "right",
            },
        )
        print(f"[종목선정] 총 {len(table)}건")
    return df


def _load_symbol_master_codes() -> List[str]:
    layout = load_kis_data_layout()
    path = symbol_master_path(layout)
    if not os.path.exists(path):
        return []
    sdf = pd.read_parquet(path, columns=["code"])
    codes = sdf["code"].astype(str).str.strip()
    # KIS inquire-price는 6자리 숫자 종목코드만 허용
    codes = codes[codes.str.fullmatch(r"\d{6}")]
    return codes.tolist()


def _inquire_open_orders(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
) -> List[Dict[str, Any]]:
    """
    미체결/주문 조회 (open id)
    - 정정/취소 가능 주문 조회 API로 통일
    """
    orders, _ = _inquire_psbl_rvsecncl(client, cano, acnt_prdt_cd, tr_id)
    return orders


def _inquire_order_status(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    ordno: str,
    code: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    timeout_sec: float = 10.0,
) -> Dict[str, Any]:
    if not ordno:
        return {}
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-order"
    now = _now_kst()
    if not start_date or not end_date:
        if now.hour < 9:
            query_date = (now - timedelta(days=1)).strftime("%Y%m%d")
        else:
            query_date = now.strftime("%Y%m%d")
        start_date = start_date or query_date
        end_date = end_date or query_date
    body = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "INQR_STRT_DT": start_date,
        "INQR_END_DT": end_date,
        "SLL_BUY_DVSN_CD": "00",
        "INQR_DVSN": "00",
        "PDNO": code or "",
        "CCLD_DVSN": "00",
        "ORD_GNO_BRNO": "",
        "ODNO": ordno,
        "INQR_ORD_QTY": "100",
        "INQR_ORD_PAGE_NO": "1",
        "INQR_DVSN_3": "00",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": "",
    }
    headers = client._headers(tr_id=tr_id)
    headers["content-type"] = "application/json"
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    r = requests.post(url, headers=headers, json=body, timeout=timeout_sec)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"주문조회 실패: {j.get('msg1')} raw={j}")
    output = j.get("output") or j.get("output1") or j.get("output2") or []
    if isinstance(output, dict):
        return output
    if isinstance(output, list):
        for row in output:
            if not isinstance(row, dict):
                continue
            odno = _to_str(_pick(row, "ordno", "odno", default=""))
            if odno == _to_str(ordno):
                return row
        return output[0] if output else {}
    return {}


def _inquire_psbl_rvsecncl(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    timeout_sec: float = 10.0,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    주식정정취소가능주문조회 (실전용)
    """
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"
    headers = client._headers(tr_id=tr_id)
    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "INQR_DVSN": "00",
        "SLL_BUY_DVSN_CD": "00",
        "INQR_STRT_DT": "",
        "INQR_END_DT": "",
        "PDNO": "",
        "ORD_GNO_BRNO": "",
        "ODNO": "",
        "INQR_DVSN_1": "0",
        "INQR_DVSN_2": "0",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }
    out: List[Dict[str, Any]] = []
    last_raw: Dict[str, Any] = {}
    for _ in range(100):
        r = requests.get(url, headers=headers, params=params, timeout=timeout_sec)
        r.raise_for_status()
        j = r.json()
        last_raw = j
        if str(j.get("rt_cd")) != "0":
            _log(f"[open_order][debug] params={params}")
            raise RuntimeError(f"정정/취소 가능 주문 조회 실패: {j.get('msg1')} raw={j}")
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
    return out, last_raw


def _run_open_id_cleanup(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_open: str,
    tr_id_cancel: str,
) -> None:
    if not tr_id_open:
        print("[mode2] tr_id_open을 config.json에 설정하세요.")
        return
    _log(f"[open_id] 정정/취소 가능 주문 조회 시작 CANO={cano}")
    orders, _ = _inquire_psbl_rvsecncl(client, cano, acnt_prdt_cd, tr_id_open)
    if not orders:
        print("[open_id] 미체결 없음")
        return
    print("[open_id] 미체결 주문")
    df = pd.DataFrame(orders)
    cols = [c for c in ["odno", "ordno", "pdno", "ord_qty", "ord_qty", "ord_unpr", "ord_tmd"] if c in df.columns]
    df_out = df[cols] if cols else df
    # 중복 제거(페이지 중복/동일 레코드 반복 방지)
    dedupe_keys = [c for c in ["ord_dt", "ord_tmd", "pdno", "ordno", "ord_qty", "ord_unpr"] if c in df_out.columns]
    if dedupe_keys:
        df_out = df_out.drop_duplicates(subset=dedupe_keys).reset_index(drop=True)
    print(df_out)
    while True:
        ans = input("[정리] 미체결 전부 취소? 1.예 2.아니오: ").strip()
        if ans in ("1", "2"):
            break
    if ans != "1":
        return
    for row in orders:
        ordno = _to_str(_pick(row, "odno", "ordno", default=""))
        code = _to_str(_pick(row, "pdno", default=""))
        qty = _to_int(_pick(row, "ord_qty", "ord_qty", default=0))
        if not ordno or not code or qty <= 0:
            continue
        try:
            _cancel_order(client, cano, acnt_prdt_cd, tr_id_cancel, ordno, code, qty)
            _log(f"[미체결취소] {code} ordno={ordno} qty={qty}")
        except Exception as e:
            print(f"[취소실패] {code} ordno={ordno} error={e}")


def _run_open_order_inquiry(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_open: str,
) -> None:
    if not tr_id_open:
        print("[mode22] tr_id_open을 config.json에 설정하세요.")
        return
    _log(f"[open_order] 정정/취소 가능 주문 조회 시작 CANO={cano}")
    orders, raw = _inquire_psbl_rvsecncl(client, cano, acnt_prdt_cd, tr_id_open)
    orders = [
        o for o in orders if _to_int(_pick(o, "psbl_qty", default=0)) > 0
    ]
    if not orders:
        _log("현재 미체결 주문이 없습니다. (psbl_qty > 0 기준)")
        if isinstance(raw, dict):
            msg_cd = _to_str(raw.get("msg_cd", ""))
            msg1 = _to_str(raw.get("msg1", ""))
            if msg_cd or msg1:
                _log(f"[open_order][응답] msg_cd={msg_cd} msg1={msg1}")
                if "없습니다" in msg1:
                    _log("[open_order] 조회 결과 없음")
            output = raw.get("output") or []
            if isinstance(output, dict):
                output = [output]
            df_raw = pd.DataFrame(output) if output else pd.DataFrame()
            cols = [
                c
                for c in [
                    "pdno",
                    "prdt_name",
                    "ord_dvsn_name",
                    "ord_qty",
                    "psbl_qty",
                    "ord_unpr",
                    "ord_tmd",
                    "odno",
                ]
                if (not df_raw.empty and c in df_raw.columns) or df_raw.empty
            ]
            if not cols:
                cols = ["pdno", "prdt_name", "ord_dvsn_name", "ord_qty", "psbl_qty", "ord_unpr", "ord_tmd", "odno"]
            rows = df_raw[cols].to_dict("records") if not df_raw.empty else [{c: "" for c in cols}]
            _log("[open_order][raw] 응답 목록")
            _print_table(
                rows,
                cols,
                {"ord_qty": "right", "psbl_qty": "right", "ord_unpr": "right", "ord_tmd": "right"},
            )
        return
    df = pd.DataFrame(orders)
    cols = [
        c
        for c in [
            "odno",
            "ordno",
            "pdno",
            "prdt_name",
            "sll_buy_dvsn_cd_name",
            "ord_qty",
            "ord_unpr",
            "ord_tmd",
        ]
        if c in df.columns
    ]
    rows = df[cols].to_dict("records") if cols else df.to_dict("records")
    _log("[open_order] 정정/취소 가능 주문 목록")
    if rows:
        _print_table(
            rows,
            cols if cols else list(rows[0].keys()),
            {
                "ord_qty": "right",
                "ord_unpr": "right",
                "ord_tmd": "right",
            },
        )


def _inquire_daily_ccld(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    start_date: str,
    end_date: str,
    ord_no: str | None = None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-order"
    body = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "INQR_STRT_DT": start_date,
        "INQR_END_DT": end_date,
        "SLL_BUY_DVSN_CD": "00",
        "INQR_DVSN": "00",
        "PDNO": "",
        "CCLD_DVSN": "00",
        "ORD_GNO_BRNO": "",
        "ODNO": ord_no or "",
        "INQR_ORD_QTY": "100",
        "INQR_ORD_PAGE_NO": "1",
        "INQR_DVSN_3": "00",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": "",
    }
    out: List[Dict[str, Any]] = []
    raw_pages: List[Dict[str, Any]] = []
    retry_conn = 0
    max_pages = 1 if ord_no else 200
    for _ in range(max_pages):
        while True:
            try:
                headers = client._headers(tr_id=tr_id)
                headers["content-type"] = "application/json"
                headers["hashkey"] = _hashkey(
                    client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body
                )
                r = requests.post(url, headers=headers, json=body, timeout=10)
                r.raise_for_status()
                j = r.json()
                raw_pages.append(j)
                break
            except requests.exceptions.RequestException:
                if retry_conn < 3:
                    retry_conn += 1
                    time.sleep(1.0 * retry_conn)
                    continue
                raise
        if str(j.get("rt_cd")) != "0":
            raise RuntimeError(f"[주문내역조회] 실패: {j.get('msg1')} raw={j}")
        output = j.get("output") or j.get("output1") or []
        if isinstance(output, dict):
            output = [output]
        if isinstance(output, list):
            out.extend(output)
        if ord_no:
            break
        tr_cont = str(j.get("tr_cont") or "N")
        ctx_fk = j.get("ctx_area_fk200") or ""
        ctx_nk = j.get("ctx_area_nk200") or ""
        if tr_cont == "N" or (not ctx_fk and not ctx_nk):
            break
        body["CTX_AREA_FK200"] = ctx_fk
        body["CTX_AREA_NK200"] = ctx_nk
        body["INQR_ORD_PAGE_NO"] = str(int(_to_int(body.get("INQR_ORD_PAGE_NO", 1))) + 1)
    return out, raw_pages


def _run_daily_ccld_inquiry(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_order: str,
) -> None:
    if not tr_id_order:
        print("[mode21] tr_id_order(주문내역조회, inquire-order)를 config.json에 설정하세요.")
        return
    now = _now_kst()
    if now.hour < 9:
        query_date = (now - timedelta(days=1)).strftime("%Y%m%d")
    else:
        query_date = now.strftime("%Y%m%d")
    cfg = load_config()
    start_date = _to_str(cfg.get("daily_ccld_start", "")).replace("-", "")
    end_date = _to_str(cfg.get("daily_ccld_end", "")).replace("-", "")
    if not start_date and not end_date:
        start_date = query_date
        end_date = query_date
    elif start_date and not end_date:
        end_date = start_date
    elif end_date and not start_date:
        start_date = end_date
    date_tag = start_date if start_date == end_date else f"{start_date}_{end_date}"
    _log(f"[주문내역조회] 시작 CANO={cano} date={date_tag}")
    orders, raw_pages = _inquire_daily_ccld(
        client, cano, acnt_prdt_cd, tr_id_order, start_date, end_date
    )
    if not orders:
        print("[주문내역조회] 체결/주문 없음")
        return
    df = pd.DataFrame(orders)
    time_col = "ord_tmd" if "ord_tmd" in df.columns else "ccld_tmd"
    if time_col in df.columns:
        df = df[df[time_col].astype(str) >= "090000"]
    cols = [
        c
        for c in [
            "ord_dt",
            "ord_tmd",
            "ccld_tmd",
            "pdno",
            "prdt_name",
            "sll_buy_dvsn_cd_name",
            "ord_dvsn_cd_name",
            "ord_stat_cd_name",
            "ord_qty",
            "ord_unpr",
            "ccld_qty",
            "ccld_unpr",
            "ordno",
            "odno",
        ]
        if c in df.columns
    ]
    if "ord_dvsn_cd_name" not in df.columns and "ord_dvsn_cd" in df.columns:
        cols.append("ord_dvsn_cd")
    for extra in ("ord_stat_cd", "ccld_yn", "cncl_yn", "rjct_brdn_name"):
        if extra in df.columns and extra not in cols:
            cols.append(extra)
    if "ccld_qty" not in df.columns and "tot_ccld_qty" in df.columns:
        cols.append("tot_ccld_qty")
    if "ccld_unpr" not in df.columns and "tot_ccld_amt" in df.columns:
        cols.append("tot_ccld_amt")
    for extra in ("ord_amt", "tot_ccld_amt", "ccld_amt"):
        if extra in df.columns and extra not in cols:
            cols.append(extra)
    df_out = df[cols] if cols else df
    # 시간순 정렬(주문일자/주문시간)
    if "ord_dt" in df_out.columns and "ord_tmd" in df_out.columns:
        df_out = df_out.sort_values(["ord_dt", "ord_tmd"]).reset_index(drop=True)
    rows = df_out.to_dict("records")
    # if rows:
    #     for r in rows:
    #         for key in ("ord_qty", "ord_unpr", "ccld_qty", "ccld_unpr", "tot_ccld_qty", "tot_ccld_amt"):
    #             if key in r:
    #                 r[key] = f"{_to_float(r.get(key)):,.0f}"
    #     _print_table(rows, cols if cols else list(rows[0].keys()), {})
    # else:
    #     print(df_out)
    print(df_out)
    out_dir = os.path.join(load_kis_data_layout().local_root, "logs")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"order_ccld(주문체결내역)_{date_tag}.csv")
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[csv] saved: {out_path}")
    raw_path = os.path.join(out_dir, f"order_ccld(주문체결내역)_{date_tag}.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(orders, f, ensure_ascii=False)
    print(f"[json] saved: {raw_path}")
    raw_pages_path = os.path.join(out_dir, f"order_ccld(주문체결내역)_raw_{date_tag}.json")
    with open(raw_pages_path, "w", encoding="utf-8") as f:
        json.dump(raw_pages, f, ensure_ascii=False)
    print(f"[json] saved: {raw_pages_path}")


def _pc_step_threshold_buy(ret: float) -> float:
    if ret >= 0.25:
        return 0.25
    if ret >= 0.20:
        return 0.20
    if ret >= 0.15:
        return 0.15
    if ret >= 0.10:
        return 0.10
    if ret >= 0.05:
        return 0.05
    return 0.0


def _pc_step_threshold_prev(ret: float) -> float:
    if ret >= 0.29:
        return 0.29
    return 0.0


def _monitor_symbol_thread(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_sell: str,
    symbol: str,
    qty: int,
    name: str,
) -> None:
    max_step = 0.0
    _log(f"[모니터링시작] {symbol} {name} qty={qty}")
    while True:
        inquire = _inquire_price_retry(client, symbol)
        cur_price = _to_float(_pick(inquire, "stck_prpr", "prpr", default=0))
        prev_close = _to_float(_pick(inquire, "stck_prdy_clpr", "prdy_clpr", default=0))
        if prev_close <= 0 or cur_price <= 0:
            time.sleep(MONITOR_INTERVAL_SEC)
            continue
        ret_prev = (cur_price / prev_close) - 1
        step = _pc_step_threshold_prev(ret_prev)
        if step > max_step:
            max_step = step
        elif max_step > 0 and ret_prev < max_step:
            try:
                resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, symbol, qty)
                _log(
                    f"[모니터매도] {symbol} {name} qty={qty} ret_prev={ret_prev:.3f} max_step={max_step:.2f}"
                )
                _log(f"[모니터종료] {symbol} {name} qty={qty}")
                odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                _append_trade_log(
                    action="SELL",
                    code=symbol,
                    name=name,
                    qty=qty,
                    price=cur_price,
                    amount=qty * cur_price,
                    ret=ret_prev,
                    reason="monitor_trailing",
                    ordno=odno,
                )
            except Exception as e:
                print(f"[모니터매도실패] {symbol} qty={qty} error={e}")
            return
        time.sleep(MONITOR_INTERVAL_SEC)


def _inquire_price_retry(
    client: KisClient, code: str, retries: int = INQUIRE_RETRY_MAX, base_delay: float = INQUIRE_RETRY_DELAY
) -> Dict[str, Any]:
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            data = client.inquire_price(code)
            if isinstance(data, dict):
                if str(data.get("rt_cd", "0")) == "0":
                    return data
                msg_cd = str(data.get("msg_cd") or "")
                if msg_cd == "EGW00201":
                    time.sleep(base_delay * (attempt + 1))
                    continue
            return data
        except Exception as e:
            last_err = e
            if "EGW00201" in str(e) or "초당" in str(e):
                time.sleep(base_delay * (attempt + 1))
                continue
            break
    if last_err:
        return {}
    return {}


def _collect_holdings_snapshot(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_balance: str,
    max_pages: int | None = None,
    page_sleep_sec: float = 0.0,
    timeout_sec: float = 10.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """잔고조회 페이지를 순회하며 보유종목 스냅샷과 요약값을 모읍니다.

    - ctx pagination을 따라 모든 페이지를 수집
    - 보유수량 0은 제외
    - code 중복은 제거
    - 보유목록(현재가/매수가/주문가능수량 포함) + 잔고요약을 반환
    """
    ctx_fk = ""
    ctx_nk = ""
    holdings: list[dict[str, Any]] = []
    seen: set[str] = set()
    summary_data: dict[str, Any] = {}
    page_count = 0
    while True:
        rows, summary, ctx_fk, ctx_nk = _get_balance_page(
            client,
            cano,
            acnt_prdt_cd,
            tr_id_balance,
            ctx_fk=ctx_fk,
            ctx_nk=ctx_nk,
            timeout_sec=timeout_sec,
        )
        summary_rows = summary if isinstance(summary, list) else [summary]
        if summary_rows and not summary_data:
            summary_data = summary_rows[0]
        output1 = rows if isinstance(rows, list) else []
        for row in output1:
            qty = _to_int(_pick(row, "hldg_qty", default=0))
            if qty <= 0:
                continue
            code = _to_str(_pick(row, "pdno", default=""))
            if not code or code in seen:
                continue
            name = _to_str(_pick(row, "prdt_name", default=""))
            cur_price = _to_float(_pick(row, "prpr", default=0))
            buy_price = _to_float(_pick(row, "pchs_avg_pric", default=0))
            prev_close = _to_float(_pick(row, "prdy_clpr", "stck_prdy_clpr", default=0))
            eval_amt = _to_float(_pick(row, "evlu_amt", default=0))
            sellable_qty = _to_int(
                _pick(row, "ord_psbl_qty", "ord_psbl_qty", "sellable_qty", default=qty)
            )
            holdings.append(
                {
                    "code": code,
                    "name": name,
                    "qty": qty,
                    "sellable_qty": sellable_qty,
                    "cur_price": cur_price,
                    "buy_price": buy_price,
                    "prev_close": prev_close,
                    "eval_amt": eval_amt,
                }
            )
            seen.add(code)
        page_count += 1
        if max_pages is not None and page_count >= max_pages:
            break
        if not ctx_fk and not ctx_nk:
            break
        if page_sleep_sec > 0:
            time.sleep(page_sleep_sec)
    return holdings, summary_data


def _load_latest_1d_info(codes: List[str]) -> Dict[str, Dict[str, float]]:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "1d_data", "kis_1d_unified_parquet_DB.parquet")
    if not os.path.exists(path) or not codes:
        return {}
    try:
        df = pd.read_parquet(path, columns=["date", "symbol", "pdy_close", "R_avr", "pdy_ctrt", "p2dy_close"])
    except Exception:
        try:
            df = pd.read_parquet(path, columns=["date", "symbol", "pdy_close", "R_avr"])
        except Exception:
            return {}
    if df.empty:
        return {}
    latest_date = df["date"].max()
    df = df[df["date"] == latest_date]
    df = df[df["symbol"].astype(str).isin(codes)]
    info: Dict[str, Dict[str, float]] = {}
    for _, row in df.iterrows():
        pdy_close = _to_float(row.get("pdy_close", 0))
        p2dy_close = _to_float(row.get("p2dy_close", 0))
        pdy_ctrt = _to_float(row.get("pdy_ctrt", 0))
        if not pdy_ctrt and pdy_close > 0 and p2dy_close > 0:
            pdy_ctrt = (pdy_close / p2dy_close) - 1
        info[str(row["symbol"])] = {
            "pdy_close": pdy_close,
            "R_avr": _to_float(row.get("R_avr", 0)),
            "pdy_ctrt": pdy_ctrt,
        }
    return info


def _next_half_hour(dt: datetime) -> datetime:
    base = dt.replace(second=0, microsecond=0)
    if base.minute < 30:
        return base.replace(minute=30)
    return (base + timedelta(hours=1)).replace(minute=0)


def _send_buy_today_report(
    client: KisClient, cano: str, acnt_prdt_cd: str, tr_id_balance: str
) -> None:
    if not _BUY_TODAY:
        _log("[30분보고] 당일 매수 종목이 없습니다.")
        return

    holdings, _ = _collect_holdings_snapshot(client, cano, acnt_prdt_cd, tr_id_balance)
    hold_map = {h["code"]: h for h in holdings}
    lines = []
    for code, info in _BUY_TODAY.items():
        h = hold_map.get(code)
        qty = _to_int(h.get("qty", 0)) if h else _to_int(info.get("qty", 0))
        if qty <= 0:
            continue
        name = info.get("name") or (h.get("name", "") if h else "")
        buy_price = _to_float(info.get("buy_price", 0)) or (h.get("buy_price", 0) if h else 0)
        if buy_price <= 0:
            continue
        inquire = _inquire_price_retry(client, code)
        cur_price = _to_float(
            _pick(inquire, "stck_prpr", "prpr", default=h.get("cur_price", 0) if h else 0)
        )
        if cur_price <= 0:
            continue
        diff_amt = (cur_price - buy_price) * qty
        ret = (cur_price / buy_price) - 1
        lines.append(f"{code} {name} qty={qty} ret={ret:.2%} diff={diff_amt:,.0f}")

    if not lines:
        _log("[30분보고] 당일 매수 보유종목이 없습니다.")
        return

    msg = "[30분보고] 당일 매수 종목 현황\n" + "\n".join(lines)
    _log(msg)


def _monitor_holdings_loop(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_balance: str,
    tr_id_sell: str,
    tr_id_buy: str,
    tr_id_open: str,
    tr_id_cancel: str,
    exclude_cash: float,
    name_map: Dict[str, str],
    start_time: datetime,
    end_time: datetime,
    tick_hook: Callable[[datetime], bool] | None = None,
    schedule_balance_report: Callable[[str, int], None] | None = None,
    star_interval_sec: float = 5.0,
) -> None:
    global MONITOR_THREAD_ID
    MONITOR_THREAD_ID = threading.get_ident()
    _log_tm(f"[모니터링] 시작 pid={os.getpid()} tid={MONITOR_THREAD_ID}")
    now = _now_kst()
    if now > end_time:
        _log("[모니터링] 종료시간 이후 시작 - 상태 확인 모드로 10분 유지")
        end_time = now + timedelta(minutes=10)

    buy_price_map: dict[str, float] = {}
    max_step_map: dict[str, float] = {}
    gap_checked: set[str] = set()
    sold_codes: set[str] = set()
    open_price_map: dict[str, float] = {}
    prev_close_map: dict[str, float] = {}
    cancel_cooldown: dict[str, datetime] = {}
    initialized = False
    initial_stock_eval = 0.0
    initial_invest = 0.0
    initial_tot_eval = 0.0
    initial_cash = 0.0
    next_report = _next_half_hour(_now_kst())
    next_heartbeat = _next_half_hour(_now_kst())
    next_order_check = _now_kst() + timedelta(minutes=5)
    next_balance_report = _now_kst() + timedelta(minutes=15)
    next_tick = time.time()
    next_holdings_check = time.time()
    next_star = time.time()
    next_alive_print = time.time() + 60

    fail_streak = 0
    while _now_kst() <= end_time:
        now_ts = time.time()
        if now_ts < next_tick:
            time.sleep(min(0.2, next_tick - now_ts))
            continue
        if time.time() >= next_star:
            print("*", end="", flush=True)
            next_star = time.time() + max(1.0, star_interval_sec)
        if time.time() >= next_alive_print:
            print(f"[모니터링] alive {now.strftime('%H:%M:%S')}")
            next_alive_print = time.time() + 60
        next_tick += 1.0
        now = _now_kst()
        if tick_hook and not tick_hook(now):
            break
        if time.time() < next_holdings_check:
            time.sleep(0.1)
            continue
        holdings = None
        summary = None
        last_err: Exception | None = None
        start_holdings_ts = time.time()
        for attempt in range(INQUIRE_RETRY_MAX):
            try:
                holdings, summary = _collect_holdings_snapshot(
                    client,
                    cano,
                    acnt_prdt_cd,
                    tr_id_balance,
                    max_pages=1,
                    page_sleep_sec=0.0,
                    timeout_sec=1.0,
                )
                last_err = None
                break
            except Exception as e:
                last_err = e
                _log(f"[모니터링] 잔고조회 실패({attempt+1}/{INQUIRE_RETRY_MAX}): {e}")
                time.sleep(INQUIRE_RETRY_DELAY * (attempt + 1))
        elapsed = time.time() - start_holdings_ts
        if elapsed >= 5:
            _log(f"[모니터링] 잔고조회 지연 {elapsed:.1f}s")
        if last_err or holdings is None:
            fail_streak += 1
            next_holdings_check = time.time() + MONITOR_POLL_SEC
            continue
        if fail_streak > 0:
            _log(f"[모니터링] 잔고조회 복구 성공 (fail_streak={fail_streak})")
            fail_streak = 0
        if not holdings:
            if not initialized:
                _log("[모니터링] 보유 종목이 없습니다. 잔고 변동 대기 모드로 전환합니다.")
            next_holdings_check = time.time() + MONITOR_POLL_SEC
            continue

        if now < start_time:
            next_holdings_check = time.time() + MONITOR_POLL_SEC
            continue

        if not initialized:
            for h in holdings:
                cur_p = _to_float(h.get("cur_price", 0))
                buy_p = _to_float(h.get("buy_price", 0)) or cur_p
                qty = _to_int(h.get("qty", 0))
                buy_price_map[h["code"]] = buy_p
                max_step_map[h["code"]] = 0.0
                initial_stock_eval += cur_p * qty
                initial_invest += buy_p * qty
            initial_tot_eval = _to_float(_pick(summary, "tot_evlu_amt", default=0))
            initial_cash = initial_tot_eval - initial_stock_eval
            _log(
                f"[모니터링] 보유종목 모니터링 시작 pid={os.getpid()} tid={MONITOR_THREAD_ID} "
                f"holdings={len(holdings)}"
            )
            for h in holdings:
                code = h["code"]
                name = h.get("name", "")
                cur_price = _to_float(h.get("cur_price", 0))
                buy_price = buy_price_map.get(code) or _to_float(h.get("buy_price", 0))
                if buy_price <= 0 or cur_price <= 0:
                    continue
                ret_buy = (cur_price / buy_price) - 1
                mark = "******" if ret_buy < 0 else ""
                _log(f"[트레일링초기] {code} {name} ret_buy={ret_buy:.3f}{mark}")
                step_buy = _pc_step_threshold_buy(ret_buy)
                if step_buy > 0:
                    max_step_map[code] = step_buy
                    mark = "******" if ret_buy < 0 else ""
                    _log(
                        f"[트레일링상향] {code} {name} ret_buy={ret_buy:.3f}{mark} "
                        f"0.00 -> {step_buy:.2f}"
                    )
            initialized = True

        for h in holdings:
            code = h["code"]
            if code in sold_codes:
                continue
            raw_qty = _to_int(h.get("qty", 0))
            raw_sellable = _to_int(h.get("sellable_qty", raw_qty))
            qty = _to_int(min(raw_qty, raw_sellable))
            if raw_sellable != raw_qty:
                _log(
                    f"[매도가능수량] {code} 보유={raw_qty} 주문가능={raw_sellable} -> 매도수량={qty}"
                )
            if qty <= 0:
                if raw_qty > 0:
                    _log(f"[매도스킵] {code} 보유={raw_qty} 주문가능={raw_sellable}")
                    last_cancel = cancel_cooldown.get(code)
                    if not last_cancel or (_now_kst() - last_cancel).total_seconds() >= 30:
                        try:
                            orders, _ = _inquire_psbl_rvsecncl(
                                client, cano, acnt_prdt_cd, tr_id_open
                            )
                            targets = [
                                o
                                for o in orders
                                if _to_str(_pick(o, "pdno", default="")) == code
                            ]
                            if targets:
                                for o in targets:
                                    ordno = _to_str(_pick(o, "odno", "ordno", default=""))
                                    qty_cancel = _to_int(
                                        _pick(o, "psbl_qty", "ord_qty", default=0)
                                    )
                                    if not ordno or qty_cancel <= 0:
                                        continue
                                    _cancel_order(
                                        client,
                                        cano,
                                        acnt_prdt_cd,
                                        tr_id_cancel,
                                        ordno,
                                        code,
                                        qty_cancel,
                                    )
                                    _log(f"[미체결취소] {code} ordno={ordno} qty={qty_cancel}")
                                cancel_cooldown[code] = _now_kst()
                                time.sleep(1)
                        except Exception as e:
                            _log(f"[미체결취소실패] {code} error={e}")
                continue
            name = h.get("name", "")
            buy_price = buy_price_map.get(code) or _to_float(h.get("buy_price", 0))
            if buy_price <= 0:
                buy_price = _to_float(h.get("cur_price", 0))
            if buy_price <= 0:
                continue
            buy_price_map[code] = buy_price

            cur_price = _to_float(h.get("cur_price", 0))
            open_p = open_price_map.get(code, 0.0)
            prev_close = prev_close_map.get(code, 0.0)
            if open_p <= 0 or prev_close <= 0 or cur_price <= 0:
                try:
                    inquire = _inquire_price_retry(client, code)
                    cur_price = _to_float(
                        _pick(inquire, "stck_prpr", "prpr", default=cur_price)
                    )
                    open_p = _to_float(_pick(inquire, "stck_oprc", "oprc", default=open_p))
                    prev_close = _to_float(
                        _pick(inquire, "stck_prdy_clpr", "prdy_clpr", default=prev_close)
                    )
                    if open_p > 0:
                        open_price_map[code] = open_p
                    if prev_close > 0:
                        prev_close_map[code] = prev_close
                except Exception:
                    pass

            if cur_price <= 0:
                continue

            if code not in gap_checked:
                gap_checked.add(code)
                if open_p > 0 and prev_close > 0 and open_p < prev_close:
                    try:
                        resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, qty)
                        odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                        amt = qty * cur_price
                        ret = (cur_price / buy_price) - 1
                        _log(
                            f"[갭하락매도] {code} {name} qty={qty} price={cur_price:,.0f} "
                            f"amt={amt:,.0f} ret={ret:.3f} ord=IOC시장가"
                        )
                        _log(f"[매도주문성공] {code} ordno={odno}")
                        _append_trade_log(
                            action="SELL",
                            code=code,
                            name=name,
                            qty=qty,
                            price=cur_price,
                            amount=amt,
                            ret=ret,
                            reason="gap_down_monitor",
                            ordno=odno,
                        )
                        sold_codes.add(code)
                        if schedule_balance_report:
                            schedule_balance_report("갭하락 매도 10초 후", 10)
                        continue
                    except Exception as e:
                        print(f"[갭하락매도실패] {code} qty={qty} error={e}")

            if cur_price <= buy_price * 0.995:
                try:
                    resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, qty)
                    odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                    amt = qty * cur_price
                    ret = (cur_price / buy_price) - 1
                    _log(
                        f"[매수가하회매도] {code} {name} qty={qty} price={cur_price:,.0f} "
                        f"amt={amt:,.0f} ret={ret:.3f} ord=IOC시장가"
                    )
                    _log(f"[매도주문성공] {code} ordno={odno}")
                    _append_trade_log(
                        action="SELL",
                        code=code,
                        name=name,
                        qty=qty,
                        price=cur_price,
                        amount=amt,
                        ret=ret,
                        reason="below_buy_price",
                        ordno=odno,
                    )
                    sold_codes.add(code)
                    if schedule_balance_report:
                        schedule_balance_report("매수가하회 매도 10초 후", 10)
                    continue
                except Exception as e:
                    print(f"[매도가하회매도실패] {code} qty={qty} error={e}")

            ret_buy = (cur_price / buy_price) - 1 if buy_price > 0 else 0.0
            ret_prev = (cur_price / prev_close) - 1 if prev_close > 0 else 0.0
            step_buy = _pc_step_threshold_buy(ret_buy)
            step_prev = _pc_step_threshold_prev(ret_prev)
            step = max(step_buy, step_prev)
            max_step = max_step_map.get(code, 0.0)
            if step > max_step:
                prev_step = max_step
                max_step_map[code] = step
                max_step = step
                mark = "******" if ret_buy < 0 else ""
                _log(
                    f"[트레일링상향] {code} {name} ret_buy={ret_buy:.3f}{mark} "
                    f"ret_prev={ret_prev:.3f} {prev_step:.2f} -> {step:.2f}"
                )
            if max_step >= 0.29:
                ret_cmp = ret_prev
            else:
                ret_cmp = ret_buy
            if max_step > 0 and ret_cmp < max_step:
                try:
                    resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, qty)
                    odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                    amt = qty * cur_price
                    _log(
                        f"[트레일링매도] {code} {name} qty={qty} price={cur_price:,.0f} "
                        f"amt={amt:,.0f} ret_buy={ret_buy:.3f} ret_prev={ret_prev:.3f} "
                        f"max_step={max_step:.2f} ord=IOC시장가"
                    )
                    _log(f"[매도주문성공] {code} ordno={odno}")
                    _append_trade_log(
                        action="SELL",
                        code=code,
                        name=name,
                        qty=qty,
                        price=cur_price,
                        amount=amt,
                        ret=ret_buy,
                        reason="trailing_stop",
                        ordno=odno,
                    )
                    sold_codes.add(code)
                    if schedule_balance_report:
                        schedule_balance_report("트레일링 매도 10초 후", 10)
                except Exception as e:
                    print(f"[트레일링매도실패] {code} qty={qty} error={e}")

            time.sleep(0.2)
        now = _now_kst()
        if now >= next_report:
            _send_buy_today_report(client, cano, acnt_prdt_cd, tr_id_balance)
            next_report = next_report + timedelta(minutes=30)
        if now >= next_heartbeat:
            _log(
                f"[모니터링] 진행중 {now.strftime('%H:%M')} 보유종목={len(holdings)} tid={MONITOR_THREAD_ID}"
            )
            next_heartbeat = next_heartbeat + timedelta(minutes=30)
        if now >= next_balance_report:
            _print_account_holdings_snapshot(
                client, cano, acnt_prdt_cd, tr_id_balance, "15분 잔고조회"
            )
            next_balance_report = next_balance_report + timedelta(minutes=15)
        if now >= next_order_check:
            try:
                orders = _inquire_open_orders(client, cano, acnt_prdt_cd, tr_id_open)
                if orders:
                    rows = []
                    for o in orders:
                        code = _to_str(_pick(o, "pdno", default=""))
                        ordno = _to_str(_pick(o, "odno", "ordno", default=""))
                        qty = _to_int(_pick(o, "ord_qty", default=0))
                        price = _to_float(_pick(o, "ord_unpr", default=0))
                        rows.append(
                            {
                                "code": code,
                                "ordno": ordno,
                                "qty": f"{qty:,d}",
                                "price": f"{price:,.0f}",
                            }
                        )
                    _log(f"[주문현황] 미체결 {len(rows)}건")
                    _print_table(rows, ["code", "ordno", "qty", "price"], {"qty": "right", "price": "right"})
                else:
                    _log("[주문현황] 미체결 없음")
            except Exception as e:
                print(f"[주문현황 조회 실패] {e}")
            next_order_check = now + timedelta(minutes=5)
        time.sleep(MONITOR_POLL_SEC)

    _, summary_end = _collect_holdings_snapshot(client, cano, acnt_prdt_cd, tr_id_balance)
    final_tot_eval = _to_float(_pick(summary_end, "tot_evlu_amt", default=0))
    total_profit = final_tot_eval - initial_tot_eval
    total_rate = total_profit / initial_tot_eval if initial_tot_eval > 0 else 0.0
    invest_profit = final_tot_eval - initial_cash - initial_invest
    invest_rate = invest_profit / initial_invest if initial_invest > 0 else 0.0
    _log(
        "[모니터링 종료] 15:30 마감\n"
        f"- 투자액 기준 손익: {invest_profit:,.0f} ({invest_rate:.2%})\n"
        f"- 총 평가액 기준 손익: {total_profit:,.0f} ({total_rate:.2%})\n"
        f"- 초기 총평가: {initial_tot_eval:,.0f} -> 최종 총평가: {final_tot_eval:,.0f}"
    )


def _monitor_holdings_simple(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_balance: str,
    tr_id_sell: str,
    tr_id_buy: str,
    exclude_cash: float,
    start_time: datetime,
    end_time: datetime,
    poll_sec: float = 5.0,
) -> None:
    """초기 보유종목 기준으로 5초마다 잔고조회 후 매수가 이하 매도."""
    _log_tm(f"[모니터링] 간단 스레드 시작 pid={os.getpid()} tid={threading.get_ident()}")
    tracked: dict[str, dict[str, Any]] = {}
    sold_codes: set[str] = set()
    initial_logged = False
    last_status_ts = 0.0
    last_sell_status_ts = 0.0
    watch_info: Dict[str, Dict[str, float]] = {}

    while _now_kst() <= end_time:
        now = _now_kst()
        auction_start = now.replace(hour=15, minute=10, second=0, microsecond=0)
        auction_end = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now < start_time:
            time.sleep(1)
            continue

        try:
            holdings, _ = _collect_holdings_snapshot(
                client,
                cano,
                acnt_prdt_cd,
                tr_id_balance,
                max_pages=1,
                page_sleep_sec=0.0,
                timeout_sec=3.0,
            )
        except Exception as e:
            _log(f"[모니터링] 잔고조회 실패: {e}")
            time.sleep(poll_sec)
            continue

        if not holdings:
            if not initial_logged:
                _log("[모니터링] 보유종목 0건 (잔고기준)")
                initial_logged = True
            time.sleep(poll_sec)
            continue

        if not tracked:
            codes = []
            for h in holdings:
                code = _to_str(h.get("code", ""))
                if not code:
                    continue
                codes.append(code)
                tracked[code] = {
                    "name": _to_str(h.get("name", "")),
                    "buy_price": _to_float(h.get("buy_price", 0)),
                    "last_qty": _to_int(h.get("qty", 0)),
                    "day_low": _to_float(h.get("cur_price", 0)),
                    "in_position": True,
                    "peak_price": _to_float(h.get("cur_price", 0)),
                    "trail_stop": 0.0,
                }
            watch_info = _load_latest_1d_info(codes)
            for code in codes:
                info = watch_info.get(code, {})
                tracked[code]["pdy_close"] = _to_float(info.get("pdy_close", 0))
                tracked[code]["R_avr"] = _to_float(info.get("R_avr", 0))
            _log(f"[모니터링] 초기 보유종목 {len(tracked)}건")
            initial_logged = True

        open_sell_by_code: dict[str, Dict[str, Any]] = {}
        if CURRENT_TR_ID_OPEN:
            try:
                orders = _inquire_open_orders(client, cano, acnt_prdt_cd, CURRENT_TR_ID_OPEN)
            except Exception:
                orders = []
            for o in orders:
                code_o = _to_str(_pick(o, "pdno", default=""))
                if not code_o:
                    continue
                side_name = _to_str(_pick(o, "sll_buy_dvsn_cd_name", default=""))
                side_cd = _to_str(_pick(o, "sll_buy_dvsn_cd", default=""))
                is_sell = "매도" in side_name or side_cd == "01"
                if not is_sell:
                    continue
                rem_qty = _to_int(_pick(o, "psbl_qty", "ord_qty", default=0))
                if rem_qty <= 0:
                    continue
                open_sell_by_code[code_o] = o

        if time.time() - last_status_ts >= poll_sec:
            _log(f"[모니터링] 현재 보유종목 {len(holdings)}건")
            rows = []
            for h in holdings:
                code = _to_str(h.get("code", ""))
                if code in sold_codes:
                    continue
                name = _to_str(h.get("name", ""))
                qty = _to_int(h.get("qty", 0))
                cur_price = _to_float(h.get("cur_price", 0))
                buy_price = _to_float(h.get("buy_price", 0))
                o = open_sell_by_code.get(code)
                ord_pr = _to_float(_pick(o, "ord_unpr", default=0)) if o else 0.0
                diff = cur_price - buy_price if buy_price > 0 else 0.0
                vdiff = diff * qty
                diff_rate = diff / buy_price if buy_price > 0 else 0.0
                rows.append(
                    {
                        "code": code,
                        "name": name,
                        "qty": f"{qty:,d}",
                        "buy_pr": f"{buy_price:,.0f}",
                        "cur_price": f"{cur_price:,.0f}",
                        "diff": f"{diff:,.0f}",
                        "Vdiff": f"{vdiff:,.0f}",
                        "diff_rate(%)": f"{diff_rate * 100:.2f}",
                        "sell_ord_pr": f"{ord_pr:,.0f}" if ord_pr > 0 else "",
                    }
                )
            if rows:
                _print_table(
                    rows,
                    ["code", "name", "qty", "buy_pr", "cur_price", "diff", "Vdiff", "diff_rate(%)", "sell_ord_pr"],
                    {
                        "qty": "right",
                        "buy_pr": "right",
                        "cur_price": "right",
                        "diff": "right",
                        "Vdiff": "right",
                        "diff_rate(%)": "right",
                        "sell_ord_pr": "right",
                    },
                )
            last_status_ts = time.time()

        holding_map = {str(h.get("code", "")): h for h in holdings if h.get("code")}
        no_sell_codes: list[str] = []
        for code, info in tracked.items():
            h = holding_map.get(code)
            if h:
                qty = _to_int(h.get("sellable_qty", h.get("qty", 0)))
                info["last_qty"] = _to_int(h.get("qty", 0))
            else:
                qty = _to_int(info.get("last_qty", 0))
            in_position = bool(info.get("in_position", False))
            buy_price = _to_float(info.get("buy_price", 0))

            inquire = _inquire_price_retry(client, code)
            cur_price = _to_float(
                _pick(
                    inquire,
                    "stck_prpr",
                    "prpr",
                    default=_to_float(h.get("cur_price", 0)) if h else 0,
                )
            )
            low_price = _to_float(_pick(inquire, "stck_lwpr", "lwpr", default=cur_price))
            exp_price = _to_float(_pick(inquire, "antc_cnpr", "antc_prc", default=cur_price))
            if cur_price > 0:
                info["day_low"] = min(_to_float(info.get("day_low", cur_price)), low_price if low_price > 0 else cur_price)

            if auction_start <= now <= auction_end and in_position and buy_price > 0 and qty > 0:
                if exp_price > 0 and exp_price < buy_price:
                    o = open_sell_by_code.get(code)
                    ordno = _to_str(_pick(o, "odno", "ordno", default="")) if o else ""
                    ord_unpr = _to_float(_pick(o, "ord_unpr", default=0)) if o else 0.0
                    if ordno and CURRENT_TR_ID_CANCEL:
                        try:
                            _cancel_order(client, cano, acnt_prdt_cd, CURRENT_TR_ID_CANCEL, ordno, code, qty)
                        except Exception as e:
                            _log(f"[동시호가정정실패] {code} ordno={ordno} error={e}")
                    if ordno and ord_unpr and abs(ord_unpr - exp_price) < 1e-6:
                        continue
                    try:
                        _sell_order(client, cano, acnt_prdt_cd, tr_id_sell, code, qty, exp_price)
                        _log(
                            f"[동시호가매도] {code} {info.get('name','')} qty={qty} "
                            f"exp={exp_price:,.0f} buy={buy_price:,.0f}"
                        )
                    except Exception as e:
                        _log(f"[동시호가매도실패] {code} qty={qty} error={e}")
                    continue

            if code in open_sell_by_code and qty > 0:
                o = open_sell_by_code[code]
                ordno = _to_str(_pick(o, "odno", "ordno", default=""))
                rem_qty = _to_int(_pick(o, "psbl_qty", "ord_qty", default=qty))
                _log(
                    f"[매도오더잔존] {code} {info.get('name','')} ordno={ordno} qty={rem_qty}"
                )
                if CURRENT_TR_ID_CANCEL and ordno and rem_qty > 0:
                    try:
                        _cancel_order(client, cano, acnt_prdt_cd, CURRENT_TR_ID_CANCEL, ordno, code, rem_qty)
                    except Exception as e:
                        _log(f"[미체결취소실패] {code} ordno={ordno} error={e}")
                if rem_qty > 0:
                    try:
                        _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, rem_qty)
                    except Exception as e:
                        _log(f"[매도재주문실패] {code} qty={rem_qty} error={e}")
                continue
            else:
                no_sell_codes.append(code)

            # 매수가 미만 즉시 매도
            if in_position and buy_price > 0 and cur_price > 0 and cur_price < buy_price and qty > 0:
                try:
                    resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, qty)
                    odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                    _log(
                        f"[매수가하회매도] {code} {info.get('name','')} qty={qty} "
                        f"price={cur_price:,.0f} buy={buy_price:,.0f}"
                    )
                    _append_trade_log(
                        action="SELL",
                        code=code,
                        name=_to_str(info.get("name", "")),
                        qty=qty,
                        price=cur_price,
                        amount=qty * cur_price,
                        ret=(cur_price / buy_price) - 1,
                        reason="below_buy_price",
                        ordno=odno,
                    )
                    info["in_position"] = False
                    sold_codes.add(code)
                except Exception as e:
                    _log(f"[매수가하회매도실패] {code} qty={qty} error={e}")
                continue

            # 당일 매수 종목 트레일링 적용
            if in_position and code in _BUY_TODAY and buy_price > 0 and cur_price > 0:
                peak = _to_float(info.get("peak_price", cur_price))
                peak = max(peak, cur_price)
                info["peak_price"] = peak
                exec_ctrt = (cur_price / buy_price) - 1
                pdy_close = _to_float(info.get("pdy_close", 0))
                trail_stop = _to_float(info.get("trail_stop", 0))
                for step in [0.05, 0.10, 0.15, 0.20, 0.25, 0.29]:
                    if step >= 0.29:
                        if pdy_close > 0 and (cur_price / pdy_close - 1) >= 0.29:
                            trail_stop = max(trail_stop, pdy_close * (1 + 0.29))
                    else:
                        if exec_ctrt >= step + 0.02:
                            trail_stop = max(trail_stop, peak * (1 - step))
                info["trail_stop"] = trail_stop
                if trail_stop > 0 and cur_price <= trail_stop and qty > 0:
                    try:
                        resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, qty)
                        odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                        _log(
                            f"[트레일링매도] {code} {info.get('name','')} qty={qty} "
                            f"price={cur_price:,.0f} stop={trail_stop:,.0f}"
                        )
                        _append_trade_log(
                            action="SELL",
                            code=code,
                            name=_to_str(info.get("name", "")),
                            qty=qty,
                            price=cur_price,
                            amount=qty * cur_price,
                            ret=(cur_price / buy_price) - 1,
                            reason="trailing_stop",
                            ordno=odno,
                        )
                        info["in_position"] = False
                        sold_codes.add(code)
                    except Exception as e:
                        _log(f"[트레일링매도실패] {code} qty={qty} error={e}")

            # 저가 돌파 기준선 매수 (14:00까지만)
            if not in_position:
                if now >= now.replace(hour=14, minute=0, second=0, microsecond=0):
                    continue
                r_avr = _to_float(info.get("R_avr", 0))
                day_low = _to_float(info.get("day_low", 0))
                if r_avr > 0 and day_low > 0 and cur_price >= day_low + (r_avr * 0.3):
                    cash = _get_orderable_cash(client, cano, acnt_prdt_cd, tr_id_balance, exclude_cash)
                    buy_qty = _to_int(info.get("last_qty", 0))
                    if buy_qty <= 0 and cur_price > 0:
                        buy_qty = int(cash // cur_price)
                    if buy_qty > 0 and buy_qty * cur_price <= cash:
                        try:
                            resp = _buy_order(client, cano, acnt_prdt_cd, tr_id_buy, code, buy_qty, None)
                            odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                            _log(
                                f"[저가돌파매수] {code} {info.get('name','')} qty={buy_qty} "
                                f"price={cur_price:,.0f} line={day_low + (r_avr * 0.3):,.0f}"
                            )
                            _append_trade_log(
                                action="BUY",
                                code=code,
                                name=_to_str(info.get("name", "")),
                                qty=buy_qty,
                                price=cur_price,
                                amount=buy_qty * cur_price,
                                reason="low_breakout",
                                ordno=odno,
                            )
                            info["buy_price"] = cur_price
                            info["in_position"] = True
                            info["peak_price"] = cur_price
                            info["trail_stop"] = 0.0
                        except Exception as e:
                            _log(f"[저가돌파매수실패] {code} qty={buy_qty} error={e}")

        if time.time() - last_sell_status_ts >= 30:
            if no_sell_codes:
                _log(f"[매도오더없음] {', '.join(no_sell_codes)}")
            last_sell_status_ts = time.time()

        time.sleep(poll_sec)


def _format_holdings_rows(holdings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    df = pd.DataFrame(holdings)
    if df.empty:
        return []
    df = (
        df.sort_values(["code", "name"])
        .drop_duplicates(subset=["code", "name"], keep="first")
        .reset_index(drop=True)
    )
    df["qty"] = df["qty"].apply(lambda v: f"{_to_int(v):,d}")
    df["price"] = df["price"].apply(lambda v: f"{_to_float(v):,.0f}")
    df["eval_amt"] = df["eval_amt"].apply(lambda v: f"{_to_float(v):,.0f}")
    if "buy_price" in df.columns:
        df["buy_price"] = df["buy_price"].apply(lambda v: f"{_to_float(v):,.0f}")
    if "diff_rate" in df.columns:
        df["diff_rate"] = df["diff_rate"].apply(lambda v: f"{_to_float(v):.2%}")
    return df.to_dict("records")


def _print_balance_snapshot(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_balance: str,
    exclude_cash: float = 0.0,
) -> None:
    ctx_fk = ""
    ctx_nk = ""
    page_idx = 1
    seen_codes: set[str] = set()
    summary_printed = False
    while True:
        rows, summary, ctx_fk, ctx_nk = _get_balance_page(
            client, cano, acnt_prdt_cd, tr_id_balance, ctx_fk=ctx_fk, ctx_nk=ctx_nk
        )
        if not summary_printed:
            summary_rows = summary if isinstance(summary, list) else [summary]
            if summary_rows:
                item = summary_rows[0]
                dnca = _to_float(_pick(item, "dnca_tot_amt", default=0))
                ord_cash = _to_float(_pick(item, "prvs_rcdl_excc_amt", default=0))
                tot_eval = _to_float(_pick(item, "tot_evlu_amt", default=0))
                real_cash = max(0.0, ord_cash - exclude_cash)
                _log(
                    f"[잔고요약] 출금가능액={dnca:,.0f} 주문가능금액={ord_cash:,.0f} "
                    f"실주문가능액={real_cash:,.0f} 총평가액={tot_eval:,.0f}"
                )
            summary_printed = True

        output1 = rows if isinstance(rows, list) else []
        holdings = []
        for row in output1:
            qty = _to_int(_pick(row, "hldg_qty", default=0))
            if qty <= 0:
                continue
            code = _to_str(_pick(row, "pdno", default=""))
            name = _to_str(_pick(row, "prdt_name", default=""))
            cur_price = _to_float(_pick(row, "prpr", default=0))
            buy_price = _to_float(_pick(row, "pchs_avg_pric", default=0))
            eval_amt = _to_float(_pick(row, "evlu_amt", default=0))
            diff_rate = (cur_price / buy_price) - 1 if buy_price > 0 else 0.0
            holdings.append(
                {
                    "code": code,
                    "name": name,
                    "qty": qty,
                    "price": cur_price,
                    "eval_amt": eval_amt,
                    "buy_price": buy_price,
                    "diff_rate": diff_rate,
                }
            )

        if not holdings:
            if page_idx == 1:
                _log("[잔고] 보유 종목이 없습니다.")
            break

        rows = _format_holdings_rows(holdings)
        if not rows:
            break
        current_codes = set(r["code"] for r in rows)
        new_codes = current_codes - seen_codes
        if not new_codes and page_idx > 1:
            _log("[잔고] 동일 페이지 반복 감지 -> 조회 종료")
            break
        seen_codes.update(current_codes)
        _log(f"[잔고] 보유 종목 (page {page_idx})")
        _print_table(
            rows,
            ["code", "name", "qty", "buy_price", "price", "diff_rate", "eval_amt"],
            {
                "qty": "right",
                "buy_price": "right",
                "price": "right",
                "diff_rate": "right",
                "eval_amt": "right",
            },
        )

        if not ctx_fk and not ctx_nk:
            break
        page_idx += 1


def _collect_holdings_for_thread(
    client: KisClient, cano: str, acnt_prdt_cd: str, tr_id_balance: str
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ctx_fk = ""
    ctx_nk = ""
    holdings: list[dict[str, Any]] = []
    seen: set[str] = set()
    summary_data: dict[str, Any] = {}
    page_idx = 1
    while True:
        print(f"[모드23] 잔고조회 요청 (page {page_idx})", flush=True)
        rows, summary, ctx_fk, ctx_nk = _get_balance_page(
            client,
            cano,
            acnt_prdt_cd,
            tr_id_balance,
            ctx_fk=ctx_fk,
            ctx_nk=ctx_nk,
            timeout_sec=3.0,
        )
        print(f"[모드23] 잔고조회 응답 (page {page_idx})", flush=True)
        summary_rows = summary if isinstance(summary, list) else [summary]
        if summary_rows and not summary_data:
            summary_data = summary_rows[0]
        output1 = rows if isinstance(rows, list) else []
        for row in output1:
            qty = _to_int(_pick(row, "hldg_qty", default=0))
            if qty <= 0:
                continue
            code = _to_str(_pick(row, "pdno", default=""))
            if not code or code in seen:
                continue
            name = _to_str(_pick(row, "prdt_name", default=""))
            buy_price = _to_float(_pick(row, "pchs_avg_pric", default=0))
            prev_close = _to_float(_pick(row, "prdy_clpr", "stck_prdy_clpr", default=0))
            holdings.append(
                {
                    "code": code,
                    "name": name,
                    "qty": qty,
                    "buy_price": buy_price,
                    "prev_close": prev_close,
                }
            )
            seen.add(code)
        if not ctx_fk and not ctx_nk:
            break
        page_idx += 1
    return holdings, summary_data


def _run_holdings_price_thread(
    client: KisClient, cano: str, acnt_prdt_cd: str, tr_id_balance: str
) -> None:
    print("[모드23] 보유종목 스레드 조회 시작", flush=True)
    try:
        start_ts = time.time()
        print("[모드23] 잔고조회 요청 (1페이지)", flush=True)
        holdings, summary_data = _collect_holdings_snapshot(
            client,
            cano,
            acnt_prdt_cd,
            tr_id_balance,
            max_pages=1,
            page_sleep_sec=0.0,
            timeout_sec=3.0,
        )
        elapsed = time.time() - start_ts
        print(f"[모드23] 잔고조회 응답 {elapsed:.1f}s", flush=True)
    except Exception as e:
        print(f"[모드23] 잔고조회 실패: {e}", flush=True)
        return
    if not holdings:
        print("[보유종목] 없음", flush=True)
        return
    stock_eval_amt = _to_float(_pick(summary_data, "evlu_amt_smtl_amt", default=0))

    print(f"[계좌] CANO={cano} ACNT_PRDT_CD={acnt_prdt_cd}", flush=True)
    now_stamp = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now_stamp}] ===============================", flush=True)
    print("[잔고요약]", flush=True)
    dnca = _to_float(_pick(summary_data, "dnca_tot_amt", default=0))
    ord_cash = _to_float(_pick(summary_data, "prvs_rcdl_excc_amt", default=0))
    tot_eval = _to_float(_pick(summary_data, "tot_evlu_amt", default=0))
    pchs_amt = _to_float(_pick(summary_data, "pchs_amt_smtl_amt", default=0))
    evlu_amt = _to_float(_pick(summary_data, "evlu_amt_smtl_amt", default=0))
    evlu_pfls = _to_float(_pick(summary_data, "evlu_pfls_smtl_amt", default=0))
    prev_tot_eval = _to_float(_pick(summary_data, "prvs_tot_evlu_amt", "bfdy_tot_evlu_amt", default=0))
    print(f"♠ 출금가능액 : {dnca:,.0f}   ♠ 주문가능금액 : {ord_cash:,.0f}", flush=True)
    print(f"- 총 매입액(pchs_amt_smtl_amt): {pchs_amt:,.0f}", flush=True)
    print(f"- 주식 평가액(evlu_amt_smtl_amt)-(a): {evlu_amt:,.0f}", flush=True)
    print(f"- 주식 평가손익(evlu_pfls_smtl_amt)-(b): {evlu_pfls:,.0f}", flush=True)
    print(f"- 주문가능금액(prvs_rcdl_excc_amt)-(c): {ord_cash:,.0f}", flush=True)
    print(f"- 총 평가액(tot_evlu_amt)-(a+b+c): {tot_eval:,.0f}", flush=True)
    if prev_tot_eval > 0:
        diff_total = tot_eval - prev_tot_eval
        diff_rate = diff_total / prev_tot_eval
        print(f"- 전일대비 총 증감 : {diff_total:,.0f}({diff_rate:+.1%})", flush=True)

    init_rows = []
    for h in holdings:
        init_rows.append(
            {
                "code": h["code"],
                "name": h["name"],
                "qty": f"{_to_int(h['qty']):,d}",
                "buy_pr": f"{_to_float(h.get('buy_price', 0)):,.0f}",
                "prev_close": f"{_to_float(h.get('prev_close', 0)):,.0f}",
            }
        )
    print(f"\n[보유종목] 총 {len(holdings)}종 주식평가액 {stock_eval_amt:,.0f}", flush=True)
    _print_table(
        init_rows,
        ["code", "name", "qty", "buy_pr", "prev_close"],
        {"qty": "right", "buy_pr": "right", "prev_close": "right"},
    )

    stop_event = threading.Event()
    last_prices: dict[str, float] = {}

    def _loop() -> None:
        while not stop_event.is_set():
            now_stamp = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{now_stamp}] ===============================", flush=True)
            print(f"[보유종목] 총 {len(holdings)}종 주식평가액 {stock_eval_amt:,.0f}", flush=True)
            rows = []
            for h in holdings:
                code = h["code"]
                name = h["name"]
                buy_price = _to_float(h.get("buy_price", 0))
                prev_close = _to_float(h.get("prev_close", 0))
                inquire = _inquire_price_retry(client, code)
                cur_price = _to_float(_pick(inquire, "stck_prpr", "prpr", default=0))
                if cur_price <= 0:
                    cur_price = last_prices.get(code, 0.0)
                else:
                    last_prices[code] = cur_price
                diff = cur_price - buy_price if buy_price > 0 else 0.0
                diff_rate = diff / buy_price if buy_price > 0 else 0.0
                prdy_rate = (cur_price / prev_close - 1) if prev_close > 0 else 0.0
                rows.append(
                    {
                        "code": code,
                        "name": name,
                        "buy_pr": f"{buy_price:,.0f}",
                        "prev_close": f"{prev_close:,.0f}",
                        "cur_price": f"{cur_price:,.0f}",
                        "dif": f"{diff:,.0f}",
                        "dif_rt(%)": f"{diff_rate * 100:.2f}",
                        "prdy_ctrt(%)": f"{prdy_rate * 100:.2f}",
                    }
                )
            _print_table(
                rows,
                ["code", "name", "buy_pr", "prev_close", "cur_price", "dif", "dif_rt(%)", "prdy_ctrt(%)"],
                {
                    "buy_pr": "right",
                    "prev_close": "right",
                    "cur_price": "right",
                    "dif": "right",
                    "dif_rt(%)": "right",
                    "prdy_ctrt(%)": "right",
                },
            )
            for _ in range(10):
                if stop_event.is_set():
                    break
                time.sleep(0.1)

    th = threading.Thread(target=_loop, daemon=True)
    th.start()

    print("[모드23] 종료하려면 Enter를 누르세요.")
    while not stop_event.is_set():
        ready, _, _ = select.select([sys.stdin], [], [], 1)
        if ready:
            sys.stdin.readline()
            stop_event.set()
            break
    th.join(timeout=1.0)


def _print_account_holdings_snapshot(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_balance: str,
    tag: str,
) -> None:
    try:
        holdings, summary = _collect_holdings_snapshot(client, cano, acnt_prdt_cd, tr_id_balance)
    except Exception as e:
        print(f"[잔고조회 실패] {tag} error={e}")
        return

    dnca = _to_float(_pick(summary, "dnca_tot_amt", default=0))
    ord_cash = _to_float(_pick(summary, "prvs_rcdl_excc_amt", "ord_psbl_cash", default=0))
    pchs_amt = _to_float(_pick(summary, "pchs_amt_smtl_amt", default=0))
    evlu_amt = _to_float(_pick(summary, "evlu_amt_smtl_amt", default=0))
    evlu_pfls = _to_float(_pick(summary, "evlu_pfls_smtl_amt", default=0))
    evlu_rt = _rate_to_ratio(_pick(summary, "evlu_pfls_rt", "evlu_pfls_rate", default=0))
    if evlu_rt == 0 and pchs_amt > 0:
        evlu_rt = evlu_pfls / pchs_amt

    print(f"[계좌정보] {tag}")
    print(
        f"- 인출가능한 금액={dnca:,.0f} 주문가능한 금액={ord_cash:,.0f} "
        f"매수금액={pchs_amt:,.0f} 평가금액={evlu_amt:,.0f} "
        f"실수익액={evlu_pfls:,.0f} 실수익등락율={evlu_rt:.2%}"
    )

    if not holdings:
        print("[보유종목] 없음")
        return

    rows = []
    for h in holdings:
        qty = _to_int(h.get("qty", 0))
        cur_price = _to_float(h.get("cur_price", 0))
        buy_price = _to_float(h.get("buy_price", 0))
        eval_amt_h = _to_float(h.get("eval_amt", 0))
        if eval_amt_h <= 0 and qty > 0 and cur_price > 0:
            eval_amt_h = qty * cur_price
        diff = cur_price - buy_price if buy_price > 0 else 0.0
        diff_rate = diff / buy_price if buy_price > 0 else 0.0
        rows.append(
            {
                "code": _to_str(h.get("code", "")),
                "name": _to_str(h.get("name", "")),
                "qty": f"{qty:,d}",
                "cur_price": f"{cur_price:,.0f}",
                "eval_amt": f"{eval_amt_h:,.0f}",
                "buy_price": f"{buy_price:,.0f}",
                "diff": f"{diff:,.0f}",
                "diff_rate": f"{diff_rate:.2%}",
            }
        )
    print("[보유종목]")
    _print_table(
        rows,
        ["code", "name", "qty", "cur_price", "eval_amt", "buy_price", "diff", "diff_rate"],
        {
            "qty": "right",
            "cur_price": "right",
            "eval_amt": "right",
            "buy_price": "right",
            "diff": "right",
            "diff_rate": "right",
        },
    )


def _run_holdings_flow(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_balance: str,
    tr_id_sell: str,
) -> None:
    ctx_fk = ""
    ctx_nk = ""
    page_idx = 1
    seen_codes: set[str] = set()
    while True:
        rows, summary, ctx_fk, ctx_nk = _get_balance_page(
            client, cano, acnt_prdt_cd, tr_id_balance, ctx_fk=ctx_fk, ctx_nk=ctx_nk
        )
        if not rows:
            if page_idx == 1:
                print("[잔고] 보유 종목이 없습니다.")
            break

        holdings = []
        for row in rows:
            qty = _to_int(_pick(row, "hldg_qty", default=0))
            if qty <= 0:
                continue
            code = _to_str(_pick(row, "pdno", default=""))
            name = _to_str(_pick(row, "prdt_name", default=""))
            cur_price = _to_float(_pick(row, "prpr", default=0))
            eval_amt = _to_float(_pick(row, "evlu_amt", default=0))
            holdings.append(
                {
                    "code": code,
                    "name": name,
                    "qty": qty,
                    "price": cur_price,
                    "eval_amt": eval_amt,
                }
            )

        if not holdings:
            if not ctx_fk and not ctx_nk:
                break
            page_idx += 1
            continue

        rows = _format_holdings_rows(holdings)
        if not rows:
            break
        current_codes = set(r["code"] for r in rows)
        new_codes = current_codes - seen_codes
        if not new_codes and page_idx > 1:
            print("[잔고] 동일 페이지 반복 감지 -> 조회 종료")
            break
        seen_codes.update(current_codes)
        print(f"[잔고] 보유 종목 (page {page_idx})")
        _print_table(
            rows,
            ["code", "name", "qty", "price", "eval_amt"],
            {"qty": "right", "price": "right", "eval_amt": "right"},
        )

        holdings = rows
        did_sell = False
        while True:
            page_action = input("[페이지] 1.매도 2.보유 3.모두 매도 4.모두 보유 선택: ").strip()
            if page_action in ("1", "2", "3", "4"):
                break

        if page_action == "4":
            pass
        elif page_action == "3":
            for h in holdings:
                code = h["code"]
                name = h["name"] or "-"
                qty = _to_int(h.get("qty", 0))
                if not code or qty <= 0:
                    continue
                try:
                    resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, qty)
                    order_no = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                    price = _to_float(h.get("price", 0))
                    print(f"[매도] {code} {name} qty={qty} 주문완료 odno={order_no}")
                    _append_trade_log(
                        action="SELL",
                        code=code,
                        name=name,
                        qty=qty,
                        price=price,
                        amount=qty * price,
                        reason="manual_sell_all",
                        ordno=order_no,
                    )
                    did_sell = True
                except Exception as e:
                    print(f"[매도실패] {code} {name} qty={qty} error={e}")
        else:
            for h in holdings:
                code = h["code"]
                name = h["name"] or "-"
                qty = _to_int(h.get("qty", 0))
                if not code or qty <= 0:
                    continue
                while True:
                    ans = input(f"[{code} {name}] 1.매도 2.보유 선택: ").strip()
                    if ans in ("1", "2"):
                        break
                if ans == "2":
                    continue
                try:
                    resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, qty)
                    order_no = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                    price = _to_float(h.get("price", 0))
                    print(f"[매도] {code} {name} qty={qty} 주문완료 odno={order_no}")
                    _append_trade_log(
                        action="SELL",
                        code=code,
                        name=name,
                        qty=qty,
                        price=price,
                        amount=qty * price,
                        reason="manual_sell",
                        ordno=order_no,
                    )
                    did_sell = True
                except Exception as e:
                    print(f"[매도실패] {code} {name} qty={qty} error={e}")

        if did_sell:
            time.sleep(3)
            ctx_fk = ""
            ctx_nk = ""
            page_idx = 1
            seen_codes = set()
            continue

        if not ctx_fk and not ctx_nk:
            break
        while True:
            go_next = input("[다음페이지] 1.계속 2.종료 선택: ").strip()
            if go_next in ("1", "2"):
                break
        if go_next == "2":
            break
        page_idx += 1


def _run_top_trading(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_balance: str,
    tr_id_buy: str,
    tr_id_sell: str,
    tr_id_open: str,
    tr_id_cancel: str,
    exclude_cash: float,
    record_path: str,
    run_mode: int,
) -> List[Dict[str, Any]]:
    now = _now_kst()
    if not _is_trading_day(now):
        print("[Top_Trading] 휴장일은 실행하지 않습니다.")
        return []

    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    closing_sell_time = now.replace(hour=15, minute=8, second=30, microsecond=0)
    closing_buy_time = now.replace(hour=15, minute=9, second=0, microsecond=0)
    closing_buy_end = now.replace(hour=15, minute=30, second=0, microsecond=0)
    after_hours_start = now.replace(hour=16, minute=0, second=0, microsecond=0)
    after_hours_end = now.replace(hour=18, minute=1, second=0, microsecond=0)
    monitor_end = now.replace(hour=15, minute=10, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now < market_open:
        _log("[Top_Trading] 09:00 이전 - 모니터링만 진행")
    if now >= market_close and not AFTER_HOURS_TEST:
        print("[Top_Trading] 거래시간(09:00~15:30) 외 입니다.")
        return []

    base_dir = os.path.dirname(os.path.abspath(__file__))
    parquet_path = os.path.join(base_dir, "data", "1d_data", "kis_1d_unified_parquet_DB.parquet")
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"parquet 파일이 없습니다: {parquet_path}")

    name_map = {}
    symbols_path = symbol_master_path(load_kis_data_layout())
    if os.path.exists(symbols_path):
        sdf = pd.read_parquet(symbols_path, columns=["code", "name"])
        name_map = dict(zip(sdf["code"].astype(str), sdf["name"].astype(str)))

    if run_mode == 4:
        _log(
            f"[Top_Trading] 옵션 - DISTRIB_BUY={DISTRIB_BUY}, Closing_Sell={Closing_Sell}, "
            f"TOP_TRADING_RET={TOP_TRADING_RET}, SELL_HOLD_RET={SELL_HOLD_RET}"
        )
        _log("[모니터링] 스레드 기반 모니터링으로 실행합니다.")

    def get_holdings_snapshot() -> List[Dict[str, Any]]:
        rows, summary, _, _ = _get_balance_page(client, cano, acnt_prdt_cd, tr_id_balance)
        output1 = rows if isinstance(rows, list) else []
        holdings = []
        for row in output1:
            qty = _to_int(_pick(row, "hldg_qty", default=0))
            if qty <= 0:
                continue
            code = _to_str(_pick(row, "pdno", default=""))
            name = _to_str(_pick(row, "prdt_name", default=""))
            holdings.append({"code": code, "name": name, "qty": qty})
        return holdings

    def get_open_order_codes() -> set[str]:
        if not tr_id_open:
            return set()
        try:
            orders = _inquire_psbl_rvsecncl(
                client, cano, acnt_prdt_cd, tr_id_open, timeout_sec=1.0
            )[0]
        except Exception as e:
            _log(f"[open_order] 조회 실패(보유중복 제외용): {e}")
            return set()
        return {
            _to_str(_pick(o, "pdno", default=""))
            for o in orders
            if _to_str(_pick(o, "pdno", default=""))
        }

    def _format_open_order_rows(
        orders: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[str], Dict[str, str]]:
        if not orders:
            return [], [], {}
        df = pd.DataFrame(orders)
        cols = [
            c
            for c in [
                "odno",
                "pdno",
                "prdt_name",
                "ord_dvsn_name",
                "ord_qty",
                "psbl_qty",
                "ord_unpr",
                "ord_tmd",
            ]
            if c in df.columns
        ]
        rows = df[cols].to_dict("records") if cols else df.to_dict("records")
        align = {"ord_qty": "right", "psbl_qty": "right", "ord_unpr": "right", "ord_tmd": "right"}
        return rows, cols if cols else list(rows[0].keys()), align

    def _print_open_orders(title: str, orders: List[Dict[str, Any]]) -> None:
        _log(title)
        if not orders:
            _log("[open_order] 없음")
            return
        rows, cols, align = _format_open_order_rows(orders)
        if rows:
            _print_table(rows, cols, align)

    def _cancel_open_orders_now(tag: str) -> None:
        if not tr_id_open or not tr_id_cancel:
            _log(f"[주문점검] {tag} 미체결 조회/취소 tr_id 누락")
            return
        try:
            orders, _ = _inquire_psbl_rvsecncl(client, cano, acnt_prdt_cd, tr_id_open)
        except Exception as e:
            _log(f"[주문점검] {tag} 미체결 조회 실패: {e}")
            return
        targets = [o for o in orders if _to_int(_pick(o, "psbl_qty", default=0)) > 0]
        if not targets:
            _log(f"[주문점검] {tag} 미체결 없음")
            return
        for o in targets:
            ordno = _to_str(_pick(o, "odno", "ordno", default=""))
            code = _to_str(_pick(o, "pdno", default=""))
            qty = _to_int(_pick(o, "psbl_qty", default=0))
            if not ordno or not code or qty <= 0:
                continue
            try:
                _cancel_order(client, cano, acnt_prdt_cd, tr_id_cancel, ordno, code, qty)
                _log(f"[미체결취소] {code} ordno={ordno} qty={qty}")
            except Exception as e:
                _log(f"[미체결취소실패] {code} ordno={ordno} error={e}")

    def _cancel_open_orders_after_wait(tag: str, wait_sec: int = 300, schedule_only: bool = False) -> None:
        if not tr_id_open or not tr_id_cancel:
            _log(f"[주문점검] {tag} 미체결 조회/취소 tr_id 누락")
            return
        if schedule_only:
            pending_cancels.append({"tag": tag, "run_at": _now_kst() + timedelta(seconds=wait_sec)})
            _log(f"[주문점검] {tag} {wait_sec//60}분 대기 예약")
            return
        _log(f"[주문점검] {tag} {wait_sec//60}분 대기 후 미체결 취소 확인")
        time.sleep(wait_sec)
        try:
            orders, _ = _inquire_psbl_rvsecncl(client, cano, acnt_prdt_cd, tr_id_open)
        except Exception as e:
            _log(f"[주문점검] {tag} 미체결 조회 실패: {e}")
            return
        targets = [o for o in orders if _to_int(_pick(o, "psbl_qty", default=0)) > 0]
        if not targets:
            _log(f"[주문점검] {tag} 미체결 없음")
            _print_account_holdings_snapshot(
                client, cano, acnt_prdt_cd, tr_id_balance, f"{tag} 주문점검 후"
            )
            return
        for o in targets:
            ordno = _to_str(_pick(o, "odno", "ordno", default=""))
            code = _to_str(_pick(o, "pdno", default=""))
            qty = _to_int(_pick(o, "psbl_qty", default=0))
            if not ordno or not code or qty <= 0:
                continue
            try:
                _cancel_order(client, cano, acnt_prdt_cd, tr_id_cancel, ordno, code, qty)
                _log(f"[미체결취소] {code} ordno={ordno} qty={qty}")
            except Exception as e:
                _log(f"[미체결취소실패] {code} ordno={ordno} error={e}")
        # 취소 후 미체결 목록 CSV 저장
        ts = _now_kst().strftime("%Y%m%d_%H%M%S")
        out_dir = os.path.join(load_kis_data_layout().local_root, "logs")
        os.makedirs(out_dir, exist_ok=True)
        csv_path = os.path.join(out_dir, f"open_orders_{tag}_{ts}.csv")
        df_orders = pd.DataFrame(targets)
        df_orders.to_csv(csv_path, index=False, encoding="utf-8-sig")
        _log(f"[주문점검] {tag} 미체결 CSV 저장: {csv_path}")
        _print_account_holdings_snapshot(
            client, cano, acnt_prdt_cd, tr_id_balance, f"{tag} 주문점검 후"
        )

    def select_top_gainers_buy_candidates(
        exclude_codes: set[str], top_n: int = 50
    ) -> pd.DataFrame:
        rows = _fetch_fluctuation_top(client, top_n=top_n, market_div=client.cfg.market_div)
        items: List[Dict[str, Any]] = []
        for row in rows:
            code = _to_str(_pick(row, "stck_shrn_iscd", "stck_cd", "code", default=""))
            if not code or code in exclude_codes:
                continue
            name = _to_str(_pick(row, "hts_kor_isnm", "stck_name", "name", default=""))
            ret_ratio = _rate_to_ratio(_pick(row, "prdy_ctrt", "ctrt", "rate", default=0))
            if ret_ratio < TOP_TRADING_RET or ret_ratio >= 0.33:
                continue
            cur_price = _to_float(_pick(row, "stck_prpr", "prpr", "price", default=0))
            prev_close = _to_float(_pick(row, "prdy_clpr", "prev_close", default=0))
            if prev_close <= 0 and cur_price > 0 and ret_ratio > -0.999:
                prev_close = cur_price / (1 + ret_ratio)
            if max(prev_close, cur_price) < 500:
                continue
            value = _to_float(_pick(row, "acml_tr_pbmn", "acml_tr_pbmn_amt", "value", default=0))
            items.append(
                {
                    "symbol": code,
                    "name": name or name_map.get(code, ""),
                    "ret": ret_ratio,
                    "prev_close": prev_close,
                    "cur_price": cur_price,
                    "value": value,
                }
            )
        if not items:
            return pd.DataFrame()
        return pd.DataFrame(items).sort_values("ret", ascending=False)

    def get_d2_cash() -> float:
        rows, summary, _, _ = _get_balance_page(client, cano, acnt_prdt_cd, tr_id_balance)
        summary_rows = summary if isinstance(summary, list) else [summary]
        if not summary_rows:
            return 0.0
        item = summary_rows[0]
        ord_cash = _to_float(_pick(item, "ord_psbl_cash", "prvs_rcdl_excc_amt", default=0))
        return max(0.0, ord_cash - exclude_cash)

    def place_equal_buys(
        candidates: pd.DataFrame, total_cash: float, reason: str
    ) -> List[Dict[str, Any]]:
        orders = []
        if candidates.empty or total_cash <= 0:
            return orders
        remain = total_cash
        symbols = []
        prev_closes = []
        values = []
        cur_prices = []
        for _, row in candidates.iterrows():
            symbol = str(row["symbol"])
            prev_close = _to_float(row.get("prev_close", 0))
            value = _to_float(row.get("value", 0))
            try:
                inquire = client.inquire_price(symbol)
                cur_price = _to_float(_pick(inquire, "stck_prpr", "prpr", default=0))
            except Exception:
                cur_price = 0.0
            symbols.append(symbol)
            prev_closes.append(prev_close)
            values.append(value)
            cur_prices.append(cur_price)

        max_limits = [
            _max_buy_limit(cp, val) if cp > 0 else 0.0 for cp, val in zip(cur_prices, values)
        ]
        allocs = _allocate_cash_by_max(total_cash, max_limits)

        for symbol, prev_close, cur_price, cash_for in zip(symbols, prev_closes, cur_prices, allocs):
            # 주문 직전 잔고 재확인
            remain = min(remain, max(0.0, get_d2_cash()))
            if remain <= 0:
                break
            if cur_price <= 0 or cash_for <= 0:
                continue
            if cash_for > remain:
                cash_for = remain
            cash_for = max(0.0, cash_for - 1000)
            price_for = None
            unit_price = cur_price
            qty = int(cash_for // unit_price)
            if qty <= 0:
                continue
            if qty * unit_price > remain:
                qty = int(remain // unit_price)
                if qty <= 0:
                    continue
            try:
                resp = _buy_order(client, cano, acnt_prdt_cd, tr_id_buy, symbol, qty, price_for)
                odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                orders.append(
                    {
                        "symbol": symbol,
                        "qty": qty,
                        "price": unit_price,
                        "limit_up": False,
                        "ordno": odno,
                    }
                )
                name = name_map.get(symbol, "")
                _log(f"[매수] {symbol} {name} qty={qty} amt={qty * unit_price:,.0f}")
                _record_buy_today(symbol, name, qty, unit_price)
                _append_trade_log(
                    action="BUY",
                    code=symbol,
                    name=name,
                    qty=qty,
                    price=unit_price,
                    amount=qty * unit_price,
                    reason=reason,
                    ordno=odno,
                )
                remain -= qty * unit_price
            except Exception as e:
                print(f"[매수실패] {symbol} qty={qty} error={e}")
                if "주문가능금액" in str(e):
                    try:
                        cur_cash = get_d2_cash()
                        _log(
                            f"[매수실패][잔액] {symbol} ord_psbl_cash={cur_cash:,.0f} "
                            f"qty={qty} price={unit_price:,.0f} req={qty * unit_price:,.0f}"
                        )
                        _cancel_open_orders_now("주문가능금액 초과")
                    except Exception:
                        pass
        return orders

    def run_additional_buy_30m() -> None:
        try:
            orders_now, _ = _inquire_psbl_rvsecncl(client, cano, acnt_prdt_cd, tr_id_open)
        except Exception as e:
            orders_now = []
            _log(f"[open_order] 조회 실패: {e}")
        _print_open_orders("[open_order] 분산매수 직전", orders_now)
        if orders_now:
            _cancel_open_orders_now("분산매수 직전")
        avail_cash = get_d2_cash()
        if avail_cash <= 0:
            _log("[추가매수] 가용자금이 없어 스킵")
            return
        _log("[추가매수] 필터: 등락률<0.33, 전일종가 또는 현재종가>=500 (시가=저가/0.29조건 미적용)")
        holdings_now = get_holdings_snapshot()
        exclude_codes = {h["code"] for h in holdings_now}
        open_order_codes = get_open_order_codes()
        exclude_codes.update(open_order_codes)
        cand_df = _top_gainers_candidates(
            client,
            exclude_codes,
            name_map,
            top_n=50,
            apply_filters=True,
            min_ret=None,
            max_ret=0.33,
            min_price=500.0,
            require_open_low=False,
        )
        if cand_df.empty:
            _log("[추가매수] 상위 50 종목 조건 충족 없음")
            return
        min_cash = 1_000_000
        if avail_cash < min_cash:
            _log("[추가매수] 잔고 100만원 미만 -> 최상위 1종목만 매수")
            top_df = cand_df.head(1).copy()
        else:
            top_df = cand_df.head(10).copy()
        _log(
            f"[추가매수] 상위 {len(top_df)}개, 최소 {min_cash:,.0f}원씩(마지막은 잔액 전부) 매수 시도"
        )
        ordered_any = False
        for i, (_, row) in enumerate(top_df.iterrows()):
            symbol = str(row["symbol"])
            cur_price = _to_float(row.get("cur_price", 0))
            prev_close = _to_float(row.get("prev_close", 0))
            ret_prev = (cur_price / prev_close) - 1 if prev_close > 0 else 0.0
            remaining = len(top_df) - i
            # 매 주문 전 가용자금 재조회로 초과 오류 최소화
            avail_cash = max(0.0, get_d2_cash() - exclude_cash)
            if remaining > 1 and avail_cash < min_cash:
                _log("[추가매수] 잔고 100만원 미만으로 잔여 주문 스킵")
                break
            if remaining <= 0 or cur_price <= 0 or avail_cash <= 0:
                continue
            if remaining == 1:
                _log("[추가매수] 마지막 주문: 5초 대기 후 잔고 재조회")
                time.sleep(5)
                avail_cash = max(0.0, get_d2_cash() - exclude_cash)
                _log(f"[추가매수] 마지막 주문 잔액 재조회: {avail_cash:,.0f}")
                spendable = avail_cash
            else:
                if avail_cash < min_cash:
                    break
                spendable = min_cash
            try:
                inquire = client.inquire_price(symbol)
                cur_price = _to_float(_pick(inquire, "stck_prpr", "prpr", default=cur_price))
            except Exception:
                pass
            # 시세 변동/수수료 여유 버퍼
            spendable = max(0.0, spendable * 0.995 - 1000)
            qty = int(spendable // cur_price)
            if qty <= 0:
                continue
            name = row.get("name", "")
            try:
                pre_cash = avail_cash
                exp_cash = max(0.0, pre_cash - qty * cur_price)
                _log(
                    f"[추가매수요청] {symbol} {name} qty={qty} price={cur_price:,.0f} "
                    f"amt={qty * cur_price:,.0f} prev_close={prev_close:,.0f} "
                    f"ret_prev={ret_prev:.3f} (조회잔액 {pre_cash:,.0f} -> 예상 {exp_cash:,.0f})"
                )
                resp = _buy_order(client, cano, acnt_prdt_cd, tr_id_buy, symbol, qty, None)
                odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                post_cash = get_d2_cash()
                _log(
                    f"[추가매수] {symbol} {name} qty={qty} price={cur_price:,.0f} "
                    f"amt={qty * cur_price:,.0f} prev_close={prev_close:,.0f} "
                    f"ret_prev={ret_prev:.3f} (잔액 {pre_cash:,.0f} -> {post_cash:,.0f})"
                )
                _log(f"[매수주문접수] {symbol} {name} ordno={odno}")
                avail_cash = max(0.0, avail_cash - qty * cur_price)
                _record_buy_today(symbol, name, qty, cur_price)
                _append_trade_log(
                    action="BUY",
                    code=symbol,
                    name=name,
                    qty=qty,
                    price=cur_price,
                    amount=qty * cur_price,
                    reason="top_gainers_30m",
                    ordno=odno,
                )
                ordered_any = True
            except Exception as e:
                try:
                    fail_cash = get_d2_cash()
                except Exception:
                    fail_cash = 0.0
                print(
                    f"[추가매수실패] {symbol} {name} qty={qty} price={cur_price:,.0f} "
                    f"amt={qty * cur_price:,.0f} 잔액={fail_cash:,.0f} error={e}"
                )
                if "주문가능금액을 초과" in str(e):
                    try:
                        cur_cash = get_d2_cash()
                        _log(
                            f"[추가매수실패][잔액] {symbol} ord_psbl_cash={cur_cash:,.0f} "
                            f"qty={qty} price={cur_price:,.0f} req={qty * cur_price:,.0f}"
                        )
                    except Exception:
                        pass
                    _schedule_retry_buy(symbol, name, delay_sec=300)
                    break
        if ordered_any:
            try:
                post_orders, _ = _inquire_psbl_rvsecncl(client, cano, acnt_prdt_cd, tr_id_open)
            except Exception as e:
                post_orders = []
                _log(f"[open_order] 조회 실패: {e}")
            _print_open_orders("[open_order] 30분 추가매수 직후", post_orders)
            _cancel_open_orders_after_wait("30분 추가매수", schedule_only=True)

    def filter_buyable_by_price_volume(
        candidates: pd.DataFrame, retry_after_sec: int = 60, require_open_low: bool = False
    ) -> pd.DataFrame:
        if candidates.empty:
            return candidates
        def _codes_preview(rows: list[pd.Series]) -> str:
            codes = [str(r["symbol"]) for r in rows[:20]]
            more = len(rows) - len(codes)
            return ", ".join(codes) + (f" +{more}" if more > 0 else "")
        rows = []
        retry_rows = []
        skip_prev_close = 0
        skip_no_price = 0
        skip_open_low = 0
        skip_price_lt_prev = 0
        fail_rows = []
        for _, row in candidates.iterrows():
            symbol = str(row["symbol"])
            prev_close = _to_float(row.get("prev_close", 0))
            if prev_close <= 0:
                prev_close = _to_float(row.get("pdy_close", 0))
            if prev_close <= 0:
                skip_prev_close += 1
                fail_rows.append(
                    {"code": symbol, "prev_close": prev_close, "cur_price": 0.0, "reason": "prev_close<=0"}
                )
                continue
            inquire = _inquire_price_retry(client, symbol)
            cur_price = _to_float(_pick(inquire, "stck_prpr", "prpr", default=0))
            volume = _to_float(_pick(inquire, "acml_vol", "volume", default=0))
            if require_open_low:
                open_price = _to_float(_pick(inquire, "stck_oprc", "oprc", default=0))
                low_price = _to_float(_pick(inquire, "stck_lwpr", "lwpr", default=0))
                if open_price <= 0 or low_price <= 0 or open_price != low_price:
                    skip_open_low += 1
                    fail_rows.append(
                        {
                            "code": symbol,
                            "prev_close": prev_close,
                            "cur_price": cur_price,
                            "reason": "open!=low",
                        }
                    )
                    continue
            if volume <= 0:
                retry_rows.append(row)
                continue
            if cur_price <= 0:
                skip_no_price += 1
                fail_rows.append(
                    {"code": symbol, "prev_close": prev_close, "cur_price": cur_price, "reason": "no_price"}
                )
                continue
            if cur_price >= prev_close:
                rows.append(row)
            else:
                skip_price_lt_prev += 1
                fail_rows.append(
                    {
                        "code": symbol,
                        "prev_close": prev_close,
                        "cur_price": cur_price,
                        "reason": "price<prev_close",
                    }
                )
        if rows:
            _log(f"[09:00 매수대상] 즉시조건 통과 {len(rows)}건: {_codes_preview(rows)}")
        if retry_rows:
            _log(f"[09:00 매수대상] 거래량 없음 재시도 {len(retry_rows)}건: {_codes_preview(retry_rows)}")
        if retry_rows and retry_after_sec > 0:
            time.sleep(retry_after_sec)
            for row in retry_rows:
                symbol = str(row["symbol"])
                prev_close = _to_float(row.get("prev_close", 0))
                if prev_close <= 0:
                    continue
                inquire = _inquire_price_retry(client, symbol)
                cur_price = _to_float(_pick(inquire, "stck_prpr", "prpr", default=0))
                volume = _to_float(_pick(inquire, "acml_vol", "volume", default=0))
                if require_open_low:
                    open_price = _to_float(_pick(inquire, "stck_oprc", "oprc", default=0))
                    low_price = _to_float(_pick(inquire, "stck_lwpr", "lwpr", default=0))
                    if open_price <= 0 or low_price <= 0 or open_price != low_price:
                        continue
                if volume > 0 and cur_price >= prev_close:
                    rows.append(row)
            if rows:
                _log(f"[09:00 매수대상] 재시도 통과 {len(rows)}건")
        if not rows:
            _log(
                "[09:00 매수대상] 조건 탈락 요약: "
                f"prev_close<=0 {skip_prev_close}, "
                f"open_low_fail {skip_open_low}, "
                f"no_price {skip_no_price}, "
                f"price<prev_close {skip_price_lt_prev}, "
                f"volume<=0 {len(retry_rows)}"
            )
            if fail_rows:
                table_rows = []
                for r in fail_rows[:20]:
                    table_rows.append(
                        {
                            "code": r["code"],
                            "prev_close": f"{_to_float(r['prev_close']):,.0f}",
                            "cur_price": f"{_to_float(r['cur_price']):,.0f}",
                            "reason": r["reason"],
                        }
                    )
                _print_table(
                    table_rows,
                    ["code", "prev_close", "cur_price", "reason"],
                    {"prev_close": "right", "cur_price": "right"},
                )
            return pd.DataFrame()
        return pd.DataFrame(rows)

    def sell_by_ret(positions: List[Dict[str, Any]], now_dt: datetime) -> None:
        for pos in positions:
            symbol = pos["symbol"]
            qty = int(pos["qty"])
            inquire = client.inquire_price(symbol)
            cur_price = _to_float(_pick(inquire, "stck_prpr", "prpr", default=0))
            prev_close = _to_float(_pick(inquire, "stck_prdy_clpr", "prdy_clpr", default=0))
            if prev_close <= 0 or cur_price <= 0:
                continue
            ret = (cur_price / prev_close) - 1
            limit_up = calc_limit_up_price(prev_close)
            if cur_price >= limit_up or ret >= 0.29:
                _log(
                    f"[종가매도보류] {symbol} qty={qty} cur={cur_price:,.0f} "
                    f"prev={prev_close:,.0f} ret={ret:.3f}"
                )
                continue
            try:
                resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, symbol, qty)
                sell_amt = qty * cur_price
                pnl_amt = (cur_price - prev_close) * qty
                name = name_map.get(symbol, "")
                odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                _log(
                    f"[종가매도] {symbol} {name} qty={qty} amt={sell_amt:,.0f} "
                    f"pnl={pnl_amt:,.0f} ret={ret:.3f} time={now_dt.strftime('%H:%M:%S')}"
                )
                _log(f"[종가매도접수] {symbol} {name} qty={qty}")
                _append_trade_log(
                    action="SELL",
                    code=symbol,
                    name=name,
                    qty=qty,
                    price=cur_price,
                    amount=sell_amt,
                    pnl=pnl_amt,
                    ret=ret,
                    reason="closing_sell",
                    ordno=odno,
                )
            except Exception as e:
                print(f"[매도실패] {symbol} qty={qty} error={e}")

    # 1) 오전 매수 (09:00~14:50 이전)
    positions: List[Dict[str, Any]] = []
    candidates_list: List[Dict[str, Any]] = []
    orders: List[Dict[str, Any]] = []
    closing_buy_candidates: List[Dict[str, Any]] = []
    closing_buy_active: Dict[str, Dict[str, Any]] = {}
    closing_buy_excluded: List[Dict[str, Any]] = []
    closing_buy_loaded = False
    closing_buy_last_log_ts = 0.0
    closing_buy_last_prices: Dict[str, float] = {}
    after_hours_targets: List[Dict[str, Any]] = []
    after_hours_orders: Dict[str, Dict[str, Any]] = {}
    after_hours_filled: Dict[str, Dict[str, Any]] = {}
    after_hours_sent = False
    after_hours_last_log_ts = 0.0
    after_hours_final_done = False
    after_hours_fallback_done = False
    after_hours_nxt_block: set[str] = set()
    after_hours_retry_done: set[str] = set()
    after_hours_next_attempt: Dict[str, datetime] = {}
    after_hours_last_slot: tuple[int, int] | None = None
    preopen_sell_orders: Dict[str, Dict[str, Any]] = {}
    last_preopen_ts = 0.0
    preopen_logged = False
    last_idle_phase = ""
    watchlist_logged = False
    today_str = now.strftime("%Y-%m-%d")
    record_dates = _load_disperse_record(record_path)

    preopen_start = now.replace(hour=8, minute=30, second=0, microsecond=0)
    precheck_time = now.replace(hour=8, minute=59, second=0, microsecond=0)
    open_time = market_open
    buy_time = now.replace(hour=9, minute=5, second=0, microsecond=0)

    holdings_snapshot: List[Dict[str, Any]] = []
    d2_cash = 0.0
    precheck_cash = 0.0

    precheck_done = False
    preopen_check_done = False
    gap_done = False
    buy_0905_done = False
    distrib_done = False
    closing_sell_done = False
    closing_buy_done = False
    closing_buy_check_done = False
    cancel_after_close_done = False

    next_top_buy = market_open + timedelta(minutes=30)
    pending_cancels: List[Dict[str, Any]] = []
    pending_balance_reports: List[Dict[str, Any]] = []
    pending_retry_buys: List[Dict[str, Any]] = []

    def _process_pending_cancels(now_dt: datetime) -> None:
        if not pending_cancels:
            return
        remaining = []
        for item in pending_cancels:
            run_at = item.get("run_at")
            if isinstance(run_at, datetime) and now_dt >= run_at:
                _cancel_open_orders_after_wait(item.get("tag", "주문점검"), wait_sec=0, schedule_only=False)
            else:
                remaining.append(item)
        pending_cancels[:] = remaining

    def _schedule_balance_report(tag: str, delay_sec: int = 10) -> None:
        pending_balance_reports.append(
            {"tag": tag, "run_at": _now_kst() + timedelta(seconds=delay_sec)}
        )

    def _process_pending_balance_reports(now_dt: datetime) -> None:
        if not pending_balance_reports:
            return
        remaining = []
        for item in pending_balance_reports:
            run_at = item.get("run_at")
            if isinstance(run_at, datetime) and now_dt >= run_at:
                _print_account_holdings_snapshot(
                    client, cano, acnt_prdt_cd, tr_id_balance, item.get("tag", "잔고조회")
                )
            else:
                remaining.append(item)
        pending_balance_reports[:] = remaining

    def _schedule_retry_buy(symbol: str, name: str, delay_sec: int = 300) -> None:
        pending_retry_buys.append(
            {"symbol": symbol, "name": name, "run_at": _now_kst() + timedelta(seconds=delay_sec)}
        )
        _log(f"[추가매수] 재시도 예약 {symbol} {name} ({delay_sec//60}분 후)")

    def _process_pending_retry_buys(now_dt: datetime) -> None:
        if not pending_retry_buys:
            return
        remaining = []
        for item in pending_retry_buys:
            run_at = item.get("run_at")
            if not isinstance(run_at, datetime) or now_dt < run_at:
                remaining.append(item)
                continue
            symbol = _to_str(item.get("symbol", ""))
            name = _to_str(item.get("name", ""))
            if not symbol:
                continue
            holdings_now = get_holdings_snapshot()
            if symbol in {h["code"] for h in holdings_now}:
                continue
            if symbol in get_open_order_codes():
                continue
            try:
                inquire = client.inquire_price(symbol)
                cur_price = _to_float(_pick(inquire, "stck_prpr", "prpr", default=0))
            except Exception:
                cur_price = 0.0
            if cur_price <= 0:
                continue
            avail_cash = max(0.0, get_d2_cash() - exclude_cash)
            if avail_cash <= 0:
                continue
            spendable = max(0.0, min(avail_cash, 1_000_000) * 0.995 - 1000)
            qty = int(spendable // cur_price)
            if qty <= 0:
                continue
            try:
                resp = _buy_order(client, cano, acnt_prdt_cd, tr_id_buy, symbol, qty, None)
                odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                _log(
                    f"[추가매수재시도] {symbol} {name} qty={qty} price={cur_price:,.0f} "
                    f"amt={qty * cur_price:,.0f} ordno={odno}"
                )
                _record_buy_today(symbol, name, qty, cur_price)
                _append_trade_log(
                    action="BUY",
                    code=symbol,
                    name=name,
                    qty=qty,
                    price=cur_price,
                    amount=qty * cur_price,
                    reason="top_gainers_30m_retry",
                    ordno=odno,
                )
            except Exception as e:
                _log(f"[추가매수재시도실패] {symbol} {name} qty={qty} error={e}")
            # 재시도는 1회만
        pending_retry_buys[:] = remaining

    def run_gap_down_sell() -> None:
        nonlocal holdings_snapshot, d2_cash
        if not holdings_snapshot:
            holdings_snapshot = get_holdings_snapshot()
        for h in holdings_snapshot:
            code = h["code"]
            qty = h["qty"]
            if not code or qty <= 0:
                continue
            open_p = 0.0
            prev_close = 0.0
            gap_deadline = _now_kst().replace(hour=9, minute=4, second=59, microsecond=0)
            while _now_kst() <= gap_deadline:
                try:
                    inquire = client.inquire_price(code)
                except Exception:
                    inquire = None
                if inquire:
                    open_p = _to_float(_pick(inquire, "stck_oprc", "oprc", "open", default=0))
                    prev_close = _to_float(
                        _pick(inquire, "stck_prdy_clpr", "prdy_clpr", default=0)
                    )
                if open_p > 0:
                    break
                time.sleep(10)
            if open_p <= 0:
                continue
            if prev_close > 0 and open_p < prev_close:
                try:
                    resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, qty)
                    name = h.get("name", "")
                    odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                    _log(f"[갭하락매도] {code} {name} qty={qty} open={open_p:,.0f}")
                    _append_trade_log(
                        action="SELL",
                        code=code,
                        name=name,
                        qty=qty,
                        price=open_p,
                        amount=qty * open_p,
                        reason="gap_down",
                        ordno=odno,
                    )
                except Exception as e:
                    print(f"[갭하락매도실패] {code} qty={qty} error={e}")
        time.sleep(60)
        _ = get_holdings_snapshot()
        d2_cash = get_d2_cash()
        _log(f"[Top_Trading] 09:01 잔고조회 완료 D+2 cash={d2_cash:,.0f}")

    def run_0905_buy() -> None:
        nonlocal positions, d2_cash
        # 초기 분산매수에서는 상승률 상위 50 매수는 적용하지 않음
        return

    def run_distrib_buy(now_dt: datetime) -> None:
        nonlocal positions, d2_cash, watchlist_logged
        d2_cash = precheck_cash or get_d2_cash()
        print(f"[Top_Trading] D+2 cash: {d2_cash:,.0f}")
        _log(f"[Top_Trading] 09:00 분산매수 시작 (date={today_str})")
        candidates = _select_candidates(
            client,
            parquet_path,
            name_map,
            mode=1,
            min_ret=TOP_TRADING_RET,
            top_n=50,
            emit=True,
        )
        _log("[Top_Trading] 분산매수 후보: 전일 Parquet_DB mode=1")
        if not watchlist_logged:
            _log("[watchlist] 초기 종목선정 결과(전체 필드) 출력")
            if candidates.empty:
                print("[watchlist] 없음")
            else:
                print(candidates)
                print(list(candidates.columns))
            watchlist_logged = True
        require_open_low = False
        _log("[Top_Trading] 분산매수 조건: 거래량>0, 현재가>=전일종가 (시가=저가 미적용)")
        buyable = filter_buyable_by_price_volume(
            candidates, retry_after_sec=60, require_open_low=require_open_low
        )
        if buyable.empty:
            _log(f"[Top_Trading] 분산매수 조건 충족 종목 없음 ({now_dt.strftime('%H:%M')})")
        else:
            positions = place_equal_buys(buyable, d2_cash, "distrib_buy_am")
        _log("[Top_Trading] 09:00 분산매수 완료")
        record_dates.add(today_str)
        _save_disperse_record(record_path, record_dates)

    def _closing_buy_orders_path(now_dt: datetime) -> str:
        out_dir = "/home/ubuntu/Stoc_Kis/data/orders"
        os.makedirs(out_dir, exist_ok=True)
        return os.path.join(out_dir, f"closing_buy_orders_{now_dt.strftime('%Y%m%d')}.json")

    def _save_closing_buy_orders(now_dt: datetime, orders: Dict[str, Dict[str, Any]]) -> None:
        path = _closing_buy_orders_path(now_dt)
        payload = {
            "ts": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "orders": list(orders.values()),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def _load_closing_buy_orders(now_dt: datetime) -> Dict[str, Dict[str, Any]]:
        path = _closing_buy_orders_path(now_dt)
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            out = {}
            for o in payload.get("orders", []):
                code = _to_str(o.get("code", ""))
                if code:
                    out[code] = o
            return out
        except Exception:
            return {}

    def monitor_preopen_auction(now_dt: datetime) -> None:
        nonlocal preopen_sell_orders, last_preopen_ts, preopen_logged
        if now_dt < preopen_start or now_dt >= open_time:
            return
        if time.time() - last_preopen_ts < 1.0:
            return
        last_preopen_ts = time.time()

        try:
            holdings, _ = _collect_holdings_snapshot(
                client,
                cano,
                acnt_prdt_cd,
                tr_id_balance,
                max_pages=1,
                page_sleep_sec=0.0,
                timeout_sec=2.0,
            )
        except Exception:
            holdings = []
        if not holdings:
            return

        if not preopen_logged:
            _log(f"[동시호가] 보유종목 {len(holdings)}건 (08:30~09:00)")
            rows = []
            for h in holdings:
                rows.append(
                    {
                        "code": _to_str(h.get("code", "")),
                        "name": _to_str(h.get("name", "")),
                        "qty": f"{_to_int(h.get('qty', 0)):,d}",
                    }
                )
            if rows:
                _print_table(rows, ["code", "name", "qty"], {"qty": "right"})
            preopen_logged = True

        price_rows = []
        for h in holdings:
            code = _to_str(h.get("code", ""))
            qty = _to_int(h.get("sellable_qty", h.get("qty", 0)))
            if not code or qty <= 0:
                continue
            inquire = _inquire_price_retry(client, code)
            prev_close = _to_float(_pick(inquire, "stck_prdy_clpr", "prdy_clpr", default=0))
            cur_price = _to_float(_pick(inquire, "stck_prpr", "prpr", default=0))
            exp_price = _to_float(_pick(inquire, "antc_cnpr", "antc_prc", default=0))
            if exp_price <= 0:
                exp_price = cur_price
            if prev_close <= 0 or exp_price <= 0:
                continue

            diff = exp_price - prev_close
            diff_rate = diff / prev_close if prev_close > 0 else 0.0
            price_rows.append(
                {
                    "code": code,
                    "exp_price": f"{exp_price:,.0f}",
                    "prev_close": f"{prev_close:,.0f}",
                    "diff": f"{diff:,.0f}",
                    "diff_rate(%)": f"{diff_rate * 100:.2f}",
                }
            )

            if exp_price < prev_close:
                last_order = preopen_sell_orders.get(code, {})
                last_price = _to_float(last_order.get("price", 0))
                ordno = _to_str(last_order.get("ordno", ""))
                if ordno and abs(last_price - exp_price) < 1e-6:
                    continue
                if ordno and CURRENT_TR_ID_CANCEL:
                    try:
                        _cancel_order(client, cano, acnt_prdt_cd, CURRENT_TR_ID_CANCEL, ordno, code, qty)
                    except Exception as e:
                        _log(f"[동시호가정정실패] {code} ordno={ordno} error={e}")
                try:
                    resp = _sell_order(client, cano, acnt_prdt_cd, tr_id_sell, code, qty, exp_price)
                    new_ordno = _to_str(_pick(resp.get("output", {}) or {}, "ODNO", "odno", default=""))
                    preopen_sell_orders[code] = {"ordno": new_ordno, "price": exp_price}
                    _log(
                        f"[동시호가매도등록] {code} qty={qty} exp={exp_price:,.0f} prev={prev_close:,.0f}"
                    )
                except Exception as e:
                    _log(f"[동시호가매도실패] {code} qty={qty} error={e}")
            else:
                last_order = preopen_sell_orders.get(code, {})
                ordno = _to_str(last_order.get("ordno", ""))
                if ordno and CURRENT_TR_ID_CANCEL:
                    try:
                        _cancel_order(client, cano, acnt_prdt_cd, CURRENT_TR_ID_CANCEL, ordno, code, qty)
                        _log(f"[동시호가매도취소] {code} exp={exp_price:,.0f} prev={prev_close:,.0f}")
                    except Exception as e:
                        _log(f"[동시호가취소실패] {code} ordno={ordno} error={e}")
                    preopen_sell_orders.pop(code, None)

        if price_rows:
            _print_table(
                price_rows,
                ["code", "exp_price", "prev_close", "diff", "diff_rate(%)"],
                {"exp_price": "right", "prev_close": "right", "diff": "right", "diff_rate(%)": "right"},
            )

    def run_closing_buy() -> None:
        nonlocal closing_buy_candidates, closing_buy_active, closing_buy_excluded
        _log("[Top_Trading] 15:09 종가매수 후보 조회 시작(등락률 상위 200)")
        rows = _fetch_fluctuation_top(client, top_n=200, market_div=client.cfg.market_div)
        if not rows:
            _log("[Top_Trading] 15:09 후보 조회 결과 없음")
            return
        try:
            raw_df = pd.DataFrame(rows)
            raw_df["ts"] = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
            out_dir = os.path.join(load_kis_data_layout().local_root, "data", "logs")
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(
                out_dir, f"top200_1509_{_now_kst().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            raw_df.to_csv(out_path, index=False, encoding="utf-8-sig")
            _log(f"[Top_Trading] 15:09 상위 200 CSV 저장: {out_path}")
        except Exception as e:
            _log(f"[Top_Trading] 15:09 CSV 저장 실패: {e}")

        def _to_ratio(val: float) -> float:
            v = _to_float(val)
            return v / 100.0 if abs(v) > 2 else v

        try:
            holdings, _ = _collect_holdings_snapshot(
                client, cano, acnt_prdt_cd, tr_id_balance, max_pages=1
            )
            hold_codes = {_to_str(h.get("code", "")) for h in holdings if h.get("code")}
        except Exception:
            hold_codes = set()

        cand_rows: List[Dict[str, Any]] = []
        for r in rows:
            code = _to_str(_pick(r, "stck_shrn_iscd", "symbol", default=""))
            name = _to_str(_pick(r, "hts_kor_isnm", "name", default=""))
            ret = _to_ratio(_pick(r, "prdy_ctrt", "ret", default=0))
            cur_price = _to_float(_pick(r, "stck_prpr", "cur_price", default=0))
            prev_close = _to_float(_pick(r, "prdy_clpr", "prev_close", default=0))
            vol = _to_float(_pick(r, "acml_vol", "volume", default=0))
            db_row = stQueryDB(code)
            pdy_vol = _to_float(db_row.get("volume", 0))
            if prev_close <= 0:
                prev_close = _to_float(db_row.get("pdy_close", 0)) or _to_float(db_row.get("close", 0))
            if ret < 0.29 or cur_price < 500:
                continue
            if pdy_vol <= 0:
                continue
            if vol < pdy_vol * 2:
                continue
            if code in hold_codes:
                continue
            limit_up = calc_limit_up_price(prev_close) if prev_close > 0 else 0.0
            limit_up_hit = "Y" if limit_up > 0 and cur_price >= limit_up else "N"
            cand_rows.append(
                {
                    "code": code,
                    "name": name,
                    "ret": ret,
                    "prev_close": prev_close,
                    "cur_price": cur_price,
                    "volume": vol,
                    "pdy_vol": pdy_vol,
                    "order_id": "",
                    "limit_up_hit": limit_up_hit,
                }
            )

        if not cand_rows:
            _log("[Top_Trading] 15:09 조건 충족 종목 없음 (0.29+, 500원+, 거래량 2배)")
            return

        cand_rows.sort(key=lambda x: (x["ret"], x["code"]), reverse=True)
        closing_buy_candidates = cand_rows
        _log(f"[Top_Trading] 15:09 후보 {len(cand_rows)}건 (최대 10개 매수)")
        _print_table(
            cand_rows,
            ["code", "name", "ret", "limit_up_hit", "prev_close", "cur_price", "volume", "pdy_vol", "order_id"],
            {
                "ret": "right",
                "limit_up_hit": "right",
                "prev_close": "right",
                "cur_price": "right",
                "volume": "right",
                "pdy_vol": "right",
                "order_id": "right",
            },
        )

        selected = cand_rows[:10]
        d2_cash_local = get_d2_cash()
        if d2_cash_local <= 0:
            _log("[Top_Trading] 15:09 주문가능금액 없음")
            return
        per_cash = d2_cash_local / max(1, len(selected))
        closing_buy_active = {}
        for item in selected:
            code = item["code"]
            name = item["name"]
            prev_close = _to_float(item["prev_close"])
            limit_price = calc_limit_up_price(prev_close) if prev_close > 0 else 0.0
            if limit_price <= 0:
                closing_buy_excluded.append({**item, "reason": "no_limit_price"})
                continue
            qty = int(per_cash // limit_price)
            if qty <= 0:
                closing_buy_excluded.append({**item, "reason": "no_cash"})
                continue
            try:
                resp = _buy_order(client, cano, acnt_prdt_cd, tr_id_buy, code, qty, limit_price)
                odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                _log(
                    f"[종가매수요청] {code} {name} qty={qty} limit={limit_price:,.0f} ordno={odno}"
                )
                closing_buy_active[code] = {
                    **item,
                    "qty": qty,
                    "limit_price": limit_price,
                    "ordno": odno,
                    "status": "ordered",
                }
            except Exception as e:
                closing_buy_excluded.append({**item, "reason": f"order_fail:{e}"})
        if closing_buy_active:
            _save_closing_buy_orders(_now_kst(), closing_buy_active)
            active_rows = [
                {
                    "code": v["code"],
                    "name": v.get("name", ""),
                    "ret": v.get("ret", 0),
                    "limit_price": v.get("limit_price", 0),
                    "order_id": v.get("ordno", ""),
                }
                for v in closing_buy_active.values()
            ]
            _log("[종가매수요청] 주문 목록")
            _print_table(
                active_rows,
                ["code", "name", "ret", "limit_price", "order_id"],
                {"ret": "right", "limit_price": "right", "order_id": "right"},
            )
        else:
            _log("[종가매수요청] 주문 없음")
        if closing_buy_excluded:
            _log("[종가매수제외] 제외 사유 목록")
            _print_table(
                closing_buy_excluded,
                ["code", "name", "ret", "prev_close", "cur_price", "reason"],
                {"ret": "right", "prev_close": "right", "cur_price": "right"},
            )

    def monitor_closing_buy(now_dt: datetime) -> None:
        nonlocal closing_buy_candidates, closing_buy_active, closing_buy_excluded
        nonlocal closing_buy_last_log_ts, closing_buy_last_prices
        if not closing_buy_active:
            return
        replaced: List[Dict[str, Any]] = []
        for code, item in list(closing_buy_active.items()):
            inquire = _inquire_price_retry(client, code)
            exp_price = _to_float(_pick(inquire, "antc_cnpr", "antc_prc", default=0))
            prev_close = _to_float(_pick(inquire, "stck_prdy_clpr", "prdy_clpr", default=item.get("prev_close", 0)))
            exp_ret = (exp_price / prev_close - 1) if prev_close > 0 and exp_price > 0 else 0.0
            last_price = closing_buy_last_prices.get(code, 0.0)
            if exp_price > 0 and abs(exp_price - last_price) > 1e-6:
                _log(f"[종가매수변동] {code} exp={exp_price:,.0f} ret={exp_ret:.3f}")
                closing_buy_last_prices[code] = exp_price
            if exp_ret < 0.29:
                ordno = _to_str(item.get("ordno", ""))
                if ordno and CURRENT_TR_ID_CANCEL:
                    try:
                        _cancel_order(client, cano, acnt_prdt_cd, CURRENT_TR_ID_CANCEL, ordno, code, item["qty"])
                    except Exception as e:
                        _log(f"[종가매수취소실패] {code} ordno={ordno} error={e}")
                closing_buy_excluded.append({**item, "reason": f"exp_ret<{0.29:.2f}"})
                del closing_buy_active[code]
                replaced.append({"code": code, "reason": f"exp_ret={exp_ret:.3f}"})

        if replaced:
            _log("[종가매수제외] 예상체결 0.29 이탈 종목")
            _print_table(replaced, ["code", "reason"], {})

        # 신규 대체 종목 선정
        active_codes = set(closing_buy_active.keys())
        for item in closing_buy_candidates:
            code = item["code"]
            if code in active_codes:
                continue
            prev_close = _to_float(item.get("prev_close", 0))
            limit_price = calc_limit_up_price(prev_close) if prev_close > 0 else 0.0
            if limit_price <= 0:
                continue
            cash = get_d2_cash()
            qty = int((cash / max(1, len(closing_buy_active) or 1)) // limit_price)
            if qty <= 0:
                continue
            try:
                resp = _buy_order(client, cano, acnt_prdt_cd, tr_id_buy, code, qty, limit_price)
                odno = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                _log(f"[종가매수대체] {code} qty={qty} limit={limit_price:,.0f} ordno={odno}")
                closing_buy_active[code] = {
                    **item,
                    "qty": qty,
                    "limit_price": limit_price,
                    "ordno": odno,
                    "status": "ordered",
                }
            except Exception as e:
                closing_buy_excluded.append({**item, "reason": f"order_fail:{e}"})
            if len(closing_buy_active) >= 10:
                break
        if replaced:
            active_rows = [
                {
                    "code": v["code"],
                    "name": v.get("name", ""),
                    "ret": v.get("ret", 0),
                    "limit_price": v.get("limit_price", 0),
                    "ordno": v.get("ordno", ""),
                }
                for v in closing_buy_active.values()
            ]
            _log("[종가매수목록] 갱신")
            if active_rows:
                _print_table(
                    active_rows,
                    ["code", "name", "ret", "limit_price", "ordno"],
                    {"ret": "right", "limit_price": "right"},
                )
        if time.time() - closing_buy_last_log_ts >= 30:
            rows = []
            for code, item in closing_buy_active.items():
                rows.append(
                    {
                        "code": code,
                        "order_id": item.get("ordno", ""),
                        "limit_price": item.get("limit_price", 0),
                    }
                )
            _log("[종가매수모니터링] 진행중")
            if rows:
                _print_table(
                    rows,
                    ["code", "order_id", "limit_price"],
                    {"order_id": "right", "limit_price": "right"},
                )
            closing_buy_last_log_ts = time.time()
            if orders:
                next_day = _next_trading_day(_now_kst()).strftime("%Y-%m-%d")
                record_dates.add(next_day)
                _save_disperse_record(record_path, record_dates)
                try:
                    post_orders, _ = _inquire_psbl_rvsecncl(client, cano, acnt_prdt_cd, tr_id_open)
                except Exception as e:
                    post_orders = []
                    _log(f"[open_order] 조회 실패: {e}")
                _print_open_orders("[open_order] 14:50 분산매수 직후", post_orders)
                _cancel_open_orders_after_wait("14:50 매수", schedule_only=True)
        else:
            candidates_list = []
            orders = []

    def run_cancel_after_close() -> None:
        cancel_targets = [o for o in orders if o.get("limit_up") and o.get("ordno")]
        if cancel_targets:
            for o in cancel_targets:
                try:
                    _cancel_order(
                        client,
                        cano,
                        acnt_prdt_cd,
                        tr_id_cancel,
                        o["ordno"],
                        o["symbol"],
                        int(o["qty"]),
                    )
                    _log(f"[취소] {o['symbol']} ordno={o['ordno']}")
                except Exception as e:
                    print(f"[취소실패] {o['symbol']} ordno={o['ordno']} error={e
                    }")

            tried = {o["symbol"] for o in orders}
            remaining = [c for c in candidates_list if c["symbol"] not in tried]
            if remaining:
                cand_df = pd.DataFrame(remaining).sort_values("value", ascending=False)
                d2_cash_local = get_d2_cash()
                place_equal_buys(cand_df, d2_cash_local, "rebuy_after_cancel")

    def tick_hook(now_dt: datetime) -> bool:
        nonlocal precheck_done, gap_done, buy_0905_done, distrib_done
        nonlocal closing_sell_done, closing_buy_done, closing_buy_check_done, cancel_after_close_done
        nonlocal preopen_check_done, last_idle_phase, closing_buy_loaded
        nonlocal closing_buy_last_log_ts, closing_buy_last_prices
        nonlocal after_hours_targets, after_hours_orders, after_hours_sent
        nonlocal after_hours_last_log_ts, after_hours_final_done, after_hours_fallback_done
        nonlocal after_hours_nxt_block, after_hours_retry_done
        nonlocal after_hours_next_attempt, after_hours_last_slot
        nonlocal holdings_snapshot, d2_cash, next_top_buy, positions

        if now_dt >= precheck_time and not precheck_done:
            holdings_snapshot = get_holdings_snapshot()
            precheck_cash = get_d2_cash()
            d2_cash = precheck_cash
            _log(f"[Top_Trading] 08:59 계좌조회 완료 D+2 cash={precheck_cash:,.0f}")
            _print_balance_snapshot(client, cano, acnt_prdt_cd, tr_id_balance, exclude_cash)
            precheck_done = True

        if preopen_start <= now_dt < open_time:
            monitor_preopen_auction(now_dt)

        if now_dt >= open_time + timedelta(minutes=1) and not preopen_check_done:
            _log("[동시호가체결확인] 09:01 잔고조회")
            _print_balance_snapshot(client, cano, acnt_prdt_cd, tr_id_balance, exclude_cash)
            preopen_check_done = True

        cur_holdings = get_holdings_snapshot()
        if not cur_holdings:
            if now_dt < preopen_start:
                phase = "장 시작 대기중"
            elif now_dt < open_time:
                phase = "08:30~09:00 동시호가 감시중"
            elif now_dt < buy_time:
                phase = "09:00~09:05 갭하락 매도 확인중"
            elif now_dt < now_dt.replace(hour=14, minute=0, second=0, microsecond=0):
                phase = "장중 모니터링/저가돌파 대기중"
            elif now_dt < closing_sell_time:
                phase = "장중 모니터링 대기중"
            elif now_dt < closing_buy_time:
                phase = "종가매도 진행중"
            elif now_dt < closing_buy_end:
                phase = "종가매수 모니터링중"
            else:
                phase = "장마감 정리중"
            if phase != last_idle_phase:
                _log(f"[대기] 보유종목 없음. 현재 단계: {phase}")
                last_idle_phase = phase

        if not gap_done and now_dt >= open_time and now_dt < buy_time:
            run_gap_down_sell()
            gap_done = True

        if not buy_0905_done and now_dt >= buy_time:
            run_0905_buy()
            buy_0905_done = True

        if DISTRIB_BUY and not distrib_done and now_dt >= open_time:
            if today_str not in record_dates or d2_cash > 0:
                run_distrib_buy(now_dt)
            else:
                _log(f"[Top_Trading] 분산매수 skip (recorded {today_str})")
            distrib_done = True

        if ENABLE_ADD_BUY_30M and now_dt < closing_sell_time and now_dt >= next_top_buy:
            run_additional_buy_30m()
            next_top_buy = next_top_buy + timedelta(minutes=30)

        if not closing_sell_done and now_dt >= closing_sell_time:
            if positions:
                if Closing_Sell:
                    sell_by_ret(positions, now_dt)
                else:
                    _log("Closing_Sell = False로 종가매도는 스킵합니다.")
            closing_sell_done = True

        if not closing_buy_done and now_dt >= closing_buy_time and now_dt <= closing_buy_end:
            if Closing_Buy:
                run_closing_buy()
            else:
                _log("Closing_Buy = False로 종가매수는 스킵합니다.")
            closing_buy_done = True

        if Closing_Buy and now_dt >= closing_buy_time and now_dt <= closing_buy_end:
            monitor_closing_buy(now_dt)

        if Closing_Buy and not closing_buy_loaded and now_dt >= closing_buy_time:
            loaded = _load_closing_buy_orders(now_dt)
            if loaded:
                closing_buy_active.update(loaded)
                _log(f"[종가매수복구] 이전 주문 {len(loaded)}건 로드")
                try:
                    orders_now = _inquire_open_orders(client, cano, acnt_prdt_cd, tr_id_open)
                except Exception as e:
                    orders_now = []
                    _log(f"[종가매수복구] 미체결 조회 실패: {e}")
                open_ids = {_to_str(_pick(o, "odno", "ordno", default="")) for o in orders_now}
                rows = []
                for code, item in loaded.items():
                    ordno = _to_str(item.get("ordno", ""))
                    rows.append(
                        {
                            "code": code,
                            "order_id": ordno,
                            "open": "Y" if ordno in open_ids else "N",
                        }
                    )
                if rows:
                    _log("[종가매수복구] 주문 상태")
                    _print_table(rows, ["code", "order_id", "open"], {})
            closing_buy_loaded = True

        if (
            Closing_Buy
            and now_dt >= closing_buy_end + timedelta(minutes=1)
            and not closing_buy_check_done
        ):
            _log("[종가매수확인] 15:31 잔고조회/와치리스트 기록")
            _print_balance_snapshot(client, cano, acnt_prdt_cd, tr_id_balance, exclude_cash)
            try:
                holdings, _ = _collect_holdings_snapshot(
                    client, cano, acnt_prdt_cd, tr_id_balance, max_pages=1
                )
                hold_map = {
                    _to_str(h.get("code", "")): h for h in holdings if _to_str(h.get("code", ""))
                }
                target_list = list(closing_buy_active.values()) if closing_buy_active else []
                target_codes = [_to_str(v.get("code", "")) for v in target_list if v.get("code")]
                info_map = _load_latest_1d_info(target_codes) if target_codes else {}
                rows = []
                for item in target_list:
                    code = _to_str(item.get("code", ""))
                    if not code:
                        continue
                    info = info_map.get(code, {})
                    holding = hold_map.get(code, {})
                    rows.append(
                        {
                            "code": code,
                            "name": _to_str(item.get("name", "")),
                            "buy_price": _to_float(holding.get("buy_price", 0)),
                            "pdy_close": _to_float(info.get("pdy_close", 0)),
                            "R_avr": _to_float(info.get("R_avr", 0)),
                            "last_update_ts": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                            "source": "closing_buy",
                        }
                    )
                if rows:
                    out_dir = "/home/ubuntu/Stoc_Kis/data/watchlist"
                    os.makedirs(out_dir, exist_ok=True)
                    out_path = os.path.join(
                        out_dir, f"watchlist_{now_dt.strftime('%Y%m%d')}.parquet"
                    )
                    pd.DataFrame(rows).to_parquet(out_path, index=False)
                    _log(f"[watchlist] 저장 완료 {out_path}")
                after_hours_targets = [
                    v
                    for v in closing_buy_active.values()
                    if _to_str(v.get("code", "")) not in hold_map
                ]
                if after_hours_targets:
                    _log("[시간외대기] 미체결 종가매수 종목")
                    _print_table(
                        after_hours_targets,
                        ["code", "name", "ret", "limit_price", "order_id"],
                        {"ret": "right", "limit_price": "right", "order_id": "right"},
                    )
                else:
                    _log("[시간외대기] 미체결 종목 없음")
            except Exception as e:
                _log(f"[watchlist] 저장 실패 error={e}")
            closing_buy_check_done = True

        if Closing_Buy and now_dt >= closing_buy_end + timedelta(minutes=5) and not after_hours_fallback_done:
            if not after_hours_targets:
                _log("[시간외대기] 종가매수 미체결 없음 -> 15:35 상위 200 재조회")
                rows = _fetch_fluctuation_top(client, top_n=200, market_div=client.cfg.market_div)
                fallback: List[Dict[str, Any]] = []
                try:
                    holdings, _ = _collect_holdings_snapshot(
                        client, cano, acnt_prdt_cd, tr_id_balance, max_pages=1
                    )
                    hold_codes = {_to_str(h.get("code", "")) for h in holdings if h.get("code")}
                except Exception:
                    hold_codes = set()
                if rows:
                    for r in rows:
                        code = _to_str(_pick(r, "stck_shrn_iscd", "symbol", default=""))
                        name = _to_str(_pick(r, "hts_kor_isnm", "name", default=""))
                        ret = _to_float(_pick(r, "prdy_ctrt", "ret", default=0))
                        ret = ret / 100.0 if abs(ret) > 2 else ret
                        cur_price = _to_float(_pick(r, "stck_prpr", "cur_price", default=0))
                        vol = _to_float(_pick(r, "acml_vol", "volume", default=0))
                        db_row = stQueryDB(code)
                        pdy_vol = _to_float(db_row.get("volume", 0))
                        if ret < 0.29 or cur_price < 500:
                            continue
                        if pdy_vol <= 0 or vol < pdy_vol * 2:
                            continue
                        if code in hold_codes:
                            continue
                        fallback.append(
                            {
                                "code": code,
                                "name": name,
                                "ret": ret,
                                "prev_close": _to_float(db_row.get("pdy_close", 0)) or _to_float(db_row.get("close", 0)),
                                "cur_price": cur_price,
                                "volume": vol,
                                "pdy_vol": pdy_vol,
                                "limit_up_hit": "Y"
                                if cur_price >= calc_limit_up_price(_to_float(db_row.get("pdy_close", 0)) or _to_float(db_row.get("close", 0)))
                                else "N",
                            }
                        )
                fallback.sort(key=lambda x: (x["ret"], x["code"]), reverse=True)
                after_hours_targets = fallback[:10]
                if after_hours_targets:
                    _log("[시간외대기] 15:35 재조회 후보")
                    _print_table(
                        after_hours_targets,
                        ["code", "name", "ret", "limit_up_hit", "cur_price", "volume", "pdy_vol"],
                        {
                            "ret": "right",
                            "limit_up_hit": "right",
                            "cur_price": "right",
                            "volume": "right",
                            "pdy_vol": "right",
                        },
                    )
                else:
                    _log("[시간외대기] 15:35 재조회 후보 없음")
            after_hours_fallback_done = True

        if (
            after_hours_targets
            and after_hours_start <= now_dt < after_hours_end
            and now_dt.minute % 10 == 0
            and now_dt.second <= 1
        ):
            after_hours_sent = False

        if after_hours_targets and now_dt >= after_hours_start and not after_hours_sent:
            _log("[시간외단일가] 매수 주문 시작")
            per_cash = max(0.0, get_d2_cash()) / max(1, len(after_hours_targets))
            for item in after_hours_targets:
                code = _to_str(item.get("code", ""))
                name = _to_str(item.get("name", ""))
                if code in after_hours_nxt_block:
                    continue
                next_at = after_hours_next_attempt.get(code)
                if next_at and now_dt < next_at:
                    continue
                close_price = _to_float(item.get("cur_price", 0))
                if close_price <= 0:
                    close_price = _to_float(item.get("prev_close", 0))
                if close_price <= 0:
                    close_price = _to_float(stQueryDB(code).get("close", 0))
                if close_price <= 0:
                    continue
                qty = int(per_cash // close_price)
                if qty <= 0:
                    continue
                try:
                    resp = _buy_order_after_hours(
                        client, cano, acnt_prdt_cd, tr_id_buy, code, qty, close_price
                    )
                    ordno = _to_str(_pick(resp.get("output", {}) or {}, "ODNO", "odno", default=""))
                    after_hours_orders[code] = {
                        "code": code,
                        "name": name,
                        "qty": qty,
                        "price": close_price,
                        "order_id": ordno,
                    }
                    _log(
                        f"[시간외단일가매수] {code} {name} qty={qty} price={close_price:,.0f} ordno={ordno}"
                    )
                except Exception as e:
                    err_str = str(e)
                    _log(f"[시간외단일가매수실패] {code} {name} qty={qty} error={e}")
                    if "APBK3020" in err_str or "NXT거래종목" in err_str:
                        after_hours_nxt_block.add(code)
                        _log(f"[시간외단일가중지] {code} {name} NXT거래종목")
                    after_hours_next_attempt[code] = now_dt + timedelta(minutes=10)
            if after_hours_orders:
                _log("[시간외단일가] 매수주문 내역")
                rows = []
                for v in after_hours_orders.values():
                    qty = _to_int(v.get("qty", 0))
                    price = _to_float(v.get("price", 0))
                    rows.append(
                        {
                            "code": v.get("code", ""),
                            "name": v.get("name", ""),
                            "qty": qty,
                            "price": price,
                            "amount": qty * price,
                            "order_id": v.get("order_id", ""),
                        }
                    )
                _print_table(
                    rows,
                    ["code", "name", "qty", "price", "amount", "order_id"],
                    {"qty": "right", "price": "right", "amount": "right", "order_id": "right"},
                )
                try:
                    orders_now = _inquire_open_orders(client, cano, acnt_prdt_cd, tr_id_open)
                    open_ids = {_to_str(_pick(o, "odno", "ordno", default="")) for o in orders_now}
                except Exception as e:
                    open_ids = set()
                    _log(f"[시간외단일가] 주문조회 실패: {e}")
                status_rows = []
                for code, item in after_hours_orders.items():
                    ordno = _to_str(item.get("order_id", ""))
                    status = ""
                    status_raw = ""
                    if CURRENT_TR_ID_ORDER:
                        try:
                            od = _inquire_order_status(client, cano, acnt_prdt_cd, CURRENT_TR_ID_ORDER, ordno)
                            status_raw = _to_str(_pick(od, "ord_stat_name", "ord_stat", "order_status", default=""))
                        except Exception:
                            status_raw = ""
                    if ordno in open_ids or "접수" in status_raw:
                        status = "접수중"
                        after_hours_next_attempt[code] = now_dt + timedelta(minutes=20)
                    elif "체결" in status_raw or "완료" in status_raw:
                        status = "매수완료"
                    elif "거부" in status_raw or "취소" in status_raw:
                        status = "거절/취소"
                        after_hours_next_attempt[code] = now_dt + timedelta(minutes=10)
                    else:
                        try:
                            ccld = _find_ccld_by_ordno(client, cano, acnt_prdt_cd, ordno)
                        except Exception:
                            ccld = None
                        if ccld and _to_int(_pick(ccld, "ccld_qty", "tot_ccld_qty", "stck_cnt", default=0)) > 0:
                            status = "매수완료"
                        else:
                            status = "거절/취소"
                            after_hours_next_attempt[code] = now_dt + timedelta(minutes=10)
                    qty = _to_int(item.get("qty", 0))
                    price = _to_float(item.get("price", 0))
                    status_rows.append(
                        {
                            "code": code,
                            "name": item.get("name", ""),
                            "qty": qty,
                            "price": price,
                            "amount": qty * price,
                            "order_id": ordno,
                            "status": status,
                        }
                    )
                _log("[시간외단일가] 매수주문 결과")
                _print_table(
                    status_rows,
                    ["code", "name", "qty", "price", "amount", "order_id", "status"],
                    {"qty": "right", "price": "right", "amount": "right", "order_id": "right"},
                )
            after_hours_sent = True

        if after_hours_sent and now_dt < after_hours_end and time.time() - after_hours_last_log_ts >= 60:
            _log("[시간외단일가] 체결 모니터링중")
            try:
                orders_now = _inquire_open_orders(client, cano, acnt_prdt_cd, tr_id_open)
                open_ids = {_to_str(_pick(o, "odno", "ordno", default="")) for o in orders_now}
            except Exception as e:
                open_ids = set()
                _log(f"[시간외단일가] 미체결 조회 실패: {e}")
            rows = []
            for code, item in list(after_hours_orders.items()):
                ordno = _to_str(item.get("order_id", ""))
                open_flag = "Y" if ordno in open_ids else "N"
                status = "접수중" if open_flag == "Y" else "확인중"
                ccld = None
                if open_flag == "N" and ordno:
                    status_raw = ""
                    if CURRENT_TR_ID_ORDER:
                        try:
                            od = _inquire_order_status(client, cano, acnt_prdt_cd, CURRENT_TR_ID_ORDER, ordno)
                            status_raw = _to_str(_pick(od, "ord_stat_name", "ord_stat", "order_status", default=""))
                        except Exception:
                            status_raw = ""
                    if "체결" in status_raw or "완료" in status_raw:
                        status = "체결"
                    elif "거부" in status_raw or "취소" in status_raw:
                        status = "거절/취소"
                    else:
                        try:
                            ccld = _find_ccld_by_ordno(client, cano, acnt_prdt_cd, ordno)
                        except Exception:
                            ccld = None
                        if ccld:
                            status = "체결"
                        else:
                            status = "거절/취소"
                    if status == "체결":
                        after_hours_filled[code] = {
                            "code": code,
                            "name": item.get("name", ""),
                            "qty": _to_int(_pick(ccld or {}, "ccld_qty", "tot_ccld_qty", "stck_cnt", default=0)),
                            "price": _to_float(_pick(ccld or {}, "ccld_unpr", "stck_prpr", "ccld_prc", default=0)),
                            "order_id": ordno,
                        }
                rows.append({"code": code, "order_id": ordno, "open": open_flag, "status": status})
                if open_flag == "N":
                    after_hours_orders.pop(code, None)
            if rows:
                _print_table(rows, ["code", "order_id", "open", "status"], {})
            after_hours_last_log_ts = time.time()

            if not after_hours_orders and not after_hours_final_done:
                _log("[시간외단일가] 최종 체결내역")
                rows = []
                try:
                    holdings, _ = _collect_holdings_snapshot(
                        client, cano, acnt_prdt_cd, tr_id_balance, max_pages=1
                    )
                    hold_map = {
                        _to_str(h.get("code", "")): h for h in holdings if h.get("code")
                    }
                except Exception:
                    hold_map = {}
                retry_targets = []
                for v in after_hours_targets:
                    code = _to_str(v.get("code", ""))
                    filled = after_hours_filled.get(code, {})
                    hold = hold_map.get(code, {})
                    qty = _to_int(filled.get("qty", 0)) or _to_int(hold.get("qty", 0))
                    price = _to_float(filled.get("price", 0)) or _to_float(hold.get("cur_price", 0)) or _to_float(v.get("cur_price", 0))
                    if qty <= 0 and code not in after_hours_retry_done and now_dt < after_hours_end:
                        retry_targets.append(v)
                    rows.append(
                        {
                            "code": code,
                            "name": v.get("name", ""),
                            "qty": qty,
                            "price": price,
                            "amount": qty * price,
                        }
                    )
                _print_table(
                    rows,
                    ["code", "name", "qty", "price", "amount"],
                    {"qty": "right", "price": "right", "amount": "right"},
                )
                _print_balance_snapshot(client, cano, acnt_prdt_cd, tr_id_balance, exclude_cash)
                if retry_targets:
                    _log("[시간외단일가] 미체결 재주문")
                    for v in retry_targets:
                        code = _to_str(v.get("code", ""))
                        name = _to_str(v.get("name", ""))
                        price = _to_float(v.get("cur_price", 0))
                        if price <= 0:
                            price = _to_float(v.get("prev_close", 0))
                        if price <= 0:
                            price = _to_float(stQueryDB(code).get("close", 0))
                        if price <= 0:
                            continue
                        qty = int(max(0.0, get_d2_cash()) // price)
                        if qty <= 0:
                            continue
                        try:
                            resp = _buy_order_after_hours(
                                client, cano, acnt_prdt_cd, tr_id_buy, code, qty, price
                            )
                            ordno = _to_str(_pick(resp.get("output", {}) or {}, "ODNO", "odno", default=""))
                            after_hours_orders[code] = {
                                "code": code,
                                "name": name,
                                "qty": qty,
                                "price": price,
                                "order_id": ordno,
                            }
                            after_hours_retry_done.add(code)
                            _log(
                                f"[시간외단일가재주문] {code} {name} qty={qty} price={price:,.0f} ordno={ordno}"
                            )
                        except Exception as e:
                            _log(f"[시간외단일가재주문실패] {code} {name} error={e}")
                else:
                    _log_tm("종가 매수가 완료되어 프로그램을 종료합니다.")
                    after_hours_final_done = True

        if now_dt >= after_hours_end and after_hours_sent and not after_hours_final_done:
            _log("[시간외단일가] 18:01 최종 주문 결과 조회")
            _print_balance_snapshot(client, cano, acnt_prdt_cd, tr_id_balance, exclude_cash)
            try:
                orders_now = _inquire_open_orders(client, cano, acnt_prdt_cd, tr_id_open)
            except Exception as e:
                orders_now = []
                _log(f"[시간외단일가] 미체결 조회 실패: {e}")
            _print_open_orders("[시간외단일가] 최종 미체결", orders_now)
            after_hours_final_done = True

        if not cancel_after_close_done and now_dt >= market_close:
            run_cancel_after_close()
            cancel_after_close_done = True

        _process_pending_cancels(now_dt)
        _process_pending_balance_reports(now_dt)
        _process_pending_retry_buys(now_dt)

        if after_hours_final_done and after_hours_sent and not after_hours_orders:
            return False
        return True

    monitor_thread = threading.Thread(
        target=_monitor_holdings_simple,
        args=(
            client,
            cano,
            acnt_prdt_cd,
            tr_id_balance,
            tr_id_sell,
            tr_id_buy,
            exclude_cash,
            market_open,
            monitor_end,
            5.0,
        ),
        daemon=True,
    )
    monitor_thread.start()

    loop_end = max(market_close, after_hours_end)
    if AFTER_HOURS_TEST and _now_kst() >= market_close:
        loop_end = max(loop_end, _now_kst() + timedelta(minutes=10))
        _log("[Top_Trading] 장 마감 후 테스트 모드: 10분간 스케줄만 실행")

    while _now_kst() <= loop_end:
        now_dt = _now_kst()
        if not tick_hook(now_dt):
            break
        time.sleep(1)

    monitor_thread.join(timeout=1.0)
    return positions


def main() -> None:
    ap = argparse.ArgumentParser(description="보유 종목 조회 후 시장가 매도")
    ap.add_argument("--config", default="config.json", help="설정 파일 경로")
    ap.add_argument("--cano-key", default=None, help="config에서 계좌키 선택 (예: cano)")
    ap.add_argument("--account-id", default=None, help="accounts 하위 계정 ID 선택")
    ap.add_argument(
        "--run-mode",
        type=int,
        default=None,
        help="1:잔고 2:open id 23:보유종목(스레드) 21:주문내역조회(inquire-order) 22:open order 조회 3:종목 선정 4:Top_Trading+모니터링 5:웹소켓 테스트(전략 시뮬레이션) 6:Top_Trading+WebSocket",
    )
    args, _ = ap.parse_known_args()

    config_path = args.config
    if not isinstance(config_path, str):
        config_path = "config.json"
    if not config_path.startswith("/"):
        config_path = config_path.strip()
    if not config_path or config_path == "config.json":
        config_path = "config.json"
    if not config_path.startswith("/") and not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_path)

    cfg = resolve_account_config(config_path, account_id=args.account_id)
    appkey = cfg.get("appkey")
    appsecret = cfg.get("appsecret")
    if not appkey or not appsecret:
        raise ValueError("appkey/appsecret이 필요합니다. config.json에 설정하세요.")

    last_pid = cfg.get("last_pid")
    if last_pid:
        try:
            pid_int = int(str(last_pid))
            if pid_int != os.getpid():
                os.kill(pid_int, 0)
                print(f"[startup] 기존 프로세스 종료 시도 pid={pid_int}")
                os.kill(pid_int, signal.SIGTERM)
                for _ in range(10):
                    time.sleep(0.5)
                    try:
                        os.kill(pid_int, 0)
                    except ProcessLookupError:
                        break
                try:
                    os.kill(pid_int, 0)
                    print(f"[startup] 기존 프로세스 종료 확인 실패(살아있음) pid={pid_int}")
                except ProcessLookupError:
                    cfg.pop("last_pid", None)
                    cfg.pop("last_pid_time", None)
                    save_config(cfg, config_path)
                    print(f"[startup] 기존 프로세스 종료 완료 pid={pid_int}")
        except Exception:
            pass

    cano_key = _to_str(args.cano_key) or CANO_KEY
    cano = cfg.get(cano_key) or cfg.get(cano_key.upper())
    acnt_prdt_cd = cfg.get("acnt_prdt_cd") or cfg.get("ACNT_PRDT_CD")
    if not cano or not acnt_prdt_cd:
        available = sorted([k for k in cfg.keys() if k.lower().startswith("cano") or k.lower() == "cano"])
        raise ValueError(
            f"CANO/ACNT_PRDT_CD가 필요합니다. config.json에 설정하세요. 사용가능 키: {available}"
        )

    base_url = cfg.get("base_url") or DEFAULT_BASE_URL
    custtype = cfg.get("custtype") or "P"
    tr_id_balance = cfg.get("tr_id_balance")
    tr_id_sell = cfg.get("tr_id_sell")
    tr_id_buy = cfg.get("tr_id_buy")
    tr_id_cancel = cfg.get("tr_id_cancel")
    tr_id_open = cfg.get("tr_id_open")
    tr_id_daily = cfg.get("tr_id_daily")
    tr_id_order = cfg.get("tr_id_order")
    tr_id_balance = tr_id_balance or "TTTC8434R"
    tr_id_sell = tr_id_sell or "TTTC0801U"
    tr_id_buy = tr_id_buy or "TTTC0802U"
    tr_id_cancel = tr_id_cancel or "TTTC0803U"
    tr_id_open = tr_id_open or "TTTC0084R"
    tr_id_daily = tr_id_daily or "TTTC0081R"
    tr_id_order = tr_id_order or "TTTC8001R"
    exclude_cash = _to_float(cfg.get("exclude_cash", 0))
    record_path = cfg.get("disperse_record_path", DISPERSE_RECORD_PATH)

    global CURRENT_TR_ID_DAILY, CURRENT_TR_ID_CANCEL, CURRENT_TR_ID_BALANCE, CURRENT_TR_ID_ORDER
    CURRENT_TR_ID_DAILY = tr_id_daily
    CURRENT_TR_ID_CANCEL = tr_id_cancel
    CURRENT_TR_ID_BALANCE = tr_id_balance
    CURRENT_TR_ID_OPEN = tr_id_open
    CURRENT_TR_ID_ORDER = tr_id_order

    global _LOG_MANAGER, _TRADE_LOG_PATH
    layout = load_kis_data_layout(config_path)
    log_dir = os.path.join(layout.local_root, "logs")
    daily_name = f"KIS_log_{_now_kst().strftime('%y%m%d')}.log"
    run_tag = f"{os.path.basename(__file__)} 시작 { _now_kst().strftime('%Y-%m-%d %H:%M:%S') }"
    _LOG_MANAGER = LogManager(log_dir, log_name=daily_name, run_tag=run_tag)
    sys.stdout = TeeStdout(_LOG_MANAGER.log_file)
    _TRADE_LOG_PATH = os.path.join(log_dir, "trade_log.csv")

    cfg["last_pid"] = str(os.getpid())
    cfg["last_pid_time"] = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
    try:
        save_config(cfg, config_path)
    except Exception:
        pass

    _log_tm(f"{os.path.basename(__file__)} 시작 되었음.")
    _log(
        f"run_mode={args.run_mode if args.run_mode is not None else RUN_MODE}, "
        f"cano_key={cano_key}, exclude_cash={exclude_cash}"
    )

    ws_url = cfg.get("ws_url", WS_URL)
    ws_tr_id = cfg.get("ws_tr_id", WS_TR_ID)
    if ws_url:
        globals()["WS_URL"] = ws_url
    if ws_tr_id:
        globals()["WS_TR_ID"] = ws_tr_id

    kis_cfg = KisConfig(
        appkey=appkey,
        appsecret=appsecret,
        base_url=base_url,
        custtype=custtype,
    )
    client = KisClient(kis_cfg)

    run_mode = args.run_mode if args.run_mode is not None else RUN_MODE
    global CURRENT_RUN_MODE
    CURRENT_RUN_MODE = run_mode
    if run_mode == 1:
        try:
            rows, summary, _, _, raw = _get_balance_page(
                client, cano, acnt_prdt_cd, tr_id_balance, return_raw=True
            )
        except RuntimeError as e:
            print(str(e))
            return
        while True:
            print(f"[계좌] CANO={cano} ACNT_PRDT_CD={acnt_prdt_cd}")
            now_stamp = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{now_stamp}] ===============================")
            print("[잔고요약]")
            summary_rows = summary if isinstance(summary, list) else [summary]
            stock_eval_amt = 0.0
            if summary_rows:
                for item in summary_rows:
                    dnca = _to_float(_pick(item, "dnca_tot_amt", default=0))
                    ord_cash = _to_float(_pick(item, "prvs_rcdl_excc_amt", default=0))
                    tot_eval = _to_float(_pick(item, "tot_evlu_amt", default=0))
                    pchs_amt = _to_float(_pick(item, "pchs_amt_smtl_amt", default=0))
                    evlu_amt = _to_float(_pick(item, "evlu_amt_smtl_amt", default=0))
                    evlu_pfls = _to_float(_pick(item, "evlu_pfls_smtl_amt", default=0))
                    prev_tot_eval = _to_float(
                        _pick(item, "prvs_tot_evlu_amt", "bfdy_tot_evlu_amt", default=0)
                    )
                    stock_eval_amt = evlu_amt
                    total_calc = evlu_amt + evlu_pfls + ord_cash
                    raw_dnca = _pick(item, "dnca_tot_amt", default="N/A")
                    raw_ord = _pick(item, "prvs_rcdl_excc_amt", default="N/A")
                    print(f"♠ 출금가능액 : {dnca:,.0f}   ♠ 주문가능금액 : {ord_cash:,.0f}")
                    print(f"- 총 매입액(pchs_amt_smtl_amt): {pchs_amt:,.0f}")
                    print(f"- 주식 평가액(evlu_amt_smtl_amt)-(a): {evlu_amt:,.0f}")
                    print(f"- 주식 평가손익(evlu_pfls_smtl_amt)-(b): {evlu_pfls:,.0f}")
                    print(f"- 주문가능금액(prvs_rcdl_excc_amt)-(c): {ord_cash:,.0f}")
                    print(f"- 총 평가액(tot_evlu_amt)-(a+b+c): {tot_eval:,.0f}")
                    if prev_tot_eval > 0:
                        diff_total = tot_eval - prev_tot_eval
                        diff_rate = diff_total / prev_tot_eval
                        print(f"- 전일대비 총 증감 : {diff_total:,.0f}({diff_rate:+.1%})")
            else:
                print("- summary 없음")

            output1 = rows if isinstance(rows, list) else []
            holdings = []
            for row in output1:
                qty = _to_int(_pick(row, "hldg_qty", default=0))
                if qty <= 0:
                    continue
                code = _to_str(_pick(row, "pdno", default=""))
                name = _to_str(_pick(row, "prdt_name", default=""))
                cur_price = _to_float(_pick(row, "prpr", default=0))
                eval_amt = _to_float(_pick(row, "evlu_amt", default=0))
                buy_price = _to_float(_pick(row, "pchs_avg_pric", default=0))
                dif = cur_price - buy_price if buy_price > 0 else 0.0
                dif_rt = dif / buy_price * 100 if buy_price > 0 else 0.0
                holdings.append(
                    {
                        "code": code,
                        "name": name,
                        "qty": qty,
                        "price": cur_price,
                        "eval_amt": eval_amt,
                        "buy_pr": buy_price,
                        "dif": dif,
                        "dif_rt": dif_rt,
                    }
                )

            print(f"\n[보유종목] 총 {len(holdings)}종 주식평가액 {stock_eval_amt:,.0f}")
            if holdings:
                rows = []
                for h in holdings:
                    rows.append(
                        {
                            "code": h["code"],
                            "name": h["name"],
                            "qty": f"{_to_int(h['qty']):,d}",
                            "price": f"{_to_float(h['price']):,.0f}",
                            "eval_amt": f"{_to_float(h['eval_amt']):,.0f}",
                            "buy_pr": f"{_to_float(h['buy_pr']):,.0f}",
                            "dif": f"{_to_float(h['dif']):,.0f}",
                            "dif_rt(%)": f"{_to_float(h['dif_rt']):.2f}",
                        }
                    )
                _print_table(
                    rows,
                    ["code", "name", "qty", "price", "eval_amt", "buy_pr", "dif", "dif_rt(%)"],
                    {
                        "qty": "right",
                        "price": "right",
                        "eval_amt": "right",
                        "buy_pr": "right",
                        "dif": "right",
                        "dif_rt(%)": "right",
                    },
                )
            else:
                print("- 보유종목 없음")

            print("[정리] 보유종목 정리/보유 진행? 1.예 2.아니오: ", end="", flush=True)
            ready, _, _ = select.select([sys.stdin], [], [], 10)
            if ready:
                proceed = sys.stdin.readline().strip()
                if proceed in ("1", "2"):
                    if proceed == "1":
                        _run_holdings_flow(client, cano, acnt_prdt_cd, tr_id_balance, tr_id_sell)
                    break

            try:
                rows, summary, _, _, _ = _get_balance_page(
                    client, cano, acnt_prdt_cd, tr_id_balance, return_raw=True
                )
            except RuntimeError as e:
                print(str(e))
                break
    elif run_mode == 2:
        _run_open_id_cleanup(client, cano, acnt_prdt_cd, tr_id_open, tr_id_cancel)
    elif run_mode == 23:
        _run_holdings_price_thread(client, cano, acnt_prdt_cd, tr_id_balance)
    elif run_mode == 21:
        _run_daily_ccld_inquiry(client, cano, acnt_prdt_cd, tr_id_order)
    elif run_mode == 22:
        _run_open_order_inquiry(client, cano, acnt_prdt_cd, tr_id_open)
    elif run_mode == 3:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        parquet_path = os.path.join(base_dir, "data", "1d_data", "kis_1d_unified_parquet_DB.parquet")
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"parquet 파일이 없습니다: {parquet_path}")
        name_map = {}
        symbols_path = symbol_master_path(load_kis_data_layout())
        if os.path.exists(symbols_path):
            sdf = pd.read_parquet(symbols_path, columns=["code", "name"])
            name_map = dict(zip(sdf["code"].astype(str), sdf["name"].astype(str)))
        stop_event = threading.Event()
        balance_thread = None
        if MODE3_THREAD_TEST:
            def _balance_poll_loop() -> None:
                _run_holdings_price_thread(client, cano, acnt_prdt_cd, tr_id_balance)

            balance_thread = threading.Thread(target=_balance_poll_loop, daemon=True)
            balance_thread.start()
        while True:
            sel = input(
                "[종목선정 기준] 1.전일 대비 금일(Parquet_DB 마지막 수신일의 전일 대비) "
                "2.전전일 대비 금일(Parquet_DB 마지막 수신일의 전전일 대비) "
                "3.전일 대비 금일(API실시간, 금일/전일) "
                "q.종료: "
            ).strip()
            if sel.lower() in ("q", "quit", "exit"):
                break
            if sel not in ("1", "2", "3"):
                print("[종목선정] 입력 오류: 1,2,3 또는 q")
                continue
            mode = int(sel)
            _select_candidates(
                client,
                parquet_path,
                name_map,
                mode,
                TOP_TRADING_RET,
                top_n=50,
                emit=True,
            )
        stop_event.set()
        if balance_thread is not None:
            balance_thread.join(timeout=1.0)
    elif run_mode == 4:
        _run_top_trading(
            client,
            cano,
            acnt_prdt_cd,
            tr_id_balance,
            tr_id_buy,
            tr_id_sell,
            tr_id_open,
            tr_id_cancel,
            exclude_cash,
            record_path,
            run_mode,
        )
    elif run_mode == 5:
        _run_ws_ohlcv(appkey, appsecret, base_url, custtype)
    elif run_mode == 6:
        ws_thread = threading.Thread(
            target=_run_ws_ohlcv, args=(appkey, appsecret, base_url, custtype), daemon=True
        )
        ws_thread.start()
        _run_top_trading(
            client,
            cano,
            acnt_prdt_cd,
            tr_id_balance,
            tr_id_buy,
            tr_id_sell,
            tr_id_open,
            tr_id_cancel,
            exclude_cash,
            record_path,
            run_mode,
        )
    else:
        _run_holdings_flow(client, cano, acnt_prdt_cd, tr_id_balance, tr_id_sell)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[done] {ts}")
    _log_tm(f"{os.path.basename(__file__)} 종료 {ts}")


if __name__ == "__main__":
    main()
