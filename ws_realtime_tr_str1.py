"""
Str1 실전 실시간 매도 전략 모듈

전략 개요:
  - 08:29~09:00 (사전 모니터링): 예상체결가(antc_prce)가 전일종가(매수가) 이하
    → 시초가 시장가 매도 주문 (09:00 동시호가에 체결)
    → 예상가 복귀(prdy_ctrt>=0 또는 antc>=매수가) 시 주문 취소
  - 09:00 이후 (정규장): 다음 조건 중 하나 충족 시 즉시 시장가 매도
    ① bidp1*(1-slippage) < wghn_avrg_stck_prc  (가중평균 하락, 슬리피지 적용)
    ② Up_trend and ma500 < ma2000  (EMA 상승추세 중 ma500/ma2000 데드크로스)
       - Up_trend: ma50 > ma200 > ma300 > ma500 > ma2000 > wghn_avrg_stck_prc
    ③ prdy_ctrt < 0  (전일종가 이하 하락)
  - 종목당 1회 매도, 매도 후 종료
  - 수수료·세금 포함 실수익 계산

※ 이 모듈은 순수 전략 판단 함수만 포함 (API 호출 없음).
   실제 주문·상태 관리는 ws_realtime_subscribe_to_DB-1.py에서 수행.
"""
from __future__ import annotations

# ── 수수료·세금 상수 ──────────────────────────────────────────────────────────
FEE_RATE_MARKET_BUY  = 0.0015   # 시장가 매수 수수료+세금+슬리피지 0.15%
FEE_RATE_MARKET_SELL = 0.0035   # 시장가 매도 수수료+세금+슬리피지 0.35%
FEE_RATE_LIMIT_BUY   = 0.00015  # 지정가 매수 수수료 0.015%
FEE_RATE_LIMIT_SELL  = 0.002    # 지정가 매도 수수료+세금 0.2%


# ── 호가단위 유틸 ──────────────────────────────────────────────────────────────
def get_tick_size(price: float, market: str = "KOSPI") -> int:
    if price < 1000:    return 1
    elif price < 5000:  return 5
    elif price < 10000: return 10
    elif price < 50000: return 10 if market == "KOSDAQ" else 50
    elif price < 100000: return 100
    elif price < 500000: return 500
    else:               return 1000


def round_to_tick(price: float, market: str = "KOSPI") -> float:
    tick = get_tick_size(price, market)
    return round(price / tick) * tick


# ── 핵심 전략 판단 함수 ────────────────────────────────────────────────────────

def check_premarket_sell(
    antc_prce: float,
    prdy_ctrt: float,
) -> tuple[bool, str]:
    """
    08:29~09:00 사전 모니터링 매도 조건 판단.

    - antc_prce: 예상체결가 (exp_ccnl 틱의 antc_prce 또는 stck_prpr)
    - prdy_ctrt: 전일대비율 (%) — 음수면 전일종가(=매수가) 이하

    매수 종목은 전일(T) 종가에 체결됐으므로, 당일(T+1) 예상가가
    전일종가 이하(prdy_ctrt < 0)이면 손실 → 시초가 시장가 매도.
    """
    if antc_prce <= 0:
        return False, ""
    if prdy_ctrt < 0:
        return True, "str1_시초가하락_예상"
    return False, ""


def check_premarket_cancel(
    antc_prce: float,
    prdy_ctrt: float,
    buy_price: float,
) -> tuple[bool, str]:
    """
    08:29~09:00 장전 매도 주문 취소 조건 판단.

    장전 매도 주문 후 예상가가 다시 올라가면 → 주문 취소.

    - antc_prce: 예상체결가
    - prdy_ctrt: 전일대비율 (%)
    - buy_price: 매수가(전일종가)

    Returns (True, reason) when cancel:
      - prdy_ctrt >= 0  : 전일대비 상승/보합 예상
      - antc_prce >= buy_price : 예상가가 매수가 이상
    """
    if antc_prce <= 0 or buy_price <= 0:
        return False, ""
    if prdy_ctrt >= 0:
        return True, "str1_장전취소_전일대비상승보합"
    if antc_prce >= buy_price:
        return True, "str1_장전취소_예상가≥매수가"
    return False, ""


def check_realtime_sell(
    bidp1: float,
    wghn_avrg: float,
    ma50: float,
    ma200: float,
    ma300: float,
    ma500: float,
    ma2000: float,
    stck_oprc: float,
    slippage_rate: float = 0.0,
) -> tuple[bool, str]:
    """
    09:00 이후 실시간 매도 조건 판단.

    - stck_oprc <= 0: 시초가 미형성 → 거래 미개시, 매도 안 함
    - ① bidp1*(1-slippage) < wghn_avrg_stck_prc : 가중평균가 하락 (슬리피지 적용)
    - ② Up_trend and ma500 < ma2000             : EMA 상승추세 중 ma500/ma2000 데드크로스
       Up_trend: ma50 > ma200 > ma300 > ma2000 > wghn_avrg (EMA 정배열 유지)
       ma500 < ma2000 이면 매도 (상승추세 붕괴)
    """
    if stck_oprc <= 0:
        return False, ""
    # ① 가중평균 하락 (슬리피지 적용)
    if bidp1 > 0 and wghn_avrg > 0:
        effective_bidp1 = bidp1 * (1.0 - slippage_rate)
        if effective_bidp1 < wghn_avrg:
            return True, "str1_wghn_하락"
    # ② Up_trend and ma500 < ma2000 (상승추세 중 ma500/ma2000 데드크로스)
    # Up_trend: ma50 > ma200 > ma300 > ma500 > ma2000 > wghn_avrg 인 구조
    # market_sell: Up_trend 상태에서 ma500 < ma2000 (추세 붕괴)
    # → (ma50>ma200>ma300>ma2000>wghn) 유지 시 ma500이 ma2000 아래로 돌파하면 매도
    if (
        ma50 > 0 and ma200 > 0 and ma300 > 0 and ma500 > 0 and ma2000 > 0 and wghn_avrg > 0
        and (ma50 > ma200 > ma300 > ma2000 > wghn_avrg)
        and (ma500 < ma2000)
    ):
        return True, "str1_Up_trend_ma500_ma2000_데드크로스"
    return False, ""


# ── 수익 계산 ──────────────────────────────────────────────────────────────────

def calc_sell_pnl(
    buy_price: float,
    sell_price: float,
    qty: int,
    market: str = "KOSPI",
) -> dict:
    """
    매도 수익·손익 계산 (수수료·세금 포함).

    buy_price  : 실제 매수 체결가 (전일 종가)
    sell_price : 매도 주문 기준가 (bidp1 또는 시장가)
    qty        : 수량

    Returns dict:
        actual_sell_price : 슬리피지 반영 실 매도가
        buy_amt           : 매수 비용 (수수료 포함)
        sell_amt          : 매도 수익 (수수료·세금 포함)
        pnl               : 손익 (sell_amt - buy_amt)
        ret_pct           : 수익률 (%)
    """
    actual_sell_price = round_to_tick(
        sell_price * (1 - FEE_RATE_MARKET_SELL), market
    )
    buy_amt  = buy_price * qty * (1 + FEE_RATE_LIMIT_BUY)
    sell_amt = actual_sell_price * qty
    pnl      = sell_amt - buy_amt
    ret_pct  = (actual_sell_price / buy_price - 1) * 100 if buy_price > 0 else 0.0
    return {
        "actual_sell_price": actual_sell_price,
        "buy_amt":  buy_amt,
        "sell_amt": sell_amt,
        "pnl":      pnl,
        "ret_pct":  ret_pct,
    }
