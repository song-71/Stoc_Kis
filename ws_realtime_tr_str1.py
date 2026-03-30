"""
Str1 실전 실시간 매도 전략 모듈

전략 개요:
  - 08:29~09:00 (사전 모니터링): 예상체결가(antc_prce)가 전일종가(매수가) 이하
    → 시초가 시장가 매도 주문 (09:00 동시호가에 체결)
    → 예상가 복귀(prdy_ctrt>=0 또는 antc>=매수가) 시 주문 취소
  - 09:00 이후 (정규장): 다음 조건 중 하나 충족 시 즉시 시장가 매도
    ① bidp1 < wghn_avrg_stck_prc  (가중평균 하락)
    ② Up_trend 지속 상태에서 ma500 < ma2000 데드크로스 → 매도 (상태 기반)
       - Up_trend(정배열): ma50 > ma200 > ma300 > ma500 > ma2000 > wghn_avrg
       - up_trend를 지속 상태로 추적, 매도조건은 매도 성공까지 유지
       - check_realtime_sell()에서 통합 판단 + up_trend 상태 반환
    ③ prdy_ctrt < 0  (전일종가 이하 하락)
  - 종목당 1회 매도, 매도 후 종료
  - 수수료·세금 포함 실수익 계산

※ 이 모듈은 순수 전략 판단 함수만 포함 (API 호출 없음).
   실제 주문·상태 관리는 ws_realtime_trading.py에서 수행.
"""
from __future__ import annotations
from kis_utils import round_to_tick                    # 호가단위 유틸 (kis_utils 통합)

# ── 수수료·세금 상수 ──────────────────────────────────────────────────────────
FEE_RATE_MARKET_BUY  = 0.0015   # 시장가 매수 수수료+세금+슬리피지 0.15%
FEE_RATE_MARKET_SELL = 0.0035   # 시장가 매도 수수료+세금+슬리피지 0.35%
FEE_RATE_LIMIT_BUY   = 0.00015  # 지정가 매수 수수료 0.015%
FEE_RATE_LIMIT_SELL  = 0.002    # 지정가 매도 수수료+세금 0.2%


# ── 핵심 전략 판단 함수 ────────────────────────────────────────────────────────

def check_opening_call_auction_sell(
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
    PREMARKET_SELL_THRESHOLD = -3.0  # 장전 매도 임계값 (%)
    if prdy_ctrt < PREMARKET_SELL_THRESHOLD:
        return True, f"str1_시초가하락_예상({prdy_ctrt:+.2f}%)"
    return False, ""


def check_opening_call_auction_cancel(
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
    PREMARKET_SELL_THRESHOLD = -0.5  # 장전 매도 임계값과 일치
    if prdy_ctrt >= PREMARKET_SELL_THRESHOLD:
        return True, "str1_장전취소_전일대비회복"
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
    was_up_trend: bool = False,
    highest_since_buy: float = 0.0,
    buy_price: float = 0.0,
    trail_activate_pct: float = 0.03,
    trail_stop_pct: float = 0.008,
) -> tuple[bool, str, bool]:
    """
    09:00 이후 실시간 매도 조건 판단 (EMA 데드크로스 + 트레일스톱 통합).

    Args:
      was_up_trend: 직전 틱까지의 up_trend 지속 상태 (호출자가 유지·전달)
      highest_since_buy: 매수 이후 최고 매수호가 (트레일스톱용)
      buy_price: 매입단가 (트레일스톱용)
      trail_activate_pct: 트레일스톱 활성화 기준 (매수가 대비 상승률, 기본 3%)
      trail_stop_pct: 트레일스톱 하락 기준 (최고가 대비 하락률, 기본 0.8%)

    Returns (should_sell, reason, is_up_trend):
      - should_sell : True이면 매도 주문
      - reason      : 매도 사유
      - is_up_trend : 갱신된 up_trend 상태 (호출자가 다음 틱에 was_up_trend로 전달)

    판단 순서:
      ⓪ 트레일스톱: 최고가가 매수가 대비 3% 이상 상승 후, 최고가 대비 0.8% 하락 시 매도
      ① bidp1 < wghn_avrg * 0.995  → 가중평균 대비 0.5% 하락 매도 (up_trend 상태 유지)
      ② 현재 정배열(ma50>ma200>ma300>ma500>ma2000>wghn)
         → up_trend 진입/유지, 매도 안 함
      ③ was_up_trend=True 이고 ma500 < ma2000
         → 데드크로스 매도 + up_trend 해제
      ④ was_up_trend=True 이고 ma500 >= ma2000
         → 다른 이유로 정배열 이탈일 뿐, up_trend 유지 (매도 안 함)
      ⑤ 그 외 → 매도 안 함, up_trend=False
    """
    # ⓪ 트레일링 스톱 — stck_oprc 여부와 무관하게 항상 판단
    if buy_price > 0 and highest_since_buy > 0:
        highest_vs_buy = highest_since_buy / buy_price - 1
        if highest_vs_buy >= trail_activate_pct:
            trail_level = highest_since_buy * (1 - trail_stop_pct)
            if bidp1 < trail_level:
                return True, f"str1_트레일스톱(고가{int(highest_since_buy):,},+{highest_vs_buy*100:.1f}%)", was_up_trend

    if stck_oprc <= 0:
        return False, "", was_up_trend

    # ① 가중평균 대비 0.5% 하락 — up_trend 상태는 그대로 유지
    if bidp1 > 0 and wghn_avrg > 0 and bidp1 < wghn_avrg * 0.995:
        return True, "str1_wghn_하락", was_up_trend

    # ② 현재 정배열 판단 (인라인)
    _is_aligned = (
        all(v > 0 for v in (ma50, ma200, ma300, ma500, ma2000, wghn_avrg))
        and ma50 > ma200 > ma300 > ma500 > ma2000 > wghn_avrg
    )
    if _is_aligned:
        return False, "", True          # up_trend 진입/유지

    # ③④ 직전 틱까지 up_trend였는데 정배열이 깨진 경우
    if was_up_trend:
        # ma500 < ma2000 이면 데드크로스 → 매도 + up_trend 해제
        if ma500 > 0 and ma2000 > 0 and ma500 < ma2000:
            return True, "str1_Up_trend_ma500_ma2000_데드크로스", False
        # 그 외 이유로 정배열 이탈 → up_trend 유지 (해제하지 않음)
        return False, "", True

    # ⑤ 그 외
    return False, "", False


# ── VI 발동 시 매수 전략 ─────────────────────────────────────────────────────

def vi_buy_strategy(
    antc_prce: float,
    stck_prpr: float,
    vi_trade_mode: str,
    already_holding: bool,
) -> tuple[bool, str]:
    """
    VI 발동 종목 매수 판단 (예상체결가 수신 시점에 호출).

    조건: 예상체결가(antc_prce) > 현재가(stck_prpr) 일 때만 매수.
    주문은 상한가로 실행 (호출측에서 처리).

    Returns (should_buy, reason):
      - should_buy : True이면 매수 주문 진행
      - reason     : 매수 사유 문자열
    """
    if vi_trade_mode == "off":
        return False, ""
    if already_holding:
        return False, ""  # 이미 VI 매수 포지션 보유 중
    if antc_prce <= 0 or stck_prpr <= 0:
        return False, ""  # 가격 정보 없음
    if antc_prce <= stck_prpr:
        return False, ""  # 예상체결가가 현재가 이하 → 매수 안 함
    return True, f"str1_VI발동매수({vi_trade_mode})"


def vi_should_cancel(antc_prce: float, stck_prpr: float) -> tuple[bool, str]:
    """
    VI 매수 주문 후 모니터링: 예상체결가 ≤ 현재가이면 취소.

    Returns (should_cancel, reason).
    """
    if antc_prce <= 0 or stck_prpr <= 0:
        return False, ""
    if antc_prce <= stck_prpr:
        return True, "str1_VI_예상가≤현재가_취소"
    return False, ""


def check_vi_sell(
    bidp1: float,
    wghn_avrg: float,
    ma50: float,
    ma200: float,
    ma300: float,
    ma500: float,
    ma2000: float,
    stck_oprc: float,
    highest_since_buy: float,
    buy_price: float,
    minutes_since_buy: float,
    trail_activate_pct: float = 0.03,
    trail_stop_pct: float = 0.008,
) -> tuple[bool, str]:
    """
    VI 매수 포지션 매도 조건.

    ① 매수가 하회: bidp1 < buy_price → 즉시 매도
    ② 트레일링 스톱: 최고가가 매수가 대비 activate_pct(3%) 이상 상승 시에만 적용,
       최고가 대비 stop_pct 하락하면 매도
    ③ 매수 후 10분 이내: ma50 < ma200 (단순 비교)
    ④ 매수 후 10분 초과: Up_trend + ma500 < ma2000
    """
    if stck_oprc <= 0 or bidp1 <= 0:
        return False, ""

    # ① 매수가 하회 → 즉시 매도
    if buy_price > 0 and bidp1 < buy_price:
        return True, "str1_VI_매수가하회"

    # ② 트레일링 스톱 — 최고가가 매수가 대비 3% 이상 상승한 경우에만 적용
    if buy_price > 0 and highest_since_buy > 0:
        highest_vs_buy = highest_since_buy / buy_price - 1
        if highest_vs_buy >= trail_activate_pct:
            trail_level = highest_since_buy * (1 - trail_stop_pct)
            if bidp1 < trail_level:
                return True, f"str1_VI_트레일스톱(고가{int(highest_since_buy):,},+{highest_vs_buy*100:.1f}%)"

    # ③ 매수 후 10분 이내: ma50 < ma200
    if minutes_since_buy <= 10.0:
        if ma50 > 0 and ma200 > 0 and ma50 < ma200:
            return True, "str1_VI_ma50<ma200(10분내)"

    # ④ 매수 후 10분 초과: Up_trend + ma500 < ma2000
    if minutes_since_buy > 10.0:
        up_trend = (
            ma50 > 0 and ma200 > 0 and ma300 > 0 and ma2000 > 0 and wghn_avrg > 0
            and ma50 > ma200 > ma300 > ma2000 > wghn_avrg
        )
        if up_trend and ma500 > 0 and ma500 < ma2000:
            return True, "str1_VI_ma500<ma2000(10분후)"

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


# ── 핫스왑: 이 파일을 직접 실행하면 config에 전략 변경 요청 기록 ──────────────
if __name__ == "__main__":
    import json
    from pathlib import Path
    from datetime import datetime

    config_path = Path(__file__).parent / "config.json"
    module_name = Path(__file__).stem  # "ws_realtime_tr_str1"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        cfg["strategy_swap"] = {
            "status": "requested",
            "module": module_name,
            "timestamp": datetime.now().isoformat(),
        }
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        print(f"전략 변경 요청 완료: {module_name}")
        print(f"  config.json → strategy_swap.status = 'requested'")
        print(f"  메인 프로그램이 자동으로 리로드합니다.")
    except Exception as e:
        print(f"전략 변경 요청 실패: {e}")
