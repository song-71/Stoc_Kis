"""
Str1 실전 실시간 매도 전략 + 상한가 근접 매수 전략 모듈

== 기존 전략 ==
- 08:29~09:00 사전 모니터링 매도 / 09:00 이후 실시간 매도 (check_realtime_sell)
- VI 매수 / VI 매도 (vi_buy_strategy / check_vi_sell)
- 수수료·세금 포함 실수익 계산 (calc_sell_pnl)

== 상한가 근접 매수 전략 (문서: docs/01. up_limit_buy_str_260420.md) ==
- 25~28% 구간 다중 필터 AND 통과 시 매수
  - uplimit_approach_buy_signal(): 13개 필터 pass/fail 판정
  - uplimit_approach_exit(): 단계적 청산 (29%→28% 절반, 27% 전량) + 손절/타임아웃
  - calc_uplimit_qty(): 수량 계산 (수수료 0.5% 버퍼)
  - calc_entry_price(): 지정가 매수 가격 (현재가 + 2틱)

== 종가매수 폭락 방지 필터 (문서: docs/02. Top30_str.md Part B) ==
- closing_crash_filter_signal(): 외국인/체결강도/거래대금 기반 차단
- closing_next_day_exit(): 익일 시초 폭락 감지 시 시장가 매도

※ 이 모듈은 순수 전략 판단 함수만 포함 (API 호출·파일 I/O 없음).
   실제 주문·상태 관리는 ws_realtime_trading.py 에서 수행.
"""
from __future__ import annotations
from kis_utils import round_to_tick, price_plus_n_ticks  # 호가단위 유틸 (kis_utils 통합)

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
    trail_stop_pct: float = 0.03,
) -> tuple[bool, str, bool]:
    """
    09:00 이후 실시간 매도 조건 판단 (EMA 데드크로스 + 트레일스톱 통합).

    Args:
      was_up_trend: 직전 틱까지의 up_trend 지속 상태 (호출자가 유지·전달)
      highest_since_buy: 매수 이후 최고 매수호가 (트레일스톱용)
      buy_price: 매입단가 (트레일스톱용)
      trail_activate_pct: 트레일스톱 활성화 기준 (매수가 대비 상승률, 기본 3%)
      trail_stop_pct: 트레일스톱 하락 기준 (최고가 대비 하락률, 기본 3%)

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
    trail_stop_pct: float = 0.03,
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


# =============================================================================
# 상한가 근접 매수 전략 (문서: docs/01. up_limit_buy_str_260420.md)
# =============================================================================

# 진입 필터 임계값 (호출측에서 override 가능)
UPLIMIT_FILTER_CTRT_MIN = 25.0          # 진입 하한
UPLIMIT_FILTER_CTRT_MAX = 29.0          # 진입 상한 (29%+ 는 EMERGENCY 영역)
UPLIMIT_FILTER_PREV_CTRT_MAX = 10.0     # 전일 등락률 상한 (작전주 차단)
UPLIMIT_FILTER_GAP_MIN = 5.0            # 시가 갭 하한 (%)
UPLIMIT_FILTER_VOL_SURGE_MIN = 3.0      # 직전 5분 거래량 / 당일 분당 평균 배수
UPLIMIT_FILTER_TIME_TO_25_MAX = 60.0    # 25% 돌파까지 최대 분
UPLIMIT_FILTER_VOLA_10D_MAX = 0.05      # 10일 일봉 std 상한
UPLIMIT_FILTER_ASK_VS_BID_MAX = 3.0     # 매도잔량/매수잔량 상한 (매도벽)
UPLIMIT_FILTER_VOLUME_POWER_MIN = 120.0 # 체결강도 하한
UPLIMIT_FILTER_MIN_VOLUME = 100_000     # 당일 최소 누적 거래량
UPLIMIT_FILTER_MIN_PRICE = 1000         # 저가주 제외

# Exit 임계값 — [260423] 재설계: 서버 스톱 지정가(ord_dvsn=22) 사용
UPLIMIT_EXIT_UPPER_LIMIT = 29.5         # 상한가 근접 인정 기준 (이 값 도달 시 스톱 지정가 주문 발주)
UPLIMIT_EXIT_HALF_THRESHOLD = 29.0      # 상한가 도달 후 이 값 하회 시 1차 절반 매도 (CNDT_PRIC)
UPLIMIT_EXIT_FULL_THRESHOLD = 28.0      # 이 값 하회 시 2차 나머지 절반 매도 (CNDT_PRIC)
UPLIMIT_EXIT_MARKET_SELL_CTRT = 25.0    # 25% 하회 시 기존 스톱 취소 + 시장가 전량 매도
UPLIMIT_EXIT_STOP_LOSS_PCT = 0.03       # -3% 손절 (매수가 대비, 상한가 미도달 시)
UPLIMIT_EXIT_TRIGGER_CTRT = 28.0        # 28% 상향 돌파 edge-trigger 임계값 (매수 진입)
# @deprecated — 아래 상수는 구 uplimit_approach_exit 참조용 (사용 안 함)
UPLIMIT_EXIT_TIMEOUT_MIN = 10.0         # deprecated
UPLIMIT_EXIT_TIMEOUT_CTRT = 25.0        # deprecated
UPLIMIT_EXIT_TRAIL_STOP_PCT = 0.02      # deprecated
UPLIMIT_EXIT_CRASH_GUARD_PCT = 0.05     # deprecated


def uplimit_approach_buy_signal(
    prdy_ctrt: float,
    prev_close: float,
    stck_oprc: float,
    acml_vol: float,
    day_avg_vol_per_min: float,
    last_5m_avg_vol_per_min: float,
    min_since_open: float,
    min_since_25pct_cross: float | None,
    vola_10d: float | None,
    bid_sum_top5: float,
    ask_sum_top5: float,
    volume_power: float | None,
    frgn_3d_net: float | None,
    prev_day_prdy_ctrt: float,
    already_holding: bool,
    now_hm: tuple[int, int],
    today_buy_count: int,
    max_daily_buys: int = 3,
    prev_ctrt: float = 0.0,   # [260423] 직전 틱 등락률 — 28% edge-trigger 용
) -> tuple[bool, str, float]:
    """
    상한가 근접 매수 진입 판단. 13개 필터 AND — 하나라도 실패 시 매수 안 함.

    [260423] F3 필터를 "28% 상향 돌파 순간" edge-trigger 로 변경.
    prev_ctrt < 28.0 ≤ prdy_ctrt < 29.5 인 경우만 통과.
    (25% 돌파 후 상승 중이어도 28% 이미 지난 종목은 재진입 안 함)

    Returns (should_buy, reason, strength):
      should_buy : True 면 매수 주문
      reason     : 통과/실패 사유
      strength   : 1.0(통과) 또는 0.0(실패).
    """
    # F1. 시간대 (09:30 ≤ now < 14:50)
    h, m = now_hm
    if h < 9 or (h == 9 and m < 30):
        return False, "uplimit_F1_시간대전", 0.0
    if h >= 15 or (h == 14 and m >= 50):
        return False, "uplimit_F1_시간대후", 0.0

    # F2. 이미 보유 중
    if already_holding:
        return False, "uplimit_F2_보유중", 0.0

    # F3. [260423] 28% 상향 돌파 edge-trigger
    #   - prev_ctrt: 직전 틱 등락률 (호출측에서 _last_prdy_ctrt 로 전달)
    #   - 돌파 순간(prev<28, cur≥28)만 통과, 이미 28+ 유지 중이면 skip (중복 방지)
    if not (prev_ctrt < UPLIMIT_EXIT_TRIGGER_CTRT <= prdy_ctrt < UPLIMIT_EXIT_UPPER_LIMIT):
        return False, f"uplimit_F3_28%돌파아님(prev={prev_ctrt:.2f},cur={prdy_ctrt:.2f})", 0.0

    # F4. 전일 등락률 < 10% (작전주/연속급등 차단 — 백테스트 1순위)
    if prev_day_prdy_ctrt >= UPLIMIT_FILTER_PREV_CTRT_MAX:
        return False, f"uplimit_F4_전일급등({prev_day_prdy_ctrt:.1f}%)", 0.0

    # F5. 저가주 제외
    if prev_close < UPLIMIT_FILTER_MIN_PRICE:
        return False, f"uplimit_F5_저가주({int(prev_close)})", 0.0

    # F6. 시가 갭 ≥ +5%
    if prev_close <= 0 or stck_oprc <= 0:
        return False, "uplimit_F6_가격누락", 0.0
    gap_pct = (stck_oprc / prev_close - 1) * 100
    if gap_pct < UPLIMIT_FILTER_GAP_MIN:
        return False, f"uplimit_F6_갭부족({gap_pct:+.1f}%)", 0.0

    # F7. 최소 누적 거래량
    if acml_vol < UPLIMIT_FILTER_MIN_VOLUME:
        return False, f"uplimit_F7_거래량부족({int(acml_vol):,})", 0.0

    # F8. 거래량 서지 (직전5분 평균 ≥ 당일 분당 평균 × 3)
    if day_avg_vol_per_min <= 0 or last_5m_avg_vol_per_min <= 0:
        return False, "uplimit_F8_서지산출불가", 0.0
    surge_ratio = last_5m_avg_vol_per_min / day_avg_vol_per_min
    if surge_ratio < UPLIMIT_FILTER_VOL_SURGE_MIN:
        return False, f"uplimit_F8_서지부족(x{surge_ratio:.2f})", 0.0

    # F9. 25% 도달 시간 — 60분 이내
    if min_since_25pct_cross is None:
        return False, "uplimit_F9_돌파시각누락", 0.0
    if min_since_25pct_cross > UPLIMIT_FILTER_TIME_TO_25_MAX:
        return False, f"uplimit_F9_돌파지연({min_since_25pct_cross:.0f}분)", 0.0

    # F10. 변동성 ≤ 5%
    if vola_10d is None:
        return False, "uplimit_F10_변동성누락", 0.0
    if vola_10d > UPLIMIT_FILTER_VOLA_10D_MAX:
        return False, f"uplimit_F10_고변동({vola_10d:.3f})", 0.0

    # F11. 호가 매도벽 (매도잔량 < 매수잔량 × 3)
    if bid_sum_top5 <= 0:
        return False, "uplimit_F11_호가누락", 0.0
    if ask_sum_top5 >= bid_sum_top5 * UPLIMIT_FILTER_ASK_VS_BID_MAX:
        return False, f"uplimit_F11_매도벽(ask/bid={ask_sum_top5/bid_sum_top5:.2f})", 0.0

    # F12. 체결강도
    if volume_power is None:
        return False, "uplimit_F12_VP누락", 0.0
    if volume_power < UPLIMIT_FILTER_VOLUME_POWER_MIN:
        return False, f"uplimit_F12_VP약({volume_power:.0f})", 0.0

    # F13. 외국인 — 3일 순매도만 차단, 데이터 없음은 허용
    if frgn_3d_net is not None and frgn_3d_net < 0:
        return False, f"uplimit_F13_외인매도({int(frgn_3d_net):,})", 0.0

    # 일일 매수 한도
    if today_buy_count >= max_daily_buys:
        return False, f"uplimit_일한도초과({today_buy_count}/{max_daily_buys})", 0.0

    reason = (
        f"uplimit_매수(ctrt{prdy_ctrt:.1f}%,갭{gap_pct:+.1f}%,"
        f"서지x{surge_ratio:.1f},돌파{min_since_25pct_cross:.0f}분,"
        f"vol{vola_10d:.3f},VP{volume_power:.0f},"
        f"bid/ask={bid_sum_top5/max(ask_sum_top5,1):.1f})"
    )
    return True, reason, 1.0


def uplimit_approach_exit(
    prdy_ctrt: float,
    bidp1: float,
    buy_price: float,
    max_prdy_ctrt: float,
    highest_since_buy: float,
    minutes_since_buy: float,
    half_sold: bool,
    now_hm: tuple[int, int],
) -> tuple[str, str, float]:
    """
    @deprecated [260423] — 재설계 후 사용 안 함.
    매수 체결 시 KIS 서버 스톱 지정가(ord_dvsn=22) 주문을 2건 발주하여
    서버가 29%/28% 하회 감지 시 자동 매도하도록 변경됨.
    25% 하회 시 클라이언트 측에서 _try_uplimit_market_sell() 를 호출하여
    기존 스톱 주문 취소 + 시장가 전량 매도.

    기존 단계적 Exit 판단 (호출측 호환용 유지).

    Returns (action, reason, sell_ratio):
      action     : "hold" | "sell_half" | "sell_all"
      sell_ratio : 0.0 | 0.5 | 1.0
    """
    if buy_price <= 0 or bidp1 <= 0:
        return "hold", "uplimit_exit_가격누락", 0.0

    reached_upper = max_prdy_ctrt >= UPLIMIT_EXIT_UPPER_LIMIT

    # 1) 마감 청산 (최우선)
    h, m = now_hm
    if h == 15 and m >= 10:
        return "sell_all", "uplimit_마감청산(15:10+)", 1.0

    # 2) 손실 방어 (단계적 청산보다 우선)
    #    2-a) 상한가 도달 후 대폭락 -5% → 단계적 청산 건너뛰고 전량 (매우 빠른 낙하)
    if reached_upper and bidp1 <= buy_price * (1 - UPLIMIT_EXIT_CRASH_GUARD_PCT):
        return "sell_all", f"uplimit_대폭락방지-5%({bidp1}/{buy_price})", 1.0
    #    2-b) 고정 손절 -3% (매수가 대비, 상한가 도달 여부 무관)
    if bidp1 <= buy_price * (1 - UPLIMIT_EXIT_STOP_LOSS_PCT):
        return "sell_all", f"uplimit_손절-3%({bidp1}/{buy_price})", 1.0

    # 3) 단계적 청산 — 상한가(29%) 도달 이력 있음 (정상 이익 실현)
    if reached_upper:
        # 3-a) 27% 하회 → 전량
        if prdy_ctrt < UPLIMIT_EXIT_FULL_THRESHOLD:
            return "sell_all", f"uplimit_27하회전량({prdy_ctrt:.1f}%)", 1.0
        # 3-b) 28% 하회 && 아직 절반 매도 안 함 → 절반
        if (not half_sold) and prdy_ctrt < UPLIMIT_EXIT_HALF_THRESHOLD:
            return "sell_half", f"uplimit_29→28하회절반({prdy_ctrt:.1f}%)", 0.5

    # 4) 타임아웃 — 10분 경과 + 25% 미만
    if minutes_since_buy >= UPLIMIT_EXIT_TIMEOUT_MIN and prdy_ctrt < UPLIMIT_EXIT_TIMEOUT_CTRT:
        return "sell_all", f"uplimit_타임아웃({prdy_ctrt:.1f}%,{minutes_since_buy:.1f}분)", 1.0

    # 5) 트레일링 (상한가 미도달일 때만 적용 — 도달 시엔 단계적 청산이 우선)
    if (not reached_upper) and highest_since_buy > 0:
        trail_level = highest_since_buy * (1 - UPLIMIT_EXIT_TRAIL_STOP_PCT)
        if bidp1 <= trail_level and highest_since_buy > buy_price:
            return "sell_all", f"uplimit_트레일링-2%(고가{int(highest_since_buy):,})", 1.0

    return "hold", "", 0.0


# =============================================================================
# [260423] 신규 Exit 보조 함수
# =============================================================================

def uplimit_should_cancel_and_market_sell(
    prdy_ctrt: float,
    has_position: bool,
) -> tuple[bool, str]:
    """
    25% 하회 감지 시 기존 스톱 지정가 주문 취소 + 시장가 전량 매도 여부 판단.
    호출측(ws_realtime_trading.py) 은 True 반환 시:
      1) _cancel_pending_stop_orders(code)  — ①② 스톱 주문 모두 취소
      2) _sell_market(code, remaining_qty)  — 시장가 전량 매도
      3) _uplimit_blacklist 에 추가하지 않음 (29% 재상회 시 재매수 허용)
    """
    if not has_position:
        return False, ""
    if prdy_ctrt < UPLIMIT_EXIT_MARKET_SELL_CTRT:
        return True, f"uplimit_25%하회_시장가전량({prdy_ctrt:.2f}%)"
    return False, ""


def uplimit_should_setup_stop_orders(
    max_prdy_ctrt: float,
    stop_orders_placed: bool,
) -> tuple[bool, str]:
    """
    상한가 근접(29.5%+) 도달 시 스톱 지정가 주문 2건 발주 여부.
    호출측 _setup_stop_limit_orders(code, qty) 를 호출해야 함.
    stop_orders_placed=True 면 중복 발주 방지.
    """
    if stop_orders_placed:
        return False, ""
    if max_prdy_ctrt >= UPLIMIT_EXIT_UPPER_LIMIT:
        return True, f"uplimit_스톱지정가_발주(max={max_prdy_ctrt:.2f}%)"
    return False, ""


# =============================================================================
# [260424] Strategy A-v4 — 28% 5분유지 + 당일홀드 + 익일MA매도
#   백테스트: 59일 863 trades, 승률 38.1%, 총 PnL +846%, 평균 +1.56%/거래
#   (symulation/Query_str_uplimit_5min_hold_nextday_v2_simulation.py)
# =============================================================================

UPLIMIT_V4_SUSTAIN_MINUTES = 5       # (deprecated v4) 28% 최초 돌파 후 N분 — v5 에서 미사용
UPLIMIT_V4_HARD_LOSS = 0.05          # 당일 매수가 대비 -5% 하드 손절 (v5 도 사용)
UPLIMIT_V4_OVERNIGHT_CTRT = 0.295    # 14:55 에 이 값 이상이면 익일 홀드
UPLIMIT_V4_HOLD_CLOSE_HM = (14, 55)  # 당일 홀드 전환 판정 시각
UPLIMIT_V4_NEXTDAY_GAP_LOSS = 0.03   # 익일 09:00~09:03 중 low<=buy*(1-x) 손절
UPLIMIT_V4_NEXTDAY_GAP_END = (9, 3)  # 익일 갭 체크 종료 시각
UPLIMIT_V4_NEXTDAY_CLOSE_HM = (14, 55)  # 익일 마감 청산

# [260427] v5 신규 (기본 컷)
UPLIMIT_V5_BUY_START_HM = (9, 30)
UPLIMIT_V5_BUY_END_HM = (14, 30)
UPLIMIT_V5_TRAIL_ARM_CTRT = 29.0     # max_prdy_ctrt 이 값 도달 후 트레일 활성
UPLIMIT_V5_TRAIL_PCT = 0.03          # 트레일 하락률

# [260427] v5 12필터 임계값 (uplimit_approach_buy_signal 의 5개 필터를 v5 전용으로 완화)
UPLIMIT_V5_FILTER_GAP_MIN = 2.0          # 시가 갭 (was 5.0)
UPLIMIT_V5_FILTER_VOL_SURGE_MIN = 2.0    # 거래량 서지 배수 (was 3.0)
UPLIMIT_V5_FILTER_VOLA_10D_MAX = 0.08    # 10일 변동성 (was 0.05)
UPLIMIT_V5_FILTER_ASK_VS_BID_MAX = 5.0   # 매도벽 (was 3.0)
UPLIMIT_V5_FILTER_VOLUME_POWER_MIN = 100.0  # 체결강도 (was 120.0)
UPLIMIT_V5_QUALIFY_EXPIRY_MIN = 10       # 12필터 통과 후 매수 트리거 가능 분


def check_uplimit_v4_sustain_buy(
    prdy_ctrt: float,
    first_28_seen_minutes_ago: float | None,
    already_holding: bool,
    now_hm: tuple[int, int],
) -> tuple[bool, str]:
    """A-v4 매수 판정 — 28% 5분 유지 확인.

    입력:
      prdy_ctrt : 현재 등락률
      first_28_seen_minutes_ago : 28% 최초 감지 후 지난 분 (None 이면 미감지)
      already_holding : 이미 보유 중
      now_hm : (시, 분)

    조건 (AND):
      F1. 09:30 ≤ now ≤ 14:30 (매수 허용 시간)
      F2. 현재 28% ≤ prdy_ctrt < 30%
      F3. 5분 전부터 28% 유지 중 (first_28_seen_minutes_ago >= 5)
      F4. 이미 보유 중 아님
    """
    if already_holding:
        return False, "v4_보유중"
    h, m = now_hm
    if h < 9 or (h == 9 and m < 30):
        return False, "v4_시간대전"
    if h >= 15 or (h == 14 and m >= 30):
        return False, "v4_시간대후"
    if not (28.0 <= prdy_ctrt < 30.0):
        return False, f"v4_구간({prdy_ctrt:.2f}%)"
    if first_28_seen_minutes_ago is None:
        return False, "v4_최초돌파기록없음"
    if first_28_seen_minutes_ago < UPLIMIT_V4_SUSTAIN_MINUTES:
        return False, f"v4_유지부족({first_28_seen_minutes_ago:.1f}<{UPLIMIT_V4_SUSTAIN_MINUTES}분)"
    return True, (
        f"v4_매수({prdy_ctrt:.2f}%,유지{first_28_seen_minutes_ago:.1f}분)"
    )


def check_uplimit_v4_overnight_hold(
    prdy_ctrt: float,
    now_hm: tuple[int, int],
) -> tuple[bool, str]:
    """14:55 에 29.5%+ 이면 익일 홀드로 전환.

    Returns (should_hold, reason):
      should_hold=True → 오늘 청산하지 말고 익일로 넘김
      should_hold=False → 14:55 현재가로 청산
    """
    h, m = now_hm
    if (h, m) < UPLIMIT_V4_HOLD_CLOSE_HM:
        return False, ""   # 아직 14:55 전이면 판정 아님
    if prdy_ctrt >= UPLIMIT_V4_OVERNIGHT_CTRT * 100:
        return True, f"v4_익일홀드({prdy_ctrt:.2f}%)"
    return False, f"v4_당일청산({prdy_ctrt:.2f}%)"


def check_uplimit_v4_nextday_sell(
    bidp1: float,
    low_bar: float,
    buy_price: float,
    ma_fast: float,
    ma_slow: float,
    now_hm: tuple[int, int],
) -> tuple[bool, str]:
    """익일 매도 판정 (overnight 포지션).

    조건 (OR):
      D1. 09:00~09:03 중 low ≤ buy×0.97 → 갭 손절
      D2. ma_fast < ma_slow 데드크로스
      D3. 14:55 마감

    입력:
      bidp1, low_bar : 현재 호가1/현재 분봉 저가 (갭 손절 판정)
      buy_price : 매수가
      ma_fast / ma_slow : 실시간 MA (틱 기반 ma500 ≈ 1분봉 MA5 근사, ma2000 ≈ MA20 근사)
      now_hm : 현재 (시, 분)
    """
    h, m = now_hm
    # D3: 마감
    if (h, m) >= UPLIMIT_V4_NEXTDAY_CLOSE_HM:
        return True, "v4_익일마감"
    # D1: 09:00~09:03 갭 손절 (분봉 저가 혹은 bidp1)
    if (h, m) <= UPLIMIT_V4_NEXTDAY_GAP_END:
        trigger_price = min(low_bar if low_bar > 0 else bidp1, bidp1)
        if buy_price > 0 and trigger_price <= buy_price * (1 - UPLIMIT_V4_NEXTDAY_GAP_LOSS):
            return True, f"v4_익일초반갭-{UPLIMIT_V4_NEXTDAY_GAP_LOSS*100:.0f}%"
    # D2: MA 데드크로스
    if ma_fast > 0 and ma_slow > 0 and ma_fast < ma_slow:
        return True, f"v4_익일MA데드크로스({ma_fast:.1f}<{ma_slow:.1f})"
    return False, ""


# =============================================================================
# [260427] Strategy A-v5 — 구독 즉시 (ma10 상회 첫 시점) 시장가 매수
#   v4 (28% 5분유지) 폐지 → v5 단일 운영
#   F1. 시간대  09:30 ≤ now ≤ 14:30
#   F2. 등락률  25% ≤ prdy_ctrt < 30%
#   F3. ma10   stck_prpr > ma10 (10틱 누적 후 첫 상회)
#   F4. 상한가 미도달 (limitup_reached set 체크 — 호출측)
#   F5. 저거래 아님 (호출측에서 _is_low_liquidity_v5 통과)
#   F6. 보유 중 아님 / 당일 매수 이력 없음 (호출측)
#   매수: 시장가 (ord_dvsn="01"), 시드 분산 3종목
#   Exit: 트레일 3% (max_prdy_ctrt≥29 도달 후) > 하드 5% > 14:55 청산
# =============================================================================

def check_uplimit_v5_qualify(
    prdy_ctrt: float,
    prev_close: float,
    stck_oprc: float,
    acml_vol: float,
    day_avg_vol_per_min: float,
    last_5m_avg_vol_per_min: float,
    min_since_25pct_cross: float | None,
    vola_10d: float | None,
    bid_sum_top5: float,
    ask_sum_top5: float,
    volume_power: float | None,
    frgn_3d_net: float | None,
    prev_day_prdy_ctrt: float,
    already_holding: bool,
    now_hm: tuple[int, int],
    today_buy_count: int,
    max_daily_buys: int = 3,
) -> tuple[bool, str]:
    """v5 종목 선정 — 12필터 AND (F1, F2, F4-F13). F3(매수 트리거)는 별도.

    [260427] 종목 선정과 매수 트리거 분리.
    - 이 함수: 12필터 모두 통과 시 종목을 "qualified" 상태로 표시
    - 매수 트리거: check_uplimit_v5_buy_trigger() 가 ma10/BB 반등으로 별도 판정

    필터:
      F1. 시간대 09:30 ≤ now ≤ 14:30
      F2. 보유 중 아님
      F4. 전일 등락률 < 10% (작전주 차단)
      F5. 저가주 제외 (prev_close ≥ 1000원)
      F6. 시가 갭 ≥ +2% (완화: 5→2)
      F7. 누적 거래량 ≥ 100K
      F8. 거래량 서지 ≥ 2배 (완화: 3→2)
      F9. 25% 도달 후 60분 이내
      F10. 10일 변동성 ≤ 0.08 (완화: 0.05→0.08)
      F11. 매도벽 ask < bid × 5 (완화: 3→5)
      F12. 체결강도 ≥ 100 (완화: 120→100)
      F13. 외국인 3일 순매수 > 0 (양수만, 데이터없음 차단)
    """
    # F1
    h, m = now_hm
    if (h, m) < UPLIMIT_V5_BUY_START_HM:
        return False, "v5_F1_시간대전"
    if (h, m) > UPLIMIT_V5_BUY_END_HM:
        return False, "v5_F1_시간대후"

    # F2
    if already_holding:
        return False, "v5_F2_보유중"

    # F4. 전일 등락률 < 10%
    if prev_day_prdy_ctrt >= UPLIMIT_FILTER_PREV_CTRT_MAX:
        return False, f"v5_F4_전일급등({prev_day_prdy_ctrt:.1f}%)"

    # F5. 저가주
    if prev_close < UPLIMIT_FILTER_MIN_PRICE:
        return False, f"v5_F5_저가주({int(prev_close)})"

    # F6. 시가 갭 ≥ +2% (완화)
    if prev_close <= 0 or stck_oprc <= 0:
        return False, "v5_F6_가격누락"
    gap_pct = (stck_oprc / prev_close - 1) * 100
    if gap_pct < UPLIMIT_V5_FILTER_GAP_MIN:
        return False, f"v5_F6_갭부족({gap_pct:+.1f}%)"

    # F7. 최소 누적 거래량
    if acml_vol < UPLIMIT_FILTER_MIN_VOLUME:
        return False, f"v5_F7_거래량부족({int(acml_vol):,})"

    # F8. 거래량 서지 (완화)
    if day_avg_vol_per_min <= 0 or last_5m_avg_vol_per_min <= 0:
        return False, "v5_F8_서지산출불가"
    surge_ratio = last_5m_avg_vol_per_min / day_avg_vol_per_min
    if surge_ratio < UPLIMIT_V5_FILTER_VOL_SURGE_MIN:
        return False, f"v5_F8_서지부족(x{surge_ratio:.2f})"

    # F9. 25% 도달 후 60분 이내
    if min_since_25pct_cross is None:
        return False, "v5_F9_돌파시각누락"
    if min_since_25pct_cross > UPLIMIT_FILTER_TIME_TO_25_MAX:
        return False, f"v5_F9_돌파지연({min_since_25pct_cross:.0f}분)"

    # F10. 변동성 (완화)
    if vola_10d is None:
        return False, "v5_F10_변동성누락"
    if vola_10d > UPLIMIT_V5_FILTER_VOLA_10D_MAX:
        return False, f"v5_F10_고변동({vola_10d:.3f})"

    # F11. 매도벽 (완화)
    if bid_sum_top5 <= 0:
        return False, "v5_F11_호가누락"
    if ask_sum_top5 >= bid_sum_top5 * UPLIMIT_V5_FILTER_ASK_VS_BID_MAX:
        return False, f"v5_F11_매도벽(ask/bid={ask_sum_top5/bid_sum_top5:.2f})"

    # F12. 체결강도 (완화)
    if volume_power is None:
        return False, "v5_F12_VP누락"
    if volume_power < UPLIMIT_V5_FILTER_VOLUME_POWER_MIN:
        return False, f"v5_F12_VP약({volume_power:.0f})"

    # F13. 외국인 3일 순매수 > 0 (양수만)
    if frgn_3d_net is None:
        return False, "v5_F13_외인데이터없음"
    if frgn_3d_net <= 0:
        return False, f"v5_F13_외인수급없음({int(frgn_3d_net):,})"

    # 일일 매수 한도
    if today_buy_count >= max_daily_buys:
        return False, f"v5_일한도초과({today_buy_count}/{max_daily_buys})"

    # 25%~30% 구간 보장
    if not (25.0 <= prdy_ctrt < 30.0):
        return False, f"v5_구간({prdy_ctrt:.2f}%)"

    return True, (
        f"v5_qualify(ctrt{prdy_ctrt:.2f}%,갭{gap_pct:+.1f}%,서지x{surge_ratio:.1f},"
        f"돌파{min_since_25pct_cross:.0f}분,vol{vola_10d:.3f},VP{volume_power:.0f},"
        f"외인{int(frgn_3d_net):,},bid/ask={bid_sum_top5/max(ask_sum_top5,1):.1f})"
    )


def check_uplimit_v5_buy_trigger(
    stck_prpr: float,
    bidp1: float,
    prev_bidp1: float,
    ma10: float,
    bb_lower: float | None,
    breach_flag: bool,
    tick_count: int,
    qualified_min_ago: float,
) -> tuple[bool, str]:
    """v5 매수 트리거 — 종목 선정 후 ma10 상회 OR BB 하단 반등 패턴.

    조건:
      Q1 만료 체크: qualified 후 10분 초과 시 트리거 비활성
      ma10 분기: tick_count≥10 + ma10>0 + stck_prpr>ma10 → 매수
      BB 분기 (BB 형성 후 + ma10 미상회 시): bidp1 > bb_lower AND prev_bidp1 > bb_lower AND bidp1 > prev_bidp1 AND breach_flag → 매수
      BB 미형성 시: ma10 트리거만 사용

    Args:
      breach_flag: qualified 이후 어느 시점에 bidp1 < bb_lower 발생했는지 (호출측이 추적)

    Returns:
      (should_buy, reason)
    """
    if qualified_min_ago > UPLIMIT_V5_QUALIFY_EXPIRY_MIN:
        return False, f"v5_T_만료({qualified_min_ago:.1f}분>10분)"

    # ma10 트리거
    if tick_count >= 10 and ma10 > 0 and stck_prpr > 0 and stck_prpr > ma10:
        return True, f"v5_T_ma10상회(prpr={stck_prpr:.0f}>ma10={ma10:.1f})"

    # BB 반등 트리거 (BB 형성된 경우만)
    if bb_lower is not None and bidp1 > 0 and prev_bidp1 > 0:
        if (breach_flag
            and prev_bidp1 > bb_lower
            and bidp1 > bb_lower
            and bidp1 > prev_bidp1):
            return True, (
                f"v5_T_BB반등(bidp1={bidp1:.0f},prev={prev_bidp1:.0f},"
                f"bb_lower={bb_lower:.1f})"
            )

    # 트리거 미발동
    return False, ""


# 호환 wrapper (기존 import 처리)
def check_uplimit_v5_instant_buy(*args, **kwargs):
    """[deprecated] check_uplimit_v5_qualify + check_uplimit_v5_buy_trigger 사용.
    호환성 위해 유지하되 호출 시 qualify 결과만 반환.
    """
    return check_uplimit_v5_qualify(*args, **kwargs)


def check_uplimit_v5_trail_exit(
    buy_price: float,
    highest_since_buy: float,
    max_prdy_ctrt: float,
    cur_price: float,
    trail_arm_ctrt: float = UPLIMIT_V5_TRAIL_ARM_CTRT,
    trail_pct: float = UPLIMIT_V5_TRAIL_PCT,
) -> tuple[bool, str]:
    """v5 트레일스톱 — max_prdy_ctrt ≥ 29 도달 후 highest×(1-3%) 하회 시 매도."""
    if max_prdy_ctrt < trail_arm_ctrt:
        return False, ""
    if highest_since_buy <= 0 or cur_price <= 0 or buy_price <= 0:
        return False, ""
    trail_level = highest_since_buy * (1 - trail_pct)
    if cur_price <= trail_level:
        return True, (
            f"v5_트레일-{int(trail_pct*100)}%"
            f"(high={highest_since_buy:.0f},cur={cur_price:.0f},max_ctrt={max_prdy_ctrt:.2f}%)"
        )
    return False, ""


# =============================================================================
# [260423] Strategy B — MA trend-following (틱 EMA 기반)
#   매수 F: ma50>ma500 골든크로스 순간 edge-trigger + 15~29% 구간 + 필수
#   매도 E: ma10<ma500 데드크로스 또는 손절/트레일/마감
# =============================================================================

MA_TREND_BUY_CTRT_MIN = 15.0        # 매수 대상 등락률 하한
MA_TREND_BUY_CTRT_MAX = 29.0        # 상한 (Strategy A 영역과 겹치지 않게)
MA_TREND_PREV_CTRT_MAX = 10.0       # 전일 등락률 상한 (작전주 차단)
MA_TREND_MIN_VOLUME = 100_000       # 최소 거래량
MA_TREND_STOP_LOSS_PCT = 0.03       # 매수가 대비 -3% 손절
MA_TREND_TRAIL_PCT = 0.03           # 고점 대비 -3% 트레일링
MA_TREND_TRAIL_ACTIVATE_PCT = 0.03  # 매수가 대비 +3% 상승 후 트레일 활성


def ma_trend_buy_signal(
    prdy_ctrt: float,
    prev_day_prdy_ctrt: float,
    acml_vol: float,
    ma50_cur: float,
    ma500_cur: float,
    ma50_prev: float,
    ma500_prev: float,
    already_holding: bool,
    now_hm: tuple[int, int],
) -> tuple[bool, str]:
    """Strategy B 매수 판단 — ma50 > ma500 골든크로스 순간 edge-trigger.

    조건 (AND):
      F1. 09:30 ≤ 시간 ≤ 14:30
      F2. 15% ≤ prdy_ctrt < 29%
      F3. ma50 > ma500 (현재)
      F4. 직전 틱 ma50 ≤ ma500 (골든크로스 edge)
      F5. 전일 등락률 < 10%
      F6. 거래량 ≥ 100,000주
      F7. 이미 보유 중 아님
    """
    h, m = now_hm
    if h < 9 or (h == 9 and m < 30):
        return False, "ma_trend_F1_시간대전"
    if h >= 15 or (h == 14 and m >= 30):
        return False, "ma_trend_F1_시간대후"
    if already_holding:
        return False, "ma_trend_F7_보유중"
    if not (MA_TREND_BUY_CTRT_MIN <= prdy_ctrt < MA_TREND_BUY_CTRT_MAX):
        return False, f"ma_trend_F2_구간({prdy_ctrt:.2f}%)"
    if prev_day_prdy_ctrt >= MA_TREND_PREV_CTRT_MAX:
        return False, f"ma_trend_F5_전일급등({prev_day_prdy_ctrt:.1f}%)"
    if acml_vol < MA_TREND_MIN_VOLUME:
        return False, f"ma_trend_F6_거래량부족({int(acml_vol):,})"
    # MA 값 유효성
    if ma50_cur <= 0 or ma500_cur <= 0 or ma50_prev <= 0 or ma500_prev <= 0:
        return False, "ma_trend_F3_MA누락"
    # F3+F4: 골든크로스 순간 (prev ≤, cur >)
    if not (ma50_prev <= ma500_prev and ma50_cur > ma500_cur):
        return False, f"ma_trend_F3_골든크로스아님(prev:{ma50_prev:.1f}/{ma500_prev:.1f},cur:{ma50_cur:.1f}/{ma500_cur:.1f})"
    return True, (
        f"ma_trend_매수_골든크로스(ma50:{ma50_prev:.1f}→{ma50_cur:.1f}, "
        f"ma500:{ma500_prev:.1f}→{ma500_cur:.1f}, ctrt:{prdy_ctrt:.2f}%)"
    )


def ma_trend_exit_signal(
    ma10_cur: float,
    ma500_cur: float,
    bidp1: float,
    buy_price: float,
    highest_since_buy: float,
    now_hm: tuple[int, int],
) -> tuple[bool, str]:
    """Strategy B 매도 판단 — ma10 < ma500 데드크로스 OR 손절 OR 트레일 OR 마감.

    OR 조건:
      E1. ma10 < ma500 데드크로스
      E2. bidp1 ≤ buy_price × (1 - 0.03) 손절
      E3. 고점 대비 -3% 트레일 (매수가 +3% 상승 후 활성)
      E4. 14:55 마감 청산
    """
    if buy_price <= 0 or bidp1 <= 0:
        return False, ""
    h, m = now_hm
    # E4 마감 청산
    if h == 14 and m >= 55:
        return True, "ma_trend_마감청산(14:55+)"
    if h >= 15:
        return True, "ma_trend_마감청산(15:00+)"
    # E2 손절
    if bidp1 <= buy_price * (1 - MA_TREND_STOP_LOSS_PCT):
        return True, f"ma_trend_손절-3%({bidp1}/{buy_price})"
    # E3 트레일링 (매수가 대비 +3% 상승 후 활성)
    if highest_since_buy > 0 and highest_since_buy >= buy_price * (1 + MA_TREND_TRAIL_ACTIVATE_PCT):
        trail_level = highest_since_buy * (1 - MA_TREND_TRAIL_PCT)
        if bidp1 <= trail_level:
            return True, f"ma_trend_트레일-3%(고가{int(highest_since_buy):,})"
    # E1 데드크로스
    if ma10_cur > 0 and ma500_cur > 0 and ma10_cur < ma500_cur:
        return True, f"ma_trend_ma10<ma500_데드크로스({ma10_cur:.1f}<{ma500_cur:.1f})"
    return False, ""


# =============================================================================
# [260423] Strategy C — 볼린저 밴드 squeeze → expansion + 하단 반등
#   매수 F: BB 폭 압축 후 확장 + bidp1 < bb_lower 이탈 후 복귀 + 첫 양틱
#   매도 E: bidp1 < bb_mid 절반 / < bb_lower 전량
# =============================================================================

BB_EXP_WIDTH_SQUEEZE = 20.0         # 이 값 미만이면 "압축" (참고: 정규화 고려 대상)
BB_EXP_WIDTH_EXPAND = 24.0          # 이 값 초과면 "확장"
BB_EXP_CROSSBACK_LOOKBACK = 30      # bb_lower 이탈 이력 조회 범위 (틱)
BB_EXP_BUY_CTRT_MIN = 0.0           # 하루 상승중 종목만 (하락중 제외)
BB_EXP_STOP_LOSS_PCT = 0.03
BB_EXP_TRAIL_PCT = 0.03
BB_EXP_TRAIL_ACTIVATE_PCT = 0.03


def bb_expansion_buy_signal(
    prdy_ctrt: float,
    bidp1: float,
    bidp1_prev: float,
    bb_lower_cur: float,
    bb_width_cur: float,
    bb_width_min_recent: float,
    bb_lower_crossed_recent: bool,
    bb_lower_crossed_prev_tick: bool,
    already_holding: bool,
    now_hm: tuple[int, int],
) -> tuple[bool, str]:
    """Strategy C 매수 판단 — BB squeeze→expansion + 하단 이탈-복귀 반등.

    입력:
      bb_width_min_recent : 최근 100틱 bb_width 최소값 (squeeze 확인)
      bb_lower_crossed_recent : 최근 N틱 내 bidp1 < bb_lower 발생 여부
      bb_lower_crossed_prev_tick : 직전 틱에 bidp1 < bb_lower 였는지 (복귀 순간 판정)

    조건 (AND):
      F1. 09:30 ≤ 시간 ≤ 14:30
      F2. 변동성 압축 이력 : bb_width_min_recent < 20
      F3. 변동성 확장 : bb_width_cur > 24
      F4. 하단 이탈 이력 있음
      F5. 직전 틱 이탈 상태 → 현재 틱 복귀 (bidp1 ≥ bb_lower)
      F6. 상승 시작 : bidp1 > bidp1_prev (첫 양틱)
      F7. 하루 상승중 : prdy_ctrt > 0
      F8. 중복 방지
    """
    h, m = now_hm
    if h < 9 or (h == 9 and m < 30):
        return False, "bb_exp_F1_시간대전"
    if h >= 15 or (h == 14 and m >= 30):
        return False, "bb_exp_F1_시간대후"
    if already_holding:
        return False, "bb_exp_F8_보유중"
    if prdy_ctrt <= BB_EXP_BUY_CTRT_MIN:
        return False, f"bb_exp_F7_하락중({prdy_ctrt:.2f}%)"
    if bb_width_min_recent >= BB_EXP_WIDTH_SQUEEZE:
        return False, f"bb_exp_F2_압축없음(min={bb_width_min_recent:.2f})"
    if bb_width_cur <= BB_EXP_WIDTH_EXPAND:
        return False, f"bb_exp_F3_확장아님(cur={bb_width_cur:.2f})"
    if not bb_lower_crossed_recent:
        return False, "bb_exp_F4_하단이탈이력없음"
    if not bb_lower_crossed_prev_tick:
        return False, "bb_exp_F5_직전틱이탈아님"
    if not (bidp1 >= bb_lower_cur):
        return False, f"bb_exp_F5_아직복귀전(bidp1={bidp1}/lower={bb_lower_cur:.1f})"
    if bidp1 <= bidp1_prev:
        return False, f"bb_exp_F6_상승아님(prev={bidp1_prev},cur={bidp1})"
    return True, (
        f"bb_exp_매수_압축후복귀반등(width:{bb_width_min_recent:.1f}→{bb_width_cur:.1f}, "
        f"lower={bb_lower_cur:.1f}, ctrt={prdy_ctrt:.2f}%)"
    )


def bb_expansion_exit_signal(
    bidp1: float,
    bb_mid_cur: float,
    bb_lower_cur: float,
    buy_price: float,
    highest_since_buy: float,
    now_hm: tuple[int, int],
) -> tuple[str, str]:
    """Strategy C 매도 판단.

    Returns (action, reason) where action in ("hold","sell_half","sell_all")

    OR 조건:
      E1. bidp1 < bb_mid → 1차 절반
      E2. bidp1 < bb_lower → 전량
      E3. 손절 -3%
      E4. 트레일 -3% (+3% 상승 후 활성)
      E5. 14:55 마감 전량
    """
    if buy_price <= 0 or bidp1 <= 0:
        return "hold", ""
    h, m = now_hm
    if h == 14 and m >= 55:
        return "sell_all", "bb_exp_마감청산(14:55+)"
    if h >= 15:
        return "sell_all", "bb_exp_마감청산(15:00+)"
    # 손절
    if bidp1 <= buy_price * (1 - BB_EXP_STOP_LOSS_PCT):
        return "sell_all", f"bb_exp_손절-3%({bidp1}/{buy_price})"
    # bb_lower 재이탈 전량
    if bb_lower_cur > 0 and bidp1 < bb_lower_cur:
        return "sell_all", f"bb_exp_bb_lower재이탈({bidp1}<{bb_lower_cur:.1f})"
    # 트레일링
    if highest_since_buy > 0 and highest_since_buy >= buy_price * (1 + BB_EXP_TRAIL_ACTIVATE_PCT):
        trail_level = highest_since_buy * (1 - BB_EXP_TRAIL_PCT)
        if bidp1 <= trail_level:
            return "sell_all", f"bb_exp_트레일-3%(고가{int(highest_since_buy):,})"
    # bb_mid 하회 — 1차 절반
    if bb_mid_cur > 0 and bidp1 < bb_mid_cur:
        return "sell_half", f"bb_exp_bb_mid하회절반({bidp1}<{bb_mid_cur:.1f})"
    return "hold", ""


def calc_uplimit_qty(avail_cash: float, buy_price: float, market: str = "KOSPI") -> int:
    """수량 계산. 수수료 0.5% 버퍼 반영."""
    if avail_cash <= 0 or buy_price <= 0:
        return 0
    rounded = round_to_tick(buy_price, market)
    if rounded <= 0:
        return 0
    return max(0, int(avail_cash * 0.995 / rounded))


def calc_entry_price(current_price: float, market: str = "KOSPI", n_ticks: int = 2) -> float:
    """지정가 매수 가격 = 현재가 + N틱. VI 발동 중이면 호출측에서 상한가로 override."""
    if current_price <= 0:
        return 0.0
    return price_plus_n_ticks(current_price, n_ticks, market)


# =============================================================================
# 종가매수 폭락 방지 필터 (문서: docs/02. Top30_str.md Part B)
# =============================================================================

CLOSING_FILTER_FRGN_BLOCK = -1.0           # 외국인 3일 순매수 < 0 이면 차단 (유일한 강제 차단)
CLOSING_FILTER_VP_MIN = 80.0               # [260423] 완화: 100 → 80 (종가매매 대상은 이미 20%+ 상승이라 VP 기준 완화 가능)
CLOSING_FILTER_TRADE_AMT_MIN = 3_000_000_000  # [260423] 완화: 50억 → 30억
CLOSING_FILTER_PASS_STRENGTH = 0.50        # [260423] 완화: 0.60 → 0.50 (점수 기반 가산은 보너스)


def closing_crash_filter_signal(
    prdy_ctrt: float,
    stck_prpr: float,
    stck_hgpr: float,
    prdy_sign: str,
    volume_power: float | None,
    acml_tr_pbmn: float,
    uplimit_bid_ratio: float | None,
    frgn_3d_net: float | None,
    vola_10d: float | None,
    institution_net_today: float | None = None,
) -> tuple[bool, str, float]:
    """
    15:18 종가매수 후보 선정 시 폭락 방지 필터.

    Returns (pass_filter, reason, strength):
      pass_filter : True 면 종가매수 대상 유지
      strength    : 최종 강도 (0.5 기준, 0.60 이상 통과)
    """
    # 기본 컷 (기존 closing_buy 조건)
    if prdy_sign not in ("1", "2"):
        return False, f"closing_prdy_sign({prdy_sign})", 0.0
    if prdy_ctrt < 20.0:
        return False, f"closing_등락률부족({prdy_ctrt:.1f}%)", 0.0
    if stck_hgpr > 0 and (stck_prpr / stck_hgpr) < 0.97:
        return False, f"closing_고가대비약({stck_prpr}/{stck_hgpr})", 0.0

    # [260424 사용자 원칙] "거래대금은 차단이 아닌 우선순위 기준"
    # → 거래대금/외인/VP 모두 차단 로직 제거. 선정된 종목은 모두 매수 대상.
    # 자원 부족 시 거래대금 큰 순서로 우선 배정 (_prepare_closing_buy_orders 에서 정렬).

    # Signal Strength (참고용 점수 — 통과 판정에는 영향 없음)
    s = 0.50
    vp_label = f"VP{volume_power:.0f}" if volume_power is not None else "VP_n/a"
    detail = [vp_label]

    if frgn_3d_net is not None and frgn_3d_net > 0:
        s += 0.15
        detail.append("외인매수")
    if volume_power is not None and volume_power >= 150.0:
        s += 0.10
        detail.append("VP강")
    if uplimit_bid_ratio is not None and uplimit_bid_ratio >= 0.20:
        s += 0.10
        detail.append(f"상잔{uplimit_bid_ratio:.2f}")
    elif uplimit_bid_ratio is not None and uplimit_bid_ratio < 0.05:
        s -= 0.10
    if vola_10d is not None and vola_10d < 0.02:
        s += 0.10
        detail.append("저변동")
    if institution_net_today is not None and institution_net_today > 0:
        s += 0.05

    # [260424 사용자 원칙] 기본 컷(prdy_sign/prdy_ctrt/hgpr) 통과 시 항상 매수 대상 인정.
    # 점수는 로그/정렬 참고용.
    return True, f"closing_통과({','.join(detail)})/s={s:.2f}", s


def closing_next_day_exit(
    antc_prce: float,
    buy_price: float,
    is_opening_tick: bool,
) -> tuple[bool, str]:
    """
    종가매수 보유 종목 익일 시초 폭락 감지.

    is_opening_tick=False (08:30~09:00 동시호가): 예상가 < 매수가 × 0.97 → 시초 시장가 매도
    is_opening_tick=True  (09:00 시초 체결 직후): 시초가 < 매수가 × 0.95 → 즉시 시장가 매도
    """
    if buy_price <= 0 or antc_prce <= 0:
        return False, ""
    if not is_opening_tick:
        if antc_prce < buy_price * 0.97:
            return True, f"closing_익일갭손절예상({antc_prce}/{buy_price})"
        return False, ""
    if antc_prce < buy_price * 0.95:
        return True, f"closing_익일시초손절({antc_prce}/{buy_price})"
    return False, ""


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
