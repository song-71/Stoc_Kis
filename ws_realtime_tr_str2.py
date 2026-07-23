"""
Str2 실시간 매수·매도 전략 모듈 (per-tick 순수 판단 함수)

설계 원칙 (서버 plug-in 규칙과 동일):
  · 이 파일은 **순수 전략 판단 함수만** 포함한다 (API 호출·파일 I/O·주문·체결 없음).
  · 주문 발주/취소/체결·포지션·현금·컬럼 기록은 호출자(드라이버)가 담당:
      - 로컬 백테스트: Fplt_kis_ws_trading_engine_str2.py 의 run_simulation_str2
      - 라이브: ws_realtime_trading.py (서버)
  · 공유 유틸(호가단위·상한가·수수료)은 **kis_utils 에서만** import 한다. (다른 전략파일 비의존)
  · 함수는 per-tick 스칼라 입력 + 호출자 보유 상태를 받아 (signal/origin/reason) 를 반환한다.
    히스토리가 필요한 값(직전 50틱 최저/최고, N틱 전 MA, 윈도우 고점)은 호출자가 미리 계산해 넘긴다.

전략 요약 (자세한 정의는 docs 및 백테스트 드라이버 docstring 참조):
  [매수대상] 09:00 이전 데이터 보유(전일 상한가) 종목.  [손절/트레일] 매수가/최고가 대비 -3%.
  b1) 장전 하락 → 09:00후 bidp0<bb_lo & ma10>=ma50 → 직전50틱 bidp0 최저가 지정가 → (ma50>ma200) 시장가전환
  b2) 장전 상승 → 09:00 시초가 시장가
  b3) 장중 상승추세 눌림목(bb_under) & ma10>ma50 → 직전50틱 bidp0 최저가 지정가 → 추세강화 시 시장가전환
  매도) 손절/트레일 / ma500gap·sell_ready 지정가매도 / 미체결강제 / ma10·ma500 데드크로스 / 15:18 비상한가 청산
  매수필터(토글) USE_BUY_FILTER: ma2000기울기 + 당일과열 + Lower-High 로 일시눌림 vs 하락추세 구분
"""
from __future__ import annotations
from dataclasses import dataclass, field

# 공유 유틸은 kis_utils 단일 출처에서만 (전략파일끼리 비의존)
from kis_utils import (  # noqa: F401  (드라이버/서버가 재노출용으로 import 하기도 함)
    round_to_tick,
    get_tick_size,
    calc_limit_up_price,
    FEE_RATE_MARKET_BUY,
    FEE_RATE_MARKET_SELL,
    FEE_RATE_LIMIT_BUY,
    FEE_RATE_LIMIT_SELL,
)

# ── 시간 상수 (장중 초) ──────────────────────────────────────────────────────
T_0855 = 8 * 3600 + 55 * 60   # 31500
T_0858 = 8 * 3600 + 58 * 60   # 32280
T_0900 = 9 * 3600             # 32400
T_1518 = 15 * 3600 + 18 * 60  # 55080 (종가 청산 판정 시점)

# ── 전략 파라미터 (단일 출처; 드라이버/서버가 import) ───────────────────────────
ROLL = 50                # "직전 50틱 이내" 윈도우 (지정가 기준가)
STOP_LOSS_PCT = 0.03     # 손절 -3%
TRAIL_STOP_PCT = 0.03    # 트레일스톱 -3%
MA500_GAP_PCT = 0.03     # ma500gap 기준 +3%
DEPTH_FLOOR_PCT = 0.04   # [260723] 진입 깊이 하한 4%(4% 미만 얕은 눌림목 제외) — 전 기간 스윕 최적·검증본·뷰어 통일
                         #   (0.5~2% 얕은구간 포함 → 장부에 깊이% 기록되므로 나중에 제외/포함 성과 재분석).
                         #   ※검증본 결론: 얕은구간 기대값≈0(2%하한이 총수익·MDD 우월). 지정가면 소폭 흑자 가능.
STOCH_N = 1500           # 스토캐스틱 %K 기간 (데이터 단계와 동일)
STOCH_M = 100

# 매수 필터 (토글): 일시눌림 반등 vs 하락추세 진입 구분
USE_BUY_FILTER = False
SLOPE_LOOKBACK = 500     # ma2000 기울기 비교 구간(틱)
SLOPE_THRESH_PCT = 0.0   # ma2000 SLOPE_LOOKBACK틱 %기울기 하한(>= 통과)
MAX_GAIN_PCT = 0.30      # 당일 저점 대비 상승률 상한
SWING_W = 300            # Lower-High 비교 윈도우(틱)


def _isnan(x: float) -> bool:
    return x != x


# ── 상태 컨테이너 (서버/드라이버가 종목별로 보유) ─────────────────────────────
@dataclass
class Str2State:
    """종목별 전략 상태. 호출자가 틱마다 갱신하며 전략 함수에 전달."""
    # 세션 셋업(장 시작 시 1회)
    b1_enabled: bool = False
    b2_pending: bool = False
    b2_used: bool = False
    prev_close: float = 0.0
    limit_up: float = 0.0
    # 진행 상태
    position: bool = False
    buy_origin: str = ""           # "b1" | "b3"
    buy_order_active: bool = False
    buy_order_price: float = 0.0
    sell_order_active: bool = False
    sell_order_price: float = 0.0
    sell_ready: bool = False
    buy_price: float = 0.0         # 손절/트레일 기준
    highest: float = 0.0           # 트레일 기준(매수 후 최고가)
    running_low: float = 0.0       # 당일(정규장) 저점 (매수필터②)
    closing_checked: bool = False  # 15:18 1회 판정
    carry_hold: bool = False       # 15:18 상한가 → 보유 이월
    limit_up_touched: bool = False # 당일 상한가 도달 이력(상한가 이탈→오더북 구독 트리거)
    pullback_low: float = 0.0      # 상한가 이탈 후 눌림 저점(상한가 재도달 시 리셋) — 진입 깊이 하한 판정
    # [260722] 검증본 상한가 눌림목 진입 상태머신 (sweep_exec_sim.py 의 armed/escaped/esc)
    armed: bool = False            # 상한가 도달 후 무장(진입 후 해제, 상한가 재도달 시 재무장)
    esc_count: int = 0             # ma10<bb_lo 연속 카운트
    escaped: bool = False          # esc_count>=ESC_MIN → 밴드 하단 이탈 확정(복귀 대기)
    entries: int = 0               # 당일 진입 횟수(MAXE 제한)
    qty: int = 0                   # 보유 수량 (체결통보로 갱신; 호출자 bookkeeping)
    last_buy_ordno: str = ""       # 직전 매수 주문번호 (취소/체결 매칭)
    last_sell_ordno: str = ""      # 직전 매도 주문번호
    last_sell_reason: str = ""     # 직전 매도 사유
    ma10_prev: float = 0.0         # 직전 틱 ma10 (데드크로스 판정)
    ma500_prev: float = 0.0        # 직전 틱 ma500
    ma2000_prev: float = 0.0       # 직전 틱 ma2000 (ma500<ma2000 데드크로스 판정)
    golden_seen: bool = False      # ma500>=ma2000(정배열) 관측 래치 → 이후 하향교차 시 매도
    buy_order_tick: int = -1       # 매수 지정가 발주 시점 tick_idx (can_fill 판정)
    sell_order_tick: int = -1      # 매도 지정가 발주 시점 tick_idx
    buy_order_qty: int = 0         # 매수 지정가 주문수량 (미체결 취소 시 사용)
    # [260722 체결모델 확정] SIMUL 흐름체결용: 지정가 주문시점 '내 앞 대기물량'(가격≥P 매수호가 잔량).
    #   이후 P 이하 체결이 올 때마다 이 대기물량을 먼저 소진하고, 남는 만큼만 내 체결로 잡는다.
    buy_queue: float = 0.0         # 매수 지정가 내 앞 대기물량(남은 값, 소진하며 감소)
    sell_queue: float = 0.0        # 매도 지정가 내 앞 대기물량(가격≤S 매도호가 잔량)
    tick_idx: int = 0              # 종목별 누적 틱 카운터


# ── 세션 셋업 ────────────────────────────────────────────────────────────────
def classify_premarket(price_0855, price_0858: float, ma50_0858: float) -> tuple[bool, bool]:
    """08:58 시점 분류 → (b1_enabled, b2_pending).
    상승(현재가>08:55 & >ma50) → b2_pending, 하락(현재가<08:55 & <ma50) → b1_enabled."""
    b1_enabled = False
    b2_pending = False
    if price_0855 is not None and price_0858 > 0 and ma50_0858 > 0:
        if price_0858 > price_0855 and price_0858 > ma50_0858:
            b2_pending = True
        elif price_0858 < price_0855 and price_0858 < ma50_0858:
            b1_enabled = True
    return b1_enabled, b2_pending


def is_limit_up(price: float, prdy_ctrt: float, limit_up: float) -> bool:
    """현재 상한가 상태인가 (가격이 상한가 도달 또는 전일대비 +29% 이상)."""
    if limit_up > 0 and price > 0 and price >= limit_up - 1e-6:
        return True
    return prdy_ctrt >= 29.0


# ── 추세/필터 ────────────────────────────────────────────────────────────────
def compute_trends(ma50, ma200, ma300, ma500, ma2000, wghn, tick_idx: int) -> tuple[bool, bool]:
    """정배열 상승추세 판정 → (ma500_up_trend, ma2000_up_trend).
    tick_idx>=2000 & ma2000>0 이면 ma2000 기준, 아니면 ma500 기준."""
    ma2000_avail = (tick_idx >= 2000 and ma2000 > 0)
    ok = (ma50 > 0 and ma200 > 0 and ma300 > 0 and ma500 > 0 and wghn > 0)
    ma500_up = (not ma2000_avail) and ok and (ma50 > ma500 and ma200 > ma500 and ma300 > ma500 and ma500 > wghn)
    ma2000_up = ma2000_avail and ok and (ma50 > ma2000 and ma200 > ma2000 and ma300 > ma2000 and ma500 > ma2000 and ma2000 > wghn)
    return ma500_up, ma2000_up


def buy_filter_allowed(
    tick_idx: int, ma2000: float, ma2000_lookback: float,
    running_low: float, prpr: float,
    recent_hi_w, prev_hi_w,
    slope_thresh_pct: float = SLOPE_THRESH_PCT,
    max_gain_pct: float = MAX_GAIN_PCT,
) -> bool:
    """일시눌림 반등 vs 하락추세 진입 게이트 (USE_BUY_FILTER 시에만 적용).
    ① ma2000 %기울기 >= 임계 (warmup: ma2000_lookback<=0 면 통과)
    ② 당일 저점 대비 상승률 <= 상한
    ③ Lower-High 회피 (recent_hi_w/prev_hi_w 가 None 이면 warmup → 통과)
    호출자가 ma2000_lookback(= ma2000[i-SLOPE_LOOKBACK]) 와 윈도우 고점을 미리 계산해 전달."""
    if tick_idx >= 2000 + SLOPE_LOOKBACK and ma2000 > 0 and ma2000_lookback > 0:
        if (ma2000 / ma2000_lookback - 1) < slope_thresh_pct:
            return False
    if running_low > 0 and prpr > 0 and (prpr / running_low - 1) > max_gain_pct:
        return False
    if recent_hi_w is not None and prev_hi_w is not None and recent_hi_w < prev_hi_w:
        return False
    return True


# ── 매수 판단 ────────────────────────────────────────────────────────────────
def buy_b1b3_origin(
    b1_enabled: bool, allowed: bool,
    bidp0: float, bb_lo: float, ma10: float, ma50: float, ma300: float, ma500: float,
    ma500_up: bool, ma2000_up: bool,
) -> str:
    """정규장 지정가 매수주문 발주 조건 → "" | "b1" | "b3".
    b1: bidp0<bb_lo & ma10>=ma50 (장전하락 활성 시)
    b3: bb_under((상승추세 & 단기꺾임) & ma10<bb_lo) & ma10>ma50"""
    bb_under = (((ma500_up and ma50 < ma300) or (ma2000_up and ma50 < ma500))
                and (ma10 > 0 and bb_lo > 0 and ma10 < bb_lo))
    if allowed and b1_enabled and bidp0 > 0 and bb_lo > 0 and bidp0 < bb_lo and ma10 > 0 and ma50 > 0 and ma10 >= ma50:
        return "b1"
    if allowed and bb_under and ma10 > ma50:
        return "b3"
    return ""


def buy_escalation(
    buy_origin: str, ma50: float, ma200: float, ma300: float, ma500: float,
    ma500_up: bool, ma2000_up: bool,
) -> str:
    """지정가 매수주문 대기 중 시장가 전환 사유 → "" | reason."""
    if buy_origin == "b1" and ma50 > 0 and ma200 > 0 and ma50 > ma200:
        return "str2_b1_시장가전환(ma50>ma200)"
    if buy_origin == "b3" and ((ma500_up and ma50 > ma300) or (ma2000_up and ma50 > ma500)):
        return "str2_b3_시장가전환(추세강화)"
    return ""


# ── 매도 판단 ────────────────────────────────────────────────────────────────
def sell_stop_trail(
    buy_price: float, pr: float, highest: float,
    stop_loss_pct: float = STOP_LOSS_PCT, trail_stop_pct: float = TRAIL_STOP_PCT,
) -> tuple[bool, str]:
    """[260722] 상한가 눌림목 검증본 반영 — 손절/트레일 '없음'(손절이 오히려 손해라 백테스트로 확인).
    항상 미발동. (매도는 상한가 재도달 / ma500<ma2000 데드크로스 / EOD 만.)"""
    return False, ""


def depth_floor_ok(limit_up: float, pullback_low: float, floor_pct: float = DEPTH_FLOOR_PCT) -> bool:
    """[260722] 검증본: 진입 깊이 하한 — 상한가 대비 눌림 저점 하락폭 >= floor_pct(2%) 여야 진입 허용.
    (하한만; 상한 없음. limit_up/pullback_low 미확정이면 보수적으로 False.)"""
    if limit_up > 0 and pullback_low > 0:
        return (limit_up - pullback_low) / limit_up >= floor_pct
    return False


def compute_ma500gap(bidp0: float, ma500: float, gap_pct: float = MA500_GAP_PCT) -> bool:
    """bidp0/ma500-1 > gap_pct → True."""
    return bidp0 > 0 and ma500 > 0 and (bidp0 / ma500 - 1) > gap_pct


def sell_ready_set(stoch_k: float, bb_himax: float, bb_hi: float, ma10: float) -> bool:
    """sell_ready 설정 조건: stock_k 유효 & bb_hi_max==bb_hi & ma10>bb_hi & stock_k==100."""
    return ((not _isnan(stoch_k)) and bb_himax > 0 and bb_hi > 0
            and (bb_hi >= bb_himax - 1e-9) and (ma10 > bb_hi) and (stoch_k >= 100.0 - 1e-9))


def sell_ready_release(ma50: float, ma200: float, buy_order_active: bool) -> bool:
    """sell_ready 해제 조건: ma50<ma200 or 매수주문 존재."""
    return (ma50 > 0 and ma200 > 0 and ma50 < ma200) or buy_order_active


def sell_limit_place_ok(sell_ready: bool, sell_order_active: bool, ma500gap: bool,
                        ma10: float, ma50: float) -> bool:
    """sell_ready & ma500gap & ma10<ma50 → 지정가 매도주문 발주."""
    return sell_ready and (not sell_order_active) and ma500gap and ma10 > 0 and ma50 > 0 and ma10 < ma50


def sell_order_resolve(sell_order_active: bool, ma50: float, ma200: float, can_fill: bool) -> tuple[bool, str, bool]:
    """지정가 매도주문 처리 → (do_sell, reason, use_limit).
    ma50<ma200 → 미체결 강제 시장가(use_limit=False); can_fill(가격이 주문가 도달) → 지정가 체결(use_limit=True).
    can_fill 은 호출자가 (i>주문틱 & pr>=주문가)로 계산해 전달."""
    if not sell_order_active:
        return False, "", False
    if ma50 > 0 and ma200 > 0 and ma50 < ma200:
        return True, "str2_매도미체결_강제매도(ma50<ma200)", False
    if can_fill:
        return True, "str2_지정가매도체결", True
    return False, "", False


def sell_deadcross(ma500gap: bool, ma10_prev: float, ma500_prev: float,
                   ma10: float, ma500: float,
                   ma2000_prev: float = 0.0, ma2000: float = 0.0,
                   golden_seen: bool = False) -> tuple[bool, str]:
    """[260722] 검증본 반영 — ma500 vs ma2000 데드크로스로 변경(구 ma10<ma500 대체).
    골든(ma500>ma2000) 관측 후 ma500<ma2000 하향교차 시 매도. 드라이버가 golden_seen(state) 갱신.
    ma2000 미제공(워밍업) 시 구 로직(ma10<ma500)으로 폴백."""
    if ma2000 > 0 and ma2000_prev > 0 and ma500 > 0 and ma500_prev > 0:
        if golden_seen and ma500_prev >= ma2000_prev and ma500 < ma2000:
            return True, "str2_ma500ma2000_데드크로스_시장가"
        return False, ""
    # 폴백(ma2000 워밍업 전): 구 ma10<ma500
    if ((not ma500gap) and ma10_prev > 0 and ma500_prev > 0 and ma10_prev > ma500_prev
            and ma10 > 0 and ma500 > 0 and ma10 < ma500):
        return True, "str2_ma10ma500_데드크로스_시장가(폴백)"
    return False, ""


def carry_nextday_premarket_sell(antc_or_pr: float, wghn: float) -> tuple[bool, str]:
    """15:18 상한가 이월(carry_hold) 포지션의 '익일 재개' 규칙.

    str1 의 장전 매도와 동일 사상 — 익일 개장 전(예상체결가 antc_prce 또는 현재가)이
    가중평균가(wghn)를 하락 이탈하면 개장 전 시장가 매도 주문을 낸다.
    (정규장 진입 후에는 str2 일반 매도 로직 sell_* 가 그대로 재개된다.)
    ※ 로컬 백테스트(단일일자)는 익일을 모델링하지 않아 이 함수를 호출하지 않으며,
       라이브(ws_realtime_trading.py)의 익일 장전 처리부에서 호출한다.
    """
    if antc_or_pr > 0 and wghn > 0 and antc_or_pr < wghn:
        return True, "str2_이월_익일장전매도(wghn이탈)"
    return False, ""
