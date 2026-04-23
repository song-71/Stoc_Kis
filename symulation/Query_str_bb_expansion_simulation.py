"""
Strategy C — 볼린저 밴드 squeeze → expansion + 하단 반등 백테스트
Query_str_bb_expansion_simulation.py

[전략]
매수 (AND):
  F1. 09:30 ≤ 시간 ≤ 14:30
  F2. bb_width 최근 100틱 최소값 < SQUEEZE_THRESH (압축 이력)
  F3. 현재 bb_width > EXPAND_THRESH (확장)
  F4. 최근 30틱 내 bidp1 < bb_lower 기록
  F5. 직전 틱 이탈 → 현재 복귀 (bidp1 ≥ bb_lower)
  F6. bidp1 > bidp1_prev (첫 양틱)
  F7. prdy_ctrt > 0 (하루 상승 중)

매도 (OR):
  E1. bidp1 < bb_mid → 1차 절반
  E2. bidp1 < bb_lower → 전량
  E3. 손절 -3%
  E4. 트레일 -3% (+3% 상승 후 활성)
  E5. 14:55 마감 전량

[임계값 그리드 서치]
- BB_WIDTH_SQUEEZE: 10 / 15 / 20 / 25
- BB_WIDTH_EXPAND: 22 / 24 / 28 / 30
- CROSS_LOOKBACK: 10 / 20 / 30 틱
- 정규화: 절대값 vs bb_width / stck_prpr × 100 (두 버전 비교)

[TODO — 실제 구현 필요]
1. 틱 parquet 로드 + BB(200틱, K=2) 계산
2. 종목별 bb_width 최근 100틱 deque, lower 이탈 최근 30틱 deque 추적
3. 매수/매도 신호 시뮬레이션
4. 임계값 그리드 서치 + 국면별 결과
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

DEFAULT_START = "20260320"
DEFAULT_END = "20260423"

BB_PERIOD = 200
BB_K = 2.0

# 그리드 서치 기본값
BB_WIDTH_SQUEEZE_DEFAULT = 20.0
BB_WIDTH_EXPAND_DEFAULT = 24.0
CROSS_LOOKBACK_DEFAULT = 30


def simulate(start: str, end: str, squeeze: float, expand: float, lookback: int) -> dict:
    """시뮬레이션 — 임계값 1조합에 대해."""
    # TODO: WSS 틱 데이터 로드 + BB 계산 + 매매 시뮬
    raise NotImplementedError("[TODO] 구현 필요")


def grid_search(start: str, end: str):
    """임계값 그리드 서치."""
    # TODO: squeeze × expand × lookback 조합 순회
    squeeze_vals = [10, 15, 20, 25]
    expand_vals = [22, 24, 28, 30]
    lookback_vals = [10, 20, 30]
    print(f"[grid] {len(squeeze_vals)*len(expand_vals)*len(lookback_vals)} 조합 테스트")
    # for s in squeeze_vals:
    #     for e in expand_vals:
    #         for lb in lookback_vals:
    #             r = simulate(start, end, s, e, lb)
    #             print(f"s={s} e={e} lb={lb} → {r}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=DEFAULT_END)
    ap.add_argument("--grid", action="store_true", help="그리드 서치 모드")
    args = ap.parse_args()

    print(f"[Strategy C] BB expansion backtest: {args.start} ~ {args.end}")
    print("[TODO] 구현 필요 — skeleton 상태")
    if args.grid:
        grid_search(args.start, args.end)


if __name__ == "__main__":
    main()
