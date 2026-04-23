"""
A+B+C 통합 비교 백테스트
Query_str_combined_simulation.py

[목적]
- Strategy A (상한가 근접 28% 돌파, 기존)
- Strategy B (MA trend, ma50>ma500)
- Strategy C (BB 압축 확장 반등)

동일 기간 동일 데이터로 각 전략 + 조합의 성과를 비교.

[평가 지표]
- 총 거래 건수 (N)
- 승률 (Win Rate)
- 평균 수익률 (Avg Return)
- 최대 손실 (Max Loss)
- MDD (Max Drawdown)
- 샤프 비율 (옵션)

[국면 분석]
KOSPI 일등락률 기준:
- 상승장 (> +0.5%)
- 횡보 (-0.5% ~ +0.5%)
- 하락장 (< -0.5%)

각 전략이 어느 국면에서 유효한지 분석.

[TODO — 실제 구현]
1. 각 개별 simulation 모듈 import / 로직 재사용
2. 공통 로더 + 순환 평가
3. 표 형태 결과 출력 (stdout + CSV 저장)

[사용법]
  python Query_str_combined_simulation.py --start 20260320 --end 20260423
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

DEFAULT_START = "20260320"
DEFAULT_END = "20260423"


def run_strategy_a(start: str, end: str) -> dict:
    """Strategy A: 상한가 근접 28% 돌파."""
    # TODO: 기존 Query_uplimit_approach_filtered_simulation.py 결과 재사용
    return {"name": "A_uplimit", "trades": 0, "win_rate": 0, "avg_ret": 0, "mdd": 0}


def run_strategy_b(start: str, end: str) -> dict:
    """Strategy B: MA trend ma50>ma500."""
    # TODO: Query_str_ma_trend_simulation.py 호출
    return {"name": "B_ma_trend", "trades": 0, "win_rate": 0, "avg_ret": 0, "mdd": 0}


def run_strategy_c(start: str, end: str) -> dict:
    """Strategy C: BB 압축 확장 반등."""
    # TODO: Query_str_bb_expansion_simulation.py 호출
    return {"name": "C_bb_exp", "trades": 0, "win_rate": 0, "avg_ret": 0, "mdd": 0}


def classify_regime(date_str: str) -> str:
    """KOSPI 일등락률 기반 시장국면 분류."""
    # TODO: 1d parquet 에서 KOSPI 지수 등락률 조회
    # return "UP" / "FLAT" / "DOWN"
    return "UNKNOWN"


def print_comparison(results: list[dict]):
    """비교 표 출력."""
    print(f"\n{'전략':<15} {'거래수':>8} {'승률':>8} {'평균수익':>10} {'MDD':>8}")
    print("-" * 60)
    for r in results:
        print(f"{r['name']:<15} {r['trades']:>8} {r['win_rate']:>7.1%} "
              f"{r['avg_ret']:>9.2%} {r['mdd']:>7.2%}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=DEFAULT_END)
    args = ap.parse_args()

    print(f"[Combined] A+B+C 비교 백테스트: {args.start} ~ {args.end}")
    print("[TODO] 구현 필요 — skeleton 상태")

    results = [
        run_strategy_a(args.start, args.end),
        run_strategy_b(args.start, args.end),
        run_strategy_c(args.start, args.end),
    ]
    print_comparison(results)


if __name__ == "__main__":
    main()
