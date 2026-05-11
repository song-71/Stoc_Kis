"""
Strategy B-v3 (방향 A): 급등 종목 breakout — N분 최고가 돌파
Query_str_1m_breakout_simulation.py

설계 원리:
  15~29% 급등 종목의 **상승 관성에 편승**. MA 크로스는 급등 종목에 부적합.
  대신 "최근 N분 최고가를 돌파" + "거래량 급증" 으로 **추세 지속** 감지.

매수 (AND):
  F1. 09:30 ≤ 시간 ≤ 14:30
  F2. 15% ≤ tdy_ret < 29%   ← 급등 구간
  F3. current close > max(high[-5:-0])  ← 최근 5분 최고가 돌파
  F4. 현재 분봉 양봉 (close > open)
  F5. 현재 분봉 거래량 ≥ 이전 5분 평균 × 2.0  ← 거래량 급증 확인

매도 (OR):
  E1. -3% 손절
  E2. 고점 -3% 트레일 (+3% 활성)
  E3. close < 매수 당시 low (breakout 실패)
  E4. 14:55 마감
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from collections import Counter

sys.path.append(str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from Query_str_1m_combined_simulation import load_day

import polars as pl

STOP_LOSS_PCT = 0.03
TRAIL_PCT = 0.03
TRAIL_ACTIVATE_PCT = 0.03
BUY_CTRT_MIN = 0.15
BUY_CTRT_MAX = 0.29
BREAKOUT_LOOKBACK = 5        # 최근 5분 최고가 돌파
VOL_SURGE_FACTOR = 2.0       # 거래량 평균 × 2


def compute_breakout_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """breakout 관련 지표 계산."""
    df = df.with_columns([
        # 직전 5분 최고가 (현재 분봉은 제외: shift 1 후 rolling max)
        pl.col("high").shift(1).rolling_max(BREAKOUT_LOOKBACK, min_samples=BREAKOUT_LOOKBACK)
        .over("code").alias("prev_5m_high"),
        pl.col("volume").rolling_mean(5, min_samples=3).over("code").alias("vol_ma5"),
        pl.col("open").alias("bar_open"),
    ])
    return df


def simulate(df):
    trades = []
    by_code = df.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        if g.height < 10:
            continue
        pos = None
        for r in g.iter_rows(named=True):
            close = r["close"]
            bar_open = r["bar_open"]
            low = r["low"]
            vol = r["volume"]
            vol_ma5 = r["vol_ma5"]
            prev_5m_high = r["prev_5m_high"]
            tdy_ret = r["tdy_ret"] or 0
            time_str = r["time"]

            if close is None or bar_open is None:
                continue

            if pos is None:
                # 매수 판정
                if prev_5m_high is None or vol_ma5 is None or vol_ma5 <= 0:
                    continue
                breakout = (close > prev_5m_high)
                bull = (close > bar_open)
                vol_surge = (vol >= vol_ma5 * VOL_SURGE_FACTOR)
                in_range = (BUY_CTRT_MIN <= tdy_ret < BUY_CTRT_MAX)
                in_window = (time_str >= "093000" and time_str <= "143000")
                if breakout and bull and vol_surge and in_range and in_window:
                    pos = {
                        "buy_price": close,
                        "buy_time": time_str,
                        "buy_low": low,   # breakout 실패 판정용
                        "highest": close,
                    }
            else:
                if close > pos["highest"]:
                    pos["highest"] = close
                exit_reason = None
                if close <= pos["buy_price"] * (1 - STOP_LOSS_PCT):
                    exit_reason = "손절-3%"
                elif (pos["highest"] >= pos["buy_price"] * (1 + TRAIL_ACTIVATE_PCT)
                      and close <= pos["highest"] * (1 - TRAIL_PCT)):
                    exit_reason = "트레일-3%"
                elif close < pos["buy_low"]:
                    exit_reason = "breakout실패"
                elif time_str >= "145500":
                    exit_reason = "마감"
                if exit_reason:
                    pnl = (close / pos["buy_price"] - 1) * 100
                    trades.append({
                        "code": code, "buy_time": pos["buy_time"], "buy_price": pos["buy_price"],
                        "sell_time": time_str, "sell_price": close, "pnl_pct": pnl,
                        "exit_reason": exit_reason,
                    })
                    pos = None
        if pos is not None and g.height > 0:
            last = g.row(-1, named=True)
            pnl = (last["close"] / pos["buy_price"] - 1) * 100
            trades.append({
                "code": code, "buy_time": pos["buy_time"], "buy_price": pos["buy_price"],
                "sell_time": last["time"], "sell_price": last["close"], "pnl_pct": pnl,
                "exit_reason": "장마감자동청산",
            })
    return trades


def report(trades, label):
    if not trades:
        print(f"  [{label}] 거래 0건")
        return
    n = len(trades)
    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    total = sum(t["pnl_pct"] for t in trades)
    big_w = sum(1 for t in trades if t["pnl_pct"] >= 3.0)
    big_l = sum(1 for t in trades if t["pnl_pct"] <= -2.0)
    print(f"  [{label}] 거래={n}건 | 승률={wins/n*100:5.1f}% | 총PnL={total:+.2f}% | "
          f"평균={total/n:+.3f}% | +3%↑={big_w} | -2%↓={big_l}")
    cnt = Counter(t["exit_reason"] for t in trades)
    for reason, cn in cnt.most_common():
        rt = [t for t in trades if t["exit_reason"] == reason]
        avg = sum(t["pnl_pct"] for t in rt) / max(1, cn)
        print(f"    {reason:<18} {cn:>4}건 평균 {avg:+.2f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="20260422,20260423")
    args = ap.parse_args()
    dates = [d.strip() for d in args.dates.split(",")]
    print(f"Strategy B-v3 (방향 A - breakout) 백테스트: {dates}")
    print(f"매수: 15~29% 급등 + 최근 5분 최고가 돌파 + 거래량×2 + 양봉")
    print("=" * 75)
    all_trades = []
    for d in dates:
        df = load_day(d)
        if df is None:
            print(f"[{d}] 데이터 없음")
            continue
        df = compute_breakout_indicators(df)
        tr = simulate(df)
        all_trades.extend(tr)
        print(f"\n[{d}] 종목수={df['code'].n_unique()}")
        report(tr, f"A {d}")
    print("\n" + "=" * 75)
    print(f"[총합 {len(dates)}일]")
    report(all_trades, "A 합계")


if __name__ == "__main__":
    main()
