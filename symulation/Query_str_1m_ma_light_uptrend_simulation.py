"""
Strategy B-v2 (방향 B): MA 골든크로스 + 가벼운 상승 (15~29%→3~15%)
Query_str_1m_ma_light_uptrend_simulation.py

목적: 기존 Strategy B 의 "급등 중 MA 골든크로스" 모순을 해결.
  본격 급등 전 초기 진입 포착 — 3~15% 상승 구간에서 MA 골든크로스.

매수 (AND):
  F1. MA5 > MA20 (현재)
  F2. 직전 분봉 MA5 ≤ MA20 (골든크로스 edge)
  F3. 3% ≤ tdy_ret < 15%   ← 가벼운 상승 구간
  F4. 거래량 ≥ 이전 5분 평균 × 1.5
  F5. 09:30 ≤ 시간 ≤ 14:30

매도:
  E1. MA5 < MA20 데드크로스
  E2. -3% 손절
  E3. 고점 -3% 트레일 (+3% 활성)
  E4. 14:55 마감
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from collections import Counter

sys.path.append(str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from Query_str_1m_combined_simulation import load_day, compute_indicators

STOP_LOSS_PCT = 0.03
TRAIL_PCT = 0.03
TRAIL_ACTIVATE_PCT = 0.03
MIN_VOL_FACTOR = 1.5

# 방향 B: 3~15% 구간
BUY_CTRT_MIN = 0.03
BUY_CTRT_MAX = 0.15


def simulate(df):
    trades = []
    by_code = df.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        if g.height < 25:
            continue
        pos = None
        for r in g.iter_rows(named=True):
            ma_f = r["ma_fast"]
            ma_s = r["ma_slow"]
            ma_f_p = r["ma_fast_prev"]
            ma_s_p = r["ma_slow_prev"]
            close = r["close"]
            vol = r["volume"]
            vol_ma5 = r["vol_ma5"]
            tdy_ret = r["tdy_ret"] or 0
            time_str = r["time"]
            if ma_f is None or ma_s is None or ma_f_p is None or ma_s_p is None:
                continue
            if pos is None:
                golden = (ma_f_p <= ma_s_p) and (ma_f > ma_s)
                in_range = (BUY_CTRT_MIN <= tdy_ret < BUY_CTRT_MAX)
                vol_ok = vol_ma5 is not None and vol_ma5 > 0 and vol >= vol_ma5 * MIN_VOL_FACTOR
                if golden and in_range and vol_ok:
                    pos = {"buy_price": close, "buy_time": time_str, "highest": close}
            else:
                if close > pos["highest"]:
                    pos["highest"] = close
                exit_reason = None
                if close <= pos["buy_price"] * (1 - STOP_LOSS_PCT):
                    exit_reason = "손절-3%"
                elif (pos["highest"] >= pos["buy_price"] * (1 + TRAIL_ACTIVATE_PCT)
                      and close <= pos["highest"] * (1 - TRAIL_PCT)):
                    exit_reason = "트레일-3%"
                elif ma_f < ma_s:
                    exit_reason = "데드크로스"
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
    print(f"Strategy B-v2 (방향 B) 백테스트: {dates}")
    print(f"매수 구간: {BUY_CTRT_MIN*100:.0f}~{BUY_CTRT_MAX*100:.0f}%, MA 골든크로스")
    print("=" * 75)
    all_trades = []
    for d in dates:
        df = load_day(d)
        if df is None:
            print(f"[{d}] 데이터 없음")
            continue
        df = compute_indicators(df)
        tr = simulate(df)
        all_trades.extend(tr)
        print(f"\n[{d}] 종목수={df['code'].n_unique()}")
        report(tr, f"B-v2 {d}")
    print("\n" + "=" * 75)
    print(f"[총합 {len(dates)}일]")
    report(all_trades, "B-v2 합계")


if __name__ == "__main__":
    main()
