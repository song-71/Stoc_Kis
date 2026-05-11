"""
Strategy C — 파라미터 그리드 서치
Query_str_1m_bb_grid_search.py

변수 (8 × 3 × 3 × 3 = 216 조합 → 실행 가능한 핵심 조합만):
  SQUEEZE_PCT : 2.0 / 3.0 / 4.0
  EXPAND_PCT  : 4.0 / 5.0 / 6.0
  TRAIL_ACTIVATE : 0.01 / 0.02 / 0.03 (트레일 활성 임계 +1%/+2%/+3%)
  TRAIL_STOP : 0.02 / 0.03 (고점 대비 -2%/-3%)
  EXIT_MODE  : "bb_mid" / "bb_mid_2bar" / "none"  (bb_mid 하회 즉시 / 2분 연속 / 안 함)

데이터: WSS 틱 → 1분봉 집계, 최근 20일
"""
from __future__ import annotations
import argparse
import sys
import itertools
from pathlib import Path
from collections import Counter, deque

sys.path.append(str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from Query_str_1m_bb_from_wss_extended import load_and_aggregate

import polars as pl

BB_PERIOD = 20
BB_K = 2.0
BB_CROSS_LOOKBACK = 10
STOP_LOSS_PCT = 0.03


def compute_bb_common(df):
    df = df.with_columns([
        pl.col("close").rolling_mean(BB_PERIOD, min_samples=BB_PERIOD).over("code").alias("bb_mid"),
        pl.col("close").rolling_std(BB_PERIOD, min_samples=BB_PERIOD).over("code").alias("bb_std"),
    ])
    df = df.with_columns([
        (pl.col("bb_mid") + BB_K * pl.col("bb_std")).alias("bb_upper"),
        (pl.col("bb_mid") - BB_K * pl.col("bb_std")).alias("bb_lower"),
        (2 * BB_K * pl.col("bb_std") / pl.col("close") * 100).alias("bb_width_pct"),
        pl.col("close").shift(1).over("code").alias("close_prev"),
    ])
    return df


def simulate_one_config(df, cfg):
    squeeze = cfg["squeeze"]
    expand = cfg["expand"]
    trail_act = cfg["trail_act"]
    trail_stop = cfg["trail_stop"]
    exit_mode = cfg["exit_mode"]

    trades = []
    by_code = df.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        if g.height < BB_PERIOD + 5:
            continue
        width_hist = deque(maxlen=100)
        cross_hist = deque(maxlen=BB_CROSS_LOOKBACK)
        pos = None
        bb_mid_below_count = 0
        for r in g.iter_rows(named=True):
            close = r["close"]
            low = r["low"]
            bb_mid = r["bb_mid"]
            bb_lower = r["bb_lower"]
            bb_width_pct = r["bb_width_pct"]
            close_prev = r["close_prev"]
            tdy_ret = r["tdy_ret"] or 0
            hhmm = r["hhmm"]
            if bb_mid is None or bb_lower is None or bb_width_pct is None or close_prev is None:
                continue
            prev_below = cross_hist[-1] if cross_hist else False
            cur_below = (low < bb_lower)
            had_below_recent = any(cross_hist) if cross_hist else False
            min_width = min(width_hist) if width_hist else bb_width_pct

            if pos is None:
                if (
                    tdy_ret > 0
                    and min_width < squeeze
                    and bb_width_pct > expand
                    and had_below_recent
                    and prev_below
                    and close >= bb_lower
                    and close > close_prev
                ):
                    pos = {"buy": close, "high": close}
                    bb_mid_below_count = 0
            else:
                if close > pos["high"]:
                    pos["high"] = close
                exit_reason = None
                if close <= pos["buy"] * (1 - STOP_LOSS_PCT):
                    exit_reason = "손절"
                elif bb_lower and close < bb_lower:
                    exit_reason = "lower재이탈"
                elif (pos["high"] >= pos["buy"] * (1 + trail_act)
                      and close <= pos["high"] * (1 - trail_stop)):
                    exit_reason = f"트레일"
                elif exit_mode != "none" and bb_mid and close < bb_mid:
                    bb_mid_below_count += 1
                    if exit_mode == "bb_mid":
                        exit_reason = "bb_mid"
                    elif exit_mode == "bb_mid_2bar" and bb_mid_below_count >= 2:
                        exit_reason = "bb_mid_2bar"
                else:
                    bb_mid_below_count = 0
                if exit_reason is None and hhmm >= "1455":
                    exit_reason = "마감"
                if exit_reason:
                    pnl = (close / pos["buy"] - 1) * 100
                    trades.append({"code": code, "pnl_pct": pnl, "reason": exit_reason})
                    pos = None
            width_hist.append(bb_width_pct)
            cross_hist.append(cur_below)
        if pos is not None and g.height > 0:
            last = g.row(-1, named=True)
            pnl = (last["close"] / pos["buy"] - 1) * 100
            trades.append({"code": code, "pnl_pct": pnl, "reason": "자동마감"})
    return trades


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20, help="최근 N일")
    args = ap.parse_args()

    # WSS 디렉터리에서 최근 N일 선택
    from pathlib import Path as _Path
    wss_dir = _Path("/home/ubuntu/Stoc_Kis/data/wss_data")
    all_dates = sorted({f.name.split("_")[0] for f in wss_dir.glob("*_wss_data.parquet")})
    dates = all_dates[-args.n:]
    print(f"[grid_search] 대상 일자: {len(dates)}일 — {dates[0]}~{dates[-1]}")

    # 일자별 1분봉 집계 + BB 계산 (한 번만)
    day_dfs = []
    for d in dates:
        df = load_and_aggregate(d)
        if df is None or df.height == 0:
            continue
        day_dfs.append((d, compute_bb_common(df)))
    print(f"[grid_search] 로드 일수: {len(day_dfs)}일")

    # 그리드
    param_space = {
        "squeeze": [2.0, 3.0, 4.0],
        "expand":  [4.0, 5.0, 6.0],
        "trail_act":  [0.01, 0.02, 0.03],
        "trail_stop": [0.02, 0.03],
        "exit_mode":  ["bb_mid", "bb_mid_2bar", "none"],
    }
    keys = list(param_space.keys())
    combos = list(itertools.product(*param_space.values()))
    print(f"[grid_search] 조합 수: {len(combos)}")

    results = []
    for combo in combos:
        cfg = dict(zip(keys, combo))
        all_trades = []
        for d, df2 in day_dfs:
            tr = simulate_one_config(df2, cfg)
            all_trades.extend(tr)
        n = len(all_trades)
        if n == 0:
            continue
        wins = sum(1 for t in all_trades if t["pnl_pct"] > 0)
        total = sum(t["pnl_pct"] for t in all_trades)
        big_w = sum(1 for t in all_trades if t["pnl_pct"] >= 3.0)
        big_l = sum(1 for t in all_trades if t["pnl_pct"] <= -2.0)
        results.append({
            **cfg,
            "n": n,
            "win_rate": wins/n*100,
            "total": total,
            "avg": total/n,
            "big_w": big_w, "big_l": big_l,
        })

    # 정렬: 총 PnL 기준
    results.sort(key=lambda x: x["total"], reverse=True)

    print("\n" + "=" * 110)
    print(f"[TOP 10 by total PnL]")
    print(f"{'sqz':>4} {'exp':>4} {'trAct':>6} {'trStp':>5} {'exit':<12} "
          f"{'n':>4} {'승률':>6} {'총PnL':>8} {'평균':>7} {'+3↑':>3} {'-2↓':>3}")
    for r in results[:10]:
        print(f"{r['squeeze']:>4.1f} {r['expand']:>4.1f} {r['trail_act']*100:>5.1f}% "
              f"{r['trail_stop']*100:>4.1f}% {r['exit_mode']:<12} "
              f"{r['n']:>4} {r['win_rate']:>5.1f}% {r['total']:>+7.2f}% {r['avg']:>+6.3f}% "
              f"{r['big_w']:>3} {r['big_l']:>3}")

    print(f"\n[BOTTOM 5 by total PnL]")
    for r in results[-5:]:
        print(f"{r['squeeze']:>4.1f} {r['expand']:>4.1f} {r['trail_act']*100:>5.1f}% "
              f"{r['trail_stop']*100:>4.1f}% {r['exit_mode']:<12} "
              f"{r['n']:>4} {r['win_rate']:>5.1f}% {r['total']:>+7.2f}% {r['avg']:>+6.3f}% "
              f"{r['big_w']:>3} {r['big_l']:>3}")

    # 승률 정렬 TOP 5
    results_by_wr = [r for r in results if r["n"] >= 50]
    results_by_wr.sort(key=lambda x: x["win_rate"], reverse=True)
    print(f"\n[TOP 5 by 승률 (n≥50)]")
    for r in results_by_wr[:5]:
        print(f"{r['squeeze']:>4.1f} {r['expand']:>4.1f} {r['trail_act']*100:>5.1f}% "
              f"{r['trail_stop']*100:>4.1f}% {r['exit_mode']:<12} "
              f"{r['n']:>4} {r['win_rate']:>5.1f}% {r['total']:>+7.2f}% {r['avg']:>+6.3f}%")


if __name__ == "__main__":
    main()
