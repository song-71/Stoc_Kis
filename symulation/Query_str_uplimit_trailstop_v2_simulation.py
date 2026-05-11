"""
Strategy A — 28% 돌파 + 지연활성 트레일스톱 v2
Query_str_uplimit_trailstop_v2_simulation.py

변경점 (v1 대비):
- 트레일 "지연 활성": 매수가 대비 +3% 상승 달성 후부터만 트레일 발동
  (잔파동에 즉시 -2% 잘리는 문제 해결)
- 그리드: trail_activate × trail_stop × hard_loss

매수: 28% 상향 돌파 edge-trigger (필터 없음)
매도 (OR):
  E1. 하드 손절: buy × (1-hard_loss) 이하
  E2. 트레일 활성 후: highest × (1-trail_stop) 이하
     활성 조건: highest ≥ buy × (1+trail_activate)
  E3. 14:55 마감
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from collections import Counter
import itertools

sys.path.append(str(Path(__file__).resolve().parents[1]))

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
WSS_DIR = DATA_DIR / "wss_data"

BUY_TRIGGER_CTRT = 28.0
BUY_UPPER_LIMIT = 30.0
MIN_PRICE = 1000
CLOSE_TIME = "145500"


def load_day(date_str: str):
    path = WSS_DIR / f"{date_str}_wss_data.parquet"
    if not path.exists():
        return None
    df = pl.read_parquet(path, columns=[
        "mksc_shrn_iscd", "stck_cntg_hour", "stck_prpr", "prdy_ctrt", "bidp1"
    ])
    df = df.with_columns([
        pl.col("mksc_shrn_iscd").cast(pl.Utf8).str.zfill(6).alias("code"),
        pl.col("stck_cntg_hour").cast(pl.Utf8).str.zfill(6).alias("hms"),
        pl.col("stck_prpr").cast(pl.Float64, strict=False),
        pl.col("prdy_ctrt").cast(pl.Float64, strict=False),
        pl.col("bidp1").cast(pl.Float64, strict=False),
    ]).sort(["code", "hms"]).filter(
        (pl.col("stck_prpr") > 0)
        & (pl.col("hms") >= "093000")
        & (pl.col("hms") <= CLOSE_TIME)
    )
    if df.height == 0:
        return None
    df = df.with_columns(
        pl.col("prdy_ctrt").shift(1).over("code").alias("prdy_ctrt_prev")
    )
    return df


def simulate(df, trail_activate, trail_stop, hard_loss):
    trades = []
    by_code = df.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        pos = None
        bought = False
        for r in g.iter_rows(named=True):
            price = r["stck_prpr"]
            bidp = r["bidp1"] or price
            hms = r["hms"]
            ctrt = r["prdy_ctrt"] or 0
            ctrt_prev = r["prdy_ctrt_prev"] or 0

            if pos is None:
                if bought:
                    continue
                if (ctrt_prev < BUY_TRIGGER_CTRT <= ctrt < BUY_UPPER_LIMIT
                    and price >= MIN_PRICE
                    and "093000" <= hms <= "143000"):
                    pos = {"buy": price, "buy_hms": hms, "high": bidp, "trail_on": False}
                    bought = True
            else:
                if bidp > pos["high"]:
                    pos["high"] = bidp
                # 트레일 활성 여부
                if not pos["trail_on"] and pos["high"] >= pos["buy"] * (1 + trail_activate):
                    pos["trail_on"] = True

                exit_reason = None
                # 하드 손절
                if bidp <= pos["buy"] * (1 - hard_loss):
                    exit_reason = f"손절-{hard_loss*100:.0f}%"
                # 트레일 (활성 후만)
                elif pos["trail_on"] and bidp <= pos["high"] * (1 - trail_stop):
                    exit_reason = f"트레일-{trail_stop*100:.0f}%"
                # 마감
                elif hms >= CLOSE_TIME:
                    exit_reason = "마감"

                if exit_reason:
                    pnl = (bidp / pos["buy"] - 1) * 100
                    trades.append({
                        "code": code, "buy_hms": pos["buy_hms"], "sell_hms": hms,
                        "buy": pos["buy"], "sell": bidp, "high": pos["high"],
                        "pnl": pnl, "reason": exit_reason,
                    })
                    pos = None
        if pos is not None and g.height > 0:
            last = g.row(-1, named=True)
            bidp = last["bidp1"] or last["stck_prpr"]
            pnl = (bidp / pos["buy"] - 1) * 100
            trades.append({
                "code": code, "buy_hms": pos["buy_hms"], "sell_hms": last["hms"],
                "buy": pos["buy"], "sell": bidp, "high": pos["high"],
                "pnl": pnl, "reason": "자동마감",
            })
    return trades


def summarize(trades):
    if not trades:
        return None
    n = len(trades)
    wins = sum(1 for t in trades if t["pnl"] > 0)
    total = sum(t["pnl"] for t in trades)
    worst = min(t["pnl"] for t in trades)
    best = max(t["pnl"] for t in trades)
    return {"n": n, "win_rate": wins/n*100, "total": total, "avg": total/n,
            "worst": worst, "best": best, "trades": trades}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="all")
    args = ap.parse_args()

    if args.dates == "all":
        dates = sorted({f.name.split("_")[0] for f in WSS_DIR.glob("*_wss_data.parquet")})
    else:
        dates = [d.strip() for d in args.dates.split(",")]

    loaded = []
    for d in dates:
        df = load_day(d)
        if df is None or df.height == 0:
            continue
        loaded.append((d, df))
    print(f"Strategy A — 28% 돌파 + 지연활성 트레일스톱")
    print(f"로드 일수: {len(loaded)}")
    print("=" * 100)

    # 그리드 서치
    grid = {
        "trail_activate": [0.02, 0.03, 0.05, 0.10],
        "trail_stop":     [0.02, 0.03, 0.05, 0.10],
        "hard_loss":      [0.03, 0.05, 0.07],
    }
    combos = list(itertools.product(*grid.values()))
    print(f"그리드 조합: {len(combos)}")
    print(f"{'trail_act':>9} {'trail_stop':>10} {'hard':>5} "
          f"{'n':>4} {'승률':>6} {'총PnL':>9} {'평균':>7} {'worst':>7} {'best':>6}")

    results = []
    for combo in combos:
        ta, ts, hl = combo
        all_tr = []
        for d, df in loaded:
            all_tr.extend(simulate(df, ta, ts, hl))
        s = summarize(all_tr)
        if s is None:
            continue
        results.append({"trail_activate": ta, "trail_stop": ts, "hard_loss": hl, **s})

    # 총 PnL 정렬 TOP 10
    results.sort(key=lambda x: x["total"], reverse=True)
    print(f"\n[TOP 10 by 총 PnL]")
    for r in results[:10]:
        print(f"{r['trail_activate']*100:>7.1f}% {r['trail_stop']*100:>8.1f}% "
              f"{r['hard_loss']*100:>3.0f}% "
              f"{r['n']:>4} {r['win_rate']:>5.1f}% {r['total']:>+8.2f}% "
              f"{r['avg']:>+6.3f}% {r['worst']:>+6.1f}% {r['best']:>+5.1f}%")

    # 승률 기준 TOP 5
    results_wr = [r for r in results if r["n"] >= 100]
    results_wr.sort(key=lambda x: x["win_rate"], reverse=True)
    print(f"\n[TOP 5 by 승률 (n≥100)]")
    for r in results_wr[:5]:
        print(f"{r['trail_activate']*100:>7.1f}% {r['trail_stop']*100:>8.1f}% "
              f"{r['hard_loss']*100:>3.0f}% "
              f"{r['n']:>4} {r['win_rate']:>5.1f}% {r['total']:>+8.2f}% "
              f"{r['avg']:>+6.3f}% {r['worst']:>+6.1f}%")

    # 최고 조합 사유별 분포
    best = results[0]
    print(f"\n[BEST 조합: trail_act={best['trail_activate']*100:.0f}%, "
          f"trail_stop={best['trail_stop']*100:.0f}%, hard={best['hard_loss']*100:.0f}%]")
    cnt = Counter(t["reason"] for t in best["trades"])
    for reason, cn in cnt.most_common():
        rt = [t for t in best["trades"] if t["reason"] == reason]
        avg = sum(t["pnl"] for t in rt) / cn
        wins = sum(1 for t in rt if t["pnl"] > 0)
        tot = sum(t["pnl"] for t in rt)
        print(f"  {reason:<15} {cn:>4}건 | 승률={wins/cn*100:5.1f}% | 평균={avg:+.3f}% | 총={tot:+.2f}%")


if __name__ == "__main__":
    main()
