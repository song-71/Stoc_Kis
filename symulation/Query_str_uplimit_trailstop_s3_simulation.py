"""
Strategy A — 28% 돌파 + 지연활성 트레일스톱 (S3 1m 데이터, 59일, 전체 종목)
Query_str_uplimit_trailstop_s3_simulation.py

데이터: s3://tfttrain/KIS_DB/market_data/1m/date=YYYY-MM-DD/ohlcv.parquet
  - 59일치 (2026-01-23 ~ 2026-04-23)
  - 전체 상장 종목 1m OHLCV

매수: 28% 상향 돌파 edge-trigger (tdy_ret 기준)
매도 (지연활성 트레일스톱):
  E1. 하드 손절: buy × (1 - hard_loss) 이하
  E2. 트레일 활성 조건 만족 후: highest × (1 - trail_stop) 이하
     활성 조건: highest ≥ buy × (1 + trail_activate)
  E3. 14:55 마감
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from collections import Counter
import itertools

sys.path.append(str(Path(__file__).resolve().parents[1]))

import duckdb
import polars as pl
from kis_utils import _configure_duckdb_s3

S3_BASE = "s3://tfttrain/KIS_DB/market_data/1m"

BUY_TRIGGER_CTRT = 0.28
BUY_UPPER_LIMIT = 0.30
MIN_PRICE = 1000
CLOSE_TIME = "145500"
MAX_BREAK_TIME = "143000"   # 28% 돌파 14:30 이전만


def list_dates(con):
    r = con.execute(f"""
        SELECT DISTINCT regexp_extract(file, 'date=([0-9-]+)', 1) as date
        FROM glob('{S3_BASE}/*/ohlcv.parquet')
        ORDER BY date
    """).df()
    return r["date"].tolist()


def load_candidates_for_day(con, date_str: str):
    """해당 일자 1m 전체 로드 (전체 종목). 28% 넘은 종목만 필터."""
    path = f"{S3_BASE}/date={date_str}/ohlcv.parquet"
    max_per_code = con.execute(f"""
        SELECT code, MAX(tdy_ret) as max_ret
        FROM read_parquet('{path}')
        WHERE CAST(time AS VARCHAR) >= '093000'
          AND CAST(time AS VARCHAR) <= '143000'
          AND CAST(close AS BIGINT) > 0
        GROUP BY code
        HAVING MAX(tdy_ret) >= {BUY_TRIGGER_CTRT}
    """).df()
    if max_per_code.empty:
        return None
    target_codes = max_per_code["code"].tolist()
    codes_sql = ",".join(f"'{c}'" for c in target_codes)
    df = con.execute(f"""
        SELECT code, time, open, high, low, close, volume, tdy_ret, pdy_close
        FROM read_parquet('{path}')
        WHERE code IN ({codes_sql})
          AND CAST(time AS VARCHAR) >= '093000'
          AND CAST(time AS VARCHAR) <= '{CLOSE_TIME}'
          AND CAST(close AS BIGINT) > 0
        ORDER BY code, time
    """).df()
    if df.empty:
        return None
    pdf = pl.from_pandas(df)
    pdf = pdf.with_columns([
        pl.col("time").cast(pl.Utf8).str.zfill(6),
        pl.col("code").cast(pl.Utf8).str.zfill(6),
        pl.col("close").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("tdy_ret").cast(pl.Float64),
    ]).sort(["code", "time"])
    pdf = pdf.with_columns(pl.col("tdy_ret").shift(1).over("code").alias("tdy_ret_prev"))
    return pdf


def simulate(df, trail_activate, trail_stop, hard_loss):
    trades = []
    by_code = df.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        pos = None
        bought = False
        for r in g.iter_rows(named=True):
            close = r["close"]
            low = r["low"]
            time_str = r["time"]
            ctrt = r["tdy_ret"] or 0
            ctrt_prev = r["tdy_ret_prev"] or 0

            if pos is None:
                if bought:
                    continue
                # 28% 상향 돌파
                if (ctrt_prev < BUY_TRIGGER_CTRT <= ctrt < BUY_UPPER_LIMIT
                    and close >= MIN_PRICE
                    and "093000" <= time_str <= MAX_BREAK_TIME):
                    pos = {"buy": close, "buy_time": time_str, "high": close, "trail_on": False}
                    bought = True
            else:
                if close > pos["high"]:
                    pos["high"] = close
                if not pos["trail_on"] and pos["high"] >= pos["buy"] * (1 + trail_activate):
                    pos["trail_on"] = True

                exit_reason = None
                # 하드 손절 (분봉 low 기준 보수적 판정)
                if low <= pos["buy"] * (1 - hard_loss):
                    exit_reason = f"손절{hard_loss*100:.0f}%"
                elif pos["trail_on"] and close <= pos["high"] * (1 - trail_stop):
                    exit_reason = f"트레일{trail_stop*100:.0f}%"
                elif time_str >= CLOSE_TIME:
                    exit_reason = "마감"

                if exit_reason:
                    # 체결가: 손절은 stop 가격, 트레일/마감은 close
                    if exit_reason.startswith("손절"):
                        sell = pos["buy"] * (1 - hard_loss)
                    elif exit_reason.startswith("트레일"):
                        sell = pos["high"] * (1 - trail_stop)
                    else:
                        sell = close
                    pnl = (sell / pos["buy"] - 1) * 100
                    trades.append({
                        "code": code, "buy_time": pos["buy_time"], "sell_time": time_str,
                        "buy": pos["buy"], "sell": sell, "high": pos["high"],
                        "pnl": pnl, "reason": exit_reason,
                    })
                    pos = None
        if pos is not None and g.height > 0:
            last = g.row(-1, named=True)
            sell = last["close"]
            pnl = (sell / pos["buy"] - 1) * 100
            trades.append({
                "code": code, "buy_time": pos["buy_time"], "sell_time": last["time"],
                "buy": pos["buy"], "sell": sell, "high": pos["high"],
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
            "worst": worst, "best": best}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="all")
    args = ap.parse_args()
    con = duckdb.connect(":memory:")
    _configure_duckdb_s3(con)

    if args.dates == "all":
        dates = list_dates(con)
    else:
        dates = [d.strip() for d in args.dates.split(",")]
    print(f"Strategy A — 28% 돌파 + 지연활성 트레일스톱 (S3 1m, 전체 종목)")
    print(f"대상 일자: {len(dates)}일 ({dates[0]} ~ {dates[-1]})")
    print("=" * 100)

    # 일자별 로드 + 후보 캐시
    day_data = []
    for d in dates:
        df = load_candidates_for_day(con, d)
        if df is None or df.height == 0:
            continue
        n_codes = df["code"].n_unique()
        day_data.append((d, df))
        print(f"  [{d}] 28%+ 도달 종목: {n_codes}개")

    print(f"\n총 {len(day_data)}일 데이터 확보 → 그리드 시작")
    print("=" * 100)

    # 그리드 서치
    grid = {
        "trail_activate": [0.01, 0.02, 0.03, 0.05],
        "trail_stop":     [0.02, 0.03, 0.05],
        "hard_loss":      [0.03, 0.05, 0.07, 0.10],
    }
    combos = list(itertools.product(*grid.values()))
    results = []
    for ta, ts, hl in combos:
        all_tr = []
        for d, df in day_data:
            all_tr.extend(simulate(df, ta, ts, hl))
        s = summarize(all_tr)
        if s is None:
            continue
        results.append({"trail_activate": ta, "trail_stop": ts, "hard_loss": hl,
                        **s, "trades": all_tr})

    results.sort(key=lambda x: x["total"], reverse=True)
    print(f"\n[TOP 10 by 총 PnL] (총 {len(combos)} 조합, {len(day_data)}일)")
    print(f"{'trl_act':>7} {'trl_stp':>7} {'hard':>5} "
          f"{'n':>5} {'승률':>6} {'총PnL':>10} {'평균':>7} {'worst':>7} {'best':>6}")
    for r in results[:10]:
        print(f"{r['trail_activate']*100:>5.1f}% {r['trail_stop']*100:>6.1f}% "
              f"{r['hard_loss']*100:>3.0f}% "
              f"{r['n']:>5} {r['win_rate']:>5.1f}% {r['total']:>+9.2f}% "
              f"{r['avg']:>+6.3f}% {r['worst']:>+6.1f}% {r['best']:>+5.1f}%")

    # 승률 기준 top 5
    rw = sorted([r for r in results if r["n"] >= 100], key=lambda x: x["win_rate"], reverse=True)
    print(f"\n[TOP 5 by 승률 (n≥100)]")
    for r in rw[:5]:
        print(f"{r['trail_activate']*100:>5.1f}% {r['trail_stop']*100:>6.1f}% "
              f"{r['hard_loss']*100:>3.0f}% "
              f"{r['n']:>5} {r['win_rate']:>5.1f}% {r['total']:>+9.2f}% "
              f"{r['avg']:>+6.3f}%")

    # best 사유별
    if results:
        best = results[0]
        print(f"\n[BEST: trail_act={best['trail_activate']*100:.0f}%, trail_stop={best['trail_stop']*100:.0f}%, hard={best['hard_loss']*100:.0f}%]")
        cnt = Counter(t["reason"] for t in best["trades"])
        for reason, cn in cnt.most_common():
            rt = [t for t in best["trades"] if t["reason"] == reason]
            avg = sum(t["pnl"] for t in rt) / cn
            wins = sum(1 for t in rt if t["pnl"] > 0)
            tot = sum(t["pnl"] for t in rt)
            print(f"  {reason:<12} {cn:>5}건 | 승률={wins/cn*100:5.1f}% | 평균={avg:+.3f}% | 총={tot:+.2f}%")


if __name__ == "__main__":
    main()
