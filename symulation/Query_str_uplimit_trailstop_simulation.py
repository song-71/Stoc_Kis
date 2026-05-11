"""
Strategy A — 28% 돌파 매수 + 트레일스톱 매도 백테스트
Query_str_uplimit_trailstop_simulation.py

데이터: WSS 틱 (data/wss_data/YYMMDD_wss_data.parquet, 최근 35일치)

매수 (edge-trigger):
  F1. prdy_ctrt 가 28% 를 상향 돌파 (직전 틱 < 28%, 현재 ≥ 28%)
  F2. 09:30 ≤ 시간 ≤ 14:30
  F3. 전일종가 ≥ 1,000원
  F4. 종목별 1회만

매도 (OR):
  E1. 고점 대비 N% 하락 (트레일스톱, 활성 조건 없음 — 매수가부터 즉시)
  E2. 매수가 대비 -3% 손절 (안전장치)
  E3. 14:55 마감 강제 청산

그리드 서치: trail_pct ∈ {2%, 3%, 5%, 7%, 10%}
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from collections import Counter

sys.path.append(str(Path(__file__).resolve().parents[1]))

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
WSS_DIR = DATA_DIR / "wss_data"

BUY_TRIGGER_CTRT = 28.0     # 28% 상향 돌파
MIN_PRICE = 1000
HARD_STOP_PCT = 0.03        # 안전장치 손절 (매수가 대비 -3%)
CLOSE_TIME = "145500"


def load_day_ticks(date_str: str) -> pl.DataFrame | None:
    path = WSS_DIR / f"{date_str}_wss_data.parquet"
    if not path.exists():
        return None
    try:
        df = pl.read_parquet(path, columns=[
            "mksc_shrn_iscd", "stck_cntg_hour", "stck_prpr",
            "prdy_ctrt", "bidp1"
        ])
    except Exception:
        return None
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
    # 직전 틱 prdy_ctrt (edge-trigger 용)
    df = df.with_columns(
        pl.col("prdy_ctrt").shift(1).over("code").alias("prdy_ctrt_prev")
    )
    return df


def simulate(df: pl.DataFrame, trail_pct: float) -> list[dict]:
    trades = []
    by_code = df.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        pos = None
        bought_once = False
        for r in g.iter_rows(named=True):
            price = r["stck_prpr"]
            bidp = r["bidp1"] or price
            hms = r["hms"]
            ctrt = r["prdy_ctrt"] or 0
            ctrt_prev = r["prdy_ctrt_prev"] or 0

            if pos is None:
                if bought_once:
                    continue  # 종목당 1회
                # 28% edge-trigger
                if (ctrt_prev < BUY_TRIGGER_CTRT <= ctrt < 30.0
                    and price >= MIN_PRICE
                    and "093000" <= hms <= "143000"):
                    pos = {
                        "buy_price": price,
                        "buy_hms": hms,
                        "buy_ctrt": ctrt,
                        "highest": bidp,
                    }
                    bought_once = True
            else:
                if bidp > pos["highest"]:
                    pos["highest"] = bidp
                exit_reason = None
                # 트레일스톱 (고점 대비 N%)
                if bidp <= pos["highest"] * (1 - trail_pct):
                    exit_reason = f"트레일-{trail_pct*100:.0f}%"
                # 하드 손절
                elif bidp <= pos["buy_price"] * (1 - HARD_STOP_PCT):
                    exit_reason = "손절-3%"
                # 마감
                elif hms >= CLOSE_TIME:
                    exit_reason = "마감"

                if exit_reason:
                    pnl_pct = (bidp / pos["buy_price"] - 1) * 100
                    trades.append({
                        "code": code,
                        "buy_hms": pos["buy_hms"], "buy_price": pos["buy_price"],
                        "sell_hms": hms, "sell_price": bidp,
                        "highest": pos["highest"],
                        "buy_ctrt": pos["buy_ctrt"],
                        "pnl_pct": pnl_pct,
                        "exit_reason": exit_reason,
                    })
                    pos = None
        if pos is not None and g.height > 0:
            last = g.row(-1, named=True)
            bidp = last["bidp1"] or last["stck_prpr"]
            pnl_pct = (bidp / pos["buy_price"] - 1) * 100
            trades.append({
                "code": code,
                "buy_hms": pos["buy_hms"], "buy_price": pos["buy_price"],
                "sell_hms": last["hms"], "sell_price": bidp,
                "highest": pos["highest"], "buy_ctrt": pos["buy_ctrt"],
                "pnl_pct": pnl_pct, "exit_reason": "장마감자동청산",
            })
    return trades


def report(label, trades):
    if not trades:
        print(f"  [{label}] 거래 0건")
        return None
    n = len(trades)
    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    total = sum(t["pnl_pct"] for t in trades)
    big_w = sum(1 for t in trades if t["pnl_pct"] >= 5.0)
    big_l = sum(1 for t in trades if t["pnl_pct"] <= -5.0)
    worst = min(t["pnl_pct"] for t in trades)
    best = max(t["pnl_pct"] for t in trades)
    print(f"  [{label}] 거래={n:4d}건 | 승률={wins/n*100:5.1f}% | "
          f"총PnL={total:+7.2f}% | 평균={total/n:+.3f}% | "
          f"+5%↑={big_w} | -5%↓={big_l} | worst={worst:+.1f}% best={best:+.1f}%")
    return {"n": n, "wins": wins, "total": total, "avg": total/n, "worst": worst, "best": best}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="all")
    ap.add_argument("--trail", default="2,3,5,7,10", help="쉼표 구분 %")
    args = ap.parse_args()

    if args.dates == "all":
        dates = sorted({f.name.split("_")[0] for f in WSS_DIR.glob("*_wss_data.parquet")})
    else:
        dates = [d.strip() for d in args.dates.split(",")]

    trail_list = [float(t.strip())/100 for t in args.trail.split(",")]

    # 데이터 한 번만 로드
    loaded = []
    for d in dates:
        df = load_day_ticks(d)
        if df is None or df.height == 0:
            continue
        loaded.append((d, df))
    print(f"Strategy A — 28% 돌파 + 트레일스톱 백테스트")
    print(f"로드 일수: {len(loaded)}/{len(dates)}")
    print(f"트레일 %: {[f'{t*100:.0f}%' for t in trail_list]}")
    print("=" * 90)

    # 각 trail 별로 전체 시뮬
    results_by_trail = {}
    for trail in trail_list:
        all_tr = []
        for d, df in loaded:
            tr = simulate(df, trail)
            all_tr.extend(tr)
        results_by_trail[trail] = all_tr
        info = report(f"trail={trail*100:.0f}%", all_tr)

    # 일자별 상세 (trail=5% 기준)
    print("\n" + "=" * 90)
    print("[trail=5% 일자별 상세]")
    for d, df in loaded:
        tr = simulate(df, 0.05)
        if not tr:
            continue
        n = len(tr)
        wins = sum(1 for t in tr if t["pnl_pct"] > 0)
        total = sum(t["pnl_pct"] for t in tr)
        print(f"  [{d}] 거래={n:3d} | 승률={wins/n*100:5.1f}% | "
              f"총={total:+.2f}% | 평균={total/n:+.3f}% | "
              f"worst={min(t['pnl_pct'] for t in tr):+.1f}%")

    # 사유별 (최고 trail 기준)
    best_trail = max(results_by_trail.keys(),
                     key=lambda t: sum(x["pnl_pct"] for x in results_by_trail[t]))
    best_trades = results_by_trail[best_trail]
    print(f"\n[trail={best_trail*100:.0f}% 매도 사유별 분포]")
    cnt = Counter(t["exit_reason"] for t in best_trades)
    for reason, cn in cnt.most_common():
        rt = [t for t in best_trades if t["exit_reason"] == reason]
        avg = sum(t["pnl_pct"] for t in rt) / cn
        wins = sum(1 for t in rt if t["pnl_pct"] > 0)
        print(f"  {reason:<18} {cn:>4}건 | 승률={wins/cn*100:5.1f}% | 평균={avg:+.3f}% | "
              f"총={sum(t['pnl_pct'] for t in rt):+.2f}%")


if __name__ == "__main__":
    main()
