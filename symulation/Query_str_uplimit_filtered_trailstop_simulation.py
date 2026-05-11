"""
Strategy A — 28% 돌파 매수 + 필터 AND + 트레일스톱 매도
Query_str_uplimit_filtered_trailstop_simulation.py

필터 (AND, 문서 01. 의 "최적 조합" 기반):
  F1. 28% 상향 돌파 edge-trigger (prev_ctrt < 28, cur_ctrt ≥ 28)
  F2. 60분 이내 도달 (돌파 시각 < 10:00)
  F3. 시가 갭 ≥ 5% (첫 틱 가격 / 전일종가 - 1 ≥ 0.05)
  F4. 직전 5분 분당 평균 거래량 ≥ 당일 분당 평균 × 3배
  F5. 전일종가 ≥ 1000원

매도 (OR):
  E1. 고점 대비 N% 트레일스톱 (그리드: 2,3,5,7%)
  E2. 매수가 대비 -3% 손절 (안전장치)
  E3. 14:55 마감 강제 청산
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from collections import Counter, defaultdict

sys.path.append(str(Path(__file__).resolve().parents[1]))

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
WSS_DIR = DATA_DIR / "wss_data"

BUY_TRIGGER_CTRT = 28.0
BUY_UPPER_LIMIT = 30.0
MAX_BREAK_TIME = "100000"    # 28% 돌파는 10:00 이전만
GAP_MIN_PCT = 5.0             # 시가 갭 ≥ 5%
VOL_SURGE_X = 3.0             # 직전 5분 분당평균 vs 당일 분당평균
MIN_PRICE = 1000
HARD_STOP_PCT = 0.03
CLOSE_TIME = "145500"


def load_and_prep(date_str: str) -> pl.DataFrame | None:
    path = WSS_DIR / f"{date_str}_wss_data.parquet"
    if not path.exists():
        return None
    try:
        df = pl.read_parquet(path, columns=[
            "mksc_shrn_iscd", "stck_cntg_hour", "stck_prpr",
            "prdy_ctrt", "bidp1", "cntg_vol", "acml_vol", "stck_oprc"
        ])
    except Exception:
        return None
    df = df.with_columns([
        pl.col("mksc_shrn_iscd").cast(pl.Utf8).str.zfill(6).alias("code"),
        pl.col("stck_cntg_hour").cast(pl.Utf8).str.zfill(6).alias("hms"),
        pl.col("stck_prpr").cast(pl.Float64, strict=False),
        pl.col("prdy_ctrt").cast(pl.Float64, strict=False),
        pl.col("bidp1").cast(pl.Float64, strict=False),
        pl.col("cntg_vol").cast(pl.Float64, strict=False),
        pl.col("acml_vol").cast(pl.Float64, strict=False),
        pl.col("stck_oprc").cast(pl.Float64, strict=False),
    ]).sort(["code", "hms"]).filter(
        (pl.col("stck_prpr") > 0)
        & (pl.col("hms") >= "090000")
        & (pl.col("hms") <= CLOSE_TIME)
    )
    if df.height == 0:
        return None
    df = df.with_columns(
        pl.col("prdy_ctrt").shift(1).over("code").alias("prdy_ctrt_prev")
    )
    return df


def minute_bucket(hms: str) -> str:
    return hms[:4]  # HHMM


def simulate(df: pl.DataFrame, trail_pct: float):
    """필터 AND + 트레일스톱 시뮬."""
    trades = []
    by_code = df.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        if g.height < 30:
            continue

        # 종목별 사전 계산
        # - 시가: 09:00~09:01 사이 첫 틱 (없으면 첫 번째 틱)
        first_tick = g.row(0, named=True)
        open_price = first_tick["stck_oprc"] if first_tick.get("stck_oprc") and first_tick["stck_oprc"] > 0 else first_tick["stck_prpr"]
        # 전일종가 역산: first_tick 의 stck_prpr / (1 + prdy_ctrt/100)
        first_ctrt = first_tick["prdy_ctrt"] or 0
        if abs(first_ctrt) < 100 and first_tick["stck_prpr"] > 0:
            prev_close = first_tick["stck_prpr"] / (1 + first_ctrt / 100)
        else:
            prev_close = 0
        if prev_close < MIN_PRICE:
            continue

        gap_pct = (open_price / prev_close - 1) * 100 if prev_close > 0 else 0
        if gap_pct < GAP_MIN_PCT:
            continue   # F3 시가 갭 < 5% 제외

        # 분봉 거래량 집계 (dict: hhmm → sum(cntg_vol))
        minute_vol = defaultdict(float)
        for r in g.iter_rows(named=True):
            hhmm = minute_bucket(r["hms"])
            minute_vol[hhmm] += r.get("cntg_vol") or 0
        sorted_minutes = sorted(minute_vol.keys())

        # 시뮬 루프
        pos = None
        bought_once = False
        for r in g.iter_rows(named=True):
            price = r["stck_prpr"]
            bidp = r["bidp1"] or price
            hms = r["hms"]
            hhmm = minute_bucket(hms)
            ctrt = r["prdy_ctrt"] or 0
            ctrt_prev = r["prdy_ctrt_prev"] or 0

            if pos is None:
                if bought_once:
                    continue
                # F1 28% 돌파
                if not (ctrt_prev < BUY_TRIGGER_CTRT <= ctrt < BUY_UPPER_LIMIT):
                    continue
                # F2 10:00 이전만
                if hms >= MAX_BREAK_TIME:
                    continue
                # F4 거래량 서지 — 직전 5분 vs 당일 분당 평균
                # 당일 평균 = 09:00~현재 각 분 거래량 평균
                mins_since_open = max(1, int(hhmm[:2]) * 60 + int(hhmm[2:]) - (9 * 60))
                past_minutes = [m for m in sorted_minutes if m < hhmm]
                if len(past_minutes) < 5:
                    continue
                last5 = past_minutes[-5:]
                last5_avg = sum(minute_vol[m] for m in last5) / 5
                day_avg = sum(minute_vol[m] for m in past_minutes) / len(past_minutes)
                if day_avg <= 0 or last5_avg < day_avg * VOL_SURGE_X:
                    continue
                # 모든 필터 통과 → 매수
                pos = {
                    "buy_price": price,
                    "buy_hms": hms,
                    "buy_ctrt": ctrt,
                    "gap_pct": gap_pct,
                    "vol_ratio": last5_avg / day_avg,
                    "highest": bidp,
                }
                bought_once = True
            else:
                if bidp > pos["highest"]:
                    pos["highest"] = bidp
                exit_reason = None
                if bidp <= pos["highest"] * (1 - trail_pct):
                    exit_reason = f"트레일-{trail_pct*100:.0f}%"
                elif bidp <= pos["buy_price"] * (1 - HARD_STOP_PCT):
                    exit_reason = "손절-3%"
                elif hms >= CLOSE_TIME:
                    exit_reason = "마감"
                if exit_reason:
                    pnl_pct = (bidp / pos["buy_price"] - 1) * 100
                    trades.append({
                        "code": code,
                        "buy_hms": pos["buy_hms"], "buy_price": pos["buy_price"],
                        "sell_hms": hms, "sell_price": bidp,
                        "highest": pos["highest"], "buy_ctrt": pos["buy_ctrt"],
                        "gap_pct": pos["gap_pct"], "vol_ratio": pos["vol_ratio"],
                        "pnl_pct": pnl_pct, "exit_reason": exit_reason,
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
                "gap_pct": pos["gap_pct"], "vol_ratio": pos["vol_ratio"],
                "pnl_pct": pnl_pct, "exit_reason": "장마감자동청산",
            })
    return trades


def report(label, trades):
    if not trades:
        print(f"  [{label}] 거래 0건")
        return
    n = len(trades)
    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    total = sum(t["pnl_pct"] for t in trades)
    big_w = sum(1 for t in trades if t["pnl_pct"] >= 5.0)
    big_l = sum(1 for t in trades if t["pnl_pct"] <= -5.0)
    worst = min(t["pnl_pct"] for t in trades)
    best = max(t["pnl_pct"] for t in trades)
    print(f"  [{label}] 거래={n:3d}건 | 승률={wins/n*100:5.1f}% | "
          f"총PnL={total:+7.2f}% | 평균={total/n:+.3f}% | "
          f"+5%↑={big_w} | -5%↓={big_l} | worst={worst:+.1f}% best={best:+.1f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="all")
    ap.add_argument("--trail", default="2,3,5,7")
    args = ap.parse_args()

    if args.dates == "all":
        dates = sorted({f.name.split("_")[0] for f in WSS_DIR.glob("*_wss_data.parquet")})
    else:
        dates = [d.strip() for d in args.dates.split(",")]
    trail_list = [float(t.strip())/100 for t in args.trail.split(",")]

    loaded = []
    for d in dates:
        df = load_and_prep(d)
        if df is None or df.height == 0:
            continue
        loaded.append((d, df))
    print(f"Strategy A — 28% 돌파 + 필터 AND + 트레일스톱")
    print(f"필터: 60분이내 + 시가갭≥{GAP_MIN_PCT}% + 거래량서지×{VOL_SURGE_X}")
    print(f"로드 일수: {len(loaded)}")
    print("=" * 95)

    all_trades_by_trail = {}
    for trail in trail_list:
        all_tr = []
        for d, df in loaded:
            tr = simulate(df, trail)
            all_tr.extend(tr)
        all_trades_by_trail[trail] = all_tr
        report(f"trail={trail*100:.0f}%", all_tr)

    # 최적 trail 기준 상세
    best_trail = max(all_trades_by_trail.keys(),
                     key=lambda t: sum(x["pnl_pct"] for x in all_trades_by_trail[t]))
    best = all_trades_by_trail[best_trail]
    print("\n" + "=" * 95)
    print(f"[trail={best_trail*100:.0f}% 매도 사유별 분포]")
    cnt = Counter(t["exit_reason"] for t in best)
    for reason, cn in cnt.most_common():
        rt = [t for t in best if t["exit_reason"] == reason]
        avg = sum(t["pnl_pct"] for t in rt) / cn
        wins = sum(1 for t in rt if t["pnl_pct"] > 0)
        print(f"  {reason:<18} {cn:>4}건 | 승률={wins/cn*100:5.1f}% | 평균={avg:+.3f}% | "
              f"총={sum(t['pnl_pct'] for t in rt):+.2f}%")

    # 상위/하위 거래 각 5건
    if best:
        print(f"\n[TOP 5 거래]")
        for t in sorted(best, key=lambda x: x["pnl_pct"], reverse=True)[:5]:
            print(f"  {t['code']} @{t['buy_hms']} buy={int(t['buy_price'])} "
                  f"sell={int(t['sell_price'])} pnl={t['pnl_pct']:+.2f}% "
                  f"gap={t['gap_pct']:.1f}% volX={t['vol_ratio']:.1f} ({t['exit_reason']})")
        print(f"\n[BOTTOM 5 거래]")
        for t in sorted(best, key=lambda x: x["pnl_pct"])[:5]:
            print(f"  {t['code']} @{t['buy_hms']} buy={int(t['buy_price'])} "
                  f"sell={int(t['sell_price'])} pnl={t['pnl_pct']:+.2f}% "
                  f"gap={t['gap_pct']:.1f}% volX={t['vol_ratio']:.1f} ({t['exit_reason']})")


if __name__ == "__main__":
    main()
