"""
Strategy C — BB 압축→확장 반등, WSS 틱→1분봉 집계 확장 백테스트
Query_str_1m_bb_from_wss_extended.py

[데이터]
WSS 틱 (data/wss_data/YYMMDD_wss_data.parquet, 35일치 가능) →
  종목×1분 단위 OHLCV 집계 (first/max/min/last/sum).

[전략 C]
- 매수 (AND):
    tdy_ret > 0
    bb_width_pct 최근 100분 최소 < squeeze% (기본 3%)
    현재 bb_width_pct > expand% (기본 5%)
    최근 10분 내 low < bb_lower 기록 (하단 이탈 이력)
    직전 분봉 close < bb_lower (직전 이탈 상태)
    현재 분봉 close ≥ bb_lower (복귀)
    현재 close > 직전 close (첫 양봉)
- 매도 (OR):
    close ≤ buy × 0.97 손절
    close < bb_lower 재이탈
    고점 대비 -3% 트레일 (+3% 상승 후 활성)
    close < bb_mid 하회
    14:55 마감
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from collections import Counter, deque

sys.path.append(str(Path(__file__).resolve().parents[1]))

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
WSS_DIR = DATA_DIR / "wss_data"

STOP_LOSS_PCT = 0.03
TRAIL_PCT = 0.03
TRAIL_ACTIVATE_PCT = 0.03
BB_PERIOD = 20
BB_K = 2.0
BB_SQUEEZE_PCT = 3.0
BB_EXPAND_PCT = 5.0
BB_CROSS_LOOKBACK = 10


def load_and_aggregate(date_str: str) -> pl.DataFrame | None:
    """WSS 틱 → 종목×1분 OHLCV."""
    path = WSS_DIR / f"{date_str}_wss_data.parquet"
    if not path.exists():
        return None
    try:
        df = pl.read_parquet(path, columns=[
            "mksc_shrn_iscd", "stck_cntg_hour", "stck_prpr",
            "prdy_ctrt", "cntg_vol", "bidp1"
        ])
    except Exception:
        return None
    df = df.with_columns([
        pl.col("mksc_shrn_iscd").cast(pl.Utf8).str.zfill(6).alias("code"),
        pl.col("stck_cntg_hour").cast(pl.Utf8).str.zfill(6).alias("hms"),
        pl.col("stck_prpr").cast(pl.Float64, strict=False),
        pl.col("prdy_ctrt").cast(pl.Float64, strict=False),
        pl.col("cntg_vol").cast(pl.Float64, strict=False),
        pl.col("bidp1").cast(pl.Float64, strict=False),
    ])
    # 1분 키: HHMM (00초~59초를 HHMM00 으로 버킷)
    df = df.with_columns([
        pl.col("hms").str.slice(0, 4).alias("hhmm"),
    ]).filter(
        (pl.col("stck_prpr") > 0)
        & (pl.col("hhmm") >= "0930")
        & (pl.col("hhmm") <= "1455")
    )
    if df.height == 0:
        return None
    # 종목×hhmm OHLCV + 평균 등락률 (prdy_ctrt 는 % 단위)
    agg = df.sort(["code", "hms"]).group_by(["code", "hhmm"], maintain_order=True).agg([
        pl.col("stck_prpr").first().alias("open"),
        pl.col("stck_prpr").max().alias("high"),
        pl.col("stck_prpr").min().alias("low"),
        pl.col("stck_prpr").last().alias("close"),
        pl.col("cntg_vol").sum().alias("volume"),
        pl.col("bidp1").last().alias("bidp1"),
        pl.col("prdy_ctrt").last().alias("tdy_ret_pct"),
    ])
    agg = agg.sort(["code", "hhmm"])
    # tdy_ret 를 소수 형태로 (WSS 는 % 단위: 예 +15.00 → 0.15)
    agg = agg.with_columns((pl.col("tdy_ret_pct") / 100.0).alias("tdy_ret"))
    return agg


def compute_bb(df):
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


def simulate(df, date_str):
    trades = []
    by_code = df.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        if g.height < BB_PERIOD + 5:
            continue
        width_hist = deque(maxlen=100)
        cross_hist = deque(maxlen=BB_CROSS_LOOKBACK)
        pos = None
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
                    and min_width < BB_SQUEEZE_PCT
                    and bb_width_pct > BB_EXPAND_PCT
                    and had_below_recent
                    and prev_below
                    and close >= bb_lower
                    and close > close_prev
                ):
                    pos = {"buy_price": close, "buy_hhmm": hhmm, "highest": close}
            else:
                if close > pos["highest"]:
                    pos["highest"] = close
                exit_reason = None
                if close <= pos["buy_price"] * (1 - STOP_LOSS_PCT):
                    exit_reason = "손절-3%"
                elif bb_lower and close < bb_lower:
                    exit_reason = "bb_lower재이탈"
                elif (pos["highest"] >= pos["buy_price"] * (1 + TRAIL_ACTIVATE_PCT)
                      and close <= pos["highest"] * (1 - TRAIL_PCT)):
                    exit_reason = "트레일-3%"
                elif bb_mid and close < bb_mid:
                    exit_reason = "bb_mid하회"
                elif hhmm >= "1455":
                    exit_reason = "마감"
                if exit_reason:
                    pnl = (close / pos["buy_price"] - 1) * 100
                    trades.append({
                        "date": date_str, "code": code,
                        "buy_price": pos["buy_price"], "sell_price": close,
                        "pnl_pct": pnl, "exit_reason": exit_reason,
                    })
                    pos = None
            width_hist.append(bb_width_pct)
            cross_hist.append(cur_below)
        if pos is not None and g.height > 0:
            last = g.row(-1, named=True)
            pnl = (last["close"] / pos["buy_price"] - 1) * 100
            trades.append({
                "date": date_str, "code": code,
                "buy_price": pos["buy_price"], "sell_price": last["close"],
                "pnl_pct": pnl, "exit_reason": "장마감자동청산",
            })
    return trades


def classify_regime(df):
    try:
        late = df.filter(pl.col("hhmm") >= "1440")
        avg = late["tdy_ret"].mean() if late.height else None
        if avg is None:
            return "UNK"
        if avg > 0.01:
            return "UP"
        elif avg < -0.01:
            return "DOWN"
        else:
            return "FLAT"
    except Exception:
        return "UNK"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="all")
    args = ap.parse_args()

    if args.dates == "all":
        dates = sorted({
            f.name.split("_")[0] for f in WSS_DIR.glob("*_wss_data.parquet")
        })
    else:
        dates = [d.strip() for d in args.dates.split(",")]

    print(f"Strategy C — BB 압축→확장 확장 백테스트 (WSS→1m 집계): {len(dates)}일")
    print(f"파라미터: period={BB_PERIOD}분, K={BB_K}, squeeze<{BB_SQUEEZE_PCT}%, expand>{BB_EXPAND_PCT}%")
    print("=" * 95)

    all_trades = []
    per_day = []
    for d in dates:
        df = load_and_aggregate(d)
        if df is None or df.height == 0:
            print(f"[{d}] 데이터 없음/집계 실패 → 스킵")
            continue
        n_codes = df["code"].n_unique()
        regime = classify_regime(df)
        df2 = compute_bb(df)
        trades = simulate(df2, d)
        all_trades.extend(trades)
        per_day.append({"date": d, "regime": regime, "trades": trades, "codes": n_codes})
        wins = sum(1 for t in trades if t["pnl_pct"] > 0)
        total = sum(t["pnl_pct"] for t in trades)
        n = len(trades)
        wr = wins/n*100 if n else 0
        avg = total/n if n else 0
        print(f"[{d}] regime={regime:<4} 종목={n_codes:3d} 거래={n:3d} "
              f"승률={wr:5.1f}% 총={total:+7.2f}% 평균={avg:+.3f}%")

    print()
    print("=" * 95)
    n = len(all_trades)
    if n == 0:
        print("[총합] 거래 0건")
        return
    wins = sum(1 for t in all_trades if t["pnl_pct"] > 0)
    total = sum(t["pnl_pct"] for t in all_trades)
    big_w = sum(1 for t in all_trades if t["pnl_pct"] >= 3.0)
    big_l = sum(1 for t in all_trades if t["pnl_pct"] <= -2.0)
    print(f"[총합 {len(per_day)}일] 거래={n}건 | 승률={wins/n*100:.1f}% | "
          f"총PnL={total:+.2f}% | 평균={total/n:+.3f}% | +3%↑={big_w} | -2%↓={big_l}")

    print(f"\n[매도 사유별]")
    cnt = Counter(t["exit_reason"] for t in all_trades)
    for reason, cn in cnt.most_common():
        rt = [t for t in all_trades if t["exit_reason"] == reason]
        avg = sum(t["pnl_pct"] for t in rt) / cn
        win_n = sum(1 for t in rt if t["pnl_pct"] > 0)
        print(f"  {reason:<18} {cn:>4}건 | 승률={win_n/cn*100:5.1f}% | 평균={avg:+.3f}%")

    print(f"\n[국면별 성과]")
    for regime in ["UP", "FLAT", "DOWN", "UNK"]:
        days = [d for d in per_day if d["regime"] == regime]
        if not days:
            continue
        all_tr = [t for d in days for t in d["trades"]]
        if not all_tr:
            print(f"  [{regime:<4}] 일수={len(days):2d} 거래=0")
            continue
        n2 = len(all_tr)
        wins2 = sum(1 for t in all_tr if t["pnl_pct"] > 0)
        total2 = sum(t["pnl_pct"] for t in all_tr)
        print(f"  [{regime:<4}] 일수={len(days):2d} 거래={n2:4d} "
              f"승률={wins2/n2*100:5.1f}% 총={total2:+7.2f}% 평균={total2/n2:+.3f}%")


if __name__ == "__main__":
    main()
