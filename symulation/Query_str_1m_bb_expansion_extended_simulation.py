"""
Strategy C — BB 압축→확장 반등, 15일 확장 백테스트
Query_str_1m_bb_expansion_extended_simulation.py

데이터: NXT 1분봉 (20260402 ~ 20260423, 690종목/일)
기본 설정: squeeze<3%, expand>5%, period=20분

[국면 분류]
KOSPI(005930 삼성전자 등락률 프록시 또는 종목 평균 등락률) 기준 추정.
간단히: 당일 거래 종목의 평균 tdy_ret 로 국면 분류.
  UP     : avg_ret > +1%
  FLAT   : -1% ~ +1%
  DOWN   : < -1%
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
M1_DIR = DATA_DIR / "1m_data"

BUY_CTRT_MIN = 0.0
STOP_LOSS_PCT = 0.03
TRAIL_PCT = 0.03
TRAIL_ACTIVATE_PCT = 0.03
BB_PERIOD = 20
BB_K = 2.0
BB_SQUEEZE_PCT = 3.0
BB_EXPAND_PCT = 5.0
BB_CROSS_LOOKBACK = 10


def load_day(date_str: str) -> pl.DataFrame | None:
    """NXT 우선, 없으면 정식 파일."""
    p_nxt = M1_DIR / f"{date_str}_1m_chart_DB_parquet_NXT.parquet"
    p_full = M1_DIR / f"{date_str}_1m_chart_DB_parquet.parquet"
    path = p_nxt if p_nxt.exists() else (p_full if p_full.exists() else None)
    if path is None:
        return None
    df = pl.read_parquet(path)
    df = df.with_columns([
        pl.col("code").cast(pl.Utf8).str.zfill(6),
        pl.col("time").cast(pl.Utf8).str.zfill(6),
        pl.col("close").cast(pl.Float64, strict=False),
        pl.col("high").cast(pl.Float64, strict=False),
        pl.col("low").cast(pl.Float64, strict=False),
        pl.col("volume").cast(pl.Float64, strict=False),
        pl.col("tdy_ret").cast(pl.Float64, strict=False),
    ]).sort(["code", "time"]).filter(
        (pl.col("time") >= "093000")
        & (pl.col("time") <= "145500")
        & (pl.col("close") > 0)
    )
    return df, path.name


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
            time_str = r["time"]
            if bb_mid is None or bb_lower is None or bb_width_pct is None or close_prev is None:
                continue
            prev_below = cross_hist[-1] if cross_hist else False
            cur_below = (low < bb_lower)
            had_below_recent = any(cross_hist) if cross_hist else False
            min_width = min(width_hist) if width_hist else bb_width_pct
            if pos is None:
                if (
                    tdy_ret > BUY_CTRT_MIN
                    and min_width < BB_SQUEEZE_PCT
                    and bb_width_pct > BB_EXPAND_PCT
                    and had_below_recent
                    and prev_below
                    and close >= bb_lower
                    and close > close_prev
                ):
                    pos = {"buy_price": close, "buy_time": time_str, "highest": close, "date": date_str}
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
                elif time_str >= "145500":
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
    """종목 평균 tdy_ret 로 국면 분류 — 장 마감 가까운 시점 기준."""
    try:
        late = df.filter(pl.col("time") >= "144000")
        if late.height == 0:
            return "UNKNOWN"
        avg_ret = late["tdy_ret"].mean()
        if avg_ret is None:
            return "UNKNOWN"
        if avg_ret > 0.01:
            return "UP"
        elif avg_ret < -0.01:
            return "DOWN"
        else:
            return "FLAT"
    except Exception:
        return "UNKNOWN"


def report_summary(label, trades):
    if not trades:
        print(f"  [{label}] 거래 0건")
        return
    n = len(trades)
    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    total = sum(t["pnl_pct"] for t in trades)
    avg = total / n
    big_w = sum(1 for t in trades if t["pnl_pct"] >= 3.0)
    big_l = sum(1 for t in trades if t["pnl_pct"] <= -2.0)
    print(f"  [{label}] 거래={n:4d}건 | 승률={wins/n*100:5.1f}% | "
          f"총PnL={total:+8.2f}% | 평균={avg:+.3f}% | +3%↑={big_w} | -2%↓={big_l}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="all", help="'all' 또는 쉼표구분 YYYYMMDD")
    args = ap.parse_args()

    if args.dates == "all":
        nxt_files = sorted(M1_DIR.glob("*_NXT.parquet"))
        full_files = sorted(M1_DIR.glob("*_chart_DB_parquet.parquet"))
        dates = sorted({
            f.name.split("_")[0] for f in (nxt_files + full_files)
            if f.name[0].isdigit()
        })
    else:
        dates = [d.strip() for d in args.dates.split(",")]

    print(f"Strategy C — BB 압축→확장 확장 백테스트: {len(dates)}일")
    print(f"대상 일자: {dates}")
    print(f"파라미터: BB period={BB_PERIOD}분, K={BB_K}, squeeze<{BB_SQUEEZE_PCT}%, expand>{BB_EXPAND_PCT}%")
    print("=" * 95)

    all_trades = []
    per_day = []
    for d in dates:
        loaded = load_day(d)
        if loaded is None:
            print(f"[{d}] 데이터 없음 → 스킵")
            continue
        df, fname = loaded
        n_codes = df["code"].n_unique()
        regime = classify_regime(df)
        df2 = compute_bb(df)
        trades = simulate(df2, d)
        all_trades.extend(trades)
        per_day.append({"date": d, "regime": regime, "trades": trades, "codes": n_codes})
        # 일자별 한 줄
        wins = sum(1 for t in trades if t["pnl_pct"] > 0)
        total = sum(t["pnl_pct"] for t in trades)
        n = len(trades)
        wr = wins/n*100 if n else 0
        avg = total/n if n else 0
        print(f"[{d}] regime={regime:<4} 종목={n_codes:4d} 거래={n:4d} "
              f"승률={wr:5.1f}% 총={total:+7.2f}% 평균={avg:+.3f}% ({fname[:40]})")

    print()
    print("=" * 95)
    print(f"[총합 {len(per_day)}일]")
    report_summary("Strategy C 합계", all_trades)

    # 사유별 분포
    if all_trades:
        print(f"\n[매도 사유별]")
        cnt = Counter(t["exit_reason"] for t in all_trades)
        for reason, n in cnt.most_common():
            rt = [t for t in all_trades if t["exit_reason"] == reason]
            avg = sum(t["pnl_pct"] for t in rt) / n
            win_n = sum(1 for t in rt if t["pnl_pct"] > 0)
            print(f"  {reason:<18} {n:>4}건 | 승률={win_n/n*100:5.1f}% | 평균={avg:+.3f}%")

    # 국면별 분포
    print(f"\n[국면별 성과]")
    for regime in ["UP", "FLAT", "DOWN", "UNKNOWN"]:
        days = [d for d in per_day if d["regime"] == regime]
        if not days:
            continue
        all_tr = [t for d in days for t in d["trades"]]
        if not all_tr:
            print(f"  [{regime}] 일수={len(days)} 거래=0")
            continue
        n = len(all_tr)
        wins = sum(1 for t in all_tr if t["pnl_pct"] > 0)
        total = sum(t["pnl_pct"] for t in all_tr)
        print(f"  [{regime:<4}] 일수={len(days):2d} 거래={n:4d} "
              f"승률={wins/n*100:5.1f}% 총={total:+7.2f}% 평균={total/n:+.3f}%")


if __name__ == "__main__":
    main()
