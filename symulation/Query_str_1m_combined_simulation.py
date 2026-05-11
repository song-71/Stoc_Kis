"""
Strategy B (MA trend) + C (BB expansion) — 1분봉 기반 백테스트
Query_str_1m_combined_simulation.py

데이터: data/1m_data/YYYYMMDD_1m_chart_DB_parquet.parquet
스키마: date, time(HHMMSS), code, name, open, high, low, close, volume, tdy_ret, pdy_close

[Strategy B — 1분봉 MA trend]
매수: MA5 > MA20 골든크로스 (edge) + 15 ≤ ret*100 < 29 + 거래량 ≥ 이전 5분평균 × 1.5
매도: MA5 < MA20 데드크로스 OR -3% 손절 OR 고점 -3% 트레일 (+3% 활성) OR 14:55 마감

[Strategy C — 1분봉 BB expansion]
BB period = 20분, K = 2.0
매수: bb_width_pct 최소값 < 3% (squeeze) + 현재 > 5% (expand)
      + 최근 10분 내 low < bb_lower 기록 + 직전 분봉 close < bb_lower
      + 현재 close > 직전 close (첫 양봉)
      + tdy_ret > 0
매도: close < bb_mid OR close < bb_lower 재이탈 OR 손절 OR 트레일 OR 14:55 마감

사용법:
  python Query_str_1m_combined_simulation.py --dates 20260422,20260423
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from collections import deque, Counter

sys.path.append(str(Path(__file__).resolve().parents[1]))

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
M1_DIR = DATA_DIR / "1m_data"

# Strategy 공통
BUY_CTRT_MIN = 0.15     # tdy_ret 기준 (15%)
BUY_CTRT_MAX = 0.29     # 29%
STOP_LOSS_PCT = 0.03
TRAIL_PCT = 0.03
TRAIL_ACTIVATE_PCT = 0.03
MIN_VOL_FACTOR = 1.5    # 현재 분봉 거래량 >= 이전 5분 평균 * 1.5

# Strategy B: MA period (분)
MA_FAST = 5
MA_SLOW = 20

# Strategy C: BB
BB_PERIOD = 20
BB_K = 2.0
BB_SQUEEZE_PCT = 3.0    # bb_width / close * 100 < 3%
BB_EXPAND_PCT = 5.0     # > 5%
BB_CROSS_LOOKBACK = 10  # 최근 10분봉 내 하단 이탈 이력


def load_day(date_str: str) -> pl.DataFrame | None:
    """YYYYMMDD 문자열로 1분봉 parquet 로드."""
    path = M1_DIR / f"{date_str}_1m_chart_DB_parquet.parquet"
    if not path.exists():
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
    ]).sort(["code", "time"])
    # 시간 필터: 09:30 ~ 14:55
    df = df.filter(
        (pl.col("time") >= "093000")
        & (pl.col("time") <= "145500")
        & (pl.col("close") > 0)
    )
    return df


def compute_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """종목별 MA, BB 계산."""
    df = df.with_columns([
        pl.col("close").ewm_mean(alpha=2/(MA_FAST+1), adjust=False).over("code").alias("ma_fast"),
        pl.col("close").ewm_mean(alpha=2/(MA_SLOW+1), adjust=False).over("code").alias("ma_slow"),
        pl.col("close").rolling_mean(BB_PERIOD, min_samples=BB_PERIOD).over("code").alias("bb_mid"),
        pl.col("close").rolling_std(BB_PERIOD, min_samples=BB_PERIOD).over("code").alias("bb_std"),
        pl.col("volume").rolling_mean(5, min_samples=3).over("code").alias("vol_ma5"),
    ])
    df = df.with_columns([
        (pl.col("bb_mid") + BB_K * pl.col("bb_std")).alias("bb_upper"),
        (pl.col("bb_mid") - BB_K * pl.col("bb_std")).alias("bb_lower"),
        (2 * BB_K * pl.col("bb_std") / pl.col("close") * 100).alias("bb_width_pct"),
        pl.col("ma_fast").shift(1).over("code").alias("ma_fast_prev"),
        pl.col("ma_slow").shift(1).over("code").alias("ma_slow_prev"),
        pl.col("close").shift(1).over("code").alias("close_prev"),
    ])
    return df


def simulate_strategy_b(df: pl.DataFrame) -> list[dict]:
    """Strategy B: MA 골든크로스/데드크로스 (1분봉)."""
    trades = []
    by_code = df.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        if g.height < MA_SLOW + 2:
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
                # 매수 조건
                golden = (ma_f_p <= ma_s_p) and (ma_f > ma_s)
                in_range = (BUY_CTRT_MIN <= tdy_ret < BUY_CTRT_MAX)
                vol_ok = vol_ma5 is not None and vol_ma5 > 0 and vol >= vol_ma5 * MIN_VOL_FACTOR
                if golden and in_range and vol_ok:
                    pos = {
                        "buy_price": close,
                        "buy_time": time_str,
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
                elif ma_f < ma_s:
                    exit_reason = "데드크로스"
                elif time_str >= "145500":
                    exit_reason = "마감"

                if exit_reason:
                    pnl_pct = (close / pos["buy_price"] - 1) * 100
                    trades.append({
                        "code": code, "buy_time": pos["buy_time"], "buy_price": pos["buy_price"],
                        "sell_time": time_str, "sell_price": close,
                        "pnl_pct": pnl_pct, "exit_reason": exit_reason,
                        "holding_min": int(time_str) - int(pos["buy_time"]),
                    })
                    pos = None
        # 미청산 강제 마감
        if pos is not None and g.height > 0:
            last = g.row(-1, named=True)
            pnl_pct = (last["close"] / pos["buy_price"] - 1) * 100
            trades.append({
                "code": code, "buy_time": pos["buy_time"], "buy_price": pos["buy_price"],
                "sell_time": last["time"], "sell_price": last["close"],
                "pnl_pct": pnl_pct, "exit_reason": "장마감자동청산",
            })
    return trades


def simulate_strategy_c(df: pl.DataFrame) -> list[dict]:
    """Strategy C: BB squeeze → expansion + 하단 반등 (1분봉)."""
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
            cur_below = (low < bb_lower)  # 이번 분봉 중에 하단 이탈 경험 여부
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
                    pos = {
                        "buy_price": close,
                        "buy_time": time_str,
                        "highest": close,
                    }
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
                    pnl_pct = (close / pos["buy_price"] - 1) * 100
                    trades.append({
                        "code": code, "buy_time": pos["buy_time"], "buy_price": pos["buy_price"],
                        "sell_time": time_str, "sell_price": close,
                        "pnl_pct": pnl_pct, "exit_reason": exit_reason,
                    })
                    pos = None

            width_hist.append(bb_width_pct)
            cross_hist.append(cur_below)

        if pos is not None and g.height > 0:
            last = g.row(-1, named=True)
            pnl_pct = (last["close"] / pos["buy_price"] - 1) * 100
            trades.append({
                "code": code, "buy_time": pos["buy_time"], "buy_price": pos["buy_price"],
                "sell_time": last["time"], "sell_price": last["close"],
                "pnl_pct": pnl_pct, "exit_reason": "장마감자동청산",
            })
    return trades


def report(label: str, trades: list[dict], date_str: str, n_codes: int):
    if not trades:
        print(f"  [{label}] {date_str}: 거래 0건 (종목수={n_codes})")
        return
    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    total = sum(t["pnl_pct"] for t in trades)
    avg = total / len(trades)
    win_rate = wins / len(trades) * 100
    big_wins = sum(1 for t in trades if t["pnl_pct"] >= 3.0)
    big_loss = sum(1 for t in trades if t["pnl_pct"] <= -2.0)
    print(f"  [{label}] {date_str}: 거래={len(trades):3d}건 | 승률={win_rate:5.1f}% | "
          f"총PnL={total:+7.2f}% | 평균={avg:+.3f}% | +3%↑={big_wins} | -2%↓={big_loss}")

    # 매도 사유 분포 (간략)
    cnt = Counter(t["exit_reason"] for t in trades)
    reason_str = " | ".join(f"{r}:{n}" for r, n in cnt.most_common(4))
    print(f"      사유: {reason_str}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="20260422,20260423")
    args = ap.parse_args()
    dates = [d.strip() for d in args.dates.split(",")]
    print(f"Strategy B/C — 1분봉 백테스트: {dates}")
    print(f"(MA: fast=EMA{MA_FAST}, slow=EMA{MA_SLOW}, BB: period={BB_PERIOD}min, "
          f"squeeze<{BB_SQUEEZE_PCT}%, expand>{BB_EXPAND_PCT}%)")
    print("=" * 75)

    all_b = []
    all_c = []
    for d in dates:
        df = load_day(d)
        if df is None or df.height == 0:
            print(f"[{d}] 데이터 없음")
            continue
        n_codes = df["code"].n_unique()
        df = compute_indicators(df)

        tb = simulate_strategy_b(df)
        tc = simulate_strategy_c(df)
        all_b.extend(tb)
        all_c.extend(tc)
        print(f"[{d}] 종목수={n_codes}")
        report("B (MA)", tb, d, n_codes)
        report("C (BB)", tc, d, n_codes)
        print()

    print("=" * 75)
    print("[총합]")
    report("B (MA)", all_b, f"{len(dates)}일", 0)
    report("C (BB)", all_c, f"{len(dates)}일", 0)

    # Strategy B 사유별 성과
    if all_b:
        print("\n[Strategy B 매도사유별 평균]")
        cnt = Counter(t["exit_reason"] for t in all_b)
        for reason, n in cnt.most_common():
            rt = [t for t in all_b if t["exit_reason"] == reason]
            avg = sum(t["pnl_pct"] for t in rt) / max(1, n)
            print(f"  {reason:<20} {n:>4}건 평균 {avg:+.2f}%")

    if all_c:
        print("\n[Strategy C 매도사유별 평균]")
        cnt = Counter(t["exit_reason"] for t in all_c)
        for reason, n in cnt.most_common():
            rt = [t for t in all_c if t["exit_reason"] == reason]
            avg = sum(t["pnl_pct"] for t in rt) / max(1, n)
            print(f"  {reason:<20} {n:>4}건 평균 {avg:+.2f}%")


if __name__ == "__main__":
    main()
