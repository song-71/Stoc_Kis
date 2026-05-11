"""
Strategy A-v4 — 28% 5분유지 + 당일홀드 + 상한가 익일MA매도
Query_str_uplimit_5min_hold_nextday_v2_simulation.py

v3 대비 변경:
  1. 하드손절: -3% → -5%
  2. 익일 갭하락 감지: 시초가 -5% → 09:00~09:03 분봉 중 하나라도 -3% 이하면 매도
  3. 매수 유지 확인: 3분 → 5분

매수:
  F1. tdy_ret 가 28% 상향 돌파 (edge) 시점 t
  F2. t+1 ~ t+5 분봉 모두 28% 이상 유지
  F3. 09:30 ≤ t ≤ 14:30
  → t+5 분봉 close 에서 매수

당일 Exit:
  E1. 분봉 low ≤ buy × 0.95 → 하드 손절 -5%
  E2. 14:55 close:
      - tdy_ret ≥ 29.5% → 익일 홀드
      - 아니면 마감 청산

익일 Exit (홀드):
  D1. 09:00~09:03 분봉 중 low ≤ buy × 0.97 → 즉시 손절 (갭 -3%)
  D2. MA5 < MA20 데드크로스 → 매도
  D3. 14:55 마감
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from collections import Counter

sys.path.append(str(Path(__file__).resolve().parents[1]))

import duckdb
import polars as pl
from kis_utils import _configure_duckdb_s3

S3_BASE = "s3://tfttrain/KIS_DB/market_data/1m"

BUY_TRIGGER_CTRT = 0.28
BUY_UPPER_LIMIT = 0.30
HOLD_ONVERNIGHT_CTRT = 0.295
MIN_PRICE = 1000
CLOSE_TIME = "145500"
MAX_BUY_TIME = "143000"
SUSTAIN_MINUTES = 5           # v4: 3 → 5
HARD_LOSS = 0.05              # v4: 3% → 5%
NEXTDAY_GAP_LOSS = 0.03       # v4: 5% → 3% (더 민감하게)
NEXTDAY_GAP_WINDOW = "090300" # 09:00~09:03 까지 체크
MA_FAST = 5
MA_SLOW = 20


def list_dates(con):
    r = con.execute(f"""
        SELECT DISTINCT regexp_extract(file, 'date=([0-9-]+)', 1) as date
        FROM glob('{S3_BASE}/*/ohlcv.parquet')
        ORDER BY date
    """).df()
    return r["date"].tolist()


def load_day(con, date_str: str):
    path = f"{S3_BASE}/date={date_str}/ohlcv.parquet"
    try:
        df = con.execute(f"""
            SELECT code, time, open, high, low, close, volume, tdy_ret, pdy_close
            FROM read_parquet('{path}')
            WHERE CAST(time AS VARCHAR) >= '090000'
              AND CAST(close AS BIGINT) > 0
            ORDER BY code, time
        """).df()
    except Exception as e:
        print(f"  [{date_str}] load error: {e}")
        return None
    if df.empty:
        return None
    pdf = pl.from_pandas(df).with_columns([
        pl.col("time").cast(pl.Utf8).str.zfill(6),
        pl.col("code").cast(pl.Utf8).str.zfill(6),
        pl.col("open").cast(pl.Float64, strict=False),
        pl.col("close").cast(pl.Float64, strict=False),
        pl.col("low").cast(pl.Float64, strict=False),
        pl.col("high").cast(pl.Float64, strict=False),
        pl.col("tdy_ret").cast(pl.Float64, strict=False),
    ]).sort(["code", "time"])
    return pdf


def simulate_day(df, buy_date: str):
    trades = []
    holdover = []

    maxr = df.group_by("code").agg(pl.col("tdy_ret").max().alias("mx"))
    target_codes = maxr.filter(pl.col("mx") >= BUY_TRIGGER_CTRT)["code"].to_list()
    if not target_codes:
        return trades, holdover
    df = df.filter(pl.col("code").is_in(target_codes))
    df = df.with_columns(
        pl.col("tdy_ret").shift(1).over("code").alias("tdy_ret_prev")
    )

    by_code = df.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        rows = g.to_dicts()
        n = len(rows)
        pos = None
        bought = False
        for i, r in enumerate(rows):
            close = r["close"]
            hms = r["time"]
            ctrt = r["tdy_ret"] or 0
            ctrt_prev = r["tdy_ret_prev"] or 0

            if pos is None:
                if bought:
                    continue
                if not (ctrt_prev < BUY_TRIGGER_CTRT <= ctrt < BUY_UPPER_LIMIT):
                    continue
                if close < MIN_PRICE:
                    continue
                if not ("093000" <= hms <= MAX_BUY_TIME):
                    continue
                # SUSTAIN_MINUTES 모두 유지 확인
                if i + SUSTAIN_MINUTES >= n:
                    continue
                sustain_ok = all(
                    (rows[i+j]["tdy_ret"] or 0) >= BUY_TRIGGER_CTRT
                    for j in range(1, SUSTAIN_MINUTES + 1)
                )
                if not sustain_ok:
                    continue
                buy_idx = i + SUSTAIN_MINUTES
                buy_row = rows[buy_idx]
                pos = {
                    "buy": buy_row["close"],
                    "buy_time": buy_row["time"],
                    "high": buy_row["close"],
                }
                bought = True
                # 이후 exit 판정
                for k in range(buy_idx + 1, n):
                    rr = rows[k]
                    c2 = rr["close"]
                    l2 = rr["low"]
                    t2 = rr["time"]
                    ctrt2 = rr["tdy_ret"] or 0
                    if c2 > pos["high"]:
                        pos["high"] = c2
                    if l2 <= pos["buy"] * (1 - HARD_LOSS):
                        sell = pos["buy"] * (1 - HARD_LOSS)
                        pnl = (sell / pos["buy"] - 1) * 100
                        trades.append({
                            "code": code, "date": buy_date,
                            "buy_time": pos["buy_time"], "sell_time": t2,
                            "buy": pos["buy"], "sell": sell, "pnl": pnl,
                            "reason": f"하드손절-{HARD_LOSS*100:.0f}%",
                        })
                        pos = None
                        break
                    if t2 >= CLOSE_TIME:
                        if ctrt2 >= HOLD_ONVERNIGHT_CTRT:
                            holdover.append({
                                "code": code, "buy_date": buy_date,
                                "buy_time": pos["buy_time"], "buy": pos["buy"],
                                "last_close": c2,
                            })
                        else:
                            pnl = (c2 / pos["buy"] - 1) * 100
                            trades.append({
                                "code": code, "date": buy_date,
                                "buy_time": pos["buy_time"], "sell_time": t2,
                                "buy": pos["buy"], "sell": c2, "pnl": pnl,
                                "reason": "당일마감",
                            })
                        pos = None
                        break
                if pos is not None:
                    last = rows[-1]
                    if last["tdy_ret"] and last["tdy_ret"] >= HOLD_ONVERNIGHT_CTRT:
                        holdover.append({
                            "code": code, "buy_date": buy_date,
                            "buy_time": pos["buy_time"], "buy": pos["buy"],
                            "last_close": last["close"],
                        })
                    else:
                        pnl = (last["close"] / pos["buy"] - 1) * 100
                        trades.append({
                            "code": code, "date": buy_date,
                            "buy_time": pos["buy_time"], "sell_time": last["time"],
                            "buy": pos["buy"], "sell": last["close"], "pnl": pnl,
                            "reason": "자동마감",
                        })
                    pos = None
                break
    return trades, holdover


def simulate_nextday(df_next, holdovers):
    trades = []
    if not holdovers or df_next is None or df_next.height == 0:
        return trades
    code_to_hold = {h["code"]: h for h in holdovers}
    df2 = df_next.filter(pl.col("code").is_in(list(code_to_hold.keys())))
    df2 = df2.with_columns([
        pl.col("close").ewm_mean(alpha=2/(MA_FAST+1), adjust=False).over("code").alias("ma_f"),
        pl.col("close").ewm_mean(alpha=2/(MA_SLOW+1), adjust=False).over("code").alias("ma_s"),
    ])

    by_code = df2.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        hold = code_to_hold[code]
        rows = g.to_dicts()
        if not rows:
            continue
        # D1: 09:00~09:03 중 low <= buy × 0.97 (-3%)
        early = [r for r in rows if r["time"] <= NEXTDAY_GAP_WINDOW]
        gap_sold = False
        for er in early:
            if (er["low"] or 0) <= hold["buy"] * (1 - NEXTDAY_GAP_LOSS):
                sell = hold["buy"] * (1 - NEXTDAY_GAP_LOSS)
                pnl = (sell / hold["buy"] - 1) * 100
                trades.append({
                    "code": code, "date": hold["buy_date"] + "→다음날",
                    "buy_time": hold["buy_time"], "sell_time": er["time"],
                    "buy": hold["buy"], "sell": sell, "pnl": pnl,
                    "reason": f"익일초반갭-{NEXTDAY_GAP_LOSS*100:.0f}%",
                })
                gap_sold = True
                break
        if gap_sold:
            continue

        # D2/D3: MA 데드크로스 or 마감
        sold = False
        for r in rows:
            ma_f = r["ma_f"]
            ma_s = r["ma_s"]
            t2 = r["time"]
            c2 = r["close"]
            if ma_f is None or ma_s is None:
                continue
            if ma_f < ma_s:
                pnl = (c2 / hold["buy"] - 1) * 100
                trades.append({
                    "code": code, "date": hold["buy_date"] + "→다음날",
                    "buy_time": hold["buy_time"], "sell_time": t2,
                    "buy": hold["buy"], "sell": c2, "pnl": pnl,
                    "reason": "익일MA데드크로스",
                })
                sold = True
                break
            if t2 >= CLOSE_TIME:
                pnl = (c2 / hold["buy"] - 1) * 100
                trades.append({
                    "code": code, "date": hold["buy_date"] + "→다음날",
                    "buy_time": hold["buy_time"], "sell_time": t2,
                    "buy": hold["buy"], "sell": c2, "pnl": pnl,
                    "reason": "익일마감",
                })
                sold = True
                break
        if not sold and rows:
            last = rows[-1]
            pnl = (last["close"] / hold["buy"] - 1) * 100
            trades.append({
                "code": code, "date": hold["buy_date"] + "→다음날",
                "buy_time": hold["buy_time"], "sell_time": last["time"],
                "buy": hold["buy"], "sell": last["close"], "pnl": pnl,
                "reason": "익일자동마감",
            })
    return trades


def report(label, trades):
    if not trades:
        print(f"  [{label}] 거래 0건")
        return
    n = len(trades)
    wins = sum(1 for t in trades if t["pnl"] > 0)
    total = sum(t["pnl"] for t in trades)
    worst = min(t["pnl"] for t in trades)
    best = max(t["pnl"] for t in trades)
    big_w = sum(1 for t in trades if t["pnl"] >= 5.0)
    big_l = sum(1 for t in trades if t["pnl"] <= -5.0)
    print(f"  [{label}] 거래={n:4d}건 | 승률={wins/n*100:5.1f}% | 총PnL={total:+8.2f}% | "
          f"평균={total/n:+.3f}% | worst={worst:+.1f}% best={best:+.1f}% | +5%↑={big_w} -5%↓={big_l}")


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
    print(f"Strategy A-v4 — 28% {SUSTAIN_MINUTES}분유지 + 당일홀드 + 상한가 익일MA매도")
    print(f"하드손절={HARD_LOSS*100:.0f}%, 익일초반갭={NEXTDAY_GAP_LOSS*100:.0f}%, hold_overnight≥{HOLD_ONVERNIGHT_CTRT*100:.1f}%")
    print(f"대상 일자: {len(dates)}일")
    print("=" * 100)

    date_dfs = {}
    for d in dates:
        df = load_day(con, d)
        if df is not None:
            date_dfs[d] = df

    all_trades = []
    all_holdovers = []
    for i, d in enumerate(dates):
        if d not in date_dfs:
            continue
        tr, ho = simulate_day(date_dfs[d], d)
        all_trades.extend(tr)
        all_holdovers.extend(ho)
        if ho:
            next_d = dates[i+1] if i+1 < len(dates) else None
            if next_d and next_d in date_dfs:
                tr_next = simulate_nextday(date_dfs[next_d], ho)
                all_trades.extend(tr_next)
            else:
                for h in ho:
                    pnl = (h["last_close"] / h["buy"] - 1) * 100
                    all_trades.append({
                        "code": h["code"], "date": h["buy_date"],
                        "buy_time": h["buy_time"], "sell_time": CLOSE_TIME,
                        "buy": h["buy"], "sell": h["last_close"], "pnl": pnl,
                        "reason": "당일마감(익일데이터없음)",
                    })

    print()
    print("=" * 100)
    report("Strategy A-v4 총합", all_trades)

    print(f"\n[매도 사유별]")
    cnt = Counter(t["reason"] for t in all_trades)
    for reason, cn in cnt.most_common():
        rt = [t for t in all_trades if t["reason"] == reason]
        avg = sum(t["pnl"] for t in rt) / cn
        wins = sum(1 for t in rt if t["pnl"] > 0)
        tot = sum(t["pnl"] for t in rt)
        print(f"  {reason:<24} {cn:>4}건 | 승률={wins/cn*100:5.1f}% | 평균={avg:+.3f}% | 총={tot:+.2f}%")

    # 익일 홀드 요약
    nd = [t for t in all_trades if t["reason"].startswith("익일")]
    print(f"\n[익일 홀드 전환 {len(all_holdovers)}건 중 익일 매도 {len(nd)}건]")
    report("익일 매도 성과", nd)


if __name__ == "__main__":
    main()
