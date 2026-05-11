"""
Strategy A-v3 — 28% 3분유지 + 당일 홀드 + 상한가 익일 MA매도
Query_str_uplimit_3min_hold_nextday_simulation.py

매수:
  F1. tdy_ret 가 28% 상향 돌파 (edge) 시점 t
  F2. t+1, t+2, t+3 분봉 모두 28% 이상 유지 확인 → t+3 분 close 로 매수
  F3. 09:30 ≤ t ≤ 14:30

당일 Exit (14:55 까지):
  E1. 분봉 low <= buy × 0.97 → 하드 손절 -3%
  E2. 14:55 close 기준:
      - tdy_ret >= 29.5% 상한가 근처 → **익일 홀드 (overnight)**
      - 아니면 → 마감 청산

익일 Exit (overnight 포지션):
  D1. 09:00 시초가 < buy × 0.95 → 즉시 손절 (갭 하락)
  D2. MA5 > MA20 상태 유지 중 → 보유
  D3. MA5 < MA20 데드크로스 → 매도
  D4. 14:55 익일 마감 → 강제 청산
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
HOLD_ONVERNIGHT_CTRT = 0.295   # 14:55 에 29.5% 이상이면 익일 홀드
MIN_PRICE = 1000
CLOSE_TIME = "145500"
MAX_BUY_TIME = "143000"
SUSTAIN_MINUTES = 3             # 28% 유지 확인 분
HARD_LOSS = 0.03
NEXTDAY_GAP_LOSS = 0.05         # 익일 시초가 -5% 이하면 즉시 매도
MA_FAST = 5
MA_SLOW = 20


def list_dates(con):
    r = con.execute(f"""
        SELECT DISTINCT regexp_extract(file, 'date=([0-9-]+)', 1) as date
        FROM glob('{S3_BASE}/*/ohlcv.parquet')
        ORDER BY date
    """).df()
    return r["date"].tolist()


def load_day(con, date_str: str) -> pl.DataFrame | None:
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
    """당일 매수/매도 시뮬. 익일 홀드 대상 리스트도 반환."""
    trades = []
    holdover = []  # 익일 홀드 대상 {code, buy_price, buy_date, buy_time, last_close_today}

    # 28% 돌파 후보 종목만 필터링
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
            low = r["low"]
            hms = r["time"]
            ctrt = r["tdy_ret"] or 0
            ctrt_prev = r["tdy_ret_prev"] or 0

            if pos is None:
                if bought:
                    continue
                # F1: 28% 돌파 edge
                if not (ctrt_prev < BUY_TRIGGER_CTRT <= ctrt < BUY_UPPER_LIMIT):
                    continue
                if close < MIN_PRICE:
                    continue
                if not ("093000" <= hms <= MAX_BUY_TIME):
                    continue
                # F2: t+1 ~ t+SUSTAIN_MINUTES 유지 확인
                if i + SUSTAIN_MINUTES >= n:
                    continue
                sustain_ok = True
                for j in range(1, SUSTAIN_MINUTES + 1):
                    if rows[i + j]["tdy_ret"] < BUY_TRIGGER_CTRT:
                        sustain_ok = False
                        break
                if not sustain_ok:
                    continue
                # 매수는 t+SUSTAIN 분봉 close 에서
                buy_idx = i + SUSTAIN_MINUTES
                buy_row = rows[buy_idx]
                pos = {
                    "buy": buy_row["close"],
                    "buy_time": buy_row["time"],
                    "high": buy_row["close"],
                }
                bought = True
                # 이후 분봉부터 exit 판정 시작
                for k in range(buy_idx + 1, n):
                    rr = rows[k]
                    c2 = rr["close"]
                    l2 = rr["low"]
                    h2 = rr["high"]
                    t2 = rr["time"]
                    ctrt2 = rr["tdy_ret"] or 0
                    if c2 > pos["high"]:
                        pos["high"] = c2
                    # 하드 손절 (분봉 low 기준)
                    if l2 <= pos["buy"] * (1 - HARD_LOSS):
                        sell = pos["buy"] * (1 - HARD_LOSS)
                        pnl = (sell / pos["buy"] - 1) * 100
                        trades.append({
                            "code": code, "date": buy_date,
                            "buy_time": pos["buy_time"], "sell_time": t2,
                            "buy": pos["buy"], "sell": sell, "pnl": pnl,
                            "reason": "하드손절-3%",
                        })
                        pos = None
                        break
                    # 14:55 도달
                    if t2 >= CLOSE_TIME:
                        # 상한가 근처면 익일 홀드
                        if ctrt2 >= HOLD_ONVERNIGHT_CTRT:
                            holdover.append({
                                "code": code, "buy_date": buy_date,
                                "buy_time": pos["buy_time"], "buy": pos["buy"],
                                "last_close": c2, "last_ctrt": ctrt2,
                            })
                        else:
                            pnl = (c2 / pos["buy"] - 1) * 100
                            trades.append({
                                "code": code, "date": buy_date,
                                "buy_time": pos["buy_time"], "sell_time": t2,
                                "buy": pos["buy"], "sell": c2, "pnl": pnl,
                                "reason": f"마감청산({ctrt2*100:.1f}%)",
                            })
                        pos = None
                        break
                # 당일 마지막까지 exit 못 시키고 끝난 경우 → 마지막 close 로 마감
                if pos is not None:
                    last = rows[-1]
                    if last["tdy_ret"] and last["tdy_ret"] >= HOLD_ONVERNIGHT_CTRT:
                        holdover.append({
                            "code": code, "buy_date": buy_date,
                            "buy_time": pos["buy_time"], "buy": pos["buy"],
                            "last_close": last["close"], "last_ctrt": last["tdy_ret"],
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
                break  # 종목당 1회 매수로 다음 종목으로
    return trades, holdover


def simulate_nextday_sell(df_next, holdovers: list):
    """익일 1분봉 에서 홀드 포지션 매도 시뮬."""
    trades = []
    if not holdovers or df_next is None or df_next.height == 0:
        return trades
    code_to_hold = {h["code"]: h for h in holdovers}
    # MA5, MA20 계산
    df2 = df_next.filter(pl.col("code").is_in(list(code_to_hold.keys())))
    df2 = df2.with_columns([
        pl.col("close").ewm_mean(alpha=2/(MA_FAST+1), adjust=False).over("code").alias("ma_f"),
        pl.col("close").ewm_mean(alpha=2/(MA_SLOW+1), adjust=False).over("code").alias("ma_s"),
    ])

    by_code = df2.partition_by("code", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        hold = code_to_hold.get(code)
        if hold is None:
            continue
        rows = g.to_dicts()
        if not rows:
            continue
        # D1: 09:00 시초가 -5% 이하 → 즉시 손절
        first = rows[0]
        if first["open"] <= hold["buy"] * (1 - NEXTDAY_GAP_LOSS):
            sell = first["open"]
            pnl = (sell / hold["buy"] - 1) * 100
            trades.append({
                "code": code, "date": hold["buy_date"] + "→다음날",
                "buy_time": hold["buy_time"], "sell_time": first["time"],
                "buy": hold["buy"], "sell": sell, "pnl": pnl,
                "reason": "익일시초갭-5%",
            })
            continue

        sold = False
        for r in rows:
            ma_f = r["ma_f"]
            ma_s = r["ma_s"]
            t2 = r["time"]
            c2 = r["close"]
            if ma_f is None or ma_s is None:
                continue
            # D3: 데드크로스
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
            # D4: 14:55 마감
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
    print(f"Strategy A-v3 — 28%3분유지 + 당일홀드 + 상한가 익일MA매도")
    print(f"대상 일자: {len(dates)}일 ({dates[0]} ~ {dates[-1]})")
    print(f"sustain={SUSTAIN_MINUTES}분, hold_overnight≥{HOLD_ONVERNIGHT_CTRT*100:.1f}%, hard_loss={HARD_LOSS*100:.0f}%")
    print("=" * 100)

    # 일자별 데이터 로드 (메모리 부담 — 모두 로드 or on-demand)
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
        df = date_dfs[d]
        tr, ho = simulate_day(df, d)
        all_trades.extend(tr)
        # 익일 데이터가 있으면 익일 매도 시뮬
        if ho:
            next_d = dates[i+1] if i+1 < len(dates) else None
            if next_d and next_d in date_dfs:
                tr_next = simulate_nextday_sell(date_dfs[next_d], ho)
                all_trades.extend(tr_next)
                all_holdovers.extend(ho)
            else:
                # 익일 데이터 없으면 last_close 로 당일 마감 처리
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
    report("Strategy A-v3 총합", all_trades)

    # 사유별
    print(f"\n[매도 사유별]")
    cnt = Counter(t["reason"] for t in all_trades)
    for reason, cn in cnt.most_common():
        rt = [t for t in all_trades if t["reason"] == reason]
        avg = sum(t["pnl"] for t in rt) / cn
        wins = sum(1 for t in rt if t["pnl"] > 0)
        tot = sum(t["pnl"] for t in rt)
        print(f"  {reason:<24} {cn:>4}건 | 승률={wins/cn*100:5.1f}% | 평균={avg:+.3f}% | 총={tot:+.2f}%")

    # 익일 홀드 비중
    print(f"\n[홀드 전환 건수] {len(all_holdovers)}건 (14:55 에 29.5%+ 였던 종목)")
    if all_holdovers:
        # 그 holdovers 실제 익일 결과
        nd_trades = [t for t in all_trades if t["reason"].startswith("익일")]
        if nd_trades:
            report("익일 매도 건수", nd_trades)


if __name__ == "__main__":
    main()
