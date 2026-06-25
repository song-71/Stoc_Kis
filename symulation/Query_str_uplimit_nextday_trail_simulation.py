"""
Strategy EarlyPop-Trail — 전일 상한가 + 익일 시초 매수 + 초반 고점 트레일링 청산
Query_str_uplimit_nextday_trail_simulation.py

핵심(검증된 사실):
  전일 상한가 종목은 익일 09:02~09:05 에 평균 고점(+5%대) → 이후 흘러내려 마감 +1.9%.
  (도메인 지식: 한국장은 09:00~09:05 초반 급등 후 하락. 빠른건 09:01~02 고점.)
  → 09:00 시초 바스켓 매수 후 '고점 대비 -Y% 트레일링'으로 반전 시 즉시 청산하면
    초반 급등을 먹고 하락은 피한다.

대상: Select_Tr_target_list.csv (str3: 당일 tdy_ctrt>=0.28 상한가 → 다음 거래일 적용)
매수: D일 첫 분봉(09:00) close 에 전 대상 종목 매수(바스켓).
청산: 진입 후 종가 고점(peak) 갱신; low ≤ peak×(1-TRAIL) → peak×(1-TRAIL) 청산. 미발생 시 마감.
      (진입 시 peak=buy → 초기 손절 = buy×(1-TRAIL) 역할 겸함. 익절캡 없음.)
"""
from __future__ import annotations
import argparse
import csv
import statistics as st
import sys
from pathlib import Path
from collections import defaultdict

sys.path.append(str(Path(__file__).resolve().parents[1]))

import duckdb
from kis_utils import _configure_duckdb_s3

S3_BASE = "s3://tfttrain/KIS_DB/market_data/1m"
CSV_PATH = str(Path(__file__).resolve().parent / "Select_Tr_target_list.csv")
MIN_PRICE = 1000
TRAIL_LEVELS = [0.01, 0.02, 0.03, 0.05]


def list_dates(con):
    return [r[0] for r in con.execute(
        f"SELECT DISTINCT regexp_extract(file,'date=([0-9-]+)',1) d "
        f"FROM glob('{S3_BASE}/*/ohlcv.parquet') ORDER BY d").fetchall()]


def load_targets(csv_path):
    out = defaultdict(dict)
    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            d = (row.get("date") or "").strip()
            sym = (row.get("symbol") or "").strip().zfill(6)
            if not (d and sym):
                continue
            try:
                strong = float(row["close"]) >= float(row["high"])
            except Exception:
                strong = False
            out[d][sym] = strong
    return out


def load_day(con, td, codes):
    inl = ",".join("'" + c + "'" for c in sorted(codes))
    rows = con.execute(f"""
        SELECT lpad(CAST(code AS VARCHAR),6,'0') code, time, high, low, close, pdy_close, open
        FROM read_parquet('{S3_BASE}/date={td}/ohlcv.parquet')
        WHERE lpad(CAST(code AS VARCHAR),6,'0') IN ({inl})
          AND CAST(time AS VARCHAR) >= '090000' AND CAST(close AS BIGINT) > 0
        ORDER BY code, time
    """).fetchall()
    by = defaultdict(list)
    for code, t, h, l, c, pc, o in rows:
        by[code].append((str(t).zfill(6), float(h), float(l), float(c), float(pc or 0), float(o)))
    return by


def simulate(seq, trail):
    """09:00 종가 매수 + 고점대비 -trail 트레일링. 반환 (pnl%, reason, gap, strong_unused)."""
    if len(seq) < 2:
        return None
    buy = seq[0][3]
    o0 = seq[0][5]
    pc = seq[0][4]
    if buy < MIN_PRICE:
        return None
    gap = (o0 / pc - 1) if pc > 0 else 0.0
    peak = buy
    for t, h, l, c, pcx, o in seq[1:]:
        stop_px = peak * (1 - trail)
        if l <= stop_px:
            return ((stop_px / buy - 1) * 100, "트레일청산", gap)
        if c > peak:
            peak = c
    return ((seq[-1][3] / buy - 1) * 100, "마감", gap)


def describe(label, pnls):
    n = len(pnls)
    if n == 0:
        print(f"  {label:<14} 0건")
        return
    wins = sum(1 for x in pnls if x > 0)
    print(f"  {label:<14} n={n:4d} | 평균={st.mean(pnls):+.3f}% | 중앙={st.median(pnls):+5.2f}% | "
          f"승률={wins/n*100:5.1f}% | 총={sum(pnls):+8.1f}% | best={max(pnls):+5.0f}% worst={min(pnls):+5.0f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="all")
    ap.add_argument("--csv", default=CSV_PATH)
    ap.add_argument("--strong-only", action="store_true")
    ap.add_argument("--gap-min", type=float, default=None, help="시초갭 하한(예 0.0)")
    ap.add_argument("--gap-max", type=float, default=None, help="시초갭 상한(예 0.03)")
    args = ap.parse_args()

    con = duckdb.connect(":memory:")
    _configure_duckdb_s3(con)
    s3_dates = list_dates(con)
    didx = {d: i for i, d in enumerate(s3_dates)}
    uplimit = load_targets(args.csv)
    trade_codes = defaultdict(dict)
    for ud in sorted(uplimit):
        i = didx.get(ud)
        if i is not None and i + 1 < len(s3_dates):
            trade_codes[s3_dates[i + 1]].update(uplimit[ud])
    trade_days = sorted(trade_codes)
    if args.dates != "all":
        want = {d.strip() for d in args.dates.split(",")}
        trade_days = [d for d in trade_days if d in want]

    print("Strategy EarlyPop-Trail — 전일 상한가 + 익일 시초매수 + 고점 트레일링 청산")
    print(f"매매일 {len(trade_days)}일 | 09:00 종가 바스켓 매수, 익절캡 없음{' | 강한상한가만' if args.strong_only else ''}")
    print("=" * 100)

    cached = []  # (td, [(seq, strong)])
    for i, td in enumerate(trade_days):
        by = load_day(con, td, set(trade_codes[td].keys()))
        items = []
        for code, seq in by.items():
            strong = trade_codes[td].get(code, False)
            if args.strong_only and not strong:
                continue
            items.append((seq, strong))
        cached.append((td, items))
        print(f"  [{i+1:2d}/{len(trade_days)}] D={td} → {len(items)}종목", flush=True)

    print("\n" + "=" * 100)
    print("[트레일링 폭별 비교]")
    best = None
    for trail in TRAIL_LEVELS:
        recs = []
        for td, items in cached:
            for seq, strong in items:
                r = simulate(seq, trail)
                if not r:
                    continue
                gap = r[2]
                if args.gap_min is not None and gap < args.gap_min:
                    continue
                if args.gap_max is not None and gap >= args.gap_max:
                    continue
                recs.append((td, strong, r[0], gap))
        pnls = [r[2] for r in recs]
        describe(f"트레일 -{trail*100:.0f}%", pnls)
        if pnls and (best is None or sum(pnls) > best[1]):
            best = (trail, sum(pnls), recs)

    # 최적 트레일 상세
    trail, _, recs = best
    print(f"\n[최적 트레일 -{trail*100:.0f}% 상세]")
    describe("강한상한가", [r[2] for r in recs if r[1]])
    describe("약한상한가", [r[2] for r in recs if not r[1]])
    # gap 세그먼트
    print("  · gap 버킷:")
    for a, b in [(-9, 0.0), (0.0, 0.03), (0.03, 0.07), (0.07, 9)]:
        sub = [r[2] for r in recs if a <= r[3] < b]
        describe(f"gap[{a if a>-9 else '−∞'}~{b if b<9 else '∞'})"[:14], sub)
    # 일자별
    byday = defaultdict(list)
    for td, strong, p, gap in recs:
        byday[td].append(p)
    davgs = [(td, sum(ps) / len(ps)) for td, ps in sorted(byday.items())]
    pos = sum(1 for _, a in davgs if a > 0)
    print(f"\n[일자별] {len(davgs)}일 중 일평균>0 인 날 {pos}일 ({pos/len(davgs)*100:.0f}%) | "
          f"일평균 {st.mean([a for _, a in davgs]):+.3f}% | 최악 {min(davgs,key=lambda x:x[1])[1]:+.2f}% 최고 {max(davgs,key=lambda x:x[1])[1]:+.2f}%")


if __name__ == "__main__":
    main()
