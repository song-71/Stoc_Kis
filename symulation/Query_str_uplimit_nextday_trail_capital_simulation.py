"""
Strategy EarlyPop-Trail — 자본 효율/유휴자금 분석
Query_str_uplimit_nextday_trail_capital_simulation.py

목적(사용자 질문): EarlyPop-Trail 백테스트는 종목당 1회·균등비중·자금재활용 없음.
  → 청산되어 현금이 확보돼도 그날 더는 투자하지 않고 유휴로 남는다.
  이 '유휴 자금' 구조를 정량화한다.

3가지 분석:
  ① 청산 시각 분포 + 평균 보유시간   → 자금이 평균 언제 풀리는가
  ② 동시보유 피크 / 실제 소요자금     → 하루 최대 몇 종목, 균등비중 시 종목당 비중, 자본 가동률
  ③ 자금회전 반영 일일 ROI            → 균등비중(1/N) 일일 포트 수익률·복리 자금곡선·MDD

기준: trail=-2%(권장), gap 0~3%(권장). --trail/--gap-min/--gap-max 로 변경 가능.
데이터/유니버스/매수·청산 로직은 Query_str_uplimit_nextday_trail_simulation.py 와 동일.
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
SESSION_OPEN = 9 * 60        # 09:00 (분)
SESSION_CLOSE = 15 * 60 + 30  # 15:30 (분) — 동시호가 포함 세션 길이 산정용


def t2min(t):
    """'HHMMSS' → 자정 기준 분."""
    t = str(t).zfill(6)
    return int(t[:2]) * 60 + int(t[2:4])


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
    """09:00 종가 매수 + 고점대비 -trail 트레일링.
    반환 (pnl%, reason, gap, exit_min, hold_min). 미적격 시 None."""
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
            em = t2min(t)
            return ((stop_px / buy - 1) * 100, "트레일청산", gap, em, em - SESSION_OPEN)
        if c > peak:
            peak = c
    em = t2min(seq[-1][0])
    return ((seq[-1][3] / buy - 1) * 100, "마감", gap, em, em - SESSION_OPEN)


def pct(n, d):
    return f"{n/d*100:5.1f}%" if d else "  0.0%"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="all")
    ap.add_argument("--csv", default=CSV_PATH)
    ap.add_argument("--trail", type=float, default=0.02, help="트레일링 폭(기본 0.02=-2%)")
    ap.add_argument("--gap-min", type=float, default=0.0)
    ap.add_argument("--gap-max", type=float, default=0.03)
    ap.add_argument("--strong-only", action="store_true")
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

    trail = args.trail
    print("EarlyPop-Trail 자본효율/유휴자금 분석")
    print(f"trail=-{trail*100:.0f}% | gap [{args.gap_min:.0%}~{args.gap_max:.0%}) | "
          f"매매일 {len(trade_days)}일{' | 강한상한가만' if args.strong_only else ''}")
    print("=" * 92)

    # 일자별 적격 거래 수집: per day → list of dicts
    byday = {}  # td → [ {pnl, reason, exit_min, hold_min, gap} ]
    for td in trade_days:
        by = load_day(con, td, set(trade_codes[td].keys()))
        recs = []
        for code, seq in by.items():
            strong = trade_codes[td].get(code, False)
            if args.strong_only and not strong:
                continue
            r = simulate(seq, trail)
            if not r:
                continue
            pnl, reason, gap, exit_min, hold_min = r
            if gap < args.gap_min or gap >= args.gap_max:
                continue
            recs.append(dict(pnl=pnl, reason=reason, exit_min=exit_min,
                             hold_min=max(hold_min, 0), gap=gap))
        if recs:
            byday[td] = recs

    all_recs = [r for recs in byday.values() for r in recs]
    n = len(all_recs)
    if n == 0:
        print("적격 거래 없음")
        return
    print(f"총 적격 거래 {n}건 / {len(byday)}일\n")

    # ───────────────────────────────────────────────────────────────
    # ① 청산 시각 분포 + 평균 보유시간
    # ───────────────────────────────────────────────────────────────
    print("① 청산 시각 분포 + 평균 보유시간 (자금이 언제 풀리는가)")
    print("-" * 92)
    buckets = [("09:01-02", 1, 3), ("09:03-05", 3, 6), ("09:06-10", 6, 11),
               ("09:11-30", 11, 31), ("09:31-10:30", 31, 91),
               ("10:31-14:59", 91, 360), ("마감(15:00~)", 360, 10 ** 9)]
    print(f"  {'청산구간':<14}{'건수':>6}{'비중':>8}{'누적비중':>9}{'평균손익':>10}")
    cum = 0
    for label, a, b in buckets:
        sub = [r for r in all_recs if a <= r["hold_min"] < b]
        cum += len(sub)
        avg = f"{st.mean([r['pnl'] for r in sub]):+.2f}%" if sub else "   -"
        print(f"  {label:<14}{len(sub):>6}{pct(len(sub),n):>8}{pct(cum,n):>9}{avg:>10}")
    holds = [r["hold_min"] for r in all_recs]
    trailcnt = sum(1 for r in all_recs if r["reason"] == "트레일청산")
    print(f"\n  보유시간(분): 평균 {st.mean(holds):.1f} · 중앙 {st.median(holds):.0f} · "
          f"최소 {min(holds):.0f} · 최대 {max(holds):.0f}")
    print(f"  청산사유: 트레일청산 {pct(trailcnt,n)} ({trailcnt}건) · "
          f"마감 {pct(n-trailcnt,n)} ({n-trailcnt}건)")
    half = sum(1 for h in holds if h <= 10)
    print(f"  → 전체의 {pct(half,n)} 가 09:10 이전 청산. "
          f"자금 대부분이 개장 직후 회수되어 잔여 세션 유휴.\n")

    # ───────────────────────────────────────────────────────────────
    # ② 동시보유 피크 / 실제 소요자금 / 자본 가동률
    # ───────────────────────────────────────────────────────────────
    print("② 동시보유 피크 / 실제 소요자금 / 자본 가동률")
    print("-" * 92)
    ns = [len(recs) for recs in byday.values()]
    nmax = max(ns)
    print(f"  종목수/일: 평균 {st.mean(ns):.1f} · 중앙 {st.median(ns):.0f} · "
          f"최소 {min(ns)} · 최대 {nmax}")
    print(f"  · 모든 종목 09:00 동시 진입 → '동시보유 피크 = 그날 종목수'(개장 시점).")
    print(f"  · 균등비중 시 종목당 비중 = 1/N (그날 N). 필요 슬롯 최대 = {nmax}개.")
    # 자본 가동률: 균등비중·세션길이 대비 보유시간 적분
    session_len = SESSION_CLOSE - SESSION_OPEN  # 390
    # 일별 평균 가동률 = mean(hold)/session_len  (균등비중이므로 비중 상쇄)
    util_day = []
    for td, recs in byday.items():
        util_day.append(st.mean([min(r["hold_min"], session_len) for r in recs]) / session_len)
    print(f"\n  자본 가동률(세션 {session_len}분 대비 평균 점유): "
          f"일평균 {st.mean(util_day)*100:.1f}% · 중앙 {st.median(util_day)*100:.1f}%")
    print(f"  → 투입자본이 하루 평균 약 {st.mean(util_day)*100:.0f}%만 가동, "
          f"나머지 {100-st.mean(util_day)*100:.0f}%는 유휴.")
    print(f"  → 단, 가동 중에는 09:00 전액 투입(1/N×N) — '진입 시점' 자본은 미사용 없음.\n")

    # ───────────────────────────────────────────────────────────────
    # ③ 자금회전 반영 일일 ROI (균등비중 1/N) + 복리 자금곡선 + MDD
    # ───────────────────────────────────────────────────────────────
    print("③ 자금회전 반영 일일 ROI (균등비중 1/N) — '투입자본 대비 일수익률'")
    print("-" * 92)
    # 균등비중 → 일일 포트 수익률 = 그날 pnl% 평균
    day_ret = []  # (td, ret%)
    for td in sorted(byday):
        recs = byday[td]
        day_ret.append((td, st.mean([r["pnl"] for r in recs])))
    rets = [r for _, r in day_ret]
    pos = sum(1 for r in rets if r > 0)
    print(f"  일일 ROI(균등비중): 평균 {st.mean(rets):+.2f}% · 중앙 {st.median(rets):+.2f}% · "
          f"흑자일 {pos}/{len(rets)} ({pos/len(rets)*100:.0f}%)")
    print(f"  최악일 {min(rets):+.2f}% · 최고일 {max(rets):+.2f}% · 표준편차 {st.pstdev(rets):.2f}%p")
    print(f"  · (참고) 이 '일일 ROI 평균'이 기존 시뮬의 '일평균'과 동일 — "
          f"균등비중 전액투입의 일수익률임.")
    # 복리 자금곡선 + MDD
    eq = 1.0
    peak = 1.0
    mdd = 0.0
    curve = []
    for td, r in day_ret:
        eq *= (1 + r / 100)
        peak = max(peak, eq)
        mdd = max(mdd, (peak - eq) / peak)
        curve.append((td, eq))
    print(f"\n  복리 자금곡선(전액 재투입 가정): 최종 배수 ×{eq:,.1f} · "
          f"최대낙폭(MDD) {mdd*100:.1f}%")
    print(f"  ⚠ ×{eq:,.0f} 는 단일국면(57일)·전액복리 가정의 비현실 수치 — "
          f"OOS·슬리피지·동시보유 한도 미반영(SUMMARY ⚠ 참조).")
    # 자금곡선 요약: 5등분 지점
    if len(curve) >= 5:
        print("  자금곡선 경로:", " → ".join(
            f"{curve[i][0][5:]}×{curve[i][1]:,.1f}"
            for i in (0, len(curve)//4, len(curve)//2, 3*len(curve)//4, -1)))

    avg_exit = SESSION_OPEN + st.mean(holds)
    print("\n" + "=" * 92)
    print("요약: 자금은 개장 직후(평균 청산 %02d:%02d경) 대부분 회수되어 잔여 세션 유휴."
          % (int(avg_exit // 60), int(avg_exit % 60)))
    print("      기존 '평균 +10%대'는 '거래 1건당' 수익률. 균등비중 '일일 ROI'도 동일 평균이나,")
    print("      자본 가동률이 낮아 '같은 날 재회전' 여지가 큼(별도 시뮬로 검증 가능).")


if __name__ == "__main__":
    main()
