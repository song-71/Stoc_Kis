"""
상승 VI 해제 후 갭 상승 매수 전략 시뮬레이션

전략:
  1) VI 발동현황 1분 스냅샷 CSV에서 상승 VI 발동 이벤트 추출
  2) 1m 차트에서 VI 동결(volume==0) 후 재개봉(첫 volume>0) 찾기
  3) 재개봉 open > 발동가 → 갭 상승 → 매수 (매수가 = 재개봉 open)
  4) 트레일링 스톱 시뮬레이션 (당일 매도)
  5) 조건별 성과 분석

사용법:
  python Query_vi_gap_up_simulation.py
"""

import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from kis_utils import print_table

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
VI_DIR = DATA_DIR / "vi_status" / "1m_fetch" / "backup"
DIR_1M = DATA_DIR / "1m_data"

# ── 분석 대상 기간 (YYMMDD) ──
DATE_FROM = "260201"   # 시작일 (None이면 전체)
DATE_TO   = "260215"   # 종료일 (None이면 전체)

SELL_TIME = "151800"
TRAIL_STEP = 0.01
STOP_LOSS = 0.05
TRAIL_ACTIVATES = [0.05, 0.10, 0.15, 0.20]

ETF_KEYWORDS = ["TIGER", "KODEX", "RISE", "ETN", "ETF", "PLUS", "KOSEF", "KBSTAR",
                 "HANARO", "SOL", "ACE", "ARIRANG", "BNK", "TIMEFOLIO", "KINDEX"]


def _is_etf_etn(code: str, name: str) -> bool:
    if code.startswith("Q"):
        return True
    name_upper = name.upper()
    return any(kw in name_upper for kw in ETF_KEYWORDS)


def _extract_vi_events() -> pd.DataFrame:
    """VI CSV 스냅샷에서 상승 VI 발동 이벤트 추출 (중복 제거)"""
    csv_files = sorted(VI_DIR.glob("vi_status_*.csv"))
    if not csv_files:
        print("VI CSV 파일이 없습니다.")
        return pd.DataFrame()

    # 기간 필터링
    if DATE_FROM or DATE_TO:
        def _in_range(f):
            d = f.stem.split("_")[2]
            if DATE_FROM and d < DATE_FROM:
                return False
            if DATE_TO and d > DATE_TO:
                return False
            return True
        csv_files = [f for f in csv_files if _in_range(f)]
        if not csv_files:
            print(f"기간 {DATE_FROM}~{DATE_TO}에 해당하는 VI CSV 파일이 없습니다.")
            return pd.DataFrame()

    # 날짜 추출
    dates = sorted(set(f.stem.split("_")[2] for f in csv_files))
    print(f"VI 데이터 날짜: {dates} ({len(csv_files)}개 파일)")

    all_rows = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, encoding="utf-8-sig", dtype=str)
        except Exception:
            continue

        if df.empty:
            continue

        # 상승 VI 필터: 상태==발동중, 괴리율>0
        df = df[df["상태"] == "발동중"].copy()
        if df.empty:
            continue
        df["괴리율_f"] = pd.to_numeric(df["괴리율"], errors="coerce")
        df = df[df["괴리율_f"] > 0].copy()
        if df.empty:
            continue

        # ETF/ETN 제외
        df = df[~df.apply(lambda r: _is_etf_etn(str(r["종목코드"]), str(r["종목명"])), axis=1)]
        if df.empty:
            continue

        # 날짜 추출 from filename
        date_str = f.stem.split("_")[2]  # YYMMDD
        df["date"] = date_str
        all_rows.append(df)

    if not all_rows:
        return pd.DataFrame()

    combined = pd.concat(all_rows, ignore_index=True)
    # 중복 제거: (종목코드, 날짜, 발동시간)
    combined["종목코드"] = combined["종목코드"].astype(str).str.zfill(6)
    combined["발동시간"] = combined["발동시간"].astype(str).str.zfill(6)
    combined = combined.drop_duplicates(subset=["종목코드", "date", "발동시간"]).copy()

    combined["발동가_f"] = pd.to_numeric(combined["발동가"], errors="coerce")
    combined["횟수_i"] = pd.to_numeric(combined["횟수"], errors="coerce").fillna(1).astype(int)
    # VI 종류 보존 (동적VI/정적VI/정적&동적)
    if "종류" in combined.columns:
        combined["vi_type"] = combined["종류"].astype(str).str.strip()
    else:
        combined["vi_type"] = "미분류"

    print(f"상승 VI 발동 이벤트: {len(combined)}건 (ETF/ETN 제외)")
    return combined


def _find_gap_up_entry(con, date_yyyymmdd: str, code: str, vi_trigger_time: str, vi_trigger_price: float):
    """
    1m 데이터에서 VI 동결 후 재개봉을 찾아 갭 상승 판정.
    Returns: (buy_price, buy_time, acml_vol_before_vi) or None
    """
    parquet_path = DIR_1M / f"20{date_yyyymmdd}_1m_chart_DB_parquet.parquet"
    if not parquet_path.exists():
        return None

    df = con.execute(f"""
        SELECT time, open, high, low, close, volume, acml_volume
        FROM read_parquet('{parquet_path}')
        WHERE code = '{code}'
        ORDER BY time
    """).df()

    if df.empty:
        return None

    df["time"] = df["time"].astype(str).str.replace(":", "").str.replace(".", "").str.strip().str.zfill(6)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
    # acml_volume이 빈 문자열인 경우 volume 누적으로 계산
    df["acml_volume"] = pd.to_numeric(df["acml_volume"], errors="coerce")
    if df["acml_volume"].isna().all():
        df["acml_volume"] = df["volume"].cumsum()
    else:
        df["acml_volume"] = df["acml_volume"].fillna(0)

    # 발동시간 이상의 봉에서 volume==0 찾기
    vi_time = vi_trigger_time.zfill(6)

    # 발동 직전 누적거래량 (발동시간 이전 마지막 봉)
    before_vi = df[df["time"] < vi_time]
    acml_vol_before = 0
    if not before_vi.empty:
        acml_vol_before = int(before_vi.iloc[-1]["acml_volume"])

    # 발동시간 포함 이후 봉
    after_vi = df[df["time"] >= vi_time].reset_index(drop=True)
    if after_vi.empty:
        return None

    # 동결 확인: volume==0인 봉 찾기
    found_freeze = False
    resume_idx = None
    for i in range(len(after_vi)):
        if after_vi.iloc[i]["volume"] == 0:
            found_freeze = True
            continue
        if found_freeze:
            resume_idx = i
            break

    if not found_freeze or resume_idx is None:
        # 첫 봉 자체가 volume>0이면 동결 없음 → 발동시간 봉이 volume==0인지 확인
        # 발동시간 봉이 volume>0이면 이미 재개된 것이므로 skip
        return None

    resume_row = after_vi.iloc[resume_idx]
    resume_open = float(resume_row["open"])
    resume_time = resume_row["time"]

    # 갭 상승 판정
    if resume_open > vi_trigger_price:
        gap_pct = (resume_open / vi_trigger_price - 1.0) * 100
        return (resume_open, resume_time, acml_vol_before, gap_pct)

    return None


def _simulate_trailing_stop_intraday(
    con, date_yyyymmdd: str, code: str, buy_price: float, buy_time: str,
    trail_activate: float,
) -> dict:
    """당일 내 트레일링 스톱 시뮬레이션"""
    parquet_path = DIR_1M / f"20{date_yyyymmdd}_1m_chart_DB_parquet.parquet"
    if not parquet_path.exists():
        return None

    df = con.execute(f"""
        SELECT time, open, high, low, close
        FROM read_parquet('{parquet_path}')
        WHERE code = '{code}'
        ORDER BY time
    """).df()

    if df.empty:
        return None

    df["time"] = df["time"].astype(str).str.replace(":", "").str.replace(".", "").str.strip().str.zfill(6)

    # 매수 시점 이후 봉만
    df = df[df["time"] >= buy_time].reset_index(drop=True)
    if df.empty:
        return None

    peak_price = 0.0
    trail_stop = 0.0
    trail_step_applied = 0.0
    stop_loss_price = buy_price * (1.0 - STOP_LOSS)
    sold = False
    stop_loss_hit = False
    trail_triggered = False
    sell_price = 0.0
    sell_time = ""
    close_1518 = 0.0

    for i in range(len(df)):
        t = df.iloc[i]["time"]
        h = float(df.iloc[i]["high"])
        lo = float(df.iloc[i]["low"])
        c = float(df.iloc[i]["close"])

        if t == SELL_TIME:
            close_1518 = c
        if t > SELL_TIME:
            break

        # 스톱로스 (트레일 미활성 시)
        if trail_stop == 0 and lo <= stop_loss_price:
            sell_price = stop_loss_price
            sell_time = t
            sold = True
            stop_loss_hit = True
            break

        if h > peak_price:
            peak_price = h

        peak_ret = (peak_price / buy_price) - 1.0

        if peak_ret >= trail_activate:
            new_step = int(peak_ret / TRAIL_STEP) * TRAIL_STEP
            if new_step > trail_step_applied:
                trail_step_applied = new_step
                trail_stop = buy_price * (1.0 + trail_step_applied)

        if trail_stop > 0 and lo <= trail_stop:
            sell_price = trail_stop
            sell_time = t
            sold = True
            trail_triggered = True
            break

    peak_ret_final = (peak_price / buy_price) - 1.0 if peak_price > 0 else 0.0

    if not sold:
        if close_1518 > 0:
            sell_price = close_1518
            sell_time = SELL_TIME
        elif len(df) > 0:
            sell_price = float(df.iloc[-1]["close"])
            sell_time = df.iloc[-1]["time"]
        else:
            return None

    if sell_price <= 0:
        return None

    return {
        "sell_price": sell_price,
        "sell_time": sell_time,
        "peak_ret": peak_ret_final,
        "trail_triggered": trail_triggered,
        "stop_loss_triggered": stop_loss_hit,
        "return": (sell_price / buy_price) - 1.0,
    }


def _print_summary_table(title: str, rows: list[dict], extra_cols=None):
    """조건별 요약 테이블 출력"""
    if not rows:
        print(f"\n  {title}: 데이터 없음")
        return

    cols = ["구간", "매수건수", "승률", "평균수익률", "중앙수익률"]
    if extra_cols:
        cols.extend(extra_cols)
    align = {c: "right" for c in cols}
    align["구간"] = "left"
    print(f"\n  {title}")
    print_table(rows, cols, align)


def main():
    start_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    print(f"[{start_ts}] 상승 VI 해제 후 갭 상승 매수 시뮬레이션 시작\n")

    # Step 1: VI 이벤트 추출
    vi_events = _extract_vi_events()
    if vi_events.empty:
        print("VI 이벤트가 없습니다.")
        return

    con = duckdb.connect()
    try:
        # Step 2: 갭 상승 매수 판정
        entries = []
        skip_no_1m = 0
        skip_no_gap = 0
        skip_no_freeze = 0

        for _, ev in vi_events.iterrows():
            code = str(ev["종목코드"]).zfill(6)
            date = str(ev["date"])
            vi_time = str(ev["발동시간"]).zfill(6)
            vi_price = float(ev["발동가_f"])
            gap_rate = float(ev["괴리율_f"])
            vi_count = int(ev["횟수_i"])
            name = str(ev["종목명"])

            result = _find_gap_up_entry(con, date, code, vi_time, vi_price)
            if result is None:
                skip_no_freeze += 1
                continue

            buy_price, buy_time, acml_vol_before, gap_pct = result
            hour = int(vi_time[:2])

            entries.append({
                "date": date,
                "code": code,
                "name": name,
                "vi_time": vi_time,
                "vi_price": vi_price,
                "gap_rate": gap_rate,
                "vi_count": vi_count,
                "vi_type": str(ev["vi_type"]),
                "buy_price": buy_price,
                "buy_time": buy_time,
                "acml_vol_before": acml_vol_before,
                "gap_pct": gap_pct,
                "hour": hour,
            })

        print(f"\n갭 상승 매수 대상: {len(entries)}건 (동결/재개 미확인: {skip_no_freeze}건)")

        if not entries:
            print("매수 대상이 없습니다.")
            return

        # Step 2-1: 거래량 분위수 계산
        acml_vols = [e["acml_vol_before"] for e in entries if e["acml_vol_before"] > 0]
        if acml_vols:
            q25 = np.percentile(acml_vols, 25)
            q75 = np.percentile(acml_vols, 75)
        else:
            q25, q75 = 0, 0

        for e in entries:
            v = e["acml_vol_before"]
            if v <= q25:
                e["vol_group"] = "하위25%"
            elif v <= q75:
                e["vol_group"] = "중위50%"
            else:
                e["vol_group"] = "상위25%"

        # Step 3: 트레일링 스톱 시뮬레이션
        all_detail_rows = []  # (trail_activate, detail_dict)

        for ta in TRAIL_ACTIVATES:
            detail_rows = []
            for e in entries:
                res = _simulate_trailing_stop_intraday(
                    con, e["date"], e["code"], e["buy_price"], e["buy_time"], ta
                )
                if res is None:
                    continue

                detail_rows.append({
                    **e,
                    "trail_activate": ta,
                    "sell_price": res["sell_price"],
                    "sell_time": res["sell_time"],
                    "peak_ret": res["peak_ret"],
                    "trail_triggered": res["trail_triggered"],
                    "stop_loss_triggered": res["stop_loss_triggered"],
                    "return": res["return"],
                })

            all_detail_rows.append((ta, detail_rows))

        # Step 4: 성과 출력

        # 4-1) 트레일 활성화 비율별 비교
        print(f"\n{'='*70}")
        print(f"  트레일링 스톱 활성화 비율별 비교 (손절: -{STOP_LOSS*100:.0f}%)")
        print(f"{'='*70}")

        trail_summary_rows = []
        for ta, rows in all_detail_rows:
            if not rows:
                continue
            rets = [r["return"] for r in rows]
            wins = sum(1 for r in rets if r >= 0)
            trail_cnt = sum(1 for r in rows if r["trail_triggered"])
            sl_cnt = sum(1 for r in rows if r["stop_loss_triggered"])
            trail_summary_rows.append({
                "구간": f"+{ta*100:.0f}%",
                "매수건수": str(len(rets)),
                "승률": f"{wins/len(rets)*100:.1f}%",
                "평균수익률": f"{np.mean(rets)*100:+.2f}%",
                "중앙수익률": f"{np.median(rets)*100:+.2f}%",
                "트레일발동": str(trail_cnt),
                "손절": str(sl_cnt),
            })

        cols = ["구간", "매수건수", "승률", "평균수익률", "중앙수익률", "트레일발동", "손절"]
        align = {c: "right" for c in cols}
        align["구간"] = "left"
        print_table(trail_summary_rows, cols, align)

        # 4-2) 종목 선별 기준 분석 (트레일 +5% 기준)
        # 가장 첫 번째 트레일 결과 사용
        base_ta, base_rows = all_detail_rows[0]
        if not base_rows:
            print("분석할 데이터가 없습니다.")
            return

        base_df = pd.DataFrame(base_rows)

        print(f"\n{'='*70}")
        print(f"  종목 선별 기준 분석 (트레일 +{base_ta*100:.0f}%, 손절 -{STOP_LOSS*100:.0f}%)")
        print(f"{'='*70}")

        # 괴리율 구간별
        gap_bins = [(6, 8), (8, 10), (10, 15), (15, 100)]
        gap_rows = []
        for lo, hi in gap_bins:
            sub = base_df[(base_df["gap_rate"] >= lo) & (base_df["gap_rate"] < hi)]
            if sub.empty:
                continue
            rets = sub["return"]
            wins = (rets >= 0).sum()
            gap_rows.append({
                "구간": f"{lo}~{hi}%",
                "매수건수": str(len(sub)),
                "승률": f"{wins/len(sub)*100:.1f}%",
                "평균수익률": f"{rets.mean()*100:+.2f}%",
                "중앙수익률": f"{rets.median()*100:+.2f}%",
            })
        _print_summary_table("괴리율 구간별 성과", gap_rows)

        # VI 횟수별
        count_bins = [(1, 1, "1회"), (2, 2, "2회"), (3, 99, "3회+")]
        count_rows = []
        for lo, hi, label in count_bins:
            sub = base_df[(base_df["vi_count"] >= lo) & (base_df["vi_count"] <= hi)]
            if sub.empty:
                continue
            rets = sub["return"]
            wins = (rets >= 0).sum()
            count_rows.append({
                "구간": label,
                "매수건수": str(len(sub)),
                "승률": f"{wins/len(sub)*100:.1f}%",
                "평균수익률": f"{rets.mean()*100:+.2f}%",
                "중앙수익률": f"{rets.median()*100:+.2f}%",
            })
        _print_summary_table("VI 횟수별 성과", count_rows)

        # 시간대별
        hour_bins = [(9, "09시"), (10, "10시"), (11, "11시"), (12, "12시+")]
        hour_rows = []
        for h_val, label in hour_bins:
            if label == "12시+":
                sub = base_df[base_df["hour"] >= 12]
            else:
                sub = base_df[base_df["hour"] == h_val]
            if sub.empty:
                continue
            rets = sub["return"]
            wins = (rets >= 0).sum()
            hour_rows.append({
                "구간": label,
                "매수건수": str(len(sub)),
                "승률": f"{wins/len(sub)*100:.1f}%",
                "평균수익률": f"{rets.mean()*100:+.2f}%",
                "중앙수익률": f"{rets.median()*100:+.2f}%",
            })
        _print_summary_table("시간대별 성과", hour_rows)

        # 발동 전 누적거래량별
        vol_order = ["하위25%", "중위50%", "상위25%"]
        vol_rows = []
        for vg in vol_order:
            sub = base_df[base_df["vol_group"] == vg]
            if sub.empty:
                continue
            rets = sub["return"]
            wins = (rets >= 0).sum()
            vol_rows.append({
                "구간": vg,
                "매수건수": str(len(sub)),
                "승률": f"{wins/len(sub)*100:.1f}%",
                "평균수익률": f"{rets.mean()*100:+.2f}%",
                "중앙수익률": f"{rets.median()*100:+.2f}%",
            })
        _print_summary_table("발동 전 누적거래량별 성과", vol_rows)

        # 갭 크기별
        gap_size_bins = [(0, 1), (1, 3), (3, 5), (5, 100)]
        gap_size_rows = []
        for lo, hi in gap_size_bins:
            sub = base_df[(base_df["gap_pct"] >= lo) & (base_df["gap_pct"] < hi)]
            if sub.empty:
                continue
            rets = sub["return"]
            wins = (rets >= 0).sum()
            label = f"{lo}~{hi}%" if hi < 100 else f"{lo}%+"
            gap_size_rows.append({
                "구간": label,
                "매수건수": str(len(sub)),
                "승률": f"{wins/len(sub)*100:.1f}%",
                "평균수익률": f"{rets.mean()*100:+.2f}%",
                "중앙수익률": f"{rets.median()*100:+.2f}%",
            })
        _print_summary_table("갭 크기별 성과 (재개봉 open vs 발동가)", gap_size_rows)

        # VI 종류별
        vi_type_rows = []
        for vt in sorted(base_df["vi_type"].unique()):
            sub = base_df[base_df["vi_type"] == vt]
            if sub.empty:
                continue
            rets = sub["return"]
            wins = (rets >= 0).sum()
            vi_type_rows.append({
                "구간": vt,
                "매수건수": str(len(sub)),
                "승률": f"{wins/len(sub)*100:.1f}%",
                "평균수익률": f"{rets.mean()*100:+.2f}%",
                "중앙수익률": f"{rets.median()*100:+.2f}%",
            })
        _print_summary_table("VI 종류별 성과 (동적VI/정적VI/정적&동적)", vi_type_rows)

        # Step 5: CSV 저장
        detail_all = pd.DataFrame([r for _, rows in all_detail_rows for r in rows])
        detail_path = BASE_DIR / "vi_gap_up_detail.csv"
        detail_all.to_csv(detail_path, index=False, encoding="utf-8-sig", float_format="%.6f")
        print(f"\n[csv] 상세 저장: {detail_path.name} ({len(detail_all)}건)")

        # 요약 CSV
        summary_rows = []
        for ta, rows in all_detail_rows:
            if not rows:
                continue
            rets = [r["return"] for r in rows]
            wins = sum(1 for r in rets if r >= 0)
            trail_cnt = sum(1 for r in rows if r["trail_triggered"])
            sl_cnt = sum(1 for r in rows if r["stop_loss_triggered"])
            summary_rows.append({
                "trail_activate": f"+{ta*100:.0f}%",
                "count": len(rets),
                "win_rate": wins / len(rets),
                "avg_return": np.mean(rets),
                "median_return": np.median(rets),
                "trail_triggered": trail_cnt,
                "stop_loss_triggered": sl_cnt,
            })
        summary_path = BASE_DIR / "vi_gap_up_summary.csv"
        pd.DataFrame(summary_rows).to_csv(summary_path, index=False, encoding="utf-8-sig", float_format="%.6f")
        print(f"[csv] 요약 저장: {summary_path.name}")

    finally:
        con.close()

    elapsed = time.perf_counter() - t0
    end_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{end_ts}] 완료 (소요시간: {elapsed:.1f}초)")


if __name__ == "__main__":
    main()
