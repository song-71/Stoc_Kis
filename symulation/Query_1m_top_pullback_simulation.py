"""
상한가 터치 후 풀백 매수 전략 분석

전략:
  1) 1m 데이터에서 장중 상한가(high >= upper_limit)를 터치한 종목 탐색
  2) 상한가 터치 후 풀백 발생 여부 및 깊이 분석
  3) 각 풀백 레벨(-1%, -2%, -3%, -5%, -7%, -10%)에서 매수 시 익일 수익률 시뮬레이션
  4) 풀백 후 상한가 복귀 여부, 첫 터치 시각별 분석

사용법:
  python Query_1m_top_pullback_simulation.py --start 20260123 --end 20260220
"""

import argparse
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from math import floor

import duckdb
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from kis_utils import load_symbol_master, print_table

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
PARQUET_1D = DATA_DIR / "1d_data" / "kis_1d_unified_parquet_DB.parquet"
DIR_1M = DATA_DIR / "1m_data"

PULLBACK_LEVELS = [0.01, 0.02, 0.03, 0.05, 0.07, 0.10]
TRAIL_ACTIVATE = 0.05   # +5% 트레일 활성화
TRAIL_STEP = 0.01       # 1% 스텝
SELL_TIME = "151800"


def _tick_size(price: float) -> int:
    """호가 단위 반환 (KRX 기준)"""
    if price < 2000:
        return 1
    elif price < 5000:
        return 5
    elif price < 20000:
        return 10
    elif price < 50000:
        return 50
    elif price < 200000:
        return 100
    elif price < 500000:
        return 500
    else:
        return 1000


def _upper_limit_price(pdy_close: float) -> float:
    """상한가 계산: 전일종가 * 1.30을 호가단위로 내림"""
    raw = pdy_close * 1.30
    tick = _tick_size(raw)
    return floor(raw / tick) * tick


def _get_trading_dates(con: duckdb.DuckDBPyConnection, start: str, end: str) -> list[str]:
    """1d parquet에서 거래일 목록 조회 (YYYY-MM-DD 형식)"""
    df = con.execute(f"""
        SELECT DISTINCT date FROM read_parquet('{PARQUET_1D}')
        WHERE date >= '{start}' AND date <= '{end}'
        ORDER BY date
    """).df()
    return [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in df["date"]]


def _get_all_trading_dates(con: duckdb.DuckDBPyConnection) -> list[str]:
    """1d parquet 전체 거래일 목록"""
    df = con.execute(f"""
        SELECT DISTINCT date FROM read_parquet('{PARQUET_1D}')
        ORDER BY date
    """).df()
    return [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in df["date"]]


def _simulate_trailing_stop(
    con: duckdb.DuckDBPyConnection,
    next_date_yyyymmdd: str,
    buy_info: dict[str, float],
    stop_loss: float | None = None,
) -> dict[str, dict]:
    """
    다음 거래일 1m 데이터로 트레일링 스톱 시뮬레이션.
    buy_info: {code: buy_price}
    stop_loss: 손절 비율 (예: 0.10이면 -10%에서 손절). None이면 손절 없음.
    """
    parquet_path = DIR_1M / f"{next_date_yyyymmdd}_1m_chart_DB_parquet.parquet"
    if not parquet_path.exists():
        return {}

    codes = list(buy_info.keys())
    codes_sql = ",".join(f"'{c}'" for c in codes)
    df = con.execute(f"""
        SELECT code, time, open, high, low, close
        FROM read_parquet('{parquet_path}')
        WHERE code IN ({codes_sql})
        ORDER BY code, time
    """).df()

    if df.empty:
        return {}

    results = {}
    for code, grp in df.groupby("code"):
        code = str(code).zfill(6)
        buy_price = buy_info.get(code)
        if not buy_price or buy_price <= 0:
            continue

        grp = grp.sort_values("time").reset_index(drop=True)
        times = grp["time"].astype(str).str.replace(":", "").str.replace(".", "").str.strip().str.zfill(6).values
        highs = grp["high"].astype(float).values
        lows = grp["low"].astype(float).values
        closes = grp["close"].astype(float).values

        peak_price = 0.0
        trail_stop = 0.0
        trail_step_applied = 0.0
        stop_loss_price = buy_price * (1.0 - stop_loss) if stop_loss else 0.0
        sold = False
        stop_loss_hit = False
        sell_price = 0.0
        sell_time = ""
        close_1518_price = 0.0
        close_1518_idx = len(grp) - 1

        for i in range(len(grp)):
            t = times[i]
            h = highs[i]
            lo = lows[i]
            c = closes[i]

            if t == SELL_TIME:
                close_1518_price = c
                close_1518_idx = i

            if t > SELL_TIME:
                break

            if stop_loss_price > 0 and trail_stop == 0 and lo <= stop_loss_price:
                sell_price = stop_loss_price
                sell_time = t
                sold = True
                stop_loss_hit = True
                break

            if h > peak_price:
                peak_price = h

            peak_ret = (peak_price / buy_price) - 1.0

            if peak_ret >= TRAIL_ACTIVATE:
                new_step = int(peak_ret / TRAIL_STEP) * TRAIL_STEP
                if new_step > trail_step_applied:
                    trail_step_applied = new_step
                    trail_stop = buy_price * (1.0 + trail_step_applied)

            if trail_stop > 0 and lo <= trail_stop:
                sell_price = trail_stop
                sell_time = t
                sold = True
                break

        peak_ret_final = (peak_price / buy_price) - 1.0 if peak_price > 0 else 0.0

        if not sold:
            if close_1518_price > 0:
                sell_price = close_1518_price
                sell_time = SELL_TIME
            else:
                sell_price = closes[close_1518_idx]
                sell_time = times[close_1518_idx]

        results[code] = {
            "sell_price": sell_price,
            "sell_time": sell_time,
            "peak_ret": peak_ret_final,
            "trail_triggered": sold and not stop_loss_hit,
            "stop_loss_triggered": stop_loss_hit,
            "trail_stop": trail_stop,
        }

    return results


def _analyze_intraday_after_upper_touch(times, highs, lows, closes, upper_limit):
    """
    한 종목의 1m 봉 데이터에서 상한가 터치 후 행동 분석.

    Returns:
        dict with keys:
          - first_touch_idx: 첫 상한가 터치 봉 인덱스
          - first_touch_time: 첫 터치 시각
          - max_pullback_pct: 터치 이후 최대 풀백 깊이 (upper_limit 대비)
          - close_at_limit: 종가 == 상한가 여부
          - recovered_to_limit: 풀백 후 다시 상한가 도달 여부
          - pullback_buys: {level: buy_price} 각 풀백 레벨별 매수가
          - eod_close: 당일 마지막 봉 종가
        None if 상한가 터치 없음
    """
    n = len(times)
    if n == 0:
        return None

    # 1) 첫 상한가 터치 찾기
    first_touch_idx = -1
    for i in range(n):
        if highs[i] >= upper_limit:
            first_touch_idx = i
            break

    if first_touch_idx < 0:
        return None

    first_touch_time = times[first_touch_idx]

    # 2) 터치 이후 봉들에서 풀백 분석
    max_pullback_pct = 0.0
    pullback_started = False
    recovered_to_limit = False
    pullback_buys = {}  # {level: buy_price}

    eod_close = closes[-1]

    for i in range(first_touch_idx + 1, n):
        lo = lows[i]
        c = closes[i]
        h = highs[i]

        # 풀백 깊이 (상한가 대비 저가 기준)
        pb_pct = (upper_limit - lo) / upper_limit
        if pb_pct > max_pullback_pct:
            max_pullback_pct = pb_pct

        # 각 풀백 레벨별 매수가 기록 (close 기준, 보수적)
        close_pb_pct = (upper_limit - c) / upper_limit
        for level in PULLBACK_LEVELS:
            if level not in pullback_buys and close_pb_pct >= level:
                pullback_buys[level] = c

        # 풀백 발생 후 상한가 복귀 체크
        if pb_pct > 0.005:  # 0.5% 이상 빠져야 풀백으로 인정
            pullback_started = True
        if pullback_started and h >= upper_limit:
            recovered_to_limit = True

    close_at_limit = (eod_close >= upper_limit)

    return {
        "first_touch_idx": first_touch_idx,
        "first_touch_time": first_touch_time,
        "max_pullback_pct": max_pullback_pct,
        "close_at_limit": close_at_limit,
        "recovered_to_limit": recovered_to_limit,
        "pullback_buys": pullback_buys,
        "eod_close": eod_close,
    }


def _process_single_day(con: duckdb.DuckDBPyConnection, date_yyyymmdd: str) -> list[dict]:
    """
    하루 1m 데이터를 로드하여 상한가 터치 종목별 풀백 분석.
    Returns: list of record dicts
    """
    parquet_path = DIR_1M / f"{date_yyyymmdd}_1m_chart_DB_parquet.parquet"
    if not parquet_path.exists():
        return []

    df = con.execute(f"""
        SELECT code, time, open, high, low, close, pdy_close
        FROM read_parquet('{parquet_path}')
        WHERE time <= '152000'
        ORDER BY code, time
    """).df()

    if df.empty:
        return []

    # 종목명 매핑
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        code_to_name = dict(zip(sdf["code"], sdf["name"].astype(str)))
    except Exception:
        code_to_name = {}

    records = []
    for code, grp in df.groupby("code"):
        code = str(code).zfill(6)
        grp = grp.sort_values("time").reset_index(drop=True)

        # pdy_close 가져오기 (첫 봉의 값 사용)
        pdy_close = float(grp["pdy_close"].iloc[0])
        if pdy_close <= 0:
            continue

        upper_limit = _upper_limit_price(pdy_close)
        if upper_limit <= 0:
            continue

        times = grp["time"].astype(str).str.replace(":", "").str.replace(".", "").str.strip().str.zfill(6).values
        highs = grp["high"].astype(float).values
        lows = grp["low"].astype(float).values
        closes = grp["close"].astype(float).values

        result = _analyze_intraday_after_upper_touch(times, highs, lows, closes, upper_limit)
        if result is None:
            continue

        name = code_to_name.get(code, "")
        eod_close = result["eod_close"]

        rec = {
            "date": date_yyyymmdd,
            "code": code,
            "name": name,
            "pdy_close": pdy_close,
            "upper_limit": upper_limit,
            "first_touch_time": result["first_touch_time"],
            "max_pullback_pct": result["max_pullback_pct"],
            "close_at_limit": result["close_at_limit"],
            "recovered_to_limit": result["recovered_to_limit"],
            "eod_close": eod_close,
        }

        # 각 풀백 레벨별 매수가
        for level in PULLBACK_LEVELS:
            key = f"buy_{int(level*100)}pct"
            rec[key] = result["pullback_buys"].get(level, np.nan)

        # 종가 매수가 (상한가 마감이 아닌 경우만)
        rec["buy_close"] = eod_close if not result["close_at_limit"] else np.nan

        records.append(rec)

    return records


def main() -> None:
    start_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    print(f"[{start_ts}] 상한가 터치 후 풀백 매수 전략 분석 시작")

    parser = argparse.ArgumentParser(description="상한가 터치 후 풀백 매수 전략 분석")
    parser.add_argument("--start", required=True, help="시작일 (YYYYMMDD)")
    parser.add_argument("--end", required=True, help="종료일 (YYYYMMDD)")
    args = parser.parse_args()

    start_date = f"{args.start[:4]}-{args.start[4:6]}-{args.start[6:8]}"
    end_date = f"{args.end[:4]}-{args.end[4:6]}-{args.end[6:8]}"

    con = duckdb.connect()
    try:
        all_dates = _get_all_trading_dates(con)
        trading_dates = _get_trading_dates(con, start_date, end_date)

        if not trading_dates:
            print(f"해당 기간({start_date} ~ {end_date})에 거래일이 없습니다.")
            return

        print(f"기간: {start_date} ~ {end_date} (거래일 {len(trading_dates)}일)")

        # ======================================================
        # Phase 1: 장중 풀백 데이터 수집
        # ======================================================
        print("\n[Phase 1] 장중 풀백 데이터 수집 중...")
        all_records = []
        for date_str in trading_dates:
            date_yyyymmdd = date_str.replace("-", "")
            records = _process_single_day(con, date_yyyymmdd)
            if records:
                all_records.extend(records)
                print(f"  {date_str}: 상한가 터치 {len(records)}종목")

        if not all_records:
            print("해당 기간에 상한가 터치 종목이 없습니다.")
            return

        df_all = pd.DataFrame(all_records)
        total = len(df_all)
        print(f"\n총 상한가 터치: {total}건")

        # ======================================================
        # Phase 2: 풀백 통계 출력
        # ======================================================
        print(f"\n{'='*60}")
        print("  상한가 터치 후 행동 분석")
        print(f"{'='*60}")

        close_at_limit = df_all["close_at_limit"].sum()
        pullback_occurred = total - close_at_limit
        avg_pullback = df_all["max_pullback_pct"].mean() * 100

        print(f"  총 상한가 터치: {total}건")
        print(f"    상한가 마감: {int(close_at_limit)}건 ({close_at_limit/total*100:.1f}%)")
        print(f"    풀백 발생:   {int(pullback_occurred)}건 ({pullback_occurred/total*100:.1f}%)")
        print(f"    평균 풀백 깊이: {avg_pullback:.2f}%")

        # 풀백 깊이 분포
        bins = [-0.001, 0.005, 0.01, 0.02, 0.03, 0.05, 0.07, 0.10, 1.0]
        labels = ["0~0.5%", "0.5~1%", "1~2%", "2~3%", "3~5%", "5~7%", "7~10%", "10%+"]
        df_all["pb_bin"] = pd.cut(df_all["max_pullback_pct"], bins=bins, labels=labels, right=True)
        print("\n  풀백 깊이 분포:")
        for label in labels:
            cnt = (df_all["pb_bin"] == label).sum()
            print(f"    {label:>8s}: {cnt}건 ({cnt/total*100:.1f}%)")

        # ======================================================
        # Phase 3: 풀백 레벨별 익일 수익률 시뮬레이션
        # ======================================================
        print(f"\n{'='*60}")
        print(f"  풀백 레벨별 익일 수익률 시뮬레이션 (트레일 +{TRAIL_ACTIVATE*100:.0f}%)")
        print(f"{'='*60}")

        # 매수 유형 리스트: ("종가매수", buy_col), ("-1%", buy_col), ...
        buy_types = [("종가매수", "buy_close")]
        for level in PULLBACK_LEVELS:
            key = f"buy_{int(level*100)}pct"
            label = f"-{int(level*100)}%"
            buy_types.append((label, key))

        summary_rows = []

        for buy_label, buy_col in buy_types:
            # 매수 가능한 종목만 필터
            df_buy = df_all[df_all[buy_col].notna()].copy()
            if df_buy.empty:
                summary_rows.append({
                    "풀백레벨": buy_label,
                    "매수건수": 0, "승률": "-", "평균수익률": "-",
                    "트레일": 0, "손절": 0,
                })
                continue

            # 날짜별로 그룹핑하여 익일 시뮬 수행
            all_returns = []
            total_trail = 0
            total_sl = 0

            for date_yyyymmdd, grp in df_buy.groupby("date"):
                date_str = f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}"
                date_idx = all_dates.index(date_str) if date_str in all_dates else -1
                if date_idx < 0 or date_idx + 1 >= len(all_dates):
                    continue
                next_date_str = all_dates[date_idx + 1]
                next_date_yyyymmdd = next_date_str.replace("-", "")

                buy_info = {}
                for _, row in grp.iterrows():
                    code = str(row["code"]).zfill(6)
                    bp = float(row[buy_col])
                    if bp > 0:
                        buy_info[code] = bp

                if not buy_info:
                    continue

                trail_results = _simulate_trailing_stop(con, next_date_yyyymmdd, buy_info)
                for code, tr in trail_results.items():
                    bp = buy_info.get(code, 0)
                    sp = tr["sell_price"]
                    if bp > 0 and sp > 0:
                        ret = (sp / bp) - 1.0
                        all_returns.append(ret)
                        if tr["trail_triggered"]:
                            total_trail += 1
                        if tr["stop_loss_triggered"]:
                            total_sl += 1

            if all_returns:
                win_count = sum(1 for r in all_returns if r >= 0)
                win_rate = win_count / len(all_returns) * 100
                avg_ret = np.mean(all_returns) * 100
                summary_rows.append({
                    "풀백레벨": buy_label,
                    "매수건수": len(all_returns),
                    "승률": f"{win_rate:.1f}%",
                    "평균수익률": f"{avg_ret:+.2f}%",
                    "트레일": total_trail,
                    "손절": total_sl,
                })
            else:
                summary_rows.append({
                    "풀백레벨": buy_label,
                    "매수건수": 0, "승률": "-", "평균수익률": "-",
                    "트레일": 0, "손절": 0,
                })

        # Phase 4: 비교 테이블 출력
        print(f"\n===== 풀백 레벨별 익일 수익률 비교 (트레일 +{TRAIL_ACTIVATE*100:.0f}%) =====")
        columns = ["풀백레벨", "매수건수", "승률", "평균수익률", "트레일", "손절"]
        align = {c: "right" for c in columns}
        align["풀백레벨"] = "left"
        print_rows = []
        for r in summary_rows:
            print_rows.append({
                "풀백레벨": r["풀백레벨"],
                "매수건수": str(r["매수건수"]),
                "승률": str(r["승률"]),
                "평균수익률": str(r["평균수익률"]),
                "트레일": str(r["트레일"]),
                "손절": str(r["손절"]),
            })
        print_table(print_rows, columns, align)

        # ======================================================
        # Phase 5: 부가 분석
        # ======================================================
        print(f"\n{'='*60}")
        print("  부가 분석")
        print(f"{'='*60}")

        # 5-1) 상한가 복귀 여부별 익일 수익률 비교
        # 풀백이 발생한 종목 중 (max_pullback_pct > 0.5%)
        df_pb = df_all[df_all["max_pullback_pct"] > 0.005].copy()
        if not df_pb.empty and "buy_close" in df_pb.columns:
            print("\n  [5-1] 상한가 복귀 여부별 익일 수익률 (종가 매수 기준)")
            for recovered_flag, label in [(True, "풀백→복귀"), (False, "풀백→미복귀")]:
                subset = df_pb[
                    (df_pb["recovered_to_limit"] == recovered_flag) & df_pb["buy_close"].notna()
                ]
                if subset.empty:
                    print(f"    {label}: 해당 종목 없음")
                    continue

                rets = []
                for date_yyyymmdd, grp in subset.groupby("date"):
                    date_str = f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}"
                    date_idx = all_dates.index(date_str) if date_str in all_dates else -1
                    if date_idx < 0 or date_idx + 1 >= len(all_dates):
                        continue
                    next_date_str = all_dates[date_idx + 1]
                    next_date_yyyymmdd = next_date_str.replace("-", "")

                    buy_info = {}
                    for _, row in grp.iterrows():
                        code = str(row["code"]).zfill(6)
                        bp = float(row["buy_close"])
                        if bp > 0:
                            buy_info[code] = bp
                    if not buy_info:
                        continue

                    trail_results = _simulate_trailing_stop(con, next_date_yyyymmdd, buy_info)
                    for code, tr in trail_results.items():
                        bp = buy_info.get(code, 0)
                        sp = tr["sell_price"]
                        if bp > 0 and sp > 0:
                            rets.append((sp / bp) - 1.0)

                if rets:
                    win_rate = sum(1 for r in rets if r >= 0) / len(rets) * 100
                    avg_ret = np.mean(rets) * 100
                    print(f"    {label}: {len(rets)}건, 승률 {win_rate:.1f}%, 평균수익률 {avg_ret:+.2f}%")
                else:
                    print(f"    {label}: 익일 데이터 없음")

        # 5-2) 첫 터치 시각별 (오전/오후) 풀백 비율 및 익일 수익률
        print("\n  [5-2] 첫 터치 시각별 분석 (오전 vs 오후)")
        df_all["touch_period"] = df_all["first_touch_time"].apply(
            lambda t: "오전" if t < "120000" else "오후"
        )

        for period in ["오전", "오후"]:
            subset = df_all[df_all["touch_period"] == period]
            if subset.empty:
                print(f"    {period}: 해당 종목 없음")
                continue

            cnt = len(subset)
            pb_cnt = (subset["max_pullback_pct"] > 0.005).sum()
            close_limit_cnt = subset["close_at_limit"].sum()
            avg_pb = subset["max_pullback_pct"].mean() * 100

            print(f"\n    [{period}] 총 {cnt}건")
            print(f"      상한가 마감: {int(close_limit_cnt)}건 ({close_limit_cnt/cnt*100:.1f}%)")
            print(f"      풀백 발생:   {int(pb_cnt)}건 ({pb_cnt/cnt*100:.1f}%)")
            print(f"      평균 풀백 깊이: {avg_pb:.2f}%")

            # 종가 매수 익일 수익률
            sub_buy = subset[subset["buy_close"].notna()]
            if not sub_buy.empty:
                rets = []
                for date_yyyymmdd, grp in sub_buy.groupby("date"):
                    date_str = f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}"
                    date_idx = all_dates.index(date_str) if date_str in all_dates else -1
                    if date_idx < 0 or date_idx + 1 >= len(all_dates):
                        continue
                    next_date_str = all_dates[date_idx + 1]
                    next_date_yyyymmdd = next_date_str.replace("-", "")

                    buy_info = {}
                    for _, row in grp.iterrows():
                        code = str(row["code"]).zfill(6)
                        bp = float(row["buy_close"])
                        if bp > 0:
                            buy_info[code] = bp
                    if not buy_info:
                        continue

                    trail_results = _simulate_trailing_stop(con, next_date_yyyymmdd, buy_info)
                    for code, tr in trail_results.items():
                        bp = buy_info.get(code, 0)
                        sp = tr["sell_price"]
                        if bp > 0 and sp > 0:
                            rets.append((sp / bp) - 1.0)

                if rets:
                    win_rate = sum(1 for r in rets if r >= 0) / len(rets) * 100
                    avg_ret = np.mean(rets) * 100
                    print(f"      종가매수 익일: {len(rets)}건, 승률 {win_rate:.1f}%, 평균수익률 {avg_ret:+.2f}%")

        # ======================================================
        # Phase 6: CSV 저장
        # ======================================================
        print(f"\n{'='*60}")
        print("  CSV 저장")
        print(f"{'='*60}")

        # 상세 CSV
        detail_path = BASE_DIR / "top_pullback_detail.csv"
        df_all.to_csv(detail_path, index=False, encoding="utf-8-sig", float_format="%.6f")
        print(f"  [csv] 상세 저장: {detail_path.name} ({len(df_all)}건)")

        # 풀백 레벨별 익일 수익률 CSV
        returns_rows = []
        for buy_label, buy_col in buy_types:
            df_buy = df_all[df_all[buy_col].notna()].copy()
            if df_buy.empty:
                continue

            for date_yyyymmdd, grp in df_buy.groupby("date"):
                date_str = f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}"
                date_idx = all_dates.index(date_str) if date_str in all_dates else -1
                if date_idx < 0 or date_idx + 1 >= len(all_dates):
                    continue
                next_date_str = all_dates[date_idx + 1]
                next_date_yyyymmdd = next_date_str.replace("-", "")

                buy_info = {}
                code_rows = {}
                for _, row in grp.iterrows():
                    code = str(row["code"]).zfill(6)
                    bp = float(row[buy_col])
                    if bp > 0:
                        buy_info[code] = bp
                        code_rows[code] = row
                if not buy_info:
                    continue

                trail_results = _simulate_trailing_stop(con, next_date_yyyymmdd, buy_info)
                for code, tr in trail_results.items():
                    bp = buy_info.get(code, 0)
                    sp = tr["sell_price"]
                    if bp > 0 and sp > 0:
                        row = code_rows[code]
                        returns_rows.append({
                            "date": date_yyyymmdd,
                            "code": code,
                            "name": row.get("name", ""),
                            "pullback_level": buy_label,
                            "buy_price": bp,
                            "sell_price": sp,
                            "sell_time": tr["sell_time"],
                            "return": (sp / bp) - 1.0,
                            "trail_triggered": tr["trail_triggered"],
                            "stop_loss_triggered": tr["stop_loss_triggered"],
                        })

        if returns_rows:
            returns_path = BASE_DIR / "top_pullback_returns.csv"
            pd.DataFrame(returns_rows).to_csv(
                returns_path, index=False, encoding="utf-8-sig", float_format="%.6f"
            )
            print(f"  [csv] 수익률 저장: {returns_path.name} ({len(returns_rows)}건)")

    finally:
        con.close()

    end_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    elapsed = time.perf_counter() - t0
    print(f"\n[{end_ts}] 프로그램 종료 (총 소요시간: {elapsed:.3f}초)")


if __name__ == "__main__":
    main()
