"""
상한가 도달 종목 익일 매도 시뮬레이션 (트레일링 스톱 포함)
Query_1m_Closing_buy_simulation.py

전략:
  1) 1d 데이터에서 당일 최고가가 상한가(전일종가 * 1.30, 호가단위 적용)에 도달한 종목 선정
  2) 해당 종목을 당일 종가(close)에 매수
  3) 다음 거래일 1m 데이터로 트레일링 스톱 시뮬레이션:
     - 최고가가 매수가 대비 +5% 이상 → 트레일링 스톱 활성화 (buy_price * 1.05)
     - 이후 1%씩 상향 (6%, 7%, ... 도달 시 트레일 스톱도 상향)
     - 저가가 트레일 스톱 이하 → 트레일 스톱 가격에 매도
     - 15:18까지 미발동 → 15:18 종가에 매도
  4) 수익률 계산 후 일자별/월별 집계 CSV 저장

사용법:
  python Query_1m_top_reach_simulation.py --start 20250103 --end 20250131
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
from kis_utils import load_symbol_master, print_table, load_kis_data_layout, _configure_duckdb_s3

# ── 기본 옵션 ──
DEFAULT_START = "20260201"
DEFAULT_END   = "20260228"

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
PARQUET_1D = DATA_DIR / "1d_data" / "kis_1d_unified_parquet_DB.parquet"

SELL_TIME = "151800"  # 매도 시각 (15:18)
TRAIL_ACTIVATE = 0.05  # 트레일링 스톱 활성화 기준 (+5%)
TRAIL_STEP = 0.01  # 트레일링 스톱 상향 단위 (1%)
CLOSE_GAP_THRESHOLDS = [0.00, 0.03, 0.05, 0.10, 0.15]  # 종가-상한가 괴리율 임계값 (0%=상한가 마감)
STOP_LOSS_LEVELS = [None, 0.05, 0.10]  # 스톱로스 수준 (None=없음, 0.05=-5%, 0.10=-10%)


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


def _find_upper_limit_stocks(con: duckdb.DuckDBPyConnection, date_str: str) -> pd.DataFrame:
    """특정 날짜에 최고가가 상한가에 도달한 종목 조회"""
    df = con.execute(f"""
        SELECT symbol, name, pdy_close, open, high, low, close
        FROM read_parquet('{PARQUET_1D}')
        WHERE date = '{date_str}'
          AND pdy_close > 0
          AND high > 0
          AND close > 0
    """).df()

    if df.empty:
        return pd.DataFrame()

    # 종목명 보완 (symbol_master 활용)
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        code_to_name = dict(zip(sdf["code"], sdf["name"].astype(str)))
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)
        df["name"] = df["symbol"].map(code_to_name).fillna(df["name"])
    except Exception:
        pass

    # 상한가 계산 및 필터
    df["upper_limit"] = df["pdy_close"].apply(_upper_limit_price)
    df = df[df["high"] >= df["upper_limit"]].copy()

    # 종가-상한가 괴리율 추가
    if not df.empty:
        df["close_gap"] = (df["upper_limit"] - df["close"]) / df["upper_limit"]
        df["close_gap"] = df["close_gap"].clip(lower=0.0)  # 종가 > 상한가 경우 0 처리

    return df


def _simulate_trailing_stop(
    con: duckdb.DuckDBPyConnection,
    layout,
    next_date_str: str,
    buy_info: dict[str, float],
    stop_loss: float | None = None,
) -> dict[str, dict]:
    """
    다음 거래일 1m 데이터(S3)로 트레일링 스톱 시뮬레이션.
    next_date_str: YYYY-MM-DD 형식.
    buy_info: {code: buy_price}
    stop_loss: 손절 비율 (예: 0.10이면 -10%에서 손절). None이면 손절 없음.
    반환: {code: {"sell_price": float, "sell_time": str, "peak_ret": float, "trail_triggered": bool, "stop_loss_triggered": bool}}
    """
    codes = list(buy_info.keys())
    codes_sql = ",".join(f"'{c}'" for c in codes)
    try:
        df = con.execute(f"""
            SELECT code, time, open, high, low, close
            FROM read_parquet('{layout.s3_1m_date(next_date_str)}')
            WHERE code IN ({codes_sql})
            ORDER BY code, time
        """).df()
    except Exception as e:
        print(f"  [warn] S3 1m 조회 실패 ({next_date_str}): {e}")
        return {}

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
        close_1518_idx = len(grp) - 1  # fallback: 마지막 봉

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

            # 스톱로스 체크 (트레일 활성화 전에만)
            if stop_loss_price > 0 and trail_stop == 0 and lo <= stop_loss_price:
                sell_price = stop_loss_price
                sell_time = t
                sold = True
                stop_loss_hit = True
                break

            # 최고가 갱신
            if h > peak_price:
                peak_price = h

            peak_ret = (peak_price / buy_price) - 1.0

            # 트레일링 스톱 계산: 5%부터 1%씩 상향
            if peak_ret >= TRAIL_ACTIVATE:
                # floor to nearest TRAIL_STEP
                new_step = int(peak_ret / TRAIL_STEP) * TRAIL_STEP
                if new_step > trail_step_applied:
                    trail_step_applied = new_step
                    trail_stop = buy_price * (1.0 + trail_step_applied)

            # 트레일 스톱 이탈 체크
            if trail_stop > 0 and lo <= trail_stop:
                sell_price = trail_stop
                sell_time = t
                sold = True
                break

        peak_ret_final = (peak_price / buy_price) - 1.0 if peak_price > 0 else 0.0

        if not sold:
            # 15:18 종가에 매도
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


def main() -> None:
    start_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    print(f"[{start_ts}] 상한가 도달 종목 익일 매도 시뮬레이션 시작")

    parser = argparse.ArgumentParser(description="상한가 도달 종목 익일 매도 시뮬레이션")
    parser.add_argument("--start", default=DEFAULT_START, help="시작일 (YYYYMMDD)")
    parser.add_argument("--end", default=DEFAULT_END, help="종료일 (YYYYMMDD)")
    args = parser.parse_args()

    start_date = f"{args.start[:4]}-{args.start[4:6]}-{args.start[6:8]}"
    end_date = f"{args.end[:4]}-{args.end[4:6]}-{args.end[6:8]}"

    layout = load_kis_data_layout()
    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con)
        all_dates = _get_all_trading_dates(con)
        trading_dates = _get_trading_dates(con, start_date, end_date)

        if not trading_dates:
            print(f"해당 기간({start_date} ~ {end_date})에 거래일이 없습니다.")
            return

        print(f"기간: {start_date} ~ {end_date} (거래일 {len(trading_dates)}일)")

        # === Phase 1: 상한가 도달 종목 + 다음 거래일 정보 수집 ===
        stock_data = []  # list of (date_str, next_date_str, buy_info, code_row_map)
        for date_str in trading_dates:
            ul_df = _find_upper_limit_stocks(con, date_str)
            if ul_df.empty:
                continue
            ul_df = ul_df[ul_df["close_gap"] <= max(CLOSE_GAP_THRESHOLDS)].copy()
            if ul_df.empty:
                continue

            date_idx = all_dates.index(date_str) if date_str in all_dates else -1
            if date_idx < 0 or date_idx + 1 >= len(all_dates):
                continue
            next_date_str = all_dates[date_idx + 1]

            buy_info = {}
            code_row_map = {}
            for _, row in ul_df.iterrows():
                code = str(row["symbol"]).zfill(6)
                bp = float(row["close"])
                if bp > 0:
                    buy_info[code] = bp
                    code_row_map[code] = row

            if buy_info:
                stock_data.append((date_str, next_date_str, buy_info, code_row_map))

        if not stock_data:
            print("해당 기간에 상한가 도달 종목이 없습니다.")
            return

        # === Phase 2: 스톱로스 레벨별 시뮬레이션 ===
        all_daily_rows = []
        all_monthly_rows = []
        all_summary_rows = []

        for sl in STOP_LOSS_LEVELS:
            sl_label = "스톱로스 없음" if sl is None else f"스톱로스 -{sl*100:.0f}%"
            print(f"\n{'='*60}")
            print(f"  [{sl_label}] 트레일 +{TRAIL_ACTIVATE*100:.0f}%")
            print(f"{'='*60}")

            detail_rows = []
            for date_str, next_date_str, buy_info, code_row_map in stock_data:
                trail_results = _simulate_trailing_stop(con, layout, next_date_str, buy_info, stop_loss=sl)
                if not trail_results:
                    continue

                for code, tr in trail_results.items():
                    row = code_row_map.get(code)
                    if row is None:
                        continue
                    buy_price = buy_info[code]
                    sell_price = tr["sell_price"]
                    if sell_price <= 0:
                        continue

                    ret = (sell_price / buy_price) - 1.0
                    detail_rows.append({
                        "buy_date": date_str,
                        "sell_date": next_date_str,
                        "code": code,
                        "name": str(row.get("name", "")),
                        "pdy_close": float(row["pdy_close"]),
                        "upper_limit": float(row["upper_limit"]),
                        "high": float(row["high"]),
                        "close_gap": float(row["close_gap"]),
                        "buy_price": buy_price,
                        "sell_price": sell_price,
                        "sell_time": tr["sell_time"],
                        "peak_ret": tr["peak_ret"],
                        "trail_triggered": tr["trail_triggered"],
                        "stop_loss_triggered": tr["stop_loss_triggered"],
                        "trail_stop": tr["trail_stop"],
                        "return": ret,
                    })

            if not detail_rows:
                print("  해당 기간에 시뮬레이션 결과가 없습니다.")
                continue

            detail_df = pd.DataFrame(detail_rows)

            # 상세 CSV
            sl_suffix = "no_sl" if sl is None else f"sl{int(sl*100):02d}"
            detail_path = BASE_DIR / f"top_reach_detail_{sl_suffix}.csv"
            detail_df.to_csv(detail_path, index=False, encoding="utf-8-sig", float_format="%.6f")
            print(f"  [csv] 상세 저장: {detail_path.name}")

            # === 괴리율별 집계 및 출력 ===
            for threshold in CLOSE_GAP_THRESHOLDS:
                pct = int(threshold * 100)
                filtered = detail_df[detail_df["close_gap"] <= threshold].copy()
                gap_label = "상한가 마감" if threshold == 0.0 else f"종가 괴리율 {pct}% 이내"

                if filtered.empty:
                    print(f"\n  ===== {gap_label} ===== → 해당 종목 없음")
                    continue

                # 일자별 집계
                daily_summary_rows = []
                for buy_date, grp in filtered.groupby("buy_date"):
                    rets = grp["return"].tolist()
                    win_count = sum(1 for r in rets if r >= 0)
                    loss_count = sum(1 for r in rets if r < 0)
                    trail_cnt = int(grp["trail_triggered"].sum())
                    sl_cnt = int(grp["stop_loss_triggered"].sum())
                    avg_ret = sum(rets) / len(rets)
                    sell_date = grp["sell_date"].iloc[0]
                    daily_summary_rows.append({
                        "date": buy_date,
                        "sell_date": sell_date,
                        "total_stocks": len(rets),
                        "win": win_count,
                        "loss": loss_count,
                        "trail_cnt": trail_cnt,
                        "sl_cnt": sl_cnt,
                        "avg_return": avg_ret,
                    })

                daily_df = pd.DataFrame(daily_summary_rows)
                # 일자별 집계를 통합 리스트에 수집
                for r in daily_summary_rows:
                    all_daily_rows.append({
                        "stop_loss": sl_label,
                        "gap_threshold": gap_label,
                        **r,
                    })

                # 월별 집계
                daily_df["month"] = daily_df["date"].str[:7]
                monthly_rows = []
                for month, mgrp in daily_df.groupby("month"):
                    total_stocks = mgrp["total_stocks"].sum()
                    total_win = mgrp["win"].sum()
                    total_loss = mgrp["loss"].sum()
                    weighted_ret = (mgrp["avg_return"] * mgrp["total_stocks"]).sum() / total_stocks if total_stocks > 0 else 0.0
                    monthly_rows.append({
                        "month": month,
                        "trading_days": len(mgrp),
                        "total_stocks": int(total_stocks),
                        "win": int(total_win),
                        "loss": int(total_loss),
                        "avg_return": weighted_ret,
                    })

                monthly_df = pd.DataFrame(monthly_rows)
                # 월별 집계를 통합 리스트에 수집
                for r in monthly_rows:
                    all_monthly_rows.append({
                        "stop_loss": sl_label,
                        "gap_threshold": gap_label,
                        **r,
                    })

                # 콘솔 일자별 출력
                print(f"\n  ===== {gap_label} ({sl_label}) =====")
                print_rows = []
                for r in daily_summary_rows:
                    row_data = {
                        "date": r["date"],
                        "sell_date": r["sell_date"],
                        "종목수": str(r["total_stocks"]),
                        "수익": str(r["win"]),
                        "손실": str(r["loss"]),
                        "트레일": str(r["trail_cnt"]),
                    }
                    if sl is not None:
                        row_data["손절"] = str(r["sl_cnt"])
                    row_data["평균수익률"] = f"{r['avg_return']:.4f}"
                    print_rows.append(row_data)

                # 전체 합계
                all_rets = filtered["return"].tolist()
                total_win = sum(1 for r in all_rets if r >= 0)
                total_loss = sum(1 for r in all_rets if r < 0)
                total_trail = int(filtered["trail_triggered"].sum())
                total_sl = int(filtered["stop_loss_triggered"].sum())
                overall_avg = sum(all_rets) / len(all_rets) if all_rets else 0.0
                win_rate = total_win / len(all_rets) * 100 if all_rets else 0.0
                all_summary_rows.append({
                    "stop_loss": sl_label,
                    "gap_threshold": gap_label,
                    "총종목수": len(all_rets),
                    "수익": total_win,
                    "손실": total_loss,
                    "트레일": total_trail,
                    "손절": total_sl,
                    "승률": win_rate,
                    "평균수익률": overall_avg,
                })
                total_row = {
                    "date": "TOTAL",
                    "sell_date": "",
                    "종목수": str(len(all_rets)),
                    "수익": str(total_win),
                    "손실": str(total_loss),
                    "트레일": str(total_trail),
                }
                if sl is not None:
                    total_row["손절"] = str(total_sl)
                total_row["평균수익률"] = f"{overall_avg:.4f}"
                print_rows.append(total_row)

                columns = ["date", "sell_date", "종목수", "수익", "손실", "트레일"]
                if sl is not None:
                    columns.append("손절")
                columns.append("평균수익률")
                align = {c: "right" for c in columns}
                align["date"] = "left"
                align["sell_date"] = "left"
                print_table(print_rows, columns, align)

                # 콘솔 월별 출력
                print(f"\n    --- 월별 요약 ({gap_label}) ---")
                month_print = []
                for r in monthly_rows:
                    month_print.append({
                        "month": r["month"],
                        "거래일수": str(r["trading_days"]),
                        "종목수": str(r["total_stocks"]),
                        "수익": str(r["win"]),
                        "손실": str(r["loss"]),
                        "평균수익률": f"{r['avg_return']:.4f}",
                    })
                mcols = ["month", "거래일수", "종목수", "수익", "손실", "평균수익률"]
                malign = {c: "right" for c in mcols}
                malign["month"] = "left"
                print_table(month_print, mcols, malign)

        # === Phase 3: 단일 CSV 통합 저장 ===
        summary_path = BASE_DIR / f"top_reach_summary_{args.start}_{args.end}.csv"
        with open(summary_path, "w", encoding="utf-8-sig") as f:
            # 섹션 1: 일자별 상세
            f.write("=== 일자별 상세 ===\n")
            if all_daily_rows:
                df_daily = pd.DataFrame(all_daily_rows)
                df_daily.to_csv(f, index=False, float_format="%.6f")
            f.write("\n")

            # 섹션 2: 월별 요약
            f.write("=== 월별 요약 ===\n")
            if all_monthly_rows:
                df_monthly = pd.DataFrame(all_monthly_rows)
                df_monthly.to_csv(f, index=False, float_format="%.6f")
            f.write("\n")

            # 섹션 3: 최종 비교표
            f.write("=== 최종 비교표 ===\n")
            if all_summary_rows:
                df_summary = pd.DataFrame(all_summary_rows)
                df_summary.to_csv(f, index=False, float_format="%.6f")

        print(f"\n[csv] 통합 요약 저장: {summary_path.name}")

        # === 콘솔 최종 비교표 출력 ===
        if all_summary_rows:
            print(f"\n{'='*60}")
            print(f"  최종 비교표")
            print(f"{'='*60}")
            comp_rows = []
            for r in all_summary_rows:
                comp_rows.append({
                    "스톱로스": r["stop_loss"],
                    "괴리율": r["gap_threshold"],
                    "총종목수": str(r["총종목수"]),
                    "수익": str(r["수익"]),
                    "손실": str(r["손실"]),
                    "승률": f"{r['승률']:.1f}%",
                    "평균수익률": f"{r['평균수익률']:.4f}",
                })
            comp_cols = ["스톱로스", "괴리율", "총종목수", "수익", "손실", "승률", "평균수익률"]
            comp_align = {c: "right" for c in comp_cols}
            comp_align["스톱로스"] = "left"
            comp_align["괴리율"] = "left"
            print_table(comp_rows, comp_cols, comp_align)

    finally:
        con.close()

    end_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    elapsed = time.perf_counter() - t0
    print(f"\n[{end_ts}] 프로그램 종료 (총 소요시간: {elapsed:.3f}초)")


if __name__ == "__main__":
    main()
