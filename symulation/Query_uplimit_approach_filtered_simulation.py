"""
상한가 근접(25%+) 장중 매수 — 필터 적용 시뮬레이션
Query_uplimit_approach_filtered_simulation.py

기본 시뮬레이션(Query_uplimit_approach_simulation.py) 결과:
  - 필터 없이 진입 시 승률 24~44%, 평균수익 -1.0~-1.6% → 손실
  - Signal Strength 필터 필요하나, 체결강도/외국인 데이터는 과거 저장 없음

이 파일에서는 1m/1d parquet에 이미 존재하는 데이터로 필터를 구성:
  1) 거래량 필터: 진입 봉 직전 5분 평균 거래량 vs 당일 평균 대비 배율
  2) 시가 갭 필터: 당일 시가 vs 전일종가 갭률
  3) 변동성 필터: 직전 10일 일간수익률 표준편차
  4) 상승 속도 필터: 장 시작부터 25%+ 도달까지 걸린 시간
  5) 전일 등락률 필터: 전일 이미 급등(연속상승) 여부

사용법:
  python Query_uplimit_approach_filtered_simulation.py --start 20260201 --end 20260418
"""

DEFAULT_START = "20260201"
DEFAULT_END   = "20260418"

import argparse
import sys
import time
from datetime import datetime
from math import floor
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from kis_utils import load_symbol_master, print_table, load_kis_data_layout, _configure_duckdb_s3

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
PARQUET_1D = DATA_DIR / "1d_data" / "kis_1d_unified_parquet_DB.parquet"

# ── 진입 ──
ENTRY_THRESHOLD = 0.28         # +28% (이전 시뮬에서 최적)
MIN_PRICE = 1000

# ── 매도 ──
SELL_TIME = "151800"
STOP_LOSS = 0.03
TIMEOUT_MINUTES = 10

# ── 필터 조합 (on/off 토글) ──
FILTERS = {
    "vol_surge":    True,   # 진입 직전 5분 거래량 > 당일 평균의 N배
    "gap_up":       True,   # 시가 갭 >= N%
    "low_vol_10d":  True,   # 10일 변동성 < N%
    "fast_rise":    True,   # 장시작~진입까지 N분 이내
    "no_prev_surge":True,   # 전일 등락률 < N% (연속급등 제외)
}

# ── 필터 파라미터 (그리드 서치용) ──
FILTER_GRID = {
    "vol_surge_ratio":  [2.0, 3.0, 5.0],     # 당일 평균 거래량 대비 배율
    "gap_min":          [0.0, 3.0, 5.0],      # 시가 갭 최소 (%)
    "vol_10d_max":      [0.03, 0.05, 999],    # 10일 변동성 최대 (999=필터 OFF)
    "rise_minutes_max": [30, 60, 999],         # 25% 도달까지 최대 분 (999=OFF)
    "prev_ctrt_max":    [10.0, 20.0, 999],    # 전일 등락률 최대 (999=OFF)
}

# ── 매도 모드 (이전 시뮬에서 최적: 상한가 트레일) ──
SELL_MODE = {"name": "상한가트레일/29%이탈", "stop_loss": 0.03,
             "trail_type": "upper_limit", "exit_pct": 0.29}

ETF_KEYWORDS = ["TIGER", "KODEX", "RISE", "ETN", "ETF", "PLUS", "KOSEF", "KBSTAR",
                "HANARO", "SOL", "ACE", "ARIRANG", "BNK", "TIMEFOLIO", "KINDEX"]


def _tick_size(price: float) -> int:
    if price < 2000:   return 1
    if price < 5000:   return 5
    if price < 20000:  return 10
    if price < 50000:  return 50
    if price < 200000: return 100
    if price < 500000: return 500
    return 1000


def _upper_limit_price(pdy_close: float) -> float:
    raw = pdy_close * 1.30
    tick = _tick_size(raw)
    return floor(raw / tick) * tick


def _is_etf_etn(code: str, name: str) -> bool:
    if code.startswith("Q"):
        return True
    return any(kw in name.upper() for kw in ETF_KEYWORDS)


def _get_trading_dates(con, start, end):
    df = con.execute(f"""
        SELECT DISTINCT date FROM read_parquet('{PARQUET_1D}')
        WHERE date >= '{start}' AND date <= '{end}'
        ORDER BY date
    """).df()
    return [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in df["date"]]


def _get_all_trading_dates(con):
    df = con.execute(f"""
        SELECT DISTINCT date FROM read_parquet('{PARQUET_1D}')
        ORDER BY date
    """).df()
    return [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in df["date"]]


def _get_available_1m_dates(con):
    try:
        df = con.execute(
            "SELECT file FROM glob('s3://tfttrain/KIS_DB/market_data/1m/*/ohlcv.parquet')"
        ).df()
        import re
        dates = set()
        for f in df["file"]:
            m = re.search(r"date=(\d{4}-\d{2}-\d{2})", str(f))
            if m:
                dates.add(m.group(1))
        return dates
    except Exception as e:
        print(f"  [warn] S3 1m 날짜 목록 조회 실패: {e}")
        return set()


def _calc_10d_volatility(con, date_str: str, all_dates: list[str]) -> dict[str, float]:
    """해당 날짜 직전 10거래일의 일간수익률 표준편차"""
    idx = all_dates.index(date_str) if date_str in all_dates else -1
    if idx < 11:
        return {}
    start_d = all_dates[idx - 11]
    end_d = all_dates[idx - 1]

    df = con.execute(f"""
        SELECT symbol, date, close, pdy_close
        FROM read_parquet('{PARQUET_1D}')
        WHERE date >= '{start_d}' AND date <= '{end_d}'
          AND pdy_close > 0 AND close > 0
        ORDER BY symbol, date
    """).df()

    if df.empty:
        return {}

    df["ret"] = df["close"] / df["pdy_close"] - 1.0
    vol = df.groupby("symbol")["ret"].std()
    return vol.to_dict()


def _calc_prev_day_ctrt(con, date_str: str, all_dates: list[str]) -> dict[str, float]:
    """전일 등락률"""
    idx = all_dates.index(date_str) if date_str in all_dates else -1
    if idx < 1:
        return {}
    prev_d = all_dates[idx - 1]

    df = con.execute(f"""
        SELECT symbol, close, pdy_close
        FROM read_parquet('{PARQUET_1D}')
        WHERE date = '{prev_d}'
          AND pdy_close > 0 AND close > 0
    """).df()

    if df.empty:
        return {}

    df["ctrt"] = (df["close"] / df["pdy_close"] - 1.0) * 100
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    return dict(zip(df["symbol"], df["ctrt"]))


def _find_candidates(con, date_str: str) -> pd.DataFrame:
    min_high_ratio = 1.0 + ENTRY_THRESHOLD
    df = con.execute(f"""
        SELECT symbol, name, pdy_close, open, high, low, close
        FROM read_parquet('{PARQUET_1D}')
        WHERE date = '{date_str}'
          AND pdy_close >= {MIN_PRICE}
          AND pdy_close > 0
          AND high >= pdy_close * {min_high_ratio}
    """).df()

    if df.empty:
        return pd.DataFrame()

    df["symbol"] = df["symbol"].astype(str).str.zfill(6)

    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        code_to_name = dict(zip(sdf["code"], sdf["name"].astype(str)))
        df["name"] = df["symbol"].map(code_to_name).fillna(df["name"])
    except Exception:
        pass

    mask = df.apply(lambda r: not _is_etf_etn(r["symbol"], str(r["name"])), axis=1)
    df = df[mask].copy()

    df["upper_limit"] = df["pdy_close"].apply(_upper_limit_price)
    df["entry_target"] = df["pdy_close"] * (1.0 + ENTRY_THRESHOLD)
    df["gap_pct"] = (df["open"] / df["pdy_close"] - 1.0) * 100

    return df


def _simulate_with_filters(
    con, layout, date_str: str, candidates: pd.DataFrame,
    vol_10d: dict, prev_ctrt: dict,
    params: dict, available_1m_dates: set | None,
) -> list[dict]:
    """1m 데이터로 필터 적용 + 매매 시뮬레이션"""

    if available_1m_dates and date_str not in available_1m_dates:
        return []

    codes = candidates["symbol"].tolist()
    if not codes:
        return []
    codes_sql = ",".join(f"'{c}'" for c in codes)

    try:
        df_1m = con.execute(f"""
            SELECT code, time, open, high, low, close, volume
            FROM read_parquet('{layout.s3_1m_date(date_str)}')
            WHERE code IN ({codes_sql})
            ORDER BY code, time
        """).df()
    except Exception:
        return []

    if df_1m.empty:
        return []

    # 종목 정보 매핑
    info = {}
    for _, row in candidates.iterrows():
        code = str(row["symbol"]).zfill(6)
        info[code] = {
            "pdy_close": float(row["pdy_close"]),
            "upper_limit": float(row["upper_limit"]),
            "entry_target": float(row["entry_target"]),
            "name": str(row.get("name", "")),
            "gap_pct": float(row["gap_pct"]),
        }

    vol_surge_ratio = params["vol_surge_ratio"]
    gap_min = params["gap_min"]
    vol_10d_max = params["vol_10d_max"]
    rise_minutes_max = params["rise_minutes_max"]
    prev_ctrt_max = params["prev_ctrt_max"]

    stop_loss = SELL_MODE["stop_loss"]
    exit_pct = SELL_MODE["exit_pct"]

    results = []
    for code, grp in df_1m.groupby("code"):
        code = str(code).zfill(6)
        ci = info.get(code)
        if ci is None:
            continue

        pdy_close = ci["pdy_close"]
        upper_limit = ci["upper_limit"]
        entry_target = ci["entry_target"]

        grp = grp.sort_values("time").reset_index(drop=True)
        times = grp["time"].astype(str).str.replace(":", "").str.replace(".", "").str.strip().str.zfill(6).values
        highs = grp["high"].astype(float).values
        lows = grp["low"].astype(float).values
        closes = grp["close"].astype(float).values
        volumes = grp["volume"].astype(float).values

        # ── 필터 1: 전일 등락률 ──
        pc = prev_ctrt.get(code, 0.0)
        if prev_ctrt_max < 999 and abs(pc) >= prev_ctrt_max:
            continue

        # ── 필터 2: 10일 변동성 ──
        v10 = vol_10d.get(code, 0.0)
        if vol_10d_max < 999 and v10 > vol_10d_max:
            continue

        # ── 필터 3: 시가 갭 ──
        if ci["gap_pct"] < gap_min:
            continue

        # ── 매수 시점 찾기 ──
        buy_price = 0.0
        buy_time = ""
        buy_idx = -1

        for i in range(len(grp)):
            if times[i] > SELL_TIME:
                break
            if highs[i] >= entry_target:
                buy_price = max(closes[i], entry_target)
                buy_time = times[i]
                buy_idx = i
                break

        if buy_price <= 0:
            continue

        # ── 필터 4: 상승 속도 ──
        if rise_minutes_max < 999:
            if buy_idx > rise_minutes_max:
                continue

        # ── 필터 5: 거래량 서지 ──
        if vol_surge_ratio > 0:
            # 진입 직전 5분 평균 거래량 vs 당일 평균(진입시점까지)
            if buy_idx >= 5:
                recent_vol = np.mean(volumes[buy_idx-5:buy_idx])
                avg_vol = np.mean(volumes[:buy_idx]) if buy_idx > 0 else 1
                if avg_vol > 0 and recent_vol < avg_vol * vol_surge_ratio:
                    continue
            # 데이터 부족 시 필터 스킵

        # ── 매도 시뮬레이션 (상한가 트레일) ──
        peak_price = buy_price
        sell_price = 0.0
        sell_time = ""
        sell_reason = ""
        reached_upper = False
        stop_loss_price = buy_price * (1.0 - stop_loss)
        exit_price_29 = pdy_close * (1.0 + exit_pct)
        close_1518 = 0.0

        for i in range(buy_idx + 1, len(grp)):
            t = times[i]
            h = highs[i]
            lo = lows[i]
            c = closes[i]

            if t == SELL_TIME:
                close_1518 = c
            if t > SELL_TIME:
                break

            if h > peak_price:
                peak_price = h
            if h >= upper_limit and not reached_upper:
                reached_upper = True

            # 손절 (상한가 미도달 시)
            if not reached_upper and lo <= stop_loss_price:
                sell_price = stop_loss_price
                sell_time = t
                sell_reason = f"손절 -{stop_loss*100:.0f}%"
                break

            # 상한가 도달 후 29% 이탈
            if reached_upper and lo <= exit_price_29:
                sell_price = exit_price_29
                sell_time = t
                sell_reason = "상한가→29%이탈"
                break

            # 타임아웃
            if TIMEOUT_MINUTES > 0 and not reached_upper:
                minutes_since = i - buy_idx
                if minutes_since >= TIMEOUT_MINUTES:
                    cur_ret = (c / buy_price) - 1.0
                    if cur_ret < 0.01:
                        peak_ret_so_far = (peak_price / buy_price) - 1.0
                        if peak_ret_so_far < 0.01:
                            sell_price = c
                            sell_time = t
                            sell_reason = f"{TIMEOUT_MINUTES}분 타임아웃"
                            break

        if sell_price <= 0:
            sell_price = close_1518 if close_1518 > 0 else closes[-1]
            sell_time = SELL_TIME if close_1518 > 0 else times[-1]
            sell_reason = "15:18 청산"

        ret = (sell_price / buy_price) - 1.0

        results.append({
            "date": date_str,
            "code": code,
            "name": ci["name"],
            "pdy_close": pdy_close,
            "buy_price": buy_price,
            "buy_time": buy_time,
            "buy_idx": buy_idx,
            "sell_price": round(sell_price, 0),
            "sell_time": sell_time,
            "sell_reason": sell_reason,
            "reached_upper": reached_upper,
            "gap_pct": round(ci["gap_pct"], 2),
            "vol_10d": round(v10, 4),
            "prev_ctrt": round(pc, 2),
            "return": round(ret, 4),
        })

    return results


def _summarize(detail_df: pd.DataFrame, label: str) -> dict:
    if detail_df.empty:
        return {}
    total = len(detail_df)
    wins = (detail_df["return"] >= 0).sum()
    win_rate = wins / total * 100
    avg_ret = detail_df["return"].mean() * 100
    med_ret = detail_df["return"].median() * 100
    max_ret = detail_df["return"].max() * 100
    min_ret = detail_df["return"].min() * 100
    upper_cnt = detail_df["reached_upper"].sum()
    upper_rate = upper_cnt / total * 100

    return {
        "label": label, "total": total, "wins": int(wins), "losses": total - int(wins),
        "win_rate": win_rate, "avg_ret": avg_ret, "med_ret": med_ret,
        "max_ret": max_ret, "min_ret": min_ret,
        "upper_cnt": int(upper_cnt), "upper_rate": upper_rate,
    }


def main():
    start_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    print(f"[{start_ts}] 상한가 근접 매수 — 필터 적용 시뮬레이션 시작")
    print(f"진입: +{int(ENTRY_THRESHOLD*100)}% | 매도: {SELL_MODE['name']}")

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    args = parser.parse_args()

    start_date = f"{args.start[:4]}-{args.start[4:6]}-{args.start[6:8]}"
    end_date = f"{args.end[:4]}-{args.end[4:6]}-{args.end[6:8]}"

    layout = load_kis_data_layout()
    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con)
        available_1m_dates = _get_available_1m_dates(con)
        if available_1m_dates:
            print(f"S3 1m 데이터: {len(available_1m_dates)}일")

        all_dates = _get_all_trading_dates(con)
        trading_dates = _get_trading_dates(con, start_date, end_date)
        if not trading_dates:
            print("거래일 없음")
            return

        print(f"기간: {start_date} ~ {end_date} ({len(trading_dates)}일)\n")

        # ── 날짜별 사전 데이터 수집 ──
        print("사전 데이터 수집 중...")
        date_data = {}
        for date_str in trading_dates:
            if available_1m_dates and date_str not in available_1m_dates:
                continue
            cands = _find_candidates(con, date_str)
            if cands.empty:
                continue
            vol_10d = _calc_10d_volatility(con, date_str, all_dates)
            prev_ctrt = _calc_prev_day_ctrt(con, date_str, all_dates)
            date_data[date_str] = (cands, vol_10d, prev_ctrt)

        print(f"후보 있는 날: {len(date_data)}일\n")

        # ── 필터 그리드 서치 ──
        from itertools import product

        grid_keys = list(FILTER_GRID.keys())
        grid_values = [FILTER_GRID[k] for k in grid_keys]
        combos = list(product(*grid_values))

        # 베이스라인 (필터 없음) 추가
        baseline_params = {
            "vol_surge_ratio": 0, "gap_min": 0, "vol_10d_max": 999,
            "rise_minutes_max": 999, "prev_ctrt_max": 999,
        }

        all_summaries = []

        # 베이스라인 먼저
        print("=" * 70)
        print(" 베이스라인 (필터 없음)")
        print("=" * 70)
        base_results = []
        for date_str, (cands, vol_10d, prev_ctrt) in date_data.items():
            res = _simulate_with_filters(
                con, layout, date_str, cands, vol_10d, prev_ctrt,
                baseline_params, available_1m_dates)
            base_results.extend(res)

        if base_results:
            base_df = pd.DataFrame(base_results)
            s = _summarize(base_df, "필터없음")
            if s:
                all_summaries.append({**s, **baseline_params})
                print(f"  거래: {s['total']}건 | 승률: {s['win_rate']:.1f}% | "
                      f"평균: {s['avg_ret']:+.2f}% | 상한가: {s['upper_rate']:.1f}%")

        # 그리드 서치
        print(f"\n필터 조합 {len(combos)}개 탐색 중...")
        best_summary = None
        best_win_rate = 0
        best_params = None

        for ci, combo in enumerate(combos):
            params = dict(zip(grid_keys, combo))

            all_results = []
            for date_str, (cands, vol_10d, prev_ctrt) in date_data.items():
                res = _simulate_with_filters(
                    con, layout, date_str, cands, vol_10d, prev_ctrt,
                    params, available_1m_dates)
                all_results.extend(res)

            if len(all_results) < 20:
                continue

            df = pd.DataFrame(all_results)
            s = _summarize(df, f"combo_{ci}")
            if not s:
                continue

            all_summaries.append({**s, **params})

            # 최적 추적 (승률 기준, 최소 50건)
            if s["total"] >= 50 and s["win_rate"] > best_win_rate:
                best_win_rate = s["win_rate"]
                best_summary = s
                best_params = params
                best_df = df

            if (ci + 1) % 50 == 0:
                print(f"  {ci+1}/{len(combos)} 완료...")

        # ── 결과 출력 ──
        print(f"\n\n{'='*80}")
        print(f"{'필터 그리드 서치 결과':^80}")
        print(f"{'='*80}")

        if not all_summaries:
            print("유효한 결과 없음")
            return

        # 상위 15개 (승률 기준, 최소 50건)
        valid = [s for s in all_summaries if s["total"] >= 50]
        valid.sort(key=lambda x: x["win_rate"], reverse=True)

        comp_rows = []
        for s in valid[:15]:
            comp_rows.append({
                "거래수": s["total"],
                "승률": f"{s['win_rate']:.1f}%",
                "평균수익": f"{s['avg_ret']:+.2f}%",
                "상한가율": f"{s['upper_rate']:.1f}%",
                "거래량배율": s.get("vol_surge_ratio", "-"),
                "갭최소%": s.get("gap_min", "-"),
                "변동성MAX": s.get("vol_10d_max", "-"),
                "도달분MAX": s.get("rise_minutes_max", "-"),
                "전일MAX%": s.get("prev_ctrt_max", "-"),
            })

        comp_df = pd.DataFrame(comp_rows)
        print("\n[승률 상위 15 조합 (최소 50건)]")
        print(comp_df.to_string(index=False))

        # 수익률 기준 상위
        valid.sort(key=lambda x: x["avg_ret"], reverse=True)
        comp_rows2 = []
        for s in valid[:15]:
            comp_rows2.append({
                "거래수": s["total"],
                "승률": f"{s['win_rate']:.1f}%",
                "평균수익": f"{s['avg_ret']:+.2f}%",
                "상한가율": f"{s['upper_rate']:.1f}%",
                "거래량배율": s.get("vol_surge_ratio", "-"),
                "갭최소%": s.get("gap_min", "-"),
                "변동성MAX": s.get("vol_10d_max", "-"),
                "도달분MAX": s.get("rise_minutes_max", "-"),
                "전일MAX%": s.get("prev_ctrt_max", "-"),
            })

        comp_df2 = pd.DataFrame(comp_rows2)
        print("\n[평균수익 상위 15 조합 (최소 50건)]")
        print(comp_df2.to_string(index=False))

        # 최적 조합 상세
        if best_summary and best_params:
            print(f"\n{'='*70}")
            print(f" 최적 조합 (승률 기준)")
            print(f"{'='*70}")
            print(f" 파라미터: {best_params}")
            print(f" 거래: {best_summary['total']}건 | 승: {best_summary['wins']} | 패: {best_summary['losses']}")
            print(f" 승률: {best_summary['win_rate']:.1f}%")
            print(f" 평균수익: {best_summary['avg_ret']:+.2f}% | 중간값: {best_summary['med_ret']:+.2f}%")
            print(f" 최대: {best_summary['max_ret']:+.2f}% | 최소: {best_summary['min_ret']:+.2f}%")
            print(f" 상한가도달: {best_summary['upper_cnt']}건 ({best_summary['upper_rate']:.1f}%)")

            # 최적 상세 CSV
            best_path = BASE_DIR / "uplimit_filtered_best.csv"
            best_df.to_csv(best_path, index=False, encoding="utf-8-sig", float_format="%.4f")
            print(f"\n [csv] {best_path.name}")

            # 매도사유 분포
            reason_counts = best_df["sell_reason"].value_counts()
            print(f"\n 매도 사유:")
            for reason, cnt in reason_counts.items():
                print(f"   {reason}: {cnt}건 ({cnt/len(best_df)*100:.1f}%)")

        # 전체 그리드 CSV
        grid_path = BASE_DIR / "uplimit_filtered_grid.csv"
        pd.DataFrame(all_summaries).to_csv(grid_path, index=False, encoding="utf-8-sig", float_format="%.4f")
        print(f"\n[csv] 전체 그리드 저장: {grid_path.name}")

    finally:
        con.close()

    elapsed = time.perf_counter() - t0
    print(f"\n완료 (소요시간: {elapsed:.1f}초)")


if __name__ == "__main__":
    main()
