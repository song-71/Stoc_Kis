"""
Top30 상승종목 트레일링 스톱 시뮬레이션

전략:
  1) 1분마다 수집한 변동율 상위 30종목 CSV에서 종목 추적
  2) 매수 진입: (A) 첫 등장 다음분 매수 / (B) 등락률 조건 매수
  3) 매도: 트레일링 스톱 vs 종가 매도 비교
  4) 조건별 성과 분석

사용법:
  python Query_top30_trailing_simulation.py
"""

import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from kis_utils import print_table

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data" / "fetch_top_list" / "1m_fetch"

# ── 분석 대상 기간 (YYMMDD) ──
DATE_FROM = None   # 시작일 (None이면 전체)
DATE_TO   = None   # 종료일 (None이면 전체)

# ── 세부분석 필터 ──
# DETAIL_FILTER_APPLY = True 이면 아래 조건으로 섹션2 세부분석 필터링
# False 이면 기존과 동일하게 전체 데이터로 분석
DETAIL_FILTER_APPLY = True
FILTER_CTRT_MIN   = 5.0     # 매수 시 등락률 하한(%)
FILTER_CTRT_MAX   = 15.0    # 매수 시 등락률 상한 미만(%)
FILTER_BUY_BEFORE = "12:00" # 매수 시간 상한(09:00~상한시간까지만 매수)
FILTER_VOL_PACE_MIN = None        # 거래 페이스 하한 (예: 3.0)
FILTER_VOL_PACE_MAX = None        # 거래 페이스 상한
FILTER_MIN_VOL_RATIO_MIN = None   # 1분 거래량 비율 하한
FILTER_MIN_VOL_RATIO_MAX = None   # 1분 거래량 비율 상한

# ── 매수 조건 ──
ENTRY_MODES = {
    "첫등장_다음분": None,
    "등락률5%이상": 5.0,
    "등락률10%이상": 10.0,
    "등락률15%이상": 15.0,
}

# ── 트레일링 스톱 파라미터 ──
TRAIL_STEP = 0.01
STOP_LOSSES = [0.0, 0.015, 0.05, 0.10]
TRAIL_ACTIVATES = [0.03, 0.05, 0.07, 0.10]

def _apply_detail_filter(df: pd.DataFrame) -> pd.DataFrame:
    """DETAIL_FILTER_APPLY=True일 때 세부분석 필터 적용"""
    if not DETAIL_FILTER_APPLY:
        return df
    filtered = df.copy()
    if FILTER_CTRT_MIN is not None:
        filtered = filtered[filtered["buy_ctrt"] >= FILTER_CTRT_MIN]
    if FILTER_CTRT_MAX is not None:
        filtered = filtered[filtered["buy_ctrt"] < FILTER_CTRT_MAX]
    if FILTER_BUY_BEFORE is not None:
        filtered = filtered[filtered["buy_time"] < FILTER_BUY_BEFORE]
    if FILTER_VOL_PACE_MIN is not None:
        filtered = filtered[filtered["vol_pace"] >= FILTER_VOL_PACE_MIN]
    if FILTER_VOL_PACE_MAX is not None:
        filtered = filtered[filtered["vol_pace"] < FILTER_VOL_PACE_MAX]
    if FILTER_MIN_VOL_RATIO_MIN is not None:
        filtered = filtered[filtered["min_vol_ratio"] >= FILTER_MIN_VOL_RATIO_MIN]
    if FILTER_MIN_VOL_RATIO_MAX is not None:
        filtered = filtered[filtered["min_vol_ratio"] < FILTER_MIN_VOL_RATIO_MAX]
    return filtered


def _calc_elapsed_min(buy_time: str, sell_time: str) -> int:
    """HH:MM → 분 변환 후 차이"""
    bh, bm = int(buy_time[:2]), int(buy_time[3:5])
    sh, sm = int(sell_time[:2]), int(sell_time[3:5])
    return (sh * 60 + sm) - (bh * 60 + bm)


ETF_KEYWORDS = ["TIGER", "KODEX", "RISE", "ETN", "ETF", "PLUS", "KOSEF", "KBSTAR",
                 "HANARO", "SOL", "ACE", "ARIRANG", "BNK", "TIMEFOLIO", "KINDEX"]


def _is_etf_etn(code: str, name: str) -> bool:
    if code.startswith("Q"):
        return True
    name_upper = name.upper()
    return any(kw in name_upper for kw in ETF_KEYWORDS)


def _load_daily_data(csv_path: Path) -> pd.DataFrame:
    """하루치 top30 CSV 로드 및 정리"""
    df = pd.read_csv(csv_path, dtype=str)
    if df.empty:
        return pd.DataFrame()

    df["stck_shrn_iscd"] = df["stck_shrn_iscd"].astype(str).str.zfill(6)
    df["stck_prpr"] = pd.to_numeric(df["stck_prpr"], errors="coerce")
    df["prdy_ctrt"] = pd.to_numeric(df["prdy_ctrt"], errors="coerce")
    df["stck_hgpr"] = pd.to_numeric(df["stck_hgpr"], errors="coerce")
    df["stck_lwpr"] = pd.to_numeric(df["stck_lwpr"], errors="coerce")
    df["acml_vol"] = pd.to_numeric(df["acml_vol"], errors="coerce")

    # ETF/ETN 제외
    df = df[~df.apply(lambda r: _is_etf_etn(str(r["stck_shrn_iscd"]), str(r["hts_kor_isnm"])), axis=1)]

    # fetch_time 정렬
    df = df.sort_values("fetch_time").reset_index(drop=True)
    return df


def _build_code_timeseries(df: pd.DataFrame) -> dict:
    """종목별 시계열 데이터 구축: {code: [(time, price, ctrt, high, low, name), ...]}"""
    result = {}
    for _, row in df.iterrows():
        code = row["stck_shrn_iscd"]
        if code not in result:
            result[code] = []
        result[code].append({
            "time": str(row["fetch_time"]),
            "price": row["stck_prpr"],
            "ctrt": row["prdy_ctrt"],
            "high": row["stck_hgpr"],
            "low": row["stck_lwpr"],
            "name": str(row["hts_kor_isnm"]),
            "acml_vol": row["acml_vol"],
        })
    return result


def _find_entry(timeseries: list, mode_threshold) -> tuple:
    """
    매수 진입점 찾기.
    mode_threshold=None: 첫 등장 다음분 매수
    mode_threshold=float: 등락률이 threshold 이상인 시점 다음분 매수
    Returns: (buy_price, buy_time, buy_ctrt, buy_idx) or None
    """
    if len(timeseries) < 2:
        return None

    if mode_threshold is None:
        # 첫 등장 다음분
        return (timeseries[1]["price"], timeseries[1]["time"],
                timeseries[1]["ctrt"], 1)
    else:
        # 등락률 조건: threshold 이상인 첫 시점의 다음분
        for i in range(len(timeseries) - 1):
            if timeseries[i]["ctrt"] >= mode_threshold:
                return (timeseries[i + 1]["price"], timeseries[i + 1]["time"],
                        timeseries[i + 1]["ctrt"], i + 1)
        return None


def _simulate_trailing(timeseries: list, buy_idx: int, buy_price: float,
                       trail_activate: float, stop_loss: float) -> dict:
    """트레일링 스톱 시뮬레이션 (top30 가격 기반)"""
    peak_price = buy_price
    trail_stop = 0.0
    trail_step_applied = 0.0
    stop_loss_price = buy_price * (1.0 - stop_loss)

    for i in range(buy_idx, len(timeseries)):
        p = timeseries[i]["price"]
        t = timeseries[i]["time"]

        if p > peak_price:
            peak_price = p

        # 스톱로스 (트레일 미활성 시, stop_loss=0이면 비활성)
        if stop_loss > 0 and trail_stop == 0 and p <= stop_loss_price:
            return {
                "sell_price": p, "sell_time": t,
                "peak_ret": (peak_price / buy_price) - 1.0,
                "trail_triggered": False, "stop_loss_triggered": True,
                "last_observed": False,
                "return": (p / buy_price) - 1.0,
            }

        peak_ret = (peak_price / buy_price) - 1.0
        if peak_ret >= trail_activate:
            new_step = int(peak_ret / TRAIL_STEP) * TRAIL_STEP
            if new_step > trail_step_applied:
                trail_step_applied = new_step
                trail_stop = buy_price * (1.0 + trail_step_applied)

        if trail_stop > 0 and p <= trail_stop:
            return {
                "sell_price": p, "sell_time": t,
                "peak_ret": (peak_price / buy_price) - 1.0,
                "trail_triggered": True, "stop_loss_triggered": False,
                "last_observed": False,
                "return": (p / buy_price) - 1.0,
            }

    # 미발동: 마지막 관찰 가격 매도
    last = timeseries[-1]
    return {
        "sell_price": last["price"], "sell_time": last["time"],
        "peak_ret": (peak_price / buy_price) - 1.0,
        "trail_triggered": False, "stop_loss_triggered": False,
        "last_observed": True,
        "return": (last["price"] / buy_price) - 1.0,
    }


def _simulate_close_sell(timeseries: list, buy_idx: int, buy_price: float) -> dict:
    """종가 매도: 마지막 관찰 가격에 매도"""
    last = timeseries[-1]
    peak_price = buy_price
    for i in range(buy_idx, len(timeseries)):
        if timeseries[i]["price"] > peak_price:
            peak_price = timeseries[i]["price"]

    return {
        "sell_price": last["price"], "sell_time": last["time"],
        "peak_ret": (peak_price / buy_price) - 1.0,
        "trail_triggered": False, "stop_loss_triggered": False,
        "last_observed": True,
        "return": (last["price"] / buy_price) - 1.0,
    }


def _print_summary_table(title: str, rows: list[dict], extra_cols=None):
    if not rows:
        print(f"\n  {title}: 데이터 없음")
        return
    cols = ["구간", "매수건수", "승률", "평균수익률", "중앙수익률", "평균소요분"]
    if extra_cols:
        cols.extend(extra_cols)
    align = {c: "right" for c in cols}
    align["구간"] = "left"
    print(f"\n  {title}")
    print_table(rows, cols, align)


def _make_stat_row(label: str, rets: list, elapsed_mins: list = None) -> dict:
    if not rets:
        return None
    wins = sum(1 for r in rets if r >= 0)
    row = {
        "구간": label,
        "매수건수": str(len(rets)),
        "승률": f"{wins/len(rets)*100:.1f}%",
        "평균수익률": f"{np.mean(rets)*100:+.2f}%",
        "중앙수익률": f"{np.median(rets)*100:+.2f}%",
    }
    if elapsed_mins:
        row["평균소요분"] = f"{np.mean(elapsed_mins):.1f}"
    return row


def main():
    start_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    print(f"[{start_ts}] Top30 상승종목 트레일링 스톱 시뮬레이션 시작\n")

    # CSV 파일 수집
    csv_files = sorted(DATA_DIR.glob("*op30_1m_merged_*.csv"))
    if not csv_files:
        print("CSV 파일이 없습니다.")
        return

    # 기간 필터
    if DATE_FROM or DATE_TO:
        def _in_range(f):
            # 파일명에서 YYMMDD 추출
            d = f.stem.split("_")[-1]  # e.g. 260219
            if DATE_FROM and d < DATE_FROM:
                return False
            if DATE_TO and d > DATE_TO:
                return False
            return True
        csv_files = [f for f in csv_files if _in_range(f)]

    if not csv_files:
        print("기간에 해당하는 CSV 파일이 없습니다.")
        return

    dates = sorted(set(f.stem.split("_")[-1] for f in csv_files))
    print(f"데이터 날짜: {dates} ({len(csv_files)}개 파일)")

    # 1D parquet 로드 (vol_ratio 계산용)
    PARQUET_1D = BASE_DIR.parent / "data" / "1d_data" / "kis_1d_unified_parquet_DB.parquet"
    df_1d = pd.read_parquet(PARQUET_1D, columns=["date", "symbol", "vema3", "vema20"])
    # date 변환: "2026-02-13" → "260213"
    df_1d["date_ymd"] = pd.to_datetime(df_1d["date"]).dt.strftime("%y%m%d")
    df_1d["symbol"] = df_1d["symbol"].astype(str).str.zfill(6)
    df_1d = df_1d.sort_values(["symbol", "date_ymd"]).reset_index(drop=True)
    # 전일 vema20 룩업 딕셔너리: {(symbol, date_ymd): vema20}  (date_ymd는 해당일 기준, 전일 값 저장)
    _vema20_prev = {}
    for sym, grp in df_1d.groupby("symbol"):
        dates_list = grp["date_ymd"].tolist()
        vema20_list = grp["vema20"].tolist()
        for i in range(1, len(dates_list)):
            _vema20_prev[(sym, dates_list[i])] = vema20_list[i - 1]

    # 전체 결과 수집
    all_results = []  # (entry_mode, trail_label, detail_dict)

    for csv_file in csv_files:
        date_str = csv_file.stem.split("_")[-1]
        df = _load_daily_data(csv_file)
        if df.empty:
            continue

        code_ts = _build_code_timeseries(df)

        for entry_name, threshold in ENTRY_MODES.items():
            for code, ts in code_ts.items():
                if len(ts) < 2:
                    continue

                entry = _find_entry(ts, threshold)
                if entry is None:
                    continue

                buy_price, buy_time, buy_ctrt, buy_idx = entry
                if buy_price is None or np.isnan(buy_price) or buy_price <= 0:
                    continue

                name = ts[0]["name"]
                first_ctrt = ts[0]["ctrt"]
                obs_count = len(ts)

                # 시간 보정 거래량 지표
                buy_acml_vol = ts[buy_idx]["acml_vol"]
                vema20_val = _vema20_prev.get((code, date_str))

                # elapsed_min: 09:00부터 매수시점까지 경과 분
                bh, bm = int(buy_time[:2]), int(buy_time[3:5])
                elapsed_min = (bh * 60 + bm) - (9 * 60)
                if elapsed_min <= 0:
                    elapsed_min = 1  # 최소 1분

                if vema20_val and vema20_val > 0 and buy_acml_vol and buy_acml_vol > 0:
                    # vol_pace: 현재 페이스 기준 하루 예상 거래량 / vema20
                    vol_pace = (buy_acml_vol / vema20_val) / (elapsed_min / 390)

                    # min_vol_ratio: 직전 1분 거래량 / vema20 1분 평균
                    if buy_idx > 0:
                        prev_acml = ts[buy_idx - 1]["acml_vol"]
                        per_min_vol = buy_acml_vol - prev_acml if prev_acml else buy_acml_vol
                    else:
                        per_min_vol = buy_acml_vol
                    vema20_per_min = vema20_val / 390
                    min_vol_ratio = per_min_vol / vema20_per_min if vema20_per_min > 0 else None
                else:
                    vol_pace = None
                    min_vol_ratio = None

                base_info = {
                    "date": date_str,
                    "code": code,
                    "name": name,
                    "entry_mode": entry_name,
                    "buy_price": buy_price,
                    "buy_time": buy_time,
                    "buy_ctrt": buy_ctrt,
                    "first_ctrt": first_ctrt,
                    "obs_count": obs_count,
                    "vol_pace": vol_pace,
                    "min_vol_ratio": min_vol_ratio,
                }

                # 종가 매도
                res_close = _simulate_close_sell(ts, buy_idx, buy_price)
                all_results.append({
                    **base_info,
                    "sell_strategy": "종가매도",
                    "trail_activate": None,
                    **res_close,
                    "elapsed_min": _calc_elapsed_min(buy_time, res_close["sell_time"]),
                })

                # 트레일링 스톱 × 스탑로스 조합
                for sl in STOP_LOSSES:
                    for ta in TRAIL_ACTIVATES:
                        res_trail = _simulate_trailing(ts, buy_idx, buy_price, ta, sl)
                        all_results.append({
                            **base_info,
                            "sell_strategy": f"트레일+{ta*100:.0f}%_SL{sl*100:.1f}%",
                            "trail_activate": ta,
                            "stop_loss": sl,
                            **res_trail,
                            "elapsed_min": _calc_elapsed_min(buy_time, res_trail["sell_time"]),
                        })

    if not all_results:
        print("매수 대상이 없습니다.")
        return

    result_df = pd.DataFrame(all_results)
    print(f"\n총 시뮬레이션 건수: {len(result_df)}건")

    # ═══════════════════════════════════════════════════════
    # 1) 매수방식별 × 매도전략별 요약 테이블
    # ═══════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print(f"  1. 매수방식별 × 매도전략별 요약")
    print(f"{'='*80}")

    sell_strategies = ["종가매도"] + [f"트레일+{ta*100:.0f}%_SL{sl*100:.1f}%"
                                      for sl in STOP_LOSSES for ta in TRAIL_ACTIVATES]
    summary_rows = []
    for entry_name in ENTRY_MODES:
        for ss in sell_strategies:
            sub = result_df[(result_df["entry_mode"] == entry_name) &
                            (result_df["sell_strategy"] == ss)]
            if sub.empty:
                continue
            rets = sub["return"]
            wins = (rets >= 0).sum()
            trail_cnt = sub["trail_triggered"].sum() if "trail_triggered" in sub else 0
            sl_cnt = sub["stop_loss_triggered"].sum() if "stop_loss_triggered" in sub else 0
            summary_rows.append({
                "매수방식": entry_name,
                "매도전략": ss,
                "매수건수": str(len(sub)),
                "승률": f"{wins/len(sub)*100:.1f}%",
                "평균수익률": f"{rets.mean()*100:+.2f}%",
                "중앙수익률": f"{rets.median()*100:+.2f}%",
                "평균소요분": f"{sub['elapsed_min'].mean():.1f}",
                "트레일발동": str(int(trail_cnt)),
                "손절": str(int(sl_cnt)),
            })

    cols = ["매수방식", "매도전략", "매수건수", "승률", "평균수익률", "중앙수익률", "평균소요분", "트레일발동", "손절"]
    align = {c: "right" for c in cols}
    align["매수방식"] = "left"
    align["매도전략"] = "left"
    print_table(summary_rows, cols, align)

    # ═══════════════════════════════════════════════════════
    # 2) 세부 분석 (첫등장_다음분 + 트레일+5% 기준)
    # ═══════════════════════════════════════════════════════
    base_entry = "첫등장_다음분"
    base_sell = "트레일+5%_SL5.0%"
    base_df = result_df[(result_df["entry_mode"] == base_entry) &
                        (result_df["sell_strategy"] == base_sell)]

    if DETAIL_FILTER_APPLY:
        pre_count = len(base_df)
        base_df = _apply_detail_filter(base_df)
        post_count = len(base_df)
        vol_filter_str = ""
        if FILTER_VOL_PACE_MIN is not None or FILTER_VOL_PACE_MAX is not None:
            vol_filter_str += f", vol_pace {FILTER_VOL_PACE_MIN}~{FILTER_VOL_PACE_MAX}"
        if FILTER_MIN_VOL_RATIO_MIN is not None or FILTER_MIN_VOL_RATIO_MAX is not None:
            vol_filter_str += f", min_vol_ratio {FILTER_MIN_VOL_RATIO_MIN}~{FILTER_MIN_VOL_RATIO_MAX}"
        filter_info = (f" | 필터: 등락률 {FILTER_CTRT_MIN}~{FILTER_CTRT_MAX}%, "
                       f"매수시간 <{FILTER_BUY_BEFORE}{vol_filter_str} ({pre_count}→{post_count}건)")
    else:
        filter_info = ""

    if base_df.empty:
        print(f"\n{base_entry} + {base_sell} 데이터 없음, 세부분석 건너뜀")
    else:
        print(f"\n{'='*80}")
        print(f"  2. 세부 분석 ({base_entry} + {base_sell}){filter_info}")
        print(f"{'='*80}")

        # 2-1) 매수 시 등락률 구간별
        ctrt_bins = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 100)]
        ctrt_rows = []
        for lo, hi in ctrt_bins:
            sub = base_df[(base_df["buy_ctrt"] >= lo) & (base_df["buy_ctrt"] < hi)]
            if sub.empty:
                continue
            label = f"{lo}~{hi}%" if hi < 100 else f"{lo}%+"
            row = _make_stat_row(label, sub["return"].tolist(), sub["elapsed_min"].tolist())
            if row:
                ctrt_rows.append(row)
        _print_summary_table("매수 시 등락률 구간별 성과", ctrt_rows)

        # 2-2) 시간대별
        hour_rows = []
        for t_range, label in [("09", "09시"), ("10", "10시"), ("11", "11시"),
                                ("12+", "12시+")]:
            if t_range == "12+":
                sub = base_df[base_df["buy_time"].str[:2] >= "12"]
            else:
                sub = base_df[base_df["buy_time"].str[:2] == t_range]
            if sub.empty:
                continue
            row = _make_stat_row(label, sub["return"].tolist(), sub["elapsed_min"].tolist())
            if row:
                hour_rows.append(row)
        _print_summary_table("매수 시간대별 성과", hour_rows)

        # 2-3) 날짜별
        date_rows = []
        for d in sorted(base_df["date"].unique()):
            sub = base_df[base_df["date"] == d]
            row = _make_stat_row(d, sub["return"].tolist(), sub["elapsed_min"].tolist())
            if row:
                date_rows.append(row)
        _print_summary_table("날짜별 성과", date_rows)

        # 2-4) 관찰 횟수별 (top30 잔류 시간)
        obs_bins = [(1, 5), (5, 15), (15, 30), (30, 999)]
        obs_rows = []
        for lo, hi in obs_bins:
            sub = base_df[(base_df["obs_count"] >= lo) & (base_df["obs_count"] < hi)]
            if sub.empty:
                continue
            label = f"{lo}~{hi}분" if hi < 999 else f"{lo}분+"
            row = _make_stat_row(label, sub["return"].tolist(), sub["elapsed_min"].tolist())
            if row:
                obs_rows.append(row)
        _print_summary_table("Top30 잔류 시간별 성과", obs_rows)

        # 2-5) 첫 등장 시 등락률 구간별
        first_ctrt_bins = [(0, 5), (5, 10), (10, 15), (15, 100)]
        first_rows = []
        for lo, hi in first_ctrt_bins:
            sub = base_df[(base_df["first_ctrt"] >= lo) & (base_df["first_ctrt"] < hi)]
            if sub.empty:
                continue
            label = f"{lo}~{hi}%" if hi < 100 else f"{lo}%+"
            row = _make_stat_row(label, sub["return"].tolist(), sub["elapsed_min"].tolist())
            if row:
                first_rows.append(row)
        _print_summary_table("첫 등장 시 등락률 구간별 성과", first_rows)

        # 2-6) vol_pace 구간별
        vp_df = base_df[base_df["vol_pace"].notna()]
        vol_pace_bins = [(0, 1), (1, 3), (3, 5), (5, 10), (10, 20), (20, 999)]
        vp_rows = []
        for lo, hi in vol_pace_bins:
            sub = vp_df[(vp_df["vol_pace"] >= lo) & (vp_df["vol_pace"] < hi)]
            if sub.empty:
                continue
            label = f"{lo}~{hi}배" if hi < 999 else f"{lo}배+"
            row = _make_stat_row(label, sub["return"].tolist(), sub["elapsed_min"].tolist())
            if row:
                vp_rows.append(row)
        _print_summary_table("vol_pace(시간보정 거래페이스) 구간별 성과", vp_rows)

        # 2-7) min_vol_ratio 구간별
        mvr_df = base_df[base_df["min_vol_ratio"].notna()]
        min_vol_bins = [(0, 3), (3, 10), (10, 30), (30, 100), (100, 999)]
        mvr_rows = []
        for lo, hi in min_vol_bins:
            sub = mvr_df[(mvr_df["min_vol_ratio"] >= lo) & (mvr_df["min_vol_ratio"] < hi)]
            if sub.empty:
                continue
            label = f"{lo}~{hi}배" if hi < 999 else f"{lo}배+"
            row = _make_stat_row(label, sub["return"].tolist(), sub["elapsed_min"].tolist())
            if row:
                mvr_rows.append(row)
        _print_summary_table("min_vol_ratio(직전1분 거래량/평소1분) 구간별 성과", mvr_rows)

    # ═══════════════════════════════════════════════════════
    # 3) 최적 조합 추천
    # ═══════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print(f"  3. 최적 조합 추천")
    print(f"{'='*80}")

    best_rows = []
    for entry_name in ENTRY_MODES:
        for ss in sell_strategies:
            sub = result_df[(result_df["entry_mode"] == entry_name) &
                            (result_df["sell_strategy"] == ss)]
            if len(sub) < 5:
                continue
            rets = sub["return"]
            wins = (rets >= 0).sum()
            best_rows.append({
                "매수방식": entry_name,
                "매도전략": ss,
                "건수": len(sub),
                "승률": wins / len(sub),
                "평균수익률": rets.mean(),
                "중앙수익률": rets.median(),
                "평균소요분": sub["elapsed_min"].mean(),
                "score": rets.mean() * (wins / len(sub)),  # 수익률×승률
            })

    if best_rows:
        best_rows.sort(key=lambda x: x["score"], reverse=True)
        print("\n  [수익률×승률 기준 상위 5개]")
        top5 = []
        for r in best_rows[:5]:
            top5.append({
                "구간": f"{r['매수방식']} + {r['매도전략']}",
                "매수건수": str(r["건수"]),
                "승률": f"{r['승률']*100:.1f}%",
                "평균수익률": f"{r['평균수익률']*100:+.2f}%",
                "중앙수익률": f"{r['중앙수익률']*100:+.2f}%",
                "평균소요분": f"{r['평균소요분']:.1f}",
            })
        _print_summary_table("최적 조합 Top 5", top5)

    # ═══════════════════════════════════════════════════════
    # 4) CSV 저장
    # ═══════════════════════════════════════════════════════
    detail_path = BASE_DIR / "top30_trailing_detail.csv"
    result_df.to_csv(detail_path, index=False, encoding="utf-8-sig", float_format="%.6f")
    print(f"\n[csv] 상세 저장: {detail_path.name} ({len(result_df)}건)")

    summary_save = pd.DataFrame(summary_rows)
    summary_path = BASE_DIR / "top30_trailing_summary.csv"
    summary_save.to_csv(summary_path, index=False, encoding="utf-8-sig", float_format="%.6f")
    print(f"[csv] 요약 저장: {summary_path.name}")

    elapsed = time.perf_counter() - t0
    end_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{end_ts}] 완료 (소요시간: {elapsed:.1f}초)")


if __name__ == "__main__":
    main()
