"""
Top30 종목 선정 시점별 매매 전략 시뮬레이션

목적:
  Top30 스냅샷을 몇 시(09:01~09:30)에 기준으로 종목을 선정하여 투자를 시작하는 것이
  가장 좋은 성과를 내는지 분석.

전략:
  1) 각 분봉 스냅샷(09:01~09:30)에서 prdy_ctrt >= 5 인 종목 선정
  2) 1m parquet에서 해당 종목의 OHLCV + 지표(SMA, BB) 계산
  3) VI 판정: 1m 데이터에서 volume==0 & OHLC 동일 → VI 발동 간주
     재개봉 시가 vs 발동가 비교로 상승/하락 VI 구분
  4) 매수: VI 상승 해제 시가 매수 / VI 하락+양봉 종가 매수 / BB 반등 매수
  5) 매도: 손절 / 추가VI 트레일링 / MA20 하회 / 15:19 일괄매도(상한가 제외)
  6) 스냅샷 시간대별 성과 비교 분석

사용법:
  python Query_top30_trading_simulation.py
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
TOP30_DIR = DATA_DIR / "fetch_top_list" / "1m_fetch"
DIR_1M = DATA_DIR / "1m_data"

# ── 분석 대상 기간 (YYMMDD) ──
DATE_FROM = None
DATE_TO = None

# ── 매수 파라미터 ──
CTRT_MIN = 5.0          # Top30에서 선정 기준 등락률
MIN_VOLUME = 20000       # 직전봉 최소 거래량
SNAPSHOT_RANGE = ("0901", "0930")  # 스냅샷 분석 범위
ENABLE_VI_BUY = False     # VI상승/VI하락양봉 매수 활성화
ENABLE_BB_BUY = False     # BB반등 매수 활성화

# ── 매도 파라미터 ──
LOSS_STOP = -0.03        # 기본 손절 -3%
LOSS_STOP_MA200 = -0.05  # ma3 < ma200 시 확대 손절
TRAIL_STEP = 0.008       # 트레일링 스톱 0.8%
MARKET_CLOSE_TIME = "151900"  # 장마감 일괄매도 시각
UPPER_LIMIT_CTRT = 25.0  # 상한가 근접 기준 (매도 제외)

ETF_KEYWORDS = ["TIGER", "KODEX", "RISE", "ETN", "ETF", "PLUS", "KOSEF", "KBSTAR",
                "HANARO", "SOL", "ACE", "ARIRANG", "BNK", "TIMEFOLIO", "KINDEX"]


def _is_etf_etn(code: str, name: str) -> bool:
    if code.startswith("Q"):
        return True
    name_upper = name.upper()
    return any(kw in name_upper for kw in ETF_KEYWORDS)


def _get_available_dates() -> list[str]:
    """Top30 merged와 1m parquet 모두 존재하는 날짜 목록 (YYMMDD)"""
    # Top30 merged 날짜
    top30_files = sorted(TOP30_DIR.glob("Top30_1m_merged_*.csv"))
    top30_dates = set()
    for f in top30_files:
        parts = f.stem.split("_")  # Top30_1m_merged_YYMMDD
        if len(parts) >= 4:
            top30_dates.add(parts[3])

    # 1m parquet 날짜
    parquet_dates = set()
    for f in DIR_1M.glob("*_1m_chart_DB_parquet.parquet"):
        # 20YYMMDD_1m_...
        d = f.stem[:8]  # 20YYMMDD
        parquet_dates.add(d[2:])  # YYMMDD

    common = sorted(top30_dates & parquet_dates)

    if DATE_FROM:
        common = [d for d in common if d >= DATE_FROM]
    if DATE_TO:
        common = [d for d in common if d <= DATE_TO]

    return common


def _load_snapshot_candidates(date_ymd: str, snapshot_hhmm: str,
                              _cache: dict = {}) -> dict:
    """
    특정 날짜/시각의 Top30 스냅샷에서 prdy_ctrt >= CTRT_MIN 종목 선정.
    merged 파일에서 fetch_time으로 필터링.
    Returns: {code: (name, ctrt, price)}
    """
    # merged 파일을 날짜별로 캐싱
    if date_ymd not in _cache:
        fpath = TOP30_DIR / f"Top30_1m_merged_{date_ymd}.csv"
        if not fpath.exists():
            _cache[date_ymd] = pd.DataFrame()
        else:
            try:
                _cache[date_ymd] = pd.read_csv(fpath, dtype=str, encoding="utf-8-sig")
            except Exception:
                _cache[date_ymd] = pd.DataFrame()

    df_all = _cache[date_ymd]
    if df_all.empty:
        return {}

    # fetch_time 매칭: "HH:MM" 형식 → snapshot_hhmm "0901" → "09:01"
    snap_fmt = f"{snapshot_hhmm[:2]}:{snapshot_hhmm[2:]}"
    df = df_all[df_all["fetch_time"] == snap_fmt].copy()
    if df.empty:
        return {}

    df["stck_shrn_iscd"] = df["stck_shrn_iscd"].astype(str).str.zfill(6)
    df["prdy_ctrt"] = pd.to_numeric(df["prdy_ctrt"], errors="coerce")
    df["stck_prpr"] = pd.to_numeric(df["stck_prpr"], errors="coerce")

    # ETF/ETN 제외
    df = df[~df.apply(lambda r: _is_etf_etn(str(r["stck_shrn_iscd"]), str(r["hts_kor_isnm"])), axis=1)]
    # 등락률 필터
    df = df[df["prdy_ctrt"] >= CTRT_MIN]

    result = {}
    for _, row in df.iterrows():
        code = row["stck_shrn_iscd"]
        result[code] = (str(row["hts_kor_isnm"]), row["prdy_ctrt"], row["stck_prpr"])
    return result


def _load_1m_data(con, date_ymd: str, code: str) -> pd.DataFrame:
    """1m parquet에서 종목 데이터 로드 + 지표 계산 (이전 2일치 포함하여 MA/BB 정확도 향상)"""
    today_path = DIR_1M / f"20{date_ymd}_1m_chart_DB_parquet.parquet"
    if not today_path.exists():
        return pd.DataFrame()

    # 이전 2일치 parquet 파일 탐색
    target = f"20{date_ymd}"
    all_parquets = sorted(DIR_1M.glob("*_1m_chart_DB_parquet.parquet"))
    prior_files = [f for f in all_parquets if f.stem[:8] < target]
    prior_files = prior_files[-2:]  # 최대 2일

    load_files = prior_files + [today_path]

    # 각 파일에서 종목 데이터 로드 후 concat
    frames = []
    for fpath in load_files:
        sub = con.execute(f"""
            SELECT time, open, high, low, close, volume, acml_volume, tdy_ret
            FROM read_parquet('{fpath}')
            WHERE code = '{code}'
            ORDER BY time
        """).df()
        if not sub.empty:
            sub["_source"] = fpath.stem[:8]  # "20YYMMDD"
            frames.append(sub)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    df["time"] = df["time"].astype(str).str.replace(":", "").str.replace(".", "").str.strip().str.zfill(6)
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
    df["tdy_ret"] = pd.to_numeric(df["tdy_ret"], errors="coerce").fillna(0)

    # SMA
    df["ma3"] = df["close"].rolling(3, min_periods=1).mean()
    df["ma20"] = df["close"].rolling(20, min_periods=1).mean()
    df["ma200"] = df["close"].rolling(200, min_periods=1).mean()

    # Bollinger Bands (20, 2)
    df["bb_mid"] = df["ma20"]
    bb_std = df["close"].rolling(20, min_periods=1).std()
    df["bb_upper"] = df["bb_mid"] + 2 * bb_std
    df["bb_lower"] = df["bb_mid"] - 2 * bb_std

    # VI 판정: volume==0 and OHLC 동일
    df["is_vi"] = (df["volume"] == 0) & (df["open"] == df["high"]) & (df["high"] == df["low"]) & (df["low"] == df["close"])

    # 당일 데이터만 필터링하여 반환
    df = df[df["_source"] == target].drop(columns=["_source"]).reset_index(drop=True)

    if len(df) < 5:
        return pd.DataFrame()

    return df


def _detect_vi_events(df: pd.DataFrame) -> list[dict]:
    """
    1m 데이터에서 VI 이벤트 감지.
    VI: volume==0 & OHLC동일인 연속 봉.
    재개봉 시가 vs 발동가 비교로 상승/하락 구분.
    Returns: [{vi_start_idx, vi_end_idx, resume_idx, vi_price, resume_open, direction}, ...]
    """
    events = []
    i = 0
    n = len(df)
    while i < n:
        if df.iloc[i]["is_vi"]:
            vi_start = i
            vi_price = df.iloc[i]["close"]  # 발동가 = VI봉의 가격
            # 연속 VI 봉 찾기
            j = i + 1
            while j < n and df.iloc[j]["is_vi"]:
                j += 1
            vi_end = j - 1  # 마지막 VI 봉
            # 재개봉
            if j < n:
                resume_open = df.iloc[j]["open"]
                direction = "up" if resume_open >= vi_price else "down"
                events.append({
                    "vi_start_idx": vi_start,
                    "vi_end_idx": vi_end,
                    "resume_idx": j,
                    "vi_price": vi_price,
                    "resume_open": resume_open,
                    "resume_time": df.iloc[j]["time"],
                    "direction": direction,
                })
            i = j
        else:
            i += 1
    return events


def _simulate_stock(df: pd.DataFrame, vi_events: list[dict], buy_start_time: str) -> list[dict]:
    """
    한 종목에 대해 매수/매도 시뮬레이션.
    buy_start_time 이후의 봉부터 매수 조건 탐색.
    Returns: list of trade dicts
    """
    trades = []
    n = len(df)

    # buy_start_time 이후 시작 인덱스
    start_idx = 0
    for i in range(n):
        if df.iloc[i]["time"] >= buy_start_time:
            start_idx = i
            break
    else:
        return trades

    # VI 이벤트를 resume_idx 기준 dict로 정리
    vi_by_resume = {ev["resume_idx"]: ev for ev in vi_events}
    vi_active_set = set()
    for ev in vi_events:
        for idx in range(ev["vi_start_idx"], ev["resume_idx"] + 1):
            vi_active_set.add(idx)

    bought = False
    buy_price = 0.0
    buy_time = ""
    buy_reason = ""
    buy_idx = 0
    peak_price = 0.0
    trail_active = False  # 추가 VI 후 트레일링 스톱 활성화
    vi_count_after_buy = 0
    was_above_ma20 = False  # 매수 후 한 번이라도 MA20 위에 있었는지
    snapshot_buy_done = False  # 시가매수 평가 완료 여부 (이후 VI매수 비활성화)

    for i in range(start_idx, n):
        row = df.iloc[i]
        t = row["time"]
        o, h, lo, c = row["open"], row["high"], row["low"], row["close"]
        vol = row["volume"]

        if not bought:
            # ── 매수 탐색 ──
            # VI 봉이면 스킵 (재개봉 대기)
            if row["is_vi"]:
                continue

            if i == 0:
                continue
            prev = df.iloc[i - 1]

            # 1) VI 상승 매수: 재개봉 시가에 즉시 매수 (시가매수 평가 전에만)
            if ENABLE_VI_BUY and not snapshot_buy_done and i in vi_by_resume and vi_by_resume[i]["direction"] == "up":
                buy_price = o
                buy_time = t
                buy_reason = "VI상승"
                buy_idx = i
                bought = True
                peak_price = h
                trail_active = False
                vi_count_after_buy = 0
                was_above_ma20 = (o >= row["ma20"])
                continue

            # 2) VI 하락 양봉 매수: 재개봉이 양봉이면 종가에 매수 (시가매수 평가 전에만)
            if ENABLE_VI_BUY and not snapshot_buy_done and i in vi_by_resume and vi_by_resume[i]["direction"] == "down":
                if c > o:
                    buy_price = c
                    buy_time = t
                    buy_reason = "VI하락양봉"
                    buy_idx = i
                    bought = True
                    peak_price = h
                    trail_active = False
                    vi_count_after_buy = 0
                    was_above_ma20 = (c >= row["ma20"])
                continue  # 양봉이든 음봉이든 VI 재개봉 판정 완료

            # 3) 스냅샷 분봉 즉시 매수 (비VI, 첫 비VI봉에서만)
            is_first_non_vi = (i == start_idx) or (
                i == start_idx + 1 and df.iloc[start_idx]["is_vi"]
            )
            if is_first_non_vi and not prev["is_vi"]:
                # 직전봉 음봉이면 매수 안 함
                if prev["close"] < prev["open"]:
                    snapshot_buy_done = True
                    continue
                if o >= prev["close"]:
                    buy_price = o
                    buy_time = t
                    buy_reason = "시가매수"
                    buy_idx = i
                    bought = True
                    peak_price = h
                    trail_active = False
                    vi_count_after_buy = 0
                    was_above_ma20 = (o >= row["ma20"])
                elif h >= prev["close"]:
                    # 시가는 낮지만 고가가 직전종가를 넘으면 직전종가에 매수
                    buy_price = prev["close"]
                    buy_time = t
                    buy_reason = "시가매수"
                    buy_idx = i
                    bought = True
                    peak_price = h
                    trail_active = False
                    vi_count_after_buy = 0
                    was_above_ma20 = (prev["close"] >= row["ma20"])
                snapshot_buy_done = True
                continue  # 시가매수 판단은 스냅샷봉에서만, 이후 BB반등 모니터링으로

            # 4) BB반등 모니터링 (위에서 매수 안 된 경우 계속 순회)
            if ENABLE_BB_BUY and not prev["is_vi"] and i >= 2:
                if row["ma3"] < row["ma200"]:
                    if prev["close"] < prev["bb_lower"] and c > row["bb_lower"]:
                        buy_price = c
                        buy_time = t
                        buy_reason = "BB반등"
                        buy_idx = i
                        bought = True
                        peak_price = h
                        trail_active = False
                        vi_count_after_buy = 0
                        was_above_ma20 = (c >= row["ma20"])
                        continue

        else:
            # ── 매도 탐색 ──
            # VI 봉이면 매매 불가 (단, 추가 VI 카운트)
            if row["is_vi"]:
                # 매수 직후(매수봉 or 다음봉) VI는 트레일링 제외
                if i > buy_idx + 1:
                    vi_count_after_buy += 1
                    trail_active = True
                continue

            if h > peak_price:
                peak_price = h

            # MA20 위로 올라간 적 있으면 플래그 갱신
            if not was_above_ma20 and h >= row["ma20"]:
                was_above_ma20 = True

            ret_current = (c / buy_price) - 1.0
            ret_low = (lo / buy_price) - 1.0

            # 1) 손절
            loss_stop = LOSS_STOP
            if row["ma3"] < row["ma200"]:
                loss_stop = LOSS_STOP_MA200
            if ret_low <= loss_stop:
                sell_price = buy_price * (1.0 + loss_stop)
                trades.append(_make_trade(
                    buy_price, buy_time, buy_reason, buy_idx,
                    sell_price, t, "손절", peak_price, df))
                bought = False
                break

            # 2) 추가 VI 후 트레일링 스톱
            if trail_active and peak_price > buy_price:
                trail_stop = peak_price * (1.0 - TRAIL_STEP)
                if lo <= trail_stop:
                    trades.append(_make_trade(
                        buy_price, buy_time, buy_reason, buy_idx,
                        trail_stop, t, "VI트레일링", peak_price, df))
                    bought = False
                    break

            # 3) MA20 하회 매도 (MA20 위에서 매수했거나 이후 MA20 위로 올라간 적 있을 때만)
            if was_above_ma20 and (c < row["ma20"] or o < row["ma20"]):
                trades.append(_make_trade(
                    buy_price, buy_time, buy_reason, buy_idx,
                    c, t, "MA20하회", peak_price, df))
                bought = False
                break

            # 4) 15:19 일괄매도 (상한가 근접 제외)
            if t >= MARKET_CLOSE_TIME:
                tdy_ret_pct = row["tdy_ret"] * 100 if abs(row["tdy_ret"]) < 1 else row["tdy_ret"]
                if tdy_ret_pct >= UPPER_LIMIT_CTRT:
                    # 상한가 근접 → 미매도, 종가로 성과 평가
                    # 마지막 봉 찾기
                    last_row = df.iloc[-1]
                    trades.append(_make_trade(
                        buy_price, buy_time, buy_reason, buy_idx,
                        last_row["close"], last_row["time"], "상한가보유", peak_price, df))
                else:
                    trades.append(_make_trade(
                        buy_price, buy_time, buy_reason, buy_idx,
                        c, t, "장마감매도", peak_price, df))
                bought = False
                break

    # 매수 후 매도 안 된 경우 → 마지막 봉 종가
    if bought:
        last_row = df.iloc[-1]
        trades.append(_make_trade(
            buy_price, buy_time, buy_reason, buy_idx,
            last_row["close"], last_row["time"], "미매도종가", peak_price, df))

    return trades


def _make_trade(buy_price, buy_time, buy_reason, buy_idx,
                sell_price, sell_time, sell_reason, peak_price, df):
    ret = (sell_price / buy_price) - 1.0 if buy_price > 0 else 0.0
    peak_ret = (peak_price / buy_price) - 1.0 if buy_price > 0 else 0.0
    return {
        "buy_price": buy_price,
        "buy_time": buy_time,
        "buy_reason": buy_reason,
        "sell_price": sell_price,
        "sell_time": sell_time,
        "sell_reason": sell_reason,
        "return": ret,
        "peak_ret": peak_ret,
    }


def _print_summary_table(title: str, rows: list[dict], cols=None, extra_cols=None):
    if not rows:
        print(f"\n  {title}: 데이터 없음")
        return
    if cols is None:
        cols = ["구간", "매수건수", "승률", "평균수익률", "중앙수익률"]
    if extra_cols:
        cols.extend(extra_cols)
    align = {c: "right" for c in cols}
    align["구간"] = "left"
    print(f"\n  {title}")
    print_table(rows, cols, align)


def _make_stat_row(label: str, rets: list) -> dict | None:
    if not rets:
        return None
    wins = sum(1 for r in rets if r >= 0)
    return {
        "구간": label,
        "매수건수": str(len(rets)),
        "승률": f"{wins / len(rets) * 100:.1f}%",
        "평균수익률": f"{np.mean(rets) * 100:+.2f}%",
        "중앙수익률": f"{np.median(rets) * 100:+.2f}%",
    }


def main():
    start_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    print(f"[{start_ts}] Top30 시점별 매매 전략 시뮬레이션 시작\n")

    dates = _get_available_dates()
    if not dates:
        print("시뮬레이션 가능한 날짜가 없습니다. (Top30 ∩ 1m parquet)")
        return
    print(f"시뮬레이션 대상 날짜: {dates}")

    # 스냅샷 시각 목록 생성 (09:01 ~ 09:30)
    snapshot_times = []
    for m in range(int(SNAPSHOT_RANGE[0][2:]), int(SNAPSHOT_RANGE[1][2:]) + 1):
        snapshot_times.append(f"09{m:02d}")

    con = duckdb.connect()
    try:
        # 1m 데이터 캐시: {(date, code): (df, vi_events)}
        cache_1m = {}

        def _get_1m(date_ymd, code):
            key = (date_ymd, code)
            if key not in cache_1m:
                df = _load_1m_data(con, date_ymd, code)
                if df.empty:
                    cache_1m[key] = (pd.DataFrame(), [])
                else:
                    vi_events = _detect_vi_events(df)
                    cache_1m[key] = (df, vi_events)
            return cache_1m[key]

        # 전체 결과: {snapshot_time: [trade_dicts]}
        all_results = []

        for date_ymd in dates:
            print(f"\n── {date_ymd} 처리 중 ──")
            for snap_time in snapshot_times:
                snap_hhmm = snap_time  # "0901" etc
                candidates = _load_snapshot_candidates(date_ymd, snap_hhmm)
                if not candidates:
                    continue

                # 매수 시작 시각: 해당 분 (스냅샷 분봉에서 즉시 판단)
                snap_min = int(snap_hhmm[2:])
                buy_start = f"09{snap_min:02d}00"

                for code, (name, ctrt, price) in candidates.items():
                    df_1m, vi_events = _get_1m(date_ymd, code)
                    if df_1m.empty:
                        continue

                    trades = _simulate_stock(df_1m, vi_events, buy_start)
                    for tr in trades:
                        tr["date"] = date_ymd
                        tr["code"] = code
                        tr["name"] = name
                        tr["snapshot_time"] = f"09:{snap_min:02d}"
                        tr["snapshot_ctrt"] = ctrt
                        all_results.append(tr)

        if not all_results:
            print("\n매수 대상이 없습니다.")
            return

        result_df = pd.DataFrame(all_results)
        print(f"\n총 거래 건수: {len(result_df)}건")

        # ═══════════════════════════════════════════════════════
        # 1) 스냅샷 시간대별 성과 (핵심 분석)
        # ═══════════════════════════════════════════════════════
        print(f"\n{'=' * 80}")
        print(f"  1. 스냅샷 시간대별 성과 (몇 시에 종목 선정하는 것이 최적인가)")
        print(f"{'=' * 80}")

        snap_rows = []
        for st in sorted(result_df["snapshot_time"].unique()):
            sub = result_df[result_df["snapshot_time"] == st]
            row = _make_stat_row(st, sub["return"].tolist())
            if row:
                # 추가: 종목수, 매수사유 분포
                row["종목수"] = str(sub["code"].nunique())
                snap_rows.append(row)

        _print_summary_table("스냅샷 시간대별 종합 성과", snap_rows,
                             cols=["구간", "종목수", "매수건수", "승률", "평균수익률", "중앙수익률"])

        # ═══════════════════════════════════════════════════════
        # 2) 스냅샷 시간대별 × 날짜별 성과
        # ═══════════════════════════════════════════════════════
        print(f"\n{'=' * 80}")
        print(f"  2. 스냅샷 시간대별 × 날짜별 성과")
        print(f"{'=' * 80}")

        for date_ymd in sorted(result_df["date"].unique()):
            date_sub = result_df[result_df["date"] == date_ymd]
            date_rows = []
            for st in sorted(date_sub["snapshot_time"].unique()):
                sub = date_sub[date_sub["snapshot_time"] == st]
                row = _make_stat_row(st, sub["return"].tolist())
                if row:
                    row["종목수"] = str(sub["code"].nunique())
                    date_rows.append(row)
            _print_summary_table(f"[{date_ymd}] 스냅샷 시간대별 성과", date_rows,
                                 cols=["구간", "종목수", "매수건수", "승률", "평균수익률", "중앙수익률"])

        # ═══════════════════════════════════════════════════════
        # 3) 매수 사유별 성과
        # ═══════════════════════════════════════════════════════
        print(f"\n{'=' * 80}")
        print(f"  3. 매수 사유별 성과")
        print(f"{'=' * 80}")

        buy_reason_rows = []
        for reason in sorted(result_df["buy_reason"].unique()):
            sub = result_df[result_df["buy_reason"] == reason]
            row = _make_stat_row(reason, sub["return"].tolist())
            if row:
                buy_reason_rows.append(row)
        _print_summary_table("매수 사유별 성과", buy_reason_rows)

        # ═══════════════════════════════════════════════════════
        # 4) 매도 사유별 성과
        # ═══════════════════════════════════════════════════════
        print(f"\n{'=' * 80}")
        print(f"  4. 매도 사유별 성과")
        print(f"{'=' * 80}")

        sell_reason_rows = []
        for reason in sorted(result_df["sell_reason"].unique()):
            sub = result_df[result_df["sell_reason"] == reason]
            row = _make_stat_row(reason, sub["return"].tolist())
            if row:
                sell_reason_rows.append(row)
        _print_summary_table("매도 사유별 성과", sell_reason_rows)

        # ═══════════════════════════════════════════════════════
        # 5) 매수 시 등락률 구간별 성과
        # ═══════════════════════════════════════════════════════
        print(f"\n{'=' * 80}")
        print(f"  5. 매수 시 등락률 구간별 성과")
        print(f"{'=' * 80}")

        ctrt_bins = [(5, 10), (10, 15), (15, 20), (20, 30), (30, 100)]
        ctrt_rows = []
        for lo, hi in ctrt_bins:
            sub = result_df[(result_df["snapshot_ctrt"] >= lo) & (result_df["snapshot_ctrt"] < hi)]
            if sub.empty:
                continue
            label = f"{lo}~{hi}%" if hi < 100 else f"{lo}%+"
            row = _make_stat_row(label, sub["return"].tolist())
            if row:
                ctrt_rows.append(row)
        _print_summary_table("등락률 구간별 성과", ctrt_rows)

        # ═══════════════════════════════════════════════════════
        # 6) 전체 요약
        # ═══════════════════════════════════════════════════════
        print(f"\n{'=' * 80}")
        print(f"  6. 전체 요약")
        print(f"{'=' * 80}")

        rets = result_df["return"]
        wins = (rets >= 0).sum()
        print(f"  총 거래: {len(rets)}건")
        print(f"  승률: {wins / len(rets) * 100:.1f}%")
        print(f"  평균수익률: {rets.mean() * 100:+.2f}%")
        print(f"  중앙수익률: {rets.median() * 100:+.2f}%")
        print(f"  최대이익: {rets.max() * 100:+.2f}%")
        print(f"  최대손실: {rets.min() * 100:+.2f}%")

        # ═══════════════════════════════════════════════════════
        # 7) 상세 거래 내역 (상위 10건, 하위 10건)
        # ═══════════════════════════════════════════════════════
        print(f"\n{'=' * 80}")
        print(f"  7. 상세 거래 내역")
        print(f"{'=' * 80}")

        detail_cols = ["date", "snapshot_time", "code", "name", "snapshot_ctrt",
                       "buy_time", "buy_reason", "buy_price",
                       "sell_time", "sell_reason", "sell_price",
                       "return", "peak_ret"]

        sorted_df = result_df.sort_values("return", ascending=False)

        print("\n  [수익 상위 10건]")
        top10 = sorted_df.head(10)[detail_cols].copy()
        top10["return"] = top10["return"].apply(lambda x: f"{x * 100:+.2f}%")
        top10["peak_ret"] = top10["peak_ret"].apply(lambda x: f"{x * 100:+.2f}%")
        top10["buy_price"] = top10["buy_price"].apply(lambda x: f"{x:,.0f}")
        top10["sell_price"] = top10["sell_price"].apply(lambda x: f"{x:,.0f}")
        rows = top10.to_dict("records")
        align = {c: "right" for c in detail_cols}
        align["name"] = "left"
        align["buy_reason"] = "left"
        align["sell_reason"] = "left"
        print_table(rows, detail_cols, align)

        print("\n  [손실 하위 10건]")
        bottom10 = sorted_df.tail(10)[detail_cols].copy()
        bottom10["return"] = bottom10["return"].apply(lambda x: f"{x * 100:+.2f}%")
        bottom10["peak_ret"] = bottom10["peak_ret"].apply(lambda x: f"{x * 100:+.2f}%")
        bottom10["buy_price"] = bottom10["buy_price"].apply(lambda x: f"{x:,.0f}")
        bottom10["sell_price"] = bottom10["sell_price"].apply(lambda x: f"{x:,.0f}")
        rows = bottom10.to_dict("records")
        print_table(rows, detail_cols, align)

        # CSV 저장
        detail_path = BASE_DIR / "top30_trading_detail.csv"
        result_df[detail_cols].to_csv(detail_path, index=False, encoding="utf-8-sig", float_format="%.6f")
        print(f"\n[csv] 상세 저장: {detail_path.name} ({len(result_df)}건)")

    finally:
        con.close()

    elapsed = time.perf_counter() - t0
    end_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{end_ts}] 완료 (소요시간: {elapsed:.1f}초)")


if __name__ == "__main__":
    main()
