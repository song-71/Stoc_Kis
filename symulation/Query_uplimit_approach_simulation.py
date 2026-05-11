"""
상한가 근접(25%+) 장중 매수 → 상한가 도달/이탈 시뮬레이션
Query_uplimit_approach_simulation.py

전략:
  1) 1d 데이터에서 당일 고가(high)가 전일종가 대비 +25% 이상인 종목 선정
  2) 해당 종목의 당일 1m 데이터에서 최초 +25% 도달 시점을 매수 시점으로 설정
  3) 매수 후 시나리오별 시뮬레이션:
     - 상한가(+30%) 도달 여부 확인
     - 상한가 도달 후 이탈 시 트레일링 스탑 적용
     - 미도달 시 손절/타임아웃 적용
  4) 다양한 진입 임계값(25%, 26%, 27%, 28%) × 매도 전략 조합 비교

매수 조건:
  - 1m 봉 고가가 최초로 pdy_close × (1 + entry_threshold) 이상 도달
  - 매수가 = 해당 봉의 close (진입 확인 후 매수 가정)

매도 조건 (3가지 모드):
  1) 손절 전용: -3% 손절, 15:18 일괄 매도
  2) 상한가 트레일: 상한가 도달 후 29% 이탈 시 매도 (상한가 미도달 시 -3% 손절)
  3) 고점 대비 트레일: 고점 대비 -2% 하락 시 매도

사용법:
  python Query_uplimit_approach_simulation.py --start 20260201 --end 20260418
"""

# ── 기본 옵션 ──
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

# ── 진입 임계값 ──
ENTRY_THRESHOLDS = [0.25, 0.26, 0.27, 0.28]   # 전일종가 대비 +25%, +26%, +27%, +28%

# ── 매도 파라미터 ──
SELL_TIME = "151800"           # 15:18 일괄 매도
STOP_LOSS = 0.03               # 손절 -3%
TIMEOUT_MINUTES = 10           # 진입 후 N분 내 +1% 미도달 시 매도 (0=비활성)
UPPER_LIMIT_PCT = 0.30         # 상한가 = 전일종가 × 1.30
MIN_PRICE = 1000               # 최소 전일종가

# ── 매도 모드 ──
SELL_MODES = [
    {"name": "손절-3% / 15:18청산",
     "stop_loss": 0.03, "trail_type": None},
    {"name": "상한가트레일 / 29%이탈매도",
     "stop_loss": 0.03, "trail_type": "upper_limit", "exit_pct": 0.29},
    {"name": "고점대비-2% 트레일",
     "stop_loss": 0.03, "trail_type": "peak_drawdown", "drawdown": 0.02},
    {"name": "고점대비-3% 트레일",
     "stop_loss": 0.03, "trail_type": "peak_drawdown", "drawdown": 0.03},
]

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


def _get_trading_dates(con, start: str, end: str) -> list[str]:
    df = con.execute(f"""
        SELECT DISTINCT date FROM read_parquet('{PARQUET_1D}')
        WHERE date >= '{start}' AND date <= '{end}'
        ORDER BY date
    """).df()
    return [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in df["date"]]


def _get_available_1m_dates(con) -> set[str]:
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


def _find_candidates(con, date_str: str, entry_threshold: float) -> pd.DataFrame:
    """당일 고가가 전일종가 대비 entry_threshold 이상인 종목"""
    min_high_ratio = 1.0 + entry_threshold
    df = con.execute(f"""
        SELECT symbol, name, pdy_close, open, high, low, close
        FROM read_parquet('{PARQUET_1D}')
        WHERE date = '{date_str}'
          AND pdy_close >= {MIN_PRICE}
          AND pdy_close > 0
          AND high > 0
          AND high >= pdy_close * {min_high_ratio}
    """).df()

    if df.empty:
        return pd.DataFrame()

    df["symbol"] = df["symbol"].astype(str).str.zfill(6)

    # ETF/ETN 제외
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
    df["entry_price_target"] = df["pdy_close"] * (1.0 + entry_threshold)
    df["high_pct"] = (df["high"] / df["pdy_close"] - 1.0) * 100
    df["reached_upper"] = df["high"] >= df["upper_limit"]

    return df


def _simulate_intraday(
    con, layout, date_str: str,
    candidates: pd.DataFrame, entry_threshold: float,
    sell_mode: dict, available_1m_dates: set[str] | None = None,
) -> list[dict]:
    """당일 1m 데이터로 장중 매수→매도 시뮬레이션"""

    if available_1m_dates is not None and date_str not in available_1m_dates:
        return []

    codes = candidates["symbol"].tolist()
    codes_sql = ",".join(f"'{c}'" for c in codes)

    try:
        df = con.execute(f"""
            SELECT code, time, open, high, low, close, volume
            FROM read_parquet('{layout.s3_1m_date(date_str)}')
            WHERE code IN ({codes_sql})
            ORDER BY code, time
        """).df()
    except Exception as e:
        print(f"  [warn] S3 1m 조회 실패 ({date_str}): {e}")
        return []

    if df.empty:
        return []

    # 종목별 정보 매핑
    info = {}
    for _, row in candidates.iterrows():
        code = str(row["symbol"]).zfill(6)
        info[code] = {
            "pdy_close": float(row["pdy_close"]),
            "upper_limit": float(row["upper_limit"]),
            "name": str(row.get("name", "")),
            "entry_target": float(row["pdy_close"]) * (1.0 + entry_threshold),
        }

    stop_loss = sell_mode.get("stop_loss", STOP_LOSS)
    trail_type = sell_mode.get("trail_type")
    drawdown_pct = sell_mode.get("drawdown", 0.02)
    exit_pct = sell_mode.get("exit_pct", 0.29)

    results = []
    for code, grp in df.groupby("code"):
        code = str(code).zfill(6)
        ci = info.get(code)
        if ci is None:
            continue

        grp = grp.sort_values("time").reset_index(drop=True)
        times = grp["time"].astype(str).str.replace(":", "").str.replace(".", "").str.strip().str.zfill(6).values
        opens = grp["open"].astype(float).values
        highs = grp["high"].astype(float).values
        lows = grp["low"].astype(float).values
        closes = grp["close"].astype(float).values
        volumes = grp["volume"].astype(float).values

        pdy_close = ci["pdy_close"]
        upper_limit = ci["upper_limit"]
        entry_target = ci["entry_target"]

        # Phase 1: 매수 시점 찾기 (최초 entry_target 도달)
        buy_price = 0.0
        buy_time = ""
        buy_idx = -1

        for i in range(len(grp)):
            if times[i] > SELL_TIME:
                break
            if highs[i] >= entry_target:
                # 매수가 = 진입 확인 후 해당 봉 close (보수적 가정)
                buy_price = max(closes[i], entry_target)  # 최소한 entry_target
                buy_time = times[i]
                buy_idx = i
                break

        if buy_price <= 0:
            continue  # 진입 못함

        # Phase 2: 매도 시뮬레이션
        peak_price = buy_price
        sell_price = 0.0
        sell_time = ""
        sell_reason = ""
        reached_upper = False
        reached_upper_time = ""
        stop_loss_price = buy_price * (1.0 - stop_loss)
        exit_price_29 = pdy_close * (1.0 + exit_pct) if trail_type == "upper_limit" else 0.0

        close_1518_price = 0.0

        for i in range(buy_idx + 1, len(grp)):
            t = times[i]
            h = highs[i]
            lo = lows[i]
            c = closes[i]

            if t == SELL_TIME:
                close_1518_price = c

            if t > SELL_TIME:
                break

            # 최고가 갱신
            if h > peak_price:
                peak_price = h

            # 상한가 도달 체크
            if h >= upper_limit and not reached_upper:
                reached_upper = True
                reached_upper_time = t

            # === 매도 조건 체크 ===

            # 1) 손절
            if lo <= stop_loss_price:
                # 상한가 트레일 모드에서 상한가 도달 후에는 손절 대신 exit_pct 적용
                if trail_type == "upper_limit" and reached_upper:
                    pass  # 아래 exit_pct 체크로 넘어감
                else:
                    sell_price = stop_loss_price
                    sell_time = t
                    sell_reason = f"손절 -{stop_loss*100:.0f}%"
                    break

            # 2) 상한가 트레일: 상한가 도달 후 exit_pct 이탈 시 매도
            if trail_type == "upper_limit" and reached_upper:
                if lo <= exit_price_29:
                    sell_price = exit_price_29
                    sell_time = t
                    sell_reason = f"상한가→{exit_pct*100:.0f}%이탈"
                    break

            # 3) 고점 대비 트레일
            if trail_type == "peak_drawdown" and peak_price > 0:
                trail_stop = peak_price * (1.0 - drawdown_pct)
                if lo <= trail_stop:
                    sell_price = trail_stop
                    sell_time = t
                    sell_reason = f"고점대비 -{drawdown_pct*100:.0f}%"
                    break

            # 4) 타임아웃 (진입 후 N분 내 +1% 미도달)
            if TIMEOUT_MINUTES > 0 and buy_idx >= 0:
                minutes_since = i - buy_idx
                if minutes_since >= TIMEOUT_MINUTES:
                    cur_ret = (c / buy_price) - 1.0
                    if cur_ret < 0.01 and sell_price == 0:
                        # 한 번만 체크
                        pct_from_entry = (peak_price / buy_price - 1.0) * 100
                        if pct_from_entry < 1.0:
                            sell_price = c
                            sell_time = t
                            sell_reason = f"{TIMEOUT_MINUTES}분 타임아웃"
                            break

        # 미매도 시 15:18 종가
        if sell_price <= 0:
            if close_1518_price > 0:
                sell_price = close_1518_price
                sell_time = SELL_TIME
            else:
                sell_price = closes[-1]
                sell_time = times[-1]
            sell_reason = "15:18 청산"

        ret = (sell_price / buy_price) - 1.0
        peak_ret = (peak_price / buy_price) - 1.0

        results.append({
            "date": date_str,
            "code": code,
            "name": ci["name"],
            "pdy_close": pdy_close,
            "upper_limit": upper_limit,
            "entry_target": entry_target,
            "buy_price": buy_price,
            "buy_time": buy_time,
            "sell_price": round(sell_price, 0),
            "sell_time": sell_time,
            "sell_reason": sell_reason,
            "peak_price": peak_price,
            "peak_ret": round(peak_ret, 4),
            "reached_upper": reached_upper,
            "reached_upper_time": reached_upper_time,
            "return": round(ret, 4),
        })

    return results


def _print_summary(detail_df: pd.DataFrame, label: str) -> dict:
    """일자별 집계 + 전체 요약 출력"""
    if detail_df.empty:
        print(f"\n  [{label}] 결과 없음")
        return {}

    total = len(detail_df)
    wins = (detail_df["return"] >= 0).sum()
    losses = total - wins
    win_rate = wins / total * 100
    avg_ret = detail_df["return"].mean() * 100
    med_ret = detail_df["return"].median() * 100
    max_ret = detail_df["return"].max() * 100
    min_ret = detail_df["return"].min() * 100
    upper_cnt = detail_df["reached_upper"].sum()
    upper_rate = upper_cnt / total * 100

    # 매도 사유 분포
    reason_counts = detail_df["sell_reason"].value_counts()

    print(f"\n  {'='*60}")
    print(f"  [{label}]")
    print(f"  {'='*60}")
    print(f"  총 거래: {total}건 | 승: {wins} | 패: {losses} | 승률: {win_rate:.1f}%")
    print(f"  평균 수익률: {avg_ret:+.2f}% | 중간값: {med_ret:+.2f}%")
    print(f"  최대: {max_ret:+.2f}% | 최소: {min_ret:+.2f}%")
    print(f"  상한가 도달: {upper_cnt}건 ({upper_rate:.1f}%)")
    print(f"  매도 사유:")
    for reason, cnt in reason_counts.items():
        print(f"    {reason}: {cnt}건 ({cnt/total*100:.1f}%)")

    # 일자별 요약
    daily_rows = []
    for date, grp in detail_df.groupby("date"):
        rets = grp["return"]
        daily_rows.append({
            "날짜": date,
            "종목수": len(grp),
            "승": (rets >= 0).sum(),
            "패": (rets < 0).sum(),
            "상한가도달": int(grp["reached_upper"].sum()),
            "평균수익률": f"{rets.mean()*100:+.2f}%",
        })

    if daily_rows:
        daily_summary = pd.DataFrame(daily_rows)
        print(f"\n  [일자별 요약]")
        print(daily_summary.to_string(index=False))

    return {
        "label": label,
        "total": total, "wins": wins, "losses": losses,
        "win_rate": win_rate, "avg_ret": avg_ret,
        "med_ret": med_ret, "max_ret": max_ret, "min_ret": min_ret,
        "upper_cnt": upper_cnt, "upper_rate": upper_rate,
    }


def main() -> None:
    start_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    print(f"[{start_ts}] 상한가 근접 매수 시뮬레이션 시작")

    parser = argparse.ArgumentParser(description="상한가 근접(25%+) 장중 매수 시뮬레이션")
    parser.add_argument("--start", default=DEFAULT_START, help="시작일 (YYYYMMDD)")
    parser.add_argument("--end", default=DEFAULT_END, help="종료일 (YYYYMMDD)")
    args = parser.parse_args()

    start_date = f"{args.start[:4]}-{args.start[4:6]}-{args.start[6:8]}"
    end_date = f"{args.end[:4]}-{args.end[4:6]}-{args.end[6:8]}"

    layout = load_kis_data_layout()
    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con)
        available_1m_dates = _get_available_1m_dates(con)
        if available_1m_dates:
            print(f"S3 1m 데이터: {len(available_1m_dates)}일 ({min(available_1m_dates)} ~ {max(available_1m_dates)})")

        trading_dates = _get_trading_dates(con, start_date, end_date)
        if not trading_dates:
            print(f"해당 기간({start_date} ~ {end_date})에 거래일이 없습니다.")
            return

        print(f"기간: {start_date} ~ {end_date} (거래일 {len(trading_dates)}일)")

        # === 진입 임계값 × 매도 모드 조합별 시뮬레이션 ===
        all_summaries = []

        for threshold in ENTRY_THRESHOLDS:
            pct = int(threshold * 100)

            # 날짜별 후보 수집
            date_candidates = []
            for date_str in trading_dates:
                if available_1m_dates and date_str not in available_1m_dates:
                    continue
                cands = _find_candidates(con, date_str, threshold)
                if not cands.empty:
                    date_candidates.append((date_str, cands))

            if not date_candidates:
                print(f"\n[진입 +{pct}%] 후보 종목 없음")
                continue

            total_cands = sum(len(c) for _, c in date_candidates)
            print(f"\n{'#'*70}")
            print(f"# 진입 임계값: +{pct}% | 후보: {total_cands}건 ({len(date_candidates)}일)")
            print(f"{'#'*70}")

            for sm in SELL_MODES:
                all_results = []
                for date_str, cands in date_candidates:
                    results = _simulate_intraday(
                        con, layout, date_str, cands, threshold, sm,
                        available_1m_dates=available_1m_dates,
                    )
                    all_results.extend(results)

                if not all_results:
                    print(f"\n  [{sm['name']}] 시뮬레이션 결과 없음")
                    continue

                detail_df = pd.DataFrame(all_results)

                # CSV 저장
                fname = f"uplimit_approach_{pct}pct_{sm['name'].replace(' ', '').replace('/', '_')}.csv"
                detail_path = BASE_DIR / fname
                detail_df.to_csv(detail_path, index=False, encoding="utf-8-sig", float_format="%.4f")
                print(f"\n  [csv] {detail_path.name}")

                summary = _print_summary(detail_df, f"+{pct}% 진입 / {sm['name']}")
                if summary:
                    summary["entry_pct"] = pct
                    summary["sell_mode"] = sm["name"]
                    all_summaries.append(summary)

        # === 전체 조합 비교표 ===
        if all_summaries:
            print(f"\n\n{'='*80}")
            print(f"{'전체 조합 비교':^80}")
            print(f"{'='*80}")

            comp_rows = []
            for s in all_summaries:
                comp_rows.append({
                    "진입": f"+{s['entry_pct']}%",
                    "매도전략": s["sell_mode"],
                    "거래수": s["total"],
                    "승률": f"{s['win_rate']:.1f}%",
                    "평균수익": f"{s['avg_ret']:+.2f}%",
                    "중간값": f"{s['med_ret']:+.2f}%",
                    "최대": f"{s['max_ret']:+.2f}%",
                    "최소": f"{s['min_ret']:+.2f}%",
                    "상한가율": f"{s['upper_rate']:.1f}%",
                })

            comp_df = pd.DataFrame(comp_rows)
            print(comp_df.to_string(index=False))

            # 비교표 CSV 저장
            comp_path = BASE_DIR / "uplimit_approach_comparison.csv"
            comp_df.to_csv(comp_path, index=False, encoding="utf-8-sig")
            print(f"\n[csv] 비교표 저장: {comp_path.name}")

    finally:
        con.close()

    elapsed = time.perf_counter() - t0
    print(f"\n완료 (소요시간: {elapsed:.1f}초)")


if __name__ == "__main__":
    main()
