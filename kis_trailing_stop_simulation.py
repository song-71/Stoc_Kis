"""
1분봉 CSV에서 트레일링 스톱 비율(0.01~0.05) 성과 비교
- 입력 파일: /home/ubuntu/Stoc_Kis/out/{YYYYMMDD}_1m_chart_data.csv
- 저장 파일: /home/ubuntu/Stoc_Kis/out/{YYYYMMDD}_1m_trailing_stop_simulation.csv
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
import pandas as pd

#===========================================================================
# 기본 날짜 설정(YYYYMMDD) : 파일 실행시 argument로 입력하지 않으면 기본 날짜로 설정
default_date = "20260123"

# 트레일링 스톱 비율 설정
trailing_stop_rates = [0.01, 0.02, 0.03, 0.04, 0.05]
#===========================================================================


@dataclass
class TrailingResult:
    code: str
    name: str
    trail: float
    ret: float
    open_price: float
    peak_ret: float
    stop_ret: float
    exit_time: str
    stop_hit: bool


def _read_csv(path: str) -> pd.DataFrame:
    for enc in ("utf-8-sig", "cp949", "utf-8"):
        try:
            df = pd.read_csv(path, encoding=enc)
            # 탭 구분인데 콤마로 읽혀 단일 컬럼만 생긴 경우 재시도
            if df.shape[1] == 1 and df.columns.size == 1 and "\t" in df.columns[0]:
                df = pd.read_csv(path, encoding=enc, sep="\t")
            return df
        except Exception:
            continue
    df = pd.read_csv(path)
    if df.shape[1] == 1 and df.columns.size == 1 and "\t" in df.columns[0]:
        df = pd.read_csv(path, sep="\t")
    return df


def _col_pick(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _normalize_time(series: pd.Series) -> pd.Series:
    return series.astype(str).str.zfill(6)


def _first_valid(series: pd.Series) -> float | None:
    if series is None:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna()
    return float(s.iloc[0]) if not s.empty else None


def _last_valid(series: pd.Series) -> float | None:
    if series is None:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna()
    return float(s.iloc[-1]) if not s.empty else None


def _to_float_series(df: pd.DataFrame, col: str | None) -> pd.Series | None:
    if not col or col not in df.columns:
        return None
    return pd.to_numeric(df[col], errors="coerce")


def _simulate_trailing(
    df: pd.DataFrame,
    code_col: str,
    name_col: str | None,
    time_col: str,
    open_col: str | None,
    high_col: str | None,
    low_col: str | None,
    price_col: str | None,
    trail: float,
) -> list[TrailingResult]:
    results: list[TrailingResult] = []
    for code, g in df.groupby(code_col):
        g = g.copy()
        g[time_col] = _normalize_time(g[time_col])
        g = g.sort_values(time_col).reset_index(drop=True)
        name = g[name_col].iloc[0] if name_col and name_col in g.columns else ""

        open_px = _first_valid(_to_float_series(g, open_col))
        if open_px is None:
            open_px = _first_valid(_to_float_series(g, price_col))
        if open_px is None:
            open_px = _first_valid(_to_float_series(g, high_col))
        if open_px is None:
            open_px = _first_valid(_to_float_series(g, low_col))
        if open_px is None or open_px == 0:
            results.append(
                TrailingResult(code=code, name=name, trail=trail, ret=0.0, exit_time="", stop_hit=False)
            )
            continue

        peak_ret = 0.0
        stop_ret = -trail
        exit_ret = None
        exit_time = ""
        stop_hit = False

        high_series = _to_float_series(g, high_col)
        if high_series is None:
            high_series = _to_float_series(g, price_col)
        if high_series is None:
            high_series = _to_float_series(g, open_col)

        low_series = _to_float_series(g, low_col)
        if low_series is None:
            low_series = _to_float_series(g, price_col)
        if low_series is None:
            low_series = _to_float_series(g, open_col)

        for idx, row in g.iterrows():
            hi = high_series.iloc[idx] if high_series is not None else None
            lo = low_series.iloc[idx] if low_series is not None else None
            if pd.isna(hi):
                hi = None
            if pd.isna(lo):
                lo = None

            if hi is not None:
                peak_ret = max(peak_ret, (hi / open_px) - 1.0)
                stop_ret = peak_ret - trail

            if lo is not None and (lo / open_px) - 1.0 <= stop_ret:
                exit_ret = stop_ret
                exit_time = row[time_col]
                stop_hit = True
                break

        if exit_ret is None:
            last_px = _last_valid(_to_float_series(g, price_col))
            if last_px is None:
                last_px = _last_valid(_to_float_series(g, open_col))
            if last_px is None:
                last_px = _last_valid(_to_float_series(g, high_col))
            if last_px is None:
                last_px = _last_valid(_to_float_series(g, low_col))
            exit_ret = (last_px / open_px) - 1.0 if last_px else 0.0
            exit_time = g[time_col].iloc[-1]

        results.append(
            TrailingResult(
                code=code,
                name=name,
                trail=trail,
                ret=exit_ret,
                open_price=open_px,
                peak_ret=peak_ret,
                stop_ret=stop_ret,
                exit_time=exit_time,
                stop_hit=stop_hit,
            )
        )

    return results


def main() -> None:
    ap = argparse.ArgumentParser(description="1분봉 CSV 트레일링 스톱 비율 탐색")
    ap.add_argument("--date", default=default_date, help="대상 일자 (YYYYMMDD)")
    ap.add_argument("--csv", default=None, help="입력 CSV 경로(선택)")
    ap.add_argument("--out", default=None, help="결과 CSV 저장 경로(선택)")
    args = ap.parse_args()

    csv_path = args.csv
    if not csv_path:
        if not args.date:
            print("입력 CSV(--csv) 또는 대상 일자(--date)가 필요합니다.")
            return
        out_dir = Path(__file__).resolve().parent / "out"
        csv_path = str(out_dir / f"{args.date}_1m_chart_data.csv")

    df = _read_csv(csv_path)
    if df.empty:
        print("입력 CSV가 비어 있습니다.")
        return

    code_col = _col_pick(df, ["code", "종목코드", "pdno"])
    time_col = _col_pick(df, ["체결시간", "time", "timestamp"])
    name_col = _col_pick(df, ["name", "종목명", "prdt_name"])
    open_col = _col_pick(df, ["시가", "open", "stck_oprc"])
    high_col = _col_pick(df, ["최고가", "high", "stck_hgpr"])
    low_col = _col_pick(df, ["최저가", "low", "stck_lwpr"])
    price_col = _col_pick(df, ["현재가", "close", "stck_prpr"])

    if not code_col or not time_col:
        print("필수 컬럼(code/체결시간)을 찾을 수 없습니다.")
        print(f"현재 컬럼: {list(df.columns)}")
        return

    all_rows: list[dict] = []
    biz_date = args.date or ""

    for trail in trailing_stop_rates:
        results = _simulate_trailing(
            df=df,
            code_col=code_col,
            name_col=name_col,
            time_col=time_col,
            open_col=open_col,
            high_col=high_col,
            low_col=low_col,
            price_col=price_col,
            trail=trail,
        )
        for r in results:
            all_rows.append(
                {
                    "date": biz_date,
                    "trail": r.trail,
                    "code": r.code,
                    "name": r.name,
                    "open_price": r.open_price,
                    "peak_ret": r.peak_ret,
                    "stop_ret": r.stop_ret,
                    "ret": r.ret,
                    "target_time": r.exit_time,
                    "stop_hit": r.stop_hit,
                }
            )

    res_df = pd.DataFrame(all_rows)
    cols = [
        "date",
        "code",
        "name",
        "trail",
        "open_price",
        "peak_ret",
        "stop_ret",
        "ret",
        "target_time",
        "stop_hit",
    ]
    print("\n[상세] 종목별 트레일링 결과")
    print(res_df[cols])

    summary = (
        res_df.groupby("trail")
        .agg(avg_ret=("ret", "mean"), count=("ret", "size"), stop_hit=("stop_hit", "sum"))
        .reset_index()
        .sort_values("trail")
    )

    print("\n[요약] trail별 평균 수익률")
    print(summary)

    out_path = args.out
    if not out_path and biz_date:
        out_dir = Path(__file__).resolve().parent / "out"
        out_path = str(out_dir / f"{biz_date}_1m_trailing_stop_simulation.csv")
    if out_path:
        res_df[cols].to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[저장] 상세 결과: {out_path}")


if __name__ == "__main__":
    main()
