"""
전일 등락률(pdy_ctrt) 조건으로 종목을 선택해 CSV로 저장합니다.

저장 위치: /home/ubuntu/Stoc_Kis/symulation/Select_Tr_target_list.csv
최종 출력 컬럼: date, symbol, name, p2dy_close, pdy_close, open, high, low, close,
              volume, value, tdy_ctrt, tdy_h_ctrt, tdy_L_ctrt, pdy_ctrt,
              pdy_uc, pdy_h, pdy_L, R_avr, R_org
"""
period_start = "2026-01-30"  # 시작일 (YYYY-MM-DD)
period_end = ""    # 종료일 (YYYY-MM-DD), 비우면 오늘 날짜까지
strategy_name = "str3"  # "str1" 또는 "str2", str3
target_pdy_ctrt = 0.20   # str1의 전일 등락률 필터 기준값

# 전략 설명
# str1: 전일 등락률(pdy_ctrt)이 target_pdy_ctrt 이상인 종목만 선택
# str2: 당일 저가(low)가 전일 종가(pdy_close) 이상이고, 전일 종가가 500원 이상인 종목만 선택
# str3: ★★ 당일 등락률(tdy_ctrt)이 target_pdy_ctrt 이상이고, 전일 종가가 500원 이상인 종목만 선택 => 당일 상한가 종목을 다음날 적용대상 선정
# str4: 당일 시가가 전일 종가보다 높은 갭상승인 종목만 선택 
# str5: period_start 날짜 1m데이터에서 09:30 tdy_ret > 0.05 종목 선택


# 컬럼 의미(한글 설명)
# pdy_uc: 전일 상한가 여부(전일 종가 기준)
# pdy_close: 전일 종가
# p2dy_close: 전전일 종가
# pdy_ctrt: 전일 등락율
# tdy_ctrt: 당일 등락율: (당일종가 / 전일종가) - 1
# tdy_h_ctrt: 당일고가비율: (high / pdy_close) - 1
# tdy_L_ctrt: 당일저가비율: (low / pdy_close) - 1


import os
import sys
import time
from datetime import datetime
from pathlib import Path
import polars as pl


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from kis_utils import KRX_code_batch


def _exclude_delisting_58(df: pl.DataFrame) -> pl.DataFrame:
    """iscd_stat_cls_code=58(상장폐지 예정) 종목 제외. utils.KRX_code_batch로 일괄 검색."""
    try:
        symbols = [str(s).strip().zfill(6) for s in df["symbol"].unique().to_list()]
        status_map = KRX_code_batch(symbols)
        exclude = [s for s in symbols if status_map.get(s) == "58"]
        if not exclude:
            return df
        return df.filter(
            ~pl.col("symbol").cast(pl.Utf8).str.zfill(6).is_in(exclude)
        )
    except Exception:
        return df


def _apply_date_filter(lf: pl.LazyFrame) -> pl.LazyFrame:
    try:
        start_dt = datetime.strptime(period_start, "%Y-%m-%d").date()
        if period_end:
            end_dt = datetime.strptime(period_end, "%Y-%m-%d").date()
        else:
            end_dt = datetime.now().date()
    except Exception:
        return lf
    return lf.filter(
        (pl.col("date") >= pl.lit(start_dt)) & (pl.col("date") <= pl.lit(end_dt))
    )


def _select_str1(df: pl.DataFrame) -> pl.DataFrame:
    if "pdy_ctrt" not in df.columns:
        return df.head(0)
    return df.filter(pl.col("pdy_ctrt") >= target_pdy_ctrt)


def _select_str2(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(
        (pl.col("low") >= pl.col("pdy_close")) & (pl.col("pdy_close") >= 500)
    )


def _select_str3(df: pl.DataFrame) -> pl.DataFrame:
    if "pdy_close" not in df.columns or "tdy_ctrt" not in df.columns:
        return df.head(0)
    return df.filter(
        (pl.col("tdy_ctrt") >= target_pdy_ctrt)
        & (pl.col("pdy_close") >= 500)
    )


def _select_str4(df: pl.DataFrame) -> pl.DataFrame:
    if "open" not in df.columns or "pdy_close" not in df.columns:
        return df.head(0)
    return df.filter(pl.col("open") > pl.col("pdy_close"))


def _resolve_1m_parquet_path(biz_date: str) -> str:
    out_dir = Path(BASE_DIR) / "data" / "1m_data"
    matches = sorted(out_dir.glob(f"{biz_date}*_1m_chart_DB_parquet.parquet"))
    if not matches:
        raise FileNotFoundError(
            f"parquet 파일이 없습니다: {out_dir}/{biz_date}*_1m_chart_DB_parquet.parquet"
        )
    return str(matches[-1])


def _select_str5(df: pl.DataFrame, period_start_override: str | None = None) -> pl.DataFrame:
    pstart = period_start_override or period_start
    if not pstart:
        return df.head(0)
    try:
        date_key = datetime.strptime(pstart, "%Y-%m-%d").strftime("%Y%m%d")
    except Exception:
        return df.head(0)

    parquet_1m = _resolve_1m_parquet_path(date_key)
    lf_1m = pl.scan_parquet(parquet_1m)
    cols = lf_1m.collect_schema().names()
    code_col = "code" if "code" in cols else "symbol" if "symbol" in cols else None
    if code_col is None or "close" not in cols or "pdy_close" not in cols or "time" not in cols:
        return df.head(0)

    select_cols = [code_col, "date", "time", "close", "pdy_close", "tdy_ret"]
    lf_1m = lf_1m.select(select_cols)
    lf_1m = lf_1m.with_columns(
        [
            pl.col("date").cast(pl.Utf8).str.replace_all("-", "").alias("date"),
            pl.col("time").cast(pl.Utf8).str.zfill(6).alias("time"),
            pl.col("close").cast(pl.Float64, strict=False).alias("close"),
            pl.col("pdy_close").cast(pl.Float64, strict=False).alias("pdy_close"),
        ]
    )
    target = (
        lf_1m.filter((pl.col("date") == date_key) & (pl.col("time") == "093000"))
        .with_columns(
            pl.when(pl.col("tdy_ret").is_not_null())
            .then(pl.col("tdy_ret").cast(pl.Float64, strict=False))
            .when(pl.col("pdy_close") > 0)
            .then(pl.col("close") / pl.col("pdy_close") - 1)
            .otherwise(None)
            .alias("tdy_ret")
        )
        .filter(pl.col("tdy_ret") > 0.05)
        .select(
            [
                pl.col(code_col).cast(pl.Utf8).alias("symbol"),
                pl.col("tdy_ret").alias("0930_tdy_ret"),
            ]
        )
        .unique()
        .collect()
    )
    if target.height == 0:
        return df.head(0)
    filtered = df.filter(
        (pl.col("date") == pl.lit(datetime.strptime(pstart, "%Y-%m-%d").date()))
        & (pl.col("symbol").is_in(target["symbol"]))
    )
    return filtered.join(target, on="symbol", how="left")


def _select_by_strategy(df: pl.DataFrame) -> pl.DataFrame:
    if strategy_name == "str2":
        return _select_str2(df)
    if strategy_name == "str3":
        return _select_str3(df)
    if strategy_name == "str4":
        return _select_str4(df)
    if strategy_name == "str5":
        return _select_str5(df)
    return _select_str1(df)


def _select_by_strategy_param(
    df: pl.DataFrame,
    strat: str,
    target_ctrt: float,
    period_start_override: str | None = None,
) -> pl.DataFrame:
    """전략별 선정 (파라미터 버전, ws_realtime 호출용)."""
    if strat == "str2":
        return _select_str2(df)
    if strat == "str3":
        if "pdy_close" not in df.columns or "tdy_ctrt" not in df.columns:
            return df.head(0)
        return df.filter(
            (pl.col("tdy_ctrt") >= target_ctrt) & (pl.col("pdy_close") >= 500)
        )
    if strat == "str4":
        return _select_str4(df)
    if strat == "str5":
        return _select_str5(df, period_start_override)
    if strat == "str1":
        if "pdy_ctrt" not in df.columns:
            return df.head(0)
        return df.filter(pl.col("pdy_ctrt") >= target_ctrt)
    return _select_str1(df)


def _apply_date_filter_param(
    lf: pl.LazyFrame, start: str, end: str
) -> pl.LazyFrame:
    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d").date()
        end_dt = (
            datetime.strptime(end, "%Y-%m-%d").date()
            if end
            else datetime.now().date()
        )
    except Exception:
        return lf
    return lf.filter(
        (pl.col("date") >= pl.lit(start_dt)) & (pl.col("date") <= pl.lit(end_dt))
    )


def get_closing_buy_candidates(
    period_start: str,
    period_end: str,
    strategy_name_param: str,
    target_pdy_ctrt_param: float,
    parquet_path: str | None = None,
) -> tuple[list[str], dict[str, dict]]:
    """
    종가매수 후보 선정. Select_Tr_target_list와 동일 조건 (58 제외, str1~5).
    반환: (codes, code_info) code_info[code] = {prev_close, name}
    ws_realtime_subscribe_to_DB-1.py 15:19 호출용.
    """
    base = Path(__file__).resolve().parent.parent
    path = parquet_path or str(base / "data" / "1d_data" / "kis_1d_unified_parquet_DB.parquet")
    if not os.path.exists(path):
        return [], {}

    columns = [
        "date", "symbol", "name", "open", "high", "low", "close",
        "volume", "value", "pdy_close", "p2dy_close", "tdy_ctrt", "pdy_ctrt",
        "pdy_uc", "pdy_h", "pdy_L", "atr3", "atr", "vema3", "vema20",
    ]
    select_cols = [c for c in columns if c not in ("pdy_volume", "pdy_vema3", "pdy_vema20")]
    lf = pl.scan_parquet(path).select(select_cols)
    lf = lf.with_columns(pl.col("date").cast(pl.Date, strict=False))
    lf = lf.sort(["symbol", "date"])
    lf = lf.with_columns(
        [
            pl.col("volume").shift(1).over("symbol").alias("pdy_volume"),
            pl.col("vema3").shift(1).over("symbol").alias("pdy_vema3"),
            pl.col("vema20").shift(1).over("symbol").alias("pdy_vema20"),
        ]
    )
    lf = _apply_date_filter_param(lf, period_start, period_end)
    df = lf.sort(["symbol", "date"]).collect()
    df = _exclude_delisting_58(df)
    if "pdy_ctrt" in df.columns:
        df = df.with_columns(pl.col("pdy_ctrt").cast(pl.Float64, strict=False))
    if "tdy_ctrt" in df.columns:
        df = df.with_columns(pl.col("tdy_ctrt").cast(pl.Float64, strict=False))

    detail = _select_by_strategy_param(
        df, strategy_name_param, target_pdy_ctrt_param, period_start
    )
    if detail.height == 0:
        return [], {}

    # str3/str1: 가장 최근 거래일 1일치만 사용
    # (기간 누적 시 수주 전 상한가 종목이 오늘 하락 중에도 선정되는 버그 방지)
    if strategy_name_param in ("str3", "str1", "str2", "str4"):
        if "date" in detail.columns and detail.height > 0:
            latest_date = detail["date"].max()
            detail = detail.filter(pl.col("date") == latest_date)
        if detail.height == 0:
            return [], {}

    if "0930_tdy_ret" in detail.columns:
        detail = detail.sort(
            ["0930_tdy_ret", "date", "symbol"],
            descending=[True, False, False],
        )
    else:
        detail = detail.sort(["date", "symbol"])

    codes: list[str] = []
    code_info: dict[str, dict] = {}
    for row in detail.iter_rows(named=True):
        c = str(row.get("symbol", "")).strip().zfill(6)
        if not c or c in {x for x in codes}:
            continue
        pclose = row.get("pdy_close")
        try:
            prev_close = float(pclose) if pclose is not None else 0.0
        except (TypeError, ValueError):
            prev_close = 0.0
        name = str(row.get("name", c) or "").strip()
        # 종목명이 없거나 코드와 같은 경우(조회 실패) 제외
        if not name or name == c:
            continue
        codes.append(c)
        code_info[c] = {"prev_close": prev_close, "name": name}

    return codes, code_info


def main() -> None:
    t0 = time.perf_counter()
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_cols(20)
    pl.Config.set_fmt_str_lengths(120)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parquet_path = os.path.join(os.path.dirname(base_dir), "data", "1d_data", "kis_1d_unified_parquet_DB.parquet")
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"parquet 파일이 없습니다: {parquet_path}")

    columns = [
        "date",
        "symbol",
        "name",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "pdy_close",
        "p2dy_close",
        "tdy_ctrt",
        "pdy_ctrt",
        "pdy_uc",
        "pdy_h",
        "pdy_L",
        "atr3",
        "atr",
        "vema3",
        "vema20",
    ]
    select_cols = [c for c in columns if c not in ("pdy_volume", "pdy_vema3", "pdy_vema20")]
    lf = pl.scan_parquet(parquet_path).select(select_cols)
    lf = lf.with_columns(pl.col("date").cast(pl.Date, strict=False))
    # 전일 volume, vema3, vema20 (필터 전에 shift 해야 전일값 존재)
    lf = lf.sort(["symbol", "date"])
    lf = lf.with_columns(
        [
            pl.col("volume").shift(1).over("symbol").alias("pdy_volume"),
            pl.col("vema3").shift(1).over("symbol").alias("pdy_vema3"),
            pl.col("vema20").shift(1).over("symbol").alias("pdy_vema20"),
        ]
    )
    lf = _apply_date_filter(lf)
    df = lf.sort(["symbol", "date"]).collect()
    df = _exclude_delisting_58(df)
    if "pdy_ctrt" in df.columns:
        df = df.with_columns(pl.col("pdy_ctrt").cast(pl.Float64, strict=False))
    if "tdy_ctrt" in df.columns:
        df = df.with_columns(pl.col("tdy_ctrt").cast(pl.Float64, strict=False))

    detail = _select_by_strategy(df)
    if detail.height == 0:
        print("조건에 맞는 데이터가 없습니다.")
        return

    detail = detail.with_columns(
        [
            pl.col("date").dt.strftime("%Y-%m-%d").alias("date"),
            pl.when(pl.col("pdy_close") > 0)
            .then((pl.col("high") / pl.col("pdy_close")) - 1)
            .otherwise(None)
            .alias("tdy_h_ctrt"),
            pl.when(pl.col("pdy_close") > 0)
            .then((pl.col("low") / pl.col("pdy_close")) - 1)
            .otherwise(None)
            .alias("tdy_L_ctrt"),
        ]
    )
    output_cols = [
        "date",
        "symbol",
        "name",
        "p2dy_close",
        "pdy_close",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "tdy_h_ctrt",
        "tdy_L_ctrt",
        "tdy_ctrt",
        "pdy_ctrt",
        "pdy_uc",
        "pdy_h",
        "pdy_L",
        "pdy_volume",
        "pdy_vema3",
        "pdy_vema20",
        "atr3",
        "atr",
        "0930_tdy_ret",
    ]
    output_cols = [c for c in output_cols if c in detail.columns]
    if "0930_tdy_ret" in detail.columns:
        detail = detail.sort(["0930_tdy_ret", "date", "symbol"], descending=[True, False, False])
    else:
        detail = detail.sort(["date", "symbol"])
    detail = detail.select(output_cols)

    print(f"\n[선정 결과] rows={detail.height}")
    print(detail)

    csv_path = os.path.join(base_dir, "Select_Tr_target_list.csv")
    detail.write_csv(csv_path, separator=",", quote_style="necessary", include_header=True)
    try:
        with open(csv_path, "rb") as f:
            data = f.read()
        with open(csv_path, "wb") as f:
            f.write(b"\xef\xbb\xbf" + data)
        print(f"[csv] saved with BOM: {csv_path} rows={detail.height}")
    except Exception:
        print(f"[csv] saved: {csv_path} rows={detail.height}")
    elapsed = time.perf_counter() - t0
    print(f"[작업시간] seconds={elapsed:.3f}")


if __name__ == "__main__":
    main()
