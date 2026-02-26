"""
지정기간 1d_통합_Parquet_DB 파일을 Polars로 읽어 화면 출력.
- CODE_TEXT에 종목코드/종목명 지정 시 해당 종목만 조회 (비우면 전종목, 느릴 수 있음)
- columns: parquet에 atr3 사용 (R_avr 대신)
"""

# 조회 옵션
period_start = "2026-02-20"  # 미지정시 처음부터
period_end = ""  # 미지정시 오늘까지
print_all_rows = 0  # 1=전체행 정렬 출력, 0=print(df)

# 종목 필터 (비우면 전종목·느림, 지정 시 해당 종목만 조회·빠름)
# 예: "005930\n유투바이오\n221800" (삼성전자, 유투바이오 2종목만)
#260220 수신대상 종목(업종) 목록 : 26개
CODE_TEXT = """
대한방직
SG세계물산
대원전선
대원전선우
KBI메탈
이지홀딩스
에이치엠넥스
나노엔텍
현대바이오
아모텍
시지메드텍
우리손에프앤지
텔코웨어
미래에셋생명
옵투스제약
엔지켐생명과학
현대ADM
유투바이오
엠디바이스
DSC인베스트먼트

"""


import os
import re
import sys
import polars as pl
import pandas as pd
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from kis_utils import load_symbol_master_cached

# 출력 컬럼 (parquet: atr3 사용)
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
    "p2dy_ctrt",
    "pdy_uc",
    "pdy_h",
    "pdy_L",
    "pdy_volume",
    "pdy_vema3",
    "pdy_vema20",
    "atr3",
    "atr",
    "vema3",
    "vema20",
]

# 추가 필터(원하면 사용)
target_pdy_ctrt = None  # 예: 0.29

# 저장 옵션
save_csv = True  # True면 symulation/temp.csv 로 저장


def _resolve_parquet_path() -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parquet_path = os.path.join(os.path.dirname(base_dir), "data", "1d_data", "kis_1d_unified_parquet_DB.parquet")
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"parquet 파일이 없습니다: {parquet_path}")
    return parquet_path


def _apply_date_filter(df: pl.DataFrame) -> pl.DataFrame:
    start_dt = None
    end_dt = None

    if period_start:
        start_dt = datetime.strptime(period_start, "%Y-%m-%d").date()
    if period_end:
        end_dt = datetime.strptime(period_end, "%Y-%m-%d").date()
    else:
        end_dt = datetime.now().date()

    if start_dt and end_dt:
        return df.filter(
            (pl.col("date") >= pl.lit(start_dt)) & (pl.col("date") <= pl.lit(end_dt))
        )
    if start_dt:
        return df.filter(pl.col("date") >= pl.lit(start_dt))
    if end_dt:
        return df.filter(pl.col("date") <= pl.lit(end_dt))
    return df


def _parse_code_text(text: str) -> list[str]:
    # CODE_TEXT에서 종목코드/종목명 추출, 6자리 코드 리스트 반환
    tokens = re.split(r"[\s,]+", text.strip())
    tokens = [t.strip() for t in tokens if t.strip()]
    if not tokens:
        return []
    codes = []
    master = load_symbol_master_cached()
    name_to_code = dict(zip(master["name"].astype(str), master["code"].astype(str).str.zfill(6)))
    for t in tokens:
        if t.isdigit():
            codes.append(t.zfill(6))
        else:
            c = name_to_code.get(t) or name_to_code.get(t.upper())
            if c:
                codes.append(c)
    return list(dict.fromkeys(codes))  # 순서 유지, 중복 제거


def main() -> None:
    parquet_path = _resolve_parquet_path()
    df = pl.read_parquet(parquet_path)
    if df.schema.get("date") == pl.Utf8:
        df = df.with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False))

    # 전일 volume, vema3, vema20 (필터 전에 shift 해야 전일값 존재)
    df = df.sort(["symbol", "date"])
    for src, dst in [("volume", "pdy_volume"), ("vema3", "pdy_vema3"), ("vema20", "pdy_vema20")]:
        if src in df.columns:
            df = df.with_columns(
                pl.col(src).shift(1).over("symbol").alias(dst)
            )

    df = _apply_date_filter(df)
    if target_pdy_ctrt is not None and "pdy_ctrt" in df.columns:
        df = df.filter(pl.col("pdy_ctrt") >= float(target_pdy_ctrt))

    # CODE_TEXT 지정 시 해당 종목만 필터 (전종목 출력 방지)
    filter_codes = _parse_code_text(CODE_TEXT)
    if filter_codes:
        df = df.filter(pl.col("symbol").cast(pl.Utf8).str.zfill(6).is_in(filter_codes))

    # 1d parquet에 name 이미 포함, 별도 조회 불필요
    want_cols = [c for c in columns if c in df.columns]
    if want_cols:
        df = df.select(want_cols)
    df = df.sort(["symbol", "date"])
    if print_all_rows:
        pd.set_option("display.colheader_justify", "right")
        print(df.to_pandas().to_string(index=False, justify="right"))
    else:
        print(df)
    print(df.columns)

    if save_csv:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(base_dir, "temp.csv")
        df.write_csv(csv_path, separator=",", quote_style="necessary", include_header=True)
        try:
            with open(csv_path, "rb") as f:
                data = f.read()
            with open(csv_path, "wb") as f:
                f.write(b"\xef\xbb\xbf" + data)
            print(f"[csv] saved with BOM: {csv_path} rows={df.height}")
        except Exception:
            print(f"[csv] saved: {csv_path} rows={df.height}")


if __name__ == "__main__":
    main()
