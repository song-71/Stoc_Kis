"""
1d + 1m Parquet DB 조회 (DuckDB 사용)
  - 종목 검색 시: 1d (시작일~종료일) 먼저 출력 → 1m (종료일) 출력
  - 저장: temp_1d.csv, temp_1m.csv
"""
# 조회 옵션 (내부 설정)
d1_start_date = "20260210"   # 1d 조회 시작일
target_date = "20260224"     # 1d 조회 종료일 + 1m 조회일
print_all_rows = 0           # 1이면 전체행 출력, 0이면 요약 출력(중간 생략)
default_save_csv_name_1d = "temp_1d.csv"
default_save_csv_name_1m = "temp_1m.csv"

CODE_TEXT = """
DSC인베스트먼트
"""


import argparse
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
sys.path.append(str(Path(__file__).resolve().parents[1]))
from kis_utils import load_symbol_master, print_table


def _normalize_date(text: str | None, default: str) -> str:
    if not text:
        return default
    cleaned = str(text).strip()
    if "-" in cleaned:
        return datetime.strptime(cleaned, "%Y-%m-%d").strftime("%Y-%m-%d")
    if len(cleaned) == 8 and cleaned.isdigit():
        return datetime.strptime(cleaned, "%Y%m%d").strftime("%Y-%m-%d")
    return default


def _resolve_1d_parquet_path() -> str:
    base_dir = Path(__file__).resolve().parent
    path = base_dir.parent / "data" / "1d_data" / "kis_1d_unified_parquet_DB.parquet"
    if not path.exists():
        raise FileNotFoundError(f"1d parquet 파일이 없습니다: {path}")
    return str(path)


def _resolve_1m_parquet_path(biz_date: str) -> str:
    base_dir = Path(__file__).resolve().parent
    out_dir = base_dir.parent / "data" / "1m_data"
    if not out_dir.exists():
        raise FileNotFoundError(f"1m_data 폴더가 없습니다: {out_dir}")
    matches = sorted(out_dir.glob(f"{biz_date}*_1m_chart_DB_parquet.parquet"))
    if not matches:
        raise FileNotFoundError(f"1m parquet 파일이 없습니다: {out_dir}/{biz_date}*")
    return str(matches[0])


def _build_name_code_map() -> tuple[dict[str, str], dict[str, str]]:
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"]))
        code_to_name = dict(zip(sdf["code"], sdf["name"].astype(str)))
        return name_to_code, code_to_name
    except Exception:
        return {}, {}


def _parse_codes(text: str, name_to_code: dict[str, str]) -> list[str]:
    codes = []
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        if raw.isdigit():
            codes.append(raw.zfill(6))
            continue
        code = name_to_code.get(raw)
        if code:
            codes.append(code)
    return codes


def _parse_input_line(text: str, name_to_code: dict[str, str]) -> list[str]:
    """프롬프트 입력 한 줄에서 종목명/코드 파싱 (쉼표·공백 구분)"""
    codes = []
    for part in text.replace(",", " ").split():
        raw = part.strip()
        if not raw:
            continue
        if raw.isdigit():
            codes.append(raw.zfill(6))
        else:
            c = name_to_code.get(raw)
            if c:
                codes.append(c)
    return codes


def _print_and_save_df(
    df: pd.DataFrame,
    out_path: Path,
    label: str,
) -> None:
    """DataFrame 출력 및 CSV 저장"""
    if df.empty:
        print(f"[{label}] 결과 없음")
        return

    columns = list(df.columns)
    num_cols = set(df.select_dtypes(include="number").columns)
    align = {c: "right" if c in num_cols else "left" for c in columns}
    rows_dict = df.fillna("").astype(str).to_dict("records")

    if print_all_rows:
        print_table(rows_dict, columns, align)
    else:
        print_table(rows_dict, columns, align, max_rows=20)

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[csv] saved: {out_path}")


def _run_1d_query_and_display(
    parquet_path: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    code_to_name: dict[str, str],
) -> None:
    """1d: start_date ~ end_date 조회"""
    if not codes:
        print("[1d] 종목 코드 없음")
        return

    codes_sql = ",".join(f"'{c}'" for c in codes)
    start_norm = _normalize_date(start_date, start_date)
    end_norm = _normalize_date(end_date, end_date)
    if len(start_norm) == 8 and "-" not in start_norm:
        start_norm = datetime.strptime(start_norm, "%Y%m%d").strftime("%Y-%m-%d")
    if len(end_norm) == 8 and "-" not in end_norm:
        end_norm = datetime.strptime(end_norm, "%Y%m%d").strftime("%Y-%m-%d")

    query = f"""
    SELECT date, symbol, name, open, high, low, close, volume, pdy_close, tdy_ctrt, pdy_ctrt, p2dy_ctrt
    FROM read_parquet('{parquet_path}')
    WHERE symbol IN ({codes_sql}) AND date BETWEEN '{start_norm}' AND '{end_norm}'
    ORDER BY symbol, date
    """
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
    finally:
        con.close()

    if not df.empty:
        # tdy_hg_ctrt = high/pdy_close - 1 (tdy_ctrt 다음에 배치)
        pdc = pd.to_numeric(df["pdy_close"], errors="coerce")
        high = pd.to_numeric(df["high"], errors="coerce")
        df["tdy_hg_ctrt"] = (high / pdc - 1).where(pdc > 0, pd.NA)
        ord_cols = ["date", "symbol", "name", "open", "high", "low", "close", "volume", "pdy_close", "tdy_ctrt", "tdy_hg_ctrt", "pdy_ctrt", "p2dy_ctrt"]
        df = df[[c for c in ord_cols if c in df.columns]]

        # 정수: open, high, low, close, volume, pdy_close
        for col in ["open", "high", "low", "close", "volume", "pdy_close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        # 소수점 4자리: tdy_ctrt, tdy_hg_ctrt, pdy_ctrt, p2dy_ctrt
        for col in ["tdy_ctrt", "tdy_hg_ctrt", "pdy_ctrt", "p2dy_ctrt"]:
            if col in df.columns:
                df[col] = df[col].map(lambda x: f"{x:.4f}" if pd.notna(x) else "")

    base_dir = Path(__file__).resolve().parent
    out_path = base_dir / default_save_csv_name_1d
    _print_and_save_df(df, out_path, f"1d ({start_date}~{end_date})")


def _run_1m_query_and_display(
    parquet_path: str,
    biz_date: str,
    codes: list[str],
    code_to_name: dict[str, str],
) -> None:
    """1m: target_date 조회"""
    if not codes:
        print("[1m] 종목 코드 없음")
        return

    codes_sql = ",".join(f"'{c}'" for c in codes)
    date_filter = f"date = '{biz_date}'"
    code_filter = f"WHERE code IN ({codes_sql}) AND {date_filter}"

    query = f"""
    SELECT date, time, code, name, open, high, low, close, volume, acml_value, tdy_ret, pdy_close, pdy_ret
    FROM read_parquet('{parquet_path}')
    {code_filter}
    ORDER BY code, date, time
    """
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
    finally:
        con.close()

    if df.empty:
        print(f"[1m] 결과 없음")
        return

    if "name" in df.columns and code_to_name:
        code_name = df["code"].map(code_to_name)
        blank_name = df["name"].isna() | df["name"].astype(str).str.strip().eq("")
        df.loc[blank_name, "name"] = code_name
        df["name"] = df["name"].fillna("")

    # tdy_hg_ctrt = high/pdy_close - 1 (tdy_ret 다음에 배치)
    pdc = pd.to_numeric(df["pdy_close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    df["tdy_hg_ctrt"] = (high / pdc - 1).where(pdc > 0, pd.NA)
    ord_cols = ["date", "time", "code", "name", "open", "high", "low", "close", "volume", "acml_value", "tdy_ret", "tdy_hg_ctrt", "pdy_close", "pdy_ret"]
    df = df[[c for c in ord_cols if c in df.columns]]

    for col in ["tdy_ret", "tdy_hg_ctrt", "pdy_ret"]:
        if col in df.columns:
            df[col] = df[col].map(lambda x: f"{x:.4f}" if pd.notna(x) else "")

    base_dir = Path(__file__).resolve().parent
    out_path = base_dir / default_save_csv_name_1m
    _print_and_save_df(df, out_path, f"1m ({biz_date})")


def _search_stock(
    codes: list[str],
    path_1d: str,
    path_1m: str,
    start_date: str,
    end_date: str,
    biz_date_1m: str,
    name_to_code: dict[str, str],
    code_to_name: dict[str, str],
) -> None:
    """종목 코드로 1d → 1m 순서로 조회"""
    if not codes:
        return
    print(f"\n{'─' * 50}")
    print(f" [1d] {start_date} ~ {end_date}")
    print(f"{'─' * 50}")
    _run_1d_query_and_display(path_1d, start_date, end_date, codes, code_to_name)
    print(f"\n{'─' * 50}")
    print(f" [1m] {biz_date_1m}")
    print(f"{'─' * 50}")
    _run_1m_query_and_display(path_1m, biz_date_1m, codes, code_to_name)


def main() -> None:
    parser = argparse.ArgumentParser(description="1d + 1m parquet 조회")
    parser.add_argument("--start", dest="start_date", default=d1_start_date)
    parser.add_argument("--end", dest="end_date", default=target_date)
    args = parser.parse_args()

    end_arg = args.end_date or target_date
    start_norm = _normalize_date(args.start_date, d1_start_date)
    end_norm = _normalize_date(end_arg, target_date)
    # 1m parquet 파일명/date 컬럼은 YYYYMMDD
    biz_date_1m = end_arg.strip().replace("-", "")[:8] if end_arg else target_date

    path_1d = _resolve_1d_parquet_path()
    path_1m = _resolve_1m_parquet_path(biz_date_1m)

    name_to_code, code_to_name = _build_name_code_map()

    print(f"[1d parquet] {path_1d}")
    print(f"[1m parquet] {path_1m}")
    print(f"[1d 기간] {start_norm} ~ {end_norm}")
    print(f"[1m 일자] {biz_date_1m}")

    # CODE_TEXT로 최초 조회
    codes = _parse_codes(CODE_TEXT, name_to_code)
    if codes:
        _search_stock(
            codes, path_1d, path_1m, start_norm, end_norm, biz_date_1m,
            name_to_code, code_to_name,
        )

    # interactive: 종목명/코드 입력 시 반복 검색
    print(f"\n{'=' * 60}")
    print(" 종목명 또는 코드 입력 (Enter만 입력 시 종료)")
    print(f"{'=' * 60}")
    while True:
        try:
            inp = input("\n종목명/코드> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not inp:
            break
        codes = _parse_input_line(inp, name_to_code)
        if not codes:
            print("해당 종목을 찾을 수 없습니다.")
            continue
        _search_stock(
            codes, path_1d, path_1m, start_norm, end_norm, biz_date_1m,
            name_to_code, code_to_name,
        )


if __name__ == "__main__":
    main()
