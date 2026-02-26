"""
SQL로 날짜별 지정된 코드에 대해 1분봉 Parquet DB 파일의 원본 구조를 확인용(DuckDB 사용)
  - 저장은 temp.csv 로 저장
  - 저장위치: /home/ubuntu/Stoc_Kis/symulation/temp.csv
"""

# 조회 옵션 (내부 설정)
target_date = "20260220"  # 예: 검색대상일, 형식: "20260123" 또는 "2026-01-23", 비우면 당일
print_all_rows = 0  # 1이면 전체행 출력, 0이면 요약 출력(중간 생략)
all_columns = 0     # 1이면 전체 컬럼(*), 0이면 선택 컬럼만 출력
default_save_csv_name = "temp.csv"

CODE_TEXT = """
에스코넥
"""




import argparse
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
sys.path.append(str(Path(__file__).resolve().parents[1]))
from kis_utils import load_symbol_master, print_table

def _normalize_biz_date(text: str | None) -> str:
    if not text:
        return datetime.now().strftime("%Y%m%d")
    cleaned = str(text).strip()
    if "-" in cleaned:
        return datetime.strptime(cleaned, "%Y-%m-%d").strftime("%Y%m%d")
    if len(cleaned) == 8 and cleaned.isdigit():
        return cleaned
    return datetime.now().strftime("%Y%m%d")


def _resolve_parquet_path(biz_date: str) -> str:
    base_dir = Path(__file__).resolve().parent
    out_dir = base_dir.parent / "data" / "1m_data"
    if not out_dir.exists():
        raise FileNotFoundError(f"1m_data 폴더가 없습니다: {out_dir}")
    matches = sorted(out_dir.glob(f"{biz_date}*_1m_chart_DB_parquet.parquet"))
    if not matches:
        raise FileNotFoundError(f"parquet 파일이 없습니다: {out_dir}/{biz_date}*_1m_chart_DB_parquet.parquet")
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


def _run_query_and_display(
    parquet_path: str,
    biz_date: str,
    codes: list[str],
    limit_arg: int,
    name_to_code: dict[str, str],
    code_to_name: dict[str, str],
) -> None:
    """지정 codes로 쿼리 실행 후 출력 및 CSV 저장"""
    code_filter = ""
    if codes:
        codes_sql = ",".join(f"'{c}'" for c in codes)
        code_filter = f"WHERE code IN ({codes_sql})"
    date_filter = f"date = '{biz_date}'"
    if code_filter:
        code_filter = f"{code_filter} AND {date_filter}"
    else:
        code_filter = f"WHERE {date_filter}"

    limit_sql = f"LIMIT {int(limit_arg)}" if isinstance(limit_arg, int) and limit_arg > 0 else ""
    select_cols = "*" if all_columns else "date, time, code, name, open, high, low, close, volume, acml_value, tdy_ret, pdy_close, pdy_ret"

    if isinstance(limit_arg, int) and limit_arg > 0 and codes:
        query = f"""
        SELECT {select_cols}
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY code ORDER BY date, time) AS rn
            FROM read_parquet('{parquet_path}')
            {code_filter}
        )
        WHERE rn <= {int(limit_arg)}
        ORDER BY code, date, time
        """
    else:
        query = f"""
        SELECT {select_cols}
        FROM read_parquet('{parquet_path}')
        {code_filter}
        ORDER BY code, date, time
        {limit_sql}
        """

    con = duckdb.connect()
    try:
        df = con.execute(query).df()
    finally:
        con.close()

    if df.empty:
        print("[결과 없음]")
        return

    if "name" in df.columns and code_to_name:
        code_name = df["code"].map(code_to_name)
        blank_name = df["name"].isna() | df["name"].astype(str).str.strip().eq("")
        df.loc[blank_name, "name"] = code_name
        df["name"] = df["name"].fillna("")

    for col in ["tdy_ret", "pdy_ret"]:
        if col in df.columns:
            df[col] = df[col].map(lambda x: f"{x:.4f}" if pd.notna(x) else "")

    if isinstance(limit_arg, int) and limit_arg > 0:
        print(f"[filtered per-code {limit_arg}]")
    else:
        print("[filtered all]")

    columns = list(df.columns)
    num_cols = set(df.select_dtypes(include="number").columns)
    align = {c: "right" if c in num_cols else "left" for c in columns}
    for col in ["tdy_ret", "pdy_ret"]:
        if col in columns:
            align[col] = "right"
    rows_dict = df.fillna("").astype(str).to_dict("records")

    if print_all_rows:
        print_table(rows_dict, columns, align)
    else:
        print_table(rows_dict, columns, align, max_rows=20)

    base_dir = Path(__file__).resolve().parent
    out_path = base_dir / default_save_csv_name
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[csv] saved: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="날짜별 1분봉 parquet 조회")
    parser.add_argument(
        "--date",
        dest="biz_date",
        help="기준일 (YYYYMMDD 또는 YYYY-MM-DD), 기본=내부 설정/당일",
    )
    parser.add_argument(
        "--limit",
        dest="limit",
        type=int,
        default=None,
        help="출력 행 제한 (0이면 제한 없음, 미지정 시 내부 설정 사용)",
    )
    args = parser.parse_args()

    date_arg = args.biz_date or target_date
    limit_arg = 0 if args.limit is None else args.limit

    biz_date = _normalize_biz_date(date_arg)
    parquet_path = _resolve_parquet_path(biz_date)

    name_to_code, code_to_name = _build_name_code_map()

    # parquet 구조 출력 (최초 1회)
    con = duckdb.connect()
    print(f"[parquet path] {parquet_path}")
    schema_df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").df()
    all_cols = schema_df["column_name"].tolist() if "column_name" in schema_df.columns else []
    print("[parquet columns]", all_cols)
    raw_head = con.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 5").df()
    print("[parquet head 5]_____raw_head.to_string(index=False, justify='right')_____")
    print(raw_head.to_string(index=False, justify="right"))
    con.close()

    # CODE_TEXT로 최초 조회
    codes = _parse_codes(CODE_TEXT, name_to_code)
    if codes:
        _run_query_and_display(
            parquet_path, biz_date, codes, limit_arg, name_to_code, code_to_name
        )

    # interactive: 종목명/코드 입력 시 반복 검색
    print("\n--- 종목명 또는 코드 입력 (Enter만 입력 시 종료) ---")
    while True:
        try:
            inp = input("종목명/코드> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not inp:
            break
        codes = _parse_input_line(inp, name_to_code)
        if not codes:
            print("해당 종목을 찾을 수 없습니다.")
            continue
        _run_query_and_display(
            parquet_path, biz_date, codes, limit_arg, name_to_code, code_to_name
        )


if __name__ == "__main__":
    main()
