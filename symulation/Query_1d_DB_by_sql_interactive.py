"""
SQL로 1d 통합 Parquet_DB 데이터를 조회해 출력 (DuckDB 사용)
  - 첫 실행: CODE_TEXT에 입력된 종목 조회 → 출력 & CSV 저장
  - 이후: 대화형 프롬프트에서 종목코드/종목명 입력 → 즉시 조회
  - 'q' 또는 빈 입력으로 종료

전체 컬럼:
['date', 'symbol', 'name', 'open', 'high', 'low', 'close', 'volume',
       'value', 'pdy_close', 'p2dy_close', 'tdy_ctrt', 'pdy_ctrt', 'p2dy_ctrt',
       'pdy_uc', 'pdy_h', 'pdy_L', 'R_avr', 'R_org']
"""
import argparse, os, sys, duckdb, shutil, subprocess
from datetime import datetime
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

# ── 옵션 설정 ────────────────────────────────────────────────────────
DEFAULT_START_DATE = "20260201"  # 시작일자 (예): 검색 "20260101" or "2026-01-01"
DEFAULT_END_DATE = ""  # 비우면 오늘 날짜까지
DEFAULT_SAVE_CSV = True  # True면 CSV로 저장
DEFAULT_LIMIT = 0  # 출력 rows 제한 (0이면 제한 없음)
DEFAULT_COPY_CLIPBOARD = False  # True면 결과를 클립보드에 복사
DEFAULT_SAVE_CSV_NAME = "temp.csv"
print_all_rows = 0  # 1이면 전체행 정렬 출력, 0이면 print(df)

# 종목코드 리스트(첫 조회 대상. all 입력 시 모두 조회)
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
# ─────────────────────────────────────────────────────────────────────

# 출력 컬럼 순서
OUTPUT_COLS = [
    "date", "symbol", "name", "open", "high", "low", "close", "volume",
    "pdy_close", "tdy_ctrt", "pdy_ctrt", "p2dy_ctrt",
]


def _normalize_date(text: str | None, default: str) -> str:
    if not text:
        return default
    cleaned = str(text).strip()
    if "-" in cleaned:
        return datetime.strptime(cleaned, "%Y-%m-%d").strftime("%Y-%m-%d")
    if len(cleaned) == 8 and cleaned.isdigit():
        return datetime.strptime(cleaned, "%Y%m%d").strftime("%Y-%m-%d")
    return default


def _parse_filters(text: str) -> tuple[list[str], list[str]]:
    codes = []
    names = []
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        if raw.lower() == "all":
            return [], []
        if raw.isdigit():
            codes.append(raw.zfill(6))
            continue
        if raw.isalnum() and any(ch.isdigit() for ch in raw):
            codes.append(raw)
            continue
        names.append(raw)
    return codes, names


def _resolve_parquet_path() -> str:
    base_dir = Path(__file__).resolve().parent
    parquet_path = base_dir.parent / "data" / "1d_data" / "kis_1d_unified_parquet_DB.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"parquet 파일이 없습니다: {parquet_path}")
    return str(parquet_path)


def _copy_to_clipboard(text: str) -> bool:
    tool = shutil.which("xclip") or shutil.which("xsel") or shutil.which("pbcopy")
    if not tool:
        print("[clipboard] 사용 가능한 도구가 없습니다 (xclip/xsel/pbcopy)")
        return False
    try:
        if tool.endswith("xclip"):
            subprocess.run([tool, "-selection", "clipboard"], input=text, text=True, check=True)
        elif tool.endswith("xsel"):
            subprocess.run([tool, "--clipboard", "--input"], input=text, text=True, check=True)
        else:
            subprocess.run([tool], input=text, text=True, check=True)
        print("[clipboard] CSV 전체를 클립보드에 복사했습니다.")
        return True
    except Exception as exc:
        print(f"[clipboard] 복사 실패: {exc}")
        return False


def _build_code_filter(codes: list[str], names: list[str]) -> str:
    if not codes and not names:
        return ""
    parts = []
    if codes:
        codes_sql = ",".join(f"'{c}'" for c in codes)
        parts.append(f"symbol IN ({codes_sql})")
    if names:
        names_sql = ",".join(f"'{n}'" for n in names)
        parts.append(f"name IN ({names_sql})")
    return f"AND ({' OR '.join(parts)})"


def _query_and_print(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    date_filter: str,
    code_filter: str,
    limit_sql: str,
    save_csv: bool,
    label: str = "",
) -> None:
    query = f"""
    SELECT *
    FROM read_parquet('{parquet_path}')
    WHERE 1=1
    {date_filter}
    {code_filter}
    ORDER BY symbol, date
    {limit_sql}
    """
    df = con.execute(query).df()

    # 출력 컬럼 정렬
    cols = [c for c in OUTPUT_COLS if c in df.columns]
    if cols:
        df_out = df[cols]
    else:
        df_out = df

    if label:
        print(f"\n{'─' * 60}")
        print(f" {label}")
        print(f"{'─' * 60}")

    if print_all_rows:
        import pandas as pd
        pd.set_option("display.colheader_justify", "right")
        print(df_out.to_string(index=False, justify="right"))
    else:
        print(df_out)

    print(f"[{len(df_out)} rows] columns: {list(df.columns)}")

    if save_csv:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        out_path = os.path.join(base_dir, DEFAULT_SAVE_CSV_NAME)
        df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[csv] saved: {out_path}")
    if DEFAULT_COPY_CLIPBOARD:
        _copy_to_clipboard(df_out.to_csv(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="1d 통합 parquet 조회")
    parser.add_argument("--start", dest="start_date", default=DEFAULT_START_DATE)
    parser.add_argument("--end", dest="end_date", default=DEFAULT_END_DATE)
    parser.add_argument("--limit", dest="limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--save_csv", action="store_true", default=DEFAULT_SAVE_CSV)
    args = parser.parse_args()

    parquet_path = _resolve_parquet_path()
    end_date = _normalize_date(args.end_date, datetime.now().strftime("%Y-%m-%d"))
    start_date = _normalize_date(args.start_date, "")
    limit_sql = f"LIMIT {int(args.limit)}" if isinstance(args.limit, int) and args.limit > 0 else ""

    date_filter = ""
    if start_date:
        date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"
    else:
        date_filter = f"AND date <= '{end_date}'"

    # DuckDB 연결 1회 (이후 재사용)
    con = duckdb.connect()
    try:
        # ── 1) 첫 조회: CODE_TEXT 기반 ──
        codes, names = _parse_filters(CODE_TEXT)
        code_filter = _build_code_filter(codes, names)
        _query_and_print(
            con, parquet_path, date_filter, code_filter, limit_sql,
            save_csv=args.save_csv,
            label="CODE_TEXT 초기 조회",
        )

        # ── 2) 대화형 루프 ──
        print(f"\n{'=' * 60}")
        print(" 종목코드 또는 종목명을 입력하세요 (q: 종료, all: 전체)")
        print(f"{'=' * 60}")
        while True:
            try:
                user_input = input("\n종목 입력> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not user_input or user_input.lower() == "q":
                break

            codes, names = _parse_filters(user_input)
            code_filter = _build_code_filter(codes, names)
            _query_and_print(
                con, parquet_path, date_filter, code_filter, limit_sql,
                save_csv=args.save_csv,
                label=user_input,
            )
    finally:
        con.close()
        print("\n[종료]")


if __name__ == "__main__":
    main()
