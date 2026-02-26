import duckdb
import polars as pl
from pathlib import Path

"""
당일 수신받은 도중에 파트파일을 읽어 CSV로 저장
 (당일 수신받은 파일은 종료 후 파일을 병합토록 되어 있으며, 그 전에 CSV로 저장하여 확인할 용도로 작성)
   * 1번은 지정된 파일 1개를 읽어 CSV 저장하는 기능임.
"""
# 옵션 설정 (항상 맨 위)
# 1  : 지정 파일 읽어 -> CSV 저장
# 21 : wss_data/parts 폴더에서 DuckDB 조회 -> CSV 저장
# 22 : wss_data/krx100/parts 폴더에서 DuckDB 조회 -> CSV 저장
# 23 : wss_data/trade_test/parts 폴더에서 DuckDB 조회 -> CSV 저장
OPTION = 1

# 옵션 1용(지정된 파일을 읽을수 있도로 파일 경로 및 명을 직접 입력)  => 자동 읽도록 하는 것은 ws_recv_data_to_CSV.py가 더 편리
path1 = "/home/ubuntu/Stoc_Kis/data/wss_data/"
file_name = "260206_wss_data.parquet"

# 옵션 21~23용
# - 시간: stck_cntg_hour (예: 0910)
# - 종목: 하림지주
QUERY_SQL = """
SELECT
  stck_cntg_hour,
  code,
  name,
  stck_prpr,
  prdy_vrss,
  prdy_ctrt,
  stck_oprc,
  stck_hgpr,
  stck_lwpr,
  askp1,
  bidp1,
  acml_vol
FROM {view}
WHERE  name IS NOT NULL
  AND stck_cntg_hour >= '0800'
  AND stck_cntg_hour <= '1506'
ORDER BY stck_cntg_hour
"""
OUT_CSV_NAME = "wss_query_result.csv"


"""
터미널 실행 명령어 (wss_data 폴더에 있는 모든 파일 조회) - 옵션에 따라 검색 대상 상이
    /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/ws_read_wss_parquet.py
"""

def _run_option_1() -> None:
    src_path = Path(path1) / file_name
    df = pl.read_parquet(src_path)
    out_path = src_path.with_suffix(".csv")
    df.write_csv(out_path, include_bom=True)
    print("saved:", out_path)


def _run_duckdb_query(base_dir: Path) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    parts_dir = base_dir / "parts"
    parts_dir.mkdir(parents=True, exist_ok=True)
    glob_path = str(parts_dir / "*.parquet")

    con = duckdb.connect(database=":memory:")
    con.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{glob_path}')")
    cols = [row[1] for row in con.execute("PRAGMA table_info('data')").fetchall()]

    view_name = "data"
    code_candidates = ["mksc_shrn_iscd", "code", "stck_shrn_iscd"]
    code_col = next((c for c in code_candidates if c in cols), None)
    if code_col is None:
        raise RuntimeError("no code column found for name mapping")

    if "code" not in cols or code_col != "code":
        con.execute(
            f"""
            CREATE VIEW data_with_code AS
            SELECT d.*, CAST(d.{code_col} AS VARCHAR) AS code
            FROM data d
            """
        )
        view_name = "data_with_code"

    if "name" not in cols:
        krx_path = "/home/ubuntu/Stoc_Kis/data/admin/symbol_master/KRX_code.parquet"
        con.execute(f"CREATE VIEW krx AS SELECT * FROM read_parquet('{krx_path}')")
        con.execute(
            f"""
            CREATE VIEW data_with_name AS
            SELECT d.*, k.name AS name
            FROM {view_name} d
            LEFT JOIN krx k
              ON CAST(d.code AS VARCHAR) = CAST(k.code AS VARCHAR)
            """
        )
        view_name = "data_with_name"

    df = con.execute(QUERY_SQL.format(view=view_name)).df()

    out_path = base_dir / OUT_CSV_NAME
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(df)
    print("saved:", out_path)


if OPTION == 1:
    _run_option_1()
elif OPTION == 21:
    _run_duckdb_query(Path("/home/ubuntu/Stoc_Kis/data/wss_data"))
elif OPTION == 22:
    _run_duckdb_query(Path("/home/ubuntu/Stoc_Kis/data/wss_data/krx100"))
elif OPTION == 23:
    _run_duckdb_query(Path("/home/ubuntu/Stoc_Kis/data/wss_data/trade_test"))
else:
    raise ValueError(f"unknown OPTION: {OPTION}")
