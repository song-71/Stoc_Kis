import argparse
import os
import time

import pandas as pd
from datetime import datetime
from typing import Optional

from kis_utils import (
    load_symbol_master_cached,
    resolve_symbol,
)
"""
1d_unified_parquet_DB.parquet 파일을 조회하여 종목의 시계열 데이터를 조회하여 터미널창에 출력해줍니다.

하단 터미널에서 종목코드 또는 종목명을 입력받아 저장된 종목의 시계열 데이터 모두를 조회합니다.
즉 parquet파일에 저장된 데이터의 전체 분량 등을 확인 가능

"""

def main() -> None:
    종목코드=""  #005930
    시작일 = "2025-01-01"
    # 종료일 = "2025-12-31"
    종료일 = datetime.now().strftime("%Y-%m-%d")
    ap = argparse.ArgumentParser(description="종목 시계열 조회")
    ap.add_argument("--query", default=None, help="종목코드 또는 종목명 일부")
    ap.add_argument("--start", default=시작일, help="YYYY-MM-DD")
    ap.add_argument("--end", default=종료일, help="YYYY-MM-DD")
    ap.add_argument("--config", default="config.json", help="설정 파일 경로")
    args = ap.parse_args()

    config_path = args.config
    df_symbols = load_symbol_master_cached(config_path)

    parquet_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "1d_data", "kis_1d_unified_parquet_DB.parquet")
    if not os.path.exists(parquet_path):
        raise SystemExit(f"parquet 파일이 없습니다: {parquet_path}")

    while True:
        query = args.query
        if not query:
            query = input(f"종목코드/이름 입력 (기본: {종목코드}): ").strip() or 종목코드
        if not query:
            print("입력이 비어 있습니다. 다시 입력하세요.")
            args.query = None
            continue
        if query.isdigit() and len(query) < 6:
            query = query.zfill(6)

        t0 = time.perf_counter()
        row, matches = resolve_symbol(query, df_symbols)
        if row is None and len(matches) > 1:
            print("여러 종목이 검색되었습니다:")
            for idx, r in enumerate(matches.itertuples(), 1):
                print(f"{idx}. {r.code} {r.name} ({r.market})")
            sel = input("번호를 선택하세요: ").strip()
            if not sel.isdigit() or not (1 <= int(sel) <= len(matches)):
                print("선택이 올바르지 않습니다. 다시 입력하세요.")
                args.query = None
                continue
            row = matches.iloc[int(sel) - 1]
        elif row is None and len(matches) == 1:
            row = matches.iloc[0]

        if row is None:
            print(f"{query}코드가 없습니다")
            args.query = None
            continue

        code = str(row["code"])
        name = str(row["name"])
        print(f"[selected] {code} {name}")
        t_select = time.perf_counter() - t0
        print(f"[elapsed] selected_seconds={t_select:.3f}")

        t1 = time.perf_counter()
        df_all = pd.read_parquet(parquet_path)
        df = df_all[(df_all["symbol"] == code) & (df_all["date"] >= args.start) & (df_all["date"] <= args.end)]
        df = df.sort_values(["date", "symbol"]).reset_index(drop=True)
        print(df)
        t_query = time.perf_counter() - t1
        print(f"[elapsed] query_seconds={t_query:.3f}")

        args.query = None


if __name__ == "__main__":
    main()
