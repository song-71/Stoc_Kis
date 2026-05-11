#!/usr/bin/env python3
"""
범용 Parquet → CSV 변환기

[사용법]
  1) 상단 PARQUET_FILE 변수에 파일 경로를 지정한 뒤 실행
     python parquet_to_csv.py

  2) 명령줄 인수로 파일 경로를 전달하면 상단 설정보다 우선 적용
     python parquet_to_csv.py /path/to/file.parquet

[저장 위치]
  - 원본 parquet 파일과 같은 디렉토리에 동일 이름의 .csv 로 저장

[대용량 처리]
  - 10만행 초과 시 상단/하단/전체 중 선택하여 저장
"""

import sys
from pathlib import Path

import polars as pl

# ──────────────────────────────────────────────
# ★ 변환할 parquet 파일 경로 (여기에 입력)
PARQUET_FILE = "/home/ubuntu/Stoc_Kis/data/investor_data/kis_investor_unified_parquet_DB.parquet"
# ──────────────────────────────────────────────

MAX_ROWS = 100_000


def convert(parquet_path: str) -> None:
    src = Path(parquet_path)
    if not src.exists():
        print(f"[ERROR] 파일을 찾을 수 없습니다: {src}")
        sys.exit(1)

    df = pl.read_parquet(src)
    total = len(df)
    print(f"[INFO] 파일: {src.name}  |  행: {total:,}  |  열: {df.width}")

    if total > MAX_ROWS:
        print(f"\n※ 행 수가 {MAX_ROWS:,}행을 초과합니다 ({total:,}행).")
        print(f"  1) 상단 {MAX_ROWS:,}행 저장")
        print(f"  2) 하단 {MAX_ROWS:,}행 저장")
        print(f"  3) 전체 저장 (대용량 주의)")

        while True:
            choice = input("\n선택 (1/2/3): ").strip()
            if choice == "1":
                df = df.head(MAX_ROWS)
                print(f"→ 상단 {MAX_ROWS:,}행 선택")
                break
            elif choice == "2":
                df = df.tail(MAX_ROWS)
                print(f"→ 하단 {MAX_ROWS:,}행 선택")
                break
            elif choice == "3":
                print(f"→ 전체 {total:,}행 저장")
                break
            else:
                print("1, 2, 3 중 선택해주세요.")

    dst = src.with_suffix(".csv")
    # [260427] Excel 한글 호환: utf-8-sig (BOM 포함) 으로 저장
    # polars write_csv 는 BOM 옵션 없으므로 pandas 경유
    df.to_pandas().to_csv(dst, index=False, encoding="utf-8-sig")
    print(f"[완료] {dst}  ({len(df):,}행, encoding=utf-8-sig)")


def main():
    # 명령줄 인수가 있으면 우선, 없으면 상단 PARQUET_FILE 사용
    parquet_path = sys.argv[1] if len(sys.argv) > 1 else PARQUET_FILE
    convert(parquet_path)


if __name__ == "__main__":
    main()
