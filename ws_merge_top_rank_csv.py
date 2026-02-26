#!/usr/bin/env python3
"""
Top30_1m_YYMMDD_HHMM.csv 통합 전담 스크립트

- 지정 날짜의 Top30_1m_260225_0906.csv 형태 파일들을 하나의 Top30_1m_merged_YYMMDD.csv 로 병합

사용법:
  python ws_merge_top_rank_csv.py [YYMMDD] [--dir 경로] [--out 출력파일]

  TARGET_DATE (상단) 또는 인자 없으면 오늘 날짜
"""
import argparse
import shutil
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import pandas as pd

TARGET_DATE = "260225"  # YYMMDD, 비어있으면 오늘
DEFAULT_DIR = Path("/home/ubuntu/Stoc_Kis/data/fetch_top_list/1m_fetch")
BACKUP_SUBDIR = "backup"


def merge_top30_1m(date_str: str, src_dir: Path, out_path: Path | None = None) -> Path | None:
    """Top30_1m_{date_str}_*.csv 파일들을 Top30_1m_merged_{date_str}.csv 로 통합. 실패 시 None 반환."""
    pattern = f"Top30_1m_{date_str}_*.csv"
    files = sorted(src_dir.glob(pattern))
    if not files:
        print(f"[오류] '{pattern}' 에 해당하는 파일 없음 (경로: {src_dir})")
        return None

    dfs: list[pd.DataFrame] = []
    for f in files:
        try:
            df = pd.read_csv(f, encoding="utf-8-sig")
        except Exception as e:
            print(f"[경고] 읽기 실패: {f.name} → {e}")
            continue

        # fetch_time 없으면 파일명에서 추출: Top30_1m_260225_0906 → 09:06
        if "fetch_time" not in df.columns:
            stem = f.stem
            parts = stem.split("_")
            if len(parts) >= 2:
                raw_time = parts[-1]  # HHMM
                if len(raw_time) == 4:
                    df.insert(0, "fetch_time", f"{raw_time[:2]}:{raw_time[2:]}")
        dfs.append(df)

    if not dfs:
        print("[오류] 읽을 수 있는 파일이 없습니다.")
        return None

    merged = pd.concat(dfs, ignore_index=True)

    if out_path is None:
        out_path = src_dir / f"Top30_1m_merged_{date_str}.csv"

    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[완료] {len(files)}개 파일 → {len(merged)}행 병합 → {out_path.name}")

    # 병합 완료 후 원본 파일을 backup 폴더로 이동
    backup_dir = src_dir / BACKUP_SUBDIR
    backup_dir.mkdir(parents=True, exist_ok=True)
    moved = 0
    for f in files:
        dst = backup_dir / f.name
        try:
            shutil.move(str(f), str(dst))
            moved += 1
        except Exception as e:
            print(f"[백업] 이동 실패 {f.name}: {e}")
    if moved:
        print(f"[백업] {moved}개 파일 → {BACKUP_SUBDIR}/")
    return out_path


def _resolve_date(cli_date: str | None) -> str:
    if cli_date:
        return cli_date
    if TARGET_DATE.strip():
        return TARGET_DATE.strip()
    return datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d")


def main():
    parser = argparse.ArgumentParser(description="Top30_1m CSV 통합")
    parser.add_argument("date", nargs="?", default=None, help="YYMMDD (예: 260225)")
    parser.add_argument("--dir", default=str(DEFAULT_DIR), help="CSV 폴더 경로")
    parser.add_argument("--out", default=None, help="출력 파일 경로")
    args = parser.parse_args()

    date_str = _resolve_date(args.date)
    print(f"[날짜] {date_str} (20{date_str[:2]}-{date_str[2:4]}-{date_str[4:6]})")

    src_dir = Path(args.dir)
    out_path = Path(args.out) if args.out else None
    result = merge_top30_1m(date_str, src_dir, out_path)


if __name__ == "__main__":
    main()
