#!/usr/bin/env python3
"""과거 daily WSS parquet 무손실 재병합 복구 스크립트 (260706).

배경:
    ~2026-07-05 까지의 daily 파일(`data/wss_data/YYMMDD_wss_data.parquet`)은 최종 병합에서
    `drop_duplicates(subset=[code, recv_ts])` 로 **같은 프레임에 묶여온 실체결 다수가 삭제**되어
    있다(하루 수신의 약 54% 소실 실측). 그러나 그 원본 조각은 병합 시 삭제가 아니라
    **`data/wss_data/backup/YYMMDD_*.parquet` 로 이동**되어 온전히 남아 있다.

이 스크립트는 backup 조각을 다시 모아 `wss_recv_ts_util.unique_recv_ts` 로
    - 완전 동일 행(진짜 재병합 중복)만 제거하고,
    - 같은 (code, recv_ts) 실체결은 수신순 순번을 붙여 유일화(6→8자리)
하여 **무손실 daily 파일로 재생성**한다.

기본은 **dry-run**(무엇이 어떻게 바뀌는지만 보고). 실제 덮어쓰기는 `--apply`.
덮어쓸 때 기존(손상) daily 는 `YYMMDD_wss_data.collapsed.bak` 로 1회 보존한다.

사용:
    python3 recover_wss_daily_lossless.py                 # backup 에 있는 모든 날짜 dry-run
    python3 recover_wss_daily_lossless.py 260703 260702   # 특정 날짜만 dry-run
    python3 recover_wss_daily_lossless.py --apply          # 전체 실제 복구
    python3 recover_wss_daily_lossless.py 260703 --apply   # 특정 날짜 실제 복구
"""
from __future__ import annotations

import argparse
import glob
import os
import re
import sys

import pandas as pd

from wss_recv_ts_util import unique_recv_ts

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "wss_data")
BACKUP = os.path.join(BASE, "backup")

_DATE_RE = re.compile(r"(\d{6})_")


def _dates_in_backup() -> list[str]:
    seen: set[str] = set()
    for p in glob.glob(os.path.join(BACKUP, "*_*.parquet")):
        m = _DATE_RE.match(os.path.basename(p))
        if m:
            seen.add(m.group(1))
    return sorted(seen)


def _load_backup_parts(date: str) -> tuple[pd.DataFrame | None, int, int]:
    parts = sorted(glob.glob(os.path.join(BACKUP, f"{date}_*.parquet")))
    frames, ok, bad = [], 0, 0
    for p in parts:
        try:
            frames.append(pd.read_parquet(p))
            ok += 1
        except Exception as e:  # noqa: BLE001
            bad += 1
            print(f"    [warn] part 읽기 실패 {os.path.basename(p)}: {e}")
    if not frames:
        return None, ok, bad
    return pd.concat(frames, ignore_index=True, sort=False), ok, bad


def _daily_rowcount(date: str) -> int | None:
    path = os.path.join(BASE, f"{date}_wss_data.parquet")
    if not os.path.exists(path):
        return None
    try:
        import pyarrow.parquet as pq

        return pq.ParquetFile(path).metadata.num_rows
    except Exception:
        try:
            return len(pd.read_parquet(path, columns=["recv_ts"]))
        except Exception:
            return None


def recover_date(date: str, apply: bool) -> None:
    print(f"[{date}]")
    df, ok, bad = _load_backup_parts(date)
    if df is None:
        print(f"    backup 조각 없음 → 복구 불가(daily 는 이미 축약됨). 건너뜀. (실패조각 {bad})")
        return
    raw_n = len(df)
    daily_n = _daily_rowcount(date)
    rec, n_full, n_dis = unique_recv_ts(df)
    rec_n = len(rec)

    daily_str = f"{daily_n:,}" if daily_n is not None else "없음"
    print(f"    backup 조각 {ok}개 → 원본 {raw_n:,}행 | 현재 daily {daily_str}행 "
          f"→ 복구 {rec_n:,}행 (완전중복 {n_full:,} 제거 / 순번유일화 {n_dis:,})")
    if daily_n is not None:
        gain = rec_n - daily_n
        if gain > 0:
            print(f"    ↳ 복원되는 소실 체결 약 {gain:,}행 (+{gain / max(1, daily_n) * 100:.0f}%)")
        elif gain == 0:
            print("    ↳ 이미 무손실(변화 없음) — 신규 형식이거나 복구 완료")
        else:
            print(f"    ↳ 주의: 복구본이 현재보다 {abs(gain):,}행 적음 — 원인 확인 필요")

    if not apply:
        print("    (dry-run — 실제 기록하려면 --apply)")
        return

    dst = os.path.join(BASE, f"{date}_wss_data.parquet")
    bak = os.path.join(BASE, f"{date}_wss_data.collapsed.bak")
    if os.path.exists(dst) and not os.path.exists(bak):
        os.replace(dst, bak)
        print(f"    기존(손상) daily 보존: {os.path.basename(bak)}")
    tmp = dst + ".tmp"
    rec.to_parquet(tmp, compression="zstd", index=False)
    os.replace(tmp, dst)
    print(f"    ✔ 무손실 daily 재생성: {os.path.basename(dst)} ({rec_n:,}행)")


def main() -> int:
    ap = argparse.ArgumentParser(description="과거 daily WSS parquet 무손실 재병합 복구")
    ap.add_argument("dates", nargs="*", help="복구할 날짜(YYMMDD). 생략 시 backup 의 모든 날짜")
    ap.add_argument("--apply", action="store_true", help="실제 덮어쓰기(미지정 시 dry-run)")
    args = ap.parse_args()

    if not os.path.isdir(BACKUP):
        print(f"backup 폴더 없음: {BACKUP}")
        return 1

    dates = args.dates or _dates_in_backup()
    if not dates:
        print("복구 대상 날짜 없음(backup 비어있음).")
        return 0

    mode = "APPLY(실제 복구)" if args.apply else "DRY-RUN(미리보기)"
    print(f"=== WSS daily 무손실 복구 [{mode}] 대상 {len(dates)}일 ===")
    for d in dates:
        recover_date(d, args.apply)
    if not args.apply:
        print("\n실제 복구하려면: python3 recover_wss_daily_lossless.py "
              + " ".join(dates[:3]) + (" ..." if len(dates) > 3 else "") + " --apply")
    return 0


if __name__ == "__main__":
    sys.exit(main())
