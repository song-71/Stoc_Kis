#!/usr/bin/env python3
"""
WSS 데이터 자동 정리 + S3 아카이브

서버 용량 점유를 막기 위해 data/wss_data/ 의 누적 파일을 정리한다.

정리 규칙:
  1) *_wss_data.parquet
       → 최근 날짜 3개(기본)만 로컬 유지, 나머지는 S3 업로드 후 로컬 삭제
  2) *_indicator_snapshot.parquet
       → 동일(최근 3개 유지, 나머지 S3 업로드 후 삭제)
  3) backup/ 폴더
       → 당일자(KST 오늘) 파일만 남기고 나머지는 전부 삭제 (S3 업로드 불필요)
  ※ parts/ 폴더는 건드리지 않는다.

S3 경로: s3://tfttrain/wss_data/<파일명 그대로>
안전장치: S3 업로드 후 head_object 로 존재를 확인한 뒤에만 로컬 파일을 삭제한다.
          업로드/검증 실패 시 로컬 파일은 보존한다(데이터 유실 방지).

사용:
  python ws_s3upload_and_cleanup_data.py              # 실제 실행
  python ws_s3upload_and_cleanup_data.py --dry-run    # 삭제/업로드 없이 계획만 출력
  python ws_s3upload_and_cleanup_data.py --keep 5     # 보관할 최근 날짜 개수 변경 (기본 3)

프로덕션(ws_realtime_trading.py) 종료 시 cleanup_wss_data() 를 import 하여 호출한다.
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
BASE_DIR = Path(__file__).resolve().parent
WSS_DATA_DIR = BASE_DIR / "data" / "wss_data"
BACKUP_DIR = WSS_DATA_DIR / "backup"

S3_BUCKET = "tfttrain"
S3_PREFIX = "wss_data"   # s3://tfttrain/wss_data/<filename>

DEFAULT_KEEP = 3


def _parse_yymmdd(name: str):
    """파일명 앞 6자리 YYMMDD → date. 파싱 실패 시 None."""
    try:
        return datetime.strptime(name[:6], "%y%m%d").date()
    except (ValueError, IndexError):
        return None


def _archive_and_delete(files, s3_client, logger, dry_run):
    """files(list[Path]) 를 S3 업로드 → head_object 검증 성공 시에만 로컬 삭제."""
    archived, freed = 0, 0
    for f in files:
        key = f"{S3_PREFIX}/{f.name}"
        try:
            size = f.stat().st_size
        except OSError:
            continue
        if dry_run:
            logger.info(
                f"[dry-run] 업로드+삭제 예정: {f.name} ({size/1e6:.1f}MB) "
                f"→ s3://{S3_BUCKET}/{key}"
            )
            archived += 1
            freed += size
            continue
        # 1) 업로드 + 검증
        try:
            s3_client.upload_file(str(f), S3_BUCKET, key)
            s3_client.head_object(Bucket=S3_BUCKET, Key=key)  # 존재 확인
        except Exception as e:
            logger.error(f"[cleanup] S3 업로드/검증 실패, 로컬 보존: {f.name}: {e}")
            continue
        # 2) 검증 성공 후에만 로컬 삭제
        try:
            f.unlink()
            archived += 1
            freed += size
            logger.info(
                f"[cleanup] 아카이브 완료: {f.name} → s3://{S3_BUCKET}/{key} (로컬 삭제, {size/1e6:.1f}MB)"
            )
        except Exception as e:
            logger.error(f"[cleanup] 로컬 삭제 실패(S3 업로드는 완료): {f.name}: {e}")
    return archived, freed


def cleanup_wss_data(keep: int = DEFAULT_KEEP, dry_run: bool = False, logger=None) -> dict:
    """
    WSS 데이터 정리 본체.
    반환: {"archived": S3 아카이브 후 삭제한 파일 수,
           "deleted_backup": backup 에서 삭제한 파일 수,
           "freed_bytes": 회수한 로컬 용량}
    """
    if logger is None:
        logger = logging.getLogger("cleanup_wss_data")

    summary = {"archived": 0, "deleted_backup": 0, "freed_bytes": 0}

    if not WSS_DATA_DIR.exists():
        logger.warning(f"[cleanup] 디렉터리 없음: {WSS_DATA_DIR}")
        return summary

    # ── 1) FINAL parquet 류: 최근 keep 날짜만 유지, 나머지 S3 업로드 후 삭제 ──
    to_archive = []
    for pattern in ("*_wss_data.parquet", "*_indicator_snapshot.parquet"):
        dated = [
            (d, f)
            for f in WSS_DATA_DIR.glob(pattern)
            if (d := _parse_yymmdd(f.name)) is not None
        ]
        if len(dated) <= keep:
            continue
        dated.sort(key=lambda x: x[0], reverse=True)  # 최신 날짜 우선
        to_archive.extend(f for _, f in dated[keep:])  # keep 이후(오래된 것)만 대상

    if to_archive:
        s3 = None
        try:
            import boto3
            s3 = boto3.client("s3")
        except Exception as e:
            logger.error(f"[cleanup] S3 클라이언트 생성 실패, 아카이브 건너뜀: {e}")
        if s3 is not None:
            a, fr = _archive_and_delete(to_archive, s3, logger, dry_run)
            summary["archived"] += a
            summary["freed_bytes"] += fr

    # ── 2) backup/ : 당일자(KST 오늘)만 유지, 나머지 전부 삭제 (S3 업로드 없음) ──
    if BACKUP_DIR.exists():
        today = datetime.now(KST).date()
        for f in BACKUP_DIR.glob("*.parquet"):
            d = _parse_yymmdd(f.name)
            if d is None:
                # 날짜 파싱 불가 파일은 안전하게 보존
                logger.warning(f"[cleanup] backup 날짜 파싱 불가, 보존: {f.name}")
                continue
            if d == today:
                continue  # 당일자 보존
            try:
                size = f.stat().st_size
            except OSError:
                continue
            if dry_run:
                logger.info(f"[dry-run] backup 삭제 예정: {f.name} ({size/1e6:.1f}MB)")
                summary["deleted_backup"] += 1
                summary["freed_bytes"] += size
                continue
            try:
                f.unlink()
                summary["deleted_backup"] += 1
                summary["freed_bytes"] += size
            except Exception as e:
                logger.error(f"[cleanup] backup 삭제 실패: {f.name}: {e}")
        logger.info(
            f"[cleanup] backup 정리: {summary['deleted_backup']}개 삭제 "
            f"(당일자 {today:%y%m%d} 보존)"
        )

    logger.info(
        f"[cleanup] 완료: S3 아카이브 {summary['archived']}개, "
        f"backup 삭제 {summary['deleted_backup']}개, "
        f"회수 {summary['freed_bytes']/1e9:.2f}GB"
        + (" (dry-run)" if dry_run else "")
    )
    return summary


def _main():
    ap = argparse.ArgumentParser(description="WSS 데이터 정리 + S3 아카이브")
    ap.add_argument("--keep", type=int, default=DEFAULT_KEEP,
                    help=f"로컬 보관할 최근 날짜 개수 (기본 {DEFAULT_KEEP})")
    ap.add_argument("--dry-run", action="store_true",
                    help="삭제/업로드 없이 계획만 출력")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    cleanup_wss_data(keep=args.keep, dry_run=args.dry_run)


if __name__ == "__main__":
    _main()
