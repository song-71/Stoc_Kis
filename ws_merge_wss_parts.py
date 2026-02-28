"""
Wss 웹소켓으로 다운받은 데이터를 병합만 별도로 해주는 프로그램
  * 당초 각 수신 프로그램에서 자체 병합을 진행하나, 유사시 대비 별도로 병합 프로그램을 만들어 둔것

사용법:
  python ws_merge_wss_parts.py          # 대화형 선택
  python ws_merge_wss_parts.py 1        # DB-1 (수신저장) 만 병합
  python ws_merge_wss_parts.py 2        # DB-2 (수신저장) 만 병합
  python ws_merge_wss_parts.py 0        # 전체 일괄 병합

병합 방식:
  - parts 폴더 내 parquet 파일을 날짜별로 그룹핑
  - 기존 최종 파일이 있으면 포함하여 누적 병합 (덮어쓰기 없음)
  - 병합 완료된 parts는 backup 폴더로 이동
"""

import logging
import shutil
import sys
from collections import defaultdict
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# ── 옵션 설정 ────────────────────────────────────────────────────────
#   0 : 전체 일괄 병합
#   1 : DB-1 (수신저장) 만 병합(data/wss_data 폴더)
#   2 : DB-2 (수신저장) 만 병합(data/wss_data-2 폴더)
#   None : 대화형 선택 (실행 시 번호 입력)
#   * 커맨드라인 인자가 있으면 이 값보다 우선 적용됨
MERGE_TARGET = 1
# ─────────────────────────────────────────────────────────────────────

KST = ZoneInfo("Asia/Seoul")
BASE_DIR = Path(__file__).resolve().parent
WSS_DATA_DIR = BASE_DIR / "data" / "wss_data"
WSS_DATA2_DIR = BASE_DIR / "data" / "wss_data-2"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── 병합 대상 경로 설정 ──────────────────────────────────────────────
#   ws_realtime_subscribe_to_DB-1.py → data/wss_data
#   ws_realtime_subscribe_to_DB-2.py → data/wss_data-2
#   최종파일 패턴의 {date}는 YYMMDD로 치환됨
TARGETS = [
    {
        "label": "DB-1 (수신저장)",
        "parts":   WSS_DATA_DIR / "parts",
        "backup":  WSS_DATA_DIR / "backup",
        "pattern": "{date}_wss_data.parquet",
        "out_dir": WSS_DATA_DIR,
    },
    {
        "label": "DB-2 (수신저장)",
        "parts":   WSS_DATA2_DIR / "parts",
        "backup":  WSS_DATA2_DIR / "backup",
        "pattern": "{date}_wss_data.parquet",
        "out_dir": WSS_DATA2_DIR,
    },
]


# ── 유틸 ─────────────────────────────────────────────────────────────
def _is_valid_parquet(path: Path) -> bool:
    try:
        if path.stat().st_size < 8:
            return False
        with path.open("rb") as f:
            head = f.read(4)
            f.seek(-4, 2)
            tail = f.read(4)
        return head == b"PAR1" and tail == b"PAR1"
    except Exception:
        return False


def _progress(msg: str) -> None:
    """제자리 출력으로 진행상태 표시 (같은 줄 덮어쓰기)."""
    try:
        width = shutil.get_terminal_size((80, 20)).columns
    except Exception:
        width = 80
    padded = (msg + " " * width)[:width]
    sys.stdout.write("\r" + padded)
    sys.stdout.flush()


def _merge_date_group(
    final_path: Path,
    parts: list[Path],
    backup_dir: Path,
    progress_msg: str = "",
) -> int:
    """parts를 final_path에 누적 병합. 기존 최종 파일이 있으면 포함."""
    tables: list[pa.Table] = []
    base_msg = progress_msg or f"  [{final_path.stem[:6]}]"

    # 기존 최종 파일 포함
    if final_path.exists():
        if _is_valid_parquet(final_path):
            tables.append(pq.read_table(final_path))
            logger.info(f"{base_msg} 기존 파일 포함: {final_path.name}")
        else:
            bad = final_path.with_suffix(final_path.suffix + ".bad")
            final_path.rename(bad)
            logger.warning(f"{base_msg} invalid parquet → {bad.name}")

    # parts 읽기 (스키마 불일치 방지: 개별 읽기 후 permissive 병합)
    _progress(f"{base_msg} parts 읽는 중 ({len(parts)}개)...")
    for p in parts:
        if _is_valid_parquet(p):
            tables.append(pq.read_table(p))

    combined = pa.concat_tables(tables, promote_options="permissive")

    # recv_ts 정렬 (있으면)
    _progress(f"{base_msg} 정렬 중...")
    df = combined.to_pandas()
    if "recv_ts" in df.columns:
        try:
            df = df.sort_values("recv_ts", kind="mergesort").reset_index(drop=True)
            before = len(df)
            df = df.drop_duplicates(subset=["recv_ts"]).reset_index(drop=True)
            removed = before - len(df)
            if removed:
                logger.info(f"{base_msg} 중복 제거: {removed:,}건")
        except Exception:
            pass
    combined = pa.Table.from_pandas(df, preserve_index=False)

    _progress(f"{base_msg} 저장 중...")
    pq.write_table(combined, final_path, compression="zstd", use_dictionary=True)

    # parts → backup
    backup_dir.mkdir(parents=True, exist_ok=True)
    _progress(f"{base_msg} backup 이동 중 ({len(parts)}개)...")
    for p in parts:
        p.replace(backup_dir / p.name)

    return combined.num_rows


def _merge_target(target: dict) -> None:
    """하나의 타겟(parts 폴더)에 대해 날짜별 그룹핑 후 병합."""
    parts_dir: Path = target["parts"]
    backup_dir: Path = target["backup"]
    out_dir: Path = target["out_dir"]
    pattern: str = target["pattern"]

    if not parts_dir.exists():
        logger.info(f"[{target['label']}] parts 폴더 없음: {parts_dir}")
        return

    all_parts = sorted(parts_dir.glob("*.parquet"))
    if not all_parts:
        logger.info(f"[{target['label']}] 병합할 parts 파일 없음")
        return

    # 날짜별 그룹핑 (파일명 앞 6자리 = YYMMDD)
    date_groups: dict[str, list[Path]] = defaultdict(list)
    for p in all_parts:
        date_prefix = p.name[:6]
        date_groups[date_prefix].append(p)

    total_dates = len(date_groups)
    logger.info(
        f"[{target['label']}] parts {len(all_parts)}개 / "
        f"날짜 {total_dates}개 ({', '.join(sorted(date_groups.keys()))})"
    )

    for idx, (date_str, date_parts) in enumerate(sorted(date_groups.items()), 1):
        progress_msg = f"  [{idx}/{total_dates}] {date_str}"
        final_name = pattern.replace("{date}", date_str)
        final_path = out_dir / final_name
        n = _merge_date_group(
            final_path, sorted(date_parts), backup_dir,
            progress_msg=progress_msg,
        )
        _progress("")  # 제자리 줄 비우기
        print(f"  [{idx}/{total_dates}] {date_str} ✓ 완료: {n:,} rows → {final_path.name}")


# ── 메인 ─────────────────────────────────────────────────────────────
def main() -> None:
    data_dir = BASE_DIR / "data"
    if not data_dir.exists():
        raise FileNotFoundError(f"data 디렉토리 없음: {data_dir}")

    # 인자 확인 (커맨드라인 > 상단 MERGE_TARGET > 대화형)
    choice = None
    if len(sys.argv) > 1:
        choice = sys.argv[1].strip()
    elif MERGE_TARGET is not None:
        choice = str(MERGE_TARGET)

    if choice is None:
        print("=" * 50)
        print(" WSS Parts 병합 도구")
        print("=" * 50)
        for i, t in enumerate(TARGETS, 1):
            parts_dir = t["parts"]
            cnt = len(list(parts_dir.glob("*.parquet"))) if parts_dir.exists() else 0
            status = f"{cnt}개" if cnt > 0 else "없음"
            print(f"  {i}. {t['label']:20s}  (parts: {status})")
        print(f"  0. 전체 일괄 병합")
        print("-" * 50)
        choice = input("번호 선택 (0~2): ").strip()

    if choice == "0":
        selected = list(range(len(TARGETS)))
    elif choice in ("1", "2"):
        selected = [int(choice) - 1]
    else:
        print(f"잘못된 입력: {choice}")
        return

    for idx in selected:
        t = TARGETS[idx]
        print(f"\n{'─' * 50}")
        print(f" [{idx+1}] {t['label']} 병합 시작")
        print(f"{'─' * 50}")
        _merge_target(t)

    print(f"\n{'=' * 50}")
    print(" 병합 완료")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
