"""
Wss 웹소켓 수신 데이터 중간 병합 (스냅샷)
  * 장중에 현재까지 수신한 parts 파일을 병합하여 스냅샷 파일 생성
  * parts 파일은 이동/삭제하지 않음 (장 종료 후 최종 병합에 영향 없음)
  * 출력: {date}_wss_data_snapshot.parquet

사용법:
  python ws_merge_wss_parts_snapshot.py          # TR (수신저장) 스냅샷
  python ws_merge_wss_parts_snapshot.py 1        # TR 만
  python ws_merge_wss_parts_snapshot.py 2        # DB-2 만
  python ws_merge_wss_parts_snapshot.py 0        # 전체
"""

import logging
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

# ── 옵션 설정 ────────────────────────────────────────────────────────
MERGE_TARGET = 1          # 1: TR, 2: DB-2, 0: 전체
TARGET_DATE = ""          # 빈 문자열이면 오늘 날짜
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

TARGETS = [
    {
        "label": "TR (수신저장)",
        "parts":   WSS_DATA_DIR / "parts",
        "pattern": "{date}_wss_data_snapshot.parquet",
        "out_dir": WSS_DATA_DIR,
    },
    {
        "label": "DB-2 (수신저장)",
        "parts":   WSS_DATA2_DIR / "parts",
        "pattern": "{date}_wss_data_snapshot.parquet",
        "out_dir": WSS_DATA2_DIR,
    },
]


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
    try:
        width = shutil.get_terminal_size((80, 20)).columns
    except Exception:
        width = 80
    padded = (msg + " " * width)[:width]
    sys.stdout.write("\r" + padded)
    sys.stdout.flush()


def _merge_snapshot(
    final_path: Path,
    parts: list[Path],
    progress_msg: str = "",
) -> int:
    """parts만 병합하여 스냅샷 생성. parts 파일은 이동하지 않음."""
    tables: list[pa.Table] = []
    base_msg = progress_msg or f"  [{final_path.stem[:6]}]"

    # ★ 기존 최종 파일(_snapshot이 아닌)은 포함하지 않음 — parts만 병합

    _progress(f"{base_msg} parts 읽는 중 ({len(parts)}개)...")
    for p in parts:
        if _is_valid_parquet(p):
            tables.append(pq.read_table(p))

    if not tables:
        logger.info(f"{base_msg} 유효한 parquet 없음")
        return 0

    # 타입 충돌 해소
    if len(tables) > 1:
        col_types: dict[str, set] = {}
        for t in tables:
            for field in t.schema:
                col_types.setdefault(field.name, set()).add(field.type)
        conflict_cols = {name for name, types in col_types.items() if len(types) > 1}
        if conflict_cols:
            logger.info(f"{base_msg} 타입 충돌 컬럼 통일: {conflict_cols}")
            unified = []
            for t in tables:
                for col_name in conflict_cols:
                    if col_name in t.column_names:
                        idx = t.schema.get_field_index(col_name)
                        ftype = t.schema.field(idx).type
                        has_numeric = any(
                            pa.types.is_floating(tp) or pa.types.is_integer(tp)
                            for tp in col_types[col_name]
                        )
                        if has_numeric and (pa.types.is_string(ftype) or pa.types.is_large_string(ftype)):
                            col = pa.compute.cast(t.column(idx), pa.float64(), safe=False)
                            t = t.set_column(idx, col_name, col)
                        elif has_numeric and pa.types.is_null(ftype):
                            col = pa.nulls(len(t), type=pa.float64())
                            t = t.set_column(idx, col_name, col)
                unified.append(t)
            tables = unified

    combined = pa.concat_tables(tables, promote_options="permissive")

    # recv_ts 정렬 + 중복 제거
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
        except Exception as e:
            logger.warning(f"{base_msg} 정렬/중복제거 실패: {e}")
    combined = pa.Table.from_pandas(df, preserve_index=False)

    _progress(f"{base_msg} 저장 중...")
    pq.write_table(combined, final_path, compression="zstd", use_dictionary=True)

    # ★ parts를 backup으로 이동하지 않음 (그대로 유지)

    return combined.num_rows


def _resolve_target_date() -> str:
    if TARGET_DATE:
        return TARGET_DATE
    return datetime.now(KST).strftime("%y%m%d")


def _merge_target(target: dict, filter_date: str = "") -> list[str]:
    """병합 수행. 생성된 스냅샷 파일 경로 리스트 반환."""
    result_paths: list[str] = []
    parts_dir: Path = target["parts"]
    out_dir: Path = target["out_dir"]
    pattern: str = target["pattern"]

    if not parts_dir.exists():
        logger.info(f"[{target['label']}] parts 폴더 없음: {parts_dir}")
        return result_paths

    all_parts = sorted(parts_dir.glob("*.parquet"))
    if not all_parts:
        logger.info(f"[{target['label']}] 병합할 parts 파일 없음")
        return result_paths

    date_groups: dict[str, list[Path]] = defaultdict(list)
    for p in all_parts:
        date_prefix = p.name[:6]
        date_groups[date_prefix].append(p)

    if filter_date:
        if filter_date in date_groups:
            date_groups = {filter_date: date_groups[filter_date]}
        else:
            logger.info(f"[{target['label']}] 날짜 {filter_date}에 해당하는 parts 없음")
            return result_paths

    total_dates = len(date_groups)
    logger.info(
        f"[{target['label']}] parts {sum(len(v) for v in date_groups.values())}개 / "
        f"날짜 {total_dates}개 ({', '.join(sorted(date_groups.keys()))})"
    )

    for idx, (date_str, date_parts) in enumerate(sorted(date_groups.items()), 1):
        progress_msg = f"  [{idx}/{total_dates}] {date_str}"
        final_name = pattern.replace("{date}", date_str)
        final_path = out_dir / final_name
        n = _merge_snapshot(
            final_path, sorted(date_parts),
            progress_msg=progress_msg,
        )
        _progress("")
        print(f"  [{idx}/{total_dates}] {date_str} ✓ 스냅샷 완료: {n:,} rows → {final_path.name}")
        if n > 0:
            result_paths.append(str(final_path))

    return result_paths


def main() -> None:
    data_dir = BASE_DIR / "data"
    if not data_dir.exists():
        raise FileNotFoundError(f"data 디렉토리 없음: {data_dir}")

    choice = None
    if len(sys.argv) > 1:
        choice = sys.argv[1].strip()
    elif MERGE_TARGET is not None:
        choice = str(MERGE_TARGET)

    if choice is None:
        print("=" * 50)
        print(" WSS Parts 중간 병합 (스냅샷)")
        print("=" * 50)
        for i, t in enumerate(TARGETS, 1):
            parts_dir = t["parts"]
            cnt = len(list(parts_dir.glob("*.parquet"))) if parts_dir.exists() else 0
            print(f"  {i}. {t['label']:20s}  (parts: {cnt}개)")
        print(f"  0. 전체")
        print("-" * 50)
        choice = input("번호 선택 (0~2): ").strip()

    if choice == "0":
        selected = list(range(len(TARGETS)))
    elif choice in ("1", "2"):
        selected = [int(choice) - 1]
    else:
        print(f"잘못된 입력: {choice}")
        return

    filter_date = _resolve_target_date()
    print(f"\n 대상 날짜: {filter_date}")
    print(f" ★ 스냅샷 모드: parts 파일 유지 (backup 이동 없음)")

    all_paths: list[str] = []
    for idx in selected:
        t = TARGETS[idx]
        print(f"\n{'─' * 50}")
        print(f" [{idx+1}] {t['label']} 스냅샷 병합")
        print(f"{'─' * 50}")
        paths = _merge_target(t, filter_date=filter_date)
        all_paths.extend(paths)

    print(f"\n{'=' * 50}")
    print(" 스냅샷 병합 완료")
    print(f"{'=' * 50}")

    # ★ 로컬에서 파싱 가능한 결과 출력 (SSH 원격 실행 시 파일 경로 획득용)
    for p in all_paths:
        print(f"SNAPSHOT_PATH={p}")


if __name__ == "__main__":
    main()
