"""
[260427] 외인/기관/개인 일자별 순매수 순위 컬럼 추가.

대상 parquet: data/investor_data/kis_investor_unified_parquet_DB.parquet
신규 4개 컬럼 (각 일자별 1=최대 순매수, 마지막=최대 순매도):
  - frgn_rank       : 외국인 순매수 수량 순위
  - orgn_rank       : 기관 순매수 수량 순위
  - prsn_rank       : 개인 순매수 수량 순위
  - frgn_orgn_rank  : 외국인+기관 합산 순매수 순위

사용법:
  # 전 일자 일괄 처리 (초기 backfill)
  python3 compute_investor_ranks.py

  # 특정 일자만 처리 (cron 통합용)
  python3 compute_investor_ranks.py --date 20260427

  # 모듈 import 사용 (fetch_foreign_investor_daily.py 에서)
  from compute_investor_ranks import compute_ranks_for_dates
  df = compute_ranks_for_dates(df, dates=["20260427"])
"""
import argparse
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_PARQUET = SCRIPT_DIR / "data" / "investor_data" / "kis_investor_unified_parquet_DB.parquet"

RANK_COLS = ("frgn_rank", "orgn_rank", "prsn_rank", "frgn_orgn_rank")


def compute_ranks_for_dates(df: pd.DataFrame, dates: list[str] | None = None) -> pd.DataFrame:
    """주어진 일자(들)에 대해 4개 rank 컬럼을 계산하여 df 갱신.

    Args:
        df: parquet 로드 결과 (stck_bsop_date, symbol, frgn_ntby_qty, orgn_ntby_qty, prsn_ntby_qty 포함)
        dates: 처리 일자 list (None=전 일자). 문자열 YYYYMMDD.

    Returns:
        rank 컬럼 추가된 df (원본 in-place 갱신).
    """
    # rank 컬럼 사전 준비 (없으면 생성)
    for col in RANK_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    # 처리 대상 일자
    if dates is None:
        target_dates = sorted(df["stck_bsop_date"].dropna().unique().tolist())
    else:
        target_dates = [str(d) for d in dates]

    if not target_dates:
        return df

    print(f"[ranks] 처리 일자: {len(target_dates)}건 ({target_dates[0]} ~ {target_dates[-1]})")

    # 외인+기관 합산 임시 컬럼 (메모리에만)
    df["_frgn_orgn_sum"] = (
        pd.to_numeric(df["frgn_ntby_qty"], errors="coerce").fillna(0)
        + pd.to_numeric(df["orgn_ntby_qty"], errors="coerce").fillna(0)
    )

    for i, d in enumerate(target_dates, 1):
        mask = df["stck_bsop_date"] == d
        sub = df.loc[mask].copy()
        if len(sub) == 0:
            continue

        # rank: ascending=False → 1=최대값, ties 는 method='min' (동일 순위 처리)
        sub["frgn_rank"] = pd.to_numeric(sub["frgn_ntby_qty"], errors="coerce").rank(
            ascending=False, method="min", na_option="bottom"
        ).astype("Int64")
        sub["orgn_rank"] = pd.to_numeric(sub["orgn_ntby_qty"], errors="coerce").rank(
            ascending=False, method="min", na_option="bottom"
        ).astype("Int64")
        sub["prsn_rank"] = pd.to_numeric(sub["prsn_ntby_qty"], errors="coerce").rank(
            ascending=False, method="min", na_option="bottom"
        ).astype("Int64")
        sub["frgn_orgn_rank"] = sub["_frgn_orgn_sum"].rank(
            ascending=False, method="min", na_option="bottom"
        ).astype("Int64")

        for col in RANK_COLS:
            df.loc[mask, col] = sub[col].values

        if i % 50 == 0 or i == len(target_dates):
            print(f"  [{i}/{len(target_dates)}] {d} 처리 완료 (종목 {len(sub)})")

    df = df.drop(columns=["_frgn_orgn_sum"])
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=str, default="", help="특정 일자 (YYYYMMDD)만 처리. 빈 값=전 일자")
    ap.add_argument("--parquet", type=str, default="", help="parquet 경로 override")
    args = ap.parse_args()

    parquet_path = Path(args.parquet) if args.parquet else DEFAULT_PARQUET
    if not parquet_path.exists():
        print(f"[ERROR] parquet not found: {parquet_path}")
        return

    print(f"[ranks] 로드: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    print(f"  shape: {df.shape}, 일자 {df['stck_bsop_date'].nunique()}, 종목 {df['symbol'].nunique()}")

    target_dates = [args.date] if args.date else None
    df = compute_ranks_for_dates(df, dates=target_dates)

    # 정렬 후 저장
    df = df.sort_values(["stck_bsop_date", "frgn_orgn_rank"], na_position="last").reset_index(drop=True)
    df.to_parquet(parquet_path, index=False)

    print(f"\n[ranks] 완료 — {parquet_path}")
    # 요약: 최근 1일 rank 통과 종목 수
    last_date = df["stck_bsop_date"].max()
    last_n = df[df["stck_bsop_date"] == last_date].shape[0]
    print(f"  최근 일자 {last_date}: {last_n} 종목 rank 부여")


if __name__ == "__main__":
    main()
