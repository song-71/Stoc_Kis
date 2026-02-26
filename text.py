#!/usr/bin/env python3
"""
wss_data parquet 파일에서 지정한 컬럼만 추출해 CSV로 저장
"""
from pathlib import Path

import pandas as pd

# =============================================================================
PARQUET_PATH = Path(__file__).resolve().parent / "data" / "wss_data" / "260213_wss_data.parquet"
OUT_CSV_PATH = Path(__file__).resolve().parent / "data" / "temp" / "260213_wss_data_temp.csv"
# =============================================================================

COLUMNS = [
    "mksc_shrn_iscd",
    "name",
    "NXT",
    "recv_ts",
    "hour",
    "recv_tm",
    "tr_id",
    "is_real_ccnl",
    "stck_cntg_hour",
    "stck_prpr",
    "askp1",
    "bidp1",
    "cntg_cls_code",
]


def main() -> None:
    df = pd.read_parquet(PARQUET_PATH)

    # buyer/seller/center 분류 (askp1, bidp1 필요)
    center_mask = None
    if "askp1" in df.columns and "bidp1" in df.columns:
        prpr = pd.to_numeric(df["stck_prpr"], errors="coerce")
        askp = pd.to_numeric(df["askp1"], errors="coerce")
        bidp = pd.to_numeric(df["bidp1"], errors="coerce")
        buyer = (prpr >= askp) & prpr.notna() & askp.notna()
        seller = (prpr <= bidp) & prpr.notna() & bidp.notna()
        center_mask = (prpr > bidp) & (prpr < askp) & prpr.notna() & askp.notna() & bidp.notna()
        print("=" * 50)
        print("buyer (stck_prpr >= askp1):", f"{buyer.sum():,}")
        print("seller (stck_prpr <= bidp1):", f"{seller.sum():,}")
        print("center (bidp1 < stck_prpr < askp1):", f"{center_mask.sum():,}")
        print("합계:", f"{buyer.sum() + seller.sum() + center_mask.sum():,}")
        print("=" * 50)

    # 기본 컬럼 선택 (parquet에 존재하는 것만)
    base_cols = [
        "mksc_shrn_iscd",
        "name",
        "recv_ts",
        "tr_id",
        "is_real_ccnl",
        "stck_cntg_hour",
        "stck_prpr",
        "cntg_cls_code",
    ]
    if "askp1" in df.columns:
        base_cols.append("askp1")
    if "bidp1" in df.columns:
        base_cols.append("bidp1")
    out = df[base_cols].copy()

    # center인 데이터만 필터
    if center_mask is not None:
        out = out[center_mask]

    # NXT: symbol_master와 조인
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from kis_utils import load_symbol_master
        sdf = load_symbol_master()
        sdf = sdf.rename(columns={"code": "mksc_shrn_iscd", "nxt": "NXT"})
        sdf = sdf[["mksc_shrn_iscd", "NXT"]]
        out = out.merge(sdf, on="mksc_shrn_iscd", how="left")
        out["NXT"] = out["NXT"].fillna("")
    except Exception:
        out["NXT"] = ""

    # hour: stck_cntg_hour에서 시(hour) 추출 (HHMMSS → HH)
    out["hour"] = out["stck_cntg_hour"].astype(str).str.zfill(6).str[:2]

    # recv_tm: recv_ts에서 시간 부분만 추출
    out["recv_tm"] = out["recv_ts"].astype(str).str.split().str[-1].str[:12]

    # 최종 순서
    out = out[COLUMNS]

    OUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"저장 완료: {OUT_CSV_PATH} ({len(out):,}행)")


if __name__ == "__main__":
    main()
