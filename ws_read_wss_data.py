"""
지정한 날짜·종목·시간 범위의 wss_data를 parquet에서 읽어 data/temp/read_wss_data.csv로 저장하는 프로그램
"""
# =============================================================================
DATA_DATE = "260213"  # 사용할 일자(YYMMDD)

# 시간 범위 옵션: stck_cntg_hour(HHMMSS) 기준 필터
# None이면 해당 방향 필터 없음. 예: "090000" ~ "100000" → 09:00:00~10:00:00
TIME_START = "085900"  # 시작 시각 (None=필터 없음)
TIME_END = "093500"    # 종료 시각 (None=필터 없음)

#260213 수신대상 종목(업종) 목록 : 26개
CODE_TEXT = """
SKAI
"""
"""
삼진식품
아로마티카
오리엔트바이오
태광산업
뉴인텍
제이케이시냅스
오리엔트정공
제이티
에스코넥
아이센스
현대ADM
유투바이오
원익IPS
이지스
서남
SKAI
레이저쎌
씨케이솔루션

"""

import time
from pathlib import Path

import polars as pl

from kis_utils import load_symbol_master

# =============================================================================
# 경로
# =============================================================================
SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / "data" / "wss_data"
TEMP_DIR = SCRIPT_DIR / "data" / "temp"
OUT_CSV = TEMP_DIR / "read_wss_data.csv"


def _parse_codes(text: str) -> list[str]:
    name_to_code = {}
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"]))
    except Exception:
        name_to_code = {}
    codes: list[str] = []
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        if raw.isdigit():
            codes.append(raw.zfill(6))
            continue
        code = name_to_code.get(raw)
        if code:
            codes.append(code)
    return codes


# def _code_name_map() -> dict[str, str]:
#     """wss_data parquet에 name 컬럼이 있으면 사용, 없을 때만 사용"""
#     try:
#         sdf = load_symbol_master()
#         sdf["code"] = sdf["code"].astype(str).str.zfill(6)
#         return dict(zip(sdf["code"], sdf["name"].astype(str)))
#     except Exception:
#         return {}


def read_wss_to_csv() -> None:
    """지정 조건의 wss_data를 parquet에서 읽어 CSV로 저장."""
    codes = _parse_codes(CODE_TEXT)
    if not codes:
        print("[read_wss] CODE_TEXT에서 종목코드를 찾지 못했습니다.")
        return

    target_set = set(codes)

    src_path = OUT_DIR / f"{DATA_DATE}_wss_data.parquet"
    if not src_path.exists():
        print(f"[read_wss] parquet not found: {src_path}")
        return

    print("[read_wss] 옵션: DATA_DATE={} TIME_START={} TIME_END={} codes={}".format(
        DATA_DATE, TIME_START, TIME_END, codes
    ))
    print(f"[read_wss] loading {src_path.name} codes={len(codes)}")

    # LazyFrame + predicate pushdown
    lf = pl.scan_parquet(src_path)
    schema_cols = lf.collect_schema().names()

    # 종목코드 컬럼 탐색
    code_col = None
    for cand in ("mksc_shrn_iscd", "stck_shrn_iscd", "code"):
        if cand in schema_cols:
            code_col = cand
            break

    start_time = time.perf_counter()

    # LazyFrame: 종목 + 시간 필터를 collect 전에 한 번에 pushdown → 디스크에서 조건에 맞는 행만 읽음
    if code_col:
        lf = lf.filter(
            pl.col(code_col).cast(pl.Utf8).str.zfill(6).is_in(target_set)
        )
    if "stck_cntg_hour" in schema_cols and (TIME_START or TIME_END):
        hour_col = pl.col("stck_cntg_hour").cast(pl.Utf8).str.zfill(6)
        if TIME_START:
            lf = lf.filter(hour_col >= TIME_START)
        if TIME_END:
            lf = lf.filter(hour_col <= TIME_END)

    df = lf.collect()

    if df.is_empty():
        print("[read_wss] 필터 후 데이터 없음")
        return

    # mksc_shrn_iscd 없을 때 code/stck_shrn_iscd를 mksc_shrn_iscd로 alias
    if "mksc_shrn_iscd" not in df.columns and code_col:
        df = df.with_columns(pl.col(code_col).alias("mksc_shrn_iscd"))

    # 컬럼 순서 지정 (존재하는 컬럼만)
    COL_ORDER = [
        "mksc_shrn_iscd", "name", "recv_ts", "tr_id", "is_real_ccnl", "stck_cntg_hour", "stck_prpr",
        "prdy_vrss_sign", "prdy_vrss", "prdy_ctrt", "wghn_avrg_stck_prc", "stck_oprc", "stck_hgpr", "stck_lwpr",
        "askp1", "bidp1", "cntg_vol", "acml_vol", "acml_tr_pbmn", "seln_cntg_csnu", "shnu_cntg_csnu", "ntby_cntg_csnu",
        "cttr", "seln_cntg_smtn", "shnu_cntg_smtn", "cntg_cls_code", "shnu_rate", "prdy_vol_vrss_acml_vol_rate",
        "oprc_hour", "oprc_vrss_prpr_sign", "oprc_vrss_prpr", "hgpr_hour", "hgpr_vrss_prpr_sign", "hgpr_vrss_prpr",
        "lwpr_hour", "lwpr_vrss_prpr_sign", "lwpr_vrss_prpr", "bsop_date", "new_mkop_cls_code", "trht_yn",
        "askp_rsqn1", "bidp_rsqn1", "total_askp_rsqn", "total_bidp_rsqn", "vol_tnrt", "prdy_smns_hour_acml_vol",
        "prdy_smns_hour_acml_vol_rate", "hour_cls_code", "mrkt_trtm_cls_code",
    ]
    out_cols = [c for c in COL_ORDER if c in df.columns]
    if out_cols:
        df = df.select(out_cols)

    load_elapsed = time.perf_counter() - start_time
    print(f"[read_wss] 데이터 로드 소요시간: {load_elapsed:.3f}초")

    # 저장 (utf-8-sig: Excel 등에서 한글 깨짐 방지)
    save_start = time.perf_counter()
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    df.write_csv(OUT_CSV, include_bom=True)
    save_elapsed = time.perf_counter() - save_start
    total_elapsed = time.perf_counter() - start_time

    print(f"[read_wss] csv 파일 저장 소요시간: {save_elapsed:.3f}초")
    print(f"[read_wss] 총 소요시간: {total_elapsed:.3f}초")
    print(f"[read_wss] saved {len(df):,} rows -> {OUT_CSV}")


if __name__ == "__main__":
    read_wss_to_csv()
