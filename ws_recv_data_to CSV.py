"""
WSS 웹소켓 수신 데이터(parquet) 조회 도구
  - 병합된 최종 parquet 파일을 읽어 종목별 필터링 후 CSV로 추출
  - 커맨드라인: python ws_query_tr_data_DB.py --date 20260209 --symbol 삼성전자

사용 예:
  python ws_query_tr_data_DB.py                     # 오늘 날짜, 전체 종목
"""
# ── 옵션 설정 ────────────────────────────────────────────────────────
#  조회 대상 parquet 소스 선택
#   1 : parquet-2 (수신저장)     → {YYMMDD}_wss_data.parquet
#   2 : Trading_test             → {YYMMDD}_wss_data_tr.parquet
#   3 : KRX100                   → krx100/{YYMMDD}_KRX100_wss_data.parquet
QUERY_SOURCE = 1

#  조회 날짜 (None이면 --date 인자 또는 오늘 날짜 사용)
#   예: "20260209", "2026-02-09", None
QUERY_DATE = "20260213"

#  조회 종목 (None이면 --symbol 인자 또는 전체 종목)
#   예: "삼성전자", "005930", None
QUERY_SYMBOL = None
# ─────────────────────────────────────────────────────────────────────

import argparse
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import time

import pandas as pd
import pyarrow.lib as pa_lib

from kis_utils import load_symbol_master


KST = ZoneInfo("Asia/Seoul")
DATA_DIR = Path("/home/ubuntu/Stoc_Kis/data/wss_data")
TEMP_DIR = Path("/home/ubuntu/Stoc_Kis/temp")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# 소스별 파일명 패턴
_SOURCE_MAP = {
    1: ("", "{yymmdd}_wss_data.parquet"),
    2: ("", "{yymmdd}_wss_data_tr.parquet"),
    3: ("krx100", "{yymmdd}_KRX100_wss_data.parquet"),
}


def _read_parquet_with_retry(path: Path, retries: int = 3, delay_sec: float = 2.0) -> pd.DataFrame:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return pd.read_parquet(path)
        except pa_lib.ArrowInvalid as e:
            last_err = e
            if attempt < retries:
                time.sleep(delay_sec)
                continue
            break
    msg = str(last_err) if last_err else "unknown error"
    raise RuntimeError(
        f"parquet read failed: {path} ({msg}). "
        "파일이 아직 쓰는 중이거나 손상되었을 수 있습니다."
    )


def _read_with_fallback(path: Path) -> pd.DataFrame:
    try:
        return _read_parquet_with_retry(path)
    except RuntimeError:
        fallback = path.with_name(path.stem + "_old" + path.suffix)
        if fallback.exists():
            print(f"[warn] read failed, fallback to: {fallback}")
            return _read_parquet_with_retry(fallback)
        raise


def _normalize_date(text: str | None) -> str:
    if not text:
        return datetime.now(KST).strftime("%Y%m%d")
    cleaned = text.strip()
    if "-" in cleaned:
        return datetime.strptime(cleaned, "%Y-%m-%d").strftime("%Y%m%d")
    if len(cleaned) == 8 and cleaned.isdigit():
        return cleaned
    raise ValueError("date must be YYYYMMDD or YYYY-MM-DD")


def _resolve_code(symbol_or_name: str | None) -> str | None:
    if not symbol_or_name:
        return None
    raw = symbol_or_name.strip()
    if not raw:
        return None
    if raw.isdigit():
        return raw.zfill(6)
    sdf = load_symbol_master()
    sdf["code"] = sdf["code"].astype(str).str.zfill(6)
    name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"]))
    return name_to_code.get(raw)


def _resolve_parquet_path(source: int, date_str: str) -> Path:
    """QUERY_SOURCE와 날짜로 parquet 파일 경로를 결정."""
    if source not in _SOURCE_MAP:
        raise ValueError(f"QUERY_SOURCE는 1, 2, 3 중 하나여야 합니다 (현재: {source})")
    subdir, pattern = _SOURCE_MAP[source]
    yymmdd = date_str[2:]  # YYYYMMDD → YYMMDD
    filename = pattern.replace("{yymmdd}", yymmdd)
    if subdir:
        return DATA_DIR / subdir / filename
    return DATA_DIR / filename


def main() -> None:
    parser = argparse.ArgumentParser(description="WSS parquet 데이터 조회 도구")
    parser.add_argument("--date", help="YYYYMMDD or YYYY-MM-DD (default: today)")
    parser.add_argument("--symbol", help="종목명 또는 종목코드")
    args = parser.parse_args()

    # 상단 옵션 > 커맨드라인 인자 > 기본값
    date_str = _normalize_date(QUERY_DATE or args.date)
    symbol_input = QUERY_SYMBOL or args.symbol

    parquet_path = _resolve_parquet_path(QUERY_SOURCE, date_str)
    print(f"[조회] source={QUERY_SOURCE} date={date_str} file={parquet_path.name}")

    if not parquet_path.exists():
        raise FileNotFoundError(f"parquet not found: {parquet_path}")
    df = _read_parquet_with_retry(parquet_path)

    code = _resolve_code(symbol_input)
    if symbol_input and not code:
        print(f"[warn] symbol not found: {symbol_input}")
        df = df.head(0)
    if code:
        # WSS 데이터의 종목코드 컬럼 후보
        code_col = None
        for cand in ("mksc_shrn_iscd", "stck_shrn_iscd", "code", "isu_cd"):
            if cand in df.columns:
                code_col = cand
                break
        if code_col:
            df = df[df[code_col].astype(str).str.zfill(6) == code]
        else:
            print(f"[warn] 종목코드 컬럼을 찾을 수 없음 (columns: {list(df.columns[:5])}...)")

    print(f"[결과] {len(df)} rows, {len(df.columns)} cols")
    print(df)
    # 원본 parquet과 같은 폴더에 CSV 저장
    out_path = parquet_path.with_suffix(".csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[csv] saved: {out_path}")


if __name__ == "__main__":
    main()
