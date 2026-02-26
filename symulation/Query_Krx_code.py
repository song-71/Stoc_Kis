""" KRX 종목 코드를 조회하는 프로그램 (daily_KRX_code_DB 기반)
  - 종목코드 또는 종목명을 여러개 입력하면 해당 일자의 "종목코드, 종목명, NXT 여부, 시장, 종목상태"를 조회하여 출력
  - QUERY_DATE: None=오늘, "2026-02-23"=해당일자, "all"=해당 종목의 모든 날짜별 현황
  - --date: CLI로도 지정 가능 (QUERY_DATE보다 우선)
"""

# 검색 날짜 (None=오늘, "2026-02-23" 등 YYYY-MM-DD, "all"=전체 날짜별 현황)
QUERY_DATE = "all"

CODE_TEXT = """
예선테크
"""

import argparse
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

import pandas as pd

from kis_utils import load_kis_data_layout, print_table

KST = ZoneInfo("Asia/Seoul")


def _normalize_code(val: str) -> str:
    return str(val).strip().zfill(6)


def _lookup_daily_krx_all(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """해당 종목의 모든 날짜별 행 반환 (date, code, name, nxt, market, iscd_stat_cls_code)"""
    q = str(query).strip()
    if not q or df.empty:
        return pd.DataFrame()
    code_norm = _normalize_code(q)
    out_cols = ["date", "code", "name", "nxt", "market", "iscd_stat_cls_code"]
    # code 매칭
    df_code = df[df["code"].astype(str).str.zfill(6) == code_norm]
    if not df_code.empty:
        sub = df_code[out_cols].copy()
        sub["date"] = pd.to_datetime(sub["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
        return sub.sort_values("date").reset_index(drop=True)
    # name 매칭
    df_name = df[df["name"].astype(str).str.upper() == q.upper()]
    if not df_name.empty:
        sub = df_name[out_cols].copy()
        sub["date"] = pd.to_datetime(sub["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
        return sub.sort_values("date").reset_index(drop=True)
    return pd.DataFrame()


def _lookup_daily_krx(df: pd.DataFrame, query: str) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    """daily_KRX_code_DB 필터된 df에서 code/name으로 검색 → (code, name, nxt, market, iscd_stat_cls_code)"""
    q = str(query).strip()
    if not q or df.empty:
        return None, None, None, None, None
    code_norm = _normalize_code(q)
    # code 매칭
    df_code = df[df["code"].astype(str).str.zfill(6) == code_norm]
    if not df_code.empty:
        row = df_code.iloc[0]
        nxt = str(row.get("nxt", "N"))
        mkt = row.get("market") if pd.notna(row.get("market")) and str(row.get("market", "")) not in ("", "nan") else None
        stat = row.get("iscd_stat_cls_code") if pd.notna(row.get("iscd_stat_cls_code")) and str(row.get("iscd_stat_cls_code", "")) not in ("", "nan") else None
        return code_norm, str(row.get("name", "")), nxt, str(mkt) if mkt else None, str(stat) if stat else None
    # name 매칭 (대소문자 무시)
    df_name = df[df["name"].astype(str).str.upper() == q.upper()]
    if not df_name.empty:
        row = df_name.iloc[0]
        code_val = _normalize_code(str(row.get("code", "")))
        nxt = str(row.get("nxt", "N"))
        mkt = row.get("market") if pd.notna(row.get("market")) and str(row.get("market", "")) not in ("", "nan") else None
        stat = row.get("iscd_stat_cls_code") if pd.notna(row.get("iscd_stat_cls_code")) and str(row.get("iscd_stat_cls_code", "")) not in ("", "nan") else None
        return code_val, str(row.get("name", "")), nxt, str(mkt) if mkt else None, str(stat) if stat else None
    return None, None, None, None, None


def main() -> None:
    default_date = QUERY_DATE if QUERY_DATE else datetime.now(KST).strftime("%Y-%m-%d")
    ap = argparse.ArgumentParser(description="daily_KRX_code_DB에서 해당 일자 종목 조회")
    ap.add_argument("--date", default=default_date, help='조회일자 (YYYY-MM-DD, "all"=전체 날짜별 현황)')
    ap.add_argument("--config", default="config.json", help="config 경로")
    args = ap.parse_args()
    args.date = str(args.date).strip().lower()

    config_path = args.config
    if not os.path.isabs(config_path):
        config_path = os.path.join(BASE_DIR, config_path)
    layout = load_kis_data_layout(config_path)
    db_path = os.path.join(layout.local_root, "admin", "symbol_master", "daily_KRX_code_DB.parquet")

    if not os.path.exists(db_path):
        print(f"[오류] daily_KRX_code_DB 없음: {db_path}")
        sys.exit(1)

    df = pd.read_parquet(db_path)
    if "date" not in df.columns:
        print("[오류] date 컬럼 없음")
        sys.exit(1)

    df["_dt"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m%d")

    if args.date == "all":
        # 해당 종목의 모든 날짜별 현황
        all_rows = []
        for q in CODE_TEXT.split("\n"):
            q = q.strip()
            if not q:
                continue
            sub = _lookup_daily_krx_all(df, q)
            if not sub.empty:
                for _, row in sub.iterrows():
                    stat = row.get("iscd_stat_cls_code")
                    if pd.isna(stat) or str(stat) in ("", "nan"):
                        stat = None
                    all_rows.append({
                        "date": str(row.get("date", "")),
                        "code": str(row.get("code", "")),
                        "name": str(row.get("name", "")),
                        "nxt": str(row.get("nxt", "N")),
                        "market": str(row.get("market", "")),
                        "iscd_stat_cls_code": stat,
                    })
            else:
                all_rows.append({"date": "-", "code": "-", "name": q, "nxt": "-", "market": "-", "iscd_stat_cls_code": "-"})
        print(f"\n[모든 날짜별 현황] (daily_KRX_code_DB)")
        columns = ["date", "code", "name", "nxt", "market", "iscd_stat_cls_code"]
        align = {"date": "left", "code": "right", "name": "left", "nxt": "center", "market": "left", "iscd_stat_cls_code": "right"}
        print_table(all_rows, columns, align)
    else:
        # 단일 날짜 조회
        date_str = args.date.replace("-", "")
        df_day = df[df["_dt"] == date_str].copy()

        if df_day.empty:
            avail = df["_dt"].dropna().unique()[:5].tolist()
            print(f"[알림] {args.date} 데이터 없음. daily_KRX_code_DB에 해당 일자 데이터가 있는지 확인하세요.")
            print(f"  존재하는 날짜 샘플: {avail}")

        rows = []
        for q in CODE_TEXT.split("\n"):
            q = q.strip()
            if not q:
                continue
            code, name, nxt, market, status = _lookup_daily_krx(df_day, q)
            rows.append({"code": code, "name": name, "nxt": nxt, "market": market, "iscd_stat_cls_code": status})

        print(f"\n[날짜] {args.date} (daily_KRX_code_DB)")
        columns = ["code", "name", "nxt", "market", "iscd_stat_cls_code"]
        align = {"code": "right", "name": "left", "nxt": "center", "market": "left", "iscd_stat_cls_code": "right"}
        print_table(rows, columns, align)


if __name__ == "__main__":
    main()
