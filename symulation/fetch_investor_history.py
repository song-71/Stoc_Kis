"""
[260427] Phase 8 Step 2 — 외인/기관 매매현황 1년 backfill.

KIS API: investor-trade-by-stock-daily (TR_ID: FHPTJ04160001)
- 1 페이지 = 30 영업일 (검증됨)
- 1년 = 9 호출/종목 (calendar 50일 간격으로 9개 입력 일자)
- 전종목 (ST) ≈ 2500 → 22,500 호출 / 18 RPS = ~21분

출력: data/investor_data/kis_investor_unified_parquet_DB.parquet
컬럼: date, symbol, name, frgn/orgn/prsn 매매 + 기관 세부 + OHLCV

사용법:
  python3 symulation/fetch_investor_history.py --limit 100   # 100종목 시범
  python3 symulation/fetch_investor_history.py               # 전체
  python3 symulation/fetch_investor_history.py --code 005930  # 단일 종목 테스트
"""
import argparse
import json
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

SCRIPT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = SCRIPT_DIR / "config.json"
KRX_CODE_PATH = SCRIPT_DIR / "data" / "admin" / "symbol_master" / "KRX_code.csv"
OUTPUT_PATH = SCRIPT_DIR / "data" / "investor_data" / "kis_investor_unified_parquet_DB.parquet"

API_URL_PATH = "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"
TR_ID = "FHPTJ04160001"
SLEEP_PER_CALL = 0.06   # 18 RPS rate limit (60/18 ≈ 56ms)

# 백테스트 핵심 컬럼만 보존 (output2 의 101개 중)
KEEP_COLUMNS = [
    "stck_bsop_date",      # 영업일
    # OHLCV
    "stck_clpr", "stck_oprc", "stck_hgpr", "stck_lwpr",
    "acml_vol", "acml_tr_pbmn", "prdy_ctrt",
    # 외인
    "frgn_ntby_qty", "frgn_ntby_tr_pbmn",
    "frgn_reg_ntby_qty", "frgn_nreg_ntby_qty",
    # 기관
    "orgn_ntby_qty", "orgn_ntby_tr_pbmn",
    "scrt_ntby_qty", "ivtr_ntby_qty", "bank_ntby_qty",
    "insu_ntby_qty", "fund_ntby_qty", "mrbn_ntby_qty",
    # 개인
    "prsn_ntby_qty", "prsn_ntby_tr_pbmn",
    # 기타
    "etc_corp_ntby_vol", "etc_orgt_ntby_vol",
]


def _load_main_credentials() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f)
    base_url = cfg.get("base_url", "https://openapi.koreainvestment.com:9443")
    main_acct = cfg.get("users", {}).get("sywems12", {}).get("accounts", {}).get("a1", {})
    return {
        "base_url": base_url,
        "appkey": main_acct.get("appkey", ""),
        "appsecret": main_acct.get("appsecret", ""),
    }


def _get_access_token(creds: dict) -> str:
    cache = SCRIPT_DIR / "data" / "admin" / f"access_token_{creds['appkey'][-6:]}.json"
    if cache.exists():
        try:
            with open(cache) as f:
                d = json.load(f)
            exp = d.get("access_token_token_expired", "")
            if exp and datetime.strptime(exp, "%Y-%m-%d %H:%M:%S") > datetime.now() + timedelta(minutes=5):
                return d["access_token"]
        except Exception:
            pass
    body = {
        "grant_type": "client_credentials",
        "appkey": creds["appkey"],
        "appsecret": creds["appsecret"],
    }
    r = requests.post(f"{creds['base_url']}/oauth2/tokenP", json=body, timeout=10)
    r.raise_for_status()
    j = r.json()
    if "access_token" not in j:
        raise RuntimeError(f"token 발급 실패: {j}")
    # cache 저장
    cache.parent.mkdir(parents=True, exist_ok=True)
    j["access_token_token_expired"] = (
        datetime.now() + timedelta(seconds=int(j.get("expires_in", 3600)))
    ).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(cache, "w") as f:
            json.dump(j, f)
    except Exception:
        pass
    return j["access_token"]


def call_one(creds: dict, token: str, code: str, input_date: str) -> list[dict]:
    """단일 종목 단일 입력일자 호출 → output2 list."""
    url = f"{creds['base_url']}{API_URL_PATH}"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": creds["appkey"],
        "appsecret": creds["appsecret"],
        "tr_id": TR_ID,
        "custtype": "P",
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
        "FID_INPUT_DATE_1": input_date,
        "FID_ORG_ADJ_PRC": "",
        "FID_ETC_CLS_CODE": "",
    }
    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        return []
    out2 = j.get("output2") or []
    return out2 if isinstance(out2, list) else [out2]


def fetch_one_code_year(creds: dict, token: str, code: str, name: str) -> list[dict]:
    """1 종목의 1년치 (9개 입력일자) 누적."""
    today = datetime.now()
    # 50 calendar 일 간격으로 9 등분 (≈ 35 영업일/구간 → 30 영업일 페이지 충분히 커버)
    input_dates = []
    for i in range(9):
        d = today - timedelta(days=i * 50)
        input_dates.append(d.strftime("%Y%m%d"))

    rows: list[dict] = []
    seen_dates: set[str] = set()
    for in_date in input_dates:
        try:
            out2 = call_one(creds, token, code, in_date)
        except Exception as e:
            print(f"  [{code}] {in_date} 호출 실패: {e}")
            time.sleep(SLEEP_PER_CALL)
            continue
        for r in out2:
            d = str(r.get("stck_bsop_date", ""))
            if not d or d in seen_dates:
                continue
            seen_dates.add(d)
            kept = {k: r.get(k) for k in KEEP_COLUMNS if k in r}
            kept["symbol"] = code
            kept["name"] = name
            rows.append(kept)
        time.sleep(SLEEP_PER_CALL)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="처리 종목 수 제한 (0=전체)")
    ap.add_argument("--code", type=str, default="", help="단일 종목 테스트 (예: 005930)")
    ap.add_argument("--out", type=str, default="", help="출력 parquet 경로 override")
    args = ap.parse_args()

    out_path = Path(args.out) if args.out else OUTPUT_PATH
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[investor_backfill] 시작 — out={out_path}")

    # 종목 list 로드 (group=ST 만, 휴장/관리 등은 그대로 포함 — 백테스트는 과거 데이터 필요)
    krx = pd.read_csv(KRX_CODE_PATH, dtype=str)
    krx.columns = [c.strip().lstrip("﻿") for c in krx.columns]
    krx["code"] = krx["code"].astype(str).str.zfill(6)
    if "group" in krx.columns:
        krx = krx[krx["group"].fillna("").str.upper() == "ST"]
    krx = krx[["code", "name"]].drop_duplicates("code").reset_index(drop=True)

    if args.code:
        krx = krx[krx["code"] == args.code.zfill(6)]
        print(f"  단일 종목 모드: {args.code}")
    elif args.limit > 0:
        krx = krx.head(args.limit)
        print(f"  시범 모드: 첫 {args.limit}종목")
    else:
        print(f"  전체 종목: {len(krx)}")

    creds = _load_main_credentials()
    token = _get_access_token(creds)

    all_rows: list[dict] = []
    t0 = time.time()
    for i, row in enumerate(krx.itertuples(index=False), 1):
        code = row.code
        name = row.name
        try:
            rows = fetch_one_code_year(creds, token, code, name)
            all_rows.extend(rows)
        except Exception as e:
            print(f"  [{code}/{name}] 예외: {e}\n{traceback.format_exc()[:200]}")
        if i % 50 == 0:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(krx) - i) / rate if rate > 0 else 0
            print(
                f"  [{i}/{len(krx)}] {code}/{name} | rows={len(all_rows):,} | "
                f"elapsed={elapsed/60:.1f}분 | ETA={eta/60:.1f}분"
            )

    if not all_rows:
        print("[investor_backfill] 수집된 행 없음 — 종료")
        return

    df = pd.DataFrame(all_rows)
    # 숫자 컬럼 변환 (가능한 것만)
    for col in df.columns:
        if col in ("symbol", "name", "stck_bsop_date"):
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["fetched_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 기존 parquet 와 merge (있으면)
    if out_path.exists():
        try:
            existing = pd.read_parquet(out_path)
            combined = pd.concat([existing, df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["stck_bsop_date", "symbol"], keep="last")
            df = combined
            print(f"  기존 행 {len(existing):,} 와 merge → 총 {len(df):,}")
        except Exception as e:
            print(f"  기존 parquet merge 실패 (덮어쓰기): {e}")

    df = df.sort_values(["stck_bsop_date", "symbol"]).reset_index(drop=True)
    df.to_parquet(out_path, index=False)
    elapsed = time.time() - t0
    print(
        f"\n[investor_backfill] 완료 — 행 {len(df):,}, "
        f"종목 {df['symbol'].nunique()}, "
        f"일자 {df['stck_bsop_date'].nunique()}, "
        f"소요 {elapsed/60:.1f}분 → {out_path}"
    )


if __name__ == "__main__":
    main()
