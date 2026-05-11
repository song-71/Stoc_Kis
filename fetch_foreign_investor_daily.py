"""
[260427] Phase 8 Step 3 — 외인/기관 매매현황 일일 cron 스크립트.

매일 16:30 cron 으로 실행 → 오늘 영업일 데이터만 수집 (전종목 × 1 호출).
unified parquet 에 append (기존 행 dedup).

비용: 2500종목 × 1 호출 = ~2.5분 (rate limit 18 RPS + sleep 0.06)

사용법:
  python3 fetch_foreign_investor_daily.py                       # 오늘 데이터 수집
  python3 fetch_foreign_investor_daily.py --date 20260424       # 특정 일자 강제 수집

cron 등록 (예시):
  30 16 * * 1-5 /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/fetch_foreign_investor_daily.py >> /home/ubuntu/Stoc_Kis/out/logs/investor_daily.log 2>&1
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

# [260427] 프로젝트 루트로 이동됨 → parent 1단계만 (이전: symulation/ → parent.parent)
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "config.json"
KRX_CODE_PATH = SCRIPT_DIR / "data" / "admin" / "symbol_master" / "KRX_code.csv"
OUTPUT_PATH = SCRIPT_DIR / "data" / "investor_data" / "kis_investor_unified_parquet_DB.parquet"

API_URL_PATH = "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"
TR_ID = "FHPTJ04160001"
SLEEP_PER_CALL = 0.06

# fetch_investor_history.py 와 동일
KEEP_COLUMNS = [
    "stck_bsop_date",
    "stck_clpr", "stck_oprc", "stck_hgpr", "stck_lwpr",
    "acml_vol", "acml_tr_pbmn", "prdy_ctrt",
    "frgn_ntby_qty", "frgn_ntby_tr_pbmn",
    "frgn_reg_ntby_qty", "frgn_nreg_ntby_qty",
    "orgn_ntby_qty", "orgn_ntby_tr_pbmn",
    "scrt_ntby_qty", "ivtr_ntby_qty", "bank_ntby_qty",
    "insu_ntby_qty", "fund_ntby_qty", "mrbn_ntby_qty",
    "prsn_ntby_qty", "prsn_ntby_tr_pbmn",
    "etc_corp_ntby_vol", "etc_orgt_ntby_vol",
]


def _load_main_credentials() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f)
    base_url = cfg.get("base_url", "https://openapi.koreainvestment.com:9443")
    main_acct = cfg.get("users", {}).get("sywems12", {}).get("accounts", {}).get("main", {})
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=str, default="", help="입력 일자 (YYYYMMDD, 기본: 오늘)")
    ap.add_argument("--limit", type=int, default=0, help="처리 종목 수 제한 (0=전체)")
    args = ap.parse_args()

    target_date = args.date if args.date else datetime.now().strftime("%Y%m%d")
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"[investor_daily] 시작 — date={target_date}, out={OUTPUT_PATH}")

    krx = pd.read_csv(KRX_CODE_PATH, dtype=str)
    krx.columns = [c.strip().lstrip("﻿") for c in krx.columns]
    krx["code"] = krx["code"].astype(str).str.zfill(6)
    if "group" in krx.columns:
        krx = krx[krx["group"].fillna("").str.upper() == "ST"]
    krx = krx[["code", "name"]].drop_duplicates("code").reset_index(drop=True)
    if args.limit > 0:
        krx = krx.head(args.limit)
    print(f"  대상 종목: {len(krx)}")

    creds = _load_main_credentials()
    token = _get_access_token(creds)

    # 오늘 일자 데이터만 수집 (1 페이지 30일치 받지만 target_date 행만 keep)
    rows: list[dict] = []
    t0 = time.time()
    for i, row in enumerate(krx.itertuples(index=False), 1):
        code, name = row.code, row.name
        try:
            out2 = call_one(creds, token, code, target_date)
            for r in out2:
                d = str(r.get("stck_bsop_date", ""))
                if d != target_date:
                    continue
                kept = {k: r.get(k) for k in KEEP_COLUMNS if k in r}
                kept["symbol"] = code
                kept["name"] = name
                rows.append(kept)
                break  # target_date 행 1개만
        except Exception as e:
            print(f"  [{code}/{name}] 호출 실패: {e}")
        time.sleep(SLEEP_PER_CALL)
        if i % 100 == 0:
            print(f"  [{i}/{len(krx)}] {code}/{name} | rows={len(rows):,} | elapsed={(time.time()-t0)/60:.1f}분")

    if not rows:
        print(f"[investor_daily] 수집된 행 없음 (휴장일이거나 API 응답 없음) — 종료")
        return

    df = pd.DataFrame(rows)
    for col in df.columns:
        if col in ("symbol", "name", "stck_bsop_date"):
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["fetched_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 기존 parquet 와 merge
    if OUTPUT_PATH.exists():
        try:
            existing = pd.read_parquet(OUTPUT_PATH)
            combined = pd.concat([existing, df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["stck_bsop_date", "symbol"], keep="last")
            df = combined
        except Exception as e:
            print(f"  기존 parquet merge 실패 (덮어쓰기): {e}")

    df = df.sort_values(["stck_bsop_date", "symbol"]).reset_index(drop=True)

    # [260427] 해당 일자 순매수 순위 컬럼 추가 (frgn/orgn/prsn/frgn_orgn_rank)
    try:
        from compute_investor_ranks import compute_ranks_for_dates
        df = compute_ranks_for_dates(df, dates=[target_date])
    except Exception as e:
        print(f"  [ranks] 계산 실패 (rank 없이 저장): {e}")

    df.to_parquet(OUTPUT_PATH, index=False)
    elapsed = time.time() - t0
    today_n = (df["stck_bsop_date"] == target_date).sum()
    print(
        f"\n[investor_daily] 완료 — 오늘 {target_date} 행 {today_n}, "
        f"전체 {len(df):,} | 종목 {df['symbol'].nunique()} | "
        f"일자 {df['stck_bsop_date'].nunique()} | 소요 {elapsed/60:.1f}분"
    )


if __name__ == "__main__":
    main()
