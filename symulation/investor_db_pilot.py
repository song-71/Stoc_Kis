"""
[260427] Phase 8 Step 1 — 외인/기관 매매현황 DB 파일럿.

KIS API `investor-trade-by-stock-daily` (TR_ID: FHPTJ04160001) 를
1 종목 (005930 삼성전자) 으로 호출하여:
  - 응답 schema (output1/output2 컬럼)
  - 1 페이지 일수
  - tr_cont 페이지네이션 동작
  - 누적 backfill 가능 일수
검증.

Reference: /tmp/kis-open-trading-api/examples_llm/domestic_stock/investor_trade_by_stock_daily/investor_trade_by_stock_daily.py
"""
import json
import time
import sys
import requests
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = SCRIPT_DIR / "config.json"

API_URL_PATH = "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"
TR_ID = "FHPTJ04160001"
SAMPLE_CODE = "005930"   # 삼성전자


def _load_main_credentials() -> dict:
    """config.json 에서 main 계정 appkey/secret/base_url 추출."""
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
    """OAuth2 token 발급 (cache 우선)."""
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
    return j["access_token"]


def call_investor_trade_daily(
    creds: dict,
    token: str,
    code: str,
    input_date: str,
    tr_cont: str = "",
) -> tuple[dict, dict]:
    """1회 호출. 반환: (response_json, response_headers)"""
    url = f"{creds['base_url']}{API_URL_PATH}"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": creds["appkey"],
        "appsecret": creds["appsecret"],
        "tr_id": TR_ID,
        "custtype": "P",
    }
    if tr_cont:
        headers["tr_cont"] = tr_cont
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",   # J:KRX
        "FID_INPUT_ISCD": code,
        "FID_INPUT_DATE_1": input_date,
        "FID_ORG_ADJ_PRC": "",
        "FID_ETC_CLS_CODE": "",
    }
    r = requests.get(url, headers=headers, params=params, timeout=10)
    return r.json(), dict(r.headers)


def run_pilot():
    print(f"\n{'='*80}")
    print(f"[investor_pilot] 시작 — 종목: {SAMPLE_CODE}, API: investor-trade-by-stock-daily")
    print(f"{'='*80}\n")

    creds = _load_main_credentials()
    token = _get_access_token(creds)
    today = datetime.now().strftime("%Y%m%d")

    # ── 1차 호출 ──
    print(f"[1차 호출] FID_INPUT_DATE_1={today}, tr_cont=''")
    j1, h1 = call_investor_trade_daily(creds, token, SAMPLE_CODE, today, "")
    rt_cd = j1.get("rt_cd")
    print(f"  rt_cd={rt_cd}, msg={j1.get('msg1', '')}")
    if rt_cd != "0":
        print(f"  [실패] 응답 raw: {json.dumps(j1, ensure_ascii=False)[:500]}")
        return
    out1 = j1.get("output1") or {}
    out2 = j1.get("output2") or []
    if isinstance(out1, list):
        out1_count = len(out1)
        out1_sample = out1[0] if out1 else {}
    else:
        out1_count = 1 if out1 else 0
        out1_sample = out1
    out2_count = len(out2) if isinstance(out2, list) else (1 if out2 else 0)

    print(f"\n  output1 항목 수: {out1_count}")
    if out1_sample:
        print(f"  output1 keys ({len(out1_sample)}개):")
        for k, v in list(out1_sample.items())[:30]:
            print(f"    {k}: {v}")

    print(f"\n  output2 항목 수: {out2_count}")
    if out2 and isinstance(out2, list):
        sample = out2[0]
        print(f"  output2[0] keys ({len(sample)}개):")
        for k, v in list(sample.items())[:30]:
            print(f"    {k}: {v}")
        # 일자 분포
        date_keys = [k for k in sample.keys() if "date" in k.lower() or k.lower() in ("dt", "stck_bsop_date")]
        print(f"\n  output2 가능한 date 컬럼: {date_keys}")
        if date_keys:
            dk = date_keys[0]
            dates = [str(r.get(dk, "")) for r in out2 if r.get(dk)]
            print(f"  output2 일자 범위: {dates[0] if dates else '?'} ~ {dates[-1] if dates else '?'} ({len(dates)}일)")

    # tr_cont 헤더
    tr_cont_resp = h1.get("tr_cont", "").strip()
    print(f"\n  응답 헤더 tr_cont = '{tr_cont_resp}' (M/F 면 다음 페이지 있음)")

    if tr_cont_resp in ("M", "F"):
        print(f"\n[2차 호출] tr_cont='N' (다음 페이지 요청)")
        time.sleep(0.1)
        j2, h2 = call_investor_trade_daily(creds, token, SAMPLE_CODE, today, "N")
        out2b = j2.get("output2") or []
        out2b_count = len(out2b) if isinstance(out2b, list) else (1 if out2b else 0)
        print(f"  output2 항목 수: {out2b_count}")
        tr_cont_resp2 = h2.get("tr_cont", "").strip()
        print(f"  응답 헤더 tr_cont = '{tr_cont_resp2}'")

        if isinstance(out2b, list) and out2b:
            sample2 = out2b[0]
            date_keys2 = [k for k in sample2.keys() if "date" in k.lower() or k.lower() in ("dt", "stck_bsop_date")]
            if date_keys2:
                dk = date_keys2[0]
                dates2 = [str(r.get(dk, "")) for r in out2b if r.get(dk)]
                print(f"  output2 일자 범위: {dates2[0] if dates2 else '?'} ~ {dates2[-1] if dates2 else '?'} ({len(dates2)}일)")

    # ── 비용 추정 ──
    if out2_count > 0:
        per_page_days = out2_count
        days_per_year = 250   # 영업일
        pages_per_code_1y = (days_per_year + per_page_days - 1) // per_page_days
        codes_total = 2500
        calls_total = codes_total * pages_per_code_1y
        seconds = calls_total / 18.0
        print(f"\n{'='*80}")
        print(f"[비용 추정] (output2 1페이지 = {per_page_days}일 가정)")
        print(f"  종목당 1년치 페이지: {pages_per_code_1y}")
        print(f"  전종목 (2500) 1년치 호출: {calls_total:,}건")
        print(f"  rate limit 18 RPS 시 소요: {seconds/60:.1f}분")
        print(f"{'='*80}\n")


if __name__ == "__main__":
    run_pilot()
