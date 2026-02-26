#!/usr/bin/env python3
"""
KIS REST API 현재가 조회 단건 테스트
- 지정 종목에 대해 /uapi/domestic-stock/v1/quotations/inquire-price 호출
- 응답 전체(JSON raw)를 출력하여 stck_tr_stp_yn 등 필드 확인
"""

import json
import sys
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from kis_utils import load_config  # noqa: E402
from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig  # noqa: E402

# ── 테스트 대상 종목 ──
TEST_CODES = [
    # 오늘 09:15 추가 → 09:25 WSS 미수신 해제된 종목
    ("058400", "KNN"),
    ("001510", "SK증권"),
    ("051780", "큐로홀딩스"),
    ("038530", "케이바이오"),
    ("080220", "제주반도체"),
    # 오늘 09:30 추가 → 09:40 WSS 미수신 해제된 종목
    ("331920", "셀레믹스"),
    ("241520", "DSC인베스트먼트"),
    ("001290", "상상인증권"),
    ("004060", "SG세계물산"),
    ("088350", "한화생명"),
]

# ── config.json에서 인증 정보 로드 ──
cfg = load_config(str(SCRIPT_DIR / "config.json"))
appkey = cfg.get("appkey")
appsecret = cfg.get("appsecret")
base_url = cfg.get("base_url") or DEFAULT_BASE_URL
custtype = cfg.get("custtype") or "P"
market_div = cfg.get("market_div") or "J"

if not appkey or not appsecret:
    print("ERROR: config.json에 appkey/appsecret이 없습니다.")
    sys.exit(1)

client = KisClient(KisConfig(
    appkey=appkey,
    appsecret=appsecret,
    base_url=base_url,
    custtype=custtype,
    market_div=market_div,
    token_cache_path=str(SCRIPT_DIR / "kis_token_main.json"),
))
client.ensure_token()

print("=" * 80)
print(f"KIS 현재가 조회 테스트 (TR_ID: FHKST01010100)")
print(f"base_url: {base_url}")
print("=" * 80)

for code, name in TEST_CODES:
    print(f"\n{'─' * 70}")
    print(f"▶ {name} ({code})")
    print(f"{'─' * 70}")

    url = f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = client._headers(tr_id="FHKST01010100")
    params = {
        "FID_COND_MRKT_DIV_CODE": market_div,
        "FID_INPUT_ISCD": code,
    }

    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        print(f"  HTTP Status: {r.status_code}")

        if r.status_code != 200:
            print(f"  ERROR: HTTP {r.status_code}")
            print(f"  Response: {r.text[:500]}")
            continue

        j = r.json()
        rt_cd = j.get("rt_cd")
        msg1 = j.get("msg1")
        output = j.get("output") or {}

        if rt_cd != "0":
            print(f"  API 오류: {msg1}")
            continue

        # 핵심 판정
        temp_stop = output.get('temp_stop_yn', '?')
        sltr = output.get('sltr_yn', '?')
        pr_val = output.get('stck_prpr', '')
        vol_val = output.get('acml_vol', '')
        mrkt_warn = output.get('mrkt_warn_cls_code', '?')
        invt_caful = output.get('invt_caful_yn', '?')

        judgments = []
        if str(temp_stop).upper() == "Y":
            judgments.append("일시정지")
        if str(sltr).upper() == "Y":
            judgments.append("정리매매")
        if not str(pr_val).strip() or str(pr_val).strip() == "0" \
           or not str(vol_val).strip() or str(vol_val).strip() == "0":
            judgments.append("거래없음")
        if str(mrkt_warn) == "03":
            judgments.append("투자위험")
        if str(invt_caful).upper() == "Y":
            judgments.append("투자주의")

        status = " / ".join(judgments) if judgments else "정상"
        stck_mxpr = output.get('stck_mxpr', '')
        stck_llam = output.get('stck_llam', '')
        prdy_vrss_sign = output.get('prdy_vrss_sign', '')
        is_upper = (str(pr_val).strip() == str(stck_mxpr).strip() and str(stck_mxpr).strip() not in ('', '0'))
        print(f"  판정: {status}")
        print(f"  현재가={pr_val} 거래량={vol_val} 전일비={output.get('prdy_ctrt')}%")
        print(f"  temp_stop={temp_stop} sltr={sltr} warn={mrkt_warn} caful={invt_caful}")
        print(f"  stck_mxpr(상한가)={stck_mxpr} stck_llam(하한가)={stck_llam} prdy_vrss_sign={prdy_vrss_sign}")
        print(f"  ★ 상한가여부: {'YES' if is_upper else 'NO'}")

    except Exception as e:
        print(f"  EXCEPTION: {type(e).__name__}: {e}")

print(f"\n{'=' * 80}")
print("테스트 완료")
