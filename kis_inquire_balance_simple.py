#!/usr/bin/env python3
"""KIS 계좌 잔고 조회 (가장 심플)."""
import os
import requests
from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig
from kis_utils import resolve_account_config

cfg = resolve_account_config(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"))
client = KisClient(KisConfig(
    appkey=cfg["appkey"], appsecret=cfg["appsecret"], base_url=cfg.get("base_url") or DEFAULT_BASE_URL,
    custtype=cfg.get("custtype") or "P",
    token_cache_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "kis_token_main.json")))

r = requests.get(
    f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-balance",
    headers=client._headers(tr_id="TTTC8434R"),
    params={
        "CANO": cfg.get("cano") or cfg.get("CANO"),
        "ACNT_PRDT_CD": cfg.get("acnt_prdt_cd") or cfg.get("ACNT_PRDT_CD") or "01",
        "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02", "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00",
        "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
    },
    timeout=10)
j = r.json()
o2 = j.get("output2") or {}
if isinstance(o2, list) and o2:
    o2 = o2[0]
s = o2 if isinstance(o2, dict) else {}

def f(v): return float(str(v or 0).replace(",", "")) or 0

print(f"출금가능: {f(s.get('dnca_tot_amt')):,.0f}  주문가능: {f(s.get('prvs_rcdl_excc_amt') or s.get('ord_psbl_cash')):,.0f}  총평가: {f(s.get('tot_evlu_amt')):,.0f}")
out1 = j.get("output1") or []
if isinstance(out1, dict):
    out1 = [out1]
for row in out1:
    qty = int(f(row.get("hldg_qty")) or 0)
    if qty > 0:
        print(f"  {row.get('pdno')} {row.get('prdt_name')} {qty}주 {f(row.get('prpr')):,.0f}원")
