"""
open id 조회/정리 - 미체결 주문 조회 및 취소
(kis_Trading_1_LmUp_str.py mode 2 에서 추출)
"""
import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pandas as pd
import requests

from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig
from kis_utils import load_config, resolve_account_config


def _pick(row: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return default


def _to_int(val: Any) -> int:
    try:
        return int(float(val))
    except Exception:
        return 0


def _to_str(val: Any) -> str:
    try:
        if val is None:
            return ""
        return str(val).strip()
    except Exception:
        return ""


def _hashkey(base_url: str, appkey: str, appsecret: str, body: Dict[str, Any]) -> str:
    url = f"{base_url}/uapi/hashkey"
    headers = {
        "content-type": "application/json",
        "appkey": appkey,
        "appsecret": appsecret,
    }
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    hk = j.get("HASH") or j.get("hash")
    if not hk:
        raise RuntimeError(f"hashkey 실패: {j}")
    return hk


def _inquire_psbl_rvsecncl(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    timeout_sec: float = 10.0,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """주식정정취소가능주문조회"""
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"
    headers = client._headers(tr_id=tr_id)
    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "INQR_DVSN": "00",
        "SLL_BUY_DVSN_CD": "00",
        "INQR_STRT_DT": "",
        "INQR_END_DT": "",
        "PDNO": "",
        "ORD_GNO_BRNO": "",
        "ODNO": "",
        "INQR_DVSN_1": "0",
        "INQR_DVSN_2": "0",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }
    out: List[Dict[str, Any]] = []
    last_raw: Dict[str, Any] = {}
    for _ in range(100):
        r = requests.get(url, headers=headers, params=params, timeout=timeout_sec)
        r.raise_for_status()
        j = r.json()
        last_raw = j
        if str(j.get("rt_cd")) != "0":
            raise RuntimeError(f"정정/취소 가능 주문 조회 실패: {j.get('msg1')} raw={j}")
        output1 = j.get("output1") or []
        if isinstance(output1, dict):
            output1 = [output1]
        out.extend(output1)
        ctx_fk = j.get("ctx_area_fk100") or ""
        ctx_nk = j.get("ctx_area_nk100") or ""
        if not ctx_fk and not ctx_nk:
            break
        params["CTX_AREA_FK100"] = ctx_fk
        params["CTX_AREA_NK100"] = ctx_nk
    return out, last_raw


def _cancel_order(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    ordno: str,
    code: str,
    qty: int,
) -> Dict[str, Any]:
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-rvsecncl"
    body = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "KRX_FWDG_ORD_ORGNO": "00000",
        "ORGN_ORDNO": ordno,
        "ORD_DVSN": "00",
        "RVSE_CNCL_DVSN_CD": "02",
        "ORD_QTY": str(qty),
        "ORD_UNPR": "0",
        "PDNO": code,
    }
    headers = client._headers(tr_id=tr_id)
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"주문취소 실패: {j.get('msg1')} raw={j}")
    return j


def run_open_id_cleanup(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_open: str,
    tr_id_cancel: str,
) -> None:
    if not tr_id_open:
        print("[open_id] tr_id_open을 config.json에 설정하세요.")
        return
    print(f"[open_id] 정정/취소 가능 주문 조회 시작 CANO={cano}")
    orders, _ = _inquire_psbl_rvsecncl(client, cano, acnt_prdt_cd, tr_id_open)
    if not orders:
        print("[open_id] 미체결 없음")
        return
    print("[open_id] 미체결 주문")
    df = pd.DataFrame(orders)
    cols = [c for c in ["odno", "ordno", "pdno", "ord_qty", "ord_unpr", "ord_tmd"] if c in df.columns]
    df_out = df[cols] if cols else df
    dedupe_keys = [c for c in ["ord_dt", "ord_tmd", "pdno", "ordno", "ord_qty", "ord_unpr"] if c in df_out.columns]
    if dedupe_keys:
        df_out = df_out.drop_duplicates(subset=dedupe_keys).reset_index(drop=True)
    print(df_out)
    while True:
        ans = input("[정리] 미체결 전부 취소? 1.예 2.아니오: ").strip()
        if ans in ("1", "2"):
            break
    if ans != "1":
        return
    for row in orders:
        ordno = _to_str(_pick(row, "odno", "ordno", default=""))
        code = _to_str(_pick(row, "pdno", default=""))
        qty = _to_int(_pick(row, "ord_qty", default=0))
        if not ordno or not code or qty <= 0:
            continue
        try:
            _cancel_order(client, cano, acnt_prdt_cd, tr_id_cancel, ordno, code, qty)
            print(f"[미체결취소] {code} ordno={ordno} qty={qty}")
        except Exception as e:
            print(f"[취소실패] {code} ordno={ordno} error={e}")


def main() -> None:
    ap = argparse.ArgumentParser(description="open id 조회/정리 (미체결 주문 조회 및 취소)")
    ap.add_argument("--config", default="config.json", help="설정 파일 경로")
    ap.add_argument("--account-id", default=None, help="accounts 하위 계정 ID 선택")
    args = ap.parse_args()

    config_path = args.config
    if not isinstance(config_path, str):
        config_path = "config.json"
    if not config_path.startswith("/") and not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_path)

    cfg = resolve_account_config(config_path, account_id=args.account_id)
    appkey = cfg.get("appkey")
    appsecret = cfg.get("appsecret")
    if not appkey or not appsecret:
        raise ValueError("appkey/appsecret이 필요합니다. config.json에 설정하세요.")

    cano = cfg.get("cano") or cfg.get("CANO")
    acnt_prdt_cd = cfg.get("acnt_prdt_cd") or cfg.get("ACNT_PRDT_CD")
    if not cano or not acnt_prdt_cd:
        raise ValueError("CANO/ACNT_PRDT_CD가 필요합니다. config.json에 설정하세요.")

    base_url = cfg.get("base_url") or DEFAULT_BASE_URL
    custtype = cfg.get("custtype") or "P"
    tr_id_open = cfg.get("tr_id_open") or "TTTC0084R"
    tr_id_cancel = cfg.get("tr_id_cancel") or "TTTC0803U"

    kis_cfg = KisConfig(
        appkey=appkey,
        appsecret=appsecret,
        base_url=base_url,
        custtype=custtype,
    )
    client = KisClient(kis_cfg)

    run_open_id_cleanup(client, cano, acnt_prdt_cd, tr_id_open, tr_id_cancel)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[done] {ts}")


if __name__ == "__main__":
    main()
