import argparse
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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


def _to_float(val: Any) -> float:
    try:
        return float(val)
    except Exception:
        return 0.0


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


def _reset_token(client: KisClient) -> None:
    client.access_token = None
    client.token_expire_dt = None
    try:
        if os.path.exists(client.cfg.token_cache_path):
            os.remove(client.cfg.token_cache_path)
    except Exception:
        pass


def _get_balance_page(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    ctx_fk: str = "",
    ctx_nk: str = "",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str, str]:
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
    headers = client._headers(tr_id=tr_id)
    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "00",
        "CTX_AREA_FK100": ctx_fk,
        "CTX_AREA_NK100": ctx_nk,
    }

    last_err: Optional[Exception] = None
    retried_for_token = False
    while True:
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            if r.status_code != 200:
                try:
                    j = r.json()
                except Exception:
                    raise RuntimeError(f"잔고조회 HTTP {r.status_code}: {r.text}")
                if (
                    str(j.get("msg_cd")) == "EGW00123"
                    and "token" in str(j.get("msg1", "")).lower()
                    and not retried_for_token
                ):
                    _reset_token(client)
                    headers = client._headers(tr_id=tr_id)
                    retried_for_token = True
                    continue
                raise RuntimeError(f"잔고조회 HTTP {r.status_code}: {j}")

            j = r.json()
            if str(j.get("rt_cd")) != "0":
                if (
                    str(j.get("msg_cd")) == "EGW00123"
                    and "token" in str(j.get("msg1", "")).lower()
                    and not retried_for_token
                ):
                    _reset_token(client)
                    headers = client._headers(tr_id=tr_id)
                    retried_for_token = True
                    continue
                raise RuntimeError(f"잔고조회 실패: {j.get('msg1')} raw={j}")
        except Exception as e:
            last_err = e
            raise
        break

    if last_err:
        raise last_err

    output1 = j.get("output1") or []
    output2 = j.get("output2") or {}
    if isinstance(output1, dict):
        output1 = [output1]
    if not isinstance(output2, dict):
        output2 = {}

    next_fk = j.get("ctx_area_fk100") or output2.get("ctx_area_fk100") or ""
    next_nk = j.get("ctx_area_nk100") or output2.get("ctx_area_nk100") or ""
    return output1, output2, str(next_fk or ""), str(next_nk or "")


def _sell_market(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id: str,
    code: str,
    qty: int,
) -> Dict[str, Any]:
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    body = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "PDNO": code,
        "ORD_DVSN": "01",  # 시장가
        "ORD_QTY": str(qty),
        "ORD_UNPR": "0",
    }
    headers = client._headers(tr_id=tr_id)
    headers["hashkey"] = _hashkey(client.cfg.base_url, client.cfg.appkey, client.cfg.appsecret, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"매도 주문 실패: {j.get('msg1')} raw={j}")
    return j


def main() -> None:
    ap = argparse.ArgumentParser(description="보유 종목 조회 후 시장가 매도")
    ap.add_argument("--config", default="config.json", help="설정 파일 경로")
    ap.add_argument("--mock", action="store_true", help="모의투자 TR-ID 사용")
    ap.add_argument("--account-id", default=None, help="accounts 하위 계정 ID 선택")
    args = ap.parse_args()

    config_path = args.config
    if not isinstance(config_path, str):
        config_path = "config.json"
    if not config_path.startswith("/"):
        config_path = config_path.strip()
    if not config_path or config_path == "config.json":
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
    tr_id_balance = cfg.get("tr_id_balance")
    tr_id_sell = cfg.get("tr_id_sell")
    if args.mock:
        tr_id_balance = tr_id_balance or "VTTC8434R"
        tr_id_sell = tr_id_sell or "VTTC0801U"
        base_url = cfg.get("base_url") or "https://openapivts.koreainvestment.com:29443"
    else:
        tr_id_balance = tr_id_balance or "TTTC8434R"
        tr_id_sell = tr_id_sell or "TTTC0801U"

    kis_cfg = KisConfig(
        appkey=appkey,
        appsecret=appsecret,
        base_url=base_url,
        custtype=custtype,
    )
    client = KisClient(kis_cfg)

    ctx_fk = ""
    ctx_nk = ""
    page_idx = 1
    seen_codes: set[str] = set()
    while True:
        rows, summary, ctx_fk, ctx_nk = _get_balance_page(
            client, cano, acnt_prdt_cd, tr_id_balance, ctx_fk=ctx_fk, ctx_nk=ctx_nk
        )
        if not rows:
            if page_idx == 1:
                print("[잔고] 보유 종목이 없습니다.")
            break

        holdings = []
        for row in rows:
            qty = _to_int(_pick(row, "hldg_qty", "hldg_qty", "hldg_qty", default=0))
            if qty <= 0:
                continue
            code = str(_pick(row, "pdno", "pdno", "symbol", "iscd", default="")).strip()
            name = str(_pick(row, "prdt_name", "prdt_nm", "prdt_name", "pdno_name", default="")).strip()
            cur_price = _to_float(_pick(row, "prpr", "stck_prpr", "prpr", default=0))
            eval_amt = _to_float(_pick(row, "evlu_amt", "evlu_amt", "evlu_pfls_smtl_amt", default=0))
            holdings.append(
                {
                    "code": code,
                    "name": name,
                    "qty": qty,
                    "price": cur_price,
                    "eval_amt": eval_amt,
                }
            )

        if not holdings:
            if not ctx_fk and not ctx_nk:
                break
            page_idx += 1
            continue

        df = pd.DataFrame(holdings)
        df = (
            df.sort_values(["code", "name"])
            .drop_duplicates(subset=["code", "name"], keep="first")
            .reset_index(drop=True)
        )
        current_codes = set(df["code"].astype(str).tolist())
        new_codes = current_codes - seen_codes
        if not new_codes and page_idx > 1:
            print("[잔고] 동일 페이지 반복 감지 -> 조회 종료")
            break
        seen_codes.update(current_codes)
        print(f"[잔고] 보유 종목 (page {page_idx})")
        print(df.to_string(index=False))

        holdings = df.to_dict("records")
        while True:
            page_action = input("[페이지] 1.매도 2.보유 3.모두 매도 4.모두 보유 선택: ").strip()
            if page_action in ("1", "2", "3", "4"):
                break

        if page_action == "4":
            pass
        elif page_action == "3":
            for h in holdings:
                code = h["code"]
                name = h["name"] or "-"
                qty = h["qty"]
                if not code or qty <= 0:
                    continue
                try:
                    resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, qty)
                    order_no = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                    print(f"[매도] {code} {name} qty={qty} 주문완료 odno={order_no}")
                except Exception as e:
                    print(f"[매도실패] {code} {name} qty={qty} error={e}")
        else:
            for h in holdings:
                code = h["code"]
                name = h["name"] or "-"
                qty = h["qty"]
                if not code or qty <= 0:
                    continue
                while True:
                    ans = input(f"[{code} {name}] 1.매도 2.보유 선택: ").strip()
                    if ans in ("1", "2"):
                        break
                if ans == "2":
                    continue
                try:
                    resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, qty)
                    order_no = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                    print(f"[매도] {code} {name} qty={qty} 주문완료 odno={order_no}")
                except Exception as e:
                    print(f"[매도실패] {code} {name} qty={qty} error={e}")

        if not ctx_fk and not ctx_nk:
            break
        while True:
            go_next = input("[다음페이지] 1.계속 2.종료 선택: ").strip()
            if go_next in ("1", "2"):
                break
        if go_next == "2":
            break
        page_idx += 1

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[done] {ts}")


if __name__ == "__main__":
    main()
