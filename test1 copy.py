#!/usr/bin/env python3
"""
1번 계좌조회 모드만 (kis_Trading_1_LmUp_str.py에서 추출)
- 잔고 조회 후 10초 간격 재조회
- 보유종목 정리(매도) 옵션 포함
"""
import argparse
import json
import os
import select
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig
from kis_utils import LogManager, TeeStdout, load_kis_data_layout, print_table, resolve_account_config, save_config

os.environ["TZ"] = "Asia/Seoul"
time.tzset()

CANO_KEY = "cano"
ORD_DVSN_IOC_MKT = "14"  # IOC 시장가

_LOG_MANAGER: Optional[LogManager] = None
_TRADE_LOG_PATH: Optional[str] = None


def _log(msg: str) -> None:
    if _LOG_MANAGER is None:
        print(msg)
        return
    _LOG_MANAGER.log(msg)


def _log_tm(msg: str) -> None:
    if _LOG_MANAGER is None:
        print(msg)
        return
    _LOG_MANAGER.log_tm(msg)


def _append_trade_log(
    action: str,
    code: str,
    name: str,
    qty: int,
    price: float,
    amount: float,
    pnl: float | None = None,
    ret: float | None = None,
    reason: str = "",
    ordno: str = "",
) -> None:
    if not _TRADE_LOG_PATH:
        return
    row = {
        "ts": _now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "action": action,
        "code": code,
        "name": name,
        "qty": qty,
        "price": f"{price:.0f}",
        "amount": f"{amount:.0f}",
        "pnl": "" if pnl is None else f"{pnl:.0f}",
        "ret": "" if ret is None else f"{ret:.4f}",
        "reason": reason,
        "ordno": ordno,
    }
    try:
        write_header = not os.path.exists(_TRADE_LOG_PATH)
        df = pd.DataFrame([row])
        df.to_csv(
            _TRADE_LOG_PATH,
            mode="a",
            header=write_header,
            index=False,
            encoding="utf-8-sig",
        )
    except Exception:
        pass


def _pick(row: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return default


def _to_int(val: Any) -> int:
    try:
        return int(float(str(val).replace(",", "") or 0))
    except Exception:
        return 0


def _to_float(val: Any) -> float:
    try:
        if isinstance(val, str):
            val = val.replace(",", "")
        return float(val)
    except Exception:
        return 0.0


def _to_str(val: Any) -> str:
    try:
        if val is None:
            return ""
        return str(val).strip()
    except Exception:
        return ""


def _print_table(rows: List[Dict[str, Any]], columns: List[str], align: Dict[str, str]) -> None:
    print_table(rows, columns, align)


def _now_kst() -> datetime:
    return datetime.now(timezone(timedelta(hours=9)))


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
    return_raw: bool = False,
    timeout_sec: float = 10.0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str, str] | Tuple[List[Dict[str, Any]], Dict[str, Any], str, str, Dict[str, Any]]:
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
    retried_for_rate = 0
    retried_for_conn = 0
    while True:
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout_sec)
            if r.status_code != 200:
                try:
                    j = r.json()
                except Exception:
                    raise RuntimeError(f"잔고조회 HTTP {r.status_code}: {r.text}")
                if str(j.get("msg_cd")) == "EGW00201" and retried_for_rate < 3:
                    retried_for_rate += 1
                    time.sleep(1.5 * retried_for_rate)
                    continue
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
                if str(j.get("msg_cd")) == "EGW00201" and retried_for_rate < 3:
                    retried_for_rate += 1
                    time.sleep(1.5 * retried_for_rate)
                    continue
                if (
                    str(j.get("msg_cd")) == "EGW00123"
                    and "token" in str(j.get("msg1", "")).lower()
                    and not retried_for_token
                ):
                    _reset_token(client)
                    headers = client._headers(tr_id=tr_id)
                    retried_for_token = True
                    continue
                msg = j.get("msg1")
                if str(j.get("msg_cd")) == "OPSQ2000" and "INVALID_CHECK_ACNO" in str(msg or ""):
                    raise RuntimeError(f"[잔고조회실패] 계좌번호 오류 CANO={cano} ACNT_PRDT_CD={acnt_prdt_cd}: {msg}")
                raise RuntimeError(f"잔고조회 실패: {msg} raw={j}")
        except requests.exceptions.RequestException as e:
            last_err = e
            if retried_for_conn < 3:
                retried_for_conn += 1
                time.sleep(1.0 * retried_for_conn)
                continue
            raise
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
    if isinstance(output2, list):
        pass
    elif not isinstance(output2, dict):
        output2 = {}

    next_fk = j.get("ctx_area_fk100")
    next_nk = j.get("ctx_area_nk100")
    if not next_fk and isinstance(output2, dict):
        next_fk = output2.get("ctx_area_fk100")
    if not next_nk and isinstance(output2, dict):
        next_nk = output2.get("ctx_area_nk100")
    if return_raw:
        return output1, output2, str(next_fk or ""), str(next_nk or ""), j
    return output1, output2, str(next_fk or ""), str(next_nk or "")


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
        "ORD_DVSN": ORD_DVSN_IOC_MKT,
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


def _format_holdings_rows(holdings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    df = pd.DataFrame(holdings)
    if df.empty:
        return []
    df = (
        df.sort_values(["code", "name"])
        .drop_duplicates(subset=["code", "name"], keep="first")
        .reset_index(drop=True)
    )
    df["qty"] = df["qty"].apply(lambda v: f"{_to_int(v):,d}")
    df["price"] = df["price"].apply(lambda v: f"{_to_float(v):,.0f}")
    df["eval_amt"] = df["eval_amt"].apply(lambda v: f"{_to_float(v):,.0f}")
    return df.to_dict("records")


def _run_holdings_flow(
    client: KisClient,
    cano: str,
    acnt_prdt_cd: str,
    tr_id_balance: str,
    tr_id_sell: str,
) -> None:
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
            qty = _to_int(_pick(row, "hldg_qty", default=0))
            if qty <= 0:
                continue
            code = _to_str(_pick(row, "pdno", default=""))
            name = _to_str(_pick(row, "prdt_name", default=""))
            cur_price = _to_float(_pick(row, "prpr", default=0))
            eval_amt = _to_float(_pick(row, "evlu_amt", default=0))
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

        rows = _format_holdings_rows(holdings)
        if not rows:
            break
        current_codes = set(r["code"] for r in rows)
        new_codes = current_codes - seen_codes
        if not new_codes and page_idx > 1:
            print("[잔고] 동일 페이지 반복 감지 -> 조회 종료")
            break
        seen_codes.update(current_codes)
        print(f"[잔고] 보유 종목 (page {page_idx})")
        _print_table(
            rows,
            ["code", "name", "qty", "price", "eval_amt"],
            {"qty": "right", "price": "right", "eval_amt": "right"},
        )

        holdings = rows
        did_sell = False
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
                qty = _to_int(h.get("qty", 0))
                if not code or qty <= 0:
                    continue
                try:
                    resp = _sell_market(client, cano, acnt_prdt_cd, tr_id_sell, code, qty)
                    order_no = _pick(resp.get("output", {}) or {}, "ODNO", "odno", default="")
                    price = _to_float(h.get("price", 0))
                    print(f"[매도] {code} {name} qty={qty} 주문완료 odno={order_no}")
                    _append_trade_log(
                        action="SELL",
                        code=code,
                        name=name,
                        qty=qty,
                        price=price,
                        amount=qty * price,
                        reason="manual_sell_all",
                        ordno=order_no,
                    )
                    did_sell = True
                except Exception as e:
                    print(f"[매도실패] {code} {name} qty={qty} error={e}")
        else:
            for h in holdings:
                code = h["code"]
                name = h["name"] or "-"
                qty = _to_int(h.get("qty", 0))
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
                    price = _to_float(h.get("price", 0))
                    print(f"[매도] {code} {name} qty={qty} 주문완료 odno={order_no}")
                    _append_trade_log(
                        action="SELL",
                        code=code,
                        name=name,
                        qty=qty,
                        price=price,
                        amount=qty * price,
                        reason="manual_sell",
                        ordno=order_no,
                    )
                    did_sell = True
                except Exception as e:
                    print(f"[매도실패] {code} {name} qty={qty} error={e}")

        if did_sell:
            time.sleep(3)
            ctx_fk = ""
            ctx_nk = ""
            page_idx = 1
            seen_codes = set()
            continue

        if not ctx_fk and not ctx_nk:
            break
        while True:
            go_next = input("[다음페이지] 1.계속 2.종료 선택: ").strip()
            if go_next in ("1", "2"):
                break
        if go_next == "2":
            break
        page_idx += 1


def main() -> None:
    ap = argparse.ArgumentParser(description="1번 계좌조회 모드")
    ap.add_argument("--config", default="config.json", help="설정 파일 경로")
    ap.add_argument("--cano-key", default=None, help="config에서 계좌키 선택")
    ap.add_argument("--account-id", default=None, help="accounts 하위 계정 ID")
    args = ap.parse_args()

    config_path = args.config
    if not isinstance(config_path, str) or not config_path or config_path == "config.json":
        config_path = "config.json"
    if not config_path.startswith("/") and not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_path)

    cfg = resolve_account_config(config_path, account_id=args.account_id)
    appkey = cfg.get("appkey")
    appsecret = cfg.get("appsecret")
    if not appkey or not appsecret:
        raise ValueError("appkey/appsecret이 필요합니다. config.json에 설정하세요.")

    cano_key = _to_str(args.cano_key) or CANO_KEY
    cano = cfg.get(cano_key) or cfg.get(cano_key.upper())
    acnt_prdt_cd = cfg.get("acnt_prdt_cd") or cfg.get("ACNT_PRDT_CD") or "01"
    if not cano or not acnt_prdt_cd:
        available = sorted([k for k in cfg.keys() if "cano" in k.lower() or k.lower() == "cano"])
        raise ValueError(f"CANO/ACNT_PRDT_CD가 필요합니다. 사용가능 키: {available}")

    base_url = cfg.get("base_url") or DEFAULT_BASE_URL
    custtype = cfg.get("custtype") or "P"
    tr_id_balance = cfg.get("tr_id_balance") or "TTTC8434R"
    tr_id_sell = cfg.get("tr_id_sell") or "TTTC0801U"

    config_dir = os.path.dirname(os.path.abspath(config_path))
    token_path = os.path.join(config_dir, "kis_token_main.json")

    global _LOG_MANAGER, _TRADE_LOG_PATH
    layout = load_kis_data_layout(config_path)
    log_dir = os.path.join(layout.local_root, "logs")
    daily_name = f"KIS_log_{_now_kst().strftime('%y%m%d')}.log"
    _LOG_MANAGER = LogManager(log_dir, log_name=daily_name, run_tag=f"test1 시작 {_now_kst().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📝 로그 파일: {_LOG_MANAGER.log_file}")
    sys.stdout = TeeStdout(_LOG_MANAGER.log_file)
    _TRADE_LOG_PATH = os.path.join(log_dir, "trade_log.csv")

    _log_tm("test1.py 시작 되었음.")
    _log(f"run_mode=1, cano_key={cano_key}")

    kis_cfg = KisConfig(
        appkey=appkey,
        appsecret=appsecret,
        base_url=base_url,
        custtype=custtype,
        token_cache_path=token_path,
    )
    client = KisClient(kis_cfg)

    try:
        rows, summary, _, _, raw = _get_balance_page(
            client, cano, acnt_prdt_cd, tr_id_balance, return_raw=True
        )
    except RuntimeError as e:
        print(str(e))
        return

    while True:
        print(f"[계좌] CANO={cano} ACNT_PRDT_CD={acnt_prdt_cd}")
        now_stamp = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now_stamp}] ===============================")
        print("[잔고요약]")
        summary_rows = summary if isinstance(summary, list) else [summary]
        stock_eval_amt = 0.0
        if summary_rows:
            for item in summary_rows:
                dnca = _to_float(_pick(item, "dnca_tot_amt", default=0))
                ord_cash = _to_float(_pick(item, "prvs_rcdl_excc_amt", default=0))
                tot_eval = _to_float(_pick(item, "tot_evlu_amt", default=0))
                pchs_amt = _to_float(_pick(item, "pchs_amt_smtl_amt", default=0))
                evlu_amt = _to_float(_pick(item, "evlu_amt_smtl_amt", default=0))
                evlu_pfls = _to_float(_pick(item, "evlu_pfls_smtl_amt", default=0))
                prev_tot_eval = _to_float(_pick(item, "prvs_tot_evlu_amt", "bfdy_tot_evlu_amt", default=0))
                stock_eval_amt = evlu_amt
                print(f"♠ 출금가능액 : {dnca:,.0f}   ♠ 주문가능금액 : {ord_cash:,.0f}")
                print(f"- 총 매입액(pchs_amt_smtl_amt): {pchs_amt:,.0f}")
                print(f"- 주식 평가액(evlu_amt_smtl_amt)-(a): {evlu_amt:,.0f}")
                print(f"- 주식 평가손익(evlu_pfls_smtl_amt)-(b): {evlu_pfls:,.0f}")
                print(f"- 주문가능금액(prvs_rcdl_excc_amt)-(c): {ord_cash:,.0f}")
                print(f"- 총 평가액(tot_evlu_amt)-(a+b+c): {tot_eval:,.0f}")
                if prev_tot_eval > 0:
                    diff_total = tot_eval - prev_tot_eval
                    diff_rate = diff_total / prev_tot_eval
                    print(f"- 전일대비 총 증감 : {diff_total:,.0f}({diff_rate:+.1%})")
        else:
            print("- summary 없음")

        output1 = rows if isinstance(rows, list) else []
        holdings = []
        for row in output1:
            qty = _to_int(_pick(row, "hldg_qty", default=0))
            if qty <= 0:
                continue
            code = _to_str(_pick(row, "pdno", default=""))
            name = _to_str(_pick(row, "prdt_name", default=""))
            cur_price = _to_float(_pick(row, "prpr", default=0))
            eval_amt = _to_float(_pick(row, "evlu_amt", default=0))
            buy_price = _to_float(_pick(row, "pchs_avg_pric", default=0))
            dif = cur_price - buy_price if buy_price > 0 else 0.0
            dif_rt = dif / buy_price * 100 if buy_price > 0 else 0.0
            holdings.append(
                {
                    "code": code,
                    "name": name,
                    "qty": qty,
                    "price": cur_price,
                    "eval_amt": eval_amt,
                    "buy_pr": buy_price,
                    "dif": dif,
                    "dif_rt": dif_rt,
                }
            )

        print(f"\n[보유종목] 총 {len(holdings)}종 주식평가액 {stock_eval_amt:,.0f}")
        if holdings:
            rows_out = []
            for h in holdings:
                rows_out.append(
                    {
                        "code": h["code"],
                        "name": h["name"],
                        "qty": f"{_to_int(h['qty']):,d}",
                        "price": f"{_to_float(h['price']):,.0f}",
                        "eval_amt": f"{_to_float(h['eval_amt']):,.0f}",
                        "buy_pr": f"{_to_float(h['buy_pr']):,.0f}",
                        "dif": f"{_to_float(h['dif']):,.0f}",
                        "dif_rt(%)": f"{_to_float(h['dif_rt']):.2f}",
                    }
                )
            _print_table(
                rows_out,
                ["code", "name", "qty", "price", "eval_amt", "buy_pr", "dif", "dif_rt(%)"],
                {"qty": "right", "price": "right", "eval_amt": "right", "buy_pr": "right", "dif": "right", "dif_rt(%)": "right"},
            )
        else:
            print("- 보유종목 없음")

        print("[정리] 보유종목 정리/보유 진행? 1.예 2.아니오: ", end="", flush=True)
        ready, _, _ = select.select([sys.stdin], [], [], 10)
        if ready:
            proceed = sys.stdin.readline().strip()
            if proceed in ("1", "2"):
                if proceed == "1":
                    _run_holdings_flow(client, cano, acnt_prdt_cd, tr_id_balance, tr_id_sell)
                break

        try:
            rows, summary, _, _, _ = _get_balance_page(
                client, cano, acnt_prdt_cd, tr_id_balance, return_raw=True
            )
        except RuntimeError as e:
            print(str(e))
            break


if __name__ == "__main__":
    main()
