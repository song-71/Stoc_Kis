#!/usr/bin/env python3
"""KIS 계좌 잔고 조회 (가장 심플).

- CLI: `python3 kis_inquire_balance_simple.py` → 요약 + 보유종목 stdout 출력
- Import: `from kis_inquire_balance_simple import fetch_balance_simple`
  → {"summary": {...}, "holdings": [...]} 반환
"""
import os
import requests
from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig
from kis_utils import resolve_account_config


def _f(v) -> float:
    try:
        return float(str(v or 0).replace(",", "")) or 0.0
    except (ValueError, TypeError):
        return 0.0


def fetch_balance_simple(config_path: str | None = None, account_id: str | None = None) -> dict:
    """KIS `inquire-balance` (TTTC8434R) 를 심플 경로로 호출하여 요약 + 보유종목 반환.

    Args:
        config_path: config.json 경로 (None이면 이 파일 옆 기본 경로)
        account_id: V2 config 에서 특정 account_id 지정 (None이면 default)

    Returns:
        {
          "ok": bool,
          "cano": str,
          "summary": {
            "dnca_tot_amt": float,        # 출금가능
            "prvs_rcdl_excc_amt": float,  # 주문가능(D+2)
            "tot_evlu_amt": float,        # 총평가
            "evlu_pfls_smtm_amt": float,  # 평가손익합계
            "thdt_buy_amt": float,        # 당일매수금액
            "thdt_sll_amt": float,        # 당일매도금액
          },
          "holdings": [  # hldg_qty>0 만
            {"pdno","hts_kor_isnm","hldg_qty","ord_psbl_qty",
             "pchs_avg_pric","prpr","evlu_amt","evlu_pfls_amt","evlu_pfls_rt"}
            ...
          ],
          "raw": {...}  # 원 응답 (디버깅용)
        }
    """
    here = os.path.dirname(os.path.abspath(__file__))
    if config_path is None:
        config_path = os.path.join(here, "config.json")
    cfg = resolve_account_config(config_path, account_id=account_id)

    client = KisClient(KisConfig(
        appkey=cfg["appkey"],
        appsecret=cfg["appsecret"],
        base_url=cfg.get("base_url") or DEFAULT_BASE_URL,
        custtype=cfg.get("custtype") or "P",
        token_cache_path=os.path.join(here, "kis_token_a1.json"),
    ))

    cano = str(cfg.get("cano") or cfg.get("CANO") or "").strip()
    acnt = str(cfg.get("acnt_prdt_cd") or cfg.get("ACNT_PRDT_CD") or "01").strip() or "01"

    holdings: list[dict] = []
    summary_out: dict = {}
    ctx_fk, ctx_nk = "", ""
    raw_last: dict = {}
    ok = False

    for _ in range(10):
        r = requests.get(
            f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-balance",
            headers=client._headers(tr_id="TTTC8434R"),
            params={
                "CANO": cano,
                "ACNT_PRDT_CD": acnt,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "02",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "00",
                "CTX_AREA_FK100": ctx_fk,
                "CTX_AREA_NK100": ctx_nk,
            },
            timeout=10,
        )
        r.raise_for_status()
        j = r.json()
        raw_last = j
        if str(j.get("rt_cd")) != "0":
            break
        ok = True
        out1 = j.get("output1") or []
        if isinstance(out1, dict):
            out1 = [out1]
        for row in out1:
            if int(_f(row.get("hldg_qty"))) <= 0:
                continue
            holdings.append({
                "pdno": str(row.get("pdno", "")).strip().zfill(6),
                "hts_kor_isnm": str(row.get("prdt_name") or row.get("hts_kor_isnm") or "").strip(),
                "hldg_qty": int(_f(row.get("hldg_qty"))),
                "ord_psbl_qty": int(_f(row.get("ord_psbl_qty"))),
                "pchs_avg_pric": _f(row.get("pchs_avg_pric")),
                "prpr": _f(row.get("prpr")),
                "evlu_amt": _f(row.get("evlu_amt")),
                "evlu_pfls_amt": _f(row.get("evlu_pfls_amt")),
                "evlu_pfls_rt": _f(row.get("evlu_pfls_rt")),
            })

        o2 = j.get("output2") or {}
        if isinstance(o2, list) and o2:
            o2 = o2[0]
        if isinstance(o2, dict) and not summary_out:
            summary_out = {
                "dnca_tot_amt": _f(o2.get("dnca_tot_amt")),
                "prvs_rcdl_excc_amt": _f(o2.get("prvs_rcdl_excc_amt") or o2.get("ord_psbl_cash")),
                "tot_evlu_amt": _f(o2.get("tot_evlu_amt")),
                "evlu_pfls_smtm_amt": _f(o2.get("evlu_pfls_smtm_amt")),
                "thdt_buy_amt": _f(o2.get("thdt_buy_amt")),
                "thdt_sll_amt": _f(o2.get("thdt_sll_amt")),
            }

        ctx_fk = str(j.get("ctx_area_fk100") or "").strip()
        ctx_nk = str(j.get("ctx_area_nk100") or "").strip()
        if not ctx_fk and not ctx_nk:
            break

    if not summary_out:
        summary_out = {
            "dnca_tot_amt": 0.0, "prvs_rcdl_excc_amt": 0.0,
            "tot_evlu_amt": 0.0, "evlu_pfls_smtm_amt": 0.0,
            "thdt_buy_amt": 0.0, "thdt_sll_amt": 0.0,
        }

    return {
        "ok": ok,
        "cano": cano,
        "acnt_prdt_cd": acnt,
        "summary": summary_out,
        "holdings": holdings,
        "raw": raw_last,
    }


def _cli_main() -> None:
    res = fetch_balance_simple()
    s = res["summary"]
    print(f"출금가능: {s['dnca_tot_amt']:,.0f}  "
          f"주문가능: {s['prvs_rcdl_excc_amt']:,.0f}  "
          f"총평가: {s['tot_evlu_amt']:,.0f}")
    for h in res["holdings"]:
        print(f"  {h['pdno']} {h['hts_kor_isnm']} {h['hldg_qty']}주 {h['prpr']:,.0f}원")


if __name__ == "__main__":
    _cli_main()
