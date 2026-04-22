"""
kis_signal_apis.py

상한가 근접 매수 전략에 필요한 보조 REST API 래퍼.
- 체결강도 (Volume Power)
- 외국인 최근 3일 순매수 합산 (frgnmem_pchs_trend)

실패 시 None 반환 → 호출측은 None 을 "필터 데이터 누락 → 매수 차단" 으로 처리.

문서: docs/01. up_limit_buy_str_260420.md 6-1, 6-3
"""
from __future__ import annotations

import time
import requests
from typing import Any


# =============================================================================
# 체결강도 (Volume Power) — inquire_price 응답의 tday_rltv 재활용
# =============================================================================

def fetch_volume_power(client, code: str) -> float | None:
    """
    당일 체결강도 조회.

    KIS 주식현재가 시세 (FHKST01010100) 응답의 tday_rltv 필드 사용.
    별도 체결강도 순위 API 호출 없이 동일 값 획득 가능.

    Returns:
        체결강도 (float, 예: 120.5) — 100이면 매수/매도 균형,
        100 초과 시 매수 우세, 100 미만 시 매도 우세.
        조회 실패 시 None.
    """
    if client is None or not code:
        return None
    try:
        resp = client.inquire_price(str(code).zfill(6))
        if not isinstance(resp, dict):
            return None
        for key in ("tday_rltv", "prdy_vrss_rltv_rate", "rltv_rate"):
            v = resp.get(key)
            if v is None or v == "":
                continue
            try:
                return float(str(v).replace(",", ""))
            except Exception:
                continue
    except Exception:
        pass
    return None


# =============================================================================
# Volume Power 랭킹 조회 (보조)
# =============================================================================

def fetch_volume_power_ranking(client, top_n: int = 500, market_div: str = "J") -> list[dict]:
    """
    체결강도 상위 순위 조회. 종목별 체결강도와 함께 순위 정보 포함.

    TR_ID: FHPST01680000
    URL: /uapi/domestic-stock/v1/ranking/volume-power

    Returns: [{"mksc_shrn_iscd": "005930", "tday_rltv": "120.5", ...}, ...]
        실패 시 빈 리스트.
    """
    if client is None:
        return []
    try:
        url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/ranking/volume-power"
        headers = client._headers(tr_id="FHPST01680000")
        params = {
            "fid_cond_mrkt_div_code": market_div,
            "fid_cond_scr_div_code": "20168",
            "fid_div_cls_code": "0",
            "fid_input_price_1": "",
            "fid_input_price_2": "",
            "fid_vol_cnt": "",
            "fid_trgt_cls_code": "0",
            "fid_trgt_exls_cls_code": "0",
            "fid_input_iscd": "0000",
        }
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code != 200:
            return []
        j = r.json()
        if str(j.get("rt_cd")) != "0":
            return []
        out = j.get("output") or []
        if isinstance(out, dict):
            out = [out]
        if top_n > 0:
            return out[:top_n]
        return out
    except Exception:
        return []


# =============================================================================
# 외국인 최근 3일 순매수 합산
# =============================================================================

def fetch_frgn_3day_net(client, code: str) -> float | None:
    """
    최근 3일 외국인 순매수량 합산.

    TR_ID: FHKST03010100 or FHKST01010900 — 주식현재가 외국인 기관 매매 추이.
    (공식 문서에 따라 endpoint 가 달라질 수 있어 2가지 경로 fallback)

    Returns:
        순매수량 (주식수, 양수=순매수, 음수=순매도). 실패 시 None.
    """
    if client is None or not code:
        return None
    code_z = str(code).zfill(6)
    base_url = client.cfg.base_url

    # Attempt 1: 외국인/기관 매매 추이 (일별)
    try:
        url = f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-investor"
        headers = client._headers(tr_id="FHKST01010900")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code_z,
        }
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code == 200:
            j = r.json()
            if str(j.get("rt_cd")) == "0":
                out = j.get("output") or []
                if isinstance(out, dict):
                    out = [out]
                if out:
                    total = 0.0
                    for i, row in enumerate(out[:3]):
                        v = (
                            row.get("frgn_ntby_qty")
                            or row.get("frgn_ntby_tr_pbmn")
                            or 0
                        )
                        try:
                            total += float(str(v).replace(",", "") or 0)
                        except Exception:
                            pass
                    return total
    except Exception:
        pass

    return None


# =============================================================================
# TTL 캐시 (호출측에서 쓰기 편하도록 간단 헬퍼)
# =============================================================================

def is_cache_fresh(fetched_ts: float, ttl_sec: float) -> bool:
    if fetched_ts <= 0:
        return False
    return (time.time() - fetched_ts) < ttl_sec
