#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NXT 프리마켓·애프터마켓 거래 + 09:00 전 주문취소 점검 테스트 (실거래 1주)
================================================================================

목적
----
ws_realtime_trading.py 의 매매 파이프라인이 다음 시간대에서 실제로 동작하는지
"실제 1주 주문"으로 점검한다. (프로덕션 코드는 건드리지 않는 독립 실행 스크립트)

  ① 프리마켓 (NXT, 08:00~08:50)  : 08:05 시장가 1주 매수 → 1분 후 시장가 매도
  ② 09:00 전 주문취소 점검 (KRX 장시작 동시호가, 08:50~09:00)
       - 08:50:05  지정가 1주 매수 접수
       - 08:58:00  정정취소가능주문조회로 취소가능 여부 확인 후 취소
       - 08:59:00  지정가 1주 매수 접수 → 08:59:50 취소
       - (직전 취소 접수 +1초) 지정가 1주 매수 접수 → 08:59:59 취소
       => 09:00(개장) 전에 주문 취소가 정상 처리되는지 점검
  ③ 애프터마켓 (NXT, 15:40~20:00) : 시작 +5분(기본 15:45) 시장가 1주 매수 → 1분 후 매도

핵심 사실 (코드 근거)
--------------------
- NXT 거래소 라우팅은 주문 본문의 EXCG_ID_DVSN_CD 로 지정한다("KRX"/"NXT"/"SOR").
  현행 프로덕션 주문 함수(_buy_order_cash 등)에는 이 필드가 없어 항상 KRX 로만 나간다.
- NXT 통합주문은 TR_ID 가 다르다:
    매수 TTTC0012U / 매도 TTTC0011U / 취소 TTTC0013U
  (근거: strategy_lab/.../order_cash/order_cash.py:104-107,
         strategy_lab/.../order_rvsecncl/order_rvsecncl.py:108-127)
  프로덕션은 레거시 KRX 전용(TTTC0802U/0801U/0803U)을 쓴다.
- 취소 본문 필드명은 신규 규격: ORGN_ODNO + QTY_ALL_ORD_YN + EXCG_ID_DVSN_CD (필수).

안전장치
--------
- 계좌: a1 (config.json users.<default_user>.accounts.a1), 기존 토큰캐시(kis_token_a1.json)
  를 재사용하므로 신규 토큰 발급·카톡 알림톡이 발생하지 않는다(유효시).
- 주문 수량은 1주 고정. DRY_RUN=True 면 실제 주문을 전송하지 않고 흐름만 로그로 점검.
- "접수통보"는 본 테스트에서 REST 주문응답(rt_cd=0 + 주문번호 ODNO)을 접수확인으로 간주한다.
  (WSS 체결통보 H0STCNI0 푸시 구독은 사용하지 않음 — 로그에 'REST접수확인'으로 명시)

실행
----
    python3 test_nxt_pre_after_market.py            # 종일 스케줄러 (기본)
    python3 test_nxt_pre_after_market.py --dry-run  # 실제 주문 없이 흐름만
    python3 test_nxt_pre_after_market.py --phase premarket   # 단일 단계 즉시 실행(수동 점검)
    python3 test_nxt_pre_after_market.py --phase cancel
    python3 test_nxt_pre_after_market.py --phase aftermarket

로그: test_nxt_pre_after_market_YYMMDD.log (+ 콘솔 + 텔레그램 주요 이벤트)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# 유틸만 임포트 (프로덕션 매매 모듈 ws_realtime_trading.py 는 임포트하지 않음 → 부작용 회피)
from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig
from kis_utils import load_config, KRX_code, round_to_tick

KST = ZoneInfo("Asia/Seoul")
SCRIPT_DIR = Path(__file__).resolve().parent

# ─────────────────────────────────────────────────────────────────────────────
# 설정 상수 (필요시 여기만 수정)
# ─────────────────────────────────────────────────────────────────────────────
ACCOUNT_KEY = "a1"
ORDER_QTY = 1
DRY_RUN = False                      # True: 실제 주문 전송 안 함(로그만)
CSV_PATH = SCRIPT_DIR / "symulation" / "Select_Tr_target_list.csv"

# 스케줄 (KST, (시,분,초))
T_PRE_BUY        = (8, 5, 0)         # ① 프리마켓 매수
T_PRE_SELL       = (8, 6, 0)         # ① 프리마켓 매도 (매수 +1분)

T_CANCEL_A_PLACE = (8, 50, 5)        # ② A 매수 접수
T_CANCEL_A_CANCEL= (8, 58, 0)        # ② A 취소(취소가능 여부 확인 후)
T_CANCEL_B_PLACE = (8, 59, 0)        # ② B 매수 접수
T_CANCEL_B_CANCEL= (8, 59, 50)       # ② B 취소
# C: B 취소 접수 +1초 후 매수, 아래 시각에 취소
T_CANCEL_C_CANCEL= (8, 59, 59)       # ② C 취소 (09:00 직전)

T_AFTER_BUY      = (15, 45, 0)       # ③ 애프터마켓 매수 (NXT 애프터 시작 15:40 + 5분)
T_AFTER_SELL     = (15, 46, 0)       # ③ 애프터마켓 매도 (매수 +1분)

# 거래소 라우팅
EXCG_NXT = "NXT"
EXCG_KRX = "KRX"

# 통합주문 TR_ID (실전)
TR_BUY    = "TTTC0012U"   # 현금 매수
TR_SELL   = "TTTC0011U"   # 현금 매도
TR_CANCEL = "TTTC0013U"   # 정정/취소
TR_PSBL   = "TTTC0084R"   # 정정취소가능주문조회
TR_BAL    = "TTTC8434R"   # 주식잔고조회

# ─────────────────────────────────────────────────────────────────────────────
# 로깅 / 알림
# ─────────────────────────────────────────────────────────────────────────────
def _ymd() -> str:
    return datetime.now(KST).strftime("%y%m%d")

LOG_PATH = SCRIPT_DIR / f"test_nxt_pre_after_market_{_ymd()}.log"

logger = logging.getLogger("nxt_test")
logger.setLevel(logging.INFO)
logger.propagate = False              # 루트 로거 전파 차단(콘솔 이중출력 방지)
_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
_fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
logger.addHandler(_fh)
logger.addHandler(_sh)

_TELE = {"token": "", "chat_id": ""}


def _ts() -> str:
    """[YYMMDD_HHMMSS] KST 접두."""
    return datetime.now(KST).strftime("[%y%m%d_%H%M%S]")


def notify(msg: str, tele: bool = True) -> None:
    """로그 + (옵션)텔레그램. 텔레그램 메시지도 항상 자체 로그에 남긴다."""
    line = f"{_ts()} {msg}"
    logger.info(line)
    if tele and _TELE["token"] and _TELE["chat_id"]:
        try:
            requests.post(
                f"https://api.telegram.org/bot{_TELE['token']}/sendMessage",
                json={"chat_id": _TELE["chat_id"], "text": line},
                timeout=10,
            )
        except Exception as e:
            logger.warning(f"{_ts()} [텔레그램 전송 실패] {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 설정/클라이언트 로딩
# ─────────────────────────────────────────────────────────────────────────────
def load_account() -> dict:
    """
    config.json 에서 계좌 정보 + 텔레그램 토큰 로딩.
    load_config(ConfigProxy)는 default_user 의 기본계좌(a1)를 최상위로 평탄화하고
    나머지 계좌만 cfg['accounts'] 하위에 둔다(프로덕션 _load_main_cfg/_load_syw2_cfg 와 동일).
      - ACCOUNT_KEY 가 최상위 평탄화 계좌(cano_alias 일치)면 최상위에서 읽음
      - 그 외(a2 등)는 cfg['accounts'][key] 에서 읽음
    """
    cfg = load_config(str(SCRIPT_DIR / "config.json"))
    base_url = cfg.get("base_url") or DEFAULT_BASE_URL
    custtype = cfg.get("custtype") or "P"

    if str(cfg.get("cano_alias", "")) == ACCOUNT_KEY and cfg.get("appkey"):
        # 최상위 평탄화된 기본계좌 (a1)
        appkey, appsecret = cfg.get("appkey"), cfg.get("appsecret")
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "") or "01").strip() or "01"
        alias = cfg.get("cano_alias", ACCOUNT_KEY)
    else:
        acct = (cfg.get("accounts", {}) or {}).get(ACCOUNT_KEY)
        if not acct:
            raise RuntimeError(f"config.json 에서 계좌 {ACCOUNT_KEY} 를 찾을 수 없습니다.")
        appkey, appsecret = acct.get("appkey"), acct.get("appsecret")
        cano = str(acct.get("cano", "")).strip()
        acnt = str(acct.get("acnt_prdt_cd", "") or "01").strip() or "01"
        alias = acct.get("cano_alias", ACCOUNT_KEY)

    if not appkey or not appsecret:
        raise RuntimeError(f"계좌 {ACCOUNT_KEY} 의 appkey/appsecret 이 비어있습니다.")
    if not cano:
        raise RuntimeError(f"계좌 {ACCOUNT_KEY} 의 cano 가 비어있습니다.")

    _TELE["token"] = str(cfg.get("BOT_TOKEN", "") or "")
    _TELE["chat_id"] = str(cfg.get("CHAT_ID", "") or "")
    return {
        "appkey": appkey, "appsecret": appsecret,
        "base_url": base_url, "custtype": custtype,
        "cano": cano, "acnt": acnt, "alias": alias,
    }


def build_client(acct: dict) -> KisClient:
    return KisClient(KisConfig(
        appkey=acct["appkey"], appsecret=acct["appsecret"],
        base_url=acct["base_url"], custtype=acct["custtype"],
        market_div="J",
        token_cache_path=str(SCRIPT_DIR / f"kis_token_{ACCOUNT_KEY}.json"),
    ))


# ─────────────────────────────────────────────────────────────────────────────
# KIS 주문/조회 원시 호출 (NXT 통합주문 규격)
# ─────────────────────────────────────────────────────────────────────────────
def _hashkey(client: KisClient, body: dict) -> str:
    url = f"{client.cfg.base_url}/uapi/hashkey"
    r = requests.post(url, headers={
        "content-type": "application/json",
        "appkey": client.cfg.appkey, "appsecret": client.cfg.appsecret,
    }, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    return j.get("HASH") or j.get("hash") or ""


def order_cash(client: KisClient, acct: dict, side: str, code: str, qty: int,
               price: float, ord_dvsn: str, excg: str) -> dict:
    """
    현금 매수/매도. side: 'buy'|'sell'.
    ord_dvsn: '00'=지정가, '01'=시장가. 시장가는 ORD_UNPR=0.
    반환: {'ok', 'odno', 'orgno', 'tmd', 'msg', 'raw'}
    """
    tr_id = TR_BUY if side == "buy" else TR_SELL
    ord_unpr = "0" if ord_dvsn == "01" else str(int(price))
    body = {
        "CANO": acct["cano"], "ACNT_PRDT_CD": acct["acnt"],
        "PDNO": code, "ORD_DVSN": ord_dvsn,
        "ORD_QTY": str(qty), "ORD_UNPR": ord_unpr,
        "EXCG_ID_DVSN_CD": excg,
        "SLL_TYPE": "", "CNDT_PRIC": "",
    }
    label = f"{side.upper()} {code} x{qty} {ord_dvsn}@{ord_unpr} EXCG={excg}"
    if DRY_RUN:
        logger.info(f"{_ts()} [DRY_RUN] 주문 미전송: {label} body={body}")
        return {"ok": True, "odno": "DRYRUN", "orgno": "00000", "tmd": "", "msg": "DRY_RUN", "raw": {}}

    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    headers = client._headers(tr_id=tr_id)
    headers["hashkey"] = _hashkey(client, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    ok = str(j.get("rt_cd")) == "0"
    out = j.get("output") or {}
    res = {
        "ok": ok,
        "odno": str(out.get("ODNO", "")).strip(),
        "orgno": str(out.get("KRX_FWDG_ORD_ORGNO", "")).strip() or "00000",
        "tmd": str(out.get("ORD_TMD", "")).strip(),
        "msg": j.get("msg1", ""),
        "raw": j,
    }
    if ok:
        logger.info(f"{_ts()} [주문성공] {label} → 주문번호={res['odno']} 조직={res['orgno']} 시각={res['tmd']} msg={res['msg']}")
    else:
        logger.error(f"{_ts()} [주문실패] {label} → msg={res['msg']} raw={j}")
    return res


def cancel_order(client: KisClient, acct: dict, odno: str, orgno: str, code: str,
                 qty: int, ord_dvsn: str, excg: str) -> dict:
    """주문 취소 (전량). 반환: {'ok','msg','raw'}"""
    label = f"CANCEL 주문번호={odno} {code} x{qty} {ord_dvsn} EXCG={excg}"
    if DRY_RUN:
        logger.info(f"{_ts()} [DRY_RUN] 취소 미전송: {label}")
        return {"ok": True, "msg": "DRY_RUN", "raw": {}}
    if not odno or odno == "DRYRUN":
        return {"ok": False, "msg": "주문번호 없음", "raw": {}}

    body = {
        "CANO": acct["cano"], "ACNT_PRDT_CD": acct["acnt"],
        "KRX_FWDG_ORD_ORGNO": orgno or "00000",
        "ORGN_ODNO": odno,
        "ORD_DVSN": ord_dvsn,
        "RVSE_CNCL_DVSN_CD": "02",          # 02 = 취소
        "ORD_QTY": str(qty), "ORD_UNPR": "0",
        "QTY_ALL_ORD_YN": "Y",              # 전량 취소
        "EXCG_ID_DVSN_CD": excg,
    }
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/order-rvsecncl"
    headers = client._headers(tr_id=TR_CANCEL)
    headers["hashkey"] = _hashkey(client, body)
    headers["content-type"] = "application/json"
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    r.raise_for_status()
    j = r.json()
    ok = str(j.get("rt_cd")) == "0"
    msg = j.get("msg1", "")
    if ok:
        notify(f"[취소성공] {label} msg={msg}")
    else:
        notify(f"[취소실패] {label} msg={msg} raw={j}")
    return {"ok": ok, "msg": msg, "raw": j}


def inquire_psbl_rvsecncl(client: KisClient, acct: dict) -> list[dict]:
    """정정취소가능주문조회 (매수 00). 취소가능 주문 목록 반환."""
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"
    headers = client._headers(tr_id=TR_PSBL)
    headers["tr_cont"] = ""
    params = {
        "CANO": acct["cano"], "ACNT_PRDT_CD": acct["acnt"],
        "INQR_DVSN": "00", "SLL_BUY_DVSN_CD": "00",
        "INQR_STRT_DT": "", "INQR_END_DT": "", "PDNO": "", "ORD_GNO_BRNO": "", "ODNO": "",
        "INQR_DVSN_1": "0", "INQR_DVSN_2": "0",
        "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
    }
    out: list[dict] = []
    for _ in range(10):
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        j = r.json()
        if str(j.get("rt_cd")) != "0":
            logger.warning(f"{_ts()} [취소가능조회 실패] msg={j.get('msg1')}")
            break
        o1 = j.get("output1") or j.get("output") or []
        if isinstance(o1, dict):
            o1 = [o1]
        out.extend(o1)
        tr_cont = str(r.headers.get("tr_cont") or "").strip().upper()
        if tr_cont not in ("F", "M"):
            break
        headers["tr_cont"] = "N"
        params["CTX_AREA_FK100"] = j.get("ctx_area_fk100") or ""
        params["CTX_AREA_NK100"] = j.get("ctx_area_nk100") or ""
    return out


def get_holding_qty(client: KisClient, acct: dict, code: str) -> int:
    """주식잔고조회로 특정 종목 보유수량 반환 (매수 체결 확인용). 실패 시 -1."""
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
    headers = client._headers(tr_id=TR_BAL)
    headers["tr_cont"] = ""
    params = {
        "CANO": acct["cano"], "ACNT_PRDT_CD": acct["acnt"], "AFHR_FLPR_YN": "N",
        "OFL_YN": "", "INQR_DVSN": "02", "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00",
        "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
    }
    try:
        for _ in range(10):
            r = requests.get(url, headers=headers, params=params, timeout=10)
            r.raise_for_status()
            j = r.json()
            if str(j.get("rt_cd")) != "0":
                return -1
            for row in (j.get("output1") or []):
                rc = str(row.get("pdno") or row.get("PDNO") or "").strip().zfill(6)
                if rc == code:
                    q = str(row.get("hldg_qty") or row.get("HLDG_QTY") or "0").replace(",", "")
                    return int(float(q or 0))
            tr_cont = str(r.headers.get("tr_cont") or "").strip().upper()
            if tr_cont not in ("F", "M"):
                break
            headers["tr_cont"] = "N"
            params["CTX_AREA_FK100"] = j.get("ctx_area_fk100") or ""
            params["CTX_AREA_NK100"] = j.get("ctx_area_nk100") or ""
    except Exception as e:
        logger.warning(f"{_ts()} [잔고조회 실패] {code}: {e}")
        return -1
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# 거래 대상 선정: NXT 거래 가능 종목 중 가장 저렴한 종목
# ─────────────────────────────────────────────────────────────────────────────
def select_cheapest_nxt() -> dict | None:
    """
    Select_Tr_target_list.csv 의 최신 날짜 행에서 NXT 거래가능(nxt=Y) 종목 중
    종가(close)가 가장 싼 종목 1개 선택. (당일 행이 없으면 가장 최근 날짜 = '전날' 사용)
    반환: {'code','name','price','date'} 또는 None
    """
    import pandas as pd
    if not CSV_PATH.exists():
        logger.error(f"{_ts()} CSV 없음: {CSV_PATH}")
        return None
    df = pd.read_csv(CSV_PATH)
    if df.empty or "date" not in df.columns or "symbol" not in df.columns:
        logger.error(f"{_ts()} CSV 형식 이상")
        return None

    today = datetime.now(KST).strftime("%Y-%m-%d")
    latest = today if today in set(df["date"].astype(str)) else str(df["date"].astype(str).max())
    rows = df[df["date"].astype(str) == latest].copy()
    logger.info(f"{_ts()} [대상선정] 기준일자={latest} (오늘={today}) 후보={len(rows)}건")

    price_col = "close" if "close" in rows.columns else "pdy_close"
    normal: list[dict] = []     # status 00(정상)
    others: list[dict] = []     # nxt=Y 이지만 상태코드 비정상(폴백용)
    for _, row in rows.iterrows():
        code = str(row["symbol"]).strip().zfill(6)
        try:
            price = float(row[price_col])
        except Exception:
            continue
        if price <= 0:
            continue
        c, name, nxt, market, status = KRX_code(code)
        if c is None or str(nxt).upper() != "Y":
            continue
        item = {"code": code, "name": name or str(row.get("name", "")),
                "price": price, "date": latest, "status": status}
        # 종목상태: 00/None=정상. 그 외(관리/정지 등)는 폴백 후보로만.
        if status in (None, "", "00", "nan"):
            normal.append(item)
        else:
            others.append(item)

    candidates = normal
    if not candidates and others:
        logger.warning(f"{_ts()} [대상선정] 정상상태 NXT 후보 없음 → 비정상상태 포함 폴백 선정")
        candidates = others
    if not candidates:
        logger.error(f"{_ts()} [대상선정] NXT 거래가능 후보 없음 (기준일 {latest})")
        return None
    candidates.sort(key=lambda x: x["price"])
    top = candidates[:5]
    logger.info(f"{_ts()} [대상선정] 최저가 후보 5: " +
                ", ".join(f"{c['name']}({c['code']}) {int(c['price']):,}원" for c in top))
    chosen = candidates[0]
    notify(f"[대상선정] 선택={chosen['name']}({chosen['code']}) {int(chosen['price']):,}원 기준일={latest}")
    return chosen


# ─────────────────────────────────────────────────────────────────────────────
# 스케줄 유틸
# ─────────────────────────────────────────────────────────────────────────────
def _target_dt(hms: tuple[int, int, int]) -> datetime:
    now = datetime.now(KST)
    return now.replace(hour=hms[0], minute=hms[1], second=hms[2], microsecond=0)


def sleep_until(hms: tuple[int, int, int], grace_sec: int = 90) -> bool:
    """
    오늘 KST hms 까지 대기. 이미 지났으면(grace 초과) False 반환(스킵).
    grace 이내로 지난 경우엔 즉시 진행(True).
    """
    target = _target_dt(hms)
    now = datetime.now(KST)
    delta = (target - now).total_seconds()
    if delta < -grace_sec:
        logger.info(f"{_ts()} [스킵] 목표시각 {hms[0]:02d}:{hms[1]:02d}:{hms[2]:02d} 이미 지남({-delta:.0f}s 경과)")
        return False
    if delta > 0:
        logger.info(f"{_ts()} [대기] {hms[0]:02d}:{hms[1]:02d}:{hms[2]:02d} 까지 {delta:.0f}초 대기")
        # 긴 대기는 끊어서 (인터럽트 반응성)
        end = time.time() + delta
        while True:
            remain = end - time.time()
            if remain <= 0:
                break
            time.sleep(min(remain, 5))
    return True


# ─────────────────────────────────────────────────────────────────────────────
# 단계 ① / ③ : 프리·애프터마켓 NXT 매수 → 1분후 매도
# ─────────────────────────────────────────────────────────────────────────────
def run_round_trip(client: KisClient, acct: dict, target: dict, label: str,
                   sell_hms: tuple[int, int, int] | None = None,
                   sell_after_sec: int = 60, use_limit: bool = False) -> None:
    """
    use_limit=True 면 시장가(01) 대신 지정가(00)로 매매한다.
    (NXT 프리마켓은 시장가 주문 불가 — KIS APBK0918 '[프리마켓] 시장가 매매 불가 시간'.
     체결을 위해 기준가 대비 매수는 +1%, 매도는 −1% 의 '시장가성 지정가'로 넣어
     호가를 가로질러 체결되게 하되 슬리피지는 1% 로 제한한다.)
    """
    code, name = target["code"], target["name"]
    ref = float(target.get("price") or 0)
    if use_limit:
        ord_dvsn = "00"
        buy_px = round_to_tick(ref * 1.01) if ref > 0 else 0
        sell_px = round_to_tick(ref * 0.99) if ref > 0 else 0
        px_desc = f"지정가 매수{int(buy_px):,}/매도{int(sell_px):,}원(기준 {int(ref):,})"
    else:
        ord_dvsn = "01"
        buy_px = sell_px = 0
        px_desc = "시장가"
    notify(f"=== [{label}] 시작: {name}({code}) NXT {px_desc} 1주 매수 ===")
    buy = order_cash(client, acct, "buy", code, ORDER_QTY, buy_px, ord_dvsn, EXCG_NXT)
    notify(f"[{label}] 매수 접수확인(REST) ok={buy['ok']} 주문번호={buy['odno']} msg={buy['msg']}")

    # 매도 시각까지 대기 (매수 +1분)
    if sell_hms is not None:
        if not sleep_until(sell_hms, grace_sec=600):
            logger.info(f"{_ts()} [{label}] 매도 목표시각 지남 → 즉시 매도 시도")
    else:
        logger.info(f"{_ts()} [{label}] 매도까지 {sell_after_sec}초 대기")
        time.sleep(sell_after_sec)

    # 체결 수량 확인 후 보유분만 매도
    hold = get_holding_qty(client, acct, code)
    logger.info(f"{_ts()} [{label}] 매수 후 보유수량={hold}")
    sell_qty = ORDER_QTY if hold < 0 else min(hold, ORDER_QTY)
    if sell_qty <= 0:
        notify(f"[{label}] ⚠ 매수 미체결 추정(보유 0) → 매도 스킵. (NXT {label} 매수 미체결 점검 필요)")
        # 혹시 미체결 매수주문이 남아있으면 취소
        if buy["ok"] and buy["odno"] not in ("", "DRYRUN"):
            cancel_order(client, acct, buy["odno"], buy["orgno"], code, ORDER_QTY, ord_dvsn, EXCG_NXT)
        return

    notify(f"=== [{label}] {name}({code}) NXT {'지정가' if use_limit else '시장가'} {sell_qty}주 매도 ===")
    sell = order_cash(client, acct, "sell", code, sell_qty, sell_px, ord_dvsn, EXCG_NXT)
    notify(f"[{label}] 매도 접수확인(REST) ok={sell['ok']} 주문번호={sell['odno']} msg={sell['msg']}")
    notify(f"=== [{label}] 종료 ===")


# ─────────────────────────────────────────────────────────────────────────────
# 단계 ② : 09:00 전 주문취소 점검 (KRX 장시작 동시호가)
# ─────────────────────────────────────────────────────────────────────────────
def run_opening_cancel(client: KisClient, acct: dict, target: dict) -> None:
    """
    KRX 동시호가(08:50~09:00)에 지정가 매수 접수/취소가 정상 처리되는지 점검.
    체결 방지를 위해 지정가는 기준가의 90% 수준(틱 정렬, 하한가 이내)으로 낮게 넣어
    동시호가에서 체결되지 않고 남도록 한다 → 취소 점검에 사용.
    """
    code, name = target["code"], target["name"]
    ref = float(target["price"])
    limit_px = round_to_tick(ref * 0.90)
    if limit_px <= 0:
        limit_px = round_to_tick(max(ref - 100, 1))
    notify(f"=== [취소점검] 시작: {name}({code}) KRX 지정가 {int(limit_px):,}원(기준 {int(ref):,}의 90%) 1주 ===")

    # --- A: 08:50:05 접수 → 08:58:00 취소가능 확인 후 취소 ---
    a = order_cash(client, acct, "buy", code, ORDER_QTY, limit_px, "00", EXCG_KRX)
    notify(f"[취소점검:A] 08:50 매수 접수확인(REST) ok={a['ok']} 주문번호={a['odno']} msg={a['msg']}")

    if sleep_until(T_CANCEL_A_CANCEL, grace_sec=300):
        # 취소가능 여부 체크
        psbl = inquire_psbl_rvsecncl(client, acct)
        odnos = {str(o.get("odno") or o.get("ODNO") or "").strip() for o in psbl}
        cancellable = a["odno"] in odnos
        notify(f"[취소점검:A] 08:58 정정취소가능조회: 총 {len(psbl)}건, A주문({a['odno']}) 취소가능={cancellable}")
        for o in psbl:
            logger.info(f"{_ts()} [취소점검:A] 취소가능주문 odno={o.get('odno') or o.get('ODNO')} "
                        f"pdno={o.get('pdno') or o.get('PDNO')} psbl_qty={o.get('psbl_qty') or o.get('PSBL_QTY')}")
        cancel_order(client, acct, a["odno"], a["orgno"], code, ORDER_QTY, "00", EXCG_KRX)

    # --- B: 08:59:00 접수 → 08:59:50 취소 ---
    if sleep_until(T_CANCEL_B_PLACE, grace_sec=30):
        b = order_cash(client, acct, "buy", code, ORDER_QTY, limit_px, "00", EXCG_KRX)
        notify(f"[취소점검:B] 08:59:00 매수 접수확인(REST) ok={b['ok']} 주문번호={b['odno']} msg={b['msg']}")

        sleep_until(T_CANCEL_B_CANCEL, grace_sec=10)
        cb = cancel_order(client, acct, b["odno"], b["orgno"], code, ORDER_QTY, "00", EXCG_KRX)

        # --- C: B 취소 접수 +1초 후 매수 → 08:59:59 취소 ---
        time.sleep(1.0)  # 접수통보(REST 취소응답) +1초
        c = order_cash(client, acct, "buy", code, ORDER_QTY, limit_px, "00", EXCG_KRX)
        notify(f"[취소점검:C] 매수 접수확인(REST) ok={c['ok']} 주문번호={c['odno']} msg={c['msg']}")

        sleep_until(T_CANCEL_C_CANCEL, grace_sec=5)
        cc = cancel_order(client, acct, c["odno"], c["orgno"], code, ORDER_QTY, "00", EXCG_KRX)

        notify(f"[취소점검] 결과요약: B취소={cb['ok']} C취소={cc['ok']} "
               f"(09:00 전 취소 정상처리 점검). 09:00 개장 시 잔여 미체결 없어야 정상.")

    # 안전: 혹시 남은 미체결 매수주문 전수 취소
    leftover = inquire_psbl_rvsecncl(client, acct)
    for o in leftover:
        rc = str(o.get("pdno") or o.get("PDNO") or "").strip().zfill(6)
        if rc != code:
            continue
        odno = str(o.get("odno") or o.get("ODNO") or "").strip()
        q = int(float(str(o.get("psbl_qty") or o.get("PSBL_QTY") or 0).replace(",", "") or 0))
        if odno and q > 0:
            notify(f"[취소점검] 잔여 미체결 정리 취소 odno={odno} qty={q}")
            cancel_order(client, acct, odno, "00000", code, q, "00", EXCG_KRX)
    notify("=== [취소점검] 종료 ===")


# ─────────────────────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    global DRY_RUN
    ap = argparse.ArgumentParser(description="NXT 프리/애프터마켓 + 09:00전 취소 점검 테스트")
    ap.add_argument("--dry-run", action="store_true", help="실제 주문 전송 없이 흐름만 점검")
    ap.add_argument("--phase", choices=["premarket", "cancel", "aftermarket"],
                    help="단일 단계 즉시 실행(미지정 시 종일 스케줄러)")
    args = ap.parse_args()
    if args.dry_run:
        DRY_RUN = True

    notify(f"########## NXT 테스트 시작 (계좌={ACCOUNT_KEY}, DRY_RUN={DRY_RUN}, 로그={LOG_PATH.name}) ##########")
    acct = load_account()
    logger.info(f"{_ts()} 계좌 cano={acct['cano']} acnt={acct['acnt']} alias={acct['alias']} base={acct['base_url']}")
    client = build_client(acct)
    try:
        client.ensure_token()
        logger.info(f"{_ts()} 토큰 준비 완료(캐시 재사용 시 신규발급 없음)")
    except Exception as e:
        notify(f"[치명] 토큰 확보 실패 → 중단: {e}")
        return

    target = select_cheapest_nxt()
    if not target:
        notify("[치명] NXT 거래대상 선정 실패 → 중단")
        return

    # 단일 단계 즉시 실행
    if args.phase == "premarket":
        run_round_trip(client, acct, target, "프리마켓", sell_hms=None, sell_after_sec=60, use_limit=True)
        return
    if args.phase == "aftermarket":
        run_round_trip(client, acct, target, "애프터마켓", sell_hms=None, sell_after_sec=60, use_limit=True)
        return
    if args.phase == "cancel":
        run_opening_cancel(client, acct, target)
        return

    # ── 종일 스케줄러 ──
    notify("[스케줄러] ① 프리마켓 08:05 / ② 취소점검 08:50~09:00 / ③ 애프터마켓 15:45 대기 시작")

    # ① 프리마켓 (NXT 08:00~08:50)
    #   08:05 매수목표를 놓쳤어도 "프리마켓 장중"이면 즉시 실행한다.
    #   (08:48 이후엔 취소점검(08:50:05)과 겹치고 프리마켓 마감이 임박하므로 스킵)
    now = datetime.now(KST)
    pre_buy_target = _target_dt(T_PRE_BUY)
    pre_market_close = _target_dt((8, 48, 0))
    if now < pre_market_close:
        if now > pre_buy_target:
            notify(f"[프리마켓] 매수목표(08:05) 경과했으나 프리마켓 장중 → 즉시 매수 실행 "
                   f"(현재 {now.strftime('%H:%M:%S')})")
        else:
            sleep_until(T_PRE_BUY, grace_sec=120)
        try:
            run_round_trip(client, acct, target, "프리마켓", T_PRE_SELL, use_limit=True)
        except Exception as e:
            notify(f"[프리마켓] 예외: {e}")
    else:
        notify(f"[프리마켓] 프리마켓 마감 임박/종료(08:48 이후, 현재 {now.strftime('%H:%M:%S')}) → 스킵")

    # ② 09:00 전 취소점검
    if sleep_until(T_CANCEL_A_PLACE, grace_sec=120):
        try:
            run_opening_cancel(client, acct, target)
        except Exception as e:
            notify(f"[취소점검] 예외: {e}")

    # ③ 애프터마켓
    if sleep_until(T_AFTER_BUY, grace_sec=120):
        try:
            run_round_trip(client, acct, target, "애프터마켓", T_AFTER_SELL, use_limit=True)
        except Exception as e:
            notify(f"[애프터마켓] 예외: {e}")

    notify("########## NXT 테스트 전체 종료 ##########")


if __name__ == "__main__":
    main()
