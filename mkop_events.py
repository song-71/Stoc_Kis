#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
장운영구분코드(H0STMKO0 MKOP_CLS_CODE) 이벤트 판정 — 순수 로직 모듈
====================================================================

이 모듈은 **부작용이 전혀 없다**(import 시 인증·연결·파일 IO 없음).
프로덕션(ws_realtime_trading.py)이 이 표·함수를 import 해 쓰며, 리플레이/단위테스트는
프로덕션을 import 하지 않고 이 모듈만 import 해 판정 로직을 검증한다.

배경 (260626 실측 + docs/KIS문의_H0STMKO0_장운영코드_260626.md)
-------------------------------------------------------------------
KIS 운영 WSS 는 개발문서(매뉴얼)의 숫자 서킷/사이드카 코드(174/184/187/388/397/398 등)를
실제로는 보내지 않고, 서킷 발동=`AF8` / 서킷 해제=`AF1` / VI단일가=`BF9`(의미 미확정) 로
보낸다. 매뉴얼 답신으로 확정되기 전까지 **숫자코드와 A/B 접두 실측코드를 함께(병행) 매핑**해
어느 쪽이 와도 감지한다.

처리 구분
---------
- 시장 전파(발동): 174/184/164 + 실측 AF8  → 신규매수 차단·REST 폴링 생략·전종목 ccnl→exp·30분 자동복귀
- 시장 전파 해제:  175/185     + 실측 AF1  → REST 폴링 재개·전종목 exp→ccnl
- 종목 이벤트 클리어: 175/185/388/398 + AF1
- 정보성만(시장 동작 없음): 사이드카 187/397/388/398, VI단일가 BF9
"""
from __future__ import annotations

# ── MKOP_CLS_CODE 정의 (매뉴얼 숫자코드 + 260626 실측 A/B 접두코드 병행) ──
MKOP_SIDECAR_CODES = {"187", "388", "397", "398"}
MKOP_CIRCUIT_CODES = {"174", "175", "182", "184", "185", "AF8", "AF1"}
MKOP_EVENT_NAMES = {
    "187": "사이드카발동", "388": "사이드카해제",
    "397": "사이드카매수발동", "398": "사이드카매수해제",
    "174": "서킷브레이크발동", "175": "서킷브레이크해제",
    "182": "서킷브레이크장종동시마감", "184": "서킷브레이크개시", "185": "서킷브레이크해제",
    "164": "시장임시정지",
    # [260703] A/B 접두 실측코드 병행 매핑 (260626 실측)
    "AF8": "서킷브레이크발동", "AF1": "서킷브레이크해제", "BF9": "VI단일가",
}
CB_ACTIVE_EVENTS = {"서킷브레이크발동", "서킷브레이크개시", "서킷브레이크동시호가"}

# 처리 분기용 코드 집합 (프로덕션 _on_market_status_krx 와 동일 기준)
CB_TRIGGER_CODES = {"174", "184", "164", "AF8"}         # 시장 전파(발동)
CB_RELEASE_CODES = {"175", "185", "AF1"}                # 시장 전파 해제
MARKET_EVENT_CLEAR_CODES = {"175", "185", "388", "398", "AF1"}  # 종목 이벤트 클리어


def classify_mkop_event(mkop: str, antc_mkop: str) -> dict | None:
    """장운영코드(현재값 mkop 우선, 비었으면 예상값 antc_mkop)로 이벤트를 판정한다.

    반환: 매칭되는 이벤트가 없으면 None, 있으면
      {evt_code, evt_src, event_name, cb_trigger, cb_release, clear_market_event}

    - evt_src: 어느 필드로 잡았는지 ("mkop_cls_code" / "antc_mkop_cls_code")
    - cb_trigger:  시장 전파(발동) 대상 (174/184/164/AF8)
    - cb_release:  시장 전파 해제 대상 (175/185/AF1)
    - clear_market_event: 종목 이벤트 클리어 대상 (175/185/388/398/AF1)
    """
    mkop = (mkop or "").strip()
    antc_mkop = (antc_mkop or "").strip()
    if MKOP_EVENT_NAMES.get(mkop):
        evt_code, evt_src = mkop, "mkop_cls_code"
    elif MKOP_EVENT_NAMES.get(antc_mkop):
        evt_code, evt_src = antc_mkop, "antc_mkop_cls_code"
    else:
        return None
    return {
        "evt_code": evt_code,
        "evt_src": evt_src,
        "event_name": MKOP_EVENT_NAMES[evt_code],
        "cb_trigger": evt_code in CB_TRIGGER_CODES,
        "cb_release": evt_code in CB_RELEASE_CODES,
        "clear_market_event": evt_code in MARKET_EVENT_CLEAR_CODES,
    }
