#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[리플레이 검증] 장운영코드 판정(mkop_events) — 260626 실제 CB 프레임 + 합성 프레임
====================================================================================

목적
----
260703 도입한 AF8/AF1/BF9 병행 감지가 실제로 동작하는지, 프로덕션 인증·연결 없이
검증한다. `ws_realtime_trading.py` 는 import 하지 않는다(import 시 a1 인증 → 프로덕션
즉사 위험). 대신 부작용 없는 `mkop_events` 만 import 해 판정 로직을 재생·단정한다.

데이터 소스
-----------
- 실데이터: 260626 서킷 발생일 원본 프레임 (out/logs/wss_TR_260626.log 의 [장운영정보] 줄)
- 합성:     매뉴얼 숫자코드(174/184/164/175/187/388 등) — 병행 매핑 유지 확인용

사용법
------
  python3 test_mkop_replay.py          # 실데이터+합성 전부 검증, 실패 시 exit 1
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from mkop_events import classify_mkop_event  # noqa: E402

LOG_260626 = SCRIPT_DIR / "out" / "logs" / "wss_TR_260626.log"

_FRAME_RE = re.compile(
    r"\[장운영정보\].*?mkop_cls_code=(?P<mkop>\S+)\s+"
    r"antc_mkop_cls_code=(?P<antc>\S+)\s+trht_yn=(?P<trht>\S+)"
)

_fail = 0
_pass = 0


def check(desc: str, got, want) -> None:
    global _fail, _pass
    ok = got == want
    _pass += ok
    _fail += (not ok)
    mark = "✅" if ok else "❌"
    print(f"  {mark} {desc}: got={got!r} want={want!r}")


def parse_260626_frames() -> list[dict]:
    """260626 로그에서 (mkop, antc) 원본 프레임 파싱. (널바이트 포함이라 errors='replace')"""
    if not LOG_260626.exists():
        return []
    frames = []
    with open(LOG_260626, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = _FRAME_RE.search(line)
            if m:
                frames.append({"mkop": m.group("mkop"), "antc": m.group("antc"), "trht": m.group("trht")})
    return frames


# ── 1. 실데이터 리플레이: 260626 AF8/AF1/BF9 ──────────────────────────────────
print("─" * 70)
print("1) 260626 실데이터 리플레이 (out/logs/wss_TR_260626.log)")
frames = parse_260626_frames()
af_frames = [fr for fr in frames if fr["mkop"] in ("AF8", "AF1", "BF9")]
print(f"  파싱된 [장운영정보] 프레임 {len(frames)}건, 그중 AF8/AF1/BF9 {len(af_frames)}건")
check("260626 AF/BF 프레임이 실제로 존재(≥7건)", len(af_frames) >= 7, True)

seen_codes = {}
for fr in af_frames:
    r = classify_mkop_event(fr["mkop"], fr["antc"])
    seen_codes.setdefault(fr["mkop"], r)

# AF8 = 서킷 발동 → 시장전파
r = seen_codes.get("AF8")
check("AF8 event_name", r and r["event_name"], "서킷브레이크발동")
check("AF8 cb_trigger(시장전파 발동)", r and r["cb_trigger"], True)
check("AF8 cb_release", r and r["cb_release"], False)
# AF1 = 서킷 해제 → 시장전파 해제 + 종목이벤트 클리어
r = seen_codes.get("AF1")
check("AF1 event_name", r and r["event_name"], "서킷브레이크해제")
check("AF1 cb_release(시장전파 해제)", r and r["cb_release"], True)
check("AF1 clear_market_event", r and r["clear_market_event"], True)
check("AF1 cb_trigger", r and r["cb_trigger"], False)
# BF9 = VI단일가 → 정보성만(시장 동작 없음)
r = seen_codes.get("BF9")
check("BF9 event_name", r and r["event_name"], "VI단일가")
check("BF9 cb_trigger", r and r["cb_trigger"], False)
check("BF9 cb_release", r and r["cb_release"], False)
check("BF9 clear_market_event", r and r["clear_market_event"], False)

# ── 2. 합성 프레임: 매뉴얼 숫자코드 병행 유지 ─────────────────────────────────
print("─" * 70)
print("2) 매뉴얼 숫자코드 병행 매핑 유지 (합성 프레임)")
for code, ename, trig, rel in [
    ("174", "서킷브레이크발동", True, False),
    ("184", "서킷브레이크개시", True, False),
    ("164", "시장임시정지", True, False),
    ("175", "서킷브레이크해제", False, True),
    ("185", "서킷브레이크해제", False, True),
    ("187", "사이드카발동", False, False),
    ("397", "사이드카매수발동", False, False),
    ("388", "사이드카해제", False, False),
]:
    r = classify_mkop_event(code, "")
    check(f"{code} event_name", r and r["event_name"], ename)
    check(f"{code} cb_trigger", r and r["cb_trigger"], trig)
    check(f"{code} cb_release", r and r["cb_release"], rel)

# 사이드카 해제(388/398)는 종목이벤트 클리어 대상이나 시장전파 해제는 아님
check("388 clear_market_event(종목이벤트 클리어)", classify_mkop_event("388", "")["clear_market_event"], True)
check("388 cb_release(시장전파 해제 아님)", classify_mkop_event("388", "")["cb_release"], False)

# ── 3. 필드 우선순위 / 비이벤트 프레임 ────────────────────────────────────────
print("─" * 70)
print("3) 필드 우선순위 + 비이벤트(정보성) 프레임")
# mkop 우선
check("mkop 우선(AF8, antc=112)", classify_mkop_event("AF8", "112")["evt_src"], "mkop_cls_code")
# mkop 비었으면 antc 폴백
check("antc 폴백(mkop='', antc=174)", classify_mkop_event("", "174")["evt_src"], "antc_mkop_cls_code")
# 260626 평시 프레임(None/311, None/112) 및 빈값 → 이벤트 아님(None)
check("None/311 → 이벤트 아님", classify_mkop_event("None", "311"), None)
check("None/112 → 이벤트 아님", classify_mkop_event("None", "112"), None)
check("''/'' → 이벤트 아님", classify_mkop_event("", ""), None)

# ── 4. 상태기계 재생: AF8 → BF9 → AF1 (시장전파 세팅 → 유지 → 해제) ──────────
print("─" * 70)
print("4) 상태기계 재생: AF8(발동) → BF9(단일가) → AF1(해제)")
market_wide = {}          # market → event_name (프로덕션 _market_wide_event 축소판)
market = "KOSPI"
for mkop in ["AF8", "BF9", "AF1"]:
    r = classify_mkop_event(mkop, "112")
    if r and r["cb_trigger"]:
        market_wide[market] = r["event_name"]
    if r and r["cb_release"]:
        market_wide.pop(market, None)
    print(f"    {mkop} → market_wide={market_wide}")
    if mkop == "AF8":
        check("AF8 후 시장전파 세팅됨", market_wide.get(market), "서킷브레이크발동")
    elif mkop == "BF9":
        check("BF9 중 시장전파 유지(서킷 지속)", market_wide.get(market), "서킷브레이크발동")
    elif mkop == "AF1":
        check("AF1 후 시장전파 해제됨", market, market if market not in market_wide else "NOT_CLEARED")

# ── 결과 ──────────────────────────────────────────────────────────────────────
print("─" * 70)
print(f"결과: PASS={_pass}  FAIL={_fail}")
sys.exit(1 if _fail else 0)
