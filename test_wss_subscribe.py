#!/usr/bin/env python3
"""WSS 검증 스크립트 — 운영 본체와 동일한 흐름으로 핸드셰이크 + subscribe + KIS 응답 확인.

목적:
  - 운영 본체(ws_realtime_trading.py)가 connect 후 첫 subscribe 송신 시점에 KIS 가 1006 으로 끊는지
  - 또는 정상 SUBSCRIBE SUCCESS 응답을 받고 데이터가 들어오는지
  를 운영 본체를 안 건드리고 별도로 검증.

운영 본체와의 차이:
  - 14종목 대신 작은 2종목으로 축소
  - parquet 저장/매수 로직 등 없음 (수신만 모니터링)
  - dwell 15초 동안만 동작 후 깔끔히 종료

종료 후 콘솔에 PASS/FAIL 명확히 출력.
"""
import sys
import time
import json
import threading
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))

import kis_auth_llm as ka
# KIS WebSocket SDK 가 내부적으로 'kis_auth' 모듈을 참조 → alias 설정 필수
# (운영 본체 ws_realtime_trading.py:450 과 동일 패턴)
sys.modules["kis_auth"] = ka
from domestic_stock_functions_ws import ccnl_krx, ccnl_notice  # noqa: F401

KST = ZoneInfo("Asia/Seoul")
DWELL_SEC = 15.0           # 검증 모니터링 시간
TEST_CODES = ["005930", "000660"]  # 삼성전자, SK하이닉스 (장 외 시간이라도 등락률 0 row 정도는 들어옴)

# ── 메트릭 ──
_metrics = {
    "system_msgs": [],   # rsp.tr_msg 모음 (SUBSCRIBE SUCCESS, ALREADY IN USE 등)
    "data_count": 0,     # on_result 호출 횟수 (실데이터 frame)
    "data_first_ts": None,
    "exceptions": [],
}


def _ts() -> str:
    return datetime.now(KST).strftime("%H:%M:%S.%f")[:-3]


def on_system(rsp):
    msg = getattr(rsp, "tr_msg", "") or ""
    tr_id = getattr(rsp, "tr_id", "") or ""
    tr_key = getattr(rsp, "tr_key", "") or ""
    line = f"[{_ts()}] [system] tr_id={tr_id} tr_key={tr_key} msg={msg}"
    print(line, flush=True)
    _metrics["system_msgs"].append({"tr_id": tr_id, "tr_key": tr_key, "msg": msg})


def on_result(ws, tr_id, result, data_info):
    _metrics["data_count"] += 1
    if _metrics["data_first_ts"] is None:
        _metrics["data_first_ts"] = time.time()
        print(f"[{_ts()}] [data] FIRST FRAME tr_id={tr_id}", flush=True)


def main():
    print("=" * 70)
    print(f"WSS subscribe 검증 — DWELL={DWELL_SEC}s, codes={TEST_CODES}")
    print("=" * 70)

    # ── 1) auth ──
    print(f"[{_ts()}] auth_ws...", flush=True)
    ka.auth(svr="prod")
    ka.auth_ws(svr="prod")
    ak = (getattr(ka, "_base_headers_ws", {}) or {}).get("approval_key")
    if not ak:
        print("✗ approval_key 발급 실패")
        sys.exit(1)
    print(f"[{_ts()}] approval_key OK ({ak[:8]}...{ak[-4:]})", flush=True)

    trenv = ka.getTREnv()
    htsid = getattr(trenv, "my_htsid", "") if trenv else ""
    print(f"[{_ts()}] htsid={htsid!r}", flush=True)

    # ── 2) subscribe (운영 본체와 동일 방식) ──
    print(f"[{_ts()}] subscribe registering: H0STCNI0(notice) + ccnl_krx({len(TEST_CODES)})", flush=True)
    if htsid:
        ka.KISWebSocket.subscribe(ccnl_notice, [htsid])
    ka.KISWebSocket.subscribe(ccnl_krx, TEST_CODES)

    # ── 3) KIS WebSocket 인스턴스 + start ──
    kws = ka.KISWebSocket(api_url="", max_retries=10)
    kws.on_system = on_system

    print(f"[{_ts()}] kws.start (max {DWELL_SEC}s)...", flush=True)
    start_ts = time.time()

    # start() 는 blocking 이므로 별도 스레드에서 실행, DWELL_SEC 후 close
    def _run():
        try:
            kws.start(on_result=on_result)
        except Exception as e:
            _metrics["exceptions"].append(repr(e))
            print(f"[{_ts()}] [exception] {e!r}", flush=True)

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    # ── 4) DWELL_SEC 동안 메트릭 누적 ──
    deadline = time.time() + DWELL_SEC
    while time.time() < deadline:
        time.sleep(0.5)
        if not t.is_alive():
            break  # kws 가 자체 종료 (max_retries 소진)

    dwell = time.time() - start_ts
    print(f"[{_ts()}] dwell={dwell:.2f}s, kws_alive={t.is_alive()}", flush=True)

    # ── 5) 깔끔히 close ──
    try:
        for attr in ("close", "shutdown"):
            fn = getattr(kws, attr, None)
            if callable(fn):
                fn()
                break
    except Exception:
        pass

    # ── 6) 판정 ──
    print()
    print("=" * 70)
    print("결과 요약")
    print("=" * 70)
    print(f"  system_msgs : {len(_metrics['system_msgs'])} 건")
    for m in _metrics["system_msgs"][:10]:
        print(f"    - tr_id={m['tr_id']} tr_key={m['tr_key']} msg={m['msg']}")
    print(f"  data_frames : {_metrics['data_count']} 건")
    print(f"  exceptions  : {len(_metrics['exceptions'])} 건")
    for e in _metrics["exceptions"][:5]:
        print(f"    - {e}")
    print(f"  dwell       : {dwell:.2f}s (목표 {DWELL_SEC}s)")
    print()

    # 판정 기준:
    #   PASS: dwell >= DWELL_SEC*0.7 + 첫 SUBSCRIBE SUCCESS 응답 또는 data frame >= 1
    #   FAIL: dwell < 12s + 1006 다발 패턴
    success_msgs = [m for m in _metrics["system_msgs"]
                    if "SUCCESS" in (m["msg"] or "").upper()]
    if dwell >= DWELL_SEC * 0.7 and (success_msgs or _metrics["data_count"] > 0):
        print("✅ PASS — KIS WSS 가 subscribe 를 정상 처리 (1006 다발 없음)")
        print("   → 882ed3c 변경이 이전 09:13/11:25 실패의 원인일 가능성 높음")
        print("   → 운영 본체도 5.8 시점 코드로는 정상 동작 예상")
        sys.exit(0)
    elif dwell < 12.0 and len(_metrics["system_msgs"]) == 0:
        print("❌ FAIL — kws.start 가 12s 미만에 종료, system 응답 0건")
        print("   → 1006 다발 (KIS 가 핸드셰이크 후 즉시 close) 패턴 그대로")
        print("   → 882ed3c 와 무관. KIS 측 차단/장애. 1544-5000 문의 필요")
        sys.exit(2)
    else:
        print("⚠ UNCLEAR — 명확한 PASS/FAIL 분류 안 됨. 메트릭 직접 검토 필요")
        sys.exit(3)


if __name__ == "__main__":
    main()
