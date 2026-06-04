#!/usr/bin/env python3
"""
test_wss_close_cycle.py — WSS 연결→구독→graceful close→즉시 재접속 사이클 독립 검증
[260602] 재시작 직후 1006(invalid approval) 의 원인/수정 검증용. 메인 프로덕션(a1) 무중단.

목적:
  - 메인을 멈추지 않고(= a2(syw_2) approval_key 사용), 실제 KISWebSocket(kis_auth_llm) 으로
    프로덕션과 "동일한 close 경로"를 태워 다음을 숫자로 측정한다.
      1) close() 소요시간 (close frame 실제 송신 시간 — KISWebSocket.close 가 "close frame 송신 완료(Nms)" 로깅)
      2) UNSUBSCRIBE 단계 소요시간 (mode=unsubclose 일 때) — 프로덕션 _request_ws_close 가 5초를
         UNSUBSCRIBE 에서 먹는지 close 에서 먹는지 분리 확인
      3) close 직후 "즉시 재접속" 시 1006(dwell<15s) 발생 여부 — 같은 approval_key 충돌 재현

모드:
  unsubclose  (기본) — UNSUBSCRIBE(2초 예산) 후 close. 프로덕션 _request_ws_close 모사.
  closeonly          — close 만 (UNSUBSCRIBE 생략). close 단독이 깨끗이 끝나는지 비교.

실행:
  ./venv/bin/python test_wss_close_cycle.py                          # a2, 2사이클, unsubclose, 즉시재접속
  ./venv/bin/python test_wss_close_cycle.py --mode closeonly
  ./venv/bin/python test_wss_close_cycle.py --reconnect-delay 5      # 재접속 전 5초 대기(세션해제 가설 검증)

주의:
  - a2 approval_key 사용. a2 WSS 를 점유하는 라이브 소비자가 없을 때 실행(현재 Daily_vi WSS 비활성).
  - 메인(a1) 과는 appkey/approval_key 가 분리되어 영향 없음.
"""
import argparse
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone, timedelta

import kis_auth_llm as ka
# domestic_stock_functions_ws 는 `import kis_auth as ka` 로 data_fetch 호출 → 별칭 등록 필수
sys.modules["kis_auth"] = ka
from domestic_stock_functions_ws import ccnl_krx  # noqa: E402

KST = timezone(timedelta(hours=9))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "out", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

_logf = None


def log(msg: str) -> None:
    line = f"[{datetime.now(KST).strftime('%H:%M:%S.%f')[:-3]}] {msg}"
    print(line, flush=True)
    if _logf:
        _logf.write(line + "\n")
        _logf.flush()


# ── KISWebSocket 내부 로그(close frame 송신/1006/approval/구독응답) 캡처 ──
_captured: list[tuple[str, str]] = []


class _Capture(logging.Handler):
    def emit(self, record):
        try:
            m = record.getMessage()
        except Exception:
            return
        if any(k in m for k in ("close frame", "1006", "ConnectionClosed",
                                 "approval", "SUBSCRIBE", "invalid")):
            _captured.append((datetime.now(KST).strftime("%H:%M:%S.%f")[:-3], m))


def pick_account(alias: str) -> dict:
    with open(os.path.join(BASE_DIR, "config.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg["users"][cfg["default_user"]]["accounts"][alias]


_tick = {"n": 0}


def _on_result(ws, tr_id, df, info) -> None:
    _tick["n"] += 1


def _on_system(rsp) -> None:
    try:
        log(f"   [SYS] tr_id={rsp.tr_id} key={getattr(rsp, 'tr_key', '')} msg={getattr(rsp, 'tr_msg', '')}")
    except Exception:
        pass


def _start_kws():
    """KISWebSocket 1개를 백그라운드 스레드에서 start (프로덕션과 동일). max_retries=1 → 자동 재접속 없이 단일 연결 관찰."""
    ka.open_map.clear()
    kws = ka.KISWebSocket(api_url="", max_retries=1)
    kws.on_system = _on_system
    t = threading.Thread(target=lambda: kws.start(on_result=_on_result), daemon=True)
    t.start()
    return kws, t


def _wait_connect(kws, timeout: float = 8.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if getattr(kws, "_ws", None) is not None:
            return time.time() - t0
        time.sleep(0.1)
    return None


def _watch_dwell(kws, watch: float = 15.0):
    """연결 후 watch초 동안 _ws 유지 감시. 끊기면 dwell(초) 반환(1006 의심), 끝까지 살아있으면 None."""
    t0 = time.time()
    while time.time() - t0 < watch:
        if getattr(kws, "_ws", None) is None:
            return time.time() - t0
        time.sleep(0.2)
    return None


def cycle(i: int, code: str, mode: str, reconnect_delay: float) -> None:
    log(f"━━━━━━ Cycle {i} (mode={mode}, reconnect_delay={reconnect_delay}s) ━━━━━━")

    # 1) 연결
    kws, t = _start_kws()
    ct = _wait_connect(kws)
    if ct is None:
        log("  [연결실패] 8s 내 미연결 → 사이클 중단")
        return
    log(f"  [연결] {ct * 1000:.0f}ms")

    # 2) 구독 + 수신
    _tick["n"] = 0
    try:
        kws.send_request(request=ccnl_krx, tr_type="1", data=code)
        log(f"  [구독] ccnl_krx {code} 송신 완료")
    except Exception as e:
        log(f"  [구독실패] {type(e).__name__}: {e}")
    time.sleep(3.0)
    log(f"  [수신] {_tick['n']}틱 (3초)")

    # 3) graceful close (단계별 측정)
    t_phase = time.time()
    if mode == "unsubclose":
        # 프로덕션 _request_ws_close 모사: UNSUBSCRIBE(스레드+예산) → close
        _us = time.time()
        ut = threading.Thread(
            target=lambda: kws.send_request(request=ccnl_krx, tr_type="2", data=code),
            daemon=True,
        )
        ut.start()
        ut.join(timeout=2.0)
        log(f"  [UNSUBSCRIBE] {'완료' if not ut.is_alive() else 'timeout(2s)'} (+{time.time() - _us:.2f}s)")

    _tc = time.time()
    kws.close(timeout=10.0)   # 실제 프로덕션과 동일한 close 경로
    log(f"  [close] kws.close() 반환 +{time.time() - _tc:.2f}s  | close단계 총 +{time.time() - t_phase:.2f}s")
    t.join(timeout=3.0)
    log(f"  [start스레드] {'정상 종료' if not t.is_alive() else '아직 살아있음(루프 미종료)'}")

    # 4) 즉시(또는 delay) 재접속 → 1006 발생 여부
    if reconnect_delay > 0:
        log(f"  [재접속 대기] {reconnect_delay}s …")
        time.sleep(reconnect_delay)
    log(f"  [재접속] 같은 approval_key 로 새 연결 시도 (delay={reconnect_delay}s)")
    _tick["n"] = 0
    kws2, t2 = _start_kws()
    ct2 = _wait_connect(kws2)
    if ct2 is None:
        log("  ★ [재접속 결과] 8s 내 미연결 (핸드셰이크 실패 추정)")
    else:
        log(f"  [재접속] 연결 {ct2 * 1000:.0f}ms → 구독 후 15초 dwell 감시")
        try:
            kws2.send_request(request=ccnl_krx, tr_type="1", data=code)
        except Exception as e:
            log(f"   [재접속 구독실패] {type(e).__name__}: {e}")
        dwell = _watch_dwell(kws2, watch=15.0)
        if dwell is not None:
            log(f"  ★ [재접속 1006 의심] 연결 {dwell:.2f}s 만에 끊김 (dwell<15s = invalid approval 패턴)")
        else:
            log(f"  ✔ [재접속 정상] 15초 유지, {_tick['n']}틱 수신 (1006 없음)")
    try:
        kws2.close(timeout=10.0)
    except Exception:
        pass
    t2.join(timeout=3.0)


def main():
    global _logf
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", default="a2")
    ap.add_argument("--code", default="005930")
    ap.add_argument("--cycles", type=int, default=2)
    ap.add_argument("--mode", default="unsubclose", choices=["unsubclose", "closeonly"])
    ap.add_argument("--reconnect-delay", type=float, default=0.0)
    args = ap.parse_args()

    logging.getLogger().addHandler(_Capture())
    logging.getLogger().setLevel(logging.INFO)

    stamp = datetime.now(KST).strftime("%y%m%d_%H%M")
    _logf = open(os.path.join(LOG_DIR, f"test_wss_close_{stamp}.log"), "w", encoding="utf-8")

    acct = pick_account(args.account)
    ka._cfg["my_app"] = acct["appkey"]
    ka._cfg["my_sec"] = acct["appsecret"]
    ka.auth_ws(svr="prod")
    key = ka._base_headers_ws.get("approval_key")
    log(f"=== WSS close 사이클 테스트 | account={args.account}(cano={acct.get('cano')}) "
        f"approval_key 앞12={str(key)[:12]} mode={args.mode} cycles={args.cycles} delay={args.reconnect_delay}s ===")

    try:
        for i in range(1, args.cycles + 1):
            cycle(i, args.code, args.mode, args.reconnect_delay)
            time.sleep(1.0)
    finally:
        log("──────── KISWebSocket 내부 캡처 로그 (close frame/1006/approval/SUBSCRIBE) ────────")
        for ts, m in _captured[-50:]:
            log(f"   [{ts}] {m[:170]}")
        if _logf:
            log(f"[로그파일] {_logf.name}")
            _logf.close()


if __name__ == "__main__":
    main()
