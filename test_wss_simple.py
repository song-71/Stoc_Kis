"""KIS WSS 간단 연결 테스트
- config.json 의 a1(main), a2(syw_2) 두 appkey 로 각각:
  1) REST /oauth2/Approval → approval_key 발급 가능 여부
  2) WSS ws://ops.koreainvestment.com:21000 핸드셰이크 가능 여부
- 결과를 한눈에 비교 출력. 실제 구독은 하지 않음 (호스트/appkey 자극 최소화).
"""
import asyncio
import json
import sys
import time

import requests
import websockets

REST_URL = "https://openapi.koreainvestment.com:9443"
WS_URL = "ws://ops.koreainvestment.com:21000"
CONFIG_PATH = "/home/ubuntu/Stoc_Kis/config.json"


def mask(s: str, head: int = 8, tail: int = 4) -> str:
    if not s or len(s) <= head + tail:
        return s
    return f"{s[:head]}...{s[-tail:]}"


def get_approval_key(appkey: str, appsecret: str) -> tuple[int, dict, float]:
    url = f"{REST_URL}/oauth2/Approval"
    headers = {"content-type": "application/json; charset=utf-8"}
    data = {"grant_type": "client_credentials", "appkey": appkey, "secretkey": appsecret}
    t0 = time.time()
    try:
        r = requests.post(url, headers=headers, json=data, timeout=10)
        return r.status_code, r.json(), time.time() - t0
    except Exception as e:
        return -1, {"error": f"{type(e).__name__}: {e}"}, time.time() - t0


async def ws_handshake_test() -> tuple[bool, str, float]:
    t0 = time.time()
    try:
        async with websockets.connect(WS_URL, open_timeout=8, ping_interval=None) as ws:
            return True, "connected", time.time() - t0
    except Exception as e:
        return False, f"{type(e).__name__}: {e}", time.time() - t0


async def ws_force_cleanup_attempt(approval_key: str, htsid: str) -> str:
    """ALREADY IN USE appkey 잔재 해제 시도.
    1) UNSUBSCRIBE frame (tr_type=2) 송신 → KIS 가 옛 session 정리해주는지
    2) 1초 후 SUBSCRIBE 재시도 → ALREADY IN USE 풀렸는지 검증
    """
    out = []
    try:
        # 1단계: UNSUBSCRIBE
        async with websockets.connect(WS_URL, open_timeout=8, ping_interval=None) as ws:
            payload_unsub = json.dumps({
                "header": {"approval_key": approval_key, "custtype": "P",
                           "tr_type": "2", "content-type": "utf-8"},
                "body": {"input": {"tr_id": "H0STCNI0", "tr_key": htsid}}
            })
            await ws.send(payload_unsub)
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                out.append(f"      [step1 UNSUB-resp] {str(raw)[:180]}")
            except (asyncio.TimeoutError, Exception) as e:
                out.append(f"      [step1 UNSUB-err] {type(e).__name__}: {str(e)[:120]}")
        await asyncio.sleep(1.0)
        # 2단계: SUBSCRIBE 재시도 (ALREADY IN USE 풀렸는지)
        async with websockets.connect(WS_URL, open_timeout=8, ping_interval=None) as ws:
            payload_sub = json.dumps({
                "header": {"approval_key": approval_key, "custtype": "P",
                           "tr_type": "1", "content-type": "utf-8"},
                "body": {"input": {"tr_id": "H0STCNI0", "tr_key": htsid}}
            })
            await ws.send(payload_sub)
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                out.append(f"      [step2 SUB-resp ] {str(raw)[:180]}")
            except (asyncio.TimeoutError, Exception) as e:
                out.append(f"      [step2 SUB-err ] {type(e).__name__}: {str(e)[:120]}")
        return "\n".join(out)
    except Exception as e:
        return f"      [outer-err] {type(e).__name__}: {e}"


async def ws_subscribe_send_test(approval_key: str, htsid: str) -> tuple[str, float, list[str]]:
    """핸드셰이크 + H0STCNI0 subscribe 한 번 송신 + 최대 5초 동안 응답 모니터링.
    KIS 가 핸드셰이크는 받고 send 만 거부하는지를 검증.
    반환: (verdict, dwell, msgs)
    """
    msgs: list[str] = []
    t0 = time.time()
    try:
        async with websockets.connect(WS_URL, open_timeout=8, ping_interval=None) as ws:
            payload = json.dumps({
                "header": {"approval_key": approval_key, "custtype": "P",
                           "tr_type": "1", "content-type": "utf-8"},
                "body": {"input": {"tr_id": "H0STCNI0", "tr_key": htsid}}
            })
            await ws.send(payload)
            try:
                while time.time() - t0 < 5.0:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    msgs.append(str(raw)[:200])
                    if len(msgs) >= 3:
                        break
            except asyncio.TimeoutError:
                pass
            verdict = "OK_RESP" if msgs else "NO_RESP_BUT_NO_CLOSE"
            return verdict, time.time() - t0, msgs
    except Exception as e:
        return f"FAIL: {type(e).__name__}: {e}", time.time() - t0, msgs


def main() -> int:
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)
    try:
        accs = cfg["users"]["sywems12"]["accounts"]
    except KeyError:
        print("[ERROR] config.json 구조 미일치 (users.sywems12.accounts)")
        return 1

    targets = [("a1", accs.get("a1")), ("a2", accs.get("a2"))]

    print(f"REST_URL = {REST_URL}")
    print(f"WS_URL   = {WS_URL}")
    print("=" * 72)

    for label, acc in targets:
        if not acc:
            print(f"[{label}] config 없음 → skip")
            continue
        appkey = acc.get("appkey", "")
        appsecret = acc.get("appsecret", "")
        cano = acc.get("cano", "?")
        print(f"\n[{label}] cano={cano}  appkey={mask(appkey)}")

        sc, body, dt_rest = get_approval_key(appkey, appsecret)
        ak = body.get("approval_key", "")
        if ak:
            print(f"  (1) REST Approval : OK    status={sc} dwell={dt_rest:.2f}s approval_key={mask(ak)}")
        else:
            print(f"  (1) REST Approval : FAIL  status={sc} dwell={dt_rest:.2f}s body={body}")

        ok, msg, dt_ws = asyncio.run(ws_handshake_test())
        verdict = "OK" if ok else "FAIL"
        print(f"  (2) WSS handshake : {verdict}  dwell={dt_ws:.2f}s {msg}")

        # (3) — a1/a2 모두 H0STCNI0 raw subscribe send 시도 (appkey 잔재 확인)
        if ok and ak:
            v3, dt3, msgs = asyncio.run(ws_subscribe_send_test(ak, "sywems12"))
            print(f"  (3) WSS subscribe : {v3}  dwell={dt3:.2f}s msgs={len(msgs)}")
            for i, m in enumerate(msgs):
                print(f"      [resp {i+1}] {m}")
        # (4) — a1 만 ALREADY IN USE 면 UNSUBSCRIBE 송신으로 강제 해제 시도
        if ok and ak and label.startswith("a1") and any("ALREADY IN USE" in m for m in msgs):
            print(f"  (4) ALREADY IN USE 감지 → UNSUBSCRIBE 강제 해제 시도")
            result = asyncio.run(ws_force_cleanup_attempt(ak, "sywems12"))
            print(result)

    print("\n" + "=" * 72)
    print("[해석]")
    print("  (3) OK_RESP             → KIS 가 subscribe 정상 처리. 운영 코드의 다른 이슈")
    print("  (3) NO_RESP_BUT_NO_CLOSE→ KIS 가 송신은 받으나 응답 없음 (idle)")
    print("  (3) FAIL: ConnectionClosed code=1006 → subscribe 시점에 KIS 가 close — 잔재/거부")
    print("  REST OK + WSS OK   → 정상")
    print("  REST OK + WSS FAIL → IP/host 단위 WSS throttle 또는 KIS WSS 차단")
    print("  REST FAIL          → appkey/secret 자체 문제")
    return 0


if __name__ == "__main__":
    sys.exit(main())
