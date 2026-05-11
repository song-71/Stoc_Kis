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


def main() -> int:
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)
    try:
        accs = cfg["users"]["sywems12"]["accounts"]
    except KeyError:
        print("[ERROR] config.json 구조 미일치 (users.sywems12.accounts)")
        return 1

    targets = [("main (a1)", accs.get("main")), ("syw_2 (a2)", accs.get("syw_2"))]

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

    print("\n" + "=" * 72)
    print("[해석]")
    print("  REST OK + WSS OK   → 정상")
    print("  REST OK + WSS FAIL → IP/host 단위 WSS throttle 또는 KIS WSS 차단")
    print("  REST FAIL          → appkey/secret 자체 문제")
    return 0


if __name__ == "__main__":
    sys.exit(main())
