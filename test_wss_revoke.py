"""KIS revokeP + WSS 슬롯 해제 시험
- main appkey 로:
  1) /oauth2/tokenP → access_token 발급
  2) /oauth2/revokeP → access_token 폐기
  3) 3s 대기
  4) /oauth2/Approval → 새 approval_key 발급
  5) WSS H0STCNI0 subscribe probe → ALREADY IN USE 풀렸는지 검증
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


def issue_access_token(appkey: str, appsecret: str) -> tuple[int, dict, float]:
    url = f"{REST_URL}/oauth2/tokenP"
    headers = {"content-type": "application/json; charset=utf-8"}
    data = {"grant_type": "client_credentials", "appkey": appkey, "appsecret": appsecret}
    t0 = time.time()
    try:
        r = requests.post(url, headers=headers, json=data, timeout=10)
        return r.status_code, r.json(), time.time() - t0
    except Exception as e:
        return -1, {"error": f"{type(e).__name__}: {e}"}, time.time() - t0


def revoke_token(appkey: str, appsecret: str, token: str) -> tuple[int, dict, float]:
    url = f"{REST_URL}/oauth2/revokeP"
    headers = {"content-type": "application/json; charset=utf-8"}
    data = {"appkey": appkey, "appsecret": appsecret, "token": token}
    t0 = time.time()
    try:
        r = requests.post(url, headers=headers, json=data, timeout=10)
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text[:200]}
        return r.status_code, body, time.time() - t0
    except Exception as e:
        return -1, {"error": f"{type(e).__name__}: {e}"}, time.time() - t0


def issue_approval_key(appkey: str, appsecret: str) -> tuple[int, dict, float]:
    url = f"{REST_URL}/oauth2/Approval"
    headers = {"content-type": "application/json; charset=utf-8"}
    data = {"grant_type": "client_credentials", "appkey": appkey, "secretkey": appsecret}
    t0 = time.time()
    try:
        r = requests.post(url, headers=headers, json=data, timeout=10)
        return r.status_code, r.json(), time.time() - t0
    except Exception as e:
        return -1, {"error": f"{type(e).__name__}: {e}"}, time.time() - t0


async def ws_subscribe_probe(approval_key: str, htsid: str) -> tuple[str, float, list[str]]:
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
                    if len(msgs) >= 2:
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
    acc = cfg["users"]["sywems12"]["accounts"]["a1"]
    appkey = acc["appkey"]
    appsecret = acc["appsecret"]
    cano = acc["cano"]
    htsid = "sywems12"

    print(f"[main] cano={cano}  appkey={mask(appkey)}")
    print("=" * 72)

    # (0) PRE: revoke 전, 현재 슬롯 상태 확인 (새 approval_key 로 probe)
    print("\n[STEP 0] revoke 전 슬롯 상태 확인")
    sc, body, dt = issue_approval_key(appkey, appsecret)
    ak0 = body.get("approval_key", "")
    if not ak0:
        print(f"  Approval FAIL status={sc} body={body}")
        return 1
    print(f"  Approval OK status={sc} dwell={dt:.2f}s approval_key={mask(ak0)}")
    v_pre, dt_pre, msgs_pre = asyncio.run(ws_subscribe_probe(ak0, htsid))
    print(f"  Probe (PRE)  : {v_pre}  dwell={dt_pre:.2f}s")
    for m in msgs_pre:
        print(f"    [resp] {m}")
    pre_locked = any("ALREADY IN USE" in m for m in msgs_pre)
    print(f"  → 슬롯 잠김 여부: {'YES (ALREADY IN USE)' if pre_locked else 'NO (정상)'}")

    if not pre_locked:
        print("\n[STOP] revoke 전에 이미 슬롯이 풀려있음 — 시험 의미 없음 (KIS 자연 만료)")
        return 0

    # (1) access_token 발급
    print("\n[STEP 1] /oauth2/tokenP → access_token 발급")
    sc, body, dt = issue_access_token(appkey, appsecret)
    token = body.get("access_token", "")
    if not token:
        print(f"  FAIL status={sc} body={body}")
        return 2
    print(f"  OK status={sc} dwell={dt:.2f}s access_token={mask(token, 12, 8)}")

    # (2) revokeP
    print("\n[STEP 2] /oauth2/revokeP → access_token 폐기")
    sc, body, dt = revoke_token(appkey, appsecret, token)
    print(f"  status={sc} dwell={dt:.2f}s body={body}")

    # (3) 3초 대기
    print("\n[STEP 3] 3s 대기 (KIS 서버 슬롯 정리 시간)")
    time.sleep(3.0)

    # (4) 새 approval_key 발급
    print("\n[STEP 4] /oauth2/Approval → 새 approval_key 발급")
    sc, body, dt = issue_approval_key(appkey, appsecret)
    ak1 = body.get("approval_key", "")
    if not ak1:
        print(f"  FAIL status={sc} body={body}")
        return 3
    print(f"  OK status={sc} dwell={dt:.2f}s approval_key={mask(ak1)}")
    same_as_before = (ak1 == ak0)
    print(f"  → approval_key 동일 여부 (revoke 전과): {'SAME (KIS throttle)' if same_as_before else 'NEW (변경됨)'}")

    # (5) probe 재실행
    print("\n[STEP 5] WSS H0STCNI0 subscribe probe (POST-revoke)")
    v_post, dt_post, msgs_post = asyncio.run(ws_subscribe_probe(ak1, htsid))
    print(f"  Probe (POST) : {v_post}  dwell={dt_post:.2f}s")
    for m in msgs_post:
        print(f"    [resp] {m}")
    post_locked = any("ALREADY IN USE" in m for m in msgs_post)

    print("\n" + "=" * 72)
    print("[결론]")
    print(f"  PRE  슬롯 잠김: {'YES' if pre_locked else 'NO'}")
    print(f"  POST 슬롯 잠김: {'YES' if post_locked else 'NO'}")
    if pre_locked and not post_locked:
        print("  ★ revokeP 가 WSS 슬롯도 해제함 → 좀비 만능 해제법 발견!")
    elif pre_locked and post_locked:
        print("  ✗ revokeP 무관 — 좀비는 KIS 자연 만료만 기다려야 함")
    return 0


if __name__ == "__main__":
    sys.exit(main())
