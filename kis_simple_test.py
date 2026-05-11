"""
KIS 공식 샘플(legacy/websocket/python/ops_ws_sample.py) 기반 최소 WSS 테스트.
주식체결가(H0STCNT0) 005930(삼성전자) 1건 구독 → 5초 수신 → 종료.
"""
import asyncio
import json
import time
import requests
import websockets

APPKEY = "PS2AwZgtkyS1XfYUONVrCbQjCS1Xz8nNmRwd"
APPSECRET = "L9KniMBz98nL/923E/NV2Evjfn4vnG4Zc5wtGFCeaUCqRw1rwxUMudK3QYXxIJAZSgw7kQPL/F7MWCFI8znKux+woYRHxXbncWce364PSZH1Tg8DxEHLSGsG0NbZ9GwNolU9+V+7krSRSaf7DCGGcePvSWgMdELSNjCpIxpVsYXZFbuPPlE="
URL_REST = "https://openapi.koreainvestment.com:9443"
URL_WS = "ws://ops.koreainvestment.com:21000"  # 실전
STOCKCODE = "005930"  # 삼성전자


def get_approval(key, secret):
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": key, "secretkey": secret}
    res = requests.post(f"{URL_REST}/oauth2/Approval", headers=headers, data=json.dumps(body), timeout=10)
    return res.json()["approval_key"]


async def connect():
    try:
        approval_key = get_approval(APPKEY, APPSECRET)
        print(f"[1] approval_key: {approval_key}")

        print(f"[2] websockets.connect({URL_WS}) 시도...")
        async with websockets.connect(URL_WS, ping_interval=None) as ws:
            print(f"[3] WSS 연결 성공!")

            # 주식체결 H0STCNT0 / 005930 등록
            senddata = (
                '{"header":{"approval_key":"' + approval_key + '","custtype":"P",'
                '"tr_type":"1","content-type":"utf-8"},'
                '"body":{"input":{"tr_id":"H0STCNT0","tr_key":"' + STOCKCODE + '"}}}'
            )
            print(f"[4] subscribe send: {senddata}")
            await ws.send(senddata)

            # 5초간 수신
            t0 = time.time()
            while time.time() - t0 < 5:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    print(f"[recv +{time.time()-t0:.1f}s] {msg[:200]}")
                except asyncio.TimeoutError:
                    print(f"[recv +{time.time()-t0:.1f}s] (no message in 1s)")

    except Exception as e:
        print(f"[ERR] {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(connect())
