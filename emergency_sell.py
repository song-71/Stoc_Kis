"""긴급 매도: 시공테크(020710) 3주 + 케이사인(192250) 1주 시장가 매도"""
import sys, json, time, requests, hashlib
sys.path.insert(0, "/home/ubuntu/Stoc_Kis")
import kis_auth_llm as ka

ka.auth(svr="prod")
trenv = ka.getTREnv()
base_url = trenv.my_url
appkey = trenv.my_app
appsecret = trenv.my_sec
cano = "43444822"
acnt = "01"

def _hashkey(body):
    url = f"{base_url}/uapi/hashkey"
    headers = {"content-type": "application/json", "appkey": appkey, "appsecret": appsecret}
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    return r.json().get("HASH", "")

def sell_market(code, qty):
    body = {"CANO": cano, "ACNT_PRDT_CD": acnt, "PDNO": code,
            "ORD_DVSN": "01", "ORD_QTY": str(qty), "ORD_UNPR": "0"}
    url = f"{base_url}/uapi/domestic-stock/v1/trading/order-cash"
    headers = {
        "content-type": "application/json",
        "authorization": ka._base_headers.get("authorization", ""),
        "appkey": appkey, "appsecret": appsecret,
        "tr_id": "TTTC0801U", "custtype": "P",
        "hashkey": _hashkey(body),
    }
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    j = r.json()
    print(f"[{code}] qty={qty} → rt_cd={j.get('rt_cd')} msg={j.get('msg1')} odno={j.get('output',{}).get('ODNO','')}")
    return j

for code, qty in [("020710", 3), ("192250", 1)]:
    try:
        sell_market(code, qty)
    except Exception as e:
        print(f"[{code}] ERROR: {e}")
    time.sleep(0.3)
