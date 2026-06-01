#!/usr/bin/env python3
"""
test_vi_wss_cycle.py — VI 발동/해제 사이클 WSS 검증 (별도 테스트, 운영 무영향 목표)
[260601 수립] kis_simple_test.py 를 확장 — 최신 vi_status 발동중 종목 대상으로:
  1) REST 현재가(FHKST01010100) 의 장운영/VI 관련 필드 확인 (temp_stop_yn 등)
  2) H0STMKO0(장운영정보) 구독 → 구독 즉시/이후 수신 프레임 + "VI 발동시각" 정보 유무 확인
     (H0STMKO0 컬럼엔 vi_cls_code/mkop_cls_code 는 있으나 발동시각 전용 필드는 없음 → 실측 확인)
  3) H0STANC0(예상체결) 동시 구독 → VI 동시호가 "결정가(예상체결가)" 수신 확인
  4) H0STMKO0 에서 VI 해제 감지 → H0STCNT0(실시간체결) 구독 전환 → 해제 직후 체결 도착 확인
  모든 수신을 타임스탬프와 함께 로그파일로 남겨 분석.

[approval_key 충돌 주의]
  - 이 테스트는 --account(기본 syw_2=a2) 의 approval_key 를 ka.auth_ws 로 확보(신규 중앙 per-appkey 캐시 재사용).
  - 같은 계좌의 다른 WSS 소비자(메인=a1, Daily_vi=a2)가 동시에 돌면 KIS 'ALREADY IN USE' 충돌 →
    해당 계좌 소비자를 멈춘 상태에서 단독 실행 권장.
  - 라이브 검증은 장중(VI 발생 시간대) 수행. 장 마감 후엔 접속/구독 구조만 확인됨.

실행:
  ./venv/bin/python test_vi_wss_cycle.py                      # a2, 최신 vi_status 발동중 자동선택, 10분
  ./venv/bin/python test_vi_wss_cycle.py --account main --code 005930 --minutes 5
"""
import argparse
import asyncio
import csv
import glob
import json
import os
import time
from datetime import datetime

import requests
import websockets

import kis_auth_llm as ka
from kis_utils import load_config
from domestic_stock_functions_ws import ccnl_krx, exp_ccnl_krx, market_status_krx  # noqa: F401

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VI_DIR = os.path.join(BASE_DIR, "data", "vi_status", "1m_fetch", "backup")
LOG_DIR = os.path.join(BASE_DIR, "out", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
URL_WS = "ws://ops.koreainvestment.com:21000"

# tr_id → (구독함수, 라벨)  / 컬럼은 구독 시 함수 반환값에서 채움
SUBS = {
    "H0STMKO0": (market_status_krx, "장운영정보"),
    "H0STANC0": (exp_ccnl_krx, "예상체결"),
    "H0STCNT0": (ccnl_krx, "실시간체결"),
}
_cols = {}            # tr_id → columns
_logf = None


def _ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def log(msg):
    line = f"[{_ts()}] {msg}"
    print(line, flush=True)
    if _logf:
        _logf.write(line + "\n"); _logf.flush()


def pick_account(alias):
    cfg = load_config(os.path.join(BASE_DIR, "config.json"))
    accts = cfg["users"][cfg["default_user"]]["accounts"]
    if alias not in accts:
        raise SystemExit(f"계좌 '{alias}' 없음. 사용가능: {list(accts)}")
    return accts[alias]


def latest_active_vi_code():
    """최신 vi_status CSV 에서 '발동중' 종목 중 발동시간이 가장 늦은 것."""
    files = sorted(glob.glob(os.path.join(VI_DIR, "vi_status_*.csv")))
    if not files:
        return None
    newest = files[-1]
    best = None
    with open(newest, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("상태", "").strip() == "발동중":
                t = row.get("발동시간", "0")
                if best is None or t > best.get("발동시간", "0"):
                    best = row
    log(f"[vi_status] 파일={os.path.basename(newest)} → 최신 발동중={best}")
    return best


def rest_price_check(acct, code):
    """FHKST01010100 현재가 — 장운영/VI 관련 필드 best-effort 로깅."""
    try:
        ka.auth(svr="prod")
        base = ka.getTREnv().my_url if hasattr(ka.getTREnv(), "my_url") else "https://openapi.koreainvestment.com:9443"
        headers = ka._getBaseHeader()
        headers.update({"appkey": acct["appkey"], "appsecret": acct["appsecret"],
                        "tr_id": "FHKST01010100", "custtype": "P"})
        r = requests.get(f"{base}/uapi/domestic-stock/v1/quotations/inquire-price",
                         headers=headers, params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
                         timeout=10)
        o = (r.json() or {}).get("output", {})
        fields = {k: o.get(k) for k in ("stck_prpr", "temp_stop_yn", "iscd_stat_cls_code",
                                        "mrkt_warn_cls_code", "vi_cls_code", "antc_cnpr")}
        log(f"[REST현재가] {code} → {fields}")
    except Exception as e:
        log(f"[REST현재가] 실패(무시): {type(e).__name__}: {e}")


def parse_data_frame(raw):
    """KIS WSS 데이터 프레임: <encrypt>|<tr_id>|<cnt>|<data^...> → (tr_id, [레코드별 {col:val}])"""
    parts = raw.split("|")
    if len(parts) < 4:
        return None, []
    tr_id = parts[1]
    data = parts[3]
    cols = _cols.get(tr_id, [])
    fields = data.split("^")
    recs = []
    n = len(cols) if cols else len(fields)
    if n <= 0:
        return tr_id, []
    for i in range(0, len(fields), n):
        chunk = fields[i:i + n]
        if cols and len(chunk) == len(cols):
            recs.append(dict(zip(cols, chunk)))
        else:
            recs.append({"_raw_fields": chunk})
    return tr_id, recs


async def subscribe(ws, tr_id, code):
    func, label = SUBS[tr_id]
    msg, cols = func("1", code) if tr_id != "H0STCNT0" else func("1", code, "real")
    _cols[tr_id] = cols
    await ws.send(json.dumps(msg))
    log(f"[구독요청] {tr_id}({label}) tr_key={code} (cols={len(cols)})")


async def run(account, code, minutes):
    acct = pick_account(account)
    log(f"=== VI WSS 사이클 테스트 시작 | account={account}(cano={acct.get('cano')}) code={code} {minutes}분 ===")

    # REST 현재가 장운영/VI 필드 확인
    rest_price_check(acct, code)

    # approval_key — 선택 계좌로 (신규 중앙 per-appkey 캐시 재사용)
    ka._cfg["my_app"] = acct["appkey"]
    ka._cfg["my_sec"] = acct["appsecret"]
    ka.auth_ws(svr="prod")
    key = ka._base_headers_ws.get("approval_key")
    log(f"[auth_ws] approval_key 앞12={str(key)[:12]}... (계좌별 캐시)")

    vi_released = False
    t0 = time.time()
    try:
        async with websockets.connect(URL_WS, ping_interval=20, ping_timeout=10) as ws:
            log("[WSS] 연결 성공")
            # 1) 장운영정보 + 예상체결 동시 구독
            await subscribe(ws, "H0STMKO0", code)
            await subscribe(ws, "H0STANC0", code)
            while time.time() - t0 < minutes * 60:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if raw.startswith("{"):
                    # 시스템 메시지(구독응답/PINGPONG)
                    if "PINGPONG" in raw:
                        await ws.send(raw); continue
                    log(f"[SYS] {raw[:200]}")
                    continue
                tr_id, recs = parse_data_frame(raw)
                for rec in recs:
                    log(f"[수신:{tr_id}] {rec}")
                    if tr_id == "H0STMKO0":
                        vi = str(rec.get("vi_cls_code", "")).strip()
                        mkop = str(rec.get("mkop_cls_code", "")).strip()
                        trht = str(rec.get("trht_yn", "")).strip()
                        log(f"   ↳ [H0STMKO0 해석] vi_cls_code={vi!r} mkop_cls_code={mkop!r} trht_yn={trht!r} "
                            f"(※ 발동시각 전용 필드 없음 — 위 값으로 발동/해제 판정)")
                        # VI 해제 추정: vi_cls_code 가 비거나 '0' → 해제 → 실시간체결 전환
                        if not vi_released and (vi in ("", "0")):
                            vi_released = True
                            log("   ★ VI 해제 감지(vi_cls_code 소거) → H0STCNT0 실시간체결 구독 전환")
                            await subscribe(ws, "H0STCNT0", code)
                    elif tr_id == "H0STANC0":
                        log(f"   ↳ [예상체결=동시호가 결정가 후보] stck_prpr/antc 관련 필드 위 rec 확인")
            log(f"=== 종료 ({time.time()-t0:.0f}s 경과) ===")
    except Exception as e:
        log(f"[ERR] {type(e).__name__}: {e}")


def main():
    global _logf
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", default="syw_2")
    ap.add_argument("--code", default=None)
    ap.add_argument("--minutes", type=int, default=10)
    args = ap.parse_args()

    code = args.code
    if not code:
        row = latest_active_vi_code()
        if not row:
            raise SystemExit("vi_status 에 발동중 종목 없음 → --code 로 직접 지정하세요.")
        code = str(row["종목코드"]).strip()

    stamp = datetime.now().strftime("%y%m%d_%H%M")
    _logf = open(os.path.join(LOG_DIR, f"test_vi_wss_{stamp}.log"), "w", encoding="utf-8")
    try:
        asyncio.run(run(args.account, code, args.minutes))
    finally:
        if _logf:
            log(f"[로그파일] {_logf.name}")
            _logf.close()


if __name__ == "__main__":
    main()
