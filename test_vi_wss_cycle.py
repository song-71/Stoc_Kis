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

[approval_key / 동시실행 (260602 현행 기준)]
  - approval_key 는 appkey(계좌)별 "공용 재사용" 이다(rules: 계좌별 1회 발급 + 24h 공용). 이 테스트는
    --account(기본 syw_2=a2) 의 appkey 로 ka.auth_ws() 중앙 함수를 호출 → 캐시된 최신 키 재사용,
    없으면 신규 발급(직접 /oauth2/Approval 호출 금지 — 캐시 우회 시 타 소비자 키 무효화).
  - 현재 WSS 소비자 topology:
      * 메인 ws_realtime_trading.py → a1(main) appkey 로만 WSS 운영.
      * Daily_inquire_vi_status.py → a2 WSS 는 260601 비활성(`_init_ws_a2()` 주석처리, _kws=None).
        해당 파일은 REST VI현황 CSV 저장만 수행하며 그 REST 는 a1 토큰을 사용 → a2 WSS 슬롯은 비어 있음.
  - 따라서 기본값 a2 로 실행하면 a2 WSS 를 점유하는 라이브 소비자가 없어 충돌 없이 단독 실행된다.
    (REST 폴링은 WSS 와 독립이라 회피 불필요.)
  - 단 --account main(a1) 으로는 절대 실행 금지: 메인이 a1 WSS 를 점유 중이라 같은 키로 2번째 소켓을
    열면 KIS 'ALREADY IN USE'(OPSP8996) → a1 키 종일(~10h) 잠김 → 메인 WSS 사망.
  - 일반 원칙: 동일 appkey 로 동시에 2개의 WSS 소켓을 열지 말 것(ALREADY IN USE 원인).
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
import sys
import time
from datetime import datetime, time as dtime, timedelta, timezone

import requests
import websockets

import kis_auth_llm as ka
# domestic_stock_functions_ws 는 `import kis_auth as ka` 로 data_fetch 를 호출한다.
# 메인(ws_realtime_trading.py:676)·Daily_inquire_vi_status.py:44 와 동일하게, 해당 import "전에"
# kis_auth 를 kis_auth_llm 으로 별칭 등록해야 한다. (진짜 kis_auth.py 에는 data_fetch 가 없음.)
sys.modules["kis_auth"] = ka
from domestic_stock_functions_ws import ccnl_krx, exp_ccnl_krx, market_status_krx  # noqa: E402,F401

KST = timezone(timedelta(hours=9))
MARKET_OPEN = dtime(9, 0)      # 정규장 개시 09:00
MARKET_CLOSE = dtime(15, 20)   # 정규장 종료 15:20

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VI_LIVE_DIR = os.path.join(BASE_DIR, "data", "vi_status", "1m_fetch")   # 당일 실시간(분당) CSV
VI_BACKUP_DIR = os.path.join(VI_LIVE_DIR, "backup")                     # 15:20 병합 후 이동분(과거)
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
    return datetime.now(KST).strftime("%H:%M:%S.%f")[:-3]


def log(msg):
    line = f"[{_ts()}] {msg}"
    print(line, flush=True)
    if _logf:
        _logf.write(line + "\n"); _logf.flush()


def pick_account(alias):
    # load_config()(kis_utils)는 가공된 dict를 반환해 raw "users" 구조가 없으므로
    # config.json 원본을 직접 읽어 V2 멀티계좌(users→default_user→accounts) 구조를 파싱한다.
    with open(os.path.join(BASE_DIR, "config.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    accts = cfg["users"][cfg["default_user"]]["accounts"]
    if alias not in accts:
        raise SystemExit(f"계좌 '{alias}' 없음. 사용가능: {list(accts)}")
    return accts[alias]


def _is_market_hours(now: datetime | None = None) -> bool:
    """정규장(09:00~15:20 KST) 여부."""
    now = now or datetime.now(KST)
    return MARKET_OPEN <= now.time() < MARKET_CLOSE


def _latest_vi_csv() -> str | None:
    """가장 최신 vi_status CSV 경로.
    당일 실시간 분당 파일('vi_status_YYMMDD_HHMM.csv') 우선 — merged/과거 파일은 제외.
    (live 디렉토리의 'vi_status_merged_*.csv' 가 사전순으로 분당파일보다 뒤라 잘못 잡히는 것 방지.)
    당일 파일이 없으면(장외/15:20 병합 후) backup 의 최근 분당 파일로 폴백."""
    today = datetime.now(KST).strftime("%y%m%d")
    files = sorted(glob.glob(os.path.join(VI_LIVE_DIR, f"vi_status_{today}_*.csv")))
    if not files:
        files = sorted(
            f for f in glob.glob(os.path.join(VI_BACKUP_DIR, "vi_status_*.csv"))
            if "merged" not in os.path.basename(f)
        )
    return files[-1] if files else None


def latest_active_vi_code():
    """최신 vi_status CSV 에서 '발동중' 종목 중 발동시간이 가장 늦은(=마지막) 종목."""
    newest = _latest_vi_csv()
    if not newest:
        return None
    best = None
    with open(newest, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("상태", "").strip() == "발동중":
                t = row.get("발동시간", "0")
                if best is None or t > best.get("발동시간", "0"):
                    best = row
    log(f"[vi_status] 파일={os.path.basename(newest)} → 발동중 마지막 종목={best}")
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
    """KIS WSS 데이터 프레임: <encrypt>|<tr_id>|<cnt>|<data^...> → (tr_id, [레코드별 {col:val}]).
    메인(kis_auth_llm)과 동일하게 컬럼 수 미달 프레임은 빈값으로 패딩한다.
    (KIS 가 마지막 컬럼을 생략해 보내는 경우 — 예: H0STMKO0 는 EXCH_CLS_CODE 미전송 →
     정의 11 / 실수신 10 — 를 흡수해 이름 기반 파싱이 깨지지 않도록.)"""
    parts = raw.split("|")
    if len(parts) < 4:
        return None, []
    tr_id = parts[1]
    data = parts[3]
    cols = _cols.get(tr_id, [])
    fields = data.split("^")
    ncols = len(cols)
    if ncols <= 0:
        return tr_id, [{"_raw_fields": fields}]
    recs = []
    if len(fields) > ncols:
        # 여러 건: 컬럼 수 단위로 분할, 마지막 미달분은 빈값 패딩
        for i in range(0, len(fields), ncols):
            chunk = fields[i:i + ncols]
            if len(chunk) < ncols:
                chunk = chunk + [""] * (ncols - len(chunk))
            recs.append(dict(zip(cols, chunk)))
    else:
        # 단일 건: 부족하면 빈값 패딩
        chunk = fields + [""] * (ncols - len(fields))
        recs.append(dict(zip(cols, chunk)))
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
    released_at = None                       # VI 해제 감지 시각 → 1분 후 연결 종료 기준
    POST_RELEASE_HOLD = 60.0                 # 해제 후 실시간체결 관찰 유지 시간(초)
    t0 = time.time()
    try:
        async with websockets.connect(URL_WS, ping_interval=20, ping_timeout=10) as ws:
            log("[WSS] 연결 성공")
            # 1) 장운영정보 + 예상체결 동시 구독
            await subscribe(ws, "H0STMKO0", code)
            await subscribe(ws, "H0STANC0", code)
            while time.time() - t0 < minutes * 60:
                # VI 해제 후 1분 경과 → 연결 종료 (해제→실시간체결 전환만 관찰하고 마무리)
                if vi_released and released_at and (time.time() - released_at >= POST_RELEASE_HOLD):
                    log(f"=== VI 해제 후 {POST_RELEASE_HOLD:.0f}초 경과 → WSS 연결 종료 ===")
                    break
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
                        # VI 해제 판정: 실측상 해제 시 vi_cls_code='N' (빈값/0 아님 — 260602 라이브 확인).
                        #   방어적으로 'N'/'0'/'' 모두 해제로 본다('Y'=발동중).
                        if not vi_released and vi in ("N", "0", ""):
                            vi_released = True
                            released_at = time.time()
                            log(f"   ★ VI 해제 감지(vi_cls_code={vi!r}) → H0STCNT0 실시간체결 구독 전환 "
                                f"({POST_RELEASE_HOLD:.0f}초 후 종료 예정)")
                            await subscribe(ws, "H0STCNT0", code)
                    elif tr_id == "H0STANC0":
                        log(f"   ↳ [예상체결=동시호가 결정가 후보] stck_prpr/antc 관련 필드 위 rec 확인")
            log(f"=== 종료 ({time.time()-t0:.0f}s 경과, vi_released={vi_released}) ===")
    except Exception as e:
        log(f"[ERR] {type(e).__name__}: {e}")


def main():
    global _logf
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", default="syw_2")
    ap.add_argument("--code", default=None)
    ap.add_argument("--minutes", type=int, default=10)
    args = ap.parse_args()

    now = datetime.now(KST)
    stamp = now.strftime("%y%m%d_%H%M")
    _logf = open(os.path.join(LOG_DIR, f"test_vi_wss_{stamp}.log"), "w", encoding="utf-8")

    code = args.code
    if code:
        log(f"[종목선택] --code 지정 → {code}")
    elif _is_market_hours(now):
        # 장 중(09:00~15:20): 즉시 최신 VI 리스트 확인 → 발동중 '마지막(최근 발동)' 종목만 테스트
        log(f"[종목선택] 장중({now.strftime('%H:%M:%S')} KST) → 최신 VI 발동중 마지막 종목 자동선택")
        row = latest_active_vi_code()
        if not row:
            log("[종목선택] 현재 발동중 종목 없음 → 종료 (장중 잠시 후 재시도 또는 --code 지정)")
            _logf.close()
            raise SystemExit("현재 VI 발동중 종목 없음")
        code = str(row["종목코드"]).strip()
        log(f"[종목선택] → {code} {row.get('종목명','')} (발동시간={row.get('발동시간')})")
    else:
        # 장외: --code 미지정 → 최신(backup 포함) 발동중 종목 시도
        log(f"[종목선택] 장외({now.strftime('%H:%M:%S')} KST), --code 미지정 → 최신 발동중 종목 시도")
        row = latest_active_vi_code()
        if not row:
            _logf.close()
            raise SystemExit("발동중 종목 없음 → 장중에 실행하거나 --code 로 직접 지정하세요.")
        code = str(row["종목코드"]).strip()

    try:
        asyncio.run(run(args.account, code, args.minutes))
    finally:
        if _logf:
            log(f"[로그파일] {_logf.name}")
            _logf.close()


if __name__ == "__main__":
    main()
