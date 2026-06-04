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
  ./venv/bin/python test_vi_wss_cycle.py                      # a2, 시작시점 vi_status '발동중 전 종목' 스냅샷 모니터링, 10분
  ./venv/bin/python test_vi_wss_cycle.py --code 005930 --minutes 5   # 단일 종목 지정
  ※ 종목 집합은 시작 1회 스냅샷으로 고정 — 실행 중 새로 발동되는 VI 는 추가하지 않음(무한 누적 방지).
  ※ 종목별 독립 사이클: 발동→예상체결(H0STANC0) / 해제(vi_cls='N')→실시간체결(H0STCNT0) /
     재발동(vi_cls='Y')→예상체결 재전환. 해제 후 60초 관찰하면 종목별 완료, 전 종목 완료 시 종료.
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


def active_vi_rows() -> list[dict]:
    """최신 vi_status CSV 의 '발동중' 종목 전체 (발동시간 오름차순)."""
    newest = _latest_vi_csv()
    if not newest:
        return []
    rows = [r for r in csv.DictReader(open(newest, encoding="utf-8-sig"))
            if r.get("상태", "").strip() == "발동중"]
    rows.sort(key=lambda r: r.get("발동시간", ""))
    log(f"[vi_status] 파일={os.path.basename(newest)} → 발동중 {len(rows)}종목")
    return rows


def rest_price_check(acct, code) -> str:
    """① 현재가조회(FHKST01010100) → vi_cls_code 로 VI 감지 가능한지 확인.
    실전 감지흐름의 1순위: 틱 끊김 시 현재가조회의 장운영(vi_cls_code)로 VI 판별.
    반환: vi_cls_code (감지 실패/오류 시 '')."""
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
        vi = str(o.get("vi_cls_code") or "").strip()
        verdict = "VI 발동 감지 ✓" if vi not in ("", "N", "0") else f"VI 미감지(vi_cls_code={vi!r})"
        log(f"   ①[{code}] 현재가조회 vi_cls_code={vi!r} stck_prpr={o.get('stck_prpr')} "
            f"antc_cnpr={o.get('antc_cnpr')} temp_stop_yn={o.get('temp_stop_yn')} → {verdict}")
        return vi
    except Exception as e:
        log(f"   ①[{code}] 현재가조회 실패(무시): {type(e).__name__}: {e}")
        return ""


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
    # ── [#1 진단] 멀티레코드인데 컬럼 수의 배수가 아님 → 정렬 어긋남(또는 KIS 불규칙/손상 프레임) ──
    #   260602 10:38:15 VI 버스트 때 예상(ANC) 파싱이 쓰레기값을 뱉은 현상의 원인 확정용. raw 덤프.
    #   ※ 단일 레코드가 컬럼보다 짧은 경우(예: H0STMKO0 10/11, EXCH_CLS_CODE 미전송)는 KIS 의
    #     정상적 마지막컬럼 생략 → 패딩으로 흡수(정상, 노이즈 제외). 진짜 이상은 '멀티레코드 + 배수 아님'.
    if len(fields) > ncols and len(fields) % ncols != 0:
        cnt = parts[2] if len(parts) > 2 else "?"
        rr = raw if len(raw) <= 1000 else (raw[:600] + " …<중략>… " + raw[-300:])
        log(f"[!파싱이상] tr_id={tr_id} 선언cnt={cnt} ncols={ncols} 실필드={len(fields)} "
            f"나머지={len(fields) % ncols} → raw_len={len(raw)} raw={rr!r}")
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


async def _send_sub(ws, tr_id, code, tr_type):
    """tr_type: '1'=구독, '2'=해제."""
    func, label = SUBS[tr_id]
    msg, cols = func(tr_type, code) if tr_id != "H0STCNT0" else func(tr_type, code, "real")
    _cols[tr_id] = cols
    await ws.send(json.dumps(msg))
    act = "구독" if tr_type == "1" else "해제"
    log(f"[{act}] {tr_id}({label}) tr_key={code}" + (f" (cols={len(cols)})" if tr_type == "1" else ""))


POST_RELEASE_HOLD = 60.0   # 해제 후 실시간체결 관찰 유지 시간(초) → 종목별 완료 기준


def _rec_code(rec: dict) -> str:
    """레코드에서 종목코드 추출 (ANC/MKO=소문자, CNT=대문자)."""
    return str(rec.get("mksc_shrn_iscd") or rec.get("MKSC_SHRN_ISCD") or "").strip()


async def run(account, codes, vi_info, minutes):
    """발동중 codes 전체를 동시 모니터링. 실전 감지흐름을 그대로 절차화:
       종목별 ① 현재가조회(vi_cls_code) 로 VI 감지 → ② CSV 발동시각 확인 → ③ 장운영정보 구독.
       이후 종목별 독립 상태머신: 발동→예상체결(H0STANC0), 해제(vi_cls='N')→실시간체결(H0STCNT0),
       재발동(vi_cls='Y')→예상체결 재전환. 해제 후 POST_RELEASE_HOLD 초 관찰 뒤 종목별 완료.
       전 종목 완료 시 종료(또는 --minutes 상한)."""
    acct = pick_account(account)
    log(f"=== VI WSS 멀티 사이클 테스트 | account={account}(cano={acct.get('cano')}) "
        f"{len(codes)}종목 {minutes}분 | 종목={codes} ===")

    # ── 실전 VI 감지 절차 검증: ① 현재가조회(vi_cls_code) → ② CSV 발동시각 ──
    log("── [VI 감지 절차] 종목별 ① 현재가조회 vi_cls_code → ② CSV 발동시각 → ③ 장운영정보 구독 ──")
    rest_detect = {}
    for c in codes:
        rest_detect[c] = rest_price_check(acct, c)                       # ① REST 감지
        info = vi_info.get(c, {})                                        # ② CSV
        log(f"   ②[{c}] CSV: 종류={info.get('종류','?')} 발동시각={info.get('발동시간','?')} "
            f"발동가={info.get('발동가','?')} 기준가={info.get('기준가','?')} 괴리율={info.get('괴리율','?')}")
    n_ok = sum(1 for v in rest_detect.values() if v not in ("", "N", "0"))
    log(f"── [현재가조회 VI 감지 결과] {n_ok}/{len(codes)} 종목 감지 "
        f"(vi_cls_code: {', '.join(f'{c}={v!r}' for c, v in rest_detect.items())}) ──")

    # approval_key — 선택 계좌로 (중앙 per-appkey 캐시 재사용/없으면 발급)
    ka._cfg["my_app"] = acct["appkey"]
    ka._cfg["my_sec"] = acct["appsecret"]
    ka.auth_ws(svr="prod")
    key = ka._base_headers_ws.get("approval_key")
    log(f"[auth_ws] approval_key 앞12={str(key)[:12]}... (계좌별 캐시)")

    # 종목별 상태: mode 'exp'(예상)|'real'(실시간), released_at, done, await_first_real(#2용)
    st = {c: {"mode": "exp", "released_at": None, "done": False, "await_first_real": False}
          for c in codes}
    t0 = time.time()
    try:
        async with websockets.connect(URL_WS, ping_interval=20, ping_timeout=10) as ws:
            log("[WSS] 연결 성공")
            log("   ③ 장운영정보(H0STMKO0) + 예상체결(H0STANC0) 구독 시작")
            for c in codes:
                await _send_sub(ws, "H0STMKO0", c, "1")
                await _send_sub(ws, "H0STANC0", c, "1")
            while time.time() - t0 < minutes * 60:
                now = time.time()
                # 종목별: 해제 후 POST_RELEASE_HOLD 경과 → 완료(구독 해제)
                for c, s in st.items():
                    if s["done"]:
                        continue
                    if s["mode"] == "real" and s["released_at"] and (now - s["released_at"] >= POST_RELEASE_HOLD):
                        for tr in ("H0STCNT0", "H0STMKO0"):
                            try:
                                await _send_sub(ws, tr, c, "2")
                            except Exception:
                                pass
                        s["done"] = True
                        log(f"   ✔ [{c}] 해제 후 {POST_RELEASE_HOLD:.0f}초 관찰 완료 → 구독 종료")
                if st and all(s["done"] for s in st.values()):
                    log("=== 전 종목 사이클 완료 → WSS 종료 ===")
                    break
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if raw.startswith("{"):
                    if "PINGPONG" in raw:
                        await ws.send(raw); continue
                    log(f"[SYS] {raw[:160]}")
                    continue
                tr_id, recs = parse_data_frame(raw)
                for rec in recs:
                    code = _rec_code(rec)
                    if code not in st:
                        # 식별불가(파싱 어긋남/타종목) → 무시하되 1회 가시화
                        log(f"[!무시] tr_id={tr_id} 식별불가 rec={str(rec)[:120]}")
                        continue
                    s = st[code]
                    if tr_id == "H0STMKO0":
                        vi = str(rec.get("vi_cls_code", "")).strip()
                        log(f"[{code}] MKO vi_cls={vi!r} mkop={rec.get('mkop_cls_code','')!r} "
                            f"antc_mkop={rec.get('antc_mkop_cls_code','')!r} trht={rec.get('trht_yn','')!r}")
                        if vi in ("N", "0", ""):            # VI 해제
                            if s["mode"] == "exp":
                                s["mode"] = "real"; s["released_at"] = now; s["await_first_real"] = True
                                await _send_sub(ws, "H0STANC0", code, "2")
                                await _send_sub(ws, "H0STCNT0", code, "1")
                                log(f"   ★[{code}] VI 해제(vi_cls={vi!r}) → 실시간체결 전환")
                        else:                               # 'Y' 등 = 발동/재발동
                            if s["mode"] == "real":
                                s["mode"] = "exp"; s["released_at"] = None; s["await_first_real"] = False
                                await _send_sub(ws, "H0STCNT0", code, "2")
                                await _send_sub(ws, "H0STANC0", code, "1")
                                log(f"   ◆[{code}] VI 재발동(vi_cls={vi!r}) → 예상체결 재전환")
                    elif tr_id == "H0STANC0":
                        log(f"[{code}] ANC 예상가={rec.get('stck_prpr')} 등락={rec.get('prdy_ctrt')}% "
                            f"cntg_vol={rec.get('cntg_vol')} acml_vol={rec.get('acml_vol')} hour_cls={rec.get('hour_cls_code')!r}")
                    elif tr_id == "H0STCNT0":
                        if s["await_first_real"]:           # #2: 해제 직후 첫 실시간 틱 = 단일가 일괄체결
                            s["await_first_real"] = False
                            vol = rec.get("CNTG_VOL"); prc = rec.get("STCK_PRPR"); acml = rec.get("ACML_VOL")
                            try:
                                amt = f"{int(vol) * int(prc):,}원"
                            except Exception:
                                amt = "?"
                            log(f"   ▶[{code}] 해제후 첫 실시간틱 = 동시호가 단일가 일괄체결: "
                                f"{vol}주 @ {prc} ({amt}) | 당일누적 ACML_VOL={acml} VI_STND_PRC={rec.get('VI_STND_PRC')}")
                        else:
                            log(f"[{code}] CNT 체결가={rec.get('STCK_PRPR')} CNTG_VOL={rec.get('CNTG_VOL')} "
                                f"ACML_VOL={rec.get('ACML_VOL')} HOUR_CLS={rec.get('HOUR_CLS_CODE')!r}")
            log(f"=== 종료 ({time.time()-t0:.0f}s 경과) 상태: "
                + ", ".join(f"{c}:{s['mode']}{'(완료)' if s['done'] else ''}" for c, s in st.items()) + " ===")
    except Exception as e:
        log(f"[ERR] {type(e).__name__}: {e}")


def main():
    global _logf
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", default="a2")
    ap.add_argument("--code", default=None)
    ap.add_argument("--minutes", type=int, default=10)
    args = ap.parse_args()

    now = datetime.now(KST)
    stamp = now.strftime("%y%m%d_%H%M")
    _logf = open(os.path.join(LOG_DIR, f"test_vi_wss_{stamp}.log"), "w", encoding="utf-8")

    # 종목 선택은 "시작 시점 스냅샷" 1회만. 실행 중 새로 발동되는 VI 는 추가하지 않는다(무한 누적 방지).
    vi_info: dict = {}   # code -> CSV row (발동시각/종류/발동가 등)
    if args.code:
        codes = [args.code.strip()]
        log(f"[종목선택] --code 지정 → {codes}")
    else:
        scope = "장중" if _is_market_hours(now) else "장외(backup 폴백)"
        log(f"[종목선택] {scope}({now.strftime('%H:%M:%S')} KST) → 발동중 전 종목 스냅샷 모니터링")
        rows = active_vi_rows()
        if not rows:
            log("[종목선택] 현재 발동중 종목 없음 → 종료 (잠시 후 재시도 또는 --code 지정)")
            _logf.close()
            raise SystemExit("현재 VI 발동중 종목 없음")
        codes = [str(r["종목코드"]).strip() for r in rows]
        vi_info = {str(r["종목코드"]).strip(): r for r in rows}
        log("[종목선택] 발동중: " + ", ".join(
            f"{r.get('종목코드')}({r.get('종목명','')},{r.get('종류','')},발동{r.get('발동시간')})" for r in rows))

    try:
        asyncio.run(run(args.account, codes, vi_info, minutes=args.minutes))
    finally:
        if _logf:
            log(f"[로그파일] {_logf.name}")
            _logf.close()


if __name__ == "__main__":
    main()
