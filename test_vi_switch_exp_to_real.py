#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[테스트] 09:00 VI 종목 — 예상체결 유지 → 장운영정보 VI 해제 시 실시간체결 전환 검증
====================================================================================

배경 / 목적
-----------
프로덕션은 현재 09:00 갭 10%+ (예상체결 연장) 종목을 **09:01:59 까지 예상체결 수신 후
시각 기준으로** 실시간체결로 전환한다. 이를 **장운영정보(H0STMKO0)의 vi_cls_code 기준**
으로 바꿔 "VI 가 실제 해제되는 순간" 전환하는 게 더 정확한지 검증하는 테스트다.

★ 프로덕션을 건드리지 않는다. a2 계정으로 독립 검증한다.

동작
----
1. (cron 08:55 기동) 09:00:05 까지 대기
2. 상승률 상위(fluctuation) 조회 → 전일대비 +15% 이상 중 **한 종목** 선정(최상위)
3. a2 WSS 로 **예상체결(H0STANC0) + 장운영정보(H0STMKO0)** 동시 구독
4. H0STMKO0 의 vi_cls_code 로 VI 발동/해제 추적 (발동='Y'/비'N','0',''→발동)
5. **VI 해제(발동→해제 전이) 순간** 예상체결 해제 + **실시간체결(H0STCNT0) 구독 전환**
6. 전환 후 실시간체결 수신 확인. 모든 H0STMKO0 프레임은 CSV 저장(사후 검증).

안전
----
- a2 appkey 전용 WSS (프로덕션 a1 과 독립). ★ 호가 레코더(ws_orderbook_recorder.py)도
  a2 WSS 를 쓰므로, 이 테스트 중에는 레코더를 동시에 돌리지 말 것(같은 a2 appkey 두 WSS =
  ALREADY IN USE).
- 휴장일이면 즉시 종료(rules 의무).

사용법
------
  python3 test_vi_switch_exp_to_real.py                 # 09:00:05 대기 후 자동
  python3 test_vi_switch_exp_to_real.py --code 005930   # 종목 직접 지정(대기 없이 즉시)
  python3 test_vi_switch_exp_to_real.py --until 09:15    # 종료 시각(기본 09:15)
  python3 test_vi_switch_exp_to_real.py --min-ctrt 15    # 선정 임계 전일대비%(기본 15)
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import threading
import time
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.extend([str(SCRIPT_DIR)])
KST = ZoneInfo("Asia/Seoul")

import kis_auth_llm as ka                       # noqa: E402
sys.modules["kis_auth"] = ka                    # domestic_stock_functions_ws 가 import kis_auth → data_fetch
from kis_utils import is_holiday, load_config, load_symbol_master  # noqa: E402
from domestic_stock_functions_ws import (        # noqa: E402
    exp_ccnl_krx,     # H0STANC0 예상체결
    ccnl_krx,         # H0STCNT0 실시간체결
    market_status_krx,  # H0STMKO0 장운영정보
)
from fetch_top30_each_1m import _init_client_syw2, _fetch_fluctuation_top  # noqa: E402

CONFIG_PATH = SCRIPT_DIR / "config.json"
OUT_DIR = SCRIPT_DIR / "data" / "vi_switch_test"
OUT_DIR.mkdir(parents=True, exist_ok=True)

RECV_TS_FMT = "%Y-%m-%d %H:%M:%S.%f"


def _log(msg: str) -> None:
    print(f"[{datetime.now(KST).strftime('%H:%M:%S.%f')[:-3]}] {msg}", flush=True)


# ── a2 계정 WSS 인증 (프로덕션 a1 과 분리) ──────────────────────────────────
def _setup_a2_auth() -> None:
    cfg = load_config(str(CONFIG_PATH))
    a2 = (cfg.get("accounts") or {}).get("a2")
    if not a2 or not a2.get("appkey") or not a2.get("appsecret"):
        raise RuntimeError("config.json accounts.a2 의 appkey/appsecret 을 찾지 못했습니다.")
    ka._cfg["my_app"] = a2["appkey"]
    ka._cfg["my_sec"] = a2["appsecret"]
    tok = Path(ka.config_root) / f"KIS_a2_vitest_{datetime.now(KST).strftime('%Y%m%d')}"
    if not tok.exists():
        tok.touch()
    ka.token_tmp = str(tok)
    _log(f"a2 인증 시작 (appkey={a2['appkey'][:8]}…{a2['appkey'][-4:]})")
    ka.auth(svr="prod")
    ka.auth_ws(svr="prod")
    if not getattr(ka, "_base_headers_ws", {}).get("approval_key"):
        raise RuntimeError("auth_ws() 실패: a2 appkey/secret 확인")
    _log("a2 approval_key 확보 (프로덕션 a1 과 독립)")


def _code_name_map() -> dict[str, str]:
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        return dict(zip(sdf["code"], sdf["name"].astype(str)))
    except Exception:
        return {}


# ── 09:00 상승률 상위에서 +15%↑ 한 종목 선정 ───────────────────────────────
def _select_one(min_ctrt: float) -> tuple[str, str, float]:
    client = _init_client_syw2()
    rows = []
    for attempt in range(6):                      # 09:00 직후 빈 응답 대비 재시도
        try:
            rows = _fetch_fluctuation_top(client, top_n=50)
        except Exception as e:
            _log(f"상승률 조회 실패({attempt+1}/6): {e}")
            rows = []
        if rows:
            break
        time.sleep(2.0)
    if not rows:
        raise RuntimeError("상승률 상위 조회 결과 없음")

    name_map = _code_name_map()

    def _ctrt(r):
        try:
            return float(str(r.get("prdy_ctrt") or 0).replace(",", "") or 0)
        except (TypeError, ValueError):
            return 0.0

    def _code(r):
        return str(r.get("stck_shrn_iscd") or r.get("mksc_shrn_iscd") or r.get("code") or "").strip().zfill(6)

    ranked = [(r, _ctrt(r), _code(r)) for r in rows]
    ranked = [(r, c, code) for (r, c, code) in ranked if code and code != "000000"]
    cand = [(r, c, code) for (r, c, code) in ranked if c >= min_ctrt]
    if cand:
        r, c, code = max(cand, key=lambda x: x[1])   # +15%↑ 중 최상위
        _log(f"선정: {name_map.get(code, code)}({code}) 전일대비 {c:+.2f}% "
             f"(+{min_ctrt}% 이상 {len(cand)}종목 중 최상위)")
    else:
        r, c, code = max(ranked, key=lambda x: x[1])  # 없으면 최상위로 폴백
        _log(f"⚠ +{min_ctrt}% 이상 없음 → 최상위 폴백: {name_map.get(code, code)}({code}) {c:+.2f}%")
    return code, name_map.get(code, code), c


# ── 상태 ─────────────────────────────────────────────────────────────────────
_code = ""
_name = ""
_kws = None
_vi_ever_active = False        # VI 발동을 한 번이라도 봤는지
_vi_active = False             # 현재 VI 발동 상태
_switched = False             # 실시간체결 전환 완료
_switch_event = threading.Event()
_stop = threading.Event()
_lock = threading.Lock()

# 카운터
_cnt = {"exp": 0, "real": 0, "mko": 0}
_last_tick_log = 0.0

# H0STMKO0 프레임 CSV 저장
_mko_csv = None
_mko_writer = None
_mko_fields: list[str] | None = None


def _save_mko(recv_ts: str, rec: dict) -> None:
    global _mko_csv, _mko_writer, _mko_fields
    if _mko_writer is None:
        path = OUT_DIR / f"{datetime.now(KST).strftime('%y%m%d')}_mko_{_code}.csv"
        _mko_fields = ["recv_ts"] + list(rec.keys())
        _mko_csv = open(path, "a", newline="", encoding="utf-8-sig")
        _mko_writer = csv.DictWriter(_mko_csv, fieldnames=_mko_fields, extrasaction="ignore")
        if path.stat().st_size == 0:
            _mko_writer.writeheader()
        _log(f"H0STMKO0 저장: {path}")
    row = dict(rec); row["recv_ts"] = recv_ts
    _mko_writer.writerow(row)
    _mko_csv.flush()


def _vi_is_active(vi_cls: str) -> bool:
    # [260602 프로덕션 실측] vi_cls_code 발동='Y' / 해제='N'. 'N'/'0'/'' 외 = 발동.
    return str(vi_cls).strip() not in ("N", "0", "")


# ── 수신 콜백 ────────────────────────────────────────────────────────────────
def on_result(ws, tr_id, result, data_info):
    global _vi_active, _vi_ever_active, _last_tick_log
    if result is None or len(result) == 0:
        return
    recv_ts = datetime.now(KST).strftime(RECV_TS_FMT)
    result = result.rename({c: c.strip().lower() for c in result.columns})
    records = result.to_dicts()
    trid = str(tr_id)

    if trid == "H0STMKO0":                         # 장운영정보
        for rec in records:
            _cnt["mko"] += 1
            _save_mko(recv_ts, rec)
            vi_cls = str(rec.get("vi_cls_code") or "").strip()
            mkop = str(rec.get("mkop_cls_code") or "").strip()
            antc = str(rec.get("antc_mkop_cls_code") or "").strip()
            trht = str(rec.get("trht_yn") or "").strip()
            active = _vi_is_active(vi_cls)
            with _lock:
                prev = _vi_active
                _vi_active = active
                if active and not prev:
                    _vi_ever_active = True
                    _log(f"🟡 [VI발동] vi_cls={vi_cls} mkop={mkop} antc={antc} trht={trht}")
                elif prev and not active:
                    _log(f"🟢 [VI해제] vi_cls={vi_cls} mkop={mkop} antc={antc} → 실시간체결 전환 트리거")
                    _switch_event.set()             # 워커가 구독 전환 수행
                else:
                    _log(f"[장운영] vi_cls={vi_cls} mkop={mkop} antc={antc} trht={trht}")
        return

    if trid == "H0STANC0":                          # 예상체결
        _cnt["exp"] += 1
    elif trid == "H0STCNT0":                         # 실시간체결
        _cnt["real"] += 1

    now = time.time()
    if now - _last_tick_log >= 2.0:
        _last_tick_log = now
        r0 = records[0]
        px = r0.get("stck_prpr") or r0.get("antc_cnpr") or r0.get("stck_cntg_hour")
        _log(f"수신 exp={_cnt['exp']} real={_cnt['real']} mko={_cnt['mko']} "
             f"| {trid} pr={px} (switched={_switched})")


# ── 구독 전환 워커 (on_result 스레드 밖에서 send_request 호출) ────────────────
def _switch_worker():
    """VI 해제 시: 예상체결 해제 → 실시간체결 구독. (send_request 는 콜백 루프 밖 스레드에서)"""
    global _switched
    while not _stop.is_set():
        if _switch_event.wait(timeout=1.0):
            if _switched:
                return
            try:
                _log("⏩ 전환 실행: H0STANC0(예상) 해제 → H0STCNT0(실시간) 구독")
                _kws.send_request(exp_ccnl_krx, "2", _code)   # 예상체결 해제
                _kws.send_request(ccnl_krx, "1", _code)       # 실시간체결 구독
                _switched = True
                _log("✅ 실시간체결 전환 완료")
            except Exception as e:
                _log(f"⚠ 전환 실패: {type(e).__name__}: {e} (3초 후 재시도)")
                _switch_event.clear(); time.sleep(3.0); _switch_event.set()
            return


# ── 종료 타이머 ──────────────────────────────────────────────────────────────
def _arm_until(until_hhmm: str):
    try:
        hh, mm = (int(x) for x in until_hhmm.split(":"))
    except Exception:
        _log(f"--until 형식 오류('{until_hhmm}') → 타이머 미설정"); return
    target = dtime(hh, mm)

    def _watch():
        while not _stop.is_set():
            if datetime.now(KST).time() >= target:
                _log(f"종료 시각 {until_hhmm} 도달 → 종료 "
                     f"(exp={_cnt['exp']} real={_cnt['real']} mko={_cnt['mko']} "
                     f"vi_ever={_vi_ever_active} switched={_switched})")
                _stop.set()
                os._exit(0)
            time.sleep(2.0)
    threading.Thread(target=_watch, name="until", daemon=True).start()


def _wait_until_open(at_hhmmss: str):
    """지정 시각(KST)까지 대기. 이미 지났으면 즉시 통과."""
    try:
        hh, mm, ss = (int(x) for x in at_hhmmss.split(":"))
    except Exception:
        hh, mm, ss = 9, 0, 5
    target = dtime(hh, mm, ss)
    while datetime.now(KST).time() < target and not _stop.is_set():
        time.sleep(0.5)


# ── main ─────────────────────────────────────────────────────────────────────
def main():
    global _code, _name, _kws
    ap = argparse.ArgumentParser(description="VI 예상→실시간 체결 전환 테스트 (a2)")
    ap.add_argument("--code", default=None, help="종목 직접 지정(6자리). 미지정 시 09:00 상승률 상위에서 선정")
    ap.add_argument("--at", default="09:00:05", help="종목 선정 시각 HH:MM:SS (기본 09:00:05)")
    ap.add_argument("--until", default="09:15", help="종료 시각 HH:MM (기본 09:15)")
    ap.add_argument("--min-ctrt", type=float, default=15.0, help="선정 전일대비 임계% (기본 15)")
    args = ap.parse_args()

    if is_holiday():
        _log("휴장일 → 즉시 종료"); return

    _setup_a2_auth()

    if args.code:
        nm = _code_name_map().get(str(args.code).zfill(6), str(args.code).zfill(6))
        _code, _name = str(args.code).zfill(6), nm
        _log(f"종목 직접 지정: {_name}({_code}) — 대기 없이 즉시 구독")
    else:
        _log(f"{args.at} 까지 대기 후 상승률 상위 조회 예정")
        _wait_until_open(args.at)
        _code, _name, ctrt = _select_one(args.min_ctrt)

    _arm_until(args.until)
    threading.Thread(target=_switch_worker, name="switch", daemon=True).start()

    # 예상체결(H0STANC0) + 장운영정보(H0STMKO0) 동시 구독
    _kws = ka.KISWebSocket(api_url="/tryitout", max_retries=50)
    _kws.subscribe(request=exp_ccnl_krx, data=[_code])
    _kws.subscribe(request=market_status_krx, data=[_code])
    _log(f"구독 시작: {_name}({_code}) ← 예상체결(H0STANC0) + 장운영정보(H0STMKO0)")
    _log("→ VI 해제 시 실시간체결(H0STCNT0) 자동 전환 대기")
    try:
        _kws.start(on_result=on_result)
    except KeyboardInterrupt:
        _log("Ctrl+C 중단")
    finally:
        _stop.set()
        if _mko_csv:
            try:
                _mko_csv.flush(); _mko_csv.close()
            except Exception:
                pass
        _log(f"종료 — exp={_cnt['exp']} real={_cnt['real']} mko={_cnt['mko']} "
             f"vi_ever_active={_vi_ever_active} switched={_switched}")


if __name__ == "__main__":
    main()
