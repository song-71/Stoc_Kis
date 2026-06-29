#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
실시간 호가(orderbook) 기록기 — 전일 상한가 종목 전체 H0STASP0 수신
================================================================

목적
----
전일 상한가 종목 전체에 대해 **순수 실시간 호가 스트림(H0STASP0)** 을 당일 내내 받아
수신 즉시 종목별 CSV 로 저장한다. (체결 데이터는 프로덕션 ws_realtime_trading.py 가
base_codes=전일 상한가로 이미 수신·저장하므로, 여기서는 호가만 보완 수집한다.)
나중에 프로덕션 wss_data(체결) 와 recv_ts 기준으로 겹쳐 그려(plot) 호가/체결 도착
시점 차이를 비교하거나, 호가 기반 전략 검증에 사용한다.

[260630] 단일 샘플종목 → 전일 상한가 ST 전체 다종목 수신으로 확장.

안전 (★ 반드시 a2 계정으로 접속)
--------------------------------
프로덕션 WSS 는 a1 appkey 로 approval_key 를 발급해 사용한다. 같은 appkey 로 새
approval_key 를 발급하면 직전 키가 무효화되어 프로덕션이 즉사한다(메모리: WSS
approval_key 무효화). 따라서 이 기록기는 **a2 appkey** 로만 접속한다. a2 는 거래
폴백용 REST 계정이며 WSS 는 쓰지 않으므로 a1 과 독립이다.

수신 데이터 (프로덕션 wss_data 와 컬럼명 통일)
---------------------------------------------
- recv_ts        : 수신 시각. datetime.now(KST) "%Y-%m-%d %H:%M:%S.%f"
                   (프로덕션 on_result 와 동일 클럭·동일 포맷 → 딜레이 직접 비교 가능)
- mksc_shrn_iscd : 종목코드 (프로덕션과 동일 컬럼명)
- name           : 종목명   (프로덕션과 동일 컬럼명)
- 이하 H0STASP0 원본 컬럼(소문자): askp1..askp10, bidp1..bidp10, 잔량 등
  ※ 사용자가 말한 "askp0/bidp0"(최우선 매도/매수호가)는 여기서 askp1/bidp1 이며,
    프로덕션 체결데이터의 askp1/bidp1 과 컬럼명이 그대로 일치한다.

저장 (parquet — CSV 대비 저장·읽기 빠름)
----------------------------------------
[260630] CSV → parquet 전환. parquet 는 행 단위 즉시 append 가 불가능한 컬럼형
포맷이므로, 프로덕션 wss_data 와 동일한 사상으로 **메모리 버퍼 + 주기적 원자 저장**
방식을 쓴다:
  - 수신 프레임을 종목별 메모리 버퍼에 누적
  - FLUSH_INTERVAL_SEC(기본 20초)마다 종목별 parquet 를 임시파일에 쓴 뒤
    os.replace() 로 원자적 교체 → 쓰기 도중 중단돼도 직전 완결본은 보존
  - 종료(--until/ Ctrl+C) 시 마지막 1회 flush
파일: data/wss_data/orderbook/{YYMMDD}_orderbook_{code}.parquet (종목별 1개)

사용법
------
  python3 orderbook_recorder.py                 # 전일 상한가 ST 종목 전체 자동 구독
  python3 orderbook_recorder.py --code 001210   # 종목 직접 지정(쉼표로 여러 개 가능)
  python3 orderbook_recorder.py --until 15:40    # 종료 시각 지정(기본 15:40, KST)
  python3 orderbook_recorder.py --flush 10       # flush 주기(초) 지정(기본 20)
  (Ctrl+C 로 언제든 중단 — 직전 flush 까지의 데이터는 손실 없음)
"""

import argparse
import os
import sys
import threading
import time
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.extend([str(SCRIPT_DIR)])

KST = ZoneInfo("Asia/Seoul")

# ── 프로젝트 모듈 ──────────────────────────────────────────────────────────
import kis_auth_llm as ka                       # noqa: E402
from kis_utils import load_config, load_symbol_master  # noqa: E402
from domestic_stock_functions_ws import asking_price_krx  # noqa: E402  H0STASP0

CONFIG_PATH = SCRIPT_DIR / "config.json"
TARGET_CSV = SCRIPT_DIR / "symulation" / "Select_Tr_target_list.csv"
KRX_CODE_CSV = SCRIPT_DIR / "data" / "admin" / "symbol_master" / "KRX_code.csv"
OUT_DIR = SCRIPT_DIR / "data" / "wss_data" / "orderbook"
OUT_DIR.mkdir(parents=True, exist_ok=True)

RECV_TS_FMT = "%Y-%m-%d %H:%M:%S.%f"   # 프로덕션 on_result 와 동일


def _log(msg: str) -> None:
    print(f"[{datetime.now(KST).strftime('%H:%M:%S')}] {msg}", flush=True)


# ============================================================================
# 1) a2 계정 자격으로 WSS 인증 (프로덕션 a1 과 분리)
# ============================================================================
def _setup_a2_auth() -> dict:
    cfg = load_config(str(CONFIG_PATH))
    # config.json: users.<htsid>.accounts.a2
    users = cfg.get("users") or {}
    a2 = None
    for _uid, u in users.items():
        accts = (u or {}).get("accounts") or {}
        if "a2" in accts:
            a2 = accts["a2"]
            break
    if not a2 or not a2.get("appkey") or not a2.get("appsecret"):
        raise RuntimeError("config.json 에서 a2 계정의 appkey/appsecret 을 찾지 못했습니다.")

    # ★ a1(루트)로 평탄화된 _cfg 를 a2 자격으로 덮어쓴다 → approval_key 가 a2 appkey 로 발급됨
    ka._cfg["my_app"] = a2["appkey"]
    ka._cfg["my_sec"] = a2["appsecret"]

    # 토큰 파일을 a2 호가기록기 전용으로 분리 (프로덕션 a1/a2 캐시와 충돌 방지)
    tok = Path(ka.config_root) / f"KIS_a2_obrec_{datetime.now(KST).strftime('%Y%m%d')}"
    if not tok.exists():
        tok.touch()
    ka.token_tmp = str(tok)

    _log(f"a2 계정으로 인증 시작 (appkey={a2['appkey'][:8]}…{a2['appkey'][-4:]}, token={tok.name})")
    ka.auth(svr="prod")          # REST env(my_url_ws 포함) 세팅 + access_token
    ka.auth_ws(svr="prod")       # a2 appkey 전용 approval_key (a1 과 독립)
    if not getattr(ka, "_base_headers_ws", {}).get("approval_key"):
        raise RuntimeError("auth_ws() 실패: a2 appkey/secret 확인 필요")
    _log("a2 approval_key 확보 — 프로덕션(a1) 과 독립된 WSS 슬롯")
    return cfg


# ============================================================================
# 2) 종목 선택 (전일 상한가 = Select_Tr_target_list.csv 최신일자 ST 종목)
# ============================================================================
def _st_code_set() -> set[str]:
    if not KRX_CODE_CSV.exists():
        return set()
    df = pd.read_csv(KRX_CODE_CSV, dtype=str, usecols=["code", "group"])
    df["code"] = df["code"].str.strip().str.zfill(6)
    df["group"] = df["group"].str.strip().str.upper()
    return set(df.loc[df["group"] == "ST", "code"])


def _select_codes(explicit: str | None) -> list[tuple[str, str]]:
    """반환: [(code6, name), ...].

    explicit 지정 시 해당 종목만(쉼표로 여러 개 가능),
    미지정 시 Select_Tr_target_list.csv 최신일자 ST(전일 상한가) **전체**.
    """
    name_map = _code_name_map()
    if explicit:
        out = []
        for raw in str(explicit).split(","):
            c = raw.strip().zfill(6)
            if c and c != "000000":
                out.append((c, name_map.get(c, c)))
        if not out:
            raise RuntimeError(f"--code 파싱 실패: {explicit!r}")
        _log(f"--code 직접 지정 {len(out)}종목: {[c for c, _ in out]}")
        return out

    if not TARGET_CSV.exists():
        raise RuntimeError(f"대상 CSV 없음: {TARGET_CSV} (--code 로 직접 지정하세요)")
    df = pd.read_csv(TARGET_CSV, dtype=str, encoding="utf-8-sig")
    if df.empty or "date" not in df.columns or "symbol" not in df.columns:
        raise RuntimeError("Select_Tr_target_list.csv 형식 이상 (--code 로 직접 지정하세요)")
    last_date = df["date"].max()
    day = df[df["date"] == last_date].copy()
    day["symbol"] = day["symbol"].str.strip().str.zfill(6)
    codes = day["symbol"].tolist()
    csv_names = dict(zip(day["symbol"], day.get("name", pd.Series(dtype=str)).astype(str))) if "name" in day.columns else {}

    st_set = _st_code_set()
    st_codes = [c for c in codes if c in st_set] if st_set else codes
    if not st_codes:
        raise RuntimeError(f"CSV({last_date}) 에서 ST 종목을 찾지 못했습니다.")

    out = [(c, name_map.get(c) or csv_names.get(c) or c) for c in st_codes]
    _log(f"전일 상한가({last_date}) ST {len(out)}종목 전체 구독: "
         f"{', '.join(f'{n}({c})' for c, n in out)}")
    return out


def _code_name_map() -> dict[str, str]:
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        return dict(zip(sdf["code"], sdf["name"].astype(str)))
    except Exception:
        return {}


# ============================================================================
# 3) 수신 → 메모리 버퍼 누적 → 주기적 parquet "조각" 저장 → 종료 시 병합
#    (production wss_data 와 동일 패턴: 조각마다 완결 parquet → 메모리·쓰기 일정)
# ============================================================================
# 컬럼 정렬: recv_ts, mksc_shrn_iscd, name 을 앞으로, 나머지는 수신 순서 유지
_FRONT_COLS = ["recv_ts", "mksc_shrn_iscd", "name"]


class OrderbookStore:
    """종목별 호가 프레임을 메모리에 누적하고, flush() 마다 **그동안 쌓인 분량만**
    새 조각(part) parquet 로 기록한 뒤 버퍼를 비운다(메모리·쓰기 비용 일정).
    각 조각은 그 자체로 완결된 parquet 이므로 쓰기 중 중단돼도 이전 조각은 안전하다.
    종료 시 merge_all() 로 종목별 조각을 모아 최종 parquet 1개로 병합한다.

    산출물:
      - 조각:  {out_dir}/parts/{yymmdd}_orderbook_{code}.{seq:04d}.parquet  (백업 보존)
      - 최종:  {out_dir}/{yymmdd}_orderbook_{code}.parquet                  (병합본)
    """

    def __init__(self, out_dir: Path, yymmdd: str, name_map: dict[str, str]):
        self.out_dir = out_dir
        self.parts_dir = out_dir / "parts"
        self.parts_dir.mkdir(parents=True, exist_ok=True)
        self.yymmdd = yymmdd
        self.name_map = name_map
        self._buffers: dict[str, list[dict]] = {}
        self._seq: dict[str, int] = {}        # code -> 다음 조각 번호
        self._lock = threading.Lock()
        self.frames = 0          # 누적 프레임 수 (전 종목 합)
        self.rows = 0            # 누적 행 수 (전 종목 합)
        self.parts_written = 0   # 누적 조각 파일 수

    def _final_path(self, code: str) -> Path:
        return self.out_dir / f"{self.yymmdd}_orderbook_{code}.parquet"

    def _part_path(self, code: str, seq: int) -> Path:
        return self.parts_dir / f"{self.yymmdd}_orderbook_{code}.{seq:04d}.parquet"

    def add(self, code: str, recv_ts: str, records: list[dict]) -> None:
        if not records:
            return
        name = self.name_map.get(code, code)
        with self._lock:
            buf = self._buffers.setdefault(code, [])
            for rec in records:
                rec = dict(rec)
                rec["recv_ts"] = recv_ts
                rec["mksc_shrn_iscd"] = code      # 안전: 프레임 코드 보정
                rec["name"] = name
                buf.append(rec)
            self.rows += len(records)
            self.frames += 1

    def _write_part(self, code: str, rows: list[dict], seq: int) -> None:
        df = pd.DataFrame(rows)
        cols = _FRONT_COLS + [c for c in df.columns if c not in _FRONT_COLS]
        df = df[cols]
        path = self._part_path(code, seq)
        tmp = path.with_name(path.name + ".tmp")
        df.to_parquet(str(tmp), index=False, engine="pyarrow", compression="zstd")
        os.replace(tmp, path)                 # 원자 교체

    def flush(self) -> int:
        """버퍼에 쌓인 분량을 종목별 새 조각으로 저장하고 버퍼를 비운다.
        저장한 조각(종목) 수 반환."""
        # 1) 버퍼를 통째로 꺼내고 즉시 비움 (락 안에서 — 메모리 일정 유지)
        with self._lock:
            snapshot: dict[str, list[dict]] = {}
            for code, buf in self._buffers.items():
                if buf:
                    snapshot[code] = buf
                    self._buffers[code] = []
            seqs = {code: self._seq.get(code, 0) for code in snapshot}
            for code in snapshot:
                self._seq[code] = seqs[code] + 1
        # 2) 디스크 I/O 는 락 밖에서 (수신 블로킹 최소화)
        saved = 0
        for code, rows in snapshot.items():
            try:
                self._write_part(code, rows, seqs[code])
                saved += 1
                self.parts_written += 1
            except Exception as e:
                _log(f"⚠ 조각 저장 실패 {code} seq={seqs[code]}: {type(e).__name__}: {e}")
                # 실패분은 버퍼 앞쪽에 되돌려 다음 주기 재시도 (시간순 보존)
                with self._lock:
                    self._buffers.setdefault(code, [])[:0] = rows
                    self._seq[code] = seqs[code]   # 번호 회수
        return saved

    def merge_all(self) -> list[tuple[str, int]]:
        """종목별 조각을 모아 최종 parquet 1개로 병합. [(code, 행수), ...] 반환.
        조각 파일은 백업용으로 남겨 둔다."""
        results = []
        codes = sorted(set(self._buffers.keys()) | set(self._seq.keys()))
        for code in codes:
            parts = sorted(self.parts_dir.glob(f"{self.yymmdd}_orderbook_{code}.[0-9]*.parquet"))
            if not parts:
                continue
            try:
                df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
                final = self._final_path(code)
                tmp = final.with_name(final.name + ".merge.tmp")
                df.to_parquet(str(tmp), index=False, engine="pyarrow", compression="zstd")
                os.replace(tmp, final)
                results.append((code, len(df)))
            except Exception as e:
                _log(f"⚠ 병합 실패 {code}: {type(e).__name__}: {e}")
        return results

    def code_count(self) -> int:
        with self._lock:
            return len(self._buffers)


_store: OrderbookStore | None = None
_last_progress = 0.0


def on_result(ws, tr_id, result, data_info):
    """KISWebSocket 콜백. result = polars DataFrame (컬럼 대문자)."""
    global _last_progress
    if result is None or len(result) == 0:
        return
    if str(tr_id) != "H0STASP0":
        return
    # 수신 시점 기록 (프로덕션 on_result 와 동일 포맷)
    recv_ts = datetime.now(KST).strftime(RECV_TS_FMT)
    # 컬럼 소문자 통일 (프로덕션 wss_data 와 동일)
    result = result.rename({c: c.strip().lower() for c in result.columns})
    records = result.to_dicts()
    # 프레임을 종목별로 분류해 버퍼에 누적 (다종목 동시 수신 대비)
    by_code: dict[str, list[dict]] = {}
    for rec in records:
        c = str(rec.get("mksc_shrn_iscd") or "").strip().zfill(6)
        if not c or c == "000000":
            continue
        by_code.setdefault(c, []).append(rec)
    for c, recs in by_code.items():
        _store.add(c, recv_ts, recs)

    now = time.time()
    if now - _last_progress >= 5.0:
        _last_progress = now
        try:
            r0 = records[0]
            _log(f"수신중 종목={_store.code_count()} frames={_store.frames} rows={_store.rows} "
                 f"[{r0.get('mksc_shrn_iscd')}] askp1={r0.get('askp1')} bidp1={r0.get('bidp1')} "
                 f"(bsop_hour={r0.get('bsop_hour')})")
        except Exception:
            pass


# ============================================================================
# 4) 주기 flush 스레드 + 종료 타이머
# ============================================================================
def _arm_flush(interval_sec: float) -> None:
    def _loop():
        while True:
            time.sleep(max(2.0, interval_sec))
            try:
                n = _store.flush() if _store else 0
                if n:
                    _log(f"flush: {n}종목 저장 (누적 frames={_store.frames} rows={_store.rows})")
            except Exception as e:
                _log(f"⚠ 주기 flush 예외: {e}")

    threading.Thread(target=_loop, name="flush-loop", daemon=True).start()
    _log(f"주기 flush 설정: {interval_sec:.0f}초마다 parquet 저장")


def _arm_until(until_hhmm: str) -> None:
    try:
        hh, mm = (int(x) for x in until_hhmm.split(":"))
    except Exception:
        _log(f"--until 형식 오류('{until_hhmm}') → 종료 타이머 미설정")
        return
    target = dtime(hh, mm)

    def _watch():
        while True:
            if datetime.now(KST).time() >= target:
                _log(f"종료 시각 {until_hhmm} 도달 → 마지막 flush + 병합 후 종료")
                if _store:
                    _store.flush()
                    merged = _store.merge_all()
                    _log(f"병합 완료: {len(merged)}종목 "
                         f"{[(c, n) for c, n in merged]}")
                os._exit(0)
            time.sleep(2.0)

    threading.Thread(target=_watch, name="until-watch", daemon=True).start()
    _log(f"종료 타이머 설정: {until_hhmm} (KST)")


# ============================================================================
# main
# ============================================================================
def main() -> None:
    global _store
    ap = argparse.ArgumentParser(description="전일 상한가 종목 실시간 호가(H0STASP0) 기록기 (a2 계정, parquet)")
    ap.add_argument("--code", default=None, help="종목코드(6자리, 쉼표로 여러 개). 미지정 시 전일 상한가 ST 전체")
    ap.add_argument("--until", default="15:40", help="종료 시각 HH:MM (KST, 기본 15:40)")
    ap.add_argument("--flush", type=float, default=20.0, help="parquet flush 주기(초, 기본 20)")
    args = ap.parse_args()

    _setup_a2_auth()
    targets = _select_codes(args.code)
    codes = [c for c, _ in targets]
    name_map = {c: n for c, n in targets}

    yymmdd = datetime.now(KST).strftime("%y%m%d")
    _store = OrderbookStore(OUT_DIR, yymmdd, name_map)

    _arm_flush(args.flush)
    _arm_until(args.until)

    # 전일 상한가 종목 전체 H0STASP0 구독 → 끊김 시 라이브러리 내부 재연결(open_map 재전송)
    kws = ka.KISWebSocket(api_url="/tryitout", max_retries=50)
    kws.subscribe(request=asking_price_krx, data=codes)
    _log(f"H0STASP0 구독 시작: {len(codes)}종목 → {OUT_DIR}/{yymmdd}_orderbook_*.parquet")
    try:
        kws.start(on_result=on_result)
    except KeyboardInterrupt:
        _log("Ctrl+C 중단")
    finally:
        if _store:
            _store.flush()
            merged = _store.merge_all()
            _log(f"종료 — 종목={_store.code_count()} frames={_store.frames} rows={_store.rows} "
                 f"parts={_store.parts_written} | 병합 {len(merged)}종목 저장: {OUT_DIR}")


if __name__ == "__main__":
    main()
