"""
09:00 ~ 15:19 매 1분마다 상승 VI 현황을 조회하여 CSV part 파일로 저장
15:20 도달 시 당일 CSV 전체를 하나로 병합(중복 제거) 후, 원본은 backup 폴더로 이동
15:20 이후 실행 시 API/WSS 초기화 없이 파트파일 병합만 수행 후 즉시 종료

nohup 실행:
nohup /home/ubuntu/Stoc_Kis/venv/bin/python -u /home/ubuntu/Stoc_Kis/Daily_inquire_vi_status.py > /home/ubuntu/Stoc_Kis/out/Daily_inquire_vi_status.out 2>&1 &

로그 보기:
tail -f /home/ubuntu/Stoc_Kis/out/Daily_inquire_vi_status.out

프로세스 확인:
pgrep -af Daily_inquire_vi_status.py

일괄 종료:
pkill -f Daily_inquire_vi_status.py

종료 확인:
pgrep -af Daily_inquire_vi_status.py || echo "no process"
"""

import json
import os
import shutil
import sys
import threading
import time
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig
from kis_utils import is_holiday

try:
    from telegMsg import tmsg
except Exception:
    tmsg = None

# ── a2 계좌 WSS (H0STMKO0 장운영정보 구독)용 모듈 ──
# domestic_stock_functions_ws가 `import kis_auth as ka`를 참조하므로 sys.modules 덮어쓰기 후 import
import kis_auth_llm as ka
sys.modules["kis_auth"] = ka
from domestic_stock_functions_ws import market_status_krx  # noqa: E402

PROGRAM_NAME = "Daily_inquire_vi_status.py"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
KST = ZoneInfo("Asia/Seoul")

# ── 조회 옵션 ──
OPT_MARKET    = "0"   # 시장구분  0:전체 / K:코스피 / Q:코스닥
OPT_DIRECTION = "1"   # 방향구분  0:전체 / 1:상승 / 2:하락
OPT_VI_TYPE   = "0"   # VI종류   0:전체 / 1:정적 / 2:동적 / 3:정적&동적

# ── 저장 경로 ──
OUT_DIR = os.path.join(SCRIPT_DIR, "data", "vi_status", "1m_fetch")
BACKUP_DIR = os.path.join(OUT_DIR, "backup")
LOG_DIR = os.path.join(SCRIPT_DIR, "out", "log")

_VI_STATUS = {"N": "해제", "Y": "발동중", "0": "해제", "1": "정적VI", "2": "동적VI", "3": "정적&동적"}
_VI_KIND   = {"1": "정적VI", "2": "동적VI", "3": "정적&동적"}

# ── VI 로그/상태 추적 ──
_log_lock = threading.Lock()
_log_path: str | None = None

_vi_state_lock = threading.Lock()
_seen_vi: dict[str, dict] = {}          # key=f"{code}_{start}" → {code,name,start,end,released_logged}
_code_name: dict[str, str] = {}         # code → 종목명

# ── WSS ──
_kws = None                              # ka.KISWebSocket 인스턴스 (a2 계좌)
_ws_ready = threading.Event()
_ws_lock = threading.Lock()
_subscribed_codes: set[str] = set()


def _vi_log(msg: str) -> None:
    """VI_status_{YYMMDD}.log에 타임스탬프와 함께 append."""
    now = datetime.now(KST)
    ts = now.strftime("%H%M%S,") + f"{now.microsecond // 1000:03d}"
    line = f"{ts} {msg}"
    with _log_lock:
        print(line)
        if _log_path:
            try:
                with open(_log_path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except Exception as e:
                print(f"[로그쓰기실패] {e}")


def _tele(msg: str) -> None:
    if tmsg is not None:
        try:
            tmsg(msg, "-t")
        except Exception:
            pass


def _init_client() -> KisClient:
    with open(os.path.join(SCRIPT_DIR, "config.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    acct = cfg["users"][cfg["default_user"]]["accounts"]["main"]
    return KisClient(KisConfig(
        appkey=acct["appkey"], appsecret=acct["appsecret"],
        base_url=cfg.get("base_url", DEFAULT_BASE_URL),
        token_cache_path=os.path.join(SCRIPT_DIR, "kis_token_main.json"),
    ))


# ══════════════════════════════════════════════════════════════════
#  a2(syw_2) 계좌 H0STMKO0 WSS 연결 — 장운영정보 실시간 구독
# ══════════════════════════════════════════════════════════════════

def _on_ws_system(rsp) -> None:
    """WSS 시스템 응답 (구독 ack 등) 로깅."""
    try:
        if rsp.tr_id == "H0STMKO0":
            code = (rsp.tr_key or "").strip()
            name = _code_name.get(code, code)
            _vi_log(f"[장운영정보 구독응답] {name}({code}) {rsp.tr_msg}")
    except Exception:
        pass


def _on_ws_result(ws, tr_id, df, data_info) -> None:
    """H0STMKO0 수신 메시지 처리 → VI_CLS_CODE 로깅 + 해제 시 구독 종료."""
    if tr_id != "H0STMKO0":
        return
    try:
        # [260601] SDK 가 넘기는 df 는 polars DataFrame → pandas .iterrows() 없음(파싱오류 원인).
        # polars .iter_rows(named=True)(=dict) 로 순회. pandas 도 들어올 수 있어 방어적으로 처리.
        rows = df.iter_rows(named=True) if hasattr(df, "iter_rows") else (r for _, r in df.iterrows())
        for row in rows:
            row = dict(row)
            code = str(row.get("mksc_shrn_iscd", "")).strip().zfill(6)
            vi_cls = str(row.get("vi_cls_code", "")).strip()
            name = _code_name.get(code, code)
            # [260601] H0STMKO0 가 실제 무엇을 주는지 전체 필드 로깅 (VI 테스트/감지 로직 설계용)
            _vi_log(f'[장운영정보] {name}({code}) {row}')
            if not vi_cls:
                continue
            # VI 해제 통보 → 구독 종료 (40슬롯 한도 관리)
            if vi_cls == "N":
                _mk_subscribe_remove(code, name)
    except Exception as e:
        _vi_log(f"[장운영정보 파싱오류] {e}")


def _init_ws_a2() -> None:
    """config.json → syw_2 계좌로 WSS 인증 + 백그라운드 스레드로 KISWebSocket 시작."""
    global _kws
    with open(os.path.join(SCRIPT_DIR, "config.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    acct = cfg["users"][cfg["default_user"]]["accounts"].get("syw_2")
    if not acct or not acct.get("appkey"):
        _vi_log("[WSS초기화 실패] syw_2 계좌 설정 없음")
        return

    ka._cfg["my_app"] = acct["appkey"]
    ka._cfg["my_sec"] = acct["appsecret"]
    ka.token_tmp = os.path.join(SCRIPT_DIR, "kis_token_syw_2.json")
    try:
        ka.auth(svr="prod")
        ka.auth_ws(svr="prod")
    except Exception as e:
        _vi_log(f"[WSS 인증 실패] {e}")
        return

    if not getattr(ka, "_base_headers_ws", {}).get("approval_key"):
        _vi_log("[WSS 인증 실패] approval_key 없음")
        return

    ka.open_map.clear()  # 빈 상태로 시작 → 동적으로 send_request로 추가
    _kws = ka.KISWebSocket(api_url="", max_retries=100)
    _kws.on_system = _on_ws_system

    def _run():
        try:
            _ws_ready.set()
            _kws.start(on_result=_on_ws_result)
        except Exception as e:
            _vi_log(f"[WSS 스레드 종료] {e}")

    t = threading.Thread(target=_run, name="a2_h0stmko0_ws", daemon=True)
    t.start()
    # 연결 수립까지 잠시 대기
    time.sleep(2.0)
    _vi_log("[WSS 시작] a2 계좌 H0STMKO0 연결 대기 완료")


def _mk_subscribe_add(code: str, name: str) -> None:
    """H0STMKO0에 종목 동적 구독 추가."""
    if _kws is None:
        return
    with _ws_lock:
        if code in _subscribed_codes:
            return
        # 연결 준비 대기 (최대 5초)
        for _ in range(50):
            if _kws._ws is not None and _kws._loop is not None:
                break
            time.sleep(0.1)
        try:
            _kws.send_request(request=market_status_krx, tr_type="1", data=code)
            _subscribed_codes.add(code)
            _vi_log(f"[장운영정보 구독] {name}({code}) 구독 요청")
        except Exception as e:
            _vi_log(f"[장운영정보 구독실패] {name}({code}) {e}")


def _mk_subscribe_remove(code: str, name: str) -> None:
    """H0STMKO0 종목 구독 해제 (VI 해제 통보 시 호출)."""
    if _kws is None:
        return
    with _ws_lock:
        if code not in _subscribed_codes:
            return
        try:
            _kws.send_request(request=market_status_krx, tr_type="2", data=code)
            _subscribed_codes.discard(code)
            _vi_log(f"[장운영정보 구독해제] {name}({code})")
        except Exception as e:
            _vi_log(f"[장운영정보 구독해제실패] {name}({code}) {e}")


def _query_vi(client: KisClient, date: str) -> list[dict]:
    """VI 현황 조회 (연속조회 포함)"""
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/quotations/inquire-vi-status"
    params = {
        "FID_COND_SCR_DIV_CODE": "20139",
        "FID_INPUT_ISCD": "",
        "FID_MRKT_CLS_CODE": OPT_MARKET,
        "FID_DIV_CLS_CODE": OPT_DIRECTION,
        "FID_RANK_SORT_CLS_CODE": OPT_VI_TYPE,
        "FID_INPUT_DATE_1": date,
        "FID_TRGT_CLS_CODE": "",
        "FID_TRGT_EXLS_CLS_CODE": "",
    }

    rows = []
    tr_cont = ""
    for _ in range(10):
        headers = client._headers(tr_id="FHPST01390000")
        headers["tr_cont"] = "N" if tr_cont else ""
        r = client.session.get(url, headers=headers, params=params, timeout=10)
        j = r.json()
        if str(j.get("rt_cd")) != "0":
            print(f"[오류] {j.get('msg1', '')}")
            break
        output = j.get("output") or []
        if not output:
            break
        rows.extend(output)
        if r.headers.get("tr_cont") == "M":
            tr_cont = "N"
            time.sleep(0.5)
        else:
            break
    return rows


def _fetch_and_save(client: KisClient, now: datetime, date_full: str) -> str | None:
    """VI 현황 조회 후 CSV 저장, 저장된 파일 경로 반환"""
    try:
        rows = _query_vi(client, date_full)
    except Exception as e:
        print(f"[{now.strftime('%H:%M:%S')}] 조회 실패: {e}")
        return None

    if not rows:
        print(f"[{now.strftime('%H:%M:%S')}] 조회 결과 없음 (VI 발동 종목 없음)")
        return None

    # 필요 컬럼만 추출하여 DataFrame 생성
    records = []
    for r in rows:
        stnd = r.get("vi_stnd_prc", "")
        dprt = r.get("vi_dprt", "")
        if not str(stnd).strip() or str(stnd).strip() == "0":
            stnd = r.get("vi_dmc_stnd_prc", "")
            dprt = r.get("vi_dmc_dprt", "")

        code = str(r.get("mksc_shrn_iscd", "")).strip().zfill(6)
        name = str(r.get("hts_kor_isnm", "")).strip()
        start = str(r.get("cntg_vi_hour", "")).strip()
        end   = str(r.get("vi_cncl_hour", "")).strip()

        # ── VI 발동/해제 diff 로깅 ──
        if code and start:
            if name:
                _code_name[code] = name
            key = f"{code}_{start}"
            with _vi_state_lock:
                entry = _seen_vi.get(key)
                if entry is None:
                    _seen_vi[key] = {
                        "code": code, "name": name, "start": start,
                        "end": end, "released_logged": False,
                    }
                    _vi_log(
                        f"[VI Status 조회_발동] {name}({code}) "
                        f"발동시각 {start} 해제시각 {end or '000000'}"
                    )
                    # 발동 시 H0STMKO0 구독 (lock 바깥에서 호출)
                    need_subscribe = True
                else:
                    need_subscribe = False
                    if end and not entry["released_logged"] and end != "000000":
                        entry["end"] = end
                        entry["released_logged"] = True
                        _vi_log(
                            f"[VI Status 조회_해제] {name}({code}) "
                            f"발동시각 {start} 해제시각 {end}"
                        )
            if need_subscribe:
                _mk_subscribe_add(code, name or code)

        records.append({
            "fetch_time": now.strftime("%H:%M"),
            "종목코드": code,
            "종목명": name,
            "상태": _VI_STATUS.get(r.get("vi_cls_code", ""), r.get("vi_cls_code", "")),
            "종류": _VI_KIND.get(r.get("vi_kind_code", ""), r.get("vi_kind_code", "")),
            "발동시간": start,
            "해제시간": end,
            "발동가": r.get("vi_prc", ""),
            "기준가": stnd,
            "괴리율": dprt,
            "횟수": r.get("vi_count", ""),
        })

    df = pd.DataFrame(records)
    ts = now.strftime("%y%m%d_%H%M")
    filename = f"vi_status_{ts}.csv"
    out_path = os.path.join(OUT_DIR, filename)
    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[{now.strftime('%H:%M:%S')}] 저장 완료: {filename} ({len(rows)}건)")
    return out_path


def _merge_and_backup(today_str: str):
    """당일 CSV 전체 병합(중복 제거) 후 원본을 backup으로 이동"""
    pattern = f"vi_status_{today_str}_"
    csv_files = sorted([
        f for f in os.listdir(OUT_DIR)
        if f.startswith(pattern) and f.endswith(".csv")
    ])

    if not csv_files:
        print("[병합] 병합할 파일이 없습니다.")
        return

    # 병합
    dfs = []
    for f in csv_files:
        path = os.path.join(OUT_DIR, f)
        try:
            dfs.append(pd.read_csv(path, encoding="utf-8-sig", dtype=str))
        except Exception as e:
            print(f"[병합] 읽기 실패 {f}: {e}")

    if not dfs:
        print("[병합] 읽을 수 있는 파일이 없습니다.")
        return

    merged = pd.concat(dfs, ignore_index=True)
    before = len(merged)

    # 중복 제거: 동일 종목코드+발동시간 기준 (fetch_time이 다르더라도 같은 VI 발동이면 중복)
    dedup_cols = ["종목코드", "발동시간", "종류"]
    merged = merged.drop_duplicates(subset=dedup_cols, keep="last").reset_index(drop=True)
    removed = before - len(merged)

    merged_filename = f"vi_status_merged_{today_str}.csv"
    merged_path = os.path.join(OUT_DIR, merged_filename)
    merged.to_csv(merged_path, index=False, encoding="utf-8-sig")
    print(f"[병합] 병합 완료: {merged_filename} ({len(merged)}행, 원본 {len(csv_files)}개, 중복제거 {removed}건)")

    # backup 폴더로 원본 이동
    os.makedirs(BACKUP_DIR, exist_ok=True)
    moved = 0
    for f in csv_files:
        src = os.path.join(OUT_DIR, f)
        dst = os.path.join(BACKUP_DIR, f)
        try:
            shutil.move(src, dst)
            moved += 1
        except Exception as e:
            print(f"[백업] 이동 실패 {f}: {e}")
    print(f"[백업] {moved}개 파일 → backup 폴더 이동 완료")


def _wait_until(target: datetime):
    """target 시각까지 대기"""
    while True:
        now = datetime.now(KST)
        remain = (target - now).total_seconds()
        if remain <= 0:
            return
        time.sleep(min(remain, 0.5))


if __name__ == "__main__":
    if is_holiday():
        msg = f"[{PROGRAM_NAME}] => 휴일로 프로그램을 종료합니다."
        print(msg)
        _tele(msg)
        raise SystemExit(0)

    now = datetime.now(KST)
    today = now.date()
    today_str = now.strftime("%y%m%d")
    merge_time = datetime(today.year, today.month, today.day, 15, 20, 0, tzinfo=KST)

    # ── 15:20 이후 실행 시: 병합만 수행하고 즉시 종료 ──
    if now >= merge_time:
        print(f"[{now.strftime('%H:%M:%S')}] 15:20 이후 → 파트파일 병합만 실행")
        _merge_and_backup(today_str)
        end_msg = f"[{PROGRAM_NAME}] 파일 병합 완료. 프로그램 종료."
        print(end_msg)
        _tele(end_msg)
        raise SystemExit(0)

    print("=" * 60)
    start_msg = f"[{PROGRAM_NAME}] 프로그램 시작 — 상승 VI 현황 1분단위 수집(09:00 ~ 15:19)"
    print(start_msg)
    print(f"저장 경로: {OUT_DIR}")
    print("=" * 60)
    _tele(start_msg)

    # ── VI 이벤트 로그 파일 초기화 ──
    os.makedirs(LOG_DIR, exist_ok=True)
    _log_path = os.path.join(LOG_DIR, f"VI_status_{datetime.now(KST).strftime('%y%m%d')}.log")
    _vi_log(f"[시작] {PROGRAM_NAME} 로그 파일: {_log_path}")

    client = _init_client()

    # ── a2 계좌 H0STMKO0 WSS 백그라운드 시작 ──
    # [260601] 비활성화: 장운영정보(H0STMKO0) 수신·처리는 ws_realtime_trading.py 가 담당
    #   (_on_market_status_krx, market_status_krx 구독, 슬롯관리) → 여기서 중복 수신 불필요.
    #   이 파일은 REST VI현황 CSV 저장만 수행. 장운영정보 관찰/실험은 test_vi_wss_cycle.py 로.
    #   (_kws 가 None 이면 _mk_subscribe_add 도 자동 no-op. WSS 함수들은 미사용 dormant.)
    # _init_ws_a2()

    date_full = now.strftime("%Y%m%d")  # API 조회용 YYYYMMDD
    start_time = datetime(today.year, today.month, today.day, 9, 0, 0, tzinfo=KST)
    end_time   = datetime(today.year, today.month, today.day, 15, 19, 0, tzinfo=KST)

    # 09:00 이전이면 대기
    if now < start_time:
        print(f"[{now.strftime('%H:%M:%S')}] 09:00까지 대기 중...")
        _wait_until(start_time)

    # ── 매 1분 루프: 09:00 ~ 15:19 ──
    current = datetime.now(KST)
    next_run = current.replace(second=0, microsecond=0) + timedelta(minutes=1)
    if next_run < start_time:
        next_run = start_time

    saved_count = 0
    print(f"[{current.strftime('%H:%M:%S')}] 다음 수집: {next_run.strftime('%H:%M')}")

    while next_run <= end_time:
        _wait_until(next_run)
        now = datetime.now(KST)
        result = _fetch_and_save(client, now, date_full)
        if result:
            saved_count += 1
        next_run += timedelta(minutes=1)

    print(f"\n[수집 종료] 총 {saved_count}개 파일 저장 완료")

    # ── 15:20 병합 ──
    print(f"[{datetime.now(KST).strftime('%H:%M:%S')}] 15:20까지 대기 중...")
    _wait_until(merge_time)
    _merge_and_backup(today_str)
    end_msg = f"[{PROGRAM_NAME}] 수집 {saved_count}개 완료, 파일 병합 완료. 프로그램 종료."
    print(end_msg)
    _tele(end_msg)
