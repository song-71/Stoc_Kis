"""
09:00 ~ 15:19 매 1분마다 상승 VI 현황을 조회하여 CSV part 파일로 저장
15:20 도달 시 당일 CSV 전체를 하나로 병합(중복 제거) 후, 원본은 backup 폴더로 이동

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

_VI_STATUS = {"N": "해제", "Y": "발동중", "0": "해제", "1": "정적VI", "2": "동적VI", "3": "정적&동적"}
_VI_KIND   = {"1": "정적VI", "2": "동적VI", "3": "정적&동적"}


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
        records.append({
            "fetch_time": now.strftime("%H:%M"),
            "종목코드": str(r.get("mksc_shrn_iscd", "")).strip(),
            "종목명": str(r.get("hts_kor_isnm", "")).strip(),
            "상태": _VI_STATUS.get(r.get("vi_cls_code", ""), r.get("vi_cls_code", "")),
            "종류": _VI_KIND.get(r.get("vi_kind_code", ""), r.get("vi_kind_code", "")),
            "발동시간": str(r.get("cntg_vi_hour", "")).strip(),
            "해제시간": str(r.get("vi_cncl_hour", "")).strip(),
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

    print("=" * 60)
    start_msg = f"[{PROGRAM_NAME}] 프로그램 시작 — 상승 VI 현황 1분단위 수집(09:00 ~ 15:19)"
    print(start_msg)
    print(f"저장 경로: {OUT_DIR}")
    print("=" * 60)
    _tele(start_msg)

    client = _init_client()

    now = datetime.now(KST)
    today = now.date()
    today_str = now.strftime("%y%m%d")
    date_full = now.strftime("%Y%m%d")  # API 조회용 YYYYMMDD

    start_time = datetime(today.year, today.month, today.day, 9, 0, 0, tzinfo=KST)
    end_time   = datetime(today.year, today.month, today.day, 15, 19, 0, tzinfo=KST)
    merge_time = datetime(today.year, today.month, today.day, 15, 20, 0, tzinfo=KST)

    # 이미 15:20 이후면 병합만 실행
    if now >= merge_time:
        print(f"[{now.strftime('%H:%M:%S')}] 15:20 이후 → 다운로드 파일 병합 실행")
        _merge_and_backup(today_str)
        end_msg = f"[{PROGRAM_NAME}] 파일 병합 완료. 프로그램 종료."
        print(end_msg)
        _tele(end_msg)
        raise SystemExit(0)

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
