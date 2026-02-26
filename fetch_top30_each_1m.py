"""
09:01 ~ 15:09 매 1분마다 등락률 상위 Top30을 조회하여 CSV 저장
15:10 도달 시 당일 CSV 전체를 하나로 병합 후, 원본은 backup 폴더로 이동
syw_2 계정 사용

nohup 실행:
nohup /home/ubuntu/Stoc_Kis/venv/bin/python -u /home/ubuntu/Stoc_Kis/fetch_top30_each_1m.py > /home/ubuntu/Stoc_Kis/out/fetch_top30_each_1m.out 2>&1 &

로그 보기:
tail -f /home/ubuntu/Stoc_Kis/out/fetch_Top30_each_1m.out

프로세스 확인:
pgrep -af fetch_top30_each_1m.py

일괄 종료:
pkill -f fetch_top30_each_1m.py

종료 확인:
pgrep -af fetch_top30_each_1m.py || echo "no process"
"""

import os
import shutil
import time
import pandas as pd
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig
from kis_utils import is_holiday, load_config

try:
    from telegMsg import tmsg
except Exception:
    tmsg = None

PROGRAM_NAME = "fetch_top30_each_1m.py"


def _tele(msg: str) -> None:
    """텔레그램 메시지 발송 (실패 시 무시)"""
    if tmsg is not None:
        try:
            tmsg(msg, "-t")
        except Exception:
            pass

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TOP_N = 50
KST = ZoneInfo("Asia/Seoul")

# ── 저장 경로 ──
OUT_DIR = os.path.join(SCRIPT_DIR, "data", "fetch_top_list", "1m_fetch")
BACKUP_DIR = os.path.join(OUT_DIR, "backup")


def _init_client_syw2() -> KisClient:
    """syw_2 계정으로 KisClient 생성"""
    cfg = load_config(os.path.join(SCRIPT_DIR, "config.json"))
    acct = cfg.get("accounts", {}).get("syw_2", {})
    appkey = acct.get("appkey")
    appsecret = acct.get("appsecret")
    if not appkey or not appsecret:
        raise ValueError("syw_2 appkey/appsecret이 필요합니다. config.json의 accounts.syw_2에 설정하세요.")
    base_url = cfg.get("base_url") or DEFAULT_BASE_URL
    custtype = cfg.get("custtype") or "P"
    market_div = cfg.get("market_div") or "J"
    kis_cfg = KisConfig(
        appkey=appkey,
        appsecret=appsecret,
        base_url=base_url,
        custtype=custtype,
        market_div=market_div,
        token_cache_path=os.path.join(SCRIPT_DIR, "kis_token_syw2.json"),
    )
    return KisClient(kis_cfg)


def _fetch_fluctuation_top(client: KisClient, top_n: int = 50) -> list[dict]:
    """등락률 상위 조회"""
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/ranking/fluctuation"
    headers = client._headers(tr_id="FHPST01700000")
    params = {
        "fid_cond_mrkt_div_code": client.cfg.market_div,
        "fid_cond_scr_div_code": "20170",
        "fid_input_iscd": "0000",
        "fid_rank_sort_cls_code": "0",
        "fid_input_cnt_1": "0",
        "fid_prc_cls_code": "0",
        "fid_input_price_1": "",
        "fid_input_price_2": "",
        "fid_vol_cnt": "",
        "fid_trgt_cls_code": "0",
        "fid_trgt_exls_cls_code": "0",
        "fid_div_cls_code": "0",
        "fid_rsfl_rate1": "",
        "fid_rsfl_rate2": "",
    }
    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    rt_cd = str(j.get("rt_cd", "")).strip()
    if rt_cd and rt_cd != "0":
        raise RuntimeError(f"[등락률순위] 실패: rt_cd={rt_cd} msg={j.get('msg1')}")
    output = j.get("output") or []
    if isinstance(output, dict):
        output = [output]
    return output[:top_n]


def _fetch_and_save(client: KisClient, now: datetime) -> str | None:
    """Top30 조회 후 CSV 저장, 저장된 파일 경로 반환"""
    try:
        rows = _fetch_fluctuation_top(client, top_n=TOP_N)
    except Exception as e:
        print(f"[{now.strftime('%H:%M:%S')}] 조회 실패: {e}")
        return None

    if not rows:
        print(f"[{now.strftime('%H:%M:%S')}] 조회 결과 없음")
        return None

    df = pd.DataFrame(rows)
    # fetch_time 컬럼 추가 (병합 시 시각 구분용)
    df.insert(0, "fetch_time", now.strftime("%H:%M"))

    ts = now.strftime("%y%m%d_%H%M")
    filename = f"Top30_1m_{ts}.csv"
    out_path = os.path.join(OUT_DIR, filename)
    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[{now.strftime('%H:%M:%S')}] 저장 완료: {filename} ({len(rows)}건)")
    return out_path


def _merge_and_backup(today_str: str):
    """당일 1분봉 CSV 전체 병합 후 원본을 backup으로 이동"""
    pattern = f"Top30_1m_{today_str}_"
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
            dfs.append(pd.read_csv(path, encoding="utf-8-sig"))
        except Exception as e:
            print(f"[병합] 읽기 실패 {f}: {e}")

    if not dfs:
        print("[병합] 읽을 수 있는 파일이 없습니다.")
        return

    merged = pd.concat(dfs, ignore_index=True)
    merged_filename = f"Top30_1m_merged_{today_str}.csv"
    merged_path = os.path.join(OUT_DIR, merged_filename)
    merged.to_csv(merged_path, index=False, encoding="utf-8-sig")
    print(f"[병합] 병합 완료: {merged_filename} ({len(merged)}행, 원본 {len(csv_files)}개)")

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
    start_msg = f"[{PROGRAM_NAME}] 프로그램 시작 — 당일 등락율 상위 Top30 1분단위 수집(09:01 ~ 15:09)"
    print(start_msg)
    print(f"저장 경로: {OUT_DIR}")
    print("=" * 60)
    _tele(start_msg)

    client = _init_client_syw2()

    now = datetime.now(KST)
    today = now.date()
    today_str = now.strftime("%y%m%d")

    # 시작 시각: 09:01 (이미 지났으면 다음 정각+1분)
    start_time = datetime(today.year, today.month, today.day, 9, 1, 0, tzinfo=KST)
    end_time = datetime(today.year, today.month, today.day, 15, 9, 0, tzinfo=KST)
    merge_time = datetime(today.year, today.month, today.day, 15, 10, 0, tzinfo=KST)

    if now >= merge_time:
        print(f"[{now.strftime('%H:%M:%S')}] 15:10 이후 → 다운로드 파일 병합 실행")
        _merge_and_backup(today_str)
        end_msg = f"[{PROGRAM_NAME}] 파일 병합 완료. 프로그램 종료."
        print(end_msg + "=" * 60)
        _tele(end_msg)
        raise SystemExit(0)

    # 09:01 이전이면 대기
    if now < start_time:
        print(f"[{now.strftime('%H:%M:%S')}] 09:01까지 대기 중...")
        _wait_until(start_time)

    # ── 매 1분 루프: 09:01 ~ 15:09 ──
    current = datetime.now(KST)
    # 다음 정분(초=0) 기준으로 스케줄 시작
    next_run = current.replace(second=0, microsecond=0) + timedelta(minutes=1)
    if next_run < start_time:
        next_run = start_time

    saved_count = 0
    print(f"[{current.strftime('%H:%M:%S')}] 다음 수집: {next_run.strftime('%H:%M')}")

    while next_run <= end_time:
        _wait_until(next_run)
        now = datetime.now(KST)
        result = _fetch_and_save(client, now)
        if result:
            saved_count += 1
        next_run += timedelta(minutes=1)

    print(f"\n[수집 종료] 총 {saved_count}개 파일 저장 완료")

    # ── 15:10 병합 ──
    print(f"[{datetime.now(KST).strftime('%H:%M:%S')}] 15:10까지 대기 중...")
    _wait_until(merge_time)
    _merge_and_backup(today_str)
    end_msg = f"[{PROGRAM_NAME}] 수집 {saved_count}개 완료, 파일 병합 완료. 프로그램 종료."
    print(end_msg)
    _tele(end_msg)
