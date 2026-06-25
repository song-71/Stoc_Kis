"""
Created on 20260402
NXT(넥스트레이드) 프리마켓/애프터마켓 1분봉 다운로드

기존 kis_1m_API_to_Parquet_all_code.py와 동일 구조.
차이점:
  - fid_cond_mrkt_div_code="NX" (NXT 시장)
  - 시간 범위: 프리마켓(08:00~08:50) + 애프터마켓(15:40~20:00)
  - 출력 파일: {date}_1m_chart_DB_parquet_NXT.parquet
  - S3 경로: .../market_data/1m_nxt/date=YYYY-MM-DD/ohlcv.parquet
  - EC2 로컬: *_NXT.parquet 최근 2일치만 유지

실행 (하루 2회):
    # 아침(프리마켓 08:00~08:50만, 당일 오전 분석용) — 08:51 KST (프리마켓 종료 직후)
    nohup .../venv/bin/python .../kis_1m_API_to_Parquet_NXT.py --session=morning > .../out/1m_nxt_parquet_morning.log 2>&1 &
    # 저녁(애프터마켓 15:40~20:00만, 아침 프리마켓 파일과 병합) — 20:01 KST
    nohup .../venv/bin/python .../kis_1m_API_to_Parquet_NXT.py --session=evening > .../out/1m_nxt_parquet.log 2>&1 &

세션(--session) 미지정 시 KST 시각 자동 판정(12시 이전=morning, 이후=evening).
아침 실행이 누락된 날은 저녁에 `--session=full`(프리+애프터)로 수동 보강 가능.

로그:
    tail -f /home/ubuntu/Stoc_Kis/out/1m_nxt_parquet.log

프로세스 관리:
    pgrep -af kis_1m_API_to_Parquet_NXT.py
    pkill -f kis_1m_API_to_Parquet_NXT.py
"""

import sys
import logging
import time
import csv
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
import boto3

sys.path.extend(['../..', '.'])
import kis_auth as ka
from kis_utils import LogManager, load_kis_data_layout, load_symbol_master, print_table, stQueryDB, is_holiday
from inquire_time_itemchartprice import inquire_time_itemchartprice

logging.basicConfig(level=logging.INFO)

# 비워두면 전체 종목 조회
CODE_TEXT = """
"""

LOG_ID = "_1m_nxt"
MARKET_CODE = "NX"  # NXT 시장


def ts_prefix() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).strftime(f"[%y%m%d_%H%M%S{LOG_ID}]")


# NXT 시간 범위: 프리마켓(08:00~08:50) + 애프터마켓(15:40~20:00)
# API 1회 호출 = 최대 30개(30분) 반환 → 30분 간격 기준점으로 호출

# 프리마켓 조회 기준시각.
# API는 기준시각 기준 "과거 최대 30봉"을 한 번에 반환(시작점 아님).
#   081000 → 08:00~08:10 (첫 봉 08:00 정각 확보용. 08:30 기준은 08:01부터라 08:00이 누락됨)
#   083000 → 08:01~08:30
#   085000 → 08:21~08:50
# → 3회로 08:00~08:50 전 구간 커버.
PREMARKET_QUERY_TIMES = ["081000", "083000", "085000"]


def _build_aftermarket_query_times() -> list[str]:
    """애프터마켓 조회 기준시각 (16:10~20:00, 30분 간격 + 20:00 보장)."""
    times: list[str] = []
    cur = datetime.strptime("161000", "%H%M%S")
    end = datetime.strptime("200000", "%H%M%S")
    while cur <= end:
        times.append(cur.strftime("%H%M%S"))
        cur += timedelta(minutes=30)
    if "200000" not in times:
        times.append("200000")
    return times


AFTERMARKET_QUERY_TIMES = _build_aftermarket_query_times()


def _build_query_times(session: str) -> list[str]:
    """세션별 조회 기준시각 목록.
    - morning: 프리마켓만 (당일 오전 분석용, 08:00~08:50)
    - evening: 애프터마켓만 (15:40~20:00). 아침 프리마켓 파일과 최종 병합.
    - full:    프리마켓 + 애프터마켓 (수동 단발 보강용. 아침 실행 누락된 날 복구)
    """
    if session == "morning":
        return list(PREMARKET_QUERY_TIMES)
    if session == "evening":
        return list(AFTERMARKET_QUERY_TIMES)
    return list(PREMARKET_QUERY_TIMES) + list(AFTERMARKET_QUERY_TIMES)


def _resolve_session(now_kst: datetime) -> str:
    """실행 세션 결정. `--session=morning|evening|full` 인자 우선,
    없으면 KST 시각 자동 판정(12시 이전=morning, 이후=evening)."""
    for a in sys.argv[1:]:
        if a.startswith("--session="):
            val = a.split("=", 1)[1].strip().lower()
            if val in ("morning", "evening", "full"):
                return val
    return "morning" if now_kst.hour < 12 else "evening"

# NXT 허용 시간 범위 (이 범위 밖 데이터는 필터링)
NXT_TIME_RANGES = [
    ("080000", "085000"),  # 프리마켓
    ("154000", "200000"),  # 애프터마켓
]


def _in_nxt_time(t: str) -> bool:
    """시간 문자열이 NXT 프리/애프터마켓 범위 내인지 확인."""
    for lo, hi in NXT_TIME_RANGES:
        if lo <= t <= hi:
            return True
    return False


def _parse_codes(text: str, name_to_code: dict[str, str] | None = None) -> list[str]:
    codes = []
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        if not raw.isdigit():
            if name_to_code:
                code = name_to_code.get(raw)
                if code:
                    codes.append(code)
            continue
        codes.append(raw.zfill(6))
    return codes


def _drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    drop_cols = []
    for col in df.columns:
        series = df[col]
        if series.isna().all():
            drop_cols.append(col)
            continue
        if series.map(lambda v: str(v).strip()).eq("").all():
            drop_cols.append(col)
    if drop_cols:
        return df.drop(columns=drop_cols)
    return df


TARGET_CODES = _parse_codes(CODE_TEXT)

COLUMN_MAPPING = {
    'stck_prdy_clpr': 'pdy_close',
    'acml_vol': 'acml_volume',
    'hts_kor_isnm': 'name',
    'stck_bsop_date': 'date',
    'stck_cntg_hour': 'time',
    'stck_prpr': 'close',
    'stck_oprc': 'open',
    'stck_hgpr': 'high',
    'stck_lwpr': 'low',
    'cntg_vol': 'volume',
    'acml_tr_pbmn': 'acml_value'
}

DESIRED_COLS = [
    "date", "time", "code", "name",
    "open", "high", "low", "close",
    "volume", "acml_volume", "acml_value",
    "tdy_ret", "pdy_close", "pdy_ret",
]

LEGACY_RENAME = {
    "영업일자": "date", "체결시간": "time",
    "시가": "open", "최고가": "high", "최저가": "low", "종가": "close",
    "거래량": "volume", "누적 거래대금": "acml_value", "누적거래대금": "acml_value",
    "누적거래량": "acml_volume", "전일대비율": "tdy_ret", "전일종가": "pdy_close",
}

NUMERIC_COLUMNS = []
print_all_rows = 0


def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)

    kst = timezone(timedelta(hours=9))
    layout = load_kis_data_layout()
    log_dir = Path(layout.local_root) / "logs"
    log_name = f"KIS_log_{datetime.now(kst).strftime('%y%m%d')}.log"
    run_tag = f"{Path(__file__).name} 시작 {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}"
    log_manager = LogManager(str(log_dir), log_name=log_name, run_tag=run_tag)

    def _log(msg: str) -> None:
        """화면 + 로그 + 텔레그램 (시작/완료/이상 종료 등 핵심 알림 전용)."""
        out = f"{ts_prefix()} {msg}"
        log_manager.log_tm(out)
        logging.info("%s", out)

    def _log_q(msg: str) -> None:
        """화면 + 로그만 (텔레그램 없음, 진행 단계 상세용)."""
        out = f"{ts_prefix()} {msg}"
        logging.info("%s", out)

    _log("[1m_NXT] 프로그램 시작")

    # 휴일 가드 — 휴장일이면 다운로드 생략하고 종료
    try:
        if is_holiday():
            msg = f"[{Path(__file__).name}(1m NXT data download)] => 휴일로 프로그램을 종료합니다."
            _log(msg)  # _log 가 텔레그램까지 전송 (send_telegram 중복 제거)
            return
    except Exception as e:
        # 휴일 판정 실패 시 다운로드는 진행하되 경고만 남김
        _log(f"[1m_NXT][WARN] 휴일 판정 실패, 다운로드 계속 진행: {e}")

    any_written = False
    summary_rows = []
    now_kst = datetime.now(kst)
    # 프리마켓은 08:00 개시이므로 08시 이후 실행이면 당일을 영업일로 간주(아침 실행 08:51 KST 대응).
    # 08시 이전 실행은 전일을 폴백 영업일로 사용. (API 응답에 거래일이 있으면 그 값이 우선)
    default_biz_date = now_kst.strftime("%Y%m%d") if now_kst.hour >= 8 else (now_kst - timedelta(days=1)).strftime("%Y%m%d")

    session = _resolve_session(now_kst)
    query_times = _build_query_times(session)
    _log_q(f"[1m_NXT] 실행 세션={session} (조회시각 {len(query_times)}개: {query_times[0]}~{query_times[-1]})")

    biz_date = None
    pdy_cache: dict[str, dict[str, float]] = {}

    name_map = {}
    name_to_code = {}
    target_codes = []
    manual_codes = []
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        name_map = dict(zip(sdf["code"], sdf["name"]))
        name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"]))
        # NXT 종목만 필터 (nxt=Y)
        if "nxt" in sdf.columns:
            nxt_df = sdf[sdf["nxt"].astype(str).str.strip().str.upper() == "Y"]
            target_codes = nxt_df["code"].tolist()
            _log_q(f"[1m_NXT] NXT 종목 {len(target_codes)}개 (전체 {len(sdf)}개 중 nxt=Y)")
        else:
            target_codes = sdf["code"].tolist()
            _log_q(f"[1m_NXT] 종목마스터에 nxt 컬럼 없음 → 전체 {len(target_codes)}종목 대상")
    except Exception as e:
        logging.warning("%s 종목마스터 로드 실패: %s", ts_prefix(), e)
        target_codes = TARGET_CODES
        manual_codes = _parse_codes(CODE_TEXT)

    if not manual_codes:
        manual_codes = _parse_codes(CODE_TEXT, name_to_code)
    if manual_codes:
        target_codes = manual_codes
    elif not target_codes:
        logging.warning("%s 대상 종목이 비어 있어 기본 코드 목록을 사용합니다.", ts_prefix())
        target_codes = TARGET_CODES

    total_codes = len(target_codes)
    progress_state = {"next_pct": 10}

    def _report_progress(idx: int) -> None:
        if total_codes <= 0:
            return
        pct = int((idx * 100) / total_codes)
        while pct >= progress_state["next_pct"]:
            _log_q(f"전체 데이터중 {progress_state['next_pct']}%({idx}/{total_codes}건) 수신 완료")
            progress_state["next_pct"] += 10

    out_dir = Path(__file__).resolve().parent / "data" / "1m_data"
    out_dir.mkdir(parents=True, exist_ok=True)
    temp_root = out_dir / "temp_1m_nxt"
    temp_root.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = temp_root / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _checkpoint_path(date_str: str) -> Path:
        # 세션별 분리: 아침(morning) 완료가 저녁(full) 실행을 skip 시키지 않도록
        return checkpoint_dir / f"{date_str}_{session}_nxt_checkpoint.csv"

    def _load_checkpoint(date_str: str) -> set[str]:
        ck_path = _checkpoint_path(date_str)
        done: set[str] = set()
        if not ck_path.exists():
            return done
        try:
            with ck_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if (row.get("date") or "").strip() == date_str and (row.get("status") or "").strip() == "ok":
                        code = (row.get("code") or "").strip()
                        if code:
                            done.add(code)
        except Exception as e:
            logging.warning("%s 체크포인트 로드 실패: %s", ts_prefix(), e)
        return done

    def _append_checkpoint(date_str: str, code: str, rows: int) -> None:
        ck_path = _checkpoint_path(date_str)
        is_new = not ck_path.exists()
        with ck_path.open("a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if is_new:
                writer.writerow(["date", "code", "rows", "status"])
            writer.writerow([date_str, code, rows, "ok"])

    done_codes: set[str] = set()
    if biz_date:
        done_codes = _load_checkpoint(biz_date)

    MAX_CONSECUTIVE_EMPTY = 20  # 연속 빈 응답 한도 (초과 시 API 장애로 판단하여 조기 종료)
    consecutive_empty = 0

    for idx, code in enumerate(target_codes, 1):
        if biz_date and code in done_codes:
            logging.info("%s 체크포인트 skip: %s", ts_prefix(), code)
            _report_progress(idx)
            continue
        code_rows = []
        name = name_map.get(code, "")
        if code not in pdy_cache:
            row = stQueryDB(code)
            pdy_close = float(row.get("pdy_close") or 0)
            p2dy_close = float(row.get("p2dy_close") or 0)
            pdy_ret = (pdy_close / p2dy_close) - 1.0 if p2dy_close > 0 else 0.0
            pdy_cache[code] = {"pdy_close": pdy_close, "pdy_ret": pdy_ret}

        for query_time in query_times:
            try:
                output1, output2 = inquire_time_itemchartprice(
                    env_dv="real",
                    fid_cond_mrkt_div_code=MARKET_CODE,  # NX
                    fid_input_iscd=code,
                    fid_input_hour_1=query_time,
                    fid_pw_data_incu_yn="Y",
                )
            except ValueError as e:
                logging.error("%s 에러 발생(%s): %s", ts_prefix(), code, str(e))
                continue

            if output2.empty:
                continue

            if biz_date is None:
                if "stck_bsop_date" in output2.columns and not output2.empty:
                    biz_date = str(output2["stck_bsop_date"].iloc[0]).strip()
                elif not output1.empty:
                    biz_date = str(output1.get("stck_bsop_date", [""])[0]).strip()

            output2 = output2.copy()
            if "stck_cntg_hour" in output2.columns:
                output2["stck_cntg_hour"] = (
                    pd.to_numeric(output2["stck_cntg_hour"], errors="coerce")
                    .fillna(0).astype(int).astype(str).str.zfill(6)
                )
                # NXT 시간 범위 필터 (프리마켓 + 애프터마켓만)
                output2 = output2[output2["stck_cntg_hour"].apply(_in_nxt_time)]

            if not output1.empty:
                for col in output1.columns:
                    if col not in output2.columns:
                        output2[col] = output1[col].iloc[0]
            output2["code"] = code
            output2["name"] = name

            output2 = output2.rename(columns=COLUMN_MAPPING)
            if "time" in output2.columns:
                output2["time"] = (
                    pd.to_numeric(output2["time"], errors="coerce")
                    .fillna(0).astype(int).astype(str).str.zfill(6)
                )
                output2 = output2[output2["time"].apply(_in_nxt_time)]

            for col in DESIRED_COLS:
                if col not in output2.columns:
                    output2[col] = ""
            pdy_close = pdy_cache.get(code, {}).get("pdy_close", 0.0)
            pdy_ret = pdy_cache.get(code, {}).get("pdy_ret", 0.0)
            if "close" in output2.columns:
                output2["close"] = pd.to_numeric(output2["close"], errors="coerce")
            if pdy_close > 0:
                output2["pdy_close"] = pdy_close
                if "close" in output2.columns:
                    output2["tdy_ret"] = (output2["close"] - pdy_close) / pdy_close
            else:
                output2["tdy_ret"] = None
            output2["pdy_ret"] = pdy_ret
            output2 = output2.reindex(columns=DESIRED_COLS)
            output2 = _drop_empty_columns(output2)

            for col in NUMERIC_COLUMNS:
                if col in output2.columns:
                    output2[col] = pd.to_numeric(output2[col], errors="coerce").round(2)

            code_rows.append(output2)

        if not code_rows:
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                _log(f"[1m_NXT] ⚠ 연속 {MAX_CONSECUTIVE_EMPTY}개 종목 빈 응답 → API 장애로 판단, 조기 종료")
                break
            _report_progress(idx)
            continue

        consecutive_empty = 0  # 데이터 수신 성공 시 리셋
        code_df = pd.concat(code_rows, ignore_index=True)

        if not code_df.empty:
            cur_date = biz_date or default_biz_date
            if not biz_date:
                biz_date = cur_date
                done_codes |= _load_checkpoint(biz_date)
                if code in done_codes:
                    logging.info("%s 체크포인트 skip: %s", ts_prefix(), code)
                    continue
            temp_dir = temp_root / cur_date
            temp_dir.mkdir(parents=True, exist_ok=True)
            temp_path = temp_dir / f"{code}.parquet"
            any_written = True
            code_df = code_df.copy()
            code_df = code_df.rename(
                columns={k: v for k, v in LEGACY_RENAME.items() if k in code_df.columns}
            )
            for col in DESIRED_COLS:
                if col not in code_df.columns:
                    code_df[col] = ""
            code_df = code_df.reindex(columns=DESIRED_COLS)
            for col in ("open", "high", "low", "close", "volume", "acml_value", "pdy_close", "tdy_ret"):
                if col in code_df.columns:
                    code_df[col] = pd.to_numeric(code_df[col], errors="coerce")
            if "pdy_close" in code_df.columns and "close" in code_df.columns:
                valid_pdy = code_df["pdy_close"].fillna(0) > 0
                code_df.loc[valid_pdy, "tdy_ret"] = (
                    (code_df.loc[valid_pdy, "close"] - code_df.loc[valid_pdy, "pdy_close"])
                    / code_df.loc[valid_pdy, "pdy_close"]
                )
            if "pdy_ret" in code_df.columns:
                code_df["pdy_ret"] = pd.to_numeric(code_df["pdy_ret"], errors="coerce")
            code_df.to_parquet(temp_path, index=False)
            _append_checkpoint(cur_date, code, len(code_df))

        summary_rows.append({
            "code": code, "name": name, "rows": len(code_df),
            "first_time": str(code_df["time"].iloc[0]) if "time" in code_df.columns and not code_df.empty else "",
            "last_time": str(code_df["time"].iloc[-1]) if "time" in code_df.columns and not code_df.empty else "",
        })
        logging.info("%s 조회완료: %s %s rows=%d", ts_prefix(), code, name, len(code_df))
        if print_all_rows:
            print(f"\n=== {code} {name} ===")
            print(code_df.to_string(index=False, justify="right"))

        _report_progress(idx)

    if not any_written:
        _log("[1m_NXT] 저장할 데이터가 없습니다. (NXT 거래 없는 날 또는 시간 확인) → 프로그램 종료")
        _log_q("[1m_NXT] 프로그램 종료")
        return

    if not biz_date:
        biz_date = default_biz_date
    temp_dir = temp_root / biz_date
    temp_files = sorted(temp_dir.glob("*.parquet"))
    if not temp_files:
        _log("[1m_NXT] 임시 parquet 파일이 없습니다. → 프로그램 종료")
        _log_q("[1m_NXT] 프로그램 종료")
        return

    _log_q(f"[1m_NXT] 데이터 병합 시작 (임시 파일 {len(temp_files)}개)")
    frames = [pd.read_parquet(p) for p in temp_files]

    # NXT 파일명: _NXT suffix
    parquet_path = out_dir / f"{biz_date}_1m_chart_DB_parquet_NXT.parquet"

    key_cols = ["date", "time", "code"]
    # 기존 파일이 있으면 덮어쓰지 않고 병합한다.
    # (아침 morning 실행으로 받은 프리마켓 + 저녁 full 실행의 애프터마켓을 누적)
    if parquet_path.exists():
        try:
            existing = pd.read_parquet(parquet_path)
            frames.append(existing)
            _log_q(f"[1m_NXT] 기존 NXT parquet 병합 (기존 {len(existing)}건 + 신규 temp {len(temp_files)}개)")
        except Exception as e:
            _log_q(f"[1m_NXT][WARN] 기존 parquet 읽기 실패, 신규 데이터만 저장: {e}")

    merged = pd.concat(frames, ignore_index=True)

    _log_q(f"[1m_NXT] 중복 제거 시작 (기준={','.join(key_cols)})")
    merged["_v_ok"] = merged["volume"].fillna(0) > 0 if "volume" in merged.columns else False
    merged["_a_ok"] = merged["acml_value"].fillna(0) > 0 if "acml_value" in merged.columns else False
    merged["_c_ok"] = merged["close"].fillna(0) > 0 if "close" in merged.columns else False
    merged["_score"] = (
        merged["_v_ok"].astype(int) * 4
        + merged["_a_ok"].astype(int) * 2
        + merged["_c_ok"].astype(int)
    )
    merged = merged.sort_values(
        key_cols + ["_score", "volume", "acml_value"],
        ascending=[True, True, True, False, False, False],
    )
    merged = merged.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)
    merged = merged.drop(columns=["_v_ok", "_a_ok", "_c_ok", "_score"], errors="ignore")

    # volume 합계가 0이면 유효 데이터 없음 (개장 전 더미 데이터 방지)
    total_vol = merged["volume"].fillna(0).sum() if "volume" in merged.columns else 0
    if total_vol == 0:
        _log(f"[1m_NXT] ⚠ 전체 volume 합계가 0 → 유효 데이터 없음. 저장 스킵 (biz_date={biz_date}) → 프로그램 종료 (개장 전 또는 NXT 거래 없는 날)")
        _log_q("[1m_NXT] 프로그램 종료 (개장 전 또는 NXT 거래 없는 날)")
        return

    _log_q(f"[1m_NXT] parquet 저장 (총 {len(merged)}건)")
    merged.to_parquet(parquet_path, index=False)

    if summary_rows:
        data_rows = [r for r in summary_rows if r["rows"] > 0]
        if data_rows:
            logging.info("%s NXT 종목별 요약 (%d종목 데이터 있음):", ts_prefix(), len(data_rows))
            print_table(
                data_rows,
                columns=["code", "name", "rows", "first_time", "last_time"],
                align={"rows": "right"},
            )
            sys.stdout.flush()

    time.sleep(0.2)
    logging.info("%s 저장 완료: %s", ts_prefix(), parquet_path)
    _log_q(f"[1m_NXT] 저장 완료: {parquet_path.name}")

    # ── S3 업로드 (1m_nxt 경로) ──
    try:
        layout = load_kis_data_layout()
        # 파일명에서 날짜 추출 (20260402_1m_chart_DB_parquet_NXT.parquet → 2026-04-02)
        file_date = parquet_path.name[:8]  # "20260402"
        date_str = f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:8]}"
        s3_url = layout.s3_1m_date(date_str).replace("/1m/", "/1m_nxt/")
        s3_parts = s3_url.replace("s3://", "").split("/", 1)
        s3_bucket, s3_key = s3_parts[0], s3_parts[1]
        s3 = boto3.client("s3")
        s3.upload_file(str(parquet_path), s3_bucket, s3_key)
        logging.info("%s [S3] uploaded %s -> %s", ts_prefix(), parquet_path.name, s3_url)
        _log_q(f"[1m_NXT] S3 업로드 완료: {s3_url}")
    except Exception as e:
        logging.error("%s [S3] 업로드 실패: %s", ts_prefix(), e)

    # ── EC2 로컬 NXT 파일 정리 (최근 2일치만 유지) ──
    try:
        data_dir = Path("data/1m_data")
        if data_dir.exists():
            nxt_files = sorted(data_dir.glob("*_1m_chart_DB_parquet_NXT.parquet"))
            dated_files = []
            for f in nxt_files:
                try:
                    d = datetime.strptime(f.name[:8], "%Y%m%d")
                    dated_files.append((d, f))
                except ValueError:
                    continue
            if len(dated_files) > 2:
                dated_files.sort(key=lambda x: x[0], reverse=True)
                for _, old_file in dated_files[2:]:
                    old_file.unlink()
                    logging.info("%s [cleanup] 삭제: %s", ts_prefix(), old_file.name)
    except Exception as e:
        logging.error("%s [cleanup] 로컬 정리 실패: %s", ts_prefix(), e)

    # ── temp 정리 ──
    try:
        if temp_dir.exists():
            for f in temp_dir.glob("*.parquet"):
                f.unlink()
            temp_dir.rmdir()
            _log_q(f"[1m_NXT] temp 정리 완료: {temp_dir}")
    except Exception as e:
        logging.warning("%s [cleanup] temp 정리 실패: %s", ts_prefix(), e)

    _log(f"[1m_NXT] 다운로드 완료 (세션={session}, 총 {len(merged)}건) → 프로그램 종료")


if __name__ == "__main__":
    main()
