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

실행 (20:00 이후):
    nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/kis_1m_API_to_Parquet_NXT.py > /home/ubuntu/Stoc_Kis/out/1m_nxt_parquet.log 2>&1 &

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
from kis_utils import LogManager, load_kis_data_layout, load_symbol_master, print_table, stQueryDB
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
QUERY_TIMES = []
# 프리마켓 (30분 간격: 08:30, 08:50 → 2회로 50분 커버)
for t in ["083000", "085000"]:
    QUERY_TIMES.append(t)
# 애프터마켓 (30분 간격 + 20:00 보장)
_cur = datetime.strptime("161000", "%H%M%S")
_after_end = datetime.strptime("200000", "%H%M%S")
while _cur <= _after_end:
    QUERY_TIMES.append(_cur.strftime("%H%M%S"))
    _cur += timedelta(minutes=30)
if "200000" not in QUERY_TIMES:
    QUERY_TIMES.append("200000")

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
        out = f"{ts_prefix()} {msg}"
        log_manager.log_tm(out)
        logging.info("%s", out)

    _log("[1m_NXT] 프로그램 시작")

    any_written = False
    summary_rows = []
    now_kst = datetime.now(kst)
    default_biz_date = now_kst.strftime("%Y%m%d") if now_kst.hour >= 9 else (now_kst - timedelta(days=1)).strftime("%Y%m%d")
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
            _log(f"[1m_NXT] NXT 종목 {len(target_codes)}개 (전체 {len(sdf)}개 중 nxt=Y)")
        else:
            target_codes = sdf["code"].tolist()
            _log(f"[1m_NXT] 종목마스터에 nxt 컬럼 없음 → 전체 {len(target_codes)}종목 대상")
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
            _log(f"전체 데이터중 {progress_state['next_pct']}%({idx}/{total_codes}건) 수신 완료")
            progress_state["next_pct"] += 10

    out_dir = Path(__file__).resolve().parent / "data" / "1m_data"
    out_dir.mkdir(parents=True, exist_ok=True)
    temp_root = out_dir / "temp_1m_nxt"
    temp_root.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = temp_root / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _checkpoint_path(date_str: str) -> Path:
        return checkpoint_dir / f"{date_str}_nxt_checkpoint.csv"

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

        for query_time in QUERY_TIMES:
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
            _report_progress(idx)
            continue

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
        _log("[1m_NXT] 저장할 데이터가 없습니다. (NXT 거래 없는 날 또는 시간 확인)")
        _log("[1m_NXT] 프로그램 종료")
        return

    if not biz_date:
        biz_date = default_biz_date
    temp_dir = temp_root / biz_date
    temp_files = sorted(temp_dir.glob("*.parquet"))
    if not temp_files:
        _log("[1m_NXT] 임시 parquet 파일이 없습니다.")
        _log("[1m_NXT] 프로그램 종료")
        return

    _log(f"[1m_NXT] 데이터 병합 시작 (임시 파일 {len(temp_files)}개)")
    merged = pd.concat([pd.read_parquet(p) for p in temp_files], ignore_index=True)

    # NXT 파일명: _NXT suffix
    parquet_path = out_dir / f"{biz_date}_1m_chart_DB_parquet_NXT.parquet"

    key_cols = ["date", "time", "code"]
    if parquet_path.exists():
        _log("[1m_NXT] 기존 NXT parquet 발견 -> 덮어쓰기 모드")

    _log(f"[1m_NXT] 중복 제거 시작 (기준={','.join(key_cols)})")
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

    _log(f"[1m_NXT] parquet 저장 (총 {len(merged)}건)")
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
    _log(f"[1m_NXT] 저장 완료: {parquet_path.name}")

    # ── S3 업로드 (1m_nxt 경로) ──
    try:
        layout = load_kis_data_layout()
        date_str = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")
        # 기존 1m 경로에서 1m_nxt로 변경
        s3_url = layout.s3_1m_date(date_str).replace("/1m/", "/1m_nxt/")
        s3_parts = s3_url.replace("s3://", "").split("/", 1)
        s3_bucket, s3_key = s3_parts[0], s3_parts[1]
        s3 = boto3.client("s3")
        s3.upload_file(str(parquet_path), s3_bucket, s3_key)
        logging.info("%s [S3] uploaded %s -> %s", ts_prefix(), parquet_path.name, s3_url)
        _log(f"[1m_NXT] S3 업로드 완료: {s3_url}")
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
            _log(f"[1m_NXT] temp 정리 완료: {temp_dir}")
    except Exception as e:
        logging.warning("%s [cleanup] temp 정리 실패: %s", ts_prefix(), e)

    _log("[1m_NXT] 프로그램 종료")


if __name__ == "__main__":
    main()
