"""
Created on 20250601 
"""

import sys
import logging
import time
import csv
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd

sys.path.extend(['../..', '.'])
import kis_auth as ka
from kis_utils import LogManager, load_kis_data_layout, load_symbol_master, print_table, stQueryDB
from inquire_time_itemchartprice import inquire_time_itemchartprice

# 로깅 설정
logging.basicConfig(level=logging.INFO)

"""
[주식당일분봉조회] API를 호출하여 당일 모든 종목에 대한 1m 데이터를 다운받아 parquet 파일로 저장합니다.
(KOSPI,KOSDAQ 모든 ST 종목들에 대해 API를 호출)

parquet/csv 파일로 저장, 파케이트는 날짜별로 자료를 저장 
{biz_date}_1m_chart_DB_parquet.parquet
=============================================

    nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/kis_1m_API_to_Parquet_all_code.py > /home/ubuntu/Stoc_Kis/out/1m_api_parquet.log 2>&1 &

로그 실시간으로 보려면(마지막 10줄 이후 계속 따라 보기):
    tail -f /home/ubuntu/Stoc_Kis/out/1m_api_parquet.log

로그 실시간으로 보기(마지막 1000줄 이후 계속 따라 보기):
    tail -n 1000 -f /home/ubuntu/Stoc_Kis/out/1m_api_parquet.log

스크롤하면서 보려면:
less +F /home/ubuntu/Stoc_Kis/out/1m_api_parquet.log
=============================================
프로세스 확인 및 종료(KILL) 방법
1) 현재 프로세스 조회(모든 PID 표시)
    pgrep -af kis_1m_API_to_Parquet_all_code.py
2) 관련 프로세스 모두 종료
    pkill -f kis_1m_API_to_Parquet_all_code.py
(강제 종료가 필요하면)
    pkill -9 -f kis_1m_API_to_Parquet_all_code.py
3) 모두 종료되었는지 확인
    pgrep -af kis_1m_API_to_Parquet_all_code.py || echo "no process"

"""
# CODE_TEXT에 종목코드/종목명을 넣으면 해당 종목만 조회합니다.
# 비워두면(빈 문자열/공백만) 전체 종목을 조회합니다.
CODE_TEXT = """
"""

LOG_ID = "_1m"  # 로그/텔레그램 메시지 식별용


def ts_prefix() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).strftime(f"[%y%m%d_%H%M%S{LOG_ID}]")


QUERY_TIMES = []
_start = datetime.strptime("092900", "%H%M%S")
_end = datetime.strptime("153000", "%H%M%S")
_cur = _start
while _cur <= _end:
    QUERY_TIMES.append(_cur.strftime("%H%M%S"))
    _cur += timedelta(minutes=30)


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
    "date",
    "time",
    "code",
    "name",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "acml_volume",
    "acml_value",
    "tdy_ret",
    "pdy_close",
    "pdy_ret",
]

LEGACY_RENAME = {
    "영업일자": "date",
    "체결시간": "time",
    "시가": "open",
    "최고가": "high",
    "최저가": "low",
    "종가": "close",
    "거래량": "volume",
    "누적 거래대금": "acml_value",
    "누적거래대금": "acml_value",
    "누적거래량": "acml_volume",
    "전일대비율": "tdy_ret",
    "전일종가": "pdy_close",
}

NUMERIC_COLUMNS = []
print_all_rows = 0  # 1이면 전체행 정렬 출력, 0이면 print(df)


def main():
    """
    주식당일분봉조회 테스트 함수
    
    이 함수는 주식당일분봉조회 API를 호출하여 결과를 출력합니다.
    테스트 데이터로 삼성전자(005930)를 사용합니다.
    
    Returns:
        None
    """

    # pandas 출력 옵션 설정
    pd.set_option('display.max_columns', None)  # 모든 컬럼 표시
    pd.set_option('display.width', None)  # 출력 너비 제한 해제
    pd.set_option('display.max_rows', None)  # 모든 행 표시

    # kis_auth는 _url_fetch 내부에서 토큰을 자동으로 발급/캐시합니다.
    kst = timezone(timedelta(hours=9))
    layout = load_kis_data_layout()
    log_dir = Path(layout.local_root) / "logs"
    log_name = f"KIS_log_{datetime.now(kst).strftime('%y%m%d')}.log"
    run_tag = f"{Path(__file__).name} 시작 {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}"
    log_manager = LogManager(str(log_dir), log_name=log_name, run_tag=run_tag)

    def _log_tm_and_info(msg: str) -> None:
        out = f"{ts_prefix()} {msg}"
        log_manager.log_tm(out)
        logging.info("%s", out)

    _log_tm_and_info("[1m_ohlcv_All_code_download] 프로그램 시작")

    any_written = False
    summary_rows = []
    now_kst = datetime.now(kst)
    if now_kst.hour < 9:
        default_biz_date = (now_kst - timedelta(days=1)).strftime("%Y%m%d")
    else:
        default_biz_date = now_kst.strftime("%Y%m%d")
    biz_date = None
    pdy_cache: dict[str, dict[str, float]] = {}

    name_map = {}
    name_to_code = {}
    target_codes = []
    manual_codes = []
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        if "group" in sdf.columns:
            sdf = sdf[sdf["group"] == "ST"]
        else:
            logging.warning("%s 종목마스터에 group 컬럼이 없어 전체 종목으로 진행합니다.", ts_prefix())
        name_map = dict(zip(sdf["code"], sdf["name"]))
        name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"]))
        target_codes = sdf["code"].tolist()
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
            _log_tm_and_info(
                f"전체 데이터중 {progress_state['next_pct']}%({idx}/{total_codes}건) 수신 완료"
            )
            progress_state["next_pct"] += 10

    out_dir = Path(__file__).resolve().parent / "data" / "1m_data"
    out_dir.mkdir(parents=True, exist_ok=True)
    temp_root = out_dir / "temp_1m"
    temp_root.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = temp_root / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _checkpoint_path(date_str: str) -> Path:
        return checkpoint_dir / f"{date_str}_checkpoint.csv"

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
            if p2dy_close > 0:
                pdy_ret = (pdy_close / p2dy_close) - 1.0
            else:
                pdy_ret = 0.0
            pdy_cache[code] = {"pdy_close": pdy_close, "pdy_ret": pdy_ret}
        for query_time in QUERY_TIMES:
            try:
                output1, output2 = inquire_time_itemchartprice(
                    env_dv="real",
                    fid_cond_mrkt_div_code="J",
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
                    .fillna(0)
                    .astype(int)
                    .astype(str)
                    .str.zfill(6)
                )
                output2 = output2[output2["stck_cntg_hour"] <= "153000"]
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
                    .fillna(0)
                    .astype(int)
                    .astype(str)
                    .str.zfill(6)
                )
                output2 = output2[output2["time"] <= "153000"]
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
            logging.warning("%s 데이터 없음: %s", ts_prefix(), code)
            _report_progress(idx)
            continue

        code_df = pd.concat(code_rows, ignore_index=True)
        if "time" in code_df.columns:
            time_norm = (
                pd.to_numeric(code_df["time"], errors="coerce")
                .fillna(0)
                .astype(int)
                .astype(str)
                .str.zfill(6)
            )
        else:
            time_norm = pd.Series([], dtype=str)
        missing_1530 = not (time_norm == "153000").any()
        if missing_1530:
            try:
                o1, o2 = inquire_time_itemchartprice(
                    env_dv="real",
                    fid_cond_mrkt_div_code="J",
                    fid_input_iscd=code,
                    fid_input_hour_1="153000",
                    fid_pw_data_incu_yn="Y",
                )
                if not o2.empty:
                    o2 = o2.rename(columns=COLUMN_MAPPING)
                    if "time" in o2.columns:
                        o2["time"] = (
                            pd.to_numeric(o2["time"], errors="coerce")
                            .fillna(0)
                            .astype(int)
                            .astype(str)
                            .str.zfill(6)
                        )
                        o2 = o2[o2["time"] == "153000"]
                    if not o2.empty:
                        o2 = o2.tail(1)
                        o2["code"] = code
                        o2["name"] = name
                        if "close" in o2.columns:
                            o2["close"] = pd.to_numeric(o2["close"], errors="coerce")
                        pdy_close = pdy_cache.get(code, {}).get("pdy_close", 0.0)
                        pdy_ret = pdy_cache.get(code, {}).get("pdy_ret", 0.0)
                        if pdy_close > 0:
                            o2["pdy_close"] = pdy_close
                            if "close" in o2.columns:
                                o2["tdy_ret"] = (o2["close"] - pdy_close) / pdy_close
                        else:
                            o2["tdy_ret"] = None
                        o2["pdy_ret"] = pdy_ret
                        for col in DESIRED_COLS:
                            if col not in o2.columns:
                                o2[col] = ""
                        o2 = o2.reindex(columns=DESIRED_COLS)
                        o2 = _drop_empty_columns(o2)
                        code_df = pd.concat([code_df, o2], ignore_index=True)
                        time_norm = (
                            pd.to_numeric(code_df["time"], errors="coerce")
                            .fillna(0)
                            .astype(int)
                            .astype(str)
                            .str.zfill(6)
                        )
            except Exception as e:
                logging.info("%s [1m_ohlcv][주의] %s 15:30 재조회 실패: %s", ts_prefix(), code, e)

        if missing_1530 and not (time_norm == "153000").any():
            msg = f"{ts_prefix()} [1m_ohlcv][주의] {code} {name} 15:30 데이터 없음 -> 재조회 시도"
            log_manager.log(msg)
            logging.info("%s", msg)
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
            for col in (
                "open",
                "high",
                "low",
                "close",
                "volume",
                "acml_value",
                "pdy_close",
                "tdy_ret",
            ):
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
        summary_rows.append(
            {
                "code": code,
                "name": name,
                "rows": len(code_df),
                "first_time": str(code_df["time"].iloc[0]) if "time" in code_df.columns else "",
                "last_time": str(code_df["time"].iloc[-1]) if "time" in code_df.columns else "",
            }
        )
        logging.info("%s 조회완료: %s %s rows=%d", ts_prefix(), code, name, len(code_df))
        print(f"\n=== {code} {name} ===")
        if print_all_rows:
            pd.set_option("display.colheader_justify", "right")
            print(code_df.to_string(index=False, justify="right"))
        else:
            print(code_df)

        _report_progress(idx)

    if not any_written:
        logging.error("%s 저장할 데이터가 없습니다. (거래일/시간/종목 거래유무 확인)", ts_prefix())
        _log_tm_and_info("[1m_ohlcv] 저장할 데이터가 없습니다.")
        _log_tm_and_info("[1m_ohlcv_All_code_download] 프로그램 종료")
        return

    if not biz_date:
        biz_date = default_biz_date
    temp_dir = temp_root / biz_date
    temp_files = sorted(temp_dir.glob("*.parquet"))
    if not temp_files:
        logging.error("%s 임시 parquet 파일이 없습니다. (temp_1m 확인)", ts_prefix())
        _log_tm_and_info("[1m_ohlcv] 임시 parquet 파일이 없습니다.")
        _log_tm_and_info("[1m_ohlcv_All_code_download] 프로그램 종료")
        return
    _log_tm_and_info(f"[1m_ohlcv] 데이터 병합 시작 (임시 파일 {len(temp_files)}개)")
    merged = pd.concat([pd.read_parquet(p) for p in temp_files], ignore_index=True)

    parquet_path = out_dir / f"{biz_date}_1m_chart_DB_parquet.parquet"

    key_cols = ["date", "time", "code"]
    if parquet_path.exists():
        _log_tm_and_info(f"[1m_ohlcv] 기존 parquet 발견 -> 덮어쓰기 모드")

    _log_tm_and_info(f"[1m_ohlcv] 중복 제거 시작 (기준={','.join(key_cols)})")
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

    _log_tm_and_info(f"[1m_ohlcv] parquet 저장 시작 (총 {len(merged)}건)")
    merged.to_parquet(parquet_path, index=False)

    if summary_rows:
        logging.info("%s 종목별 요약:", ts_prefix())
        print_table(
            summary_rows,
            columns=["code", "name", "rows", "first_time", "last_time"],
            align={"rows": "right"},
        )
        sys.stdout.flush()

    # 요약 출력이 완전히 끝난 뒤 로그를 남기도록 순서 보장
    time.sleep(0.2)
    logging.info("%s 저장 완료: %s", ts_prefix(), parquet_path)
    _log_tm_and_info(f"[1m_ohlcv] 저장 완료: {parquet_path.name}")
    _log_tm_and_info("[1m_ohlcv_All_code_download] 프로그램 종료")


if __name__ == "__main__":
    main()