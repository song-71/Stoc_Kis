"""
- kis_kospi_kosdaq_ohlcv_value.py에서 명칭을 수정(kis_ohlcv_Daily_fetch_manager.py)
 -> 1d, 1m 모두 지원하도록 구성, 단, 아직 1m은 테스트 중..
(1d는 완벽하게 다운 및 날짜별 변환하여 s3에 업로드, 1m은 별도 프로그램으로 처리)

==> 매일 1d 데이터틀 다운받아 s3에 업로드 후, 당일자 데이터만 별도로 추출하여 parquet 파일로 저장(kis_1d_unified_parquet_DB.parquet)

< 프로세스 흐름 >
1. 임시 폴더 정리 : temp, temp_out 디렉터리 삭제 후 재생성 준비
2. 종목 마스터 로드 : 시장(KOSPI/KOSDAQ), 그룹(ST) 필터링   - kis_KRX_code_fetch 프로그램으로 종목코드 다운로드
3. 종목별 데이터 다운로드 (1d)
  - kis_API_ohlcv_download 호출, 다운로드 
  - 표준화(standardize_1d_df) 후 병합/중복 제거
  - 종목코드 단위별로 parquet 파일 저장 (temp 폴더)
4. 종목별 코드 parquet 파일 => 날짜별 parquet 파일 변환, 저장 (temp_out 폴더)
5. temp_out 폴더의 날짜별 parquet 파일을 s3에 업로드 => temp, temp_out 내 파일 삭제
5. S3에 날짜별로 저장한 parquet 파일을 조회하여 EC2에 parquetDB 파일 생성
  -  kis_1d_unified_parquet.py를 실행 -> 이 스크립트가 S3에 쌓인 1d 데이터를 기간 범위로 읽어 하나의 파일로 병합해
     kis_1d_unified_parquet_DB.parquet을 생성하여 EC2의 Stoc_Kis/data/1d_data/ 폴더에 저장합니다.
     (즉, kis_1d_unified_parquet_DB.parquet는 최근 자주 사용하는 데이터만 ec2 로컬에 저장한 작은 DB파일임.)

백그라운드 실행 명령어:
nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/kis_1d_Daily_ohlcv_fetch_manager.py > /home/ubuntu/Stoc_Kis/out/1d_api_parquet.log 2>&1 &

로그 확인:  아래에서 날짜 바꿔 실행 필요
tail -f /home/ubuntu/Stoc_Kis/data/logs/KIS_log_YYMMDD.log


프로세스 확인:
pgrep -af kis_1d_Daily_ohlcv_fetch_manager.py

프로세스 확인 및 종료(KILL) 방법
1) 현재 프로세스 조회(모든 PID 표시)
    pgrep -af kis_1d_Daily_ohlcv_fetch_manager.py
2) 관련 프로세스 모두 종료
    pkill -f kis_1d_Daily_ohlcv_fetch_manager.py
(강제 종료가 필요하면)
    pkill -9 -f kis_1d_Daily_ohlcv_fetch_manager.py
3) 모두 종료되었는지 확인
    pgrep -af kis_1d_Daily_ohlcv_fetch_manager.py || echo "no process"


"""
DEFAULT_USE_CSV = False  # True면 기본 CSV로 파이프라인 실행
DEFAULT_USE_CSV_PATH = "/home/ubuntu/Stoc_Kis/ohlcv_output.csv"


import argparse
import csv
import os
import time
import shutil
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, List

import pandas as pd

from kis_utils import (
    KisDataLayout,
    LogManager,
    load_checkpoints,
    load_config,
    load_kis_data_layout,
    load_symbol_master,
    send_telegram,
)
from fetch_daily_KRX_code import main as build_symbol_master
from kis_API_ohlcv_download_Utils import (
    KisClient,
    KisConfig,
    run_compact_upload_cleanup,
    standardize_1d_df,
    standardize_1m_df,
)


MARKET_DIV_MAP = {
    "kospi": "J",
    "kosdaq": "J",
}

PAGE_LIMIT = 100
PAGE_LIMIT_1M = 120

LOG_ID = "_1d"  # 로그/텔레그램 메시지 식별용


def ts_prefix() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).strftime(f"[%y%m%d_%H%M%S{LOG_ID}]")


def parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def read_codes_from_csv(
    path: str,
    group_col: str | None = None,
    group_value: str | None = None,
) -> List[str]:
    codes: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
        if not rows:
            return codes
        header = rows[0]
        start_idx = 1 if header and "단축코드" in header[0] else 0
        group_idx = None
        if group_col and header and group_col in header:
            group_idx = header.index(group_col)
        for row in rows[start_idx:]:
            if not row:
                continue
            if group_idx is not None and group_value is not None:
                if group_idx >= len(row) or (row[group_idx] or "").strip() != group_value:
                    continue
            code = (row[0] or "").strip()
            if code:
                codes.append(code)
    return codes


def fetch_market_ohlcv(
    client: KisClient,
    codes: Iterable[str],
    start: str,
    end: str,
    sleep_sec: float,
    limit: int | None = None,
    debug_cols: bool = False,
    layout: KisDataLayout | None = None,
    freq: str = "1d",
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    codes_list = list(codes)
    total = len(codes_list)
    if limit:
        total = min(total, limit)
    send_telegram(f"{ts_prefix()} [KIS][download] start freq={freq} range={start}~{end} total={total}")
    start_dt = parse_yyyymmdd(start)
    end_dt = parse_yyyymmdd(end)
    logged_cols = False
    failed_codes: List[str] = []
    consecutive_failures = 0
    for idx, code in enumerate(codes_list, 1):
        if limit and idx > limit:
            break
        progress_tag = f"[{idx}/{total}]"
        cur_end = end_dt
        cur_dt = datetime.combine(end_dt, datetime.min.time())
        cur_time = "153000"
        merged = pd.DataFrame()
        req_count = 0
        if freq == "1m":
            print(f"{ts_prefix()} {progress_tag} code={code} 1m start range={start}~{end}")
        while cur_end >= start_dt:
            try:
                if freq == "1d":
                    raw = client.inquire_daily_itemchartprice(
                        code=code,
                        start_date=start,
                        end_date=cur_end.strftime("%Y%m%d"),
                        period="D",
                    )
                else:
                    raw = client.inquire_time_dailychartprice(
                        code=code,
                        input_date=cur_dt.strftime("%Y%m%d"),
                        input_hour=cur_time,
                    )
            except Exception as e:
                print(f"{ts_prefix()} {progress_tag} code={code} ERROR: {e}")
                failed_codes.append(code)
                merged = pd.DataFrame()
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    print(f"{ts_prefix()} [abort] 연속 실패 {consecutive_failures}회 발생, 중단합니다.")
                    raise SystemExit(1)
                break

            if raw.empty:
                if freq == "1m":
                    print(f"{ts_prefix()} {progress_tag} code={code} 1m no data date={cur_dt.strftime('%Y%m%d')}")
                break
            consecutive_failures = 0

            if debug_cols and not logged_cols:
                print(f"{ts_prefix()} [debug] raw columns: {list(raw.columns)}")
                logged_cols = True

            std = standardize_1d_df(raw, symbol=code) if freq == "1d" else standardize_1m_df(raw, symbol=code)
            if std.empty:
                break

            merged = pd.concat([merged, std], ignore_index=True) if not merged.empty else std
            merged = merged.drop_duplicates(subset=["date", "symbol"], keep="last")

            if freq == "1d":
                if len(raw) < PAGE_LIMIT:
                    break
            else:
                if len(raw) < PAGE_LIMIT_1M:
                    break

            if freq == "1d":
                oldest_date = merged["date"].min()
                if not oldest_date:
                    break
                oldest_dt = datetime.strptime(str(oldest_date), "%Y-%m-%d").date()
                next_end = oldest_dt - timedelta(days=1)
                if next_end >= cur_end:
                    break
                cur_end = next_end
            else:
                oldest_ts = merged["ts"].min()
                if not oldest_ts:
                    break
                oldest_dt = datetime.strptime(str(oldest_ts), "%Y-%m-%d %H:%M:%S")
                next_dt = oldest_dt - timedelta(minutes=1)
                if next_dt.date() < start_dt:
                    break
                cur_dt = next_dt
                cur_time = next_dt.strftime("%H%M%S")
                req_count += 1
                if req_count % 10 == 0:
                    print(
                        f"{ts_prefix()} {progress_tag} code={code} 1m progress req={req_count} rows={len(merged)} cur_dt={cur_dt.strftime('%Y%m%d %H%M')}"
                    )
            time.sleep(sleep_sec)

        rows = len(merged)
        if not merged.empty:
            if freq == "1d":
                merged = merged.sort_values(["date", "symbol"]).reset_index(drop=True)
            else:
                merged = merged.sort_values(["date", "ts", "symbol"]).reset_index(drop=True)
            print(f"{ts_prefix()} {progress_tag} code={code} range={start}~{end}: {rows}")
            if layout is not None:
                temp_dir = (
                    layout.local_temp_by_symbol_1d() if freq == "1d" else layout.local_temp_by_symbol_1m()
                )
                os.makedirs(temp_dir, exist_ok=True)
                temp_path = os.path.join(temp_dir, f"{code}.parquet")
                merged.to_parquet(temp_path, index=False)
            frames.append(merged)
        # 텔레그램 진행 메시지는 시장 단위 시작/종료에서만 전송
        time.sleep(sleep_sec)

    if failed_codes and layout is not None:
        log_dir = os.path.join(layout.local_root, "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "failed_symbols.txt")
        with open(log_path, "a", encoding="utf-8") as f:
            for code in failed_codes:
                f.write(f"{code}\n")
        print(f"{ts_prefix()} [warn] failed symbols logged: {log_path}")

    if not frames:
        cols = ["date", "symbol", "open", "high", "low", "close", "volume", "value"]
        if freq == "1m":
            cols = ["date", "ts", "symbol", "open", "high", "low", "close", "volume", "value"]
        return pd.DataFrame(columns=cols)
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="KOSPI/KOSDAQ OHLCV + value 조회")
    ap.add_argument("--market", default="all", choices=["kospi", "kosdaq", "all"])
    ap.add_argument("--freq", default="1d", choices=["1d", "1m"], help="다운로드 주기")
    ap.add_argument("--start", default="20260101", help="YYYYMMDD")
    # ap.add_argument("--end", default="20260119", help="YYYYMMDD")
    ap.add_argument("--end", default=None, help="YYYYMMDD (기본: 오늘)")
    ap.add_argument("--limit", type=int, default=None, help="종목 제한(테스트용)/무제한: 10/None")
    ap.add_argument("--sleep_sec", type=float, default=0.2, help="API 호출 딜레이(초)")
    ap.add_argument("--debug_cols", action="store_true", help="첫 응답 컬럼 목록 출력")
    ap.add_argument("--out_csv", default=None, help="결과 CSV 저장 경로")
    ap.add_argument("--use_csv", action="store_true", help="API 대신 기본 CSV로 파이프라인만 실행")
    ap.add_argument("--config", default="config.json", help="설정 파일 경로")
    args = ap.parse_args()

    config_path = args.config
    if not os.path.isabs(config_path) and not os.path.exists(config_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, config_path)
    config = load_config(config_path)
    layout = load_kis_data_layout(config_path)
    kst = timezone(timedelta(hours=9))
    log_dir = os.path.join(layout.local_root, "logs")
    log_name = f"KIS_log_{datetime.now(kst).strftime('%y%m%d')}.log"
    run_tag = f"{os.path.basename(__file__)} 시작 {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}"
    log_manager = LogManager(log_dir, log_name=log_name, run_tag=run_tag)
    log_manager.log_tm(f"{ts_prefix()} [KIS][start] {os.path.basename(__file__)} 실행")

    appkey = config.get("appkey")
    appsecret = config.get("appsecret")
    if not appkey or not appsecret:
        raise ValueError("appkey/appsecret이 필요합니다. config.json 또는 환경변수를 설정하세요.")

    start_arg = args.start or config.get("start")
    end_arg = args.end or date.today().strftime("%Y%m%d")
    if not start_arg or not end_arg:
        raise ValueError("start/end가 필요합니다. --start/--end 또는 config.json에 start/end를 설정하세요.")

    if args.freq == "1m":
        try:
            end_dt = parse_yyyymmdd(end_arg)
            if end_dt.weekday() >= 5:
                adj = end_dt
                while adj.weekday() >= 5:
                    adj -= timedelta(days=1)
                print(f"{ts_prefix()} [1m] end date weekend -> adjusted {end_arg} -> {adj.strftime('%Y%m%d')}")
                end_arg = adj.strftime("%Y%m%d")
        except Exception:
            pass

    checkpoints = load_checkpoints(config_path)
    latest_cp = checkpoints.get("market_data_1d_latest" if args.freq == "1d" else "market_data_1m_latest")
    if latest_cp:
        try:
            start_dt = parse_yyyymmdd(start_arg)
            end_dt = parse_yyyymmdd(end_arg)
            cp_dt = datetime.strptime(latest_cp, "%Y-%m-%d").date()
            if start_dt <= cp_dt <= end_dt:
                start_arg = cp_dt.strftime("%Y%m%d")
                print(f"{ts_prefix()} [checkpoint] start adjusted -> {start_arg}")
        except Exception:
            pass

    temp_root = os.path.join(layout.local_root, "temp")
    out_root = os.path.join(layout.local_root, "temp_out")
    for path in (temp_root, out_root):
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"{ts_prefix()} [clean] removed: {path}")

    def _write_temp_from_df(df: pd.DataFrame) -> None:
        if df.empty:
            return
        temp_dir = (
            layout.local_temp_by_symbol_1d() if args.freq == "1d" else layout.local_temp_by_symbol_1m()
        )
        os.makedirs(temp_dir, exist_ok=True)
        for code, group in df.groupby("symbol", dropna=False):
            code_str = str(code)
            if args.freq == "1d":
                group = group.sort_values(["date", "symbol"]).reset_index(drop=True)
            else:
                group = group.sort_values(["date", "ts", "symbol"]).reset_index(drop=True)
            temp_path = os.path.join(temp_dir, f"{code_str}.parquet")
            group.to_parquet(temp_path, index=False)

    use_csv_path = DEFAULT_USE_CSV_PATH if (args.use_csv or DEFAULT_USE_CSV) else ""
    if use_csv_path:
        csv_path = use_csv_path
        if not os.path.isabs(csv_path):
            csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), csv_path)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV 파일이 없습니다: {csv_path}")
        combined = pd.read_csv(csv_path)
        _write_temp_from_df(combined)
        results = [combined]
        print(f"{ts_prefix()} [csv] loaded: {csv_path} rows={len(combined)}")
    else:
        send_telegram(f"{ts_prefix()} [KIS][start] OHLCV download freq={args.freq} range={start_arg}~{end_arg} market={args.market}")
        log_manager.log_tm(
            f"{ts_prefix()} [KIS][download] 모든 KRX 종목 {args.freq} 데이터 다운로드 시작 "
            f"market={args.market} range={start_arg}~{end_arg}"
        )

        try:
            symbol_df = load_symbol_master(config_path)
        except FileNotFoundError:
            print(f"{ts_prefix()} [symbol_master] 없음 -> 생성 시도")
            build_symbol_master()
            symbol_df = load_symbol_master(config_path)

        results = []

    if not use_csv_path:
        codes = (
            symbol_df[symbol_df["group"] == "ST"]["code"]
            .astype(str)
            .tolist()
        )
        kis_cfg = KisConfig(appkey=appkey, appsecret=appsecret, market_div=MARKET_DIV_MAP["kospi"])
        kis = KisClient(kis_cfg)
        df = fetch_market_ohlcv(
            kis,
            codes=codes,
            start=start_arg,
            end=end_arg,
            sleep_sec=args.sleep_sec,
            limit=args.limit,
            debug_cols=args.debug_cols,
            layout=layout,
            freq=args.freq,
        )
        if not df.empty:
            df["market"] = "KRX"
            results.append(df)
        send_telegram(f"{ts_prefix()} [KIS][done] KRX download complete count={len(codes)} freq={args.freq}")

    if not results:
        print(f"{ts_prefix()} 조회 결과가 없습니다.")
        log_manager.log_tm(f"{ts_prefix()} [KIS][done] 조회 결과가 없습니다.")
        return

    combined = pd.concat(results, ignore_index=True)
    combined = combined.sort_values(["date", "market", "symbol"]).reset_index(drop=True)
    print(combined)

    if args.out_csv:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        out_csv = args.out_csv
        if not os.path.isabs(out_csv):
            out_csv = os.path.join(base_dir, out_csv)
        combined.to_csv(out_csv, index=False, encoding="utf-8-sig")
        print(f"{ts_prefix()} CSV 저장 완료: {out_csv}")
    else:
        print(f"{ts_prefix()} CSV 저장 생략 (--out_csv 지정 시 저장)")

    print(f"{ts_prefix()} [pipeline] TEMP->OUT->S3 파이프라인 실행")
    try:
        run_compact_upload_cleanup(config_path, freq=args.freq)
    except Exception as e:
        print(f"{ts_prefix()} [pipeline] 실패: {e}")
        raise
    log_manager.log_tm(f"{ts_prefix()} [KIS][done] 데이터 다운로드 및 S3 업로드 완료 (총 {len(combined)}건)")

    if args.freq == "1d":
        print(f"{ts_prefix()} [pipeline] unified parquet build 실행")
        unified_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kis_1d_unified_parquet.py")
        venv_python = os.path.join(os.path.dirname(os.path.abspath(__file__)), "venv", "bin", "python")
        if not os.path.exists(venv_python):
            venv_python = "python"
        exit_code = os.system(f"{venv_python} {unified_path} --config {config_path}")
        if exit_code == 0:
            log_manager.log_tm(f"{ts_prefix()} [KIS][done] EC2 통합 parquet 생성 완료 (data/1d_data/kis_1d_unified_parquet_DB.parquet)")
        else:
            log_manager.log_tm(f"{ts_prefix()} [KIS][warn] EC2 통합 parquet 생성 실패 (data/1d_data/kis_1d_unified_parquet_DB.parquet)")
    log_manager.log_tm(f"{ts_prefix()} [KIS][end] {os.path.basename(__file__)} 종료")


if __name__ == "__main__":
    main()
