"""
s3에 저장된 1d 데이터를 단일 Parquet로 병합 -> 시작일은 아래 지정한 일자부터, 종료일은 오늘날짜로 자동 설정
=> kis_1d_unified_parquet_DB.parquet 파일 생성(일부 보조피쳐 생성, 예: 전일상한가 여부, 전일/전전일 등락율, 3일간 평균 변동액, 전일변동액 등)

 - (매일) kis_ohlcv_Daily_fetch_manager.py 실행 후 자동으로 실행되도록 되어 있음.
 - (수시) 현재 파일의 시작일, 종료일을 직접 수정해서 실행해도 됨.
 - (참고) S3에서 지정한 시작일과 종료일에 대한 데이터를 다운받아 파일로 저장하는 스크립트 : kis_ohlcv_Daily_fetch_manager.py
         (★★★★ 기존 파일에 추가가 아니라 매번 새로운 파일로 교체, 저장하여 DB화 함. 기존 파일은 삭제함.★★★★)
columns_example = ['date', 'symbol', 'name', 'stat'(날짜별종목상태_daily_KRX_code_DB조인_없으면None),
    'open', 'high', 'low', 'close', 'volume', 'value',
    'pdy_close'(전일종가), 'p2dy_close'(전전일종가), 'tdy_ctrt'(등락율), 'pdy_ctrt'(전일등락율), 'p2dy_ctrt'(전전일등락율), 'pdy_uc'(전일상한가 여부),
    'pdy_h'(전일고가), 'pdy_L'(전일저가), 'atr3'(3일 ATR), 'atr'(당일 변동폭), 'vema3'(거래량 EMA3), 'vema24'(거래량 EMA24)]
[save] /home/ubuntu/Stoc_Kis/data/1d_data/kis_1d_unified_parquet_DB.parquet
"""
import argparse, os, time, duckdb, polars as pl
from datetime import datetime
from zoneinfo import ZoneInfo

from kis_utils import (
    _configure_duckdb_s3,
    calc_limit_up_price,
    list_s3_1d_paths,
    load_kis_data_layout,
    load_symbol_master,
)


def main() -> None:
    시작일 = "2025-01-01"
    종료일 = datetime.now().strftime("%Y-%m-%d")
    ap = argparse.ArgumentParser(description="기간 내 1d 데이터를 단일 Parquet로 병합")
    ap.add_argument("--start", default=시작일, help="YYYY-MM-DD")
    ap.add_argument("--end", default=종료일, help="YYYY-MM-DD")
    ap.add_argument("--config", default="config.json", help="설정 파일 경로")
    args = ap.parse_args()

    KST = ZoneInfo("Asia/Seoul")
    started_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    print("\n"+"=" * 60)
    print(f"[시작시각] {started_at}")
    print(f"[start] unified parquet build range={args.start}~{args.end}")
    print("데이터 취합 작업 중....")
    t0 = time.perf_counter()
    config_path = args.config
    if not os.path.isabs(config_path) and not os.path.exists(config_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, config_path)

    layout = load_kis_data_layout(config_path)
    paths = list_s3_1d_paths(layout, args.start, args.end)
    if not paths:
        print("[warn] 대상 기간에 해당하는 S3 파일이 없습니다.")
        return

    files_sql = "[" + ",".join([f"'{p}'" for p in paths]) + "]"
    query = f"""
    SELECT date, symbol, open, high, low, close, volume, value
    FROM read_parquet({files_sql})
    WHERE date BETWEEN '{args.start}' AND '{args.end}'
    ORDER BY symbol, date
    """

    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con, config_path)
        df = con.execute(query).pl()          #['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'value']
    finally:
        con.close()
    print(df.columns)
    t1 = time.perf_counter()
    # 추가 필드: 전일/전전일 종가, 등락율, 어제 상한가 여부, 어제 고/저가, 변동성 지표
    if df.schema.get("date") == pl.Utf8:
        df = df.with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
    df = df.sort(["symbol", "date"])

    df = df.with_columns(
        [
            pl.col("close").shift(1).over("symbol").alias("pdy_close"),
            pl.col("close").shift(2).over("symbol").alias("p2dy_close"),
            pl.col("close").shift(3).over("symbol").alias("_p3dy_close"),
            pl.col("high").shift(1).over("symbol").alias("pdy_h"),
            pl.col("low").shift(1).over("symbol").alias("pdy_L"),
        ]
    ).with_columns(
        [
            ((pl.col("close") / pl.col("pdy_close")) - 1).alias("tdy_ctrt"),
            ((pl.col("pdy_close") / pl.col("p2dy_close")) - 1).alias("pdy_ctrt"),
            ((pl.col("p2dy_close") / pl.col("_p3dy_close")) - 1).alias("p2dy_ctrt"),
            (pl.col("high") - pl.col("low")).rolling_mean(3).over("symbol").alias("atr3"),
            (pl.col("high") - pl.col("low")).alias("atr"),
            pl.col("volume").ewm_mean(span=3, adjust=False).over("symbol").alias("vema3"),
            pl.col("volume").ewm_mean(span=20, adjust=False).over("symbol").alias("vema20"),
        ]
    )

    df = df.with_columns(
        pl.col("p2dy_close")
        .cast(pl.Float64)
        .map_elements(
            lambda v: float(calc_limit_up_price(v)) if v is not None else None,
            return_dtype=pl.Float64,
        )
        .alias("limit_up_price")
    ).with_columns(
        (pl.col("pdy_close") >= pl.col("limit_up_price")).alias("pdy_uc")
    ).drop("limit_up_price", "_p3dy_close")
    t2 = time.perf_counter()

    # 종목명 매핑: map_elements(737k회) 대신 join으로 최적화 (수 분 → 수 초)
    t_name_start = time.perf_counter()
    master = load_symbol_master(config_path)
    master_pl = pl.from_pandas(
        master[["code", "name"]].assign(code=master["code"].astype(str).str.zfill(6))
    )
    df = df.with_columns(pl.col("symbol").cast(pl.Utf8).str.zfill(6).alias("_sym"))
    df = df.join(
        master_pl.select(pl.col("code").alias("_sym"), pl.col("name")),
        on="_sym",
        how="left",
    ).drop("_sym")
    t_name_end = time.perf_counter()

    # daily_KRX_code_DB에서 날짜별 종목상태(iscd_stat_cls_code) 조인 → stat 컬럼 (없으면 None)
    t_stat_start = time.perf_counter()
    daily_krx_path = os.path.join(
        layout.local_root, "admin", "symbol_master", "daily_KRX_code_DB.parquet"
    )
    if os.path.exists(daily_krx_path):
        try:
            krx_daily = pl.read_parquet(daily_krx_path)
            if "date" in krx_daily.columns and "code" in krx_daily.columns and "iscd_stat_cls_code" in krx_daily.columns:
                krx_stat = krx_daily.select(
                    pl.col("date").cast(pl.Date, strict=False).alias("date"),
                    pl.col("code").cast(pl.Utf8).str.zfill(6).alias("symbol"),
                    pl.col("iscd_stat_cls_code").alias("stat"),
                )
                df = df.join(krx_stat, on=["date", "symbol"], how="left")
                print(f"[stat 매핑] daily_KRX_code_DB 조인 완료 krx_rows={krx_stat.height}")
            else:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("stat"))
        except Exception as e:
            print(f"[stat 매핑] daily_KRX_code_DB 조인 실패: {e} → stat=None")
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("stat"))
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("stat"))
    t_stat_end = time.perf_counter()

    desired_cols = [
        "date",
        "symbol",
        "name",
        "stat",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "pdy_close",
        "p2dy_close",
        "tdy_ctrt",
        "pdy_ctrt",
        "p2dy_ctrt",
        "pdy_uc",
        "pdy_h",
        "pdy_L",
        "atr3",
        "atr",
        "vema3",
        "vema20",
    ]
    df = df.select([c for c in desired_cols if c in df.columns])

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "1d_data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "kis_1d_unified_parquet_DB.parquet")

    t_write_start = time.perf_counter()
    df.write_parquet(out_path)
    t_write_end = time.perf_counter()

    print(df)
    print(df.columns)
    elapsed = time.perf_counter() - t0
    feature_elapsed = t2 - t1
    query_elapsed = t1 - t0
    name_elapsed = t_name_end - t_name_start
    write_elapsed = t_write_end - t_write_start
    done_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    file_size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"[done] {out_path} rows={df.height}")
    print(f"[종료시각] {done_at}")
    print(f"[검색시간] seconds={query_elapsed:.3f}")
    print(f"[피쳐가공시간] seconds={feature_elapsed:.3f}")
    print(f"[종목명매핑시간] seconds={name_elapsed:.3f}")
    stat_elapsed = t_stat_end - t_stat_start
    print(f"[종목상태매핑시간] seconds={stat_elapsed:.3f}")
    print(f"[parquet저장시간] seconds={write_elapsed:.3f}")
    print(f"[총소요시간] seconds={elapsed:.3f}")
    print(f"[file_size] {file_size_mb:.2f}MB")


if __name__ == "__main__":
    main()
