import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
sys.path.extend(['../..', '.'])
import kis_auth as ka
from kis_utils import load_symbol_master, print_table, stQueryDB
from inquire_time_itemchartprice import inquire_time_itemchartprice
# 로깅 설정
logging.basicConfig(level=logging.INFO)

"""
아래 리스트한 여러 종목코드들에 대해 
1분봉 데이터를 API를 통해 다운받아 csv 파일로 저장(당일 데이터만 저장)

"""
##############################################################################################
# [국내주식] 기본시세 > 주식당일분봉조회[v1_국내주식-022]
# 주식당일 1분봉조회 API를 호출하여 결과를 조회, parquet/csv 파일로 저장합니다.
##############################################################################################

# 종목코드 리스트
CODE_TEXT = """
경남제약


"""
"""
10100
23960
32820
49180
76610
94940
138360
217500
340810
380540
457190
"""

# 조회 시간 리스트
# 090000부터 153000까지 30분 단위로 1분봉 일괄 조회
QUERY_TIMES = []
_start = datetime.strptime("090000", "%H%M%S")
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
    'prdy_ctrt': 'tdy_ret',
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

    all_rows = []
    summary_rows = []
    biz_date = None

    name_map = {}
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        name_map = dict(zip(sdf["code"], sdf["name"]))
    except Exception as e:
        logging.warning("종목마스터 로드 실패: %s", e)

    name_to_code = {}
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"]))
    except Exception as e:
        logging.warning("종목마스터 로드 실패: %s", e)

    target_codes = _parse_codes(CODE_TEXT, name_to_code)
    if not target_codes:
        target_codes = TARGET_CODES

    pdy_cache: dict[str, dict[str, float]] = {}
    for code in target_codes:
        code_rows = []
        name = name_map.get(code, "")
        if code not in pdy_cache:
            row = stQueryDB(code)
            pdy_close = float(row.get("pdy_close") or 0)
            p2dy_close = float(row.get("p2dy_close") or 0)
            pdy_ret = (pdy_close / p2dy_close - 1.0) if p2dy_close > 0 else 0.0
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
                logging.error("에러 발생(%s): %s", code, str(e))
                continue

            if output2.empty:
                continue

            # # DEBUG: output1 컬럼 확인용 (정상 확인 후 주석 처리)  => 확인 결과, output1 데이터는 수신되지 않았음
            # logging.info("[DEBUG] output1.columns (%s %s): %s", code, query_time, output1.columns.tolist())

            if biz_date is None:
                if "stck_bsop_date" in output2.columns and not output2.empty:
                    biz_date = str(output2["stck_bsop_date"].iloc[0]).strip()
                elif not output1.empty:
                    biz_date = str(output1.get("stck_bsop_date", [""])[0]).strip()

            output2 = output2.copy()
            if not output1.empty:
                for col in output1.columns:
                    if col not in output2.columns:
                        output2[col] = output1[col].iloc[0]
            output2["code"] = code
            output2["name"] = name

            output2 = output2.rename(columns=COLUMN_MAPPING)
            desired_cols = [
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
            if "time" in output2.columns:
                output2["time"] = output2["time"].astype(str).str.zfill(6)
                output2 = output2[output2["time"] <= "153000"]
            if "close" in output2.columns:
                output2["close"] = pd.to_numeric(output2["close"], errors="coerce")
            pdy_close = pdy_cache.get(code, {}).get("pdy_close", 0.0)
            pdy_ret = pdy_cache.get(code, {}).get("pdy_ret", 0.0)
            if pdy_close > 0:
                output2["pdy_close"] = pdy_close
                if "close" in output2.columns:
                    output2["tdy_ret"] = (output2["close"] - pdy_close) / pdy_close
            output2["pdy_ret"] = pdy_ret
            for col in desired_cols:
                if col not in output2.columns:
                    output2[col] = ""
            output2 = output2.reindex(columns=desired_cols)
            output2 = _drop_empty_columns(output2)

            for col in NUMERIC_COLUMNS:
                if col in output2.columns:
                    output2[col] = pd.to_numeric(output2[col], errors="coerce").round(2)

            code_rows.append(output2)

        if not code_rows:
            logging.warning("데이터 없음: %s", code)
            continue

        code_df = pd.concat(code_rows, ignore_index=True)
        code_df = code_df.sort_values(["code", "date", "time"]).reset_index(drop=True)
        all_rows.append(code_df)
        summary_rows.append(
            {
                "code": code,
                "name": name,
                "rows": len(code_df),
                "first_time": str(code_df["time"].iloc[0]) if "time" in code_df.columns else "",
                "last_time": str(code_df["time"].iloc[-1]) if "time" in code_df.columns else "",
            }
        )
        logging.info("조회완료: %s %s rows=%d", code, name, len(code_df))
        print(f"\n=== {code} {name} ===")
        if print_all_rows:
            pd.set_option("display.colheader_justify", "right")
            print(code_df.to_string(index=False, justify="right"))
        else:
            print(code_df)

    if not all_rows:
        logging.error("저장할 데이터가 없습니다. (거래일/시간/종목 거래유무 확인)")
        return

    merged = pd.concat(all_rows, ignore_index=True)
    for col in ("code", "date", "time"):
        if col in merged.columns:
            merged[col] = merged[col].astype(str)
    merged = merged.sort_values(["code", "date", "time"]).reset_index(drop=True)
    if not biz_date:
        biz_date = pd.Timestamp.now().strftime("%Y%m%d")

    out_dir = Path(__file__).resolve().parent / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / f"{biz_date}_1m_[codes]_chart_parquet.csv"

    key_cols = ["date", "time", "code"]
    if csv_path.exists():
        logging.info("기존 csv 발견 -> 덮어쓰기 모드")

    merged = merged.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)

    merged.to_csv(csv_path, index=False, encoding="utf-8-sig")

    if summary_rows:
        logging.info("종목별 요약:")
        print_table(
            summary_rows,
            columns=["code", "name", "rows", "first_time", "last_time"],
            align={"rows": "right"},
        )

    logging.info("저장 완료: %s", csv_path)


if __name__ == "__main__":
    main()