"""
Created on 20250601 
당일 또는 과거 1분봉 데이터 출력
"""

import sys
import logging
from datetime import datetime, timedelta

import pandas as pd

sys.path.extend(['../..', '.'])
import kis_auth as ka
from kis_utils import load_symbol_master, stQueryDB
from inquire_time_itemchartprice import inquire_time_itemchartprice, inquire_time_dailychartprice

# 로깅 설정
logging.basicConfig(level=logging.INFO)

"""
1개의 종목에 대해 API를 호출하여 1분봉 데이터를 다운받아 출력 =============================================
 이 함수는 주식당일분봉조회 API를 호출하여 결과를 출력합니다.
 테스트 데이터로 삼성전자(005930)를 사용합니다.
"""
##############################################################################################
# [국내주식] 기본시세 > 주식당일분봉조회[v1_국내주식-022]
# 주식당일 1분봉조회 API를 호출하여 결과를 조회 및 출력
##############################################################################################
CODE_TEXT = """
삼성전자


"""

COLUMN_MAPPING = {
    "prdy_ctrt": "tdy_ret",
    "stck_prdy_clpr": "pdy_close",
    "acml_vol": "acml_volume",
    "hts_kor_isnm": "name",
    "stck_bsop_date": "date",
    "stck_cntg_hour": "time",
    "stck_prpr": "close",
    "stck_oprc": "open",
    "stck_hgpr": "high",
    "stck_lwpr": "low",
    "cntg_vol": "volume",
    "acml_tr_pbmn": "acml_value",
}

NUMERIC_COLUMNS = []
print_all_rows = 0  # 1이면 전체행 정렬 출력, 0이면 print(df)

# 조회 날짜: None=당일 API, "YYYYMMDD"=일별분봉 API(과거날짜 전용)
# ※ 당일 장중에는 일별분봉 API(inquire-time-dailychartprice)가 500 에러를 반환합니다.
#   → 오늘 날짜가 감지되면 자동으로 당일분봉 API(inquire-time-itemchartprice)로 전환합니다.
QUERY_DATE = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y%m%d")

_today_kst = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y%m%d")
_USE_INTRADAY_API = (QUERY_DATE is None) or (QUERY_DATE == _today_kst)

# ── 조회 시간 리스트 자동 생성 ──────────────────────────────────────────────────
# 두 API는 FID_INPUT_HOUR_1 의미가 반대입니다.
#   일별분봉 (inquire_time_dailychartprice) : 지정 시각 *이후* 30분 반환 (순방향)
#     090000 → 09:00~09:29, 093000 → 09:30~09:59 ...
#   당일분봉 (inquire_time_itemchartprice)  : 지정 시각 *이전* 30분 반환 (역방향)
#     093000 → 09:00~09:29, 100000 → 09:30~09:59 ...
#     090000 을 입력하면 장전(08:30~08:59) 데이터를 요청하는 것 → 500 에러!
#
# _USE_INTRADAY_API = True 인 경우: 093000 시작, 현재 시각까지 30분 단위로 생성
# _USE_INTRADAY_API = False 인 경우: 090000 시작, 153000 끝 (전일 전체)
if _USE_INTRADAY_API:
    _now_kst = datetime.utcnow() + timedelta(hours=9)
    _start = datetime.strptime("093000", "%H%M%S").replace(
        year=_now_kst.year, month=_now_kst.month, day=_now_kst.day
    )
    # 현재 시각에서 30분 올림(ceil)하여 마지막 슬롯 포함
    _end_min = _now_kst.hour * 60 + _now_kst.minute
    _end_min_ceil = ((_end_min + 29) // 30) * 30  # 30분 단위 올림
    _end = _now_kst.replace(hour=_end_min_ceil // 60, minute=_end_min_ceil % 60,
                             second=0, microsecond=0)
    _end = min(_end, _now_kst.replace(hour=15, minute=30, second=0, microsecond=0))
else:
    _start = datetime.strptime("090000", "%H%M%S")
    _end   = datetime.strptime("153000", "%H%M%S")

QUERY_TIMES = []
_cur = _start
while _cur <= _end:
    QUERY_TIMES.append(_cur.strftime("%H%M%S"))
    _cur += timedelta(minutes=30)


def _build_name_code_map() -> tuple[dict[str, str], dict[str, str]]:
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"]))
        code_to_name = dict(zip(sdf["code"], sdf["name"].astype(str)))
        return name_to_code, code_to_name
    except Exception as e:
        logging.warning("종목마스터 로드 실패: %s", e)
        return {}, {}


def _parse_codes(text: str, name_to_code: dict[str, str]) -> list[str]:
    codes = []
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        if raw.isdigit():
            codes.append(raw.zfill(6))
            continue
        code = name_to_code.get(raw)
        if code:
            codes.append(code)
    return codes


def main():
    # pandas 출력 옵션 설정
    pd.set_option('display.max_columns', None)  # 모든 컬럼 표시
    pd.set_option('display.width', None)  # 출력 너비 제한 해제
    pd.set_option('display.max_rows', None)  # 모든 행 표시

    # kis_auth는 _url_fetch 내부에서 토큰을 자동으로 발급/캐시합니다.

    if _USE_INTRADAY_API:
        logging.info("조회: 당일분봉 API (inquire-time-itemchartprice) ← 오늘 장중 데이터")
    else:
        logging.info("조회 날짜: %s (일별분봉 API, 과거 날짜)", QUERY_DATE)

    name_to_code, code_to_name = _build_name_code_map()
    codes = _parse_codes(CODE_TEXT, name_to_code)
    if not codes:
        logging.error("CODE_TEXT에 종목코드 또는 종목명을 입력하세요.")
        return
    pdy_cache: dict[str, float] = {}

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

    for code in codes:
        logging.info("=== 조회: %s ===", code)
        code_rows = []
        name = code_to_name.get(code, "")
        if code not in pdy_cache:
            row = stQueryDB(code)
            pdy_close = float(row.get("pdy_close") or 0)
            p2dy_close = float(row.get("p2dy_close") or 0)
            pdy_ret = (pdy_close / p2dy_close - 1.0) if p2dy_close > 0 else 0.0
            pdy_cache[code] = {"pdy_close": pdy_close, "pdy_ret": pdy_ret}
        for query_time in QUERY_TIMES:
            try:
                if _USE_INTRADAY_API:
                    # 당일 장중: 주식당일분봉조회 API 사용 (일별분봉 API는 당일에 500 에러)
                    output1, output2 = inquire_time_itemchartprice(
                        env_dv="real",
                        fid_cond_mrkt_div_code="J",
                        fid_input_iscd=code,
                        fid_input_hour_1=query_time,
                        fid_pw_data_incu_yn="Y",
                    )
                else:
                    # 과거 날짜: 일별분봉조회 API 사용
                    output1, output2 = inquire_time_dailychartprice(
                        env_dv="real",
                        fid_cond_mrkt_div_code="J",
                        fid_input_iscd=code,
                        fid_input_date_1=QUERY_DATE,
                        fid_input_hour_1=query_time,
                        fid_pw_data_incu_yn="Y",
                    )
            except Exception as e:
                logging.error("에러 발생(%s %s): %s", code, query_time, e)
                continue

            if output2.empty:
                continue

            # QUERY_DATE 지정 시 해당 날짜가 아닌 행 제외 (서버 혼선 방지)
            if QUERY_DATE:
                _date_col = "stck_bsop_date" if "stck_bsop_date" in output2.columns else "date"
                if _date_col in output2.columns:
                    output2 = output2[output2[_date_col].astype(str).str.replace("-", "") == str(QUERY_DATE)]
            if output2.empty:
                continue
            output1 = output1.rename(columns=COLUMN_MAPPING)
            output2 = output2.rename(columns=COLUMN_MAPPING)
            output2["code"] = code
            if "name" in output1.columns and not output1.empty:
                output2["name"] = output1["name"].iloc[0]
            else:
                output2["name"] = name
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

            for col in NUMERIC_COLUMNS:
                if col in output2.columns:
                    output2[col] = pd.to_numeric(output2[col], errors='coerce').round(2)

            code_rows.append(output2)

        if not code_rows:
            logging.warning("데이터 없음: %s", code)
            continue

        df_all = pd.concat(code_rows, ignore_index=True)
        if "name" in df_all.columns and not df_all.empty:
            df_all["name"] = df_all["name"].fillna("").replace("", name)
        df_all = df_all.sort_values(["code", "date", "time"]).reset_index(drop=True)
        logging.info("결과 (output2):")
        if print_all_rows:
            pd.set_option("display.colheader_justify", "right")
            print(df_all.to_string(index=False, justify="right"))
        else:
            print(df_all)


if __name__ == "__main__":
    main()