"""
KRX 종목 코드 파일 생성
한국투자증권 OpenAPI 종목 마스터 자동 다운로드 및 통합
  - new.real.download.dws.co.kr 에서 kospi_code.mst.zip, kosdaq_code.mst.zip 다운로드
    (github.com/koreainvestment/open-trading-api stocks_info 참고)
  - /home/ubuntu/Stoc_Kis/data/admin/symbol_master/ 폴더에 다음 파일들을 저장
    * kospi_code.mst, kosdaq_code.mst, nxt_kospi_code.mst, nxt_kosdaq_code.mst
  - 위의 파일들을 읽어 들여 각각 csv파일로 변환해 놓고
    * kospi_code.csv, kosdaq_code.csv, nxt_kospi_code.csv, nxt_kosdaq_code.csv
  - 이를 통합해서 latest.* / KRX_code.*로 생성  (NXT 종목여부, iscd_stat_cls_code 포함)
    * latest.csv, latest.parquet, KRX_code.csv, KRX_code.parquet
  - daily_KRX_code_DB.parquet: 날짜별 데이터 축적 (해당 일자 이미 있으면 추가 제외, 일 중 재실행 시 중복 방지)

실행/모니터링

직접 실행:
  /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/fetch__daily_KRX_code.py

cron (매일 KST 08:20 = UTC 23:20 전날, DB-1과 동일 nohup 패턴):
  20 23 * * * nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/fetch__daily_KRX_code.py >> /home/ubuntu/Stoc_Kis/out/fetch_daily_KRX_code.log 2>&1 &

nohup 실행:
  nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/fetch__daily_KRX_code.py >> /home/ubuntu/Stoc_Kis/out/fetch_daily_KRX_code.log 2>&1 &

로그 모니터링:
  tail -f /home/ubuntu/Stoc_Kis/out/fetch_daily_KRX_code.log

출력 파일:
  - data/admin/symbol_master/latest.parquet, KRX_code.parquet, daily_KRX_code_DB.parquet
"""

import os
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from kis_utils import load_kis_data_layout

try:
    from telegMsg import tmsg
except Exception:
    tmsg = None

KST = ZoneInfo("Asia/Seoul")
PROGRAM_NAME = "fetch__daily_KRX_code"


STANDARD_COLUMNS = [
    "code",
    "std_code",
    "name",
    "nxt",
    "market",
    "iscd_stat_cls_code",
    "group",
    "market_cap_size",
    "sector_l",
    "sector_m",
    "sector_s",
    "venture",
    "low_liquidity",
    "krx",
    "etp",
    "krx100",
    "krx300",
    "base_price",
    "lot_size",
    "after_hours_lot_size",
    "halted",
    "delisting",
    "managed",
    "warning",
    "warning_notice",
    "disclosure",
    "backdoor",
    "lock",
    "par_change",
    "capital_increase",
    "margin_ratio",
    "credit_possible",
    "credit_period",
    "prev_volume",
    "par_value",
    "listed_date",
    "listed_shares",
    "capital",
    "fiscal_month",
    "ipo_price",
    "preferred",
    "short_overheat",
    "surge",
    "market_cap",
    "group_company_code",
    "credit_limit_over",
    "collateral_loan",
    "borrowable",
]


def _normalize_code(val: str) -> str:
    return str(val).strip().zfill(6)


# 한국투자증권 OpenAPI 공식 GitHub 참고: github.com/koreainvestment/open-trading-api
# stocks_info/kis_kospi_code_mst.py, kis_kosdaq_code_mst.py 에서 사용하는 다운로드 경로
MST_DOWNLOAD_BASE_URL = "https://new.real.download.dws.co.kr/common/master/"


def download_mst_from_kis(target_dir: str) -> tuple[str, str]:
    """
    KIS 종목 마스터(kospi_code.mst.zip, kosdaq_code.mst.zip) 다운로드 후 압축 해제.
    출처: new.real.download.dws.co.kr (한국투자증권 OpenAPI GitHub stocks_info 참고)
    반환: (kospi_mst_path, kosdaq_mst_path)
    다운로드 실패 시 에러 발생 (기존 파일 fallback 없음).
    """
    os.makedirs(target_dir, exist_ok=True)
    kospi_mst = os.path.join(target_dir, "kospi_code.mst")
    kosdaq_mst = os.path.join(target_dir, "kosdaq_code.mst")
    errors: list[str] = []

    for market, zip_path, mst_path in [
        ("kospi", os.path.join(target_dir, "kospi_code.mst.zip"), kospi_mst),
        ("kosdaq", os.path.join(target_dir, "kosdaq_code.mst.zip"), kosdaq_mst),
    ]:
        url = f"{MST_DOWNLOAD_BASE_URL}{market}_code.mst.zip"
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                f.write(r.content)
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(target_dir)
            if os.path.exists(zip_path):
                os.remove(zip_path)
            print(f"[KRX_code_fetch] 다운로드 완료: {market}_code.mst")
        except Exception as e:
            errors.append(f"{market}: {e}")

    if errors:
        raise RuntimeError(
            f"[KRX_code_fetch] mst 다운로드 실패 (새 데이터 수신 불가): " + "; ".join(errors)
        )
    return kospi_mst, kosdaq_mst


def _extract_iscd_stat_from_mst(file_path: str, encoding: str = "cp949") -> list[str]:
    """mst 파일 각 라인에서 144:146 바이트(종목상태구분 iscd_stat_cls_code) 추출"""
    result: list[str] = []
    if not os.path.exists(file_path):
        return result
    with open(file_path, "r", encoding=encoding) as f:
        for line in f:
            val = line[144:146].strip() if len(line) >= 146 else ""
            result.append(val)
    return result


def _build_output(
    df: pd.DataFrame,
    market: str,
    mapping: dict[str, str],
    nxt_codes: set[str],
) -> pd.DataFrame:
    """market: kospi_code.mst → "KOSPI", kosdaq_code.mst → "KOSDAQ"
    nxt: nxt_kospi_code.mst, nxt_kosdaq_code.mst에 있는 종목코드는 "Y", 그 외 "N"
    """
    out = pd.DataFrame()
    for std_col in STANDARD_COLUMNS:
        if std_col in ("market", "nxt"):
            continue
        src = mapping.get(std_col)
        if src and src in df.columns:
            out[std_col] = df[src].astype(str)
        else:
            out[std_col] = None
    out["code"] = out["code"].astype(str).map(_normalize_code)
    # 파일 출처에 따라 market 설정 (kospi_code.mst → KOSPI, kosdaq_code.mst → KOSDAQ)
    out["market"] = market
    # NXT: nxt_kospi_code.mst, nxt_kosdaq_code.mst에 등장한 종목코드만 Y, 나머지 N
    out["nxt"] = out["code"].map(lambda c: "Y" if c in nxt_codes else "N")
    return out[STANDARD_COLUMNS]


def _read_kospi_codes(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=str)
    if "단축코드" not in df.columns or "한글명" not in df.columns:
        raise ValueError(f"필수 컬럼 누락: {csv_path}")
    mapping = {
        "code": "단축코드",
        "std_code": "표준코드",
        "name": "한글명",
        "group": "그룹코드",
        "market_cap_size": "시가총액규모",
        "sector_l": "지수업종대분류",
        "sector_m": "지수업종중분류",
        "sector_s": "지수업종소분류",
        "venture": "제조업",
        "low_liquidity": "저유동성",
        "krx": "KRX",
        "etp": "ETP",
        "krx100": "KRX100",
        "krx300": "KRX300",
        "base_price": "기준가",
        "lot_size": "매매수량단위",
        "after_hours_lot_size": "시간외수량단위",
        "halted": "거래정지",
        "delisting": "정리매매",
        "managed": "관리종목",
        "warning": "시장경고",
        "warning_notice": "경고예고",
        "disclosure": "불성실공시",
        "backdoor": "우회상장",
        "lock": "락구분",
        "par_change": "액면변경",
        "capital_increase": "증자구분",
        "margin_ratio": "증거금비율",
        "credit_possible": "신용가능",
        "credit_period": "신용기간",
        "prev_volume": "전일거래량",
        "par_value": "액면가",
        "listed_date": "상장일자",
        "listed_shares": "상장주수",
        "capital": "자본금",
        "fiscal_month": "결산월",
        "ipo_price": "공모가",
        "preferred": "우선주",
        "short_overheat": "공매도과열",
        "surge": "이상급등",
        "market_cap": "시가총액",
        "group_company_code": "그룹사코드",
        "credit_limit_over": "회사신용한도초과",
        "collateral_loan": "담보대출가능",
        "borrowable": "대주가능",
    }
    return _build_output(df, "KOSPI", mapping, set())


def _read_kosdaq_codes(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=str)
    if "단축코드" not in df.columns or "한글종목명" not in df.columns:
        raise ValueError(f"필수 컬럼 누락: {csv_path}")
    mapping = {
        "code": "단축코드",
        "std_code": "표준코드",
        "name": "한글종목명",
        "group": "증권그룹구분코드",
        "market_cap_size": "시가총액 규모 구분 코드 유가",
        "sector_l": "지수업종 대분류 코드",
        "sector_m": "지수 업종 중분류 코드",
        "sector_s": "지수업종 소분류 코드",
        "venture": "벤처기업 여부 (Y/N)",
        "low_liquidity": "저유동성종목 여부",
        "krx": "KRX 종목 여부",
        "etp": "ETP 상품구분코드",
        "krx100": "KRX100 종목 여부 (Y/N)",
        "krx300": "KRX300 종목 여부 (Y/N)",
        "base_price": "주식 기준가",
        "lot_size": "정규 시장 매매 수량 단위",
        "after_hours_lot_size": "시간외 시장 매매 수량 단위",
        "halted": "거래정지 여부",
        "delisting": "정리매매 여부",
        "managed": "관리 종목 여부",
        "warning": "시장 경고 구분 코드",
        "warning_notice": "시장 경고위험 예고 여부",
        "disclosure": "불성실 공시 여부",
        "backdoor": "우회 상장 여부",
        "lock": "락구분 코드",
        "par_change": "액면가 변경 구분 코드",
        "capital_increase": "증자 구분 코드",
        "margin_ratio": "증거금 비율",
        "credit_possible": "신용주문 가능 여부",
        "credit_period": "신용기간",
        "prev_volume": "전일 거래량",
        "par_value": "주식 액면가",
        "listed_date": "주식 상장 일자",
        "listed_shares": "상장 주수(천)",
        "capital": "자본금",
        "fiscal_month": "결산 월",
        "ipo_price": "공모 가격",
        "preferred": "우선주 구분 코드",
        "short_overheat": "공매도과열종목여부",
        "surge": "이상급등종목여부",
        "market_cap": "전일기준 시가총액 (억)",
        "group_company_code": "그룹사 코드",
        "credit_limit_over": "회사신용한도초과여부",
        "collateral_loan": "담보대출가능여부",
        "borrowable": "대주가능여부",
    }
    return _build_output(df, "KOSDAQ", mapping, set())


def _parse_mst(
    file_path: str,
    part2_len: int,
    field_specs: list[int],
    part1_columns: list[str],
    part2_columns: list[str],
    encoding: str = "cp949",
) -> pd.DataFrame:
    tmp_fil1 = f"{file_path}.part1.tmp"
    tmp_fil2 = f"{file_path}.part2.tmp"

    with open(file_path, mode="r", encoding=encoding) as f, open(
        tmp_fil1, mode="w", encoding=encoding
    ) as wf1, open(tmp_fil2, mode="w", encoding=encoding) as wf2:
        for row in f:
            rf1 = row[0 : len(row) - part2_len]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + "," + rf1_2 + "," + rf1_3 + "\n")
            rf2 = row[-part2_len:]
            wf2.write(rf2)

    df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding=encoding)
    df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns, encoding=encoding)
    df = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)
    os.remove(tmp_fil1)
    os.remove(tmp_fil2)
    return df


def _kospi_mst_to_df(file_path: str) -> pd.DataFrame:
    part1_columns = ["단축코드", "표준코드", "한글명"]
    field_specs = [
        2,
        1,
        4,
        4,
        4,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        9,
        5,
        5,
        1,
        1,
        1,
        2,
        1,
        1,
        1,
        2,
        2,
        2,
        3,
        1,
        3,
        12,
        12,
        8,
        15,
        21,
        2,
        7,
        1,
        1,
        1,
        1,
        9,
        9,
        9,
        5,
        9,
        8,
        9,
        3,
        1,
        1,
        1,
    ]
    part2_columns = [
        "그룹코드",
        "시가총액규모",
        "지수업종대분류",
        "지수업종중분류",
        "지수업종소분류",
        "제조업",
        "저유동성",
        "지배구조지수종목",
        "KOSPI200섹터업종",
        "KOSPI100",
        "KOSPI50",
        "KRX",
        "ETP",
        "ELW발행",
        "KRX100",
        "KRX자동차",
        "KRX반도체",
        "KRX바이오",
        "KRX은행",
        "SPAC",
        "KRX에너지화학",
        "KRX철강",
        "단기과열",
        "KRX미디어통신",
        "KRX건설",
        "Non1",
        "KRX증권",
        "KRX선박",
        "KRX섹터_보험",
        "KRX섹터_운송",
        "SRI",
        "기준가",
        "매매수량단위",
        "시간외수량단위",
        "거래정지",
        "정리매매",
        "관리종목",
        "시장경고",
        "경고예고",
        "불성실공시",
        "우회상장",
        "락구분",
        "액면변경",
        "증자구분",
        "증거금비율",
        "신용가능",
        "신용기간",
        "전일거래량",
        "액면가",
        "상장일자",
        "상장주수",
        "자본금",
        "결산월",
        "공모가",
        "우선주",
        "공매도과열",
        "이상급등",
        "KRX300",
        "KOSPI",
        "매출액",
        "영업이익",
        "경상이익",
        "당기순이익",
        "ROE",
        "기준년월",
        "시가총액",
        "그룹사코드",
        "회사신용한도초과",
        "담보대출가능",
        "대주가능",
    ]
    return _parse_mst(file_path, 228, field_specs, part1_columns, part2_columns)


def _kosdaq_mst_to_df(file_path: str) -> pd.DataFrame:
    part1_columns = ["단축코드", "표준코드", "한글종목명"]
    field_specs = [
        2,
        1,
        4,
        4,
        4,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        9,
        5,
        5,
        1,
        1,
        1,
        2,
        1,
        1,
        1,
        2,
        2,
        2,
        3,
        1,
        3,
        12,
        12,
        8,
        15,
        21,
        2,
        7,
        1,
        1,
        1,
        1,
        9,
        9,
        9,
        5,
        9,
        8,
        9,
        3,
        1,
        1,
        1,
    ]
    part2_columns = [
        "증권그룹구분코드",
        "시가총액 규모 구분 코드 유가",
        "지수업종 대분류 코드",
        "지수 업종 중분류 코드",
        "지수업종 소분류 코드",
        "벤처기업 여부 (Y/N)",
        "저유동성종목 여부",
        "KRX 종목 여부",
        "ETP 상품구분코드",
        "KRX100 종목 여부 (Y/N)",
        "KRX 자동차 여부",
        "KRX 반도체 여부",
        "KRX 바이오 여부",
        "KRX 은행 여부",
        "기업인수목적회사여부",
        "KRX 에너지 화학 여부",
        "KRX 철강 여부",
        "단기과열종목구분코드",
        "KRX 미디어 통신 여부",
        "KRX 건설 여부",
        "(코스닥)투자주의환기종목여부",
        "KRX 증권 구분",
        "KRX 선박 구분",
        "KRX섹터지수 보험여부",
        "KRX섹터지수 운송여부",
        "KOSDAQ150지수여부 (Y,N)",
        "주식 기준가",
        "정규 시장 매매 수량 단위",
        "시간외 시장 매매 수량 단위",
        "거래정지 여부",
        "정리매매 여부",
        "관리 종목 여부",
        "시장 경고 구분 코드",
        "시장 경고위험 예고 여부",
        "불성실 공시 여부",
        "우회 상장 여부",
        "락구분 코드",
        "액면가 변경 구분 코드",
        "증자 구분 코드",
        "증거금 비율",
        "신용주문 가능 여부",
        "신용기간",
        "전일 거래량",
        "주식 액면가",
        "주식 상장 일자",
        "상장 주수(천)",
        "자본금",
        "결산 월",
        "공모 가격",
        "우선주 구분 코드",
        "공매도과열종목여부",
        "이상급등종목여부",
        "KRX300 종목 여부 (Y/N)",
        "매출액",
        "영업이익",
        "경상이익",
        "단기순이익",
        "ROE(자기자본이익률)",
        "기준년월",
        "전일기준 시가총액 (억)",
        "그룹사 코드",
        "회사신용한도초과여부",
        "담보대출가능여부",
        "대주가능여부",
    ]
    return _parse_mst(file_path, 222, field_specs, part1_columns, part2_columns)


def _append_to_daily_db(df: pd.DataFrame, db_path: str) -> bool:
    """
    daily_KRX_code_DB.parquet에 날짜별 데이터 축적.
    - 해당 날짜 데이터가 이미 있으면 추가 제외 (08:30 수집 후 일 중 재실행 시 중복 방지)
    - 없으면 추가 등록
    Returns: True=추가함, False=스킵(이미 존재)
    """
    today_str = datetime.now(KST).strftime("%Y-%m-%d")
    df_with_date = df.copy()
    df_with_date["date"] = today_str

    if os.path.exists(db_path):
        try:
            existing = pd.read_parquet(db_path, columns=["date"])
            if not existing.empty:
                # date 컬럼이 datetime/str 등 다양한 형식일 수 있음
                dt_ser = pd.to_datetime(existing["date"], errors="coerce")
                dates_set = set(dt_ser.dropna().dt.strftime("%Y-%m-%d"))
                if today_str in dates_set:
                    print(f"[daily_KRX_code_DB] {today_str} 데이터 이미 존재 → 추가 스킵")
                    return False
        except Exception as e:
            print(f"[daily_KRX_code_DB] 기존 파일 확인 실패: {e} → 신규 추가 시도")

    try:
        if os.path.exists(db_path):
            existing_full = pd.read_parquet(db_path)
            combined = pd.concat([existing_full, df_with_date], ignore_index=True)
        else:
            combined = df_with_date
        combined.to_parquet(db_path, index=False)
        print(f"[daily_KRX_code_DB] {today_str} 추가 저장 완료 rows={len(df_with_date)} path={db_path}")
        return True
    except Exception as e:
        print(f"[daily_KRX_code_DB] 저장 실패: {e}")
        return False


def main() -> None:
    if tmsg is not None:
        try:
            tmsg(f"[{PROGRAM_NAME}] 프로그램 시작", "-t")
        except Exception:
            pass
    base_dir = os.path.dirname(os.path.abspath(__file__))
    default_dir = os.path.join(base_dir, "data", "admin", "symbol_master")
    os.makedirs(default_dir, exist_ok=True)
    kospi_mst = os.path.join(default_dir, "kospi_code.mst")
    kosdaq_mst = os.path.join(default_dir, "kosdaq_code.mst")
    nxt_kospi_mst = os.path.join(default_dir, "nxt_kospi_code.mst")
    nxt_kosdaq_mst = os.path.join(default_dir, "nxt_kosdaq_code.mst")

    # KIS 종목 마스터 mst 다운로드 (실패 시 에러 발생, 기존 파일 사용 안 함)
    download_mst_from_kis(default_dir)

    if not os.path.exists(kospi_mst) or not os.path.exists(kosdaq_mst):
        raise FileNotFoundError(
            "kospi_code.mst 또는 kosdaq_code.mst를 찾을 수 없습니다. "
            f"expected: {kospi_mst}, {kosdaq_mst}"
        )

    layout = load_kis_data_layout(os.path.join(base_dir, "config.json"))
    out_dir = os.path.join(layout.local_root, "admin", "symbol_master")
    os.makedirs(out_dir, exist_ok=True)

    df_kospi_raw = _kospi_mst_to_df(kospi_mst)
    kospi_stats = _extract_iscd_stat_from_mst(kospi_mst)
    df_kospi_raw["iscd_stat_cls_code"] = (
        kospi_stats[: len(df_kospi_raw)]
        if len(kospi_stats) >= len(df_kospi_raw)
        else kospi_stats + [""] * (len(df_kospi_raw) - len(kospi_stats))
    )
    df_kosdaq_raw = _kosdaq_mst_to_df(kosdaq_mst)
    kosdaq_stats = _extract_iscd_stat_from_mst(kosdaq_mst)
    df_kosdaq_raw["iscd_stat_cls_code"] = (
        kosdaq_stats[: len(df_kosdaq_raw)]
        if len(kosdaq_stats) >= len(df_kosdaq_raw)
        else kosdaq_stats + [""] * (len(df_kosdaq_raw) - len(kosdaq_stats))
    )
    kospi_csv = os.path.join(out_dir, "kospi_code.csv")
    kosdaq_csv = os.path.join(out_dir, "kosdaq_code.csv")
    df_kospi_raw.to_csv(kospi_csv, index=False, encoding="utf-8-sig")
    df_kosdaq_raw.to_csv(kosdaq_csv, index=False, encoding="utf-8-sig")

    nxt_codes: set[str] = set()
    if os.path.exists(nxt_kospi_mst):
        df_nxt_kospi = _kospi_mst_to_df(nxt_kospi_mst)
        df_nxt_kospi.to_csv(
            os.path.join(out_dir, "nxt_kospi_code.csv"),
            index=False,
            encoding="utf-8-sig",
        )
        nxt_codes |= set(df_nxt_kospi["단축코드"].astype(str).map(_normalize_code))
    if os.path.exists(nxt_kosdaq_mst):
        df_nxt_kosdaq = _kosdaq_mst_to_df(nxt_kosdaq_mst)
        df_nxt_kosdaq.to_csv(
            os.path.join(out_dir, "nxt_kosdaq_code.csv"),
            index=False,
            encoding="utf-8-sig",
        )
        nxt_codes |= set(df_nxt_kosdaq["단축코드"].astype(str).map(_normalize_code))

    df_kospi = _build_output(df_kospi_raw, "KOSPI", {
        "code": "단축코드",
        "std_code": "표준코드",
        "name": "한글명",
        "iscd_stat_cls_code": "iscd_stat_cls_code",
        "group": "그룹코드",
        "market_cap_size": "시가총액규모",
        "sector_l": "지수업종대분류",
        "sector_m": "지수업종중분류",
        "sector_s": "지수업종소분류",
        "venture": "제조업",
        "low_liquidity": "저유동성",
        "krx": "KRX",
        "etp": "ETP",
        "krx100": "KRX100",
        "krx300": "KRX300",
        "base_price": "기준가",
        "lot_size": "매매수량단위",
        "after_hours_lot_size": "시간외수량단위",
        "halted": "거래정지",
        "delisting": "정리매매",
        "managed": "관리종목",
        "warning": "시장경고",
        "warning_notice": "경고예고",
        "disclosure": "불성실공시",
        "backdoor": "우회상장",
        "lock": "락구분",
        "par_change": "액면변경",
        "capital_increase": "증자구분",
        "margin_ratio": "증거금비율",
        "credit_possible": "신용가능",
        "credit_period": "신용기간",
        "prev_volume": "전일거래량",
        "par_value": "액면가",
        "listed_date": "상장일자",
        "listed_shares": "상장주수",
        "capital": "자본금",
        "fiscal_month": "결산월",
        "ipo_price": "공모가",
        "preferred": "우선주",
        "short_overheat": "공매도과열",
        "surge": "이상급등",
        "market_cap": "시가총액",
        "group_company_code": "그룹사코드",
        "credit_limit_over": "회사신용한도초과",
        "collateral_loan": "담보대출가능",
        "borrowable": "대주가능",
    }, nxt_codes)

    df_kosdaq = _build_output(df_kosdaq_raw, "KOSDAQ", {
        "code": "단축코드",
        "std_code": "표준코드",
        "name": "한글종목명",
        "iscd_stat_cls_code": "iscd_stat_cls_code",
        "group": "증권그룹구분코드",
        "market_cap_size": "시가총액 규모 구분 코드 유가",
        "sector_l": "지수업종 대분류 코드",
        "sector_m": "지수 업종 중분류 코드",
        "sector_s": "지수업종 소분류 코드",
        "venture": "벤처기업 여부 (Y/N)",
        "low_liquidity": "저유동성종목 여부",
        "krx": "KRX 종목 여부",
        "etp": "ETP 상품구분코드",
        "krx100": "KRX100 종목 여부 (Y/N)",
        "krx300": "KRX300 종목 여부 (Y/N)",
        "base_price": "주식 기준가",
        "lot_size": "정규 시장 매매 수량 단위",
        "after_hours_lot_size": "시간외 시장 매매 수량 단위",
        "halted": "거래정지 여부",
        "delisting": "정리매매 여부",
        "managed": "관리 종목 여부",
        "warning": "시장 경고 구분 코드",
        "warning_notice": "시장 경고위험 예고 여부",
        "disclosure": "불성실 공시 여부",
        "backdoor": "우회 상장 여부",
        "lock": "락구분 코드",
        "par_change": "액면가 변경 구분 코드",
        "capital_increase": "증자 구분 코드",
        "margin_ratio": "증거금 비율",
        "credit_possible": "신용주문 가능 여부",
        "credit_period": "신용기간",
        "prev_volume": "전일 거래량",
        "par_value": "주식 액면가",
        "listed_date": "주식 상장 일자",
        "listed_shares": "상장 주수(천)",
        "capital": "자본금",
        "fiscal_month": "결산 월",
        "ipo_price": "공모 가격",
        "preferred": "우선주 구분 코드",
        "short_overheat": "공매도과열종목여부",
        "surge": "이상급등종목여부",
        "market_cap": "전일기준 시가총액 (억)",
        "group_company_code": "그룹사 코드",
        "credit_limit_over": "회사신용한도초과여부",
        "collateral_loan": "담보대출가능여부",
        "borrowable": "대주가능여부",
    }, nxt_codes)

    df = pd.concat([df_kospi, df_kosdaq], ignore_index=True)
    df = df.dropna(subset=["code", "name"]).drop_duplicates(subset=["code"]).reset_index(drop=True)

    parquet_path = os.path.join(out_dir, "latest.parquet")
    csv_path = os.path.join(out_dir, "latest.csv")
    krx_parquet = os.path.join(out_dir, "KRX_code.parquet")
    krx_csv = os.path.join(out_dir, "KRX_code.csv")
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_parquet(krx_parquet, index=False)
    df.to_csv(krx_csv, index=False, encoding="utf-8-sig")

    print(f"[symbol_master] parquet: {parquet_path} rows={len(df)}")
    print(f"[symbol_master] csv: {csv_path} rows={len(df)}")
    print(f"[symbol_master] KRX parquet: {krx_parquet} rows={len(df)}")
    print(f"[symbol_master] KRX csv: {krx_csv} rows={len(df)}")

    # daily_KRX_code_DB.parquet: 날짜별 축적 (해당 일자 데이터 이미 있으면 추가 제외)
    daily_db_path = os.path.join(out_dir, "daily_KRX_code_DB.parquet")
    _append_to_daily_db(df, daily_db_path)


if __name__ == "__main__":
    _err = None
    try:
        main()
    except Exception as e:
        _err = e
        raise
    finally:
        if tmsg is not None:
            try:
                msg = f"[{PROGRAM_NAME}] 프로그램 종료" if _err is None else f"[{PROGRAM_NAME}] 프로그램 종료 (오류: {_err})"
                tmsg(msg, "-t")
            except Exception:
                pass
