"""
변동성완화장치(VI) 현황 조회 [국내주식-055]

KIS REST API를 이용하여 VI 발동/해제 현황을 조회하고 출력합니다.
- TR ID: FHPST01390000
- URL: /uapi/domestic-stock/v1/quotations/inquire-vi-status

※ 아래 [조회 옵션] 을 직접 수정하거나, CLI 인자로 override 가능
  python inquire_vi_status.py                        # 상단 옵션 그대로 실행
  python inquire_vi_status.py --code 005930          # 종목코드만 override
  python inquire_vi_status.py --market K --date 20260303
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

# ── 프로젝트 경로 ────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig

# ══════════════════════════════════════════════════════════════════
#  [조회 옵션] — 여기서 직접 설정 (CLI 인자가 없으면 이 값 사용)
# ══════════════════════════════════════════════════════════════════
OPT_CODE      = ""          # 종목코드 (빈값=전체, 예: "005930")
OPT_MARKET    = "0"         # 시장구분  0:전체 / K:코스피 / Q:코스닥
OPT_DIRECTION = "1"         # 방향구분  0:전체 / 1:상승 / 2:하락
OPT_VI_TYPE   = "0"         # VI종류   0:전체 / 1:정적 / 2:동적 / 3:정적&동적
OPT_DATE      = "20260303"          # 영업일자  YYYYMMDD (빈값=당일, 예: "20260303")
OPT_CSV_ONLY  = False       # True: 화면 출력 없이 CSV만 저장
# ══════════════════════════════════════════════════════════════════

# ── 상수 ─────────────────────────────────────────────────────────
_KST = timezone(timedelta(hours=9))
_CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

API_URL = "/uapi/domestic-stock/v1/quotations/inquire-vi-status"
TR_ID = "FHPST01390000"

# VI 상태 코드 매핑 (실제 API 응답 값 기준)
VI_STATUS_MAP = {
    "N": "해제",
    "Y": "발동 중",
    "0": "해제",
    "1": "정적VI 발동",
    "2": "동적VI 발동",
    "3": "정적&동적VI 발동",
}

VI_KIND_MAP = {
    "1": "정적VI",
    "2": "동적VI",
    "3": "정적&동적VI",
}

MARKET_MAP = {
    "0": "전체",
    "K": "코스피",
    "Q": "코스닥",
}

DIRECTION_MAP = {
    "0": "전체",
    "1": "상승",
    "2": "하락",
}


def _load_config() -> dict:
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _build_client(cfg: dict) -> KisClient:
    """config.json에서 main 계좌 기준으로 KisClient 생성."""
    user_key = cfg.get("default_user", "sywems12")
    user = cfg["users"][user_key]
    acct = user["accounts"]["main"]
    token_path = os.path.join(BASE_DIR, "kis_token_main.json")
    return KisClient(
        KisConfig(
            appkey=acct["appkey"],
            appsecret=acct["appsecret"],
            base_url=cfg.get("base_url", DEFAULT_BASE_URL),
            token_cache_path=token_path,
        )
    )


def inquire_vi_status(
    client: KisClient,
    *,
    fid_input_iscd: str = "",             # 종목코드 (빈값=전체)
    fid_mrkt_cls_code: str = "0",       # 시장구분 (0:전체, K:코스피, Q:코스닥)
    fid_div_cls_code: str = "0",        # 방향구분 (0:전체, 1:상승, 2:하락)
    fid_rank_sort_cls_code: str = "0",  # VI종류 (0:전체, 1:정적, 2:동적, 3:정적&동적)
    fid_input_date_1: str = "",         # 영업일자 YYYYMMDD (빈값=당일)
) -> pd.DataFrame:
    """VI 현황 조회 (페이지네이션 포함)."""

    if not fid_input_date_1:
        fid_input_date_1 = datetime.now(_KST).strftime("%Y%m%d")

    url = f"{client.cfg.base_url}{API_URL}"

    params = {
        "FID_COND_SCR_DIV_CODE": "20139",
        "FID_INPUT_ISCD": fid_input_iscd,
        "FID_MRKT_CLS_CODE": fid_mrkt_cls_code,
        "FID_DIV_CLS_CODE": fid_div_cls_code,
        "FID_RANK_SORT_CLS_CODE": fid_rank_sort_cls_code,
        "FID_INPUT_DATE_1": fid_input_date_1,
        "FID_TRGT_CLS_CODE": "",
        "FID_TRGT_EXLS_CLS_CODE": "",
    }

    all_rows: list[dict] = []
    tr_cont = ""
    max_pages = 10

    for page in range(max_pages):
        headers = client._headers(tr_id=TR_ID)
        if tr_cont:
            headers["tr_cont"] = "N"
        else:
            headers["tr_cont"] = ""

        try:
            r = client.session.get(url, headers=headers, params=params, timeout=client.cfg.timeout_sec)
            r.raise_for_status()
            j = r.json()
        except Exception as e:
            print(f"[VI조회] API 호출 실패: {e}")
            break

        if str(j.get("rt_cd")) != "0":
            print(f"[VI조회] API 오류: {j.get('msg1', '')} (rt_cd={j.get('rt_cd')})")
            break

        output = j.get("output") or []
        if not output:
            break

        all_rows.extend(output)

        # 연속조회 확인
        tr_cont_resp = r.headers.get("tr_cont", "")
        if tr_cont_resp == "M":
            tr_cont = "N"
            time.sleep(0.5)  # 초당 호출 제한 방지
        else:
            break

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    return df


def _format_time(t: str) -> str:
    """HHMMSS → HH:MM:SS 형식으로 변환."""
    t = str(t).strip()
    if len(t) == 6:
        return f"{t[:2]}:{t[2:4]}:{t[4:6]}"
    return t


def _format_number(val) -> str:
    """숫자를 천단위 콤마 포함 문자열로 변환."""
    try:
        n = float(val)
        if n == int(n):
            return f"{int(n):,}"
        return f"{n:,.2f}"
    except (ValueError, TypeError):
        return str(val)


def display_vi_status(df: pd.DataFrame, date_str: str) -> None:
    """VI 현황 DataFrame을 보기 좋게 출력."""
    if df.empty:
        print(f"\n조회된 VI 현황 데이터가 없습니다. (날짜: {date_str})")
        return

    print(f"\n{'='*90}")
    print(f"  변동성완화장치(VI) 현황  —  조회일: {date_str}")
    print(f"{'='*90}")

    total_count = len(df)

    # VI 발동 중 / 해제 건수
    vi_active = 0
    vi_released = 0
    if "vi_cls_code" in df.columns:
        vi_active = len(df[df["vi_cls_code"].astype(str).isin(["Y", "1", "2", "3"])])
        vi_released = len(df[df["vi_cls_code"].astype(str).isin(["N", "0"])])

    print(f"  총 {total_count}건  |  발동 중: {vi_active}건  |  해제: {vi_released}건")
    print(f"{'-'*90}")

    # 테이블 헤더
    header_fmt = f"{'번호':>4}  {'종목명':<14} {'코드':^8} {'VI상태':<12} {'VI종류':<10} " \
                 f"{'발동시간':^10} {'해제시간':^10} {'발동가격':>10} {'기준가':>10} {'괴리율':>8} {'횟수':>4}"
    print(header_fmt)
    print(f"{'-'*90}")

    for idx, (_, row) in enumerate(df.iterrows(), 1):
        name = str(row.get("hts_kor_isnm", "")).strip()[:12]
        code = str(row.get("mksc_shrn_iscd", "")).strip()
        vi_status = VI_STATUS_MAP.get(str(row.get("vi_cls_code", "")).strip(), str(row.get("vi_cls_code", "")))
        vi_kind = VI_KIND_MAP.get(str(row.get("vi_kind_code", "")).strip(), str(row.get("vi_kind_code", "")))
        vi_hour = _format_time(row.get("cntg_vi_hour", ""))
        vi_cancel = _format_time(row.get("vi_cncl_hour", ""))
        vi_prc = _format_number(row.get("vi_prc", ""))

        # 정적 VI 기준가 / 괴리율 우선, 없으면 동적
        vi_stnd = row.get("vi_stnd_prc", "")
        vi_dprt = row.get("vi_dprt", "")
        if not str(vi_stnd).strip() or str(vi_stnd).strip() == "0":
            vi_stnd = row.get("vi_dmc_stnd_prc", "")
            vi_dprt = row.get("vi_dmc_dprt", "")

        vi_stnd_str = _format_number(vi_stnd)
        vi_dprt_str = f"{float(vi_dprt):.2f}%" if vi_dprt and str(vi_dprt).strip() not in ("", "0") else ""
        vi_cnt = str(row.get("vi_count", "")).strip()

        # 발동 중인 건은 강조 표시
        marker = " *" if str(row.get("vi_cls_code", "")).strip() in ("Y", "1", "2", "3") else "  "

        line = f"{idx:>4}{marker}{name:<14} {code:^8} {vi_status:<12} {vi_kind:<10} " \
               f"{vi_hour:^10} {vi_cancel:^10} {vi_prc:>10} {vi_stnd_str:>10} {vi_dprt_str:>8} {vi_cnt:>4}"
        print(line)

    print(f"{'-'*90}")
    print(f"  * = 현재 VI 발동 중")
    print(f"{'='*90}")

    # 상세 데이터도 CSV로 저장
    csv_name = f"vi_status_{date_str}.csv"
    csv_path = os.path.join(BASE_DIR, csv_name)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n  CSV 저장: {csv_path}")


def main():
    parser = argparse.ArgumentParser(description="변동성완화장치(VI) 현황 조회 [국내주식-055]")
    parser.add_argument("--code", default=None, help="종목코드 (빈값=전체, 예: 005930)")
    parser.add_argument("--market", default=None, choices=["0", "K", "Q"],
                        help="시장구분 (0:전체, K:코스피, Q:코스닥)")
    parser.add_argument("--direction", default=None, choices=["0", "1", "2"],
                        help="방향구분 (0:전체, 1:상승, 2:하락)")
    parser.add_argument("--vi-type", default=None, choices=["0", "1", "2", "3"],
                        help="VI종류 (0:전체, 1:정적, 2:동적, 3:정적&동적)")
    parser.add_argument("--date", default=None,
                        help="영업일자 YYYYMMDD (빈값=당일)")
    parser.add_argument("--csv-only", action="store_true", default=None,
                        help="화면 출력 없이 CSV만 저장")

    args = parser.parse_args()

    # CLI 인자가 있으면 override, 없으면 상단 OPT_ 설정 사용
    code      = args.code      if args.code is not None      else OPT_CODE
    market    = args.market    if args.market is not None    else OPT_MARKET
    direction = args.direction if args.direction is not None else OPT_DIRECTION
    vi_type   = args.vi_type   if args.vi_type is not None   else OPT_VI_TYPE
    date_str  = args.date      if args.date is not None      else OPT_DATE
    csv_only  = args.csv_only  if args.csv_only              else OPT_CSV_ONLY

    # config 로드 & 클라이언트 생성
    cfg = _load_config()
    client = _build_client(cfg)

    date_str = date_str or datetime.now(_KST).strftime("%Y%m%d")

    print(f"[VI조회] 조회 시작 — 날짜: {date_str}, "
          f"시장: {MARKET_MAP.get(market, market)}, "
          f"방향: {DIRECTION_MAP.get(direction, direction)}, "
          f"VI종류: {VI_KIND_MAP.get(vi_type, '전체')}, "
          f"종목: {code or '전체'}")

    df = inquire_vi_status(
        client,
        fid_input_iscd=code,
        fid_mrkt_cls_code=market,
        fid_div_cls_code=direction,
        fid_rank_sort_cls_code=vi_type,
        fid_input_date_1=date_str,
    )

    if not csv_only:
        display_vi_status(df, date_str)
    else:
        if df.empty:
            print("조회된 데이터 없음")
        else:
            csv_name = f"vi_status_{date_str}.csv"
            csv_path = os.path.join(BASE_DIR, csv_name)
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            print(f"CSV 저장 완료: {csv_path} ({len(df)}건)")


if __name__ == "__main__":
    main()
