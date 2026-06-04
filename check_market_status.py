"""
장 상태 확인 스크립트 (REST 1회 조회)
- 삼성전자(005930) 현재가 조회 (FHKST01010100)
- 장 상태 관련 필드 전체 표시

사용법:
    python check_market_status.py
"""
import json
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig

KST = ZoneInfo("Asia/Seoul")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── 코드 → 설명 매핑 ──
ISCD_STAT = {
    "51": "관리종목", "52": "투자위험", "53": "투자경고", "54": "투자주의",
    "55": "신용가능", "57": "증거금100%", "58": "거래정지", "59": "단기과열종목",
}
MRKT_WARN = {
    "00": "없음", "01": "투자주의", "02": "투자경고",
    "03": "투자위험", "04": "투자주의환기",
}


def init_client() -> KisClient:
    with open(os.path.join(SCRIPT_DIR, "config.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    acct = cfg["users"][cfg["default_user"]]["accounts"]["a1"]
    return KisClient(KisConfig(
        appkey=acct["appkey"], appsecret=acct["appsecret"],
        base_url=cfg.get("base_url", DEFAULT_BASE_URL),
        token_cache_path=os.path.join(SCRIPT_DIR, "kis_token_a1.json"),
    ))


def main():
    now = datetime.now(KST)
    print(f"=== 장 상태 확인 ({now.strftime('%Y-%m-%d %H:%M:%S')}) ===\n")

    client = init_client()
    out = client.inquire_price("005930")

    # 현재가 기본 정보
    name = out.get("hts_kor_isnm", "삼성전자")
    price = out.get("stck_prpr", "")
    if price:
        sign_map = {"1": "▲", "2": "▲", "3": " ", "4": "▼", "5": "▼"}
        sign = sign_map.get(out.get("prdy_vrss_sign", ""), "")
        print(f"  {name} 현재가: {int(price):,}원  {sign}{out.get('prdy_vrss', '')} ({out.get('prdy_ctrt', '')}%)")
    else:
        print(f"  {name} 현재가: 장외시간")

    # 장 상태 관련 필드 전체 표시
    fields = [
        ("iscd_stat_cls_code", "종목 상태 구분 코드", ISCD_STAT),
        ("temp_stop_yn",       "임시 정지 여부",     None),
        ("oprc_rang_cont_yn",  "시가 범위 연장 여부", None),
        ("clpr_rang_cont_yn",  "종가 범위 연장 여부", None),
        ("mrkt_warn_cls_code", "시장경고코드",        MRKT_WARN),
        ("short_over_yn",      "단기과열여부",        None),
        ("invt_caful_yn",      "투자유의여부",        None),
        ("vi_cls_code",        "VI적용구분코드",      None),
        ("ovtm_vi_cls_code",   "시간외단일가VI적용구분코드", None),
        ("crdt_able_yn",       "신용 가능 여부",      None),
        ("sltr_yn",            "정리매매여부",        None),
        ("mang_issu_cls_code", "관리종목여부",        None),
        ("ssts_yn",            "공매도가능여부",      None),
    ]

    print(f"\n{'─' * 60}")
    for key, label, code_map in fields:
        val = out.get(key, "")
        desc = ""
        if code_map and val in code_map:
            desc = f" ({code_map[val]})"
        print(f"  {label:28s} ({key:24s}) = {val}{desc}")
    print(f"{'─' * 60}")


if __name__ == "__main__":
    main()
