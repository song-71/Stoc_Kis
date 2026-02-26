"""
현재가 조회 스크립트 (KIS API 직접 호출)

[목적]
- KOSPI/KOSDAQ 전체 종목(ST 그룹) 중 전일 종가 대비 +29% 이상 종목을 출력합니다.

[데이터 소스]
- parquet 파일을 읽지 않습니다.
- KIS 서버 API를 통해 종목별 현재가를 직접 조회합니다.

[처리 흐름]
1) config.json 로드 → appkey/appsecret 확인
2) symbol_master 로드 → KOSPI/KOSDAQ + ST 그룹 종목 리스트 구성
3) 각 종목에 대해 KIS 현재가(inquire_price) 호출
4) 전일종가 대비 +threshold 이상 종목만 필터링
5) 결과를 DataFrame으로 정렬 후 콘솔출력(파일저장은 없음.)

[주요 옵션]
- --market: kospi/kosdaq/all (기본 all)
- --threshold: 기준 상승률(기본 0.29 = 29%)
- --sleep_sec: API 호출 딜레이(기본 0.2초)
- --limit: 테스트용 종목 수 제한
"""

import argparse
import os
from typing import Any, Dict, List

import pandas as pd

from kis_ohlcv_download import KisClient, KisConfig
from kis_utils import load_config, load_symbol_master


def _to_float(val: Any) -> float:
    try:
        return float(val)
    except Exception:
        return 0.0


def _pick(row: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return default


def _extract_row(code: str, name: str, market: str, out: Dict[str, Any]) -> Dict[str, Any]:
    cur_price = _to_float(_pick(out, "stck_prpr", "prpr", "last", default=0))
    prev_close = _to_float(
        _pick(
            out,
            "stck_prdy_clpr",
            "stck_clpr",
            "prdy_clpr",
            "prdy_close",
            default=0,
        )
    )
    chg_rate = _to_float(_pick(out, "prdy_ctrt", "prdy_ctrt", "change_rate", default=0))
    if prev_close > 0 and chg_rate == 0:
        chg_rate = (cur_price / prev_close) - 1

    return {
        "code": code,
        "name": name,
        "market": market,
        "prev_close": prev_close,
        "current": cur_price,
        "today_ret": chg_rate,
        "open": _to_float(_pick(out, "stck_oprc", "open", default=0)),
        "high": _to_float(_pick(out, "stck_hgpr", "high", default=0)),
        "low": _to_float(_pick(out, "stck_lwpr", "low", default=0)),
        "volume": _to_float(_pick(out, "acml_vol", "volume", default=0)),
        "value": _to_float(_pick(out, "acml_tr_pbmn", "acml_tr_pbmn_amt", "value", default=0)),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="현재가 조회: 전일 종가 대비 +29% 이상 종목 출력")
    ap.add_argument("--config", default="config.json", help="설정 파일 경로")
    ap.add_argument("--market", default="all", choices=["kospi", "kosdaq", "all"])
    ap.add_argument("--threshold", type=float, default=0.29, help="전일 종가 대비 비율 (0.29=29%%)")
    ap.add_argument("--sleep_sec", type=float, default=0.2, help="API 호출 딜레이(초)")
    ap.add_argument("--limit", type=int, default=None, help="테스트용 종목 제한")
    args = ap.parse_args()

    config_path = args.config
    if not os.path.isabs(config_path) and not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_path)
    cfg = load_config(config_path)
    appkey = cfg.get("appkey")
    appsecret = cfg.get("appsecret")
    if not appkey or not appsecret:
        raise ValueError("appkey/appsecret이 필요합니다. config.json에 설정하세요.")

    symbol_df = load_symbol_master(args.config)

    results: List[Dict[str, Any]] = []
    for market in ("KOSPI", "KOSDAQ"):
        if args.market != "all" and args.market.upper() != market:
            continue
        market_div = "J"
        client = KisClient(KisConfig(appkey=appkey, appsecret=appsecret, market_div=market_div))
        codes = (
            symbol_df[(symbol_df["market"] == market) & (symbol_df["group"] == "ST")][["code", "name"]]
            .astype(str)
            .values
            .tolist()
        )
        if args.limit:
            codes = codes[: args.limit]
        for code, name in codes:
            out = client.inquire_price(code)
            row = _extract_row(code, name, market, out)
            if row["prev_close"] > 0 and row["current"] >= row["prev_close"] * (1 + args.threshold):
                results.append(row)
            if args.sleep_sec > 0:
                import time

                time.sleep(args.sleep_sec)

    if not results:
        print("[result] 해당 종목 없음")
        return

    df = pd.DataFrame(results)
    df = df.sort_values(["market", "today_ret", "code"], ascending=[True, False, True]).reset_index(drop=True)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
