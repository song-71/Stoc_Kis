#!/usr/bin/env python3
"""
H0STCNI0 체결통보 구독 연결 테스트

주의: ws_realtime_trading.py 등 다른 프로그램이 동일 appkey로
      연결 중이면 "ALREADY IN USE appkey" 에러가 납니다. 해당 프로그램을
      중지한 뒤 테스트하세요.
"""
import argparse
import json as _json
import logging
import sys
import threading
import time
from pathlib import Path

# ============================================================
# ■ 설정 옵션 (여기서 수정)
# ============================================================
# 주문 옵션: 1=매수, 2=매도
TRADING_OPTION = 2

# 주문 종목/수량 설정
ORDERS = [("285800", 1),    # ("종목코드", 수량),
]

# 계좌 선택: "main" (43444822) 또는 "syw_2" (63614390)
ACCOUNT = "syw_2"

# tr_key: 체결통보 구독 키 (HTS 로그인 ID). 빈 문자열이면 config에서 자동 추출
TR_KEY = ""



# 웹소켓 대기 시간 (초)
TIMEOUT_SEC = 8.0

# 매수 주문 후 체결통보 대기 시간 (초)
CCNL_WAIT_SEC = 15.0
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(Path.home() / "open-trading-api" / "examples_user" / "domestic_stock"))

LOG_FILE = SCRIPT_DIR / "test_ccnl_notice.log"
_log_fmt = logging.Formatter("%(asctime)s %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
_sh = logging.StreamHandler()
_sh.setFormatter(_log_fmt)
_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(_log_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_sh, _fh])
logger = logging.getLogger(__name__)

# websocket 라이브러리 로그 억제
logging.getLogger("websockets").setLevel(logging.WARNING)


def _load_account_config() -> dict:
    """config.json에서 ACCOUNT에 해당하는 계좌 설정 로드"""
    from kis_utils import load_config

    cfg = load_config(str(SCRIPT_DIR / "config.json"), account_id=ACCOUNT)
    if not cfg.get("appkey"):
        raise ValueError(f"계좌 '{ACCOUNT}' 을 config에서 찾을 수 없음")
    return cfg


def test_get_tr_key() -> tuple[bool, str]:
    """1단계: config에서 tr_key(my_htsid) 추출 테스트 (네트워크 없음)"""
    try:
        acct = _load_account_config()
        my_htsid = acct.get("htsid", "").strip()
        if not my_htsid:
            return False, "config에 htsid 없음 (HTS 로그인 ID)"
        return True, my_htsid
    except Exception as e:
        return False, str(e)


def submit_orders() -> list[tuple[str, int, bool, str]]:
    """ORDERS에 설정된 주문 실행. [(종목코드, 수량, 성공여부, 메시지)] 반환"""
    if not ORDERS:
        return []

    import requests
    from kis_API_ohlcv_download_Utils import KisClient, KisConfig

    results = []
    try:
        acct = _load_account_config()
        cano = acct.get("cano", "")
        acnt_prdt_cd = acct.get("acnt_prdt_cd", "01") or "01"
        base_url = acct.get("base_url", "https://openapi.koreainvestment.com:9443")
        appkey = acct.get("appkey", "")
        appsecret = acct.get("appsecret", "")

        token_name = "kis_token_main.json" if ACCOUNT == "main" else "kis_token_syw2.json"
        cfg = KisConfig(
            appkey=appkey, appsecret=appsecret,
            base_url=base_url,
            token_cache_path=str(SCRIPT_DIR / token_name),
        )
        client = KisClient(cfg)

        def _hashkey(body: dict) -> str:
            url = f"{base_url}/uapi/hashkey"
            r = requests.post(url, headers={"content-type": "application/json", "appkey": appkey, "appsecret": appsecret},
                              data=_json.dumps(body), timeout=10)
            r.raise_for_status()
            j = r.json()
            return j.get("HASH") or j.get("hash") or ""

        tr_id = "TTTC0802U" if TRADING_OPTION == 1 else "TTTC0801U"
        order_type = "매수" if TRADING_OPTION == 1 else "매도"
        for code, qty in ORDERS:
            code = str(code).zfill(6)
            try:
                logger.info(f"    [{order_type}] 주문 파라미터: CANO={cano}, ACNT_PRDT_CD={acnt_prdt_cd}")
                body = {"CANO": cano, "ACNT_PRDT_CD": acnt_prdt_cd,
                        "PDNO": code, "ORD_DVSN": "01", "ORD_QTY": str(int(qty)), "ORD_UNPR": "0"}
                headers = client._headers(tr_id=tr_id)
                headers["hashkey"] = _hashkey(body)
                headers["content-type"] = "application/json"
                url = f"{base_url}/uapi/domestic-stock/v1/trading/order-cash"
                r = requests.post(url, headers=headers, data=_json.dumps(body), timeout=10)
                r.raise_for_status()
                resp = r.json()
                rt_cd = resp.get("rt_cd", "")
                msg1 = resp.get("msg1", "")
                ok = rt_cd == "0"
                results.append((code, qty, ok, msg1))
            except Exception as e:
                results.append((code, qty, False, str(e)))
    except Exception as e:
        for code, qty in ORDERS:
            results.append((str(code).zfill(6), qty, False, str(e)))
    return results


def run_full_test(tr_key: str) -> int:
    """WSS 구독 유지 + 매수 주문 + 체결통보 수신 통합 테스트"""
    import kis_auth_llm as ka

    # ── 인증 ──
    acct = _load_account_config()
    appkey = acct.get("appkey")
    appsecret = acct.get("appsecret")
    if not appkey or not appsecret:
        logger.error(f"계좌 '{ACCOUNT}'에 appkey/appsecret 없음")
        return 1

    # sys.modules 대체를 domestic_stock_functions_ws import 전에 수행해야
    # domestic_stock_functions_ws가 kis_auth_llm의 data_fetch를 참조함
    sys.modules["kis_auth"] = ka
    from domestic_stock_functions_ws import ccnl_notice  # noqa: F401 — must be after sys.modules override
    ka._cfg["my_app"] = appkey
    ka._cfg["my_sec"] = appsecret
    token_file = "kis_token_main.json" if ACCOUNT == "main" else f"kis_token_{ACCOUNT}.json"
    ka.token_tmp = str(SCRIPT_DIR / token_file)
    ka.auth(svr="prod")
    ka.auth_ws(svr="prod")

    if not getattr(ka, "_base_headers_ws", {}).get("approval_key"):
        logger.error("auth_ws() 실패 - approval_key 없음")
        return 1

    # ── 상태 추적 ──
    subscribe_ok = threading.Event()
    ccnl_received = threading.Event()
    ccnl_count = 0

    def on_system(rsp):
        if rsp.tr_id == "H0STCNI0":
            ok = rsp.isOk and rsp.tr_msg and "SUCCESS" in (rsp.tr_msg or "")
            logger.info(f"[H0STCNI0] 구독 응답: {rsp.tr_msg} (ok={ok})")
            if ok:
                subscribe_ok.set()

    order_qty_map = {}  # {종목코드: 주문수량} — submit_orders 결과 저장용

    def on_result(ws, tr_id, df, data_info):
        nonlocal ccnl_count
        if tr_id == "H0STCNI0":
            ccnl_count += 1
            logger.info(f"[H0STCNI0] ★ 체결통보 #{ccnl_count} 수신!")
            for _, row in df.iterrows():
                cntg_yn = str(row.get("CNTG_YN", "?"))
                seln_byov = str(row.get("SELN_BYOV_CLS", "")).strip()
                side = "매도" if seln_byov == "01" else "매수" if seln_byov == "02" else f"구분({seln_byov})"

                code = str(row.get("STCK_SHRN_ISCD", "?"))
                name = str(row.get("CNTG_ISNM40", "")).strip()
                stock_label = f"{name}({code})" if name else code

                oder_no = row.get("ODER_NO", "?")

                if cntg_yn == "2":  # 체결
                    try:
                        price = int(row.get("CNTG_UNPR", 0))
                    except (ValueError, TypeError):
                        price = 0
                    try:
                        cntg_qty = int(row.get("CNTG_QTY", 0))
                    except (ValueError, TypeError):
                        cntg_qty = 0
                    try:
                        tot_qty = int(row.get("ODER_QTY", 0)) or order_qty_map.get(code, 0)
                    except (ValueError, TypeError):
                        tot_qty = order_qty_map.get(code, 0)
                    remaining = max(tot_qty - cntg_qty, 0) if tot_qty else "?"
                    filled_info = f"{cntg_qty}/{tot_qty}, 잔여 {remaining}" if tot_qty else f"{cntg_qty}주"
                    logger.info(
                        f"  [{side}체결] {stock_label} 체결 {cntg_qty}주 x {price:,}원 ({filled_info}) | 주문번호={oder_no}"
                    )
                elif cntg_yn == "1":  # 접수
                    try:
                        qty = int(row.get("ODER_QTY", 0)) or order_qty_map.get(code, 0)
                    except (ValueError, TypeError):
                        qty = order_qty_map.get(code, 0)
                    qty_str = f" {qty}주" if qty else ""
                    logger.info(
                        f"  [{side}접수] {stock_label} 주문{qty_str} | 주문번호={oder_no}"
                    )
                else:
                    logger.info(
                        f"  [기타({cntg_yn})] {stock_label} | 주문번호={oder_no}"
                    )
            ccnl_received.set()

    # ── WSS 연결 (백그라운드 스레드) ──
    ka.open_map.clear()
    ka.add_open_map("ccnl_notice", ccnl_notice, [tr_key])

    kws = ka.KISWebSocket(api_url="", max_retries=1)
    kws.on_system = on_system

    ws_error = {}

    def _run_ws():
        try:
            kws.start(on_result=on_result)
        except Exception as e:
            ws_error["err"] = str(e)

    ws_thread = threading.Thread(target=_run_ws, daemon=True)
    ws_thread.start()

    # ── SUBSCRIBE SUCCESS 대기 ──
    logger.info(f"[2] H0STCNI0 구독 대기 (최대 {TIMEOUT_SEC}초)...")
    if not subscribe_ok.wait(timeout=TIMEOUT_SEC):
        logger.error(f"    구독 실패 (타임아웃 {TIMEOUT_SEC}초)")
        if ws_error:
            logger.error(f"    WSS 에러: {ws_error['err']}")
        kws.close()
        ws_thread.join(timeout=3.0)
        return 1
    logger.info("    구독 성공!")

    # ── 주문 (WSS 열린 상태에서) ──
    order_type = "매수" if TRADING_OPTION == 1 else "매도"
    if ORDERS:
        logger.info(f"[3] {order_type} 주문 실행 ({len(ORDERS)}건)...")
        results = submit_orders()
        any_ok = False
        for code, qty, success, msg in results:
            if success:
                order_qty_map[str(code).zfill(6)] = qty
            status = "OK" if success else "FAIL"
            logger.info(f"    {code} x {qty}주: {status} - {msg}")
            if success:
                any_ok = True

        if any_ok:
            logger.info(f"[4] 체결통보 대기 (최대 {CCNL_WAIT_SEC}초)...")
            if ccnl_received.wait(timeout=CCNL_WAIT_SEC):
                # 추가 체결통보가 더 올 수 있으므로 2초 더 대기
                time.sleep(2.0)
                logger.info(f"    ✔ 체결통보 총 {ccnl_count}건 수신 완료!")
            else:
                logger.warning(f"    ⚠ 체결통보 미수신 ({CCNL_WAIT_SEC}초 타임아웃)")
        else:
            logger.warning(f"    {order_type} 주문 전부 실패 → 체결통보 대기 스킵")

    # ── 정리 ──
    kws.close()
    ws_thread.join(timeout=3.0)

    logger.info("=" * 60)
    logger.info("테스트 완료")
    return 0


def main():
    parser = argparse.ArgumentParser(description="H0STCNI0 체결통보 구독 연결 테스트")
    parser.add_argument("--tr-key-only", action="store_true", help="tr_key 추출만 테스트 (네트워크 X)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("H0STCNI0 체결통보 구독 연결 테스트")
    logger.info(f"  계좌: {ACCOUNT}")
    order_type = "매수" if TRADING_OPTION == 1 else "매도"
    if ORDERS:
        for code, qty in ORDERS:
            logger.info(f"  {order_type}: {str(code).zfill(6)} x {qty}주")
    logger.info("=" * 60)

    # 1. tr_key
    if TR_KEY:
        tr_key = TR_KEY.strip()
        logger.info(f"[1] tr_key (직접 지정): {tr_key}")
    else:
        ok, tr_key = test_get_tr_key()
        logger.info(f"[1] tr_key 추출: {'OK' if ok else 'FAIL'}")
        logger.info(f"    결과: {tr_key}")
        if not ok:
            logger.error("[종료] config 확인 필요")
            return 1

    if args.tr_key_only:
        logger.info("[완료] --tr-key-only 모드")
        return 0

    # 2~4. WSS 구독 유지 + 매수 + 체결통보 수신 (통합)
    return run_full_test(tr_key)


if __name__ == "__main__":
    sys.exit(main())
