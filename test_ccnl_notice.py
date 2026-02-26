#!/usr/bin/env python3
"""
H0STCNI0 체결통보 구독 연결 테스트

테스트 대상:
1. _get_ccnl_notice_tr_key(): config에서 tr_key 추출
2. ccnl_notice 구독: 웹소켓 연결 후 H0STCNI0 SUBSCRIBE 결과 확인

실행: python test_ccnl_notice.py
옵션: --tr-key-only  (1번만, 네트워크 없이)
      --tr-key sywems12 (직접 지정, 기본값: config의 my_htsid)

주의: ws_realtime_subscribe_to_DB-1.py 등 다른 프로그램이 동일 appkey로
      연결 중이면 "ALREADY IN USE appkey" 에러가 납니다. 해당 프로그램을
      중지한 뒤 테스트하세요.
"""
import argparse
import logging
import sys
import threading
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(Path.home() / "open-trading-api" / "examples_user" / "domestic_stock"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# websocket 라이브러리 로그 억제
logging.getLogger("websockets").setLevel(logging.WARNING)


def test_get_tr_key() -> tuple[bool, str]:
    """1단계: config에서 tr_key(my_htsid) 추출 테스트 (네트워크 없음)"""
    try:
        from kis_utils import load_config

        CONFIG_PATH = SCRIPT_DIR / "config.json"
        cfg = load_config(str(CONFIG_PATH))
        my_htsid = str(cfg.get("my_htsid", "") or "").strip()
        if not my_htsid:
            return False, "config에 my_htsid 없음 (HTS 로그인 ID)"
        return True, my_htsid
    except Exception as e:
        return False, str(e)


def test_ccnl_notice_connection(tr_key: str, timeout_sec: float = 8.0) -> tuple[bool, str]:
    """2단계: 웹소켓 구독 연결 테스트"""
    result = {"ok": False, "msg": "", "received": []}

    def on_system(rsp):
        result["received"].append({
            "tr_id": getattr(rsp, "tr_id", ""),
            "tr_key": getattr(rsp, "tr_key", ""),
            "isOk": getattr(rsp, "isOk", False),
            "msg": getattr(rsp, "tr_msg", ""),
        })
        if rsp.tr_id == "H0STCNI0":
            result["ok"] = rsp.isOk and rsp.tr_msg and "SUCCESS" in (rsp.tr_msg or "")
            result["msg"] = rsp.tr_msg or ""

    def on_result(ws, tr_id, df, data_info):
        pass  # 데이터 수신 무시 (체결 시에만 옴)

    def run_ws():
        import kis_auth_llm as ka

        ka.open_map.clear()
        ka.add_open_map("ccnl_notice", ccnl_notice, [tr_key])

        kws = ka.KISWebSocket(api_url="", max_retries=1)
        kws.on_system = on_system

        def _run():
            try:
                kws.start(on_result=on_result)
            except Exception as e:
                result["msg"] = str(e)
                result["ok"] = False

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=timeout_sec)
        if t.is_alive():
            kws.close()
            t.join(timeout=2.0)
        return result

    try:
        # 인증 및 domestic_stock import
        from kis_utils import load_config

        cfg = load_config(str(SCRIPT_DIR / "config.json"))
        appkey = cfg.get("appkey")
        appsecret = cfg.get("appsecret")
        if not appkey or not appsecret:
            return False, "config에 appkey/appsecret 없음"

        import kis_auth_llm as ka

        sys.modules["kis_auth"] = ka
        ka._cfg["my_app"] = appkey
        ka._cfg["my_sec"] = appsecret
        ka.token_tmp = str(SCRIPT_DIR / "kis_token_main.json")
        ka.auth(svr="prod")
        ka.auth_ws(svr="prod")

        if not getattr(ka, "_base_headers_ws", {}).get("approval_key"):
            return False, "auth_ws() 실패 - approval_key 없음"

        from domestic_stock_functions_ws import ccnl_notice  # noqa: F401

        run_ws()

        if result["ok"]:
            return True, f"SUBSCRIBE SUCCESS (tr_key={tr_key})"
        if result["received"]:
            last = result["received"][-1]
            return False, f"tr_key={tr_key} 응답: isOk={last.get('isOk')} msg={last.get('msg')}"
        return False, result["msg"] or "응답 수신 없음 (타임아웃 또는 연결 실패)"
    except Exception as e:
        return False, str(e)


def main():
    parser = argparse.ArgumentParser(description="H0STCNI0 체결통보 구독 연결 테스트")
    parser.add_argument("--tr-key-only", action="store_true", help="tr_key 추출만 테스트 (네트워크 X)")
    parser.add_argument("--tr-key", type=str, default="", help="직접 tr_key 지정 (예: 005930 또는 4344482201)")
    parser.add_argument("--timeout", type=float, default=8.0, help="웹소켓 대기 초")
    args = parser.parse_args()

    print("=" * 60)
    print("H0STCNI0 체결통보 구독 연결 테스트")
    print("=" * 60)

    # 1. tr_key
    if args.tr_key:
        tr_key = args.tr_key.strip()
        print(f"\n[1] tr_key (직접 지정): {tr_key}")
    else:
        ok, tr_key = test_get_tr_key()
        print(f"\n[1] tr_key 추출: {'OK' if ok else 'FAIL'}")
        print(f"    결과: {tr_key}")
        if not ok:
            print("\n[종료] config 확인 필요")
            return 1

    if args.tr_key_only:
        print("\n[완료] --tr-key-only 모드")
        return 0

    # 2. 연결 테스트
    print(f"\n[2] H0STCNI0 구독 연결 테스트 (최대 {args.timeout}초)...")
    ok, msg = test_ccnl_notice_connection(tr_key, args.timeout)
    print(f"    결과: {'OK' if ok else 'FAIL'}")
    print(f"    메시지: {msg}")

    print("\n" + "=" * 60)
    if ok:
        print("테스트 통과")
        return 0
    print("테스트 실패")
    if "ALREADY IN USE appkey" in msg:
        print("  ※ 다른 프로그램(ws_realtime 등)이 같은 appkey로 연결 중이면 이 에러가 납니다.")
        print("    → 해당 프로그램을 중지한 뒤 다시 테스트하세요.")
    print("  tr_key = my_htsid (HTS 로그인 ID, 예: --tr-key sywems12)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
