"""
[260427] a2-WSS 별도 OS 프로세스 (KIS 공식 multiprocessing 패턴).

- syw_2 계정 appkey 로 자체 KISWebSocket 인스턴스 생성
- H0STMKO0 (장운영정보) + H0STANC0 (예상체결가) 전담 구독
- main process 와 multiprocessing.Queue IPC 로 데이터/명령 교환
- 자식 프로세스 namespace 격리 → _base_headers_ws swap race 없음

KIS 공식 reference: legacy/websocket/python/multi_processing_sample_ws.py

IPC:
  data_queue (a2 → main): {'tr_id': str, 'rows': list[dict], 'recv_ts': str}
  cmd_queue  (main → a2):
    {'cmd': 'sub',   'tr_id': 'H0STMKO0' | 'H0STANC0', 'codes': list[str]}
    {'cmd': 'unsub', 'tr_id': 'H0STMKO0' | 'H0STANC0', 'codes': list[str]}
    {'cmd': 'stop'}                          # 정상 종료
"""
import json
import os
import sys
import time
import threading
import logging
import traceback
import asyncio
from datetime import datetime
from pathlib import Path

# Stoc_Kis 모듈 path 추가 (자식 프로세스도 동일 모듈 import 가능)
SCRIPT_DIR = str(Path(__file__).resolve().parent)
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

logger = logging.getLogger("a2-subproc")


def _setup_logger():
    """자식 프로세스 자체 logger (부모와 별도)."""
    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | [a2-subproc] %(message)s")
    handler.setFormatter(fmt)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def run_a2_subprocess(config_path: str, data_queue, cmd_queue):
    """multiprocessing.Process target.

    Args:
        config_path: config.json 절대 경로
        data_queue: a2 → main 데이터 채널 (multiprocessing.Queue)
        cmd_queue:  main → a2 명령 채널  (multiprocessing.Queue)
    """
    _setup_logger()
    pid = os.getpid()
    logger.info(f"a2 subprocess started (pid={pid})")

    try:
        # ── 1) syw_2 정보 로드 + ka._cfg override ──
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        syw2 = cfg.get("accounts", {}).get("syw_2", {})
        if not syw2:
            logger.error("config.json 에 syw_2 계정 누락 — a2 subprocess 종료")
            return

        # ka 모듈을 자식 프로세스에서 import (부모와 격리된 namespace)
        import kis_auth_llm as ka  # noqa: E402
        from domestic_stock_functions_ws import market_status_krx, exp_ccnl_krx  # noqa: E402

        # _cfg override → auth_ws 가 syw_2 키로 발급
        ka._cfg["my_app"] = syw2.get("appkey", "")
        ka._cfg["my_sec"] = syw2.get("appsecret", "")
        ka._cfg["my_acct"] = syw2.get("cano", "")
        ka._cfg["my_prod"] = syw2.get("acnt_prdt_cd", "01")
        ka._cfg["my_htsid"] = syw2.get("htsid", "")

        # syw_2 approval_key 발급
        ka.auth_ws(svr="prod")
        if not ka._base_headers_ws.get("approval_key"):
            logger.error("syw_2 approval_key 발급 실패 — a2 subprocess 종료")
            return
        logger.info("syw_2 approval_key 발급 완료")

        # ── 2) on_result 콜백: polars DataFrame → list[dict] → queue ──
        def on_result(ws, tr_id, result, data_info):
            try:
                if result is None or len(result) == 0:
                    return
                # 컬럼 lower-case 정규화 (부모 코드와 일치)
                try:
                    result = result.rename({c: c.strip().lower() for c in result.columns})
                except Exception:
                    pass
                rows = result.to_dicts() if hasattr(result, "to_dicts") else []
                if not rows:
                    return
                msg = {
                    "tr_id": str(tr_id),
                    "rows": rows,
                    "recv_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                }
                data_queue.put_nowait(msg)
            except Exception as e:
                logger.warning(f"on_result 변환 실패: {e}")

        # ── 3) KISWebSocket 인스턴스 (single, isolated) ──
        kws = ka.KISWebSocket(api_url="", max_retries=5)

        # ── 4) cmd_queue 처리 thread ──
        _stop_flag = {"stop": False}

        def _send_dynamic(req_func, codes, tr_type):
            """connect 후 동적 subscribe/unsubscribe 호출."""
            if kws._ws is None or kws._loop is None:
                logger.warning(f"WSS 미연결 상태 — 동적 send skip ({req_func.__name__})")
                return
            try:
                fut = asyncio.run_coroutine_threadsafe(
                    kws.send_multiple(kws._ws, req_func, tr_type, list(codes), None),
                    kws._loop,
                )
                fut.result(timeout=5.0)
            except Exception as e:
                logger.warning(f"동적 send 실패 ({req_func.__name__}, {tr_type}): {e}")

        def cmd_loop():
            while not _stop_flag["stop"]:
                try:
                    cmd = cmd_queue.get(timeout=1.0)
                except Exception:
                    continue
                try:
                    if not cmd or cmd.get("cmd") == "stop":
                        logger.info("stop 명령 수신 → close")
                        _stop_flag["stop"] = True
                        try:
                            kws.close(timeout=2.0)
                        except Exception:
                            pass
                        return
                    name = cmd.get("cmd")
                    tr_id = cmd.get("tr_id")
                    codes = cmd.get("codes") or []
                    if not codes:
                        continue
                    req_func = None
                    if tr_id == "H0STMKO0":
                        req_func = market_status_krx
                    elif tr_id == "H0STANC0":
                        req_func = exp_ccnl_krx
                    if req_func is None:
                        logger.warning(f"알 수 없는 tr_id: {tr_id}")
                        continue
                    if name == "sub":
                        # open_map 등록 + connect 후라면 즉시 send
                        ka.KISWebSocket.subscribe(req_func, list(codes))
                        _send_dynamic(req_func, codes, "1")
                        logger.info(f"sub {tr_id} {len(codes)}종목")
                    elif name == "unsub":
                        _send_dynamic(req_func, codes, "2")
                        logger.info(f"unsub {tr_id} {len(codes)}종목")
                    else:
                        logger.warning(f"알 수 없는 cmd: {name}")
                except Exception as e:
                    logger.warning(f"cmd 처리 실패: {e}\n{traceback.format_exc()}")

        threading.Thread(target=cmd_loop, daemon=True, name="a2-cmd-loop").start()

        # ── 5) 초기 base subscribe (cmd_queue 에 미리 들어와 있는 명령들 처리) ──
        # KISWebSocket.start() 가 connect 후 open_map 의 모든 항목 자동 send 하므로
        # connect 전에 cmd_queue 에서 'sub' 명령을 받아 subscribe() (open_map 등록) 만 해두면 됨
        # cmd_loop 가 별도 thread 라 동시 처리됨

        # 초기 명령 처리 시간 (부모가 cmd_queue 채울 시간 1초 확보)
        time.sleep(1.0)

        # ── 6) WSS 시작 (블로킹) ──
        logger.info("WSS connect 시작")
        kws.start(on_result=on_result)
        logger.info("WSS connect 종료")

    except Exception as e:
        logger.error(f"a2 subprocess 치명 오류: {e}\n{traceback.format_exc()}")
    finally:
        logger.info(f"a2 subprocess 종료 (pid={os.getpid()})")


if __name__ == "__main__":
    # 단독 실행 테스트 (디버그용)
    import multiprocessing as mp
    dq = mp.Queue()
    cq = mp.Queue()
    cq.put({"cmd": "sub", "tr_id": "H0STMKO0", "codes": ["005930"]})
    p = mp.Process(target=run_a2_subprocess, args=(str(Path(SCRIPT_DIR) / "config.json"), dq, cq))
    p.start()
    print(f"a2 subprocess pid={p.pid}")
    try:
        while p.is_alive():
            try:
                msg = dq.get(timeout=2.0)
                print(f"[recv] {msg}")
            except Exception:
                pass
    except KeyboardInterrupt:
        cq.put({"cmd": "stop"})
        p.join(timeout=5)
