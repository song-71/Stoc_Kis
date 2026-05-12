#!/usr/bin/env python3
"""ws_realtime_trading.py 제어 스크립트 (launch.json F5용).

사용법:
    아래 OPTION 값을 1 또는 2로 직접 수정한 뒤 실행하세요.
"""
import subprocess
import sys
import time

# ============================================================
#  실행 옵션 (직접 수정해서 사용)
#    1 = 재시작 (kill + runner 재기동)
#    2 = 중지   (프로세스 종료만)
# ============================================================
OPTION = 1
# ============================================================

BASE_DIR = "/home/ubuntu/Stoc_Kis"
RESTART_SH = f"{BASE_DIR}/restart_wss_trading.sh"
SCRIPT_NAME = "ws_realtime_trading.py"
RUNNER_NAME = "ws_realtime_trading_runner.sh"


def do_restart() -> int:
    return subprocess.call(["/bin/bash", RESTART_SH], cwd=BASE_DIR)


def do_stop() -> int:
    print("[stop] 기존 프로세스 종료 요청 (SIGTERM)...")
    subprocess.call(["pkill", "-f", SCRIPT_NAME])
    subprocess.call(["pkill", "-f", RUNNER_NAME])

    # 0.5초마다 종료 여부 확인 (최대 30초 = 60회)
    exited = False
    for i in range(1, 61):
        rc = subprocess.call(
            ["pgrep", "-f", SCRIPT_NAME],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        if rc != 0:
            print(f"[stop] 프로세스 정상 종료 확인 ({i * 0.5:.1f}초 경과)")
            exited = True
            break
        time.sleep(0.5)

    if not exited:
        print("[stop] 30초 경과 후에도 살아있음 → 강제 종료 (SIGKILL)...")
        subprocess.call(["pkill", "-9", "-f", SCRIPT_NAME])
        subprocess.call(["pkill", "-9", "-f", RUNNER_NAME])
        time.sleep(1)

    rc = subprocess.call(
        ["pgrep", "-f", SCRIPT_NAME],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if rc != 0:
        print("[stop] 완료 - 모든 프로세스 종료됨.")
        return 0
    print("[stop] 경고: 일부 프로세스가 아직 살아있을 수 있음.")
    subprocess.call(["pgrep", "-af", SCRIPT_NAME])
    return 1


def main() -> int:
    if OPTION == 1:
        print("[main] OPTION=1 → 재시작")
        return do_restart()
    if OPTION == 2:
        print("[main] OPTION=2 → 중지")
        return do_stop()
    print(f"[error] 잘못된 OPTION 값: {OPTION!r} (1 또는 2)")
    return 2


if __name__ == "__main__":
    sys.exit(main())
