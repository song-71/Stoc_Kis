#!/usr/bin/env python3
"""
1d/1m 데이터 다운로드를 한 번에 실행.

노협(nohup) 실행:
  nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/Daily_fetch_auto_run.py >> /home/ubuntu/Stoc_Kis/data/logs/Daily_fetch_auto_run.log 2>&1 &

모니터링 (노협 실행 시 이 로그로 진행상황 확인):
  tail -f /home/ubuntu/Stoc_Kis/data/logs/Daily_fetch_auto_run.log

프로세스 확인:
  pgrep -af Daily_fetch_auto_run.py

중단:
  pkill -f Daily_fetch_auto_run.py
  pkill -9 -f Daily_fetch_auto_run.py   # 강제 종료

일반 실행:
  python Daily_fetch_auto_run.py

- 휴일이면 즉시 종료
- 1d 다운로드 완료 후 1m 다운로드 시작 (1m은 백그라운드)
"""
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "data" / "logs"
VENV_PY = BASE_DIR / "venv" / "bin" / "python"
if not VENV_PY.exists():
    VENV_PY = BASE_DIR / "venv" / "Scripts" / "python.exe"  # Windows

os.chdir(BASE_DIR)
sys.path.insert(0, str(BASE_DIR))

LOG_DIR.mkdir(parents=True, exist_ok=True)
today = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
LOG_FILE = LOG_DIR / f"{today}_Daily_fetch_data.log"


def log(msg: str) -> None:
    ts = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S KST")
    line = f"[{ts}] {msg}\n"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line)
    print(line.strip())


def main() -> int:
    # 휴일 체크
    try:
        from kis_utils import is_holiday, send_telegram

        if is_holiday():
            msg = "[Daily_fetch_auto_run.py] 휴일로 프로그램 종료합니다."
            log(msg)
            send_telegram(msg)
            return 1
    except Exception as e:
        log(f"[Daily_fetch_auto_run.py] 휴일 체크 실패: {e}")
        return 1

    log("===============================")
    log(f"started -> {LOG_FILE}")

    if not VENV_PY.exists():
        log(f"[ERROR] venv python 없음: {VENV_PY}")
        return 1

    # 1d 다운로드 (완료될 때까지 대기) - stdout 상속 → 노협 시 로그에 그대로 출력
    log("[1d] kis_1d_Daily_ohlcv_fetch_manager.py 시작")
    try:
        subprocess.run(
            [str(VENV_PY), str(BASE_DIR / "kis_1d_Daily_ohlcv_fetch_manager.py"), "--freq", "1d", "--end", today],
            cwd=BASE_DIR,
            stdout=sys.stdout,
            stderr=subprocess.STDOUT,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        log(f"[1d] 실패: {e}")
        return 1
    log("[1d] 완료")

    # 1m 다운로드 (백그라운드)
    log("[1m] kis_1m_API_to_Parquet_all_code.py 시작 (백그라운드)")
    subprocess.Popen(
        [str(VENV_PY), str(BASE_DIR / "kis_1m_API_to_Parquet_all_code.py")],
        cwd=BASE_DIR,
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    )
    log("[1m] 백그라운드 실행됨")

    return 0


if __name__ == "__main__":
    sys.exit(main())
