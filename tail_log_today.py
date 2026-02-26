"""
KIS 로그를 실시간으로 보기 위한 유틸.
nohup 출력 로그(예: /home/ubuntu/Stoc_Kis/out/1m_api_parquet.log)가 아니라,
LogManager가 기록하는 일자별 로그(/home/ubuntu/KIS_DB/data/logs/KIS_log_YYMMDD.log)를 바로 연다.
"""

import os
import subprocess
import sys


def main() -> None:
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/logs")
    log_dir = os.path.abspath(log_dir)
    from datetime import datetime, timezone, timedelta

    kst = timezone(timedelta(hours=9))
    arg = sys.argv[1].strip() if len(sys.argv) > 1 else ""
    if arg:
        digits = "".join(ch for ch in arg if ch.isdigit())
        if len(digits) == 8:  # YYYYMMDD
            y, m, d = digits[2:4], digits[4:6], digits[6:8]
            date_tag = f"{y}{m}{d}"
        elif len(digits) == 6:  # YYMMDD
            date_tag = digits
        else:
            print("[tail] 날짜 형식 오류: YYMMDD 또는 YYYYMMDD")
            return
    else:
        date_tag = datetime.now(kst).strftime("%y%m%d")
    name = f"KIS_log_{date_tag}.log"
    log_path = os.path.join(log_dir, name)
    os.makedirs(log_dir, exist_ok=True)
    if not os.path.exists(log_path):
        # 파일이 없으면 생성
        with open(log_path, "a", encoding="utf-8"):
            pass
    mode = sys.argv[2].strip().lower() if len(sys.argv) > 2 else ""
    try:
        if mode in ("less", "full", "page"):
            print(f"[less] {log_path}")
            subprocess.run(["less", log_path], check=False)
        else:
            print(f"[less+F] {log_path} (Ctrl+C: scroll mode, F: follow, q: quit)")
            subprocess.run(["less", "+F", log_path], check=False)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
