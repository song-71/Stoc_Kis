#!/usr/bin/env python3
"""
국내휴장일조회[국내주식-040] API를 활용해 오늘이 주식 휴장일/개장일인지 확인

- kis_utils.is_holiday() 호출로 휴장일 여부 출력
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from kis_utils import is_holiday  # noqa: E402


def main() -> None:
    try:
        holiday = is_holiday()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    from datetime import datetime
    from zoneinfo import ZoneInfo

    now = datetime.now(ZoneInfo("Asia/Seoul"))
    print("=" * 60)
    print(f"오늘: {now.strftime('%Y-%m-%d (%A)')} KST")
    print(f"결과: {'휴장일' if holiday else '개장일'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
