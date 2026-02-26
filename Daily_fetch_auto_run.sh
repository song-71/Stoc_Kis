#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/home/ubuntu/Stoc_Kis"
VENV_PY="${BASE_DIR}/venv/bin/python"
LOG_DIR="/home/ubuntu/Stoc_Kis/data/logs"
LOG_FILE="${LOG_DIR}/$(date +%Y%m%d)_Daily_fetch_data.log"
export TZ="Asia/Seoul"
END_DATE="$(date +%Y%m%d)"

mkdir -p "${LOG_DIR}"

# 휴일 체크 — 휴일이면 종료 (프로그램명 출력 + 텔레그램 전송)
cd "${BASE_DIR}" || true
if ! "${VENV_PY}" -c "
import sys
sys.path.insert(0, '${BASE_DIR}')
from kis_utils import is_holiday, send_telegram
if is_holiday():
    msg = '[Daily_fetch_auto_run.sh(daily 1d/1m data fetch)] => 휴일로 프로그램을 종료합니다.'
    print(msg)
    send_telegram(msg)
    sys.exit(1)
" >> "${LOG_FILE}" 2>&1; then
  echo "[Daily_fetch_auto_run.sh] 휴일로 프로그램 종료" >> "${LOG_FILE}"
  exit 1
fi

echo "===============================" >> "${LOG_FILE}"
TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "[${TS}] started tz=${TZ} python=${VENV_PY} -> ${LOG_FILE}" >> "${LOG_FILE}"
#nohup "${VENV_PY}" "${BASE_DIR}/kis_KRX_code_fetch.py" >> "${LOG_FILE}" 2>&1

# 1일 1회 종목별 OHLCV 데이터(1d) 다운로드
nohup "${VENV_PY}" "${BASE_DIR}/kis_1d_Daily_ohlcv_fetch_manager.py" --freq 1d --end "${END_DATE}" >> "${LOG_FILE}" 2>&1

# 1시간 1회 종목별 OHLCV 데이터(1m) 다운로드
nohup "${VENV_PY}" "${BASE_DIR}/kis_1m_API_to_Parquet_all_code.py" >> "${LOG_FILE}" 2>&1 &
# nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/kis_1m_API_to_Parquet_all_code.py > /home/ubuntu/Stoc_Kis/out/1m_api_parquet.log 2>&1 &


# 로그 파일어
# tail -n 50 /home/ubuntu/Stoc_Kis/data/logs/Daily_fetch_data.log

# 스케줄링(매일 20:30) - 아래 내용을 crontab에 등록해야 동작
# 등록 방법:
#   crontab -e
#   아래 두 줄 추가 후 저장:
#   CRON_TZ=Asia/Seoul
#   30 20 * * 1-5 /home/ubuntu/Stoc_Kis/kin_ohlcv_fetch_auto_run.sh
