#!/bin/bash
# ws_realtime_trading.py 재시작 (runner 포함)
# - 기존 trading + runner 프로세스 모두 종료
# - runner 래퍼를 통해 재시작 (자동 재시작 보장)
set -e
SCRIPT_NAME="ws_realtime_trading.py"
RUNNER_SCRIPT="/home/ubuntu/Stoc_Kis/ws_realtime_trading_runner.sh"
OUT="/home/ubuntu/Stoc_Kis/out/wss_realtime_trading.out"

echo "[restart] 기존 프로세스 종료 요청 (SIGTERM)..."
pkill -f "$SCRIPT_NAME" 2>/dev/null || true
pkill -f "ws_realtime_trading_runner.sh" 2>/dev/null || true

# 0.5초마다 종료 여부 확인 (최대 30초 = 60회)
MAX_ITER=60
EXITED=0
for i in $(seq 1 $MAX_ITER); do
    if ! pgrep -f "$SCRIPT_NAME" > /dev/null 2>&1; then
        ELAPSED=$(echo "scale=1; $i * 5 / 10" | bc)
        echo "[restart] 프로세스 정상 종료 확인 (${ELAPSED}초 경과)"
        EXITED=1
        break
    fi
    sleep 0.5
done

if [ "$EXITED" -eq 0 ]; then
    echo "[restart] 30초 경과 후에도 살아있음 → 강제 종료 (SIGKILL)..."
    pkill -9 -f "$SCRIPT_NAME" 2>/dev/null || true
    pkill -9 -f "ws_realtime_trading_runner.sh" 2>/dev/null || true
    sleep 1
fi

echo "[restart] runner를 통해 새 프로세스 시작 (log_mode=append: 기존 .out 에 이어서 기록)..."
nohup bash "$RUNNER_SCRIPT" append > /dev/null 2>&1 &
sleep 2
if pgrep -f "$SCRIPT_NAME" > /dev/null 2>&1; then
    pgrep -af "$SCRIPT_NAME"
    echo "[restart] 완료 (runner 포함). 로그: tail -f $OUT"
else
    echo "[restart] 프로세스 시작 대기 중... (runner가 시작 중)"
    sleep 3
    pgrep -af "$SCRIPT_NAME" || echo "[restart] 프로세스 확인 실패"
    echo "[restart] 로그: tail -f $OUT"
fi
