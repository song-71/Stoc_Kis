#!/bin/bash
# ws_realtime_subscribe_to_DB-1.py 재시작 (기존 종료 후 업데이트 버전 실행)
set -e
SCRIPT_NAME="ws_realtime_subscribe_to_DB-1.py"
SCRIPT="/home/ubuntu/Stoc_Kis/${SCRIPT_NAME}"
OUT="/home/ubuntu/Stoc_Kis/out/wss_realtime_DB-1.out"
PYTHON="/home/ubuntu/Stoc_Kis/venv/bin/python"

echo "[restart] 기존 프로세스 종료 요청 (SIGTERM)..."
pkill -f "$SCRIPT_NAME" 2>/dev/null || true

# 0.5초마다 종료 여부 확인 (최대 30초 = 60회)
# → 프로세스가 flush 완료 즉시 다음 단계로 진행 (불필요한 대기 없음)
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
    sleep 1
fi

echo "[restart] 새 프로세스 시작..."
nohup "$PYTHON" "$SCRIPT" > "$OUT" 2>&1 &
sleep 1
pgrep -af "$SCRIPT_NAME" || echo "[restart] 프로세스 확인 실패"
echo "[restart] 완료. 로그: tail -f $OUT"
