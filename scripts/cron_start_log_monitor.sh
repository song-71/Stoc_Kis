#!/bin/bash
# cron 으로 매일 23:20 (KST 08:20) 실행 — ws_log_monitor.py 시작 전 누적 인스턴스 정리.
# 자체 자동 종료 로직(SHUTDOWN_TIME=18:35) 의 tail-F blocking 버그로 종료가 안 되는 경우 누적 발생 → 매일 시작 전 강제 정리.
# 인라인 cron 으로 처리할 경우 cron sh 자체가 pkill 패턴에 매칭되어 위험하므로 wrapper 로 분리.
set -u
TARGET="/home/ubuntu/Stoc_Kis/ws_log_monitor.py"
VENV_PY="/home/ubuntu/Stoc_Kis/venv/bin/python"
OUT_LOG="/home/ubuntu/Stoc_Kis/out/log_monitor.out"

# 기존 인스턴스 종료 (SIGTERM → 5초 후 SIGKILL)
pkill -f "$TARGET" 2>/dev/null || true
sleep 5
pkill -9 -f "$TARGET" 2>/dev/null || true

# 새 인스턴스 시작
nohup "$VENV_PY" "$TARGET" >> "$OUT_LOG" 2>&1 &
disown
