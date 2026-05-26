#!/bin/bash
# cron 으로 매일 23:25 (KST 08:25) 실행 — ws_realtime_watchdog.py 시작 전 누적 인스턴스 정리.
# 워치독은 자체 종료 로직이 없는 영구 데몬(5분 주기 루프)이므로 매일 시작 전 강제 정리 필요.
# 인라인 cron 으로 처리할 경우 cron sh 자체가 pkill 패턴에 매칭되어 위험하므로 wrapper 로 분리.
# [목적] ws_realtime_trading.py 좀비 잔존 차단 (status=2 + alive 또는 20:00 이후 alive → SIGTERM/SIGKILL)
set -u
TARGET="/home/ubuntu/Stoc_Kis/ws_realtime_watchdog.py"
VENV_PY="/home/ubuntu/Stoc_Kis/venv/bin/python"
OUT_LOG="/home/ubuntu/Stoc_Kis/out/ws_realtime_watchdog.out"

# 기존 인스턴스 종료 (SIGTERM → 5초 후 SIGKILL)
pkill -f "$TARGET" 2>/dev/null || true
sleep 5
pkill -9 -f "$TARGET" 2>/dev/null || true

# 새 인스턴스 시작 (-u: line-buffered, 5분 간격 로그 즉시 반영)
nohup "$VENV_PY" -u "$TARGET" >> "$OUT_LOG" 2>&1 &
disown
