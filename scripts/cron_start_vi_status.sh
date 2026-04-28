#!/bin/bash
# cron 으로 매일 23:58 (KST 08:58) 실행 — Daily_inquire_vi_status.py 시작 전 누적 인스턴스 정리.
# 자체 종료 로직은 있으나 텔레그램/병합 단계 hang 으로 종료 실패 케이스가 누적 발견됨 → 매일 시작 전 강제 정리.
# 인라인 cron 으로 처리할 경우 cron sh 자체가 pkill 패턴에 매칭되어 위험하므로 wrapper 로 분리.
set -u
TARGET="/home/ubuntu/Stoc_Kis/Daily_inquire_vi_status.py"
VENV_PY="/home/ubuntu/Stoc_Kis/venv/bin/python"
OUT_LOG="/home/ubuntu/Stoc_Kis/out/Daily_inquire_vi_status.out"

# 기존 인스턴스 종료 (SIGTERM → 5초 후 SIGKILL)
pkill -f "$TARGET" 2>/dev/null || true
sleep 5
pkill -9 -f "$TARGET" 2>/dev/null || true

# 새 인스턴스 시작
nohup "$VENV_PY" -u "$TARGET" > "$OUT_LOG" 2>&1 &
disown
