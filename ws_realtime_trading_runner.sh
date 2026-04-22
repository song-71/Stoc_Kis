#!/usr/bin/env bash
# ws_realtime_trading.py 감시/재시작 래퍼
# - 사망 시 Telegram 알림 + 자동 재시작
# - 20:10 이후 재시작 안 함, 하루 최대 20회 제한
#
# 로그 모드 (첫 인자):
#   fresh  (기본) — 매일 자동 cron 시작용. 기존 .out 을 날짜스탬프로 rotate 후 새 파일 시작
#   append         — ws_realtime_trading_Restart.py 수동 재시작용. 기존 .out 에 이어서 append
set -u
export TZ="Asia/Seoul"

LOG_MODE="${1:-fresh}"   # 기본값 fresh (crontab 수정 불필요)
if [[ "$LOG_MODE" != "fresh" && "$LOG_MODE" != "append" ]]; then
    echo "[runner] 알 수 없는 로그 모드: '$LOG_MODE' (허용: fresh|append) — fresh 로 폴백" >&2
    LOG_MODE="fresh"
fi

BASE_DIR="/home/ubuntu/Stoc_Kis"
VENV_PY="${BASE_DIR}/venv/bin/python"
TARGET="${BASE_DIR}/ws_realtime_trading.py"
OUT_LOG="${BASE_DIR}/out/wss_realtime_trading.out"
RUNNER_LOG="${BASE_DIR}/out/logs/runner_$(date +%y%m%d).log"
MAX_RESTARTS=20
END_HHMM="20:10"
START_HHMM="07:45"

mkdir -p "$(dirname "$RUNNER_LOG")"

# ── 로그 모드에 따른 OUT_LOG 처리 ─────────────────────────────────────────
# fresh  : 기존 .out 이 있으면 .{yymmdd_HHMM}.out 으로 rotate 후 새로 시작
# append : 아무것도 안 함 (기존 파일 뒤에 이어서 기록)
if [[ "$LOG_MODE" == "fresh" ]]; then
    if [[ -s "$OUT_LOG" ]]; then
        ROTATE_TS="$(date +%y%m%d_%H%M)"
        ROTATED="${OUT_LOG%.out}_${ROTATE_TS}.out"
        mv "$OUT_LOG" "$ROTATED" 2>/dev/null || true
        : > "$OUT_LOG"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [runner] LOG_MODE=fresh — rotated to $(basename "$ROTATED")" | tee -a "$RUNNER_LOG"
    else
        : > "$OUT_LOG"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [runner] LOG_MODE=fresh — created new $(basename "$OUT_LOG")" | tee -a "$RUNNER_LOG"
    fi
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [runner] LOG_MODE=append — appending to existing $(basename "$OUT_LOG")" | tee -a "$RUNNER_LOG"
fi

_tele() {
    "${VENV_PY}" -c "
import sys; sys.path.insert(0,'${BASE_DIR}')
from telegMsg import tmsg
tmsg('''$1''', '-t')
" 2>/dev/null || true
}

_now_hhmm() { date +%H:%M; }

_log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$RUNNER_LOG"
}

_log "runner start pid=$$ target=${TARGET} log_mode=${LOG_MODE}"
_tele "[runner] ws_realtime_trading.py 시작 (runner pid=$$, log_mode=${LOG_MODE})"

restart_count=0

while :; do
    now_hhmm=$(_now_hhmm)
    if [[ "$now_hhmm" < "$START_HHMM" ]]; then
        _log "now=$now_hhmm < $START_HHMM → 종료"
        break
    fi
    if [[ "$now_hhmm" > "$END_HHMM" ]]; then
        _log "now=$now_hhmm > $END_HHMM → 오늘 운영 종료"
        _tele "[runner] $now_hhmm ≥ $END_HHMM → 오늘 자동재시작 종료"
        break
    fi

    now_ts=$(date +%s)
    _log "launch (#${restart_count}) at $now_hhmm"
    echo -e "\n\n============================================================" >> "$OUT_LOG"
    echo "===== [$(date '+%Y-%m-%d %H:%M:%S')] restart #${restart_count} =====" >> "$OUT_LOG"
    echo "============================================================" >> "$OUT_LOG"
    "${VENV_PY}" -u "${TARGET}" >> "$OUT_LOG" 2>&1
    exit_code=$?
    end_ts=$(date +%s)
    ran_sec=$(( end_ts - now_ts ))

    _log "exit code=${exit_code} ran=${ran_sec}s"

    # 정상 종료(exit 0)면 재시작 안 함
    if [[ $exit_code -eq 0 ]]; then
        _log "정상 종료 (exit 0) → 루프 종료"
        _tele "[runner] ws_realtime_trading 정상 종료 (exit 0, 실행 ${ran_sec}s)"
        break
    fi

    # 종료 사유 추정
    reason="비정상 exit=${exit_code}"
    if [[ $exit_code -eq 137 ]]; then
        reason="SIGKILL (137) — OOM 또는 외부 강제종료"
    elif [[ $exit_code -eq 139 ]]; then
        reason="SIGSEGV (139) — 세그멘테이션 폴트"
    elif [[ $exit_code -eq 143 ]]; then
        reason="SIGTERM (143)"
    elif [[ $exit_code -eq 2 ]]; then
        reason="watchdog 강제종료 (os._exit 2)"
    elif [[ $exit_code -gt 128 ]]; then
        reason="signal $((exit_code-128)) 에 의한 종료"
    fi

    restart_count=$(( restart_count + 1 ))
    if [[ $restart_count -gt $MAX_RESTARTS ]]; then
        _tele "[runner][CRITICAL] 재시작 한계(${MAX_RESTARTS}회) 초과 — 루프 종료. 마지막: ${reason}"
        _log "restart limit exceeded"
        break
    fi

    # 백오프: 60초 미만 사망이면 10초, 아니면 5초
    if [[ $ran_sec -lt 60 ]]; then
        backoff=10
    else
        backoff=5
    fi

    _tele "[runner] 사망 감지 ($(date +%H:%M:%S))
사유: ${reason}
실행시간: ${ran_sec}s
${backoff}초 후 재시작 (#${restart_count}/${MAX_RESTARTS})"
    sleep $backoff

    # 재시작 전 END_HHMM 재확인
    now_hhmm=$(_now_hhmm)
    if [[ "$now_hhmm" > "$END_HHMM" ]]; then
        _log "now=$now_hhmm > $END_HHMM → 재시작 중단"
        break
    fi
done

_tele "[runner] 루프 종료 (restart=${restart_count})"
_log "runner end"
