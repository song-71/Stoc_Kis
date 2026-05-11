#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ws_realtime_trading.py 워치독 (강제 종료 전용)

[감시 대상] /home/ubuntu/Stoc_Kis/ws_realtime_trading.py 의 좀비 잔존

[동작]
- 5분 주기로 다음 두 조건 검사 후 위반 시 강제 종료
  1) config.json 의 ws_realtime_trading_status == 2 (정상 종료 기록) 인데
     ws_realtime_trading.py 프로세스가 여전히 살아있음 → SIGTERM → 5초 → SIGKILL
  2) 현재 KST 시각이 20:00 이후이고, ws_realtime_trading.py 프로세스가 살아있음
     → status 무관하게 SIGTERM → 5초 → SIGKILL

[배경]
- 4/28 ~ 5/6 사건: 종료 신호 후에도 kws.start() 의 자동 reconnect 루프가
  47시간 좀비로 살아남아 같은 IP 에서 KIS WSS 에 폭주 reconnect 시도 → IP 단위 throttle
- 본 워치독은 그러한 잔존을 운영 단계에서 즉각 차단하기 위한 안전망

[실행]
nohup /home/ubuntu/Stoc_Kis/venv/bin/python -u /home/ubuntu/Stoc_Kis/ws_realtime_watchdog.py \
    > /home/ubuntu/Stoc_Kis/out/ws_realtime_watchdog.out 2>&1 &

[확인]
pgrep -af ws_realtime_watchdog.py

[종료]
pkill -f ws_realtime_watchdog.py
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, time as dtime, timezone, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

try:
    from telegMsg import tmsg
except Exception:
    tmsg = None

KST = timezone(timedelta(hours=9))
PROGRAM_NAME = Path(__file__).name
LOG_ID = "WD"
TARGET_SCRIPT = "ws_realtime_trading.py"
TARGET_PATH = SCRIPT_DIR / TARGET_SCRIPT
CONFIG_PATH = SCRIPT_DIR / "config.json"

LOG_DIR = SCRIPT_DIR / "out" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

CHECK_INTERVAL_SEC = 300         # 5분 주기
SIGKILL_GRACE_SEC = 5            # SIGTERM 후 SIGKILL 까지 대기
NIGHT_KILL_HOUR = 20             # 20:00 이후 무조건 강제 종료

RUNTIME_STATUS_KEY = "ws_realtime_trading_status"
RUNTIME_STATUS_RUNNING = 1
RUNTIME_STATUS_STOPPED = 2


def _today_log_path() -> Path:
    return LOG_DIR / f"watchdog_{datetime.now(KST).strftime('%y%m%d')}.log"


def ts_prefix() -> str:
    return datetime.now(KST).strftime(f"[%y%m%d_%H%M%S_{LOG_ID}]")


def _log(msg: str, tele: bool = False) -> None:
    line = f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} | {msg}"
    print(line, flush=True)
    try:
        with open(_today_log_path(), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass
    if tele and tmsg is not None:
        try:
            tmsg(f"{ts_prefix()} {msg}")
        except Exception as e:
            print(f"[tele 실패] {e}", flush=True)


def _find_target_pids() -> list[int]:
    """ws_realtime_trading.py 를 실행 중인 PID 목록"""
    try:
        out = subprocess.check_output(
            ["pgrep", "-af", TARGET_SCRIPT], text=True, stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError:
        return []
    pids: list[int] = []
    self_pid = os.getpid()
    for line in out.splitlines():
        parts = line.strip().split(maxsplit=1)
        if not parts:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        cmd = parts[1] if len(parts) > 1 else ""
        # 자기 자신 + watchdog 자체 제외
        if pid == self_pid:
            continue
        if PROGRAM_NAME in cmd:
            continue
        # ws_realtime_trading.py 가 실제 실행 인자로 들어간 경우만
        if TARGET_SCRIPT not in cmd:
            continue
        pids.append(pid)
    return pids


def _read_status() -> int | None:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        v = cfg.get(RUNTIME_STATUS_KEY)
        return int(v) if v is not None else None
    except Exception as e:
        _log(f"[status_read] config 읽기 실패: {e}")
        return None


def _force_kill(pids: list[int], reason: str) -> None:
    if not pids:
        return
    _log(f"[FORCE_KILL] reason='{reason}' targets={pids} → SIGTERM 발송", tele=True)
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except Exception as e:
            _log(f"[FORCE_KILL] SIGTERM pid={pid} 실패: {e}")
    # grace 후 잔존 검사
    time.sleep(SIGKILL_GRACE_SEC)
    survivors: list[int] = []
    for pid in pids:
        try:
            os.kill(pid, 0)
            survivors.append(pid)
        except ProcessLookupError:
            pass
        except Exception:
            pass
    if survivors:
        _log(f"[FORCE_KILL] {SIGKILL_GRACE_SEC}s 후 잔존={survivors} → SIGKILL", tele=True)
        for pid in survivors:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except Exception as e:
                _log(f"[FORCE_KILL] SIGKILL pid={pid} 실패: {e}")
    else:
        _log(f"[FORCE_KILL] SIGTERM 으로 모두 종료 확인")


def _check_once() -> None:
    now = datetime.now(KST)
    pids = _find_target_pids()
    status = _read_status()
    is_night = now.time() >= dtime(NIGHT_KILL_HOUR, 0)

    _log(
        f"[check] pids={pids} status={status} "
        f"hhmm={now.strftime('%H:%M')} night_window={is_night}"
    )

    if not pids:
        return

    # 규칙 2: 20:00 이후 살아있으면 무조건 강제 종료
    if is_night:
        _force_kill(pids, reason=f"{NIGHT_KILL_HOUR}:00 이후 잔존 (status={status})")
        return

    # 규칙 1: status=2 인데 프로세스 살아있음 → 좀비
    if status == RUNTIME_STATUS_STOPPED:
        _force_kill(
            pids,
            reason=f"config status={status}(STOPPED) 인데 프로세스 잔존 (좀비 의심)",
        )
        return

    # 정상 (status=1 + 시각 < 20:00 + 프로세스 살아있음)


def main() -> int:
    _log(
        f"[start] {PROGRAM_NAME} interval={CHECK_INTERVAL_SEC}s "
        f"target={TARGET_SCRIPT} night_kill_after={NIGHT_KILL_HOUR}:00",
        tele=True,
    )
    while True:
        try:
            _check_once()
        except Exception as e:
            _log(f"[check_once] 예외: {type(e).__name__}: {e}")
        time.sleep(CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        _log("[stop] KeyboardInterrupt — 워치독 종료")
        sys.exit(0)
