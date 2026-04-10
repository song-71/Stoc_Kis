#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
실시간 거래 로그 모니터링 스크립트 (Claude 검토 방식, v2)

동작:
  매 5분마다
    1) out/logs/wss_TR_{YYMMDD}.log 의 지난 5분 청크 수집 (오프셋 유지)
    2) 현재 시각 + 로그 청크 + docs/trading_project_main_plan.md 경로를
       Claude CLI (`claude -p`) 에 전달
    3) Claude 응답(JSON) 을 파싱
       → severity(OK/WARN/CRITICAL) + 요약 + 근거 로그 라인 + 권장 대응
    4) research 파일로 저장: out/monitor/research_{YYMMDD_HHMM}.md
    5) 요약 1줄을 out/monitor/monitor_summary_{YYMMDD}.log 에 append
    6) CRITICAL 이면 즉시 Telegram 전송

옵션:
  --dry-run    Telegram/Claude 미호출, 로그 청크만 확인
  --interval N 주기 초 (기본 300)
  --once       1회만 실행 후 종료 (테스트용)
"""

import sys
import os
import json
import time
import argparse
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

try:
    from telegMsg import tmsg  # noqa: E402
except Exception:
    tmsg = None

KST = timezone(timedelta(hours=9))


def today_yymmdd() -> str:
    return datetime.now(KST).strftime("%y%m%d")


def now_hhmm() -> str:
    return datetime.now(KST).strftime("%H:%M")


def now_stamp() -> str:
    return datetime.now(KST).strftime("%y%m%d_%H%M")


LOG_DIR = SCRIPT_DIR / "out" / "logs"
MONITOR_DIR = SCRIPT_DIR / "out" / "monitor"
MONITOR_DIR.mkdir(parents=True, exist_ok=True)

MAIN_PLAN_PATH = SCRIPT_DIR / "docs" / "trading_project_main_plan.md"
SUMMARY_LOG = MONITOR_DIR / f"monitor_summary_{today_yymmdd()}.log"
OFFSET_FILE = MONITOR_DIR / f".offset_{today_yymmdd()}"

END_TIME_HHMM = "20:10"  # 이후 자동 종료
CLAUDE_CLI_TIMEOUT = 120  # Claude 호출 타임아웃(초)
MAX_CHUNK_CHARS = 60_000  # Claude 전달 청크 최대 글자수 (초과 시 tail)

DRY_RUN = False


# ─────────────────────────────────────────────────────────────────────────────
# Log chunk reader (offset 기반 tail)
# ─────────────────────────────────────────────────────────────────────────────
def read_log_chunk(log_path: Path, offset: int) -> tuple[str, int]:
    """log_path 를 offset 부터 끝까지 읽어 (text, new_offset) 반환."""
    if not log_path.exists():
        return "", offset
    try:
        size = log_path.stat().st_size
    except OSError:
        return "", offset
    if size < offset:
        # 로그 rotation / 파일 교체 → 처음부터
        offset = 0
    if size == offset:
        return "", offset
    try:
        with open(log_path, "rb") as f:
            f.seek(offset)
            data = f.read()
        new_offset = offset + len(data)
        text = data.decode("utf-8", errors="replace")
        if len(text) > MAX_CHUNK_CHARS:
            text = "... (청크 앞부분 생략) ...\n" + text[-MAX_CHUNK_CHARS:]
        return text, new_offset
    except Exception as e:
        print(f"[monitor] read_log_chunk error: {e}", file=sys.stderr)
        return "", offset


def load_offset() -> int:
    if OFFSET_FILE.exists():
        try:
            return int(OFFSET_FILE.read_text().strip())
        except Exception:
            return 0
    return 0


def save_offset(offset: int) -> None:
    try:
        OFFSET_FILE.write_text(str(offset))
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Claude CLI 검토
# ─────────────────────────────────────────────────────────────────────────────
CLAUDE_PROMPT_TEMPLATE = """아래는 Stoc_Kis 실시간 트레이딩 로그의 최근 {minutes}분 청크입니다. 현재 시각은 {now_hhmm} (KST) 입니다.

판정 기준 문서: {main_plan_path}
→ 이 문서의 "## 1. 운영 시간대별 운영 방향" 섹션과 "## 3. 관측 포인트 & 이상 판정 기준" 섹션을 기준으로 판단해주세요.

검토 지침:
1. 현재 시각({now_hhmm}) 에 해당하는 운영 시간대 섹션을 참조
2. 로그 라인 중 운영 방향과 어긋나거나, 공통 CRITICAL/WARN 패턴에 해당하는 것이 있는지 확인
3. 정상이면 severity="OK" 로 응답

출력 형식: **JSON 만** 출력 (다른 텍스트 없이). 마크다운 코드블록도 제외.
{{
  "severity": "OK" | "WARN" | "CRITICAL",
  "summary": "한 줄 요약 (한글 60자 이내)",
  "findings": [
    {{"level": "WARN" | "CRITICAL", "evidence": "근거 로그 라인 원문 또는 요약", "reason": "왜 문제인지", "recommendation": "권장 대응"}}
  ]
}}

로그 청크:
---
{log_chunk}
---
"""


def call_claude_review(log_chunk: str, minutes: int) -> dict | None:
    """Claude CLI 호출. 성공 시 파싱된 dict, 실패 시 None."""
    if DRY_RUN:
        print("[monitor] DRY_RUN — Claude 호출 스킵")
        return None
    prompt = CLAUDE_PROMPT_TEMPLATE.format(
        minutes=minutes,
        now_hhmm=now_hhmm(),
        main_plan_path=str(MAIN_PLAN_PATH),
        log_chunk=log_chunk,
    )
    try:
        proc = subprocess.run(
            ["claude", "-p", prompt],
            capture_output=True,
            text=True,
            timeout=CLAUDE_CLI_TIMEOUT,
        )
    except FileNotFoundError:
        print("[monitor] claude CLI not found", file=sys.stderr)
        return None
    except subprocess.TimeoutExpired:
        print("[monitor] claude CLI timeout", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[monitor] claude CLI error: {e}", file=sys.stderr)
        return None

    if proc.returncode != 0:
        print(f"[monitor] claude rc={proc.returncode} stderr={proc.stderr[:300]}", file=sys.stderr)
        return None

    out = (proc.stdout or "").strip()
    # 마크다운 코드블록이 포함된 경우 제거
    if out.startswith("```"):
        lines = out.splitlines()
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        out = "\n".join(lines).strip()
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        # JSON 블록만 추출 시도
        try:
            s = out.find("{")
            e = out.rfind("}")
            if s >= 0 and e > s:
                data = json.loads(out[s:e + 1])
            else:
                raise
        except Exception:
            print(f"[monitor] Claude 응답 파싱 실패: {out[:300]}", file=sys.stderr)
            return None
    return data


# ─────────────────────────────────────────────────────────────────────────────
# 결과 기록 / 알림
# ─────────────────────────────────────────────────────────────────────────────
def write_research_file(chunk: str, result: dict | None) -> Path:
    path = MONITOR_DIR / f"research_{now_stamp()}.md"
    lines = [
        f"# Log Monitor Research — {now_stamp()}",
        "",
        f"- 생성시각: {datetime.now(KST).isoformat()}",
        f"- 판정기준: {MAIN_PLAN_PATH}",
        "",
        "## Claude 판정",
        "```json",
        json.dumps(result or {"severity": "UNKNOWN", "summary": "Claude 응답 없음"}, ensure_ascii=False, indent=2),
        "```",
        "",
        "## 로그 청크",
        "```",
        chunk or "(빈 청크)",
        "```",
    ]
    try:
        path.write_text("\n".join(lines), encoding="utf-8")
    except Exception as e:
        print(f"[monitor] research 파일 저장 실패: {e}", file=sys.stderr)
    return path


def append_summary(result: dict | None, chunk_bytes: int, research_path: Path) -> None:
    severity = (result or {}).get("severity", "UNKNOWN")
    summary = (result or {}).get("summary", "Claude 응답 없음")
    line = f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} | {severity:<8} | bytes={chunk_bytes} | {summary} | {research_path.name}\n"
    try:
        with open(SUMMARY_LOG, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        print(f"[monitor] summary append 실패: {e}", file=sys.stderr)
    print(line, end="")


def maybe_telegram_alert(result: dict | None, research_path: Path) -> None:
    if DRY_RUN or tmsg is None or not result:
        return
    severity = result.get("severity", "OK")
    if severity != "CRITICAL":
        return
    summary = result.get("summary", "(no summary)")
    findings = result.get("findings") or []
    head = f"🚨 [log_monitor CRITICAL] {now_hhmm()}\n{summary}\n\n"
    body_lines = []
    for i, f in enumerate(findings[:3], 1):
        lvl = f.get("level", "?")
        reason = f.get("reason", "")
        evidence = (f.get("evidence", "") or "")[:200]
        body_lines.append(f"{i}. [{lvl}] {reason}\n   {evidence}")
    body = "\n".join(body_lines) if body_lines else "(findings 없음)"
    tail = f"\n\nresearch: {research_path.name}"
    try:
        tmsg(head + body + tail)
    except Exception as e:
        print(f"[monitor] tmsg 실패: {e}", file=sys.stderr)


# ─────────────────────────────────────────────────────────────────────────────
# 메인 루프
# ─────────────────────────────────────────────────────────────────────────────
def run_once(interval_sec: int) -> None:
    yymmdd = today_yymmdd()
    log_path = LOG_DIR / f"wss_TR_{yymmdd}.log"
    offset = load_offset()
    chunk, new_offset = read_log_chunk(log_path, offset)
    if not chunk.strip():
        print(f"[monitor] {now_hhmm()} 청크 없음 (offset={offset}) → 스킵")
        return
    save_offset(new_offset)

    minutes = max(1, interval_sec // 60)
    result = call_claude_review(chunk, minutes)
    research_path = write_research_file(chunk, result)
    append_summary(result, len(chunk), research_path)
    maybe_telegram_alert(result, research_path)


def main() -> None:
    global DRY_RUN
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Claude/Telegram 미호출")
    ap.add_argument("--interval", type=int, default=300, help="주기 초 (기본 300)")
    ap.add_argument("--once", action="store_true", help="1회만 실행")
    args = ap.parse_args()
    DRY_RUN = args.dry_run

    print(f"[monitor] start interval={args.interval}s dry_run={DRY_RUN} main_plan={MAIN_PLAN_PATH}")
    if not MAIN_PLAN_PATH.exists():
        print(f"[monitor] 경고: {MAIN_PLAN_PATH} 없음 — Claude 가 판정 기준을 찾지 못할 수 있음", file=sys.stderr)

    if args.once:
        run_once(args.interval)
        return

    while True:
        try:
            hhmm = now_hhmm()
            if hhmm >= END_TIME_HHMM:
                print(f"[monitor] {hhmm} ≥ {END_TIME_HHMM} → 종료")
                break
            run_once(args.interval)
        except KeyboardInterrupt:
            print("[monitor] 중단")
            break
        except Exception as e:
            print(f"[monitor] loop error: {e}", file=sys.stderr)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
