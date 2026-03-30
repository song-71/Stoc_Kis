#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
실시간 거래 로그 모니터링 스크립트
- tail -F 로 wss_TR_{yymmdd}.log 실시간 감시
- 심각도별 분류: CRITICAL / MODERATE / LOG_WEAK
- CRITICAL → 즉시 텔레그램 + Claude 긴급 수정안
- MODERATE → 30분 배치 텔레그램 + Claude 일일 수정안
- 18:10 → Claude 전체 로그 정밀 분석
"""

import subprocess, re, sys, os, json, signal, threading, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

# ─── 경로 ───────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from telegMsg import tmsg

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).strftime("%y%m%d")

LOG_DIR = SCRIPT_DIR / "out" / "logs"
LOG_PATH = LOG_DIR / f"wss_TR_{TODAY}.log"
MONITOR_DIR = SCRIPT_DIR / "out" / "monitor"
MONITOR_DIR.mkdir(parents=True, exist_ok=True)

ISSUES_PATH = MONITOR_DIR / f"issues_{TODAY}.jsonl"
FIX_URGENT_PATH = MONITOR_DIR / f"fix_urgent_{TODAY}.md"
FIX_DAILY_PATH = MONITOR_DIR / f"fix_daily_{TODAY}.md"
REVIEW_PATH = MONITOR_DIR / f"review_{TODAY}.md"

# ─── 설정 ───────────────────────────────────────────────────────────────
DEDUP_WINDOW_SEC = 300       # 동일 에러 5분 내 중복 억제
BATCH_INTERVAL_SEC = 1800    # MODERATE 배치 30분
END_OF_DAY = "18:10"         # 일일 리뷰 트리거 시각
SHUTDOWN_TIME = "18:35"      # 자동 종료 시각
DRY_RUN = "--dry-run" in sys.argv  # 테스트 모드: 텔레그램/Claude 미호출

# ─── 심각도 패턴 ────────────────────────────────────────────────────────
CRITICAL_PATTERNS = [
    (re.compile(r"매[수도]주문.*실패"), "주문실패"),
    (re.compile(r"취소\s*실패"), "취소실패"),
    (re.compile(r"Traceback"), "Traceback"),
    (re.compile(r"=== WSS END ==="), "비정상종료"),
    (re.compile(r"프로그램\s*종료"), "비정상종료"),
    (re.compile(r"(?:WebSocket|websocket).*(?:closed|lost|disconnect)", re.I), "WS끊김"),
]

MODERATE_PATTERNS = [
    (re.compile(r"RemoteDisconnected"), "RemoteDisconnected"),
    (re.compile(r"조회\s*실패(?!.*보유종목 없음)"), "조회실패"),
    (re.compile(r"구독.*실패"), "구독실패"),
    (re.compile(r"no data for.*resubscribe"), "데이터미수신"),
    (re.compile(r"\| WARNING \|"), "WARNING"),
]

# 비정상종료 판정: 15:30 이전에만 CRITICAL
MARKET_CLOSE = "15:30"


class LogMonitor:
    def __init__(self):
        self.dedup: dict[str, float] = {}          # signature → last_sent_ts
        self.dedup_count: dict[str, int] = defaultdict(int)
        self.moderate_buffer: list[dict] = []
        self.daily_stats: dict[str, int] = defaultdict(int)
        self.traceback_lines: list[str] = []
        self.in_traceback = False
        self.total_lines = 0
        self.daily_review_done = False
        self._stop = threading.Event()
        self._batch_timer: threading.Timer | None = None

    # ─── 분류 ───────────────────────────────────────────────────────
    def _extract_time(self, line: str) -> str:
        """로그 라인에서 시각(HH:MM) 추출, 실패 시 현재 시각"""
        m = re.match(r"\d{4}-\d{2}-\d{2} (\d{2}:\d{2})", line)
        return m.group(1) if m else datetime.now(KST).strftime("%H:%M")

    def classify(self, line: str) -> tuple[str, str]:
        """라인을 분류하여 (severity, category) 반환"""
        for pat, cat in CRITICAL_PATTERNS:
            if pat.search(line):
                # 비정상종료는 장중(15:30 이전)에만 CRITICAL
                if cat == "비정상종료":
                    log_time = self._extract_time(line)
                    if log_time >= MARKET_CLOSE:
                        return ("INFO", "정상종료")
                return ("CRITICAL", cat)
        for pat, cat in MODERATE_PATTERNS:
            if pat.search(line):
                return ("MODERATE", cat)
        return ("INFO", "")

    # ─── 중복 억제 ──────────────────────────────────────────────────
    def _dedup_key(self, category: str, line: str) -> str:
        """에러 시그니처 생성"""
        if category == "Traceback":
            # Traceback은 마지막 줄(에러 메시지)로 시그니처
            return f"TB:{line.strip()[-100:]}"
        return f"{category}:{line.strip()[-80:]}"

    def _is_duplicate(self, key: str) -> bool:
        now = time.time()
        if key in self.dedup and (now - self.dedup[key]) < DEDUP_WINDOW_SEC:
            self.dedup_count[key] += 1
            return True
        self.dedup[key] = now
        self.dedup_count[key] = 1
        return False

    # ─── Traceback 수집 ─────────────────────────────────────────────
    def _is_log_line(self, line: str) -> bool:
        """타임스탬프가 있는 정규 로그 라인인지"""
        return bool(re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", line))

    def _flush_traceback(self):
        """수집된 traceback을 하나의 에러로 처리"""
        if not self.traceback_lines:
            return
        full_tb = "\n".join(self.traceback_lines)
        self.traceback_lines.clear()
        self.in_traceback = False

        key = self._dedup_key("Traceback", full_tb)
        if self._is_duplicate(key):
            return
        self._handle_critical("Traceback", full_tb)

    # ─── 이슈 기록 ──────────────────────────────────────────────────
    def _log_issue(self, line: str, severity: str, category: str):
        entry = {
            "ts": datetime.now(KST).isoformat(),
            "severity": severity,
            "category": category,
            "line": line.strip(),
        }
        with open(ISSUES_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    # ─── CRITICAL 처리 ──────────────────────────────────────────────
    def _handle_critical(self, category: str, context: str):
        self.daily_stats[f"CRITICAL_{category}"] += 1
        self._log_issue(context, "CRITICAL", category)

        # 텔레그램
        now_s = datetime.now(KST).strftime("%H:%M")
        short = context.strip()[:200]
        tele_msg = f"[CRITICAL] {now_s} {category}\n{short}\n→ 즉시 확인 필요"
        if DRY_RUN:
            print(f"[DRY-RUN TELE] {tele_msg}")
        else:
            try:
                tmsg(tele_msg, "-t")
            except Exception:
                pass

        # Claude 긴급 수정안
        self._invoke_claude_urgent(category, context)

    def _invoke_claude_urgent(self, category: str, context: str):
        if DRY_RUN:
            print(f"[DRY-RUN CLAUDE] urgent: {category}")
            return
        prompt = (
            f"[긴급 수정 필요] 거래 시스템에서 CRITICAL 에러가 발생했습니다.\n"
            f"카테고리: {category}\n"
            f"에러 내용:\n{context[:2000]}\n\n"
            f"1. 에러 원인을 분석해줘\n"
            f"2. 수정안을 {FIX_URGENT_PATH}에 마크다운으로 저장해줘 (기존 내용 뒤에 추가)\n"
            f"3. 수정이 필요한 파일과 라인을 구체적으로 명시해줘\n"
            f"프로젝트 경로: {SCRIPT_DIR}"
        )
        try:
            subprocess.Popen(
                ["claude", "-p", prompt, "--allowedTools",
                 "Read,Write,Edit,Grep,Glob"],
                cwd=str(SCRIPT_DIR),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as e:
            print(f"[monitor] claude 호출 실패: {e}")

    # ─── MODERATE 처리 ──────────────────────────────────────────────
    def _handle_moderate(self, category: str, line: str):
        self.daily_stats[f"MODERATE_{category}"] += 1
        self._log_issue(line, "MODERATE", category)
        self.moderate_buffer.append({
            "ts": datetime.now(KST).strftime("%H:%M"),
            "category": category,
            "line": line.strip()[:200],
        })

    def _flush_moderate(self):
        """30분 배치: MODERATE 항목 텔레그램 + Claude 호출"""
        if self._stop.is_set():
            return
        # 타이머 재설정
        self._schedule_batch_timer()

        if not self.moderate_buffer:
            return

        items = self.moderate_buffer.copy()
        self.moderate_buffer.clear()

        # 카테고리별 집계
        counts: dict[str, int] = defaultdict(int)
        for it in items:
            counts[it["category"]] += 1

        now_s = datetime.now(KST).strftime("%H:%M")
        summary_parts = [f"{cat} {cnt}건" for cat, cnt in counts.items()]
        tele_msg = f"[30분 요약] ~{now_s}\n- " + "\n- ".join(summary_parts)

        if DRY_RUN:
            print(f"[DRY-RUN TELE] {tele_msg}")
        else:
            try:
                tmsg(tele_msg, "-t")
            except Exception:
                pass

        # Claude 일일 수정안에 누적
        self._invoke_claude_daily_fix(items)

    def _invoke_claude_daily_fix(self, items: list[dict]):
        if DRY_RUN:
            print(f"[DRY-RUN CLAUDE] daily fix: {len(items)} items")
            return
        items_text = "\n".join(
            f"- [{it['ts']}] {it['category']}: {it['line']}" for it in items
        )
        prompt = (
            f"거래 시스템에서 MODERATE 수준 이슈가 발생했습니다.\n"
            f"이슈 목록:\n{items_text}\n\n"
            f"1. 각 이슈의 원인과 개선 방향을 분석해줘\n"
            f"2. {FIX_DAILY_PATH}에 마크다운으로 저장 (기존 내용 뒤에 추가)\n"
            f"3. 긴급하지 않으니 다음 배포 시 반영할 수준으로 정리해줘\n"
            f"프로젝트 경로: {SCRIPT_DIR}"
        )
        try:
            subprocess.Popen(
                ["claude", "-p", prompt, "--allowedTools",
                 "Read,Write,Edit,Grep,Glob"],
                cwd=str(SCRIPT_DIR),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as e:
            print(f"[monitor] claude daily fix 호출 실패: {e}")

    # ─── 일일 리뷰 (18:10) ──────────────────────────────────────────
    def _invoke_daily_review(self):
        if self.daily_review_done:
            return
        self.daily_review_done = True

        # 일일 통계 텔레그램
        stats_lines = [f"  {k}: {v}" for k, v in sorted(self.daily_stats.items())]
        stats_text = "\n".join(stats_lines) if stats_lines else "  이슈 없음"
        tele_msg = (
            f"[일일 리포트] {datetime.now(KST).strftime('%Y-%m-%d')}\n"
            f"총 {self.total_lines:,}줄 처리\n"
            f"{stats_text}\n"
            f"→ 상세 분석 생성 중..."
        )
        if DRY_RUN:
            print(f"[DRY-RUN TELE] {tele_msg}")
            print(f"[DRY-RUN CLAUDE] daily review")
            return

        try:
            tmsg(tele_msg, "-t")
        except Exception:
            pass

        # Claude 전체 로그 분석
        issues_info = f"이슈 기록: {ISSUES_PATH}" if ISSUES_PATH.exists() else "감지된 이슈 없음"
        prompt = (
            f"오늘({datetime.now(KST).strftime('%Y-%m-%d')}) 거래 로그를 정밀 분석해줘.\n"
            f"로그 파일: {LOG_PATH}\n"
            f"{issues_info}\n"
            f"일일 통계: {dict(self.daily_stats)}\n\n"
            f"분석 내용:\n"
            f"1. 전체 에러/경고 패턴 분류\n"
            f"2. 시간대별 이상 구간 식별\n"
            f"3. 로그가 부족하여 추적이 어려운 부분 (LOG_WEAK)\n"
            f"4. 구체적인 코드 개선 방안 (파일:라인 명시)\n"
            f"5. 결과를 {REVIEW_PATH}에 마크다운으로 저장\n"
            f"6. changelog-manager 에이전트를 호출하여 history_code_dev.md에도 기록\n"
            f"프로젝트 경로: {SCRIPT_DIR}"
        )
        try:
            subprocess.Popen(
                ["claude", "-p", prompt, "--allowedTools",
                 "Read,Write,Edit,Grep,Glob,Agent"],
                cwd=str(SCRIPT_DIR),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as e:
            print(f"[monitor] claude daily review 호출 실패: {e}")

    # ─── 배치 타이머 ────────────────────────────────────────────────
    def _schedule_batch_timer(self):
        if self._stop.is_set():
            return
        self._batch_timer = threading.Timer(BATCH_INTERVAL_SEC, self._flush_moderate)
        self._batch_timer.daemon = True
        self._batch_timer.start()

    # ─── 라인 처리 ──────────────────────────────────────────────────
    def process_line(self, line: str):
        self.total_lines += 1

        # Traceback 수집 모드
        if self.in_traceback:
            if self._is_log_line(line):
                self._flush_traceback()
                # 이 라인은 아래에서 정상 처리
            else:
                self.traceback_lines.append(line.rstrip())
                return

        severity, category = self.classify(line)

        if severity == "CRITICAL":
            if category == "Traceback":
                self.in_traceback = True
                self.traceback_lines = [line.rstrip()]
                return
            key = self._dedup_key(category, line)
            if not self._is_duplicate(key):
                self._handle_critical(category, line.strip())
        elif severity == "MODERATE":
            key = self._dedup_key(category, line)
            if not self._is_duplicate(key):
                self._handle_moderate(category, line.strip())

        # 시간 체크
        now_t = datetime.now(KST).strftime("%H:%M")
        if now_t >= END_OF_DAY and not self.daily_review_done:
            self._flush_traceback()
            self._flush_moderate()
            self._invoke_daily_review()

    # ─── 메인 루프 ──────────────────────────────────────────────────
    def run(self):
        print(f"[monitor] 시작 {datetime.now(KST).strftime('%H:%M:%S')}")
        print(f"[monitor] 로그: {LOG_PATH}")
        print(f"[monitor] DRY_RUN: {DRY_RUN}")

        # 로그 파일 대기
        while not LOG_PATH.exists() and not self._stop.is_set():
            print(f"[monitor] 로그 파일 대기 중... {LOG_PATH}")
            time.sleep(10)
            # 날짜 변경 체크
            new_today = datetime.now(KST).strftime("%y%m%d")
            if new_today != TODAY:
                print(f"[monitor] 날짜 변경 감지 ({TODAY} → {new_today}), 종료")
                return

        if self._stop.is_set():
            return

        # 배치 타이머 시작
        self._schedule_batch_timer()

        # tail -F 시작
        proc = subprocess.Popen(
            ["tail", "-F", "-n", "0", str(LOG_PATH)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )

        try:
            for line in proc.stdout:
                if self._stop.is_set():
                    break
                if line.strip():
                    self.process_line(line)

                # 자동 종료 체크
                now_t = datetime.now(KST).strftime("%H:%M")
                if now_t >= SHUTDOWN_TIME:
                    print(f"[monitor] {SHUTDOWN_TIME} 도달, 종료")
                    break
        finally:
            proc.terminate()
            proc.wait(timeout=5)
            if self._batch_timer:
                self._batch_timer.cancel()

        # 종료 전 미처리 항목 처리
        self._flush_traceback()
        if not self.daily_review_done:
            self._flush_moderate()
            self._invoke_daily_review()

        print(f"[monitor] 종료. 총 {self.total_lines:,}줄 처리")

    def stop(self):
        self._stop.set()


# ─── 진입점 ─────────────────────────────────────────────────────────────
def main():
    monitor = LogMonitor()

    def _signal_handler(sig, frame):
        print(f"\n[monitor] 시그널 {sig} 수신, 종료 중...")
        monitor.stop()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        monitor.run()
    except Exception as e:
        # 모니터 자체 크래시 → 텔레그램 알림
        err_msg = f"[MONITOR CRASH] log_monitor.py 크래시: {e}"
        print(err_msg)
        try:
            tmsg(err_msg, "-t")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
