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

import subprocess, re, sys, os, json, signal, threading, time, select
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

# ─── 경로 ───────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from telegMsg import tmsg
from kis_utils import is_holiday

KST = timezone(timedelta(hours=9))
PROGRAM_NAME = Path(__file__).name
LOG_ID = "MO"  # 로그 prefix 식별자
TODAY = datetime.now(KST).strftime("%y%m%d")

def ts_prefix() -> str:
    return datetime.now(KST).strftime(f"[%y%m%d_%H%M%S_{LOG_ID}]")

LOG_DIR = SCRIPT_DIR / "out" / "logs"
LOG_PATH = LOG_DIR / f"wss_TR_{TODAY}.log"
MONITOR_DIR = SCRIPT_DIR / "out" / "monitor"
MONITOR_DIR.mkdir(parents=True, exist_ok=True)

# 모니터 자체 로그 파일 (텔레그램 메시지 포함 모든 활동 기록)
MONITOR_LOG_PATH = MONITOR_DIR / f"monitor_{TODAY}.log"

def _monitor_log(msg: str):
    """모니터 자체 로그 파일에 기록"""
    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    with open(MONITOR_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

ISSUES_PATH = MONITOR_DIR / f"issues_{TODAY}.jsonl"
CRITICAL_ANALYSIS_PATH = MONITOR_DIR / f"critical_analysis_{TODAY}.md"
ISSUE_ANALYSIS_PATH = MONITOR_DIR / f"issue_analysis_{TODAY}.md"
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
    # NXT 모니터링 대상 종목 수 비정상 (nxt 필터 누락 등)
    (re.compile(r"NXT.*(?:프리마켓|애프터마켓)\s*시작.*모니터링 시작"), "NXT대상점검"),
    # 모니터링/배너 반복 출력 (동일 메시지 10분 내 재출력)
    (re.compile(r"NXT.*모니터링 시작"), "NXT반복출력"),
]

CRITICAL_PATTERNS_EXTRA = [
    # 시스템 재시작 (watchdog 강제 종료)
    (re.compile(r"\[watchdog\]\[FATAL\].*강제 종료"), "watchdog강제종료"),
    # os._exit 또는 프로세스 재시작
    (re.compile(r"os\._exit|프로세스 강제 종료"), "시스템강제종료"),
    # runner에 의한 재시작 감지
    (re.compile(r"launch\s*\(#\d+\)"), "시스템재시작"),
]

# 비정상종료 판정: 15:30 이전에만 CRITICAL
MARKET_CLOSE = "15:30"

# 프로덕션 정상 종료(장 마감 후 EOD)의 확정 표식.
# 프로덕션이 그날 일과를 정상 종료하면 마지막에 이 줄을 찍는다
# (예: "[scheduler] EXIT 모드 정리 완료 → os._exit(0)").
# 이 표식이 (15:30 이후에) 보이면 모니터도 일일 리뷰 후 정상 종료한다.
NORMAL_EOD_EXIT_PATTERN = re.compile(r"\[scheduler\] EXIT 모드 정리 완료")

# 신호(SIGTERM/SIGINT)에 의한 종료 표식 — 수동 재시작/중지(ws_realtime_trading_Restart.py),
# watchdog 재시작, restart 스크립트 등이 보내는 신호를 프로덕션의 _shutdown("signal=NN")가
# 로그로 남긴다. 정상 EOD 종료([scheduler] EXIT)와 구분해 "수동 강제종료"로 표시한다.
SIGNAL_SHUTDOWN_PATTERN = re.compile(r"\[shutdown\]\s*signal=(\d+)")
# 신호 종료 직후 os._exit(좀비 thread) 줄을 같은 수동종료로 묶는 시간창(초)
SIGNAL_SHUTDOWN_WINDOW_SEC = 30


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
        # 추가 CRITICAL 패턴 (watchdog 강제 종료, 시스템 재시작)
        for pat, cat in CRITICAL_PATTERNS_EXTRA:
            if pat.search(line):
                # os._exit / '강제 종료' 류는 종료코드·문맥으로 세분 분류한다.
                # (정상종료 / 정상재시작 / 강제종료예고 / 비정상강제종료)
                if cat == "시스템강제종료":
                    return self._classify_exit(line)
                return ("CRITICAL", cat)
        for pat, cat in MODERATE_PATTERNS:
            if pat.search(line):
                return ("MODERATE", cat)
        return ("INFO", "")

    def _classify_exit(self, line: str) -> tuple[str, str]:
        """os._exit / '강제 종료' 류 로그를 종료코드·문맥으로 세분 분류한다.

        - watchdog 예고문('... 프로세스 강제 종료 예정')  → INFO  강제종료예고 (실제 종료 아님)
        - os._exit(0) + scheduler EXIT(EOD 정리 완료)      → INFO  정상종료
        - os._exit(0) (좀비 thread 차단 등 재시작 절차)     → INFO  정상재시작
        - os._exit(N≠0) (watchdog 무수신 자폭 등)          → CRITICAL 비정상강제종료
        """
        # 1) 예고문: 실제 종료가 아니라 '~ 예정' 경고일 뿐
        if "예정" in line:
            return ("INFO", "강제종료예고")
        # 2) 종료 코드로 정상/비정상 구분
        m = re.search(r"os\._exit\((\d+)\)", line)
        if m:
            if int(m.group(1)) == 0:
                # 종료코드 0 = 정상. EOD 정리 종료와 단순 재시작 절차를 구분한다.
                if NORMAL_EOD_EXIT_PATTERN.search(line) or "[scheduler]" in line:
                    return ("INFO", "정상종료")
                return ("INFO", "정상재시작")
            # 종료코드가 0이 아니면 비정상 강제종료
            return ("CRITICAL", "비정상강제종료")
        # 3) 종료코드가 안 보이는 '프로세스 강제 종료' 류 — 보수적으로 비정상 처리
        return ("CRITICAL", "비정상강제종료")

    # ─── 중복 억제 ──────────────────────────────────────────────────
    def _dedup_key(self, category: str, line: str) -> str:
        """에러 시그니처 생성"""
        if category == "Traceback":
            # Traceback은 마지막 줄(에러 메시지)로 시그니처
            return f"TB:{line.strip()[-100:]}"
        if category == "비정상강제종료":
            # 한 번의 강제종료가 예고/실행 등 여러 줄로 찍혀 중복 집계되지 않도록
            # 분(分) 단위로 묶는다 (같은 분의 os._exit 라인은 1건으로).
            return f"비정상강제종료:{self._extract_time(line)}"
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
        short = context.strip()[:200]
        tele_msg = f"{ts_prefix()} [CRITICAL] {category}\n{short}\n→ 즉시 확인 필요"
        _monitor_log(f"[TELE] {tele_msg}")
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
        _monitor_log(f"[CLAUDE] urgent 호출: {category}")
        if DRY_RUN:
            print(f"[DRY-RUN CLAUDE] urgent: {category}")
            return
        prompt = (
            f"[긴급 수정 필요] 거래 시스템에서 CRITICAL 에러가 발생했습니다.\n"
            f"카테고리: {category}\n"
            f"에러 내용:\n{context[:2000]}\n\n"
            f"아래 구조로 분석하여 {CRITICAL_ANALYSIS_PATH}에 마크다운으로 저장해줘 (기존 내용 뒤에 추가):\n"
            f"## Issue: <이슈 제목>\n"
            f"### 현상\n- 로그/증상 그대로 인용\n"
            f"### 원인 (파일명:행번호)\n- 코드를 직접 읽고 추적한 결과 (추측 금지)\n"
            f"### 수정 방안\n- 구체적 수정 방법과 코드 예시\n"
            f"### 결정 사항\n- 긴급도/우선순위\n\n"
            f"규칙: 행번호는 반드시 현재 파일에서 직접 확인. 추측으로 원인 단정 금지.\n"
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
        tele_msg = f"{ts_prefix()} [30분 요약] ~{now_s}\n- " + "\n- ".join(summary_parts)

        _monitor_log(f"[TELE] {tele_msg}")
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
        _monitor_log(f"[CLAUDE] daily fix 호출: {len(items)}건")
        if DRY_RUN:
            print(f"[DRY-RUN CLAUDE] daily fix: {len(items)} items")
            return
        items_text = "\n".join(
            f"- [{it['ts']}] {it['category']}: {it['line']}" for it in items
        )
        prompt = (
            f"거래 시스템에서 MODERATE 수준 이슈가 발생했습니다.\n"
            f"이슈 목록:\n{items_text}\n\n"
            f"아래 구조로 각 이슈를 분석하여 {ISSUE_ANALYSIS_PATH}에 마크다운으로 저장해줘 (기존 내용 뒤에 추가):\n"
            f"## Issue N: <이슈 제목>\n"
            f"### 현상\n- 로그/증상 그대로 인용\n"
            f"### 원인 (파일명:행번호)\n- 코드를 직접 읽고 추적한 결과 (추측 금지)\n"
            f"### 수정 방안\n- 구체적 수정 방법, 다음 배포 시 반영할 수준으로 정리\n\n"
            f"규칙: 행번호는 반드시 현재 파일에서 직접 확인. 추측으로 원인 단정 금지.\n"
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
            f"{ts_prefix()} [일일 리포트] {datetime.now(KST).strftime('%Y-%m-%d')}\n"
            f"총 {self.total_lines:,}줄 처리\n"
            f"{stats_text}\n"
            f"→ 상세 분석 생성 중..."
        )
        _monitor_log(f"[TELE] {tele_msg}")
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
            f"아래 구조로 분석하여 {REVIEW_PATH}에 마크다운으로 저장해줘:\n"
            f"# 일일 리뷰 {datetime.now(KST).strftime('%Y-%m-%d')}\n\n"
            f"## Issue N: <이슈 제목> (이슈별 반복)\n"
            f"### 현상\n- 로그/증상 그대로 인용\n"
            f"### 원인 (파일명:행번호)\n- 코드를 직접 읽고 추적한 결과 (추측 금지)\n"
            f"### 수정 방안\n- 구체적 수정 방법과 코드 예시\n\n"
            f"추가 분석:\n"
            f"- 전체 에러/경고 패턴 분류\n"
            f"- 시간대별 이상 구간 식별\n"
            f"- 로그가 부족하여 추적이 어려운 부분 (LOG_WEAK)\n"
            f"- 로그 순서 역전 감지: 주문send 로그가 체결통보보다 늦게 찍히는 등 "
            f"논리적 선후관계가 뒤바뀐 로그 쌍을 식별하고 원인/개선방안 제시\n"
            f"- NXT 모니터링 대상 종목 검증: NXT 프리마켓/애프터마켓 대상이 실제 nxt=Y인지 점검\n"
            f"- 동일 배너/메시지 반복 출력 감지: 같은 '시작' 메시지가 반복되면 루프 버그 의심\n"
            f"- 시스템 재시작 감지: watchdog FATAL, os._exit, runner 재시작 등은 반드시 분석 대상\n"
            f"- 15:30 전후 WS 연결 끊김/구독 해제 실패가 연쇄적으로 시스템 다운을 유발하는지 점검\n\n"
            f"규칙: 행번호는 반드시 현재 파일에서 직접 확인. 추측으로 원인 단정 금지.\n"
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

    # ─── 정상 종료 처리 (EOD) ───────────────────────────────────────
    def _handle_normal_eod(self):
        """프로덕션이 정상 종료(장 마감 후 EOD)되면 일일 리뷰를 마치고 모니터도 정상 종료한다."""
        # INFO 알림: 프로덕션 정상 종료 사실을 명시
        info_msg = (f"{ts_prefix()} [INFO] ws_realtime_trading 이 정상 종료되었습니다. "
                    f"→ 모니터링도 정상 종료합니다.")
        print(info_msg)
        _monitor_log(f"[TELE] {info_msg}")
        if DRY_RUN:
            print(f"[DRY-RUN TELE] {info_msg}")
        else:
            try:
                tmsg(info_msg, "-t")
            except Exception:
                pass

        # 미처리 항목 flush + 일일 리뷰 (아직 안 했으면) — stop() 보다 먼저 해야
        # _flush_moderate 가 _stop 가드에 걸려 건너뛰지 않는다.
        if not self.daily_review_done:
            self._flush_traceback()
            self._flush_moderate()
            self._invoke_daily_review()

        # 모니터 종료 트리거 (run 루프가 다음 폴링에서 빠져나간다)
        self.stop()

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
        elif severity == "INFO" and category in ("정상종료", "정상재시작", "강제종료예고"):
            # 정상 생명주기 이벤트 — 경보(텔레그램/Claude)는 울리지 않고
            # 일일 리포트에 INFO 건수로만 '표시'한다.
            key = self._dedup_key(category, line)
            if not self._is_duplicate(key):
                self.daily_stats[f"INFO_{category}"] += 1
                self._log_issue(line.strip(), "INFO", category)

        # 프로덕션 정상 종료(EOD) 감지 → 일일 리뷰 후 모니터도 정상 종료
        if (self._extract_time(line) >= MARKET_CLOSE
                and NORMAL_EOD_EXIT_PATTERN.search(line)):
            self._handle_normal_eod()
            return

        # 시간 체크
        now_t = datetime.now(KST).strftime("%H:%M")
        if now_t >= END_OF_DAY and not self.daily_review_done:
            self._flush_traceback()
            self._flush_moderate()
            self._invoke_daily_review()

    # ─── 메인 루프 ──────────────────────────────────────────────────
    def run(self):
        # 휴일 가드 — 휴장일이면 감시할 거래 로그가 없으므로 종일 idle 방지 위해 즉시 종료
        try:
            if is_holiday():
                msg = f"{ts_prefix()} {PROGRAM_NAME} => 휴일이므로 로그 모니터링을 종료합니다."
                print(msg)
                _monitor_log(f"[TELE] {msg}")
                if not DRY_RUN:
                    try:
                        tmsg(msg, "-t")
                    except Exception:
                        pass
                return
        except Exception as e:
            # 휴일 판정 실패 시 감시는 진행하되 경고만 남김
            print(f"[monitor][WARN] 휴일 판정 실패, 감시 계속 진행: {e}")
            _monitor_log(f"[WARN] 휴일 판정 실패, 감시 계속 진행: {e}")

        start_msg = f"{ts_prefix()} {PROGRAM_NAME} => 로그 모니터링을 시작합니다. (감시대상: {LOG_PATH.name})"
        print(start_msg)
        _monitor_log(f"[TELE] {start_msg}")
        if not DRY_RUN:
            try:
                tmsg(start_msg, "-t")
            except Exception:
                pass

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

        # tail -F 출력은 line-buffered. 새 라인이 없어도 1초마다 깨어나
        # _stop / SHUTDOWN_TIME 을 검사해야 SIGTERM·자연 종료가 즉시 동작한다.
        try:
            stdout_fd = proc.stdout
            while not self._stop.is_set():
                ready, _, _ = select.select([stdout_fd], [], [], 1.0)
                if ready:
                    line = stdout_fd.readline()
                    if not line:  # tail 종료(EOF)
                        break
                    if line.strip():
                        self.process_line(line)
                # 자동 종료 체크 (라인 유무와 무관하게 매 폴링마다)
                if datetime.now(KST).strftime("%H:%M") >= SHUTDOWN_TIME:
                    print(f"[monitor] {SHUTDOWN_TIME} 도달, 종료")
                    break
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
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
        err_msg = f"{ts_prefix()} [MONITOR CRASH] {PROGRAM_NAME} 크래시: {e}"
        print(err_msg)
        _monitor_log(f"[TELE] {err_msg}")
        try:
            tmsg(err_msg, "-t")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
