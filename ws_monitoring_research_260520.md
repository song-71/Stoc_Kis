# WS Monitoring Research 260520: 09:00 핸드셰이크 다발 원인 + 1007 근원 차단 패치 무효 발견

## 사용자 보고

- 오늘(260520) 09:00 전후 핸드셰이크 다발 발생, **재시작한 것으로 보임**
- 어제(260519)는 08:50경 핸드셰이크 다발이 있었으나 재시작까지는 가지 않았음(사용자 관찰)
- 질문: 원인이 뭔지 / 그냥 두면 지난번(260513)처럼 사망하는지 / 우리가 한 보호 작업이 효과가 있었는지

---

## Issue 1: "재시작"의 실체 — 오늘은 재시작 0회 (내부 재연결로 자가복구)

### 검증 (로그: `out/logs/wss_TR_260520.log`, `wss_TR_260519.log`)

`=== WSS START ===` 배너(프로세스 기동 표식) 카운트:

| 날짜 | 프로세스 기동 횟수 | 시각 |
|------|------------------|------|
| **260520 (오늘)** | **1회** | 08:28:04 — 종일 재시작 0회 (로그 끝 10:02+까지) |
| **260519 (어제)** | **5회** | 08:28:03, 09:40:51, 10:13:22, 10:14:20, 13:44:35 |

**결론: 오늘은 프로세스 재시작이 없었다.** 사용자가 "재시작"으로 본 것은 09:00:46 의 *내부* WSS 강제 재연결(`재구독 3회 연속 실패 → WSS 강제 재연결 (zombie socket 의심)`)이다 — 같은 프로세스 안에서 소켓만 재연결한 것. 09:01:17 데이터 재개로 자가복구.

오히려 **어제가 장중 4회 더 재시작**(08:50 storm 자체는 회복했으나 09:40~13:44 사이 graceful shutdown→runner 재시작 반복). 안정성은 오늘이 더 좋았다.

### 오늘 09:00 사건 타임라인

```
08:59:29  sent 1007 (invalid frame payload data) invalid start byte at position 361   ← KIS 깨진 UTF-8 프레임
08:59:45  sent 1007 ... at position 363
08:59:46  kws.start dwell=14.16s < 15s → 핸드셰이크 실패 다발 추정
08:59:46  handshake_fail_count=1/4 → long backoff 90s (storm 차단)
09:00:00  장 시작 (예상체결→실시간체결 전환)            ← 90s backoff 가 시초 구간과 겹침
09:00:16  no data for 15s -> resubscribe (1/3)
09:00:31  no data for 30s -> resubscribe (2/3)
09:00:46  재구독 3회 실패 → WSS 강제 재연결 (zombie socket 의심)   ← 사용자가 본 "재시작"
09:01:16  connect attempt=3 (backoff 만료)
09:01:17  데이터 재개 (427건/s)
```

**순효과: 09:00:00~09:01:16 약 92초간 시초 실시간 틱 손실** (VI 급등 후보 진원생명과학/졸스/바른손이앤에이 포함).

---

## Issue 2: 보호 작업 효과 — 있음 (다운스트림 한정), 그러나 근원 패치는 무효

### 효과 있음: 260513 치명 연쇄는 이틀 다 미재현

260513 사망 연쇄 = `1007 → SDK 10회 재시도 폭주 → invalid approval throttle → 슬롯 잠금 → 장중 사망`.

| 지표 | 260519 | 260520 |
|------|--------|--------|
| invalid approval | 0 | 0 |
| ALREADY IN USE | 0 | 0 |
| self-stop (exit=2) | 0 | 0 |
| 슬롯 잠금 | 없음 | 없음 |

→ `max_retries=1` + `HANDSHAKE_FAIL_SELF_STOP_LIMIT=4` + backoff 등 다운스트림 보호가 제대로 작동. **그냥 둬도 260513 식 사망은 막힌다.**

### 무효 발견: UTF-8 lenient 패치(1007 근원 차단용)가 죽어 있었음

| 항목 | 260519 | 260520 |
|------|--------|--------|
| `[ws][utf8-lenient] decode skip` (실제 catch) | **0건** | **0건** |
| `sent 1007` 발생 | 7건 | 2건 |

9회 기동 동안 패치가 단 1건도 잡지 못했는데 1007 은 계속 발생 → 패치가 작동하는 코드 경로가 아님.

**근본 원인 (websockets 15.0.1):**
- 기존 패치는 `websockets.legacy.protocol.WebSocketCommonProtocol.read_message`(레거시 API)를 후킹.
- 그러나 KIS SDK(`kis_auth_llm.py:818`)는 **새 asyncio API** 사용: `websockets.connect()` → `await ws.recv()` → `websockets.ClientConnection`.
- 1007 발생 실제 경로:
  ```
  ws.recv() → Connection.recv (decode=None)
            → Assembler.get() → data.decode()   ← strict UTF-8, 깨진 한글에서 UnicodeDecodeError
            → Connection.recv except UnicodeDecodeError (asyncio/connection.py:311)
            → protocol.fail(CloseCode.INVALID_DATA, f"{exc.reason} at position {exc.start}")
            → close 1007 "invalid start byte at position 361"
  ```
- 레거시 `read_message` 는 이 경로에 전혀 없음 → 0건 catch 확정.

---

## 구현 (260520)

사용자 선택: **"근원 차단 + 시초 fallback"**

### Part A — 1007 근원 차단 (asyncio API lenient 패치)  `ws_realtime_trading.py`
- 기존 legacy 패치는 `[legacy]` 태그 + "현 SDK 미사용 경로, 무해 안전망" 주석으로 명시(false confidence 제거), 유지.
- **새 패치 추가**: `websockets.asyncio.messages.Assembler.get` 을 lenient 재구현으로 교체.
  - 원본 본문(15.0.1) 복제 + 마지막 텍스트 디코드만: 정상 프레임 `data.decode()`(strict, 동작 불변) → 실패 시에만 `data.decode("utf-8","replace")` + rate-limit(30s) 로그 `[ws][utf8-lenient-asyncio] decode skip`.
  - 깨진 1글자만 `?` 치환, 연결 유지 → 1007 미발생.
  - `get_iter` 안전망으로 모듈 전역 `UTF8Decoder` 도 lenient factory 로 치환.
  - websockets 15.x 가 아니면 본문 구조 불일치 위험 → skip(경고만).

### Part B — 시초 near-open backoff 단축  `ws_realtime_trading.py`
- 신규 상수 `MAIN_WSS_BACKOFF_NEAR_OPEN_SEC = 15.0`.
- 재연결 backoff 결정부: **핸드셰이크 실패(dwell<15s) 분기에 한해** `08:55 ≤ now < 09:02` 면 backoff 90s→15s 단축(`near-open 단축` 로그).
- **ALREADY IN USE 는 단축 안 함**(KIS stale session 정리에 실제 시간 필요 — 단축 시 재유발).
- Part A 가 동작하면 1007 자체가 안 나서 이 fallback 은 거의 호출되지 않음(이중 안전망).

---

## 검증 결과

**Part A (격리 테스트, venv):**
1. 정상 한글 프레임 `"진원생명과학 체결가=4085"` → strict 와 동일하게 디코드 ✓
2. 깨진 프레임 `b"abc\xff\xfe def \xea\xb0"` → lenient get: 예외 없이 `'abc�� def �'` 반환, skip 카운트 +1 ✓ (1007 미발생)
3. 동일 깨진 프레임 → 원본 strict get: `UnicodeDecodeError: invalid start byte at 3` raise (= 1007 의 정확한 원인 재현) ✓
4. `py_compile ws_realtime_trading.py` 통과 ✓

**Part B (로직 테스트):**
- 오늘 08:59:46 핸드셰이크 실패 → 15s(near-open 단축) ✓
- 어제 08:50:56(08:55 이전) → 90s ✓ / 09:30 mid-session → 90s ✓
- 08:59 ALREADY IN USE → 90s(단축 안 함) ✓ / 경계 09:01:59→15s, 09:02:00→90s ✓

**실거래 검증 지표(다음 세션 로그):**
- `[ws][utf8-lenient-asyncio] monkey-patch applied` 기동마다 출력
- `[ws][utf8-lenient-asyncio] decode skip` 가 잡히고 `sent 1007` 발생이 0(또는 급감) → **효과 확정**
- 개장 직전 storm 시 `near-open → backoff 단축` 출력 + 시초 공백 ~15초 이내

---

## 변경하지 않은 것 (scope 고정)

- 다운스트림 보호(`max_retries=1`, `HANDSHAKE_FAIL_SELF_STOP_LIMIT=4`, invalid approval 60s backoff, no-data 3-strike force reconnect) — 정상 동작 확인, 미변경
- ALREADY IN USE 90s backoff(개장 외 구간) / self-stop·exit code 정책 — 미변경
