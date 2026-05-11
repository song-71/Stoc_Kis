# WS Monitoring Research 260415: a2-wss send timeout 및 연쇄 장애

## 근본 원인 요약

08:50:03에 `ingest_loop`에서 Polars 미변환 버그(pandas DataFrame에 `.with_columns()` 호출)로 무한 루프 → 08:54:11 watchdog 강제 종료(`os._exit(2)`) → 프로세스 재시작. 재시작 과정에서 a2-wss 연결이 zombie 상태로 남아 send timeout 연쇄 발생. 이후 09:06:14 SIGTERM 수신 시 `_shutdown()` 함수의 `global _active_kws` 누락으로 `UnboundLocalError` 발생, WebSocket 정리 실패 → 재연결 시 "ALREADY IN USE appkey" 발생.

---

## Issue 1: a2-wss WebSocket send timeout (5s) 연쇄 실패

### 현상
```
2026-04-15 08:54:32 [a2-wss] exp_ccnl 구독 실패: 엑스게이트(356680) WebSocket send timeout (5s)
2026-04-15 08:54:32 [a2-wss] H0STCNI0 체결통보 구독 실패: WebSocket send timeout (5s)
... (08:54~09:01 약 7분간 모든 send_request 5초 타임아웃)
```
- 08:54:26부터 09:01까지 a2-wss의 모든 `send_request` 호출이 5초 타임아웃으로 실패
- exp_ccnl 구독/해제, H0STCNI0 구독 모두 영향

### 원인 (kis_auth_llm.py:921-929, ws_realtime_trading.py:5306-5368)

**직접 원인**: a2 KISWebSocket의 내부 asyncio 이벤트 루프가 응답 불가 상태.

- `send_request()` (`kis_auth_llm.py:921-929`)는 `asyncio.run_coroutine_threadsafe(coro, self._loop)`로 코루틴을 이벤트 루프에 예약하고 `fut.result(timeout=5)`로 5초 대기
- 이벤트 루프가 블로킹되면 코루틴 실행이 안 되어 무조건 5초 후 `TimeoutError` → `RuntimeError("WebSocket send timeout (5s)")` 변환

```python
# kis_auth_llm.py:921-929
def send_request(self, request, tr_type: str, data, kwargs: dict | None = None):
    if self._ws is None or self._loop is None:
        raise RuntimeError("WebSocket not connected")
    coro = self.send_multiple(self._ws, request, tr_type, data, kwargs)
    fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
    try:
        return fut.result(timeout=5)
    except TimeoutError:
        raise RuntimeError("WebSocket send timeout (5s)")
```

**근본 원인**: 08:54:11에 watchdog가 `os._exit(2)`로 프로세스를 강제 종료. `os._exit()`는 Python 정리 절차(atexit, finally 등)를 전혀 수행하지 않으므로:
- a2 WSS의 WebSocket 연결이 서버 측에서 정리되지 않음
- 프로세스 재시작 후 a2 WSS가 새로 연결을 시도하지만, `_init_a2_wss()` (`ws_realtime_trading.py:5306`)는 daemon 스레드로 1회만 실행
- `_kws_a2`는 `max_retries=9999`로 내부 재시도하지만 (`ws_realtime_trading.py:5318`), 이벤트 루프가 죽은 상태에서는 `send_request`가 계속 타임아웃

**a2 재연결 부재**: `_init_a2_wss()`는 `_kws_a2.start(_a2_on_result)`를 호출하여 blocking으로 실행 (`ws_realtime_trading.py:5368`). `__runner`가 내부적으로 `max_retries`까지 재시도하지만, 외부에서 a2 연결 상태를 모니터링하거나 강제 재생성하는 로직이 없음.

### 수정 방안
1. **a2 WSS 상태 모니터링 추가**: `scheduler_loop`에서 `_kws_a2`의 연결 상태를 주기적으로 확인하고, 연속 send timeout 발생 시 `_kws_a2.shutdown()` → 재생성
2. **send timeout 후 자동 재연결 트리거**: `_a2_wss_subscribe()` 등에서 연속 N회 timeout 시 `_kws_a2`를 재초기화하는 로직 추가
3. **a2 연결 상태 플래그 관리**: `_kws_a2_ready` 이벤트를 연결 끊김 시 clear하여, 다른 함수들이 연결 상태를 정확히 판단 가능하게

---

## Issue 2: H0STCNI0 체결통보 구독 실패 — WebSocket not connected

### 현상
```
2026-04-15 08:54:26 [a2-wss] H0STCNI0 체결통보 구독 실패: WebSocket not connected
2026-04-15 08:59:51 [a2-wss] H0STCNI0 체결통보 구독 실패: WebSocket not connected
```
- 프로세스 재시작 직후 a2 WSS 연결 완료 전에 체결통보 구독 시도

### 원인 (kis_auth_llm.py:922-923, ws_realtime_trading.py:5370-5384)

`send_request()` (`kis_auth_llm.py:922-923`)에서 `self._ws is None` 체크:
```python
# kis_auth_llm.py:922-923
if self._ws is None or self._loop is None:
    raise RuntimeError("WebSocket not connected")
```

`_a2_subscribe_ccnl_notice()` (`ws_realtime_trading.py:5370-5384`)는 `_kws_a2`가 None이 아닌지만 체크하고 `_kws_a2_ready` 이벤트를 확인하지 않음:
```python
# ws_realtime_trading.py:5373
if _kws_a2 is None or _a2_ccnl_notice_done:
    return
```

`_kws_a2` 인스턴스는 `_init_a2_wss()`에서 즉시 생성되지만 (`ws_realtime_trading.py:5318`), 실제 WebSocket 연결(`_ws` 할당)은 `__runner` → `websockets.connect()` 완료 후. 그 사이에 다른 스레드에서 `_a2_subscribe_ccnl_notice()`를 호출하면 "WebSocket not connected" 발생.

### 수정 방안
- `_a2_subscribe_ccnl_notice()`와 `_a2_wss_subscribe()`에서 `_kws_a2_ready.wait(timeout=10)` 호출 후 진행하거나, `_kws_a2_ready.is_set()` 체크 추가

---

## Issue 3: ALREADY IN USE appkey

### 현상
```
2026-04-15 09:00:00 [ws][system] tr_id=(null) tr_key= msg=ALREADY IN USE appkey
2026-04-15 09:10:38 [ws][system] tr_id=(null) tr_key= msg=ALREADY IN USE appkey
2026-04-15 09:11:25 [ws][system] tr_id=(null) tr_key= msg=ALREADY IN USE appkey
```
- a1 WSS 재연결 시 KIS 서버가 이전 세션이 아직 살아있다고 거부

### 원인 (ws_realtime_trading.py:8956-8966, 9305, kis_auth_llm.py:807-842)

KIS WebSocket 서버는 동일 appkey로 1개 세션만 허용. 재연결 시 이전 세션이 서버 측에서 정리되지 않았으면 "ALREADY IN USE appkey" 응답.

이전 세션 미정리 원인:
1. `os._exit(2)` 강제 종료 (watchdog, line 위치는 `_watchdog_loop` 내): WebSocket close 프레임 미전송
2. `_shutdown()` (`ws_realtime_trading.py:8956-8966`)의 `global _active_kws` 누락 → `UnboundLocalError` → `_request_ws_close()` 미실행 (Issue 4 참조)

`run_ws_forever()` (`ws_realtime_trading.py:9248-9305`)에서 재연결 시:
- `ka.auth_ws(svr="prod")`로 approval_key 재발급 (line 9254)
- 새 `KISWebSocket` 인스턴스 생성 (line 9305)
- 하지만 이전 세션의 close를 서버에 명시적으로 요청하지 않음

KIS 서버는 이전 세션 타임아웃(약 60~120초)까지 새 연결을 거부하므로, 빈번한 재시작 시 반복 발생.

### 수정 방안
1. `run_ws_forever()`에서 재연결 전에 이전 `kws` 인스턴스의 `close()`/`shutdown()` 명시 호출 (이미 `finally` 블록에서 하고 있지만, `os._exit` 경로에서는 무효)
2. "ALREADY IN USE" 수신 시 일정 시간(예: 10초) 대기 후 재시도하는 백오프 로직 추가 (`_on_system` 콜백에서 플래그 설정)
3. watchdog의 `os._exit()` 호출 전에 WebSocket close를 시도하는 grace period 추가

---

## Issue 4: _active_kws 변수 UnboundLocalError

### 현상
```
2026-04-15 09:06:14 [ws] message parse error (skipped): cannot access local variable '_active_kws' where it is not associated with a value
```
- SIGTERM(15) 수신 시 `_shutdown()` 호출에서 발생

### 원인 (ws_realtime_trading.py:8956-8966)

`_shutdown()` 함수에서 `_active_kws = None` (line 8966)을 할당하지만 `global _active_kws` 선언이 없음. Python은 함수 내에서 변수에 할당이 있으면 해당 변수를 함수 전체에서 로컬로 취급. 따라서 line 8964의 `if _active_kws is not None:` 에서 아직 값이 할당되지 않은 로컬 변수를 읽으려 해서 `UnboundLocalError` 발생.

```python
# ws_realtime_trading.py:8956-8966
def _shutdown(reason: str):
    if _stop_event.is_set():
        return
    logger.warning(f"\n{'=' * 66}\n[shutdown] {reason}\n{'=' * 66}")
    _stop_event.set()
    _ws_rebuild_event.set()
    with _kws_lock:
        if _active_kws is not None:      # ← line 8964: UnboundLocalError 발생
            _request_ws_close(_active_kws)
            _active_kws = None            # ← line 8966: 이 할당 때문에 로컬 변수로 처리됨
```

**동일 버그 존재 위치**: `_switch_to_closing_codes()` (`ws_realtime_trading.py:6866-6924`)
- `global _base_codes`는 있지만 `global _active_kws` 누락
- line 6921에서 `_active_kws` 읽기, line 6923에서 `_active_kws = None` 할당
- 15:20 종가매매 전환 시 동일한 `UnboundLocalError` 발생 가능

### 수정 방안
1. `_shutdown()` (line 8956)에 `global _active_kws` 추가:
```python
def _shutdown(reason: str):
    global _active_kws          # 추가 필요
    if _stop_event.is_set():
        return
```

2. `_switch_to_closing_codes()` (line 6866)에 `global _active_kws` 추가:
```python
def _switch_to_closing_codes() -> None:
    global _base_codes, _active_kws    # _active_kws 추가 필요
```

---

## Issue 5: H0STCNT0 구독 추가 실패 (no close frame)

### 현상
```
2026-04-15 09:00:00 [구독 추가 실패] TR_ID=H0STCNT0 종목=KR모터스(000040), ... err=no close frame received or sent
2026-04-15 09:00:00 [구독 추가 실패] TR_ID=H0STCNT0 종목=가온그룹(078890), ... err=no close frame received or sent
2026-04-15 09:00:00 [구독 추가 실패] TR_ID=H0STCNT0 종목=루멘스(038060), ... err=no close frame received or sent
```
- 09:00:00 시점에 a1 WSS 다수 종목 구독이 일괄 실패

### 원인 (ws_realtime_trading.py:9038-9071, kis_auth_llm.py:807-842)

`run_ws_forever()`에서 새 `KISWebSocket` 인스턴스를 생성하고 `kws.start(on_result)` 호출 시, `__runner` 내부의 `websockets.connect()` (`kis_auth_llm.py:825`)가 연결 후 `open_map`의 구독을 일괄 전송. 이때 서버가 "ALREADY IN USE appkey"로 응답한 직후 연결을 서버 측에서 끊으면, 이미 예약된 `send_multiple` 호출들이 "no close frame received or sent" 에러와 함께 실패.

`_send_subscribe()` (`ws_realtime_trading.py:9058-9071`)에서 `kws.send_request()` 호출 시:
```python
# kis_auth_llm.py:924-929
coro = self.send_multiple(self._ws, request, tr_type, data, kwargs)
fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
```
WebSocket이 이미 닫혔으므로 `send_multiple` 내부의 `ws.send()` → `no close frame` 예외 발생.

이 에러는 Issue 3 (ALREADY IN USE appkey)의 연쇄 결과.

### 수정 방안
- Issue 3의 수정 (이전 세션 정리 + 백오프)이 근본 해결
- `_on_system` 콜백에서 "ALREADY IN USE" 감지 시 즉시 `kws.close()` 호출하여 빠른 재연결 유도

---

## Issue 6: H0STMKO0/exp_ccnl 해제 실패 (send timeout, 09:10~09:19)

### 현상
```
2026-04-15 09:10~09:19 [a2-wss] exp_ccnl 해제 실패: ... WebSocket send timeout (5s)
2026-04-15 09:10~09:19 [a2-wss] H0STMKO0 해제 실패: ... send timeout (5s)
```

### 원인

Issue 1과 동일한 근본 원인. a2 WSS의 이벤트 루프가 zombie 상태이므로 해제 요청도 동일하게 5초 타임아웃 실패.

호출 경로:
- `_a2_mkstatus_unsubscribe()` (`ws_realtime_trading.py:5432-5442`): `_kws_a2.send_request()` 호출
- `_a2_wss_subscribe(code, "2")` (`ws_realtime_trading.py:5386-5401`): 해제 시에도 동일 경로

### 수정 방안
- Issue 1의 수정 (a2 상태 모니터링 + 자동 재생성)이 근본 해결

---

## 이슈 간 인과관계 (타임라인)

```
08:50:03  ingest_loop에서 pandas DataFrame에 .with_columns() 호출 → AttributeError
          → ingest_loop 무한 재시도 (0.5초 간격으로 같은 아이템 dequeue)
08:54:11  watchdog: ingest_loop 127초 무응답 → os._exit(2) 강제 종료
          → WebSocket close 프레임 미전송 (a1, a2 모두)
08:54:26  프로세스 재시작 → a2 WSS 연결 미완료 상태에서 H0STCNI0 구독 시도
          → "WebSocket not connected" (Issue 2)
08:54:32  a2 WSS 연결은 됐으나 이벤트 루프 zombie → send timeout 시작 (Issue 1)
08:59:37  또 다시 watchdog 강제 종료 (ingest_loop에서 동일 pandas 버그 재발)
09:00:00  프로세스 재시작 → a1 WSS "ALREADY IN USE appkey" (Issue 3)
          → open_map 구독 전송 시 "no close frame" (Issue 5)
09:06:14  SIGTERM → _shutdown()에서 global _active_kws 누락 → UnboundLocalError (Issue 4)
          → _request_ws_close() 미실행 → 이전 세션 미정리
09:10:38  재연결 시 다시 "ALREADY IN USE appkey" (Issue 3 반복)
09:10~19  a2 WSS 여전히 zombie → send timeout 지속 (Issue 6)
```

진짜 근본 원인은 `ingest_loop`의 pandas/Polars 미변환 버그(별도 커밋 2ccc3ac로 수정됨)이며, 나머지는 연쇄 결과. 그러나 연쇄 과정에서 드러난 코드 결함(Issue 4의 `global` 누락, Issue 1의 a2 재연결 부재)은 독립적으로 수정 필요.

---

## Issue 7: 15:25 H0STANC0 구독 해제 실패 (장마감 종가전환 시점)

### 현상
```
2026-04-15 15:25:14,576 | WARNING | [260415_152514_TR] [구독 해제 실패] TR_ID=H0STANC0 종목=대한전선(001440), 라닉스(317120)...
```
- a1-wss에서 exp_ccnl_krx(=H0STANC0) 구독 해제 시도 시 실패 (WARNING 4건)
- 15:20 종가매매 전환 이후 약 5분 경과 시점

### 원인 (ws_realtime_trading.py:9097-9101, 6950-6954, 4627-4632)

**로그 출처**: `_send_subscribe()` 함수 (line 9097-9101)
```python
# line 9086
action = "구독 추가" if tr_type == "1" else "구독 해제"
# line 9097-9101
except Exception as e:
    code_names = [f"{code_name_map.get(c, c)}({c})" for c in chunk]
    logger.warning(
        f"{ts_prefix()} [{action} 실패] TR_ID={rid or 'unknown'} "
        f"종목={', '.join(sorted(code_names))} err={e}"
    )
```

**호출 흐름**:
1. 15:20에 `_switch_to_closing_codes()` (line 6897) 실행
2. line 6950-6954: a1-wss를 `_request_ws_close()`로 닫음 (구독 40개 해제 대신 재연결으로 슬롯 초기화)
3. `run_ws_forever()` (line 9267) 재연결 루프 진입
4. 재연결 시 line 9292: `_subscribed.clear()` 실행
5. line 9309-9317: `_desired_subscription_map()` 호출 -> 15:20~15:30이고 a2 연결 시 `return {}` (line 9241-9242)
6. 따라서 open_map은 비어있는 상태로 a1-wss 연결
7. 하지만 재연결 후 `scheduler_loop`의 주기적 `_apply_subscriptions()` (line 4627-4632) 호출 시점에, a1이 재연결 과정에서 이전 세션의 잔여 구독이 `_subscribed`에 남아있을 수 있음
8. 또는 Issue 4의 `global _active_kws` 누락 (`_switch_to_closing_codes` line 6950에서도 동일) 때문에 `_active_kws = None` 할당이 실패하여 stale 포인터 상태로 `_apply_subscriptions` 호출

**가장 유력한 원인**: `_switch_to_closing_codes()` (line 6897)에서 line 6953의 `_active_kws = None` 할당에 `global _active_kws` 선언이 없어 `UnboundLocalError` 발생 -> `_request_ws_close` 미실행 -> a1-wss가 닫히지 않고 이전 상태 유지 -> 이후 scheduler_loop에서 stale `_subscribed` 기반으로 exp_ccnl_krx 해제 시도 -> 이벤트 루프 블로킹으로 timeout

검증: line 6950의 `with _kws_lock:` 블록 내에서 `_active_kws`를 읽는 line 6951과 None 할당하는 line 6953이 있는데, `global _active_kws`가 없으면 `UnboundLocalError` 발생.

```python
# ws_realtime_trading.py:6950-6954
with _kws_lock:
    if _active_kws is not None:        # line 6951: UnboundLocalError (로컬로 취급)
        kws_ref = _active_kws          # line 6952
        _active_kws = None             # line 6953: 이 할당이 함수 전체를 로컬화
        _request_ws_close(kws_ref)     # line 6954: 도달 불가
```

하지만 이 함수는 `global _base_codes` (line 6898)만 선언하고 `global _active_kws`가 없다. 이것이 Issue 4와 동일한 버그.

**주의**: 실제로 이 UnboundLocalError가 발생했다면 `_switch_to_closing_codes()` 전체가 except 없이 중단되어 종가전환 자체가 실패했을 가능성이 높다. 그러나 로그에서 "종가매매 전환 완료" 메시지가 나왔다면, 이 블록이 try-except로 감싸져 있거나 다른 경로로 실행됐을 수 있다.

### 수정 방안
1. `_switch_to_closing_codes()` (line 6897)에 `global _active_kws` 추가 (Issue 4와 동일):
```python
def _switch_to_closing_codes() -> None:
    """15:20 종가매매 전환..."""
    global _base_codes, _active_kws    # _active_kws 추가
```

2. 재연결 직전 `_subscribed.clear()` (line 9292)가 이미 있으므로, 정상적으로 재연결되면 stale 해제 시도는 발생하지 않아야 함. `global` 누락 수정이 근본 해결.

---

## Issue 8: a2-wss WebSocket send timeout (5s) 장마감 시점 (15:25:20 ~ 15:27:25)

### 현상
```
2026-04-15 15:25:20,592 | WARNING | [260415_152520_TR] [a2-wss] exp_ccnl 구독 실패: 아주IB투자(027360) WebSocket send timeout (5s)
2026-04-15 15:25:20,593 | WARNING | [260415_152520_TR] [a2-wss] H0STCNI0 체결통보 구독 실패: WebSocket send timeout (5s)
```
- a2-wss에서 15:25~15:27까지 약 2분간 모든 send_request가 5초 타임아웃
- exp_ccnl 구독 + H0STCNI0 구독 모두 실패

### 원인 (kis_auth_llm.py:802-803, 932-940, ws_realtime_trading.py:5471-5506)

Issue 1과 동일한 메커니즘이지만, 이번에는 **장마감 동시호가 데이터 대량 유입**이 트리거.

**이벤트 루프 블로킹 경로**:
1. `__subscriber()` (kis_auth_llm.py:709)가 WebSocket 메시지를 수신
2. line 802-803: `self.on_result(ws, tr_id, df, data_map[tr_id])` **동기 호출**
3. `_a2_on_result()` (ws_realtime_trading.py:5324): pandas->polars 변환 + queue.put
4. H0STCNI0인 경우 line 5342: `on_result(ws, tr_id, df, data_info)` -- a1의 무거운 콜백 직접 호출
5. 15:20~15:30 전 종목 예상체결가 대량 유입 -> 콜백 연속 실행 -> 이벤트 루프 수초간 점유
6. `send_request()`의 `run_coroutine_threadsafe` 코루틴이 실행 기회 없음 -> 5초 타임아웃

**개별 구독 루프가 타임아웃을 증폭**:
`_a2_apply_subscriptions()` (line 5504-5506):
```python
if to_add:
    for c in list(to_add):
        _a2_wss_subscribe(c, "1")  # 종목당 1회 send_request (5초 타임아웃)
```
N종목 * 5초 = 약 2분 블로킹.

**15:25 시점 트리거**:
- 15:20 `_switch_to_closing_codes()` -> line 6966: `_a2_apply_subscriptions(datetime.now(KST))`
- `desired = all_codes` (line 5494-5495, 15:20~15:30 구간)
- 전 종목에 대해 개별 구독 시도 -> 각각 5초 타임아웃 -> 약 2분 소요

### 수정 방안

**방안 A (즉시 적용, 고효과): 개별 구독을 배치로 전환**

`_a2_apply_subscriptions()` (line 5501-5506)에서 `_a2_wss_subscribe_batch()` 사용:
```python
# 변경 전 (line 5501-5506)
if to_remove:
    for c in list(to_remove):
        _a2_wss_subscribe(c, "2")
if to_add:
    for c in list(to_add):
        _a2_wss_subscribe(c, "1")

# 변경 후
if to_remove:
    _a2_wss_subscribe_batch(exp_ccnl_krx, list(to_remove), "2")
if to_add:
    _a2_wss_subscribe_batch(exp_ccnl_krx, list(to_add), "1")
```
N회 send_request -> 1회로 줄여 타임아웃 시에도 5초 1회만 지연.

**방안 B (중기, 근본 해결): on_result 콜백을 executor로 오프로드**

`kis_auth_llm.py` line 802-803 변경:
```python
# 변경 전
if show_result is True and self.on_result is not None:
    self.on_result(ws, tr_id, df, data_map[tr_id])

# 변경 후
if show_result is True and self.on_result is not None:
    await asyncio.get_running_loop().run_in_executor(
        None, self.on_result, ws, tr_id, df, data_map[tr_id]
    )
```
이벤트 루프가 콜백에 의해 블로킹되지 않아 `run_coroutine_threadsafe` 코루틴이 즉시 실행 가능. 단, `on_result`에서 ws 객체를 직접 사용하는 경우 thread-safety 검토 필요.

### 권장 수정 순서
1. Issue 7 수정: `_switch_to_closing_codes()`에 `global _active_kws` 추가
2. 방안 A: 배치 구독 전환 (저위험, 즉시 적용)
3. 방안 B: on_result executor 오프로드 (중기)

---

## 15:25 장마감 시점 이슈 타임라인

```
15:19:00  _prepare_closing_buy_orders() 종가매수 준비
15:19:30  _batch_sell_1919() 일괄매도
15:19:55  _pre_unsub_closing_done 사전 구독 해제 (a1 ccnl_krx 해제)
15:20:00  _switch_to_closing_codes() 실행
          → line 6953: _active_kws = None (global 누락 → UnboundLocalError 가능)
          → line 6966: _a2_apply_subscriptions() → desired = all_codes
          → a2에서 전 종목 개별 구독 시도 시작
15:20~25  a2 이벤트 루프 블로킹 (장마감 예상체결가 대량 유입)
          → 개별 구독마다 5초 타임아웃 연쇄
15:25:14  a1 재연결 후 scheduler_loop에서 stale H0STANC0 해제 시도 → 실패 (Issue 7)
15:25:20  a2 exp_ccnl/H0STCNI0 구독 여전히 타임아웃 (Issue 8)
15:27:25  타임아웃 연쇄 종료 (구독 대상 소진)
```

### 결정 사항
- (대기 중)
