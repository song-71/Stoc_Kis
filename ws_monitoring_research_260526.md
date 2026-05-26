# WS Monitoring Research 260526: 장중 실시간 체결 데이터 56분 두절 사고 (PID 1495038)

대상 로그: `/home/ubuntu/Stoc_Kis/out/logs/wss_TR_260526.log` (PID 1495038)
대상 코드: `/home/ubuntu/Stoc_Kis/ws_realtime_trading.py` (13584행), `/home/ubuntu/Stoc_Kis/kis_auth_llm.py` (SDK)
사고 시간: 09:01:05 마지막 정상 수신 → 10:01:37 수동 재시작까지 약 56분 무수신.

---

## 근본원인 한 줄 요약

09:00:01 "종목별 구독 분리"(UNSUBSCRIBE 4 + SUBSCRIBE 4) 직후 SDK asyncio 이벤트 루프가 wedge 되었고,
그 다음 `scheduler_loop` 주기 iteration 이 `_kws_lock` 을 든 채 `_apply_subscriptions → _send_subscribe → kws.send_request → run_coroutine_threadsafe(coro, self._loop).result()` 에서 **타임아웃 없이 영원히 블로킹**(kis_auth_llm.py:1074) 되어 scheduler_loop 가 통째로 정지했다.
scheduler_loop 안에 들어 있는 무수신 watchdog(WSS_NO_RECV_EXIT_SEC) 과 no-data 재구독 로직이 같은 스레드라 함께 죽었고, 그래서 dead socket 을 감지해도 `os._exit(2)` 에스컬레이션이 발동하지 못했다.

---

## Issue 1: scheduler_loop 가 `_kws_lock` 든 채 `send_request` 무한 블로킹 → 스레드 통째 정지 (핵심 원인)

### 현상
```
2026-05-26 09:00:01,014 | INFO | [260526_090001_TR] VI 급등 조건 → 종목별 구독 분리: 실시간체결 4종목, 예상체결유지 6종목 ...
2026-05-26 09:00:01,419 | INFO | [260526_090001_TR] [구독 해제 결과] TR_ID=unknown 종목=광진실업(026910), 빛과전자(069540), 시지트로닉스(429270), 피델릭스(032580)
2026-05-26 09:00:01,932 | INFO | [260526_090001_TR] [구독 추가 결과] TR_ID=unknown 종목=광진실업(026910), 빛과전자(069540), 시지트로닉스(429270), 피델릭스(032580)
2026-05-26 09:00:02,952 | INFO | [260526_090002_TR] [recv_분포] 구독=10/활성(≤60s)=9/stale(>60s)=1
   ← 이후 [recv_분포] 가 10:02:17 (재시작 후)까지 완전히 끊김
2026-05-26 10:02:17,851 | INFO | [260526_100217_TR] [recv_분포] 구독=16/활성(≤60s)=8/stale(>60s)=8
```
- `[구독 해제 결과]`/`[구독 추가 결과]`(`_send_subscribe` 가 send 성공 시 찍는 로그, ws_realtime_trading.py:8816) 가 09:00:01 (line 277) 이후 10:02:39 (재시작 후)까지 단 한 줄도 없음 → 09:00:01 의 분리가 **마지막으로 성공한 send**.
- scheduler_loop 의 5분 주기 heartbeat `[recv_분포] 구독=...`(ws_realtime_trading.py:5478) 가 08:50, 08:55, **09:00:02 를 끝으로 끊김**. 09:05, 09:10 ... 이 전부 누락. → scheduler_loop 스레드 자체가 09:00:02 직후 정지했다는 결정적 증거.
- 같은 시각 `[top] 랭킹 조회 시작 (schedule #N)` 과 `[snapshot] 저장` 은 계속 출력 → 별도 스레드(top_rank_loop / REST 계열)는 살아 있었음. 멈춘 건 scheduler_loop 한 스레드.

### 원인 (ws_realtime_trading.py + kis_auth_llm.py, 행번호 직접 확인)

scheduler_loop 는 정규장 시간대에 매 iteration 마다 다음 블록을 실행한다:

```python
# ws_realtime_trading.py:5839
                with _kws_lock:
                    if _active_kws is not None:
                        desired = _desired_subscription_map(now)
                        _apply_subscriptions(_active_kws, desired)   # ← 5842
```

`_apply_subscriptions` 는 변경이 없어도(desired==have) 분기 계산만 하고 빠르게 끝나지만, 변경이 있을 때 `_send_subscribe` 를 호출한다(ws_realtime_trading.py:12867, 12880). `_send_subscribe` 는:

```python
# ws_realtime_trading.py:12809
def _send_subscribe(kws, req, data, tr_type: str) -> None:
    ...
        try:
            kws.send_request(request=req, tr_type=tr_type, data=chunk)   # ← 12814
```

`kws.send_request` 는 SDK 에서 **타임아웃 없는** future 대기다:

```python
# kis_auth_llm.py:1069
def send_request(self, request, tr_type: str, data, kwargs: dict | None = None):
    if self._ws is None or self._loop is None:
        raise RuntimeError("WebSocket not connected")
    coro = self.send_multiple(self._ws, request, tr_type, data, kwargs)
    fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
    return fut.result()        # ← 1074: timeout 인자 없음 → 루프가 죽으면 영원히 반환 안 됨
```

비교: 같은 SDK 의 `close()` 는 의도적으로 timeout 을 건다.
```python
# kis_auth_llm.py:1088
                fut = asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)
                fut.result(timeout=timeout)   # ← 1089: close 만 timeout 보유
```
즉 close 경로만 dead-loop 방어가 있고, **send_request 경로는 무방비**다.

소켓 레벨 증거(ss -tnp, PID 1495038 fd=5)에서 Recv-Q 442466 bytes = KIS 는 계속 보내는데 앱이 소켓을 전혀 읽지 않음 → SDK `__subscriber` 코루틴(`await asyncio.wait_for(ws.recv(), 60)`, kis_auth_llm.py:818)이 더 이상 진행되지 않는 wedge 상태. 이 상태에서 `run_coroutine_threadsafe` 로 던진 send coro 는 영원히 스케줄/완료되지 못하고, `fut.result()`(타임아웃 없음)가 scheduler_loop 스레드를 영구 점유.

결과적으로 scheduler_loop 는 line 5842 에서 `_kws_lock` 을 든 채 멈췄고, **그 위쪽에 있는 watchdog/no-data 로직(같은 while 루프 안)** 이 다시 실행될 기회를 영영 못 얻었다(→ Issue 2).

### 수정 방안

(1) SDK `send_request` 에 timeout 부여 (가장 근본). `close()` 와 동일하게:
```python
# kis_auth_llm.py:1069 수정안
def send_request(self, request, tr_type: str, data, kwargs: dict | None = None, timeout: float = 5.0):
    if self._ws is None or self._loop is None:
        raise RuntimeError("WebSocket not connected")
    coro = self.send_multiple(self._ws, request, tr_type, data, kwargs)
    fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
    return fut.result(timeout=timeout)   # 타임아웃 → TimeoutError 로 호출자에게 반환
```
이렇게 하면 `_send_subscribe`(ws_realtime_trading.py:12814)의 try/except 가 예외를 잡고 "구독 추가/해제 실패" 로 로깅하며 즉시 락을 풀고 빠져나온다 → scheduler_loop 가 다음 iteration 으로 진행 → Issue 2 의 watchdog 이 동작.

(2) 호출측 방어(SDK 를 못 고치는 경우): scheduler_loop 의 line 5839~5842 와 같이 hot-path 에서 `_kws_lock` 안에서 `_apply_subscriptions` 를 직접 호출하는 모든 경로를, `_trigger_ws_rebuild` 가 쓰는 것과 동일한 "별도 thread + join(timeout)" 패턴으로 감싼다. 단 이건 락 점유 문제는 남으므로 (1) 이 우선.

### 결정 사항
- (미정 — 사용자 합의 필요)

---

## Issue 2: dead socket 을 감지해도 `os._exit(2)` 에스컬레이션이 안 됨 (watchdog / no-data 재구독이 같은 죽은 스레드에 묶임)

### 현상
```
2026-05-26 09:03:35,631 | WARNING | [ws] _trigger_ws_rebuild timeout(5s) — dead socket 의심, 강제 종료
   ... (09:03~09:59 사이 16회+ 반복) ...
2026-05-26 09:59:05,647 | WARNING | [ws] _trigger_ws_rebuild timeout(5s) — dead socket 의심, 강제 종료
```
- 56분 내내 `_trigger_ws_rebuild timeout` 만 16회 반복. 그런데 로그 전체에 다음이 **단 한 번도 없음**:
  - `[wss_watchdog] ★ WSS N초 무수신 감지` / `[wss_watchdog] ★★ ... → os._exit(2)`
  - `[ws] no data for Ns -> resubscribe (N/3)`
  - `[ws] 재구독 N회 연속 실패 → WSS 강제 재연결`
- 즉 dead socket 은 인지(rebuild timeout)했지만, 강제 종료/재시작 에스컬레이션은 발동조차 못 함.

### 원인 (ws_realtime_trading.py, 행번호 직접 확인)

무수신 watchdog 과 no-data 재구독은 **둘 다 scheduler_loop 의 while 루프 본문 안**에 있다.

무수신 watchdog (장중 무수신 시 os._exit(2) 유도):
```python
# ws_realtime_trading.py:5426  while not _stop_event.is_set():
# ws_realtime_trading.py:5432
                if (dtime(9, 30) <= _now_t < dtime(15, 20)
                    and (time.time() - _process_start_ts) > WSS_WATCHDOG_GRACE_SEC):
                    _idle_sec = time.time() - _last_any_recv_ts if _last_any_recv_ts > 0 else 0   # 5434
                    ...
                    if _idle_sec >= WSS_NO_RECV_EXIT_SEC:       # 5442 (180초)
                        ...
                        os._exit(2)                              # 5450
```

no-data 재구독 (3회 연속 실패 시 강제 재연결):
```python
# ws_realtime_trading.py:5892
                    if idle_sec >= NO_DATA_REBUILD_SEC and (now_ts - _last_rebuild_ts) >= NO_DATA_REBUILD_SEC:
                        ...
                        _no_data_rebuild_count += 1            # 5895
                        if _no_data_rebuild_count >= NO_DATA_RECONNECT_COUNT:   # 5896 (=3)
                            ... _request_ws_close(_active_kws); _ws_rebuild_event.set()  # 5907~5911
```

이 두 로직 모두 line 5839~5842(`_apply_subscriptions` 무한 블로킹) **다음**에 도달하거나, 5842 가 끝나야 다음 while iteration 으로 돌아와 5432/5892 를 다시 실행할 수 있다. Issue 1 에서 scheduler_loop 는 5842 에서 영구 정지했으므로:
- 09:30 이 되어 무수신 idle 이 180초를 한참 넘겼어도 line 5432 의 watchdog 코드가 **다시 실행되지 않음** → `os._exit(2)`(5450) 영원히 미도달.
- no-data 재구독(5892) 도 같은 이유로 미실행 → `_no_data_rebuild_count` 가 0 그대로, 강제 재연결 미발동.

또한 watchdog 시간창이 `09:30 ≤ t < 15:20`(line 5432)이라, 사고 발생 시점(09:01) 부터 09:30 까지는 **설령 scheduler_loop 가 살아있었어도** watchdog 이 비활성이었다. 즉 트리거 시각이 watchdog 사각지대 안이었던 점도 가중요인.

추가로, 반복된 `_trigger_ws_rebuild timeout` 자체는 별도 스레드에서 발생한 것이다. `[top]` 랭킹 루프(top_rank_loop, 별도 스레드)가 구독 변경 감지 시 `_trigger_ws_rebuild()` 를 호출(ws_realtime_trading.py:9546)하는데, 로그상 매 timeout 직전에 `[top] 구독 추가/해제` 가 찍힌다(예: 09:03:30 "[top] 구독 추가 ... 아모텍" → 09:03:35 rebuild timeout). `_trigger_ws_rebuild` 는:
```python
# ws_realtime_trading.py:4609
def _trigger_ws_rebuild():
    def _do():
        with _kws_lock:                         # 4612 ← scheduler_loop 가 점유 중 → 획득 불가
            if _active_kws is not None:
                desired = _desired_subscription_map(datetime.now(KST))
                _apply_subscriptions(_active_kws, desired)
    t = threading.Thread(target=_do, daemon=True)
    t.start()
    t.join(timeout=5.0)                          # 4619 ← _do 가 _kws_lock 못 얻어 5초 timeout
    if t.is_alive():
        logger.warning(... "_trigger_ws_rebuild timeout(5s) — dead socket 의심, 강제 종료")  # 4623
        try:
            if _active_kws is not None:
                _request_ws_close(_active_kws)   # 4626 ← 이것도 dead loop 에서 close 10s timeout
        except Exception:
            pass
```
즉 16회의 timeout 은 "dead socket" 이라기보다 정확히는 **scheduler_loop 가 `_kws_lock` 을 영구 점유해서 `_do` 가 락을 못 얻은 것**이다. 4626 의 `_request_ws_close` 도 같은 dead loop 위에서 `close()` 를 부르므로 10초 close timeout 만 찍고(09:03:49 등) 실효 없음. 진단 메시지가 원인을 dead socket 으로 좁혀버려 진짜 원인(락 점유 + send_request 무한대기)을 가렸다.

### 수정 방안

(1) **rebuild 연속 실패를 os._exit(2) 로 에스컬레이션** (사용자 요청 안전판). `_trigger_ws_rebuild` 가 N회/N초 연속 timeout 이면 프로세스를 하드 종료해 runner 가 재시작하도록:
```python
# ws_realtime_trading.py:4609 부근 수정안 (개념)
_REBUILD_FAIL_LIMIT = 3          # 연속 timeout 한계
_rebuild_fail_count = 0          # 전역, 성공 시 0 리셋

def _trigger_ws_rebuild():
    global _rebuild_fail_count
    def _do(): ...
    t = threading.Thread(target=_do, daemon=True); t.start(); t.join(timeout=5.0)
    if t.is_alive():
        _rebuild_fail_count += 1
        logger.warning(f"{ts_prefix()} [ws] _trigger_ws_rebuild timeout(5s) "
                       f"({_rebuild_fail_count}/{_REBUILD_FAIL_LIMIT}) — dead socket/락점유 의심")
        if _rebuild_fail_count >= _REBUILD_FAIL_LIMIT:
            _notify(f"{ts_prefix()} [ws] rebuild {_rebuild_fail_count}회 연속 실패 → os._exit(2) 하드종료", tele=True)
            logger.error(f"{ts_prefix()} [ws] rebuild {_rebuild_fail_count}회 연속 실패 → os._exit(2)")
            time.sleep(0.5)
            os._exit(2)
        # (기존 _request_ws_close 시도는 별도 thread+timeout 으로만)
    else:
        _rebuild_fail_count = 0   # 성공 시 리셋
```
주의: rebuild 의 timeout 원인은 dead socket 이 아니라 `_kws_lock` 점유일 수 있으므로(이번 사고), 안전판은 "락을 못 얻어도 결국 프로세스 재시작" 이라는 점에서 둘 다 커버한다.

(2) **무수신 watchdog 을 scheduler_loop 밖 독립 스레드로 분리.** 현재 watchdog 이 scheduler_loop 안에 있어 scheduler 가 죽으면 같이 죽는 구조적 단일점이다. `_last_any_recv_ts` 만 읽어 판정하는 가벼운 독립 데몬 스레드로 빼면, scheduler/락 상태와 무관하게 `os._exit(2)` 가 보장된다.
```python
# 개념: 독립 watchdog thread
def _wss_norecv_watchdog():
    while not _stop_event.is_set():
        now_t = datetime.now(KST).time()
        if (dtime(9, 0) <= now_t < dtime(15, 20)   # 사각지대 줄이려면 09:00 부터
                and (time.time() - _process_start_ts) > WSS_WATCHDOG_GRACE_SEC):
            idle = time.time() - _last_any_recv_ts if _last_any_recv_ts > 0 else 0
            if idle >= WSS_NO_RECV_EXIT_SEC:
                logger.error(f"{ts_prefix()} [wss_watchdog] idle={int(idle)}s → os._exit(2)")
                _notify(..., tele=True); time.sleep(1.0); os._exit(2)
        time.sleep(1.0)
```

(3) **watchdog 시간창 09:30→09:00 으로 확대.** 트리거가 개장 직후 구독 분리(09:00:01)이므로, 사각지대(09:00~09:30)를 없애야 한다. 단 개장 직후 일시적 무수신 오탐 방지를 위해 GRACE/ALERT 시간을 별도 튜닝.

### `_last_any_recv_ts` 리셋 경로 점검 (사용자 지적: 타이머 리셋으로 watchdog 무력화 여부)
- `_last_any_recv_ts` 는 데이터 수신 시 갱신(ws_realtime_trading.py:12330), 연결 종료(finally)에서 0.0 으로 리셋(ws_realtime_trading.py:13310~13311), shutdown 시 0.0(13311).
- 이번 사고에서는 `kws.start()`(ws_realtime_trading.py:13238)가 wedge 상태로 **반환하지 않았으므로** finally(13304~13312)에 도달하지 못함 → `_last_any_recv_ts` 는 09:01 값을 그대로 유지(리셋 안 됨). 따라서 "리셋이 watchdog 을 무력화" 한 것은 아니다. **watchdog 이 안 뜬 직접 원인은 Issue 2(scheduler_loop 정지로 watchdog 코드 미실행)** 이다.
- 다만 watchdog 분리(수정안 2) 시 주의점: `_idle_sec = ... if _last_any_recv_ts > 0 else 0`(5434) 로직은 `_last_any_recv_ts==0` 이면 idle 을 0 으로 본다. 연결 finally 가 0 으로 리셋한 직후 재연결 전 구간에서 watchdog 이 무수신을 못 보는 빈틈이 있으니, 분리 watchdog 에서는 "연결 시도 중" 상태를 별도로 고려하는 편이 안전.

### 결정 사항
- (미정 — 사용자 합의 필요. 특히 os._exit(2) 안전판의 N회/N초 임계값)

---

## Issue 3: 09:00 "종목별 구독 분리" 가 wedge 트리거인지 / decode skip 과의 연관

### 현상
```
2026-05-26 09:00:01,014 | INFO | VI 급등 조건 → 종목별 구독 분리: 실시간체결 4종목, 예상체결유지 6종목 ...
2026-05-26 09:00:01,419 | INFO | [구독 해제 결과] ... 광진실업, 빛과전자, 시지트로닉스, 피델릭스
2026-05-26 09:00:01,932 | INFO | [구독 추가 결과] ... 광진실업, 빛과전자, 시지트로닉스, 피델릭스
2026-05-26 09:01:05  ← 마지막 정상 WSS save/수신
```
- 09:00:01 의 UNSUBSCRIBE 4 + SUBSCRIBE 4 (예상체결→실시간체결 전환) 직후부터 수신이 끊김 → 시점상 가장 강한 트리거 후보.
- decode skip WARNING("unexpected end of data") 은 08:52:30, 08:55:02 (장전 예상체결가 구간)에 발생했고 **09:00 wedge 시점과 시각이 일치하지 않음** → 이번 wedge 의 직접 트리거로 보기 어려움(별개의 상시 증상).

### 원인 (코드 추적 결과 / 추측 구분)
- `_apply_subscriptions`(ws_realtime_trading.py:12827)는 1단계 일괄 UNSUBSCRIBE → `time.sleep(0.1)`(12874) → 2단계 SUBSCRIBE 를, 모두 `kws.send_request`(동기, `fut.result()` 무한대기)로 수행한다. 이 일련의 send 들이 09:00:01 까지는 정상 반환됐다(로그에 결과 출력됨).
- **확정**: 09:00:01 이 마지막 성공 send, 직후 SDK 이벤트 루프가 wedge 됐다는 것은 (a) Recv-Q 442KB 누적, (b) 이후 모든 `_send_subscribe` 로그 소실, (c) scheduler_loop 정지로 입증됨.
- **미확정(추측 금지 영역)**: "구독 분리 send 자체가 루프를 wedge 시킨 직접 원인" 인지, 아니면 동시간대 KIS 측 프레임 이상(개장 직후 폭주 + 간헐 truncated 프레임)이 `__subscriber` 의 `ws.recv()` 처리(kis_auth_llm.py:818~945)에서 코루틴을 멈춘 것인지는 이 로그만으로 단정 불가. `__subscriber` 의 except 절(933~945)은 개별 메시지 파싱 에러는 `continue` 로 흡수하므로 파싱 예외로 루프가 죽지는 않는다. recv() 자체(websockets 내부)가 멈춘 경우라면 SDK/websockets 레벨 추가 계측 필요.
- 핵심은 **트리거가 무엇이든** Issue 1/2 의 구조적 결함(무한대기 send + 단일 스레드 watchdog)이 있는 한 56분 두절로 증폭된다는 점. 트리거 차단보다 증폭 차단(Issue 1, 2 수정)이 우선.

### 수정 방안
- 단기: Issue 1(send timeout) + Issue 2(독립 watchdog + os._exit 에스컬레이션) 으로 트리거 종류와 무관하게 자동 복구 보장.
- 중기(원인 계측): `__subscriber`(kis_auth_llm.py:806) recv 루프에 "마지막 recv 진행 시각" 을 매 loop 마다 기록하고, 일정 시간 진척 없으면 self-raise → `__runner` 의 except(986)로 떨어져 reconnect 하도록 하는 SDK 레벨 self-heal 추가 검토.

### 결정 사항
- (미정)

---

## Issue 4: 09:01 두절 ~ 09:03:35 첫 감지 갭 / rebuild 종료조건 부재

### 현상
- 09:01:05 무수신 시작, 첫 이상 로그(`_trigger_ws_rebuild timeout`)는 09:03:35 → 약 2.5분 무감지 갭.
- rebuild 가 16회 반복되는 동안 "성공 처리/중단" 조건이 없어 무한 반복.

### 원인 (행번호)
- 첫 감지가 09:03:35 인 것은, rebuild 가 scheduler 의 주기 감시가 아니라 `[top]` 루프의 구독 변경(ws_realtime_trading.py:9546) 이벤트에 의해 우발적으로 호출되기 때문. 즉 구독 변경이 있어야만 비로소 `_kws_lock` 충돌로 timeout 이 드러난다. scheduler 의 정상 무수신 감시(5892, 5432)는 Issue 2 로 죽어 있어 09:01 직후 즉시 감지하지 못했다.
- rebuild 무한 반복: `_trigger_ws_rebuild`(4609)에는 연속 실패 카운터/상한이 없다. timeout 마다 경고만 찍고(4623) 종료하므로, `[top]` 루프가 호출할 때마다 무한히 같은 timeout 을 반복한다(종료조건 부재).

### 수정 방안
- Issue 2 (1) 의 `_rebuild_fail_count` + `os._exit(2)` 상한이 그대로 종료조건이 된다(연속 3회 → 하드종료 → runner 재시작).
- Issue 2 (2) 의 독립 watchdog 이 도입되면 09:01 직후 idle 누적을 scheduler 와 무관하게 감지 → 약 180초 후 자동 종료(현재의 2.5분 무감지 + 56분 방치 구조를 제거).

### 결정 사항
- (미정)

---

## 부수 발견 (가볍게 기록): 5/20 기동 프로세스 PID 1328909 6일 좀비 — 동일 wedge 패턴 의심

### 현상
```
2026-05-20 15:58:00,477 | INFO | [15:58_시간외종가] 잔고검증 완료(4822) — 변동 없음
2026-05-20 16:00:00,104 | INFO | [시간외] 예상체결가 구독 시작
   ← 이후 로그 완전 정지 (18:00 EXIT/os._exit(0) 미도달)
2026-05-26 10:01:32,791 | WARNING | [shutdown] signal=15 @ 2026-05-26 10:01:32   ← 수동 kill
```
- `wss_TR_260520.log` 가 2026-05-20 16:00:00 "[시간외] 예상체결가 구독 시작" 이후 6일간 멈춤. CLOSE-WAIT 소켓 누수(WSS fd=8 1개, REST :9443 다수). runner 1328891 이 이 hung 프로세스 wait 에 영구 묶임. 매일 cron 새 runner 가 떠 정상 동작은 했으나 좀비만 누적.

### 원인 (코드 추적, 가벼운 수준)
- "[시간외] 예상체결가 구독 시작" 은 `_log_mode_transition(... OVERTIME_EXP)`(ws_realtime_trading.py:4713)에서 찍힌다. 이 직후 scheduler_loop 는 line 5839~5842 의 `with _kws_lock: _apply_subscriptions(...)` 로 시간외 예상체결가 구독(send)을 실행한다.
- **Issue 1 과 동일 메커니즘으로 추정**: 16:00 전환 시 `send_request`(kis_auth_llm.py:1074, 무한대기)가 wedge 된 루프 위에서 블로킹 → scheduler_loop 정지 → 18:00 EXIT 경로의 `os._exit(0)`(ws_realtime_trading.py:5756)에 영원히 도달하지 못함 → 프로세스가 좀비로 잔존.
- 즉 5/26 장중 사고와 5/20 좀비는 **같은 단일 결함(타임아웃 없는 send_request + 단일 스레드 의존)** 의 다른 발현이다. Issue 1 수정이 양쪽을 동시에 해소한다.

### 수정 방안 (가벼움)
- Issue 1 (send_request timeout) 적용 시 16:00 시간외 전환 send 도 timeout 으로 빠져나와 scheduler_loop 가 EXIT 경로까지 진행 가능 → 좀비 방지.
- 보강: 18:00 EXIT 의 `os._exit(0)`(5756)에 도달 못 하는 케이스 대비, runner 측에서 "프로세스가 살아있되 N분 무로그/무진척" 이면 강제 kill 하는 외부 워치독(또는 Issue 2 (2)의 독립 watchdog 이 시간외에도 동작) 권장.

### 결정 사항
- (미정)

---

## 종합 수정 우선순위 (행번호 포함)

1. **[필수] kis_auth_llm.py:1074** `fut.result()` → `fut.result(timeout=N)` (send_request 무한대기 제거). 5/26 장중 두절 + 5/20 좀비 양쪽의 근본.
2. **[필수] ws_realtime_trading.py:4609 `_trigger_ws_rebuild`** 연속 실패 카운터 + 상한 도달 시 `os._exit(2)` (rebuild 무한루프 → runner 재시작 에스컬레이션, 사용자 요청 안전판).
3. **[권장] 무수신 watchdog 분리** — 현재 scheduler_loop 내부(ws_realtime_trading.py:5426~5456)에 종속된 watchdog 을 독립 데몬 스레드로 분리하여 scheduler 정지와 무관하게 `os._exit(2)`(5450) 보장.
4. **[권장] watchdog 시간창 확대** — ws_realtime_trading.py:5432 `dtime(9,30)` → `dtime(9,0)` (개장 직후 구독 분리 트리거 사각지대 제거; GRACE/ALERT 별도 튜닝).
5. **[검토] SDK __subscriber self-heal** — kis_auth_llm.py:806~945 recv 진척 감시 후 정체 시 self-raise → __runner reconnect(986).
