# WS Monitoring Research 260407: WSS 실시간 데이터 미수신 (260403, 260406)

## Issue 1: WSS 구독 생략으로 인한 실시간 데이터 전면 미수신

### 현상
```
260401: total=588420, WSS=581844(98.9%), REST=6576(1.1%)   -- 정상
260402: total=788223, WSS=783850(99.4%), REST=4373(0.6%)   -- 정상
260403: total=14142,  WSS=0(0.0%),      REST=14142(100.0%) -- 전면 REST
260406: total=17175,  WSS=0(0.0%),      REST=17175(100.0%) -- 전면 REST
260407: total=459234, WSS=458876(99.9%), REST=358(0.1%)    -- 정상 (현재 진행 중)
```
- 260403, 260406 양일 모두 WSS 실시간 체결 데이터(H0STCNT0, H0STANC0) 수신이 0건
- `data/wss_data/` parquet의 `tr_id` 컬럼 기준: 전량 `FHKST01010100` (REST API 보충)
- 260407은 정상 수신 중 (99.9% WSS)

### 1분봉 parquet (data/1m_data/) 관련 참고
- `data/1m_data/20260403_1m_chart_DB_parquet.parquet` (1,689,872건)은 장 종료 후 `kis_1m_API_to_Parquet_all_code.py` 배치 스크립트가 REST API로 전 종목 1분봉을 다운로드한 결과물
- 이 파일은 WSS 실시간 데이터와 무관하며, 별도의 `source` 컬럼이 없음
- WSS 실시간 데이터는 `data/wss_data/` 경로에 별도 저장됨

### 원인 (ws_realtime_trading.py:8648-8665)

**근본 원인: 보유종목 없을 때 WSS 연결을 완전히 건너뛰는 로직**

`run_ws_forever()` 함수 진입 시, 보유종목(`_str1_sell_state`)이 없으면 WSS 연결 자체를 시도하지 않고 `END_TIME(20:01)`까지 30분 간격 sleep 루프에 진입한다.

```python
# ws_realtime_trading.py:8648-8665
# ── 보유종목 없으면 WSS 구독 불필요 → END_TIME까지 대기 ──
with _str1_sell_state_lock:
    _has_held = any(not st.get("sold") for st in _str1_sell_state.values())
if not _has_held:
    now_t = datetime.now(KST).time()
    in_closing = (dtime(15, 20) <= now_t < dtime(15, 31)) or bool(_closing_codes)
    in_nxt = mode in (RunMode.NXT_PRE, RunMode.NXT_AFTER) and bool(_nxt_target_codes)
    if in_closing or in_nxt:
        logger.info(f"... [ws] 보유종목 없지만 {'NXT' if in_nxt else '종가매매'} 구간 → WSS 유지")
    elif now_t < END_TIME:
        logger.info(f"... [ws] 보유종목 없음 → WSS 구독 생략, {END_TIME.strftime('%H:%M')}까지 대기")
        while not _stop_event.is_set():
            if datetime.now(KST).time() >= END_TIME:
                break
            time.sleep(1800.0)  # 30분 sleep
        break  # <-- run_ws_forever 완전 종료
```

**260403/260406의 실행 흐름:**
1. 07:50 프로그램 시작, 보유종목 0건
2. `run_ws_forever()` 진입 -> `_has_held = False`
3. 종가매매/NXT 구간 아님 -> "WSS 구독 생략" 출력
4. 30분 sleep 루프 진입 -> 20:01까지 WSS 연결 없이 대기
5. `[top]` 스레드는 정상 작동하며 종목을 추가하지만, WSS 연결이 없으므로 실제 구독은 불가
6. `[price]` watchdog + `[rest]` worker만 REST API로 소량의 보충 데이터 저장

**260407이 정상인 이유:**
- 07:50 시작 시 보유종목 1건 (모비릭스 348030, 11주)
- `_has_held = True` -> WSS 연결 시도 -> 정상 수신

로그 증거:
```
# 260403 (비정상)
[260403_075009_TR] [ws] 보유종목 없음 → WSS 구독 생략, 20:01까지 대기

# 260406 (비정상)
[260406_075009_TR] [ws] 보유종목 없음 → WSS 구독 생략, 20:01까지 대기

# 260407 (정상)
[260407_075010_TR] [ws] connect attempt=1 mode=PREOPEN_WAIT
```

### 부가 현상: WSS 미연결 상태에서의 "no data" 경고 스팸

260403 로그에 78건, 260406 로그에 57건의 `[ws] no data for Ns -> resubscribe` 경고가 발생했다.

원인: `scheduler_loop` (line 4509)의 watchdog이 `_last_any_recv_ts > 0`일 때 작동하는데, REST 보충 데이터가 ingest되면서 `_last_any_recv_ts`가 갱신(line 8166)됨. 이후 WSS 데이터가 없으므로 idle 타이머 초과 -> resubscribe 시도. 그러나 `_active_kws`가 None이므로 실제 재구독은 불가.

### 수정 방안

**방안 A (권장): 보유종목 없어도 WSS 연결 유지**

`run_ws_forever()` line 8648-8665의 "보유종목 없으면 WSS 생략" 로직을 제거하거나, 조건을 완화한다. 트레이딩 시스템은 매수 기회를 잡기 위해 항상 실시간 시세를 수신해야 한다.

```python
# 수정: 보유종목 없어도 정규장/시간외 시간대에는 WSS 유지
# line 8648-8665 블록을 삭제하거나, 조건을 다음으로 변경:
if not _has_held:
    now_t = datetime.now(KST).time()
    # 정규장 전/후 대기 시간만 sleep (장 시작 전 07:50~08:50)
    if now_t < dtime(8, 50):
        # 08:50까지만 대기, 이후 WSS 연결
        ...
    # 그 외에는 WSS 연결 유지
```

**방안 B (최소 수정): sleep 간격을 줄이고 보유종목 변화 감지**

30분(1800초) sleep을 짧은 간격(예: 10초)으로 변경하고, 매수가 발생하면(`_has_held` 변경) WSS 연결을 시작하도록 한다.

```python
# line 8663 수정
while not _stop_event.is_set():
    if datetime.now(KST).time() >= END_TIME:
        break
    with _str1_sell_state_lock:
        if any(not st.get("sold") for st in _str1_sell_state.values()):
            break  # 보유종목 생김 -> WSS 연결 시작
    time.sleep(10.0)  # 30분 -> 10초
# break -> continue (run_ws_forever 루프 재진입)
```

### 결정 사항
- (미결정 - 사용자 확인 필요)

---

## Issue 2: top 스레드가 WSS 미연결 상태에서 구독 추가 시도

### 현상
```
[260403_090000_TR] [top] 구독 추가: ['000020', ...] (총 9/40)
[260403_090500_TR] [top] 구독 추가: ['065770', ...] (총 9/40)
... (260403에 16회, 260406에 16회 구독 추가 시도)
```
- WSS 연결이 없는 상태에서 `[top]` 스레드가 종목을 계속 추가
- 실제 구독은 이루어지지 않으므로 데이터 수신 불가

### 원인
- `[top]` 스레드는 `run_ws_forever()`와 독립적으로 실행
- `_subscribed` dict에 종목코드를 추가하지만, WSS 연결(`_active_kws`)이 None이므로 실제 서버 구독은 불가
- top 스레드는 WSS 연결 상태를 확인하지 않음

### 수정 방안
- Issue 1이 해결되면 자동으로 해결됨 (WSS가 항상 연결되므로)
- 추가적으로: top 스레드에서 `_active_kws is None`이면 구독 요청을 스킵하는 가드 추가 가능

### 결정 사항
- (미결정 - Issue 1 해결에 종속)

---

## 요약

| 날짜 | WSS 데이터 | REST 데이터 | 상태 | 원인 |
|------|-----------|------------|------|------|
| 260401 | 581,844 (98.9%) | 6,576 (1.1%) | 정상 | - |
| 260402 | 783,850 (99.4%) | 4,373 (0.6%) | 정상 | - |
| 260403 | 0 (0.0%) | 14,142 (100%) | 비정상 | 보유종목 없음 -> WSS 생략 |
| 260406 | 0 (0.0%) | 17,175 (100%) | 비정상 | 보유종목 없음 -> WSS 생략 |
| 260407 | 458,876 (99.9%) | 358 (0.1%) | 정상 | 보유종목 있음 (모비릭스 11주) |

**근본 원인**: `run_ws_forever()` (line 8648-8665)에서 보유종목이 없으면 WSS 연결을 20:01까지 완전히 건너뛰는 설계. 매수 기회 탐색을 위한 실시간 시세 수신이 불가능해짐.

**관련 파일**:
- `/home/ubuntu/Stoc_Kis/ws_realtime_trading.py` (line 8648-8665: WSS 생략 로직, line 4509: no data watchdog)
- `/home/ubuntu/Stoc_Kis/data/wss_data/` (WSS 실시간 데이터 저장)
- `/home/ubuntu/Stoc_Kis/out/logs/wss_TR_2604*.log` (실행 로그)
