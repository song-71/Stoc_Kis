# WS Monitoring Research 260416: WebSocket 데이터 미수신 resubscribe 무한 루프

## Issue 1: resubscribe 반복 실패로 인한 장기 데이터 미수신 (09:20~09:50)

### 현상
```
09:20~09:30: "no data for 564s~1158s -> resubscribe" 반복
09:30:13: WARNING 발생 후 리셋
09:31:58: "no data for 15s -> resubscribe" 재시작
09:31~09:50: idle_sec 15s -> 1098s 까지 증가 (약 18분 추가 장애)
```
- resubscribe가 15초 간격으로 반복 실행되지만 데이터가 복구되지 않음
- 동일 패턴이 두 번 발생 (09:20~09:30, 09:31~09:50)

### 원인 (ws_realtime_trading.py)

**"no data for" 로그**: line 4458-4459
**resubscribe 호출**: line 4461-4463 -> `_apply_subscriptions(force=True)` (line 8320-8334)
**idle_sec 계산**: line 4444 (`now_ts - _last_any_recv_ts`)
**_last_any_recv_ts 갱신**: line 7976 (ingest_loop에서 데이터 수신 시만)

핵심 구조적 문제:
1. **재연결 에스컬레이션 부재**: scheduler의 no-data 감지 경로 (line 4455-4464)에 `_request_ws_close()` 호출이 없음. 죽은 소켓에 resubscribe만 무한 반복
2. **_send_subscribe 실패 무시**: line 8305-8310에서 예외를 warning으로만 처리. `_subscribed` 맵은 실패와 무관하게 갱신 (line 8331)
3. **idle_sec 리셋 불가**: `_last_any_recv_ts`는 실제 데이터 수신 시에만 갱신되므로, resubscribe 성공/실패와 무관하게 계속 증가

### 수정 방안
1. `NO_DATA_FORCE_RECONNECT_SEC = 60` 상수 추가, 60초 이상 미수신 시 `_request_ws_close(_active_kws)` 호출하여 WSS 재연결 에스컬레이션 (line 4455 부근)
2. `_send_subscribe` 실패 시 `_subscribed` 갱신 스킵 + 연속 실패 카운터로 재연결 트리거
3. `_apply_subscriptions(force=True)` 반환값으로 성공 여부 전파

상세 코드 예시는 `/home/ubuntu/Stoc_Kis/out/monitor/issue_analysis_260416.md` Issue 6 참조.

### 결정 사항
- (대기중)

---

## Issue 2: 전체 로그 정밀 분석 -- 시간대별 이벤트 타임라인

### 1차 기동 (07:50:04 ~ 09:00:51) -- WSS 미연결 상태 운영

```
07:50:09 [ws] 보유종목 없음 -> WSS 구독 생략, 18:00까지 대기
07:50:09 [a2-wss] 연결 시작
08:50:00 예상체결가 구독 시작: 25종목 (WSS 없는 상태에서 시도)
09:00:00 [top] 구독 추가: 5종목 (총 30/40)
09:00:00 실시간 체결가 구독 시작: 30종목 (WSS 미연결)
09:00:28 10초 데이터 미수신 -> 재구독 시도
09:00:51 [shutdown] signal=15 (수동 종료)
```
- 원인: `ws_realtime_trading.py:8102-8114` -- 보유종목 없으면 WSS 연결 자체를 건너뛰고 18:00까지 sleep(1800)
- 08:50/09:00 시간대 전환은 스케줄러가 구독 맵만 변경하지만 `_active_kws`가 None이라 무효

### 2차 기동 (09:00:55 ~ 09:30:13) -- WSS 반복 끊김 + 장기 미수신

```
09:01:25 connect #1 -- ccnl_krx=30, H0STCNI0=1 (total=31/41)
09:03:59 kws.start returned (2.5분 유지 후 끊김)
09:04:01 connect #2 -- 데이터 수신 정상
09:05:06 top 교체: 5해제 + 5추가 (총 30/40)
09:05:42 kws.start returned (1.7분)
09:05:44 connect #3
09:06:53 kws.start returned (1.1분)
09:06:55 connect #4
09:08:29 kws.start returned (1.5분)
09:08:31 connect #5
09:09:52 kws.start returned (1.3분)
09:09:54 connect #6
09:10:00 [a2-wss] 연결 완료 (8.5분 소요)
09:10:01 top 교체: 4해제 + 5추가 (총 31/40)
09:10:03 구독 해제/추가 실패: no close frame received or sent
09:10:04 kws.start returned (10초)
09:10:06 connect #7 -- ccnl_krx=30, H0STCNI0=a2에서 처리
09:10:48~ force resubscribe 무한 반복 (~80회, 15초 간격)
09:30:13 [shutdown] signal=15 (수동 종료)
```

### 3차 기동 (09:30:48 ~ 10:14:52) -- 잠시 수신 후 장기 미수신

```
09:31:08 connect #1 -- 30종목
09:31:37 [a2-wss] H0STCNI0 구독 완료 (30초 만에 연결)
09:31:43 데이터 수신 정상 (tr_id전환 로그 존재)
09:31:53 10초 미수신 -> force resubscribe 무한 반복
10:14:42 2579초(43분) 미수신 -> force resubscribe 계속
10:14:52 [shutdown] signal=15 (수동 종료)
```

### 4~7차 기동 (10:15 ~ 10:57+) -- 4.1 복원본 전환, WSS 구독 생략

```
10:16:02 [ws] 보유종목 없음 -> WSS 구독 생략 (4.1 복원본)
10:17:01 signal=15
10:17:26 WSS 구독 생략
10:18:22 signal=15
10:18:54 WSS 구독 생략
(... 이후 반복 ...)
10:57:43 WSS 구독 생략 -- REST 보충만으로 운영
```

---

## Issue 3: 구독 슬롯 사용량 분석 (41건 한도 대비)

### a1 WSS 구독 내역

| 시각 | ccnl_krx (H0STCNT0) | H0STCNI0 | H0STMKO0 | 합계 | 한도(41) | 비고 |
|------|---------------------|----------|----------|------|----------|------|
| 09:01:25 | 30 | 1 (sywems12) | 0 | **31** | OK | 보유종목 없어서 H0STMKO0=0 |
| 09:05:06 | 30 (-5+5) | 1 | 0 | **31** | OK | top 교체 |
| 09:10:01 | 31 (-4+5) | 0 (a2에서) | 0 | **31** | OK | a2 연결 후 H0STCNI0 a1 스킵 |
| 09:31:08 | 30 | 1 | 0 | **31** | OK | 3차 기동 |

**결론: 41건 한도 초과는 단 한 번도 발생하지 않았음.** MAX_WSS_SUBSCRIBE=40 설정으로 ccnl_krx를 40건까지 제한하고, H0STCNI0 1건은 별도 확보. 실제 사용량은 최대 31~32건.

---

## Issue 4: H0STCNI0 체결통보 구독 방식 확인

### 코드 분석 (ws_realtime_trading_v0416.py)

**초기 구독 (line 8487-8498):**
- a2 WSS 연결 완료 시: a2에서 H0STCNI0 1건 구독 (tr_key=sywems12), a1에서는 스킵
- a2 미연결 시: a1에서 H0STCNI0 1건 구독 (tr_key=sywems12)

**주문 시 동적 구독 (_ccnl_notice_sub_add, line 2315-2339):**
- a2 연결 + 구독 완료 상태면: set에 코드 추가만 하고 WSS 요청 안 보냄
- a2 미연결이면: a1에서 `_send_subscribe(ccnl_notice, [htsid], "1")` -- 동일 htsid 재구독 (ALREADY IN SUBSCRIBE 응답)

**결론: H0STCNI0은 HTS ID(sywems12) 단위 1건만 구독. 종목별 개별 구독 아님. 슬롯 낭비 없음.**

---

## Issue 5: force resubscribe 무한 반복의 근본 원인

### 코드 경로 (ws_realtime_trading_v0416.py)

```
scheduler_loop (line 4436-4464):
  idle_sec = now_ts - _last_any_recv_ts
  if idle_sec > NO_DATA_REBUILD_SEC (15초):
      _apply_subscriptions(force=True)  -- UNSUB 30종목 + SUB 30종목
      (데이터 미수신이면 15초 후 또 실행)

  if idle_sec > 15s 별도 경로:
      [ws] no data for {idle_sec}s -> resubscribe
      _apply_subscriptions(force=True)  -- 또 실행
```

- 15초마다 2개 경로에서 각각 force resubscribe 호출 (실질적으로 이중 실행)
- 매 cycle: UNSUBSCRIBE 30건(3 batch) + SUBSCRIBE 30건(3 batch) = 6회 send
- 20분간 약 80cycle = **480회 구독 메시지 전송**
- WSS 연결은 살아있으나 서버가 데이터를 보내지 않는 "좀비 소켓" 상태
- `kws.start()`가 블로킹으로 반환되지 않아 reconnect loop 진입 불가

### Issue 1 수정 방안에 추가

- **P1 (이미 제안됨):** force resubscribe 연속 3회 실패 시 `kws.close()` 강제 호출
- **추가:** 이중 경로 정리 -- 두 곳에서 동시에 force resubscribe 호출하는 것을 하나로 통합

---

## Issue 6: "보유종목 없음 -> WSS 구독 생략" 설계 결함

### 코드 (ws_realtime_trading.py:8098-8114)

```python
# run_ws_forever() 진입부
mode = _get_mode()
if mode == RunMode.EXIT:
    break

with _str1_sell_state_lock:
    _has_held = any(not st.get("sold") for st in _str1_sell_state.values())
if not _has_held:
    now_t = datetime.now(KST).time()
    if now_t < END_TIME:
        logger.info(f"... [ws] 보유종목 없음 -> WSS 구독 생략, ...")
        while not _stop_event.is_set():
            if datetime.now(KST).time() >= END_TIME:
                break
            time.sleep(1800.0)  # 30분 sleep
        ...
    break
```

- 이 로직은 **매수 판단을 위한 실시간 데이터 수신 자체를 차단**함
- 보유종목 없을 때 WSS를 열지 않으면 매수 기회를 잡을 수 없음
- 4.1 원본 및 복원본 모두에 존재

### 수정 방안
- **P0 (최우선):** `if not _has_held:` 블록 전체 제거
- 대안: 보유종목 없어도 WSS 연결은 진행, H0STMKO0/H0STCNI0 구독만 생략

---

## Issue 7: a2 WSS 연결 상태

### 타임라인
| 시각 | 이벤트 | 소요시간 |
|------|--------|----------|
| 07:50:09 | a2 연결 시작 | (1차 기동) |
| 09:00:51 | a2 종료 | - (07:50 시작 후 연결 완료 로그 없음) |
| 09:01:24 | a2 연결 시작 | (2차 기동) |
| 09:10:00 | a2 연결 완료 | **8분 36초** |
| 09:30:13 | a2 종료 | |
| 09:31:06 | a2 연결 시작 | (3차 기동) |
| 09:31:36 | a2 연결 완료 | **30초** |
| 10:14:52 | a2 종료 | |
| 10:15+ | a2 연결 시작 없음 | 4.1 복원본에 a2 로직 없음 |

- 1차 기동: a2 WSS approval_key는 발급되었으나 연결 완료 로그 없음 (WSS 구독 생략으로 메인 루프 안 돎)
- 2차 기동: a2 연결에 8.5분 소요 -- 비정상적으로 긴 시간
- 3차 기동: 30초 만에 정상 연결
- 4차 이후: 4.1 복원본에 a2 WSS 로직 미포함

---

## 종합 결론

### 금일 장애 원인 3가지

1. **"보유종목 없음 -> WSS 구독 생략" 설계 결함** (P0)
   - 1차 기동(07:50)과 4~7차 기동(10:15~)에서 WSS 미연결
   - 실시간 데이터 없이 REST 보충만으로 운영 -- 매수 판단 불가

2. **WSS 반복 끊김 + 좀비 소켓 대응 부재** (P1)
   - 2~3차 기동에서 1~2분마다 WSS 끊김, 이후 좀비 소켓 상태 진입
   - force resubscribe 무한 반복 (480회+), reconnect 에스컬레이션 없음

3. **4.1 복원본에 a2 WSS 로직 누락** (P3)
   - 10:15 이후 체결통보 수신 경로(a2) 자체가 사라짐

### 41건 한도 초과 여부
- **초과 없음.** 최대 사용량 31~32건 (ccnl_krx 30~31 + H0STCNI0 1)

### H0STCNI0 종목별 구독 여부
- **종목별 구독 아님.** HTS ID(sywems12) 단위 1건만 구독. 슬롯 낭비 없음.

### stale -> 재구독 반복 횟수
- 2차 기동 09:10~09:30: 약 80회 (20분, 15초 간격)
- 3차 기동 09:31~10:14: 약 170회 (43분, 15초 간격)
- 합계: **약 250회**의 force resubscribe cycle (각 cycle에서 UNSUB 30 + SUB 30)
