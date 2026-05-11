# WS Monitoring Research 260417: WSS 장전 반복 끊김 + 매도 수량 불일치 + 09:08 데이터 수신 중단 + MAX SUBSCRIBE OVER

## Issue 1: WSS 장전 반복 끊김 (08:51~08:59, attempt 2~19)

### 현상
```
08:51:25 [ws] kws.start returned (mode=PREOPEN_WAIT)
08:51:27 [ws] connect attempt=2 mode=PREOPEN_EXP
08:52:10 [ws] kws.start returned (mode=PREOPEN_EXP)
08:52:12 [ws] connect attempt=3 mode=PREOPEN_EXP
...
08:59:32 [ws] kws.start returned (mode=PREOPEN_EXP)
08:59:34 [ws] connect attempt=19 mode=PREOPEN_EXP
```
- 약 10~20초마다 `kws.start returned` -> reconnect 반복
- PREOPEN_EXP 모드에서 20종목 exp_ccnl_krx + 4종목 H0STMKO0 + 12종목 H0STCNI0 = 36슬롯 구독
- 일부 구간에서 "10초 데이터 미수신 -> 재구독 시도" 메시지 동반

### 원인
- KIS WSS 서버가 장전 동시호가 시간대(08:50~09:00)에 불안정하여 WebSocket 연결을 주기적으로 끊음
- 이 시간대에 KIS REST API도 간헐적 500 에러 발생 (잔고조회, 미체결조회 등)
- `kws.start`가 정상적으로 `return`하므로 `run_ws_forever` 루프가 정상 동작하여 재연결 수행
- 데이터 자체는 연결 사이사이에 수신됨 (parquet part save 정상 진행)

### 수정 방안
- 이 문제 자체는 KIS 서버측 불안정으로 코드 수정으로 해결 불가
- 현재 reconnect 로직은 정상 동작 중
- 다만 장전 시간대 reconnect 빈도가 높으면 `backoff`을 장전 시간대에만 늘리는 것을 검토 가능
- 08:50~09:00 구간에서 reconnect 시 auth_ws 재발급이 매번 일어나므로, 불필요한 경우 캐시 검토

---

## Issue 2: 시공테크(020710) 및 케이사인(192250) 매도 수량 불일치

### 현상
```
08:42:02 [str1_sell] 미매도 포지션 복원 2건
  시공테크(020710) 수량=15
  케이사인(192250) 수량=5

08:51:03 [보유종목 모니터링]
  020710  시공테크     3         3     5,990   -> 실보유 3주
  192250  케이사인     1         1    15,880   -> 실보유 1주

08:55:39 매도주문 실패 케이사인(192250) 수량=5 -> 주문 가능한 수량을 초과
08:59:01 매도주문 실패 시공테크(020710) 수량=15 -> 주문 가능한 수량을 초과
```
- sell_state qty=15 vs 실보유=3 (시공테크)
- sell_state qty=5 vs 실보유=1 (케이사인)
- 시초가하락 매도 시도 -> "주문 가능한 수량을 초과" 에러
- 이후 매도 재시도 없음 (sold=False, sell_ordered 상태 불명)

### 원인 (ws_realtime_trading.py:1232~1284)
1. **07:50 시작 시 잔고조회 500 에러** (로그 line 15-16): `_startup_balance_map`이 빈 dict
2. **08:42 매도 상태 보충** (line 4248-4264): `_get_balance_holdings()` 재호출
3. **잔고조회 반환값 문제**: KIS API가 500 에러를 간헐적으로 반환하면서, `_get_balance_page`의 첫 페이지는 성공하고 후속 페이지에서 실패. 이때 `hold_map`에는 첫 페이지 데이터가 남아있어 2종목 반환.

```python
# ws_realtime_trading.py:1269~1274
qty = int(float(str(row.get("hldg_qty", 0) or 0).replace(",", "") or 0))
psbl = int(float(str(row.get("ord_psbl_qty", 0) or 0).replace(",", "") or 0))
pchs = float(str(row.get("pchs_avg_pric", 0) or 0).replace(",", "") or 0)
effective_qty = psbl if psbl > 0 else qty  # 핵심 문제
```

4. **qty 결정 로직 버그**: `ord_psbl_qty`가 0이면 `hldg_qty`를 사용. KIS API 불안정 시 `ord_psbl_qty=0`을 반환할 수 있으며, 이때 `hldg_qty`는 미결제/대기 수량을 포함한 총 보유수량을 반환.
   - 시공테크: hldg_qty=15 (이전 closing_buy 미결제 포함), ord_psbl_qty=0 -> effective_qty=15
   - 케이사인: hldg_qty=5 (이전 closing_buy 미결제 포함), ord_psbl_qty=0 -> effective_qty=5
   - 실제 매도 가능 수량: 시공테크=3, 케이사인=1

5. **매도 실패 후 재시도 부재**: 매도 실패 시 `sell_ordered=False`로 리셋하지만, 동시호가 매도 시간(08:55~08:59)이 지나면 재시도 트리거가 없음. 정규장 매도(`check_sell_conditions`)는 WSS 데이터 수신 기반이나, 09:08 이후 WSS 중단으로 작동 불가.

### 수정 방안
1. **`_get_balance_holdings` qty 결정 로직 개선**: `ord_psbl_qty=0`일 때 `hldg_qty` 전체를 사용하지 말고, 매도 주문 시 실제 `ord_psbl_qty`를 재조회하여 사용
2. **매도 주문 시 수량 재검증**: 매도 직전에 `ord_psbl_qty`를 한번 더 확인하고, sell_state의 qty를 보정
3. **매도 실패 후 재시도 메커니즘**: sell_ordered=False 상태에서 정규장 중 WSS 틱 수신 시 재매도 시도
4. **sell_state 등록 시 hldg_qty와 ord_psbl_qty를 분리 저장**: `balance_qty`(총보유)와 `sellable_qty`(매도가능)를 구분

### 결정 사항
- (미결정)

---

## Issue 3: 09:08 이후 WSS 수신 완전 중단 + reconnect 미발동

### 현상
```
09:08:56 [part] 1000rows: rows=2000  (마지막 정상 parquet save)
09:09:01 [종목상태] 케이사인(192250) H0STMKO0 iscd_stat=57  (마지막 WSS 데이터)
09:09:56 [part] periodic: rows=2463  (잔여 버퍼 flush)
09:10:05 [ws] _trigger_ws_rebuild timeout(5s) -- 구독 갱신 지연
09:15:06 [ws] _trigger_ws_rebuild timeout(5s)
09:30:05 [ws] _trigger_ws_rebuild timeout(5s)
10:00:05 [ws] _trigger_ws_rebuild timeout(5s)
... (이후 매 top rank 조회마다 반복)
12:03:45 [shutdown] signal=15 (수동 종료)
12:03:45 10484초 데이터 미수신 -> 재구독 시도
```
- `kws.start returned` 로그 없음 -> `kws.start`가 아직 블로킹 중
- `run_ws_forever`의 reconnect 루프 진입 불가
- 프로세스는 살아있지만 WSS 데이터 수신 전무 (약 3시간)

### 원인 (ws_realtime_trading.py:3738~3751, 8596~8780)

**1단계: WSS 연결 silent death**
- attempt 19 (08:59:34) 이후 `kws.start(on_result=on_result)` (line 8738) 실행 중
- 09:00에 모드 전환 (exp_ccnl_krx -> ccnl_krx), 구독 해제/추가 정상 동작
- 09:08~09:09 사이에 KIS WSS 서버가 연결을 끊었으나, TCP close frame/exception 없이 silent disconnect 발생
- `kws.start` 내부의 `recv()`가 무한 대기 상태에 진입

**2단계: `_trigger_ws_rebuild` 교착**
```python
# ws_realtime_trading.py:3738~3751
def _trigger_ws_rebuild():
    def _do():
        with _kws_lock:                     # (A) RLock 획득
            if _active_kws is not None:
                desired = _desired_subscription_map(datetime.now(KST))
                _apply_subscriptions(_active_kws, desired)  # (B) kws.send() 호출

    t = threading.Thread(target=_do, daemon=True)
    t.start()
    t.join(timeout=5.0)                     # (C) 5초 대기
    if t.is_alive():
        logger.warning("timeout(5s)")       # 로그만 남기고 종료
```

- `_apply_subscriptions` 내부에서 `_send_subscribe(kws, req, batch, "2")` 호출 (line 8510)
- `_send_subscribe`는 `kws.send()` -> dead socket에 write -> TCP send buffer가 가득 차면 블로킹
- 또는 KIS WebSocket 라이브러리의 `send()`가 내부적으로 recv 대기와 경합
- 5초 timeout 후 daemon thread는 버려지지만, `_active_kws`는 여전히 dead socket을 가리킴
- **다음 `_trigger_ws_rebuild` 호출도 동일하게 dead socket에 send 시도 -> timeout 반복**

**3단계: reconnect 불가**
- `kws.start`가 return하지 않으므로 `run_ws_forever`의 while 루프가 다음 iteration으로 진행 불가
- `_trigger_ws_rebuild`는 reconnect가 아닌 구독 갱신만 시도 (line 3751 주석: "소켓은 닫지 않음")
- **강제로 `kws.close()`를 호출하는 watchdog가 없음**

**구조적 결함**:
- 데이터 미수신 감지 로직(`10초 데이터 미수신`)은 `_trigger_ws_rebuild`를 호출하지만, rebuild가 실패해도 소켓을 닫지 않음
- `run_ws_forever`가 `kws.start` 블로킹에서 벗어나려면 `kws.close()`가 외부에서 호출되어야 하는데, 그런 메커니즘이 없음

### 수정 방안
1. **Dead connection watchdog 추가**: 마지막 데이터 수신 후 N초(예: 60초) 경과 시 `_active_kws.close()` 강제 호출 -> `kws.start` return -> reconnect 루프 진행
2. **`_trigger_ws_rebuild` timeout 시 소켓 강제 종료**: timeout(5s) 발생 시 `_active_kws`를 `None`으로 설정하고 `kws.close()` 호출
3. **TCP keepalive 설정**: KIS WebSocket 연결에 TCP keepalive 파라미터 설정하여 silent death 방지
4. **`_trigger_ws_rebuild` 연속 실패 카운터**: N회 연속 timeout 시 소켓 강제 종료

구체적 구현 예시 (watchdog 방식):
```python
# scheduler_loop 등에서 주기적 체크
def _check_ws_liveness():
    now = time.time()
    if _active_kws is not None and _last_any_recv_ts > 0:
        elapsed = now - _last_any_recv_ts
        if elapsed > 60:  # 60초 데이터 미수신
            logger.warning(f"[ws] {elapsed:.0f}s 데이터 미수신 -> 소켓 강제 종료")
            try:
                _active_kws.close()
            except Exception:
                pass
```

### 결정 사항
- (미결정)

---

## Issue 4: 12:08 재시작 후 MAX SUBSCRIBE OVER

### 현상
```
12:08:45 [ws] open_map prepared: exp_ccnl_krx=1종목, ccnl_krx=38종목, H0STMKO0=2종목 total=41
12:08:49 [ws] connect attempt=2
12:08:49 [ws] open_map prepared: exp_ccnl_krx=1종목, ccnl_krx=38종목, H0STMKO0=2종목 total=41
12:08:55 [ws][system] tr_id=H0STMKO0 tr_key=005930 msg=MAX SUBSCRIBE OVER
```
- 구독 총 슬롯: H0STCNI0 1 + exp_ccnl_krx 1 + ccnl_krx 38 + H0STMKO0 2 = 42
- KIS WSS 최대 구독 슬롯: 40
- H0STMKO0 구독 시 MAX SUBSCRIBE OVER 발생

### 원인 (ws_realtime_trading.py:453, 8656~8694)

```python
# line 453
MAX_WSS_SUBSCRIBE = 40  # WSS 구독 총 상한 (H0STCNI0 체결통보 1슬롯 별도 확보)
```

- `MAX_WSS_SUBSCRIBE=40`은 종목 데이터 구독(ccnl_krx, exp_ccnl_krx)에 대한 상한
- `total_sub`에 ccnl_krx(38) + exp_ccnl_krx(1) = 39를 계산하고, H0STMKO0(2) 추가 -> 41
- 여기에 H0STCNI0(1) -> 합계 42
- KIS WSS 서버의 실제 상한은 **40슬롯 (모든 구독 유형 합산)**
- 코드의 `MAX_WSS_SUBSCRIBE` 체크 (line 8658-8661)는 WARNING만 출력하고 구독을 진행함
- H0STCNI0를 "별도 확보"로 간주하지만 실제로는 총 슬롯에 포함됨

슬롯 계산:
| 구독 유형 | 슬롯 수 | 비고 |
|-----------|---------|------|
| H0STCNI0 (체결통보) | 1 | tr_key=sywems12 |
| ccnl_krx (실시간체결) | 38 | 종목별 1슬롯 |
| exp_ccnl_krx (예상체결) | 1 | 셀리드(299660) |
| H0STMKO0 (장운영정보) | 2 | 삼성전자+카카오게임즈 |
| **합계** | **42** | **40 초과** |

### 수정 방안
1. **`MAX_WSS_SUBSCRIBE` 계산에 H0STCNI0, H0STMKO0 포함**: 총 슬롯 = H0STCNI0(1) + H0STMKO0(N) + 종목데이터(M), M <= 40 - 1 - N
2. **종목 데이터 상한을 동적 계산**: `available_data_slots = 40 - 1(H0STCNI0) - len(mko_codes)(H0STMKO0)`
3. **구독 전 총 슬롯 체크 후 초과 시 우선순위 낮은 종목 제거**: H0STMKO0 keep-alive는 보유종목과 겸용 가능
4. **H0STMKO0 keep-alive를 보유종목이 없을 때만 추가**: 재시작 시 보유종목 없음 -> keep-alive 2개만으로도 2슬롯 차지

재시작 시 구독 상한을 준수하려면:
- 종목 데이터 슬롯: 40 - 1(H0STCNI0) - 2(H0STMKO0 keep-alive) = 37
- 현재 39종목(ccnl_krx 38 + exp_ccnl_krx 1) -> 37로 줄여야 함
- top_added에서 2종목 제외하거나, H0STMKO0 keep-alive를 1개로 줄이기

### 결정 사항
- (미결정)

---

## 부가 사항: KIS REST API 500 에러 빈도

로그에서 확인된 500 에러 발생 시점:
- 07:50:06 시작 잔고조회 실패
- 07:50:08 미체결취소 실패
- 08:42:00 잔고조회 실패 (str1_sell 보충 중)
- 08:50:03 장전 예상체결가 모니터링 조회 실패
- 08:55:40 미체결 매도 취소 실패
- 08:58:00 08:58 잔고조회 실패
- 08:59:02 미체결 매도 취소 실패
- 08:59:04 장전 예상체결가 모니터링 조회 실패
- 09:05:07 보유종목 모니터링 조회 실패
- 09:06:07 보유종목 모니터링 조회 실패
- 12:08:45 미체결취소 실패 (재시작 후)

KIS API 서버가 이날 전반적으로 불안정했으며, 이것이 Issue 2(qty 불일치)와 Issue 3(WSS silent death)의 간접 원인.
