# 트레이딩 시스템 심층 분석 보고서

## 1. 문제 요약

| # | 문제 | 영향 |
|---|------|------|
| 1 | 동일 종목에 종가매수(15:30), 시간외종가(15:41), 시간외단일가(16:10) 총 3회 매수 발생 (2주 x 3 = 6주) | 의도치 않은 초과 매수, 자금 초과 사용 |
| 2 | 시간외 단일가 128건 주문 중 3건만 체결 (97.7% 미체결) | 매수 실패율 비정상적으로 높음 |
| 3 | 예상체결가/체결가 전환 로직이 사용자 기대와 불일치 | 불필요한 구독, 리소스 낭비 |

---

## 2. 코드 분석

### 2.1 시간대별 매수 함수 흐름

```
15:19    _prepare_closing_buy_orders()     — 종목선정, 수량계산, 주문 데이터 생성
15:20    _switch_to_closing_codes()
           → _run_closing_buy_orders()     — ORD_DVSN=01(시장가) 주문 발송
                                              _closing_buy_placed에 기록
                                              _ccnl_notice_sub_add() 호출
                                              거래장부: buy_order 기록
15:30    체결통보(H0STCNI0) 수신           → _on_ccnl_notice_filled()
                                              _closing_filled_by_code[code] += qty
                                              거래장부: buy_fill 기록
15:30~31 종가 체결 수신 완료               → WS 닫기 → 16:00까지 대기
15:40    _run_afternoon_extra_closing_buy() — ORD_DVSN=06(장후시간외종가) 미체결분 매수
16:00    _run_overtime_buy_orders()         — ORD_DVSN=07(시간외단일가) 미체결분 매수
```

### 2.2 핵심 데이터 구조

- **`_closing_buy_prepared`**: 주문 사전 준비 데이터 (15:19에 생성)
- **`_closing_buy_placed`**: 실제 주문 발송 결과 (주문번호 포함)
- **`_closing_filled_by_code`**: 체결통보 기반 체결 누적 수량 (in-memory dict)
- **`closing_buy_state_{yymmdd}.json`**: 주문 상태 영구 저장 (orders 배열에 filled_qty, remain_qty)
- **`closing_filled_{yymmdd}.json`**: 체결 수량 영구 저장 (재시작 복원용)

### 2.3 미체결(remain) 계산 로직 — `_get_closing_filled_for_remain()` [근본 결함 확인]

```python
# 라인 2783~2816
def _get_closing_filled_for_remain(state):
    persisted = _load_closing_filled()          # 파일에서 로드
    merged = dict(persisted)
    for k, v in _closing_filled_by_code.items(): # in-memory 병합
        merged[c] = max(merged.get(c, 0), v)
    # 체결통보가 하나도 없으면 잔고조회로 보완
    if not has_any_filled:
        # REST API _get_balance_page() 호출
        merged[c] = min(ord_q, hold_map.get(c, 0))
    return merged
```

#### 근본 결함 분석

**이 함수의 "체결 기록 없으면 즉시 잔고조회" 로직(라인 2797~2813)이 중복매수의 직접적 원인이다.**

**현재 동작 (잘못된 구조)**:
1. `_run_afternoon_extra_closing_buy()` (15:40)과 `_run_overtime_buy_orders()` (16:00)에서 호출
2. 이 함수들은 각각 done 플래그로 **1회만** 실행됨
3. 함수 내에서 `_closing_filled_by_code`에 체결 기록이 하나도 없으면 **즉시** REST API 잔고조회
4. 하지만 주문 직후에는 아직 체결이 안 됐으므로 잔고도 변화 없음 → "미매수"로 판단 → 중복 주문

**왜 이게 문제인가**:
- 주문을 넣자마자 체결되는 게 아님. 체결은 시간이 걸림
- 체결통보(H0STCNI0)가 와야 `_closing_filled_by_code`가 업데이트됨
- 그런데 WS가 닫혀서 체결통보를 못 받음 → filled가 비어있음
- 비어있으니 즉시 잔고조회 → 아직 체결 안 됐으니 잔고도 변화 없음
- 결론: "미매수"로 판단 → 15:40, 16:00에서 중복 주문 발생

**올바른 흐름 (수정 방향)**:
1. 주문 발송 (15:20에 ThreadPoolExecutor 병렬 처리 — 이건 잘 되어 있음)
2. 체결통보 수신 → `_closing_filled_by_code` 업데이트 (이것이 1차 소스)
3. **지정 시간 후 폴백 잔고조회로 검증** (체결통보 누락분 보완):
   - 15:31 → 종가매수 체결 결과 확인
   - 15:58 → 시간외 종가매매 체결 결과 확인
   - 16:11, 16:21, ..., 18:01 (10분 단위) → 시간외 단일가 체결 결과 확인
4. 폴백 잔고조회 결과를 `_closing_filled_by_code`에 병합
5. 이후 시간대 매수 함수에서는 병합된 데이터로 remain 계산

**`_get_closing_filled_for_remain()` 수정 방향**:
- "체결 기록 없으면 즉시 잔고조회" 로직 (라인 2796~2815) 제거
- 체결통보 기반 데이터만 반환하는 순수 함수로 변경
- 잔고조회는 별도 스케줄러에서 지정 시간에만 실행

**중복 매수 방지 체계 (3단계)**:
1. 1단계: 체결통보 수신 → 즉시 filled 업데이트 (실시간)
2. 2단계: 지정 시간 폴백 잔고조회 → filled 검증/보완 (스케줄)
3. 3단계: 시간외 매수 함수에서 remain 계산 시 1+2단계 병합 데이터 사용

---

## 3. 근본 원인 분석

### 3.1 문제 1: 중복 매수 — 핵심 원인

#### 원인 A: 15:30~16:00 WS 연결 부재로 체결통보 수신 불가

**코드 경로** (라인 7812~7826):
```python
# WS 연결 루프의 finally 블록
should_wait = _close_force_stopped or (dtime(15, 31) <= now_t < dtime(16, 0))
if should_wait and now_t < dtime(16, 0):
    while not _stop_event.is_set():
        if datetime.now(KST).time() >= dtime(16, 0):
            break
        time.sleep(1.0)
```

15:30 종가 체결 수신 완료 후 WS가 닫히고, 16:00까지 **재연결하지 않고 대기**한다.
이 구간에서:
- 15:30 종가매수 체결통보(H0STCNI0)가 오지 않음 → `_closing_filled_by_code`에 반영 안 됨
- 15:41 시간외종가 체결통보도 오지 않음 → 거래장부 기록 안 됨

#### 원인 B: `_run_afternoon_extra_closing_buy()`의 체결 판단 실패

**코드** (라인 2819~2873):
```python
filled_map = _get_closing_filled_for_remain(state)
# filled_map에서 remain을 계산하여 미체결분 추가 매수
```

15:30 종가매수가 **실제로는 체결**되었지만, WS가 끊겨 체결통보를 못 받았으므로:
1. `_closing_filled_by_code`가 비어있음
2. `closing_filled_{yymmdd}.json`도 비어있음
3. `_get_closing_filled_for_remain()`의 잔고조회 폴백이 작동하지만...

**잔고조회 폴백의 타이밍 문제** (라인 2797):
```python
has_any_filled = any(merged.get(c, 0) > 0 for c, _ in ordered_codes)
if not has_any_filled and ordered_codes:
    # 잔고조회 수행
```

=> 잔고조회는 체결결과 통보가 다 완료된 후 검증용으로 사용한다. 즉 15:30의 1분 후인 15:31분에 체결조회를 한다.(이때는 모든 거래 정보가 잔고조회에 다 반영되어 있음.)

#### 원인 C: `_run_afternoon_extra_closing_buy()`의 단일 계좌 사용

**코드** (라인 2849~2854):
```python
cfg = _read_cfg() or load_config(str(CONFIG_PATH))
cano = str(cfg.get("cano", "")).strip()  # main 계좌만
```

`_run_closing_buy_orders()`는 `_iter_enabled_accounts(trade_only=True)`로 다중 계좌를 사용하지만,
`_run_afternoon_extra_closing_buy()`와 `_run_overtime_buy_orders()`는 `_read_cfg().get("cano")`로 **main 계좌만** 사용한다. ConfigProxy는 main 계좌의 cano를 반환하므로 동작은 하지만, 다중 계좌 환경에서 불일치 가능성이 있다. => 이것 또한 구조는 다중 계좌를 사용가능하도록 구성 필요

#### 요약: 중복 매수 발생 메커니즘

```
15:20  종가매수 2주 주문 → 15:30 체결 (체결통보: WS 끊김으로 미수신)
       → _closing_filled_by_code = {} (빈 상태 유지)
       → closing_buy_state.json: filled_qty=0, remain_qty=2

15:40  _run_afternoon_extra_closing_buy() 호출
       → _get_closing_filled_for_remain() → filled=0 (체결통보 없음)
       → 잔고조회 폴백 → 잔고에 2주 반영되었을 수도/안 되었을 수도
       → remain=2로 판단 → ORD_DVSN=06 매수 2주 추가 주문
       → 15:41 즉시 체결 (체결통보: WS 끊김으로 미수신)

16:00  _run_overtime_buy_orders() 호출
       → _get_closing_filled_for_remain() → filled=0 (체결통보 여전히 없음)
       → remain=2로 판단 → ORD_DVSN=07 매수 2주 추가 주문
       → 16:10 체결 (이 시점에 WS 재연결됨, 체결통보 수신)
```

**핵심**: 15:30~16:00 WS 대기 구간에서 체결통보를 받지 못하면, 이미 체결된 주문을 미체결로 오판하여 중복 주문을 낸다. 잔고조회 폴백이 있지만, `has_any_filled` 조건과 타이밍 문제로 정확하지 않다.

---

### 3.2 문제 2: 시간외 단일가 낮은 체결률 [정정]

#### 이전 분석의 오류

이전 분석에서 "상한가(전일종가+30%)로 주문하므로 당일종가 ±10% 범위를 초과하여 무효 처리된다"고 했으나, 이 분석은 **잘못**되었다.

#### 실제 주문 가격과 유효 범위

시간외 단일가(ORD_DVSN=07)의 주문 가격은 `_prepare_closing_buy_orders()`에서 계산된 **`limit_up`(전일종가 기준 상한가)**을 사용한다 (라인 2935~2953). 그런데 종가매수 대상 종목은 **전일대비 +20% 이상 상승한 종목**이며, 상한가(+30%) 종목도 포함된다.

시간외 단일가의 가격 범위는 **당일 종가 ±10%**이다. 종가매수 대상이 당일 상한가(+30%)에 마감한 경우:
- 당일종가 = 전일종가 x 1.30
- 상한가(주문가) = 전일종가 x 1.30 = 당일종가
- 당일종가 x 1.10 = 전일종가 x 1.43

따라서 **상한가로 주문해도 당일종가 +10% 범위 내**에 있으므로 가격 때문에 무효가 되지 않는다. +20%~+29% 상승 종목의 경우에도 상한가(+30%) 주문가격은 당일종가 대비 약 +1~8% 수준이므로 ±10% 범위 내이다.

#### 128건 중 3건만 체결된 실제 원인

2026-03-20 로그 기준으로 실제 데이터를 확인하면:
- 9종목 종가매수 주문 → 15:30에 오르비텍(046120) 1종목만 체결통보 수신
- 15:40 시간외종가 8종목 추가 주문 → 15:55 기준 매수수량=0 (체결통보 미수신, WS 없음)
- 16:00 시간외단일가 8종목 주문 → 16:10에 더코디(224060) 1종목만 체결
- 16:20~17:50 나머지 7종목 계속 미체결

**미체결 7종목의 실제 원인**: 가격 범위 문제가 아니라 **매도 물량 부족**이다.
- 시간외 단일가 시장은 거래량이 극히 적음
- 대상 종목이 소형주(SK증권우, 에쎈테크, 에이프로젠바이오로직스 등) 위주
- 상한가/급등 종목은 투자자들이 보유를 유지하려 하므로 매도 물량이 거의 없음
- 10분 단위 일괄 매칭에서 매도 호가가 없으면 체결 자체가 불가
- 128건(과거 누적) 중 3건만 체결된 것은 시간외 단일가의 구조적 한계

**결론**: 시간외 단일가 주문 가격은 문제가 아니다. 근본적으로 급등/상한가 종목을 시간외 단일가에서 매수하는 것 자체가 체결률이 낮을 수밖에 없다. 시간외 단일가 매수는 보조 수단일 뿐, 핵심은 15:30 종가매수와 15:40 시간외종가에서 체결을 확보하는 것이다.

---

### 3.3 문제 3: 예상체결가/체결가 전환 로직

#### 현재 구현 (라인 3288~3326, `calc_mode()`)

```python
def calc_mode(now):
    if t < dtime(8, 50):       return PREOPEN_WAIT
    if t < dtime(9, 0):        return PREOPEN_EXP      # 예상체결가
    if t < dtime(15, 20):      return REGULAR_REAL      # 실시간 체결가
    if t < dtime(15, 30):      return CLOSE_EXP         # 종가 예상체결가
    if t < dtime(16, 0):       return CLOSE_REAL         # 종가 체결가
    if t < END_TIME:           return OVERTIME_EXP       # 시간외 예상체결가
                                 (또는 OVERTIME_REAL if _overtime_real_active)
```

#### WS 연결 상태와 실제 동작

| 시간대 | 모드 | WS 상태 | 비고 |
|--------|------|---------|------|
| 08:50~09:00 | PREOPEN_EXP | 연결됨 | 예상체결가 구독 |
| 09:00~15:20 | REGULAR_REAL | 연결됨 | 실시간 체결가 구독 |
| 15:20~15:30 | CLOSE_EXP | 연결됨 (15:20에 재연결) | 예상체결가 구독 |
| 15:30~15:31 | CLOSE_REAL | 연결됨 → **종가 체결 수신 후 WS 닫기** | |
| **15:31~16:00** | **CLOSE_REAL** | **WS 없음 (대기 상태)** | **체결통보 수신 불가** |
| 16:00~18:00 | OVERTIME_EXP | 연결됨 (16:00에 재연결) | 예상체결가 구독 |

#### 사용자 의견과 비교

사용자 의견:
- 15:21~15:30: 예상체결가 유지 후 종료 → **현재와 일치** (CLOSE_EXP)
- 15:40~16:00: 시간외 종가매매 - 예상체결가 구독 불필요 → **현재: WS 자체가 없으므로 구독 없음 (일치)**
- 16:00~18:00: 16:01에 예상체결가 구독 시작, 18:00까지 유지 → **현재와 일치** (OVERTIME_EXP)

현재 전환 로직 자체는 사용자 의견과 대체로 일치한다. 문제는 전환 로직이 아니라 **15:31~16:00 WS 부재 구간에서 체결통보를 못 받는 것**이다.

---

## 4. 예상체결가/체결가 전환 로직 검토

### 현재 구현의 핵심 이슈

1. **15:30~16:00 WS 대기**: `_close_force_stopped` 후 16:00까지 WS 재연결 없이 대기 (라인 7812~7826). 이 구간에서:
   - 15:30 종가 체결통보 미수신 가능
   - 15:40 시간외종가 매수 후 체결통보 미수신
   - `_buy_order_cash()` 내의 `_ccnl_notice_sub_add()` 호출도 WS가 없으므로 실패

2. **시간외 단일가 예상체결가 → 체결가 전환**: 10분마다 체결 시점에 `_overtime_real_active = True`로 전환 후, 전 종목 수신 완료 시 다시 예상체결가로 복귀하는 로직이 있다 (라인 3275~3278). 이것은 정상적인 구현이나 복잡도가 높다.

### 사용자 의견대로 변경하는 것이 맞는지

사용자 의견과 현재 구현은 큰 틀에서 일치하므로, 전환 로직 자체의 변경보다는 **15:31~16:00 WS 부재 문제** 해결이 우선이다.

---

## 5. 개선 방안

### 5.1 [최우선] 중복 매수 방지

#### 방안 A: 시간외 매수 전 REST API 잔고조회 강제 실행

`_run_afternoon_extra_closing_buy()`와 `_run_overtime_buy_orders()` 시작 시 `_get_closing_filled_for_remain()` 의존 대신, 직접 REST API 잔고조회를 수행하여 실제 보유수량 확인.

**수정 포인트**:
- `_run_afternoon_extra_closing_buy()` (라인 2829): `filled_map` 계산 전에 잔고조회 결과를 `_closing_filled_by_code`에 반영
- `_run_overtime_buy_orders()` (라인 2894): 동일

```python
# 예시: _run_afternoon_extra_closing_buy() 시작부
try:
    live_balance = _get_balance_holdings()  # REST API 잔고조회
    for o in state["orders"]:
        code = str(o.get("code", "")).zfill(6)
        held = live_balance.get(code, 0)
        _closing_filled_by_code[code] = max(_closing_filled_by_code.get(code, 0), held)
    _save_closing_filled()
except Exception:
    pass
```

#### 방안 B: `_get_closing_filled_for_remain()` 잔고조회 조건 완화

현재는 `has_any_filled`가 False일 때만 잔고조회를 수행한다 (라인 2797). 이 조건을 제거하고 항상 잔고조회를 수행하도록 변경.  => 항상이라는게 무슨 항상을 얘기하는거야? 주문시점을 얘기하는거니? 왜 이렇게 애매한 문구를 사용하지? 잔고조회는 지정된 시간에만 추가로 하도록 해줘..  (15:31, 15:58, 16:11 ~ 18:01까지 10분단위), 그리고 장 중에는 매수 및 매도 주문을 하고 체결이 완료되기 전까지는 1분 마다 한번씩 잔고조회 하도록 개선해줘..

**수정 포인트**: 라인 2796~2797
```python
# 변경 전
has_any_filled = any(merged.get(c, 0) > 0 for c, _ in ordered_codes)
if not has_any_filled and ordered_codes:

# 변경 후 — 항상 잔고조회
if ordered_codes:
```

#### 방안 C: 15:31~16:00 WS 유지 (근본 해결)

15:30 종가 체결 수신 완료 후에도 WS를 유지하여 체결통보 수신을 계속한다. 종가 체결가/예상체결가 구독은 해제하되, H0STCNI0 구독은 유지.

**수정 포인트**: 라인 7812~7826의 대기 로직 제거 또는 WS 유지 모드 추가

이 방안은 변경 범위가 크므로, 방안 A를 먼저 적용하는 것이 실용적이다.  => 이 방안은 실시간 체결가 수신에 대해서만 별도의 웹소켓으로 계속 실행되도록 하면 처리가 가능한지 검토해줘..  그렇게 별도의 웹소켓으로 분리가 어려운 경우에는 15:31~16:00까지의 구간까지는 16:40 매수 요청 전인 16:39분에 실시간 체결통보를 구둑 신청하면 되지 않겠니?


### 5.3 다중 계좌 일관성

`_run_afternoon_extra_closing_buy()`와 `_run_overtime_buy_orders()`에서 `_read_cfg().get("cano")` 대신 `_iter_enabled_accounts(trade_only=True)` 사용.

**수정 포인트**:
- `_run_afternoon_extra_closing_buy()` 라인 2849~2854
- `_run_overtime_buy_orders()` 라인 2920~2926

---

## 6. 결론

### 중복 매수의 근본 원인

**15:30 종가 체결 수신 후 WS를 닫고 16:00까지 재연결하지 않는 구조**가 모든 문제의 시작점이다.

1. WS 부재 → 15:30 종가매수 체결통보 미수신 → `_closing_filled_by_code` 미갱신
2. 15:40 시간외종가 매수 시 `remain`을 잘못 계산 → 이미 체결된 수량을 미체결로 판단 → 중복 주문
3. 16:00 시간외단일가 매수 시에도 동일한 문제 반복 → 3중 매수

### 우선순위별 수정 권장

| 순위 | 수정 내용 | 영향도 | 난이도 |
|------|----------|--------|--------|
| 1 | 시간외 매수 전 REST 잔고조회 강제 (방안 A) | 중복매수 방지 | 낮음 | => 이건 생략하도록 해줘.. 아래 7.1 잔고조회 개선시 해결되므로 이 건은 적용 제외
| 2 | `_get_closing_filled_for_remain()` 잔고조회 항상 수행 (방안 B) | 중복매수 방지 보완 | 낮음 | => 이것도 제외.. 아래 7.1 방법 조회시 해결됨.
| 3 | 시간외 매수 함수 다중계좌 지원 | 일관성 | 중간 |
| 4 | 15:31~16:00 WS 유지 (방안 C) | 근본 해결 | 높음 | => 이것도 15:40 매수요청 직전(15:39)에 실시간 체결통보를 구독하면 쉽게 해결됨..

---

## 7. 추가 분석 (2026-03-22)

### 7.1 잔고조회 기반 폴백 검증 설계

#### 현재 잔고조회 시점

현재 코드에서 잔고조회는 다음 시점에 수행된다:
1. **08:25 시작 시**: `_print_startup_balance()` — 보유종목 출력 + 매도 상태 초기화
2. **08:58**: `_run_balance_0858()` — 장 시작 전 1회 잔고 확인
3. **15:40 시간외종가 매수 전**: `_get_closing_filled_for_remain()` 내부에서 체결통보 미반영 시 폴백으로 1회 호출
4. **16:00 시간외단일가 매수 전**: 동일하게 `_get_closing_filled_for_remain()` 호출

#### 추가 잔고조회 설계

사용자 요청 시점과 목적:

| 시점 | 목적 | 설계 |
|------|------|------|
| **15:31** | 종가 체결 결과 확인 | 15:30 동시호가 체결 후 잔고 반영 확인. `_closing_filled_by_code`에 반영하여 15:40 중복매수 방지 |
| **15:58** | 시간외 종가매매 체결 결과 확인 | 15:40~15:58 사이 시간외종가 체결 여부 확인. 잔고에 반영되었으면 `_closing_filled_by_code` 갱신 |
| **16:11, 16:21, ..., 18:01** | 시간외 단일가 10분 단위 체결 확인 | 10분 단위 일괄 매칭 후 체결 결과 확인. 로그/텔레그램 통보 |

#### 구체적 구현 방안

`scheduler_loop`의 시간 체크 블록에 추가:

```python
# 15:31 — 종가 체결 결과 검증
if dtime(15, 31, 0) <= now.time() <= dtime(15, 31, 5):
    if not _closing_balance_1531_done:
        _closing_balance_1531_done = True
        _run_closing_balance_verification("15:31_종가체결")

# 15:58 — 시간외 종가 체결 결과 검증
if dtime(15, 58, 0) <= now.time() <= dtime(15, 58, 5):
    if not _closing_balance_1558_done:
        _closing_balance_1558_done = True
        _run_closing_balance_verification("15:58_시간외종가")

# 16:11, 16:21, ..., 18:01 — 시간외 단일가 체결 결과 (10분 단위)
if dtime(16, 11) <= now.time() < END_TIME:
    minute = now.minute
    if minute % 10 == 1 and now.second <= 5:
        slot_key = f"{now.hour:02d}:{minute:02d}"
        if slot_key not in _overtime_balance_checked:
            _overtime_balance_checked.add(slot_key)
            _run_closing_balance_verification(f"{slot_key}_시간외단일가")
```

`_run_closing_balance_verification()` 함수:
```python
def _run_closing_balance_verification(label: str) -> None:
    """REST API 잔고조회로 체결 결과 검증 + _closing_filled_by_code 갱신."""
    state = _load_closing_buy_state()
    if not state or not state.get("orders"):
        return
    try:
        cfg = _read_cfg() or load_config(str(CONFIG_PATH))
        cano = str(cfg.get("cano", "")).strip()
        acnt = str(cfg.get("acnt_prdt_cd", "01")).strip() or "01"
        client = _top_client or _init_top_client()
        out1, _, _, _ = _get_balance_page(client, cano, acnt, "TTTC8434R")
        rows = out1 if isinstance(out1, list) else ([out1] if isinstance(out1, dict) else [])
        hold_map = {}
        for row in rows:
            c = str(row.get("pdno", "")).strip().zfill(6)
            if c:
                hold_map[c] = int(float(str(row.get("hldg_qty", 0) or 0).replace(",", "") or 0))

        changes = []
        for o in state["orders"]:
            code = str(o.get("code", "")).zfill(6)
            order_qty = int(o.get("order_qty", o.get("qty", 0)))
            old_filled = _closing_filled_by_code.get(code, 0)
            held = hold_map.get(code, 0)
            new_filled = min(order_qty, held)
            if new_filled > old_filled:
                _closing_filled_by_code[code] = new_filled
                name = o.get("name", code_name_map.get(code, code))
                changes.append(f"{name}({code}) {old_filled}→{new_filled}주")
        _save_closing_filled()

        if changes:
            msg = f"{ts_prefix()} [{label}] 잔고검증 체결 반영: {', '.join(changes)}"
            _notify(msg, tele=True)
        else:
            _notify(f"{ts_prefix()} [{label}] 잔고검증 완료 — 변동 없음")
    except Exception as e:
        logger.warning(f"{ts_prefix()} [{label}] 잔고검증 실패: {e}")
```

**핵심 포인트**:
- 15:31 잔고조회는 **중복매수 방지의 핵심 방어선**이다. 이 시점에서 `_closing_filled_by_code`를 정확히 갱신하면 15:40 시간외종가에서 중복 주문을 하지 않게 된다.
- 잔고조회 결과 텔레그램 통보 조건:
  - 체결통보가 온 경우 → 즉시 텔레그램 통보
  - 체결통보가 안 왔는데 잔고조회에서 체결 확인됨 → 체결 처리 + 텔레그램 통보
  - 잔고조회 했는데 변화 없으면 → **통보하지 않음**
- **`_get_closing_filled_for_remain()` 내부의 즉시 잔고조회 폴백(라인 2796~2815)은 제거해야 한다** — 이것이 중복매수의 직접적 원인 (아래 섹션 7.6 참조). 잔고조회는 위 스케줄러에서만 지정 시간에 실행하여 `_closing_filled_by_code`를 갱신하고, 이 함수는 `_closing_filled_by_code`를 읽기만 하는 순수 함수로 변경

---

### 7.2 체결통보 미수신 원인 — WS 종료 경로 정밀 분석 (2026-03-22 갱신)

#### 1. WS 연결 구조: 단일 연결, 체결가+체결통보 공유

WS 연결은 **1개**다. `run_ws_forever()` (라인 7680~7831)에서 단일 `KISWebSocket` 객체(`kws`)를 생성하고, 그 위에 체결가(H0STCNT0/H0STANC0), 예상체결가, 장운영정보(H0STMKO0), **체결통보(H0STCNI0)** 모두를 구독한다.

```
[단일 WS 연결]
  ├── H0STCNI0  (체결통보 — htsid 단위, 계좌의 모든 주문 체결 수신)
  ├── H0STCNT0  (실시간 체결가 — 종목별)
  ├── H0STANC0  (예상 체결가 — 종목별)
  ├── H0STMKO0  (장운영정보 — 종목별)
  └── overtime_* (시간외 체결가/예상체결가)
```

**따라서 WS 연결 자체가 닫히면 체결통보를 포함한 모든 구독이 중단된다.**

#### 2. 15:30:33 WS 종료의 정확한 코드 경로

로그 `wss_TR_260320.log` 라인 38812~38814:
```
15:30:32  [구독 해제 결과] 15종목 전부 해제
15:30:32  15:30 종가 체결 수신 완료 (15/15종목), 전체 구독을 중지하고 16:00까지 대기모드로 진입
15:30:33  [ws] kws.start returned (mode=CLOSE_EXP)
15:30:33  [ws] 종가 체결 수신 완료, 16:00 시간외까지 대기
```

**코드 경로 (scheduler_loop → WS 종료):**

1. `scheduler_loop()` 0.5초 루프에서 `_desired_subscription_map(now)` 호출 (라인 3933)
2. `_desired_subscription_map()` 라인 7664~7670:
   ```python
   if dtime(15, 30) <= t < dtime(16, 0):
       if len(_regular_real_seen) >= len(codes):  # 전 종목 종가 체결 수신 완료
           return {}  # ← 빈 dict 반환
       if t >= dtime(15, 31):  # 타임아웃
           return {}
       return {ccnl_krx: all_codes}  # 15:30~15:31 수신 대기
   ```
3. `_apply_subscriptions()` 호출로 모든 체결가 구독 해제 (desired가 빈 dict이므로)
4. 라인 3935~3948: `desired`가 빈 dict이고 `_regular_real_seen >= codes` 조건 충족:
   ```python
   if (new_mode == RunMode.CLOSE_REAL
           and not desired
           and len(_regular_real_seen) >= len(codes)):
       _request_ws_close(_active_kws)   # ← WS 닫기 (라인 3947)
       _active_kws = None               # ← 라인 3948
   ```
5. `_request_ws_close()` (라인 7501~7518): `kws.close()` 또는 `kws.ws.close()` 호출
6. `run_ws_forever()`의 `kws.start(on_result=on_result)` (라인 7789)가 반환됨 (라인 7791)
7. 라인 7812~7826: `_close_force_stopped=True`이므로 16:00까지 `time.sleep(1.0)` 루프 진입

**핵심: `_request_ws_close()`는 체결가 구독뿐 아니라 WS 연결 자체를 닫는다.**
`_apply_subscriptions()`에서 체결가 구독만 해제하고 끝나는 게 아니라,
그 다음 `_request_ws_close()` 호출로 **WS TCP 연결 자체가 종료**된다.

H0STCNI0 체결통보 구독의 해제 여부와 무관하게, WS가 닫히면 수신이 불가능하다.

#### 3. 체결통보 구독 라이프사이클

| 시점 | 이벤트 | H0STCNI0 상태 |
|------|--------|---------------|
| WS 연결 시 | `run_ws_forever()` 라인 7709~7711 | **구독 시작** (htsid='sywems12') |
| 주문 성공 시 | `_ccnl_notice_sub_add()` 라인 2097 | 이미 구독 중이면 종목 추적만 추가 |
| 전량 체결 시 | `_ccnl_notice_sub_remove()` 라인 2117 | **구독 해제** (htsid 단위이므로 모든 체결통보 중단!) |
| 15:30:33 | `_request_ws_close()` | WS 닫힘 → 구독 무관하게 수신 불가 |
| 16:00:00 | `run_ws_forever()` 재연결 attempt=3 | **구독 재시작** |

**위험: `_ccnl_notice_sub_remove()`의 설계 결함**

라인 2117~2134에서 전량 체결 종목에 대해 `_send_subscribe(..., "2")` (해제)를 보내는데,
H0STCNI0은 htsid 단위 구독이므로 **1종목이 전량 체결되어 해제하면 다른 종목의 체결통보도 중단**될 수 있다.

실제 260320 로그에서 15:30:17에 오르비텍 전량 체결 → H0STCNI0 구독 해제가 발생했다:
```
15:30:17  [체결통보] H0STCNI0 구독 해제: 오르비텍(046120), tr_key=sywems12
```
이 시점 이후 다른 종목의 체결통보는 **KIS 서버에서 더 이상 발송하지 않았을 가능성**이 있다.
(다만 나머지 8종목이 실제 미체결이었을 수도 있어 확정 불가)

#### 4. 15:30~16:00 체결통보 수신 불가 구간 정리

```
15:30:00~15:30:17  WS 연결 O, H0STCNI0 구독 O → 체결통보 수신 가능
15:30:17~15:30:33  WS 연결 O, H0STCNI0 구독 해제됨 → 체결통보 수신 불가 (!!!)
15:30:33~16:00:00  WS 연결 X → 체결통보 수신 불가
16:00:00~          WS 재연결, H0STCNI0 재구독 → 체결통보 수신 가능
```

**결론: 두 가지 독립적인 문제가 존재한다:**
1. **H0STCNI0 조기 해제**: 1종목 전량체결 시 htsid 단위 해제 → 나머지 종목 체결통보 중단
2. **WS 연결 종료**: 체결가 수신 완료 → WS 닫기 → 15:40 시간외종가 주문 체결통보 수신 불가

#### 5. 대책 설계

##### 대책 A: H0STCNI0 구독 해제 금지 (즉시 적용 가능, 핵심 수정)

**문제**: `_ccnl_notice_sub_remove()` (라인 2117~2134)가 전량 체결 시 htsid 단위로 해제
**수정**: 종목별 추적만 제거하고, 실제 WSS 해제 메시지는 보내지 않음

수정 포인트 — 라인 2117~2134 (`_ccnl_notice_sub_remove`):
```python
def _ccnl_notice_sub_remove(code: str) -> None:
    """전량 체결 시 종목 추적만 제거. WSS 해제는 보내지 않음 (htsid 단위이므로 타 종목 영향)."""
    _ccnl_notice_sub_codes.discard(code)
    _ccnl_notice_order_ts.pop(code, None)
    name = code_name_map.get(code, code)
    logger.info(f"{ts_prefix()} [체결통보] 종목 추적 해제: {name}({code}), WSS 구독은 유지")
```

H0STCNI0은 슬롯 1개만 차지하므로 해제하지 않아도 구독 상한(40)에 영향 없다.
WS 연결이 끊어질 때 서버가 자동으로 정리하므로 명시적 해제가 불필요하다.

##### 대책 B: 15:30~16:00 WS 연결 유지 (체결통보 수신 보장)

**문제**: 라인 3947에서 `_request_ws_close()` 호출 → WS 닫힘 → 15:40 주문 체결통보 수신 불가
**수정**: WS를 닫지 않고 체결가 구독만 해제한 상태로 유지

수정 포인트 — 라인 3935~3962 (scheduler_loop 내):
```python
# 수정 전:
if (new_mode == RunMode.CLOSE_REAL
        and not desired
        and len(_regular_real_seen) >= len(codes)):
    _request_ws_close(_active_kws)      # WS 닫기
    _active_kws = None

# 수정 후:
if (new_mode == RunMode.CLOSE_REAL
        and not desired
        and len(_regular_real_seen) >= len(codes)):
    if not _close_force_stopped:
        _close_force_stopped = True
        _notify(
            f"{ts_prefix()} 15:30 종가 체결 수신 완료 "
            f"({len(_regular_real_seen)}/{len(codes)}종목), "
            f"체결가 구독 해제, WS 연결은 유지 (체결통보 수신 지속)",
            tele=True,
        )
    # WS를 닫지 않음 — 체결가 구독은 이미 _apply_subscriptions(desired={})에서 해제됨
    # H0STCNI0 구독만 살아 있어 15:40 시간외종가 체결통보 수신 가능
```

동시에 `run_ws_forever()` 라인 7812~7826의 대기 로직도 수정:
- WS가 닫히지 않으므로 `kws.start()`가 반환되지 않음
- 대신 scheduler_loop에서 16:00 도달 시 시간외 구독을 `_apply_subscriptions()`로 추가

수정 포인트 — `_desired_subscription_map()` 라인 7664~7672:
```python
if dtime(15, 30) <= t < dtime(16, 0):
    if _close_force_stopped:
        return {}  # 체결가 구독 없이 WS만 유지 (H0STCNI0은 별도)
    if len(_regular_real_seen) >= len(codes):
        return {}
    if t >= dtime(15, 31):
        return {}
    return {ccnl_krx: all_codes}
```

##### 대책 C: 잔고조회 폴백 강화 (보완적)

기존 7.1절의 설계 그대로. WS 체결통보와 무관하게 15:31, 15:55 등에 REST 잔고조회로 체결 확인.
대책 A/B와 함께 적용하면 이중 안전장치가 된다.

##### 우선순위

| 순서 | 대책 | 효과 | 난이도 |
|------|------|------|--------|
| 1 | **A: H0STCNI0 해제 금지** | 1종목 체결 후 나머지 체결통보 중단 방지 | 매우 낮음 (5줄 수정) |
| 2 | **B: 15:30~16:00 WS 유지** | 시간외종가 주문 체결통보 실시간 수신 | 중간 (3곳 수정) |
| 3 | **C: 잔고조회 폴백** | WS 장애 시에도 체결 확인 보장 | 중간 (신규 함수) |

---

### 7.3 거래장부 저장 방식 점검

#### 현재 거래장부 구조

거래장부는 **체결 즉시 CSV 파일에 append**된다:

1. **당일 CSV** (체결 즉시 저장):
   - 경로: `data/ledger/trade_ledger_{yymmdd}.csv`
   - 함수: `_append_ledger()` (라인 592~631)
   - 스레드 안전: `_ledger_lock` 사용
   - 호출 시점: 매수주문 시(buy_order), 체결통보 시(buy_fill/sell_fill), 취소 시(buy_cancel) 등
   - **즉시 파일에 기록** (메모리 버퍼 없음, `open("a")` 모드로 append)

2. **연도별 Parquet** (18:00 종료 시 1회 갱신):
   - 경로: `data/ledger/trade_ledger_{yyyy}.parquet`
   - 함수: `_append_daily_ledger_to_yearly()` (라인 634~660)
   - 호출 시점: `RunMode.EXIT` 전환 시 (18:00 이후, 라인 3845~3846)
   - 당일 CSV를 읽어 연도별 parquet에 append + 중복 제거

3. **연도별 Parquet → CSV 변환** (종료 직전):
   - 라인 7944~7949에서 연도별 parquet을 CSV로 변환 저장
   - 엑셀 등에서 열어볼 수 있도록 하는 편의 기능

#### 결론

- 거래 기록은 **체결 즉시 CSV에 저장**됨 (메모리에만 있다가 나중에 저장하는 것이 아님)
- Parquet 변환은 18:00 이후 종료 전에 수행하는 것이 맞음
- 다만 `_append_ledger()`는 체결통보(`_on_ccnl_notice_filled`)에서 호출되므로, **15:31~16:00 WS 부재 구간에서 체결된 주문은 거래장부에 기록되지 않는 문제**가 있다
- 이는 섹션 7.1의 잔고조회 폴백으로 보완 가능: 잔고조회에서 체결 확인 시 거래장부에도 기록하도록 설계

---

### 7.4 종가매수 대상 선정 로직 분석 및 상한가 제한 설계

#### 현재 종가매수 대상 선정 조건

`_top_rank_loop()` 내부 (라인 5807~5843)에서 15:19에 종가매수 대상을 선정한다:

```python
# 조건:
# 1. prdy_vrss_sign이 "1"(상한가) 또는 "2"(상승)
# 2. prdy_ctrt >= CLOSING_TARGET_PDY_CTRT * 100 (현재 20.0%)
# 3. stck_prpr / stck_hgpr >= 0.97 (현재가가 고가 대비 3% 이내)
```

즉 현재는 **전일대비 20% 이상 상승 + 현재가가 당일고가 근접** 조건이다. 상한가(+30%) 종목뿐 아니라 +20~29% 상승 종목도 포함된다.

실제 로그 (2026-03-20):
- 9건 종가매수 주문 중 상한가가 아닌 종목이 포함 (SK증권우, 한신공영, 에쎈테크 등 소형주)
- 15:30 종가 체결에서 오르비텍만 1주 체결 → 나머지 8종목 미체결

#### 상한가 종목 제한 설계

사용자 요청: 종가매수 대상을 상한가 달성 종목만으로 제한

**판단 기준**: `prdy_vrss_sign == "1"` (상한가) 또는 `prdy_ctrt >= 29.5` (상한가 근접)

현재 코드에서 이미 상한가 여부 표시가 있다 (라인 5870):
```python
is_limit = "▲상한가" if prdy >= 29.5 else "▲"
```

**구현 방안 1: 설정 기반 필터링**

```python
# config.json에 추가
"closing_buy_limit_up_only": true

# _top_rank_loop() 내부 필터 추가 (라인 5825~5832 사이)
if cfg.get("closing_buy_limit_up_only", False):
    if prdy_sign != "1" and prdy_ctrt < 29.5:
        continue  # 상한가 아닌 종목 스킵
```

**구현 방안 2: CLOSING_TARGET_PDY_CTRT 조정**

더 간단하게, `CLOSING_TARGET_PDY_CTRT`를 0.295로 변경하면 29.5% 이상만 선정된다:

```python
CLOSING_TARGET_PDY_CTRT = 0.295  # 기존 0.20 → 0.295 (상한가 근접만)
```

그러나 이 방법은 +29.5% 미만인 실제 상한가(호가단위에 따라 정확히 30%가 아닐 수 있음)를 놓칠 수 있다.

**권장: 방안 1** — `prdy_vrss_sign == "1"` (KIS API에서 상한가 표시)을 기준으로 필터링하는 것이 가장 정확하다. `prdy_vrss_sign` 값:
- "1": 상한
- "2": 상승
- "3": 보합
- "4": 하한
- "5": 하락

config에 `closing_buy_limit_up_only` 옵션을 추가하여 필요 시 on/off 가능하도록 한다.

**추가 고려사항**: 상한가 종목만 대상으로 하면 15:19 시점에 대상 종목이 0개일 수 있다. 이 경우 종가매수를 스킵하고 텔레그램 알림을 보내야 한다 (현재 코드에서도 `_closing_codes`가 비어있으면 매수를 스킵하므로 별도 처리 불필요).

---

### 7.5 체결통보 미수신 원인 종합 결론 (2026-03-22 갱신)

2026-03-20 로그와 코드 정밀 분석 결과, **두 가지 독립적인 구조적 문제**가 확인됨:

#### 문제 1: H0STCNI0 조기 해제 (htsid 단위 구독/해제의 함정)

- H0STCNI0 체결통보는 **종목코드가 아닌 htsid 단위** 구독이다
- 코드에서 종목별로 `_ccnl_notice_sub_remove()`를 호출하지만, 실제로는 htsid 단위로 해제 메시지를 보냄
- 15:30:17에 오르비텍 1종목 전량 체결 → `_ccnl_notice_sub_remove()` → htsid 해제 → **나머지 종목의 체결통보도 중단**
- 이것이 나머지 8종목의 체결통보가 오지 않은 원인일 수 있음 (미체결이었을 가능성도 있지만 검증 불가)

#### 문제 2: 15:30~16:00 WS 연결 공백

- 체결가 수신 완료 → `_request_ws_close()` → **WS TCP 연결 자체 종료** (라인 3947)
- 체결가 구독 해제와 WS 종료가 분리되지 않고, WS를 통째로 닫아버림
- 15:40 시간외종가 주문의 체결통보를 받을 수 없는 구조
- `_ccnl_notice_sub_add()` 호출해도 `_active_kws = None`이므로 구독 자체가 불가

#### 타임라인 (수정)

```
15:30:00~15:30:17  WS O, H0STCNI0 O  → 오르비텍 체결통보 수신
15:30:17           오르비텍 전량 체결 → H0STCNI0 htsid 해제 (!!)
15:30:17~15:30:33  WS O, H0STCNI0 X  → 나머지 종목 체결통보 수신 불가
15:30:33~16:00:00  WS X               → 모든 수신 불가 (시간외종가 포함)
16:00:00~          WS 재연결, H0STCNI0 재구독 → 정상
```

#### 대책 요약

1. **H0STCNI0 해제 금지** (대책 A): `_ccnl_notice_sub_remove()`에서 실제 WSS 해제 메시지를 보내지 않음. 5줄 수정으로 즉시 적용 가능.
2. **15:30~16:00 WS 유지** (대책 B): `_request_ws_close()` 호출 제거, 체결가 구독만 해제하고 WS 연결은 유지.
3. **잔고조회 폴백** (대책 C): WS 장애 시 REST 잔고조회로 체결 확인 (기존 7.1절).

상세 설계는 7.2절 대책 설계 참조.

---

### 7.6 `_get_closing_filled_for_remain()` 근본 결함 상세 분석

#### 현재 동작 (잘못된 구조)

1. `_run_afternoon_extra_closing_buy()` (15:40)과 `_run_overtime_buy_orders()` (16:00)에서 호출
2. 이 함수들은 각각 done 플래그(`_afternoon_extra_closing_done`, `_overtime_order_done`)로 **1회만** 실행됨
3. 함수 내에서 `_closing_filled_by_code`에 체결 기록이 하나도 없으면 **즉시** REST API 잔고조회 (라인 2797~2813)
4. 주문 직후에는 아직 체결이 안 됐으므로 잔고도 변화 없음 → "미매수"로 판단 → 중복 주문

#### 왜 이게 중복매수의 직접적 원인인가

- 주문을 넣자마자 체결되는 게 아님. 체결은 시간이 걸림
- 체결통보(H0STCNI0)가 와야 `_closing_filled_by_code`가 업데이트됨
- 그런데 WS가 닫혀서(7.2절 참조) 체결통보를 못 받음 → filled가 비어있음
- 비어있으니 즉시 잔고조회 → 아직 체결 안 됐으니 잔고도 변화 없음
- 결론: "미매수"로 판단 → 15:40, 16:00에서 중복 주문 발생

#### 수정 방향

- **라인 2796~2815의 "체결 기록 없으면 즉시 잔고조회" 로직 제거**
- 이 함수는 `_closing_filled_by_code`를 읽기만 하는 순수 함수로 변경
- 잔고조회는 7.1절의 스케줄러(15:31, 15:58, 16:11~18:01)에서만 실행
- 스케줄러가 `_closing_filled_by_code`를 미리 갱신해두면, 이 함수는 그 데이터를 그대로 반환

#### 올바른 체결 추적 흐름 (3단계)

```
1단계: 체결통보 수신 → _closing_filled_by_code 즉시 업데이트 (실시간)
2단계: 지정 시간 잔고조회 → 체결통보 누락분 확인 → _closing_filled_by_code 보완 (스케줄)
3단계: 시간외 매수 함수에서 _closing_filled_by_code 읽기 → remain 계산
```

---

### 7.7 `_closing_filled_by_code` 변수명 및 아키텍처 문제

#### 변수명 문제

- `closing` → "종가매수"를 의미하지만, 시간외 종가(15:40), 시간외 단일가(16:00~18:00)에서도 사용
- `by_code` → "종목코드별"이라는 의미인데, "코드 기반으로 동작한다"로 오독될 수 있음
- **변경 권장**: `_filled_qty_by_stock` 또는 `_ccnl_filled_qty` 등 범용적 이름

#### 아키텍처 문제: 파편화된 체결 추적

현재 `_on_ccnl_notice_filled()` (라인 2170)에서 체결통보 수신 시:
- 매수 체결 reason (라인 2235~2238):
  ```python
  if any(p.get("code") == code for p in _closing_buy_prepared):
      _buy_fill_reason = "종가매수_체결(15:20)"
  elif code in _vi_buy_pending:
      _buy_fill_reason = "VI매수_체결"
  ```
- **종가매수와 VI매수만 reason이 있고, 장중 일반매수 등은 빈 문자열**

이 변수와 체결 추적 로직이 **종가매수 전용으로 설계**되어 있다. 장중 매수, 프리마켓 매수 등은 별도 체계이거나 체결 추적이 통합되어 있지 않다.

#### 검토 방향: 통합 체결 추적

- 매수/매도 체결 추적은 시간대와 무관하게 하나의 통합된 로직이어야 함
- 주문 → 체결통보 수신 → 보유수량 갱신 + 현금 갱신 + 거래장부 기록 (프리마켓/장중/종가/시간외 동일)
- reason은 주문 시점에 결정하여 매개변수로 전달하면 됨:
  - 주문 함수에서 `reason="종가매수"`, `reason="VI매수"`, `reason="장중매수"` 등을 지정
  - 체결통보 처리 함수는 reason을 받아서 거래장부에 기록
  - 이렇게 하면 체결 추적 함수를 시간대별로 분리할 필요 없이, 모든 호출에서 동일한 함수를 사용 가능
- 변수명도 `_closing_filled_by_code` 대신 범용 이름으로 변경하여 혼란 제거
