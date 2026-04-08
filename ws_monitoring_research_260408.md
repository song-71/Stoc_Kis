# WS Monitoring Research 260408: 미체결취소 500에러, NXT 0종목 로그, 08:30~08:40 주문내역 표시

## Issue 1: 미체결취소 500 Server Error (07:50:09)

### 현상
```
[260408_075008_TR] [미체결취소] 매수주문 확인 중...
[260408_075009_TR] [미체결취소] 실패: 500 Server Error: Internal Server Error for url: https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl?CANO=43444822&ACNT_PRDT_CD=01&...
```
- 프로그램 재시작 시 `_run_open_buy_order_cancel_on_startup()` 에서 미체결 매수주문 조회 API 호출 중 500 에러 발생

### 원인 (ws_realtime_trading.py:735~745)

API 호출 파라미터에 빈 문자열 전달이 원인일 가능성이 높다.

```python
# 735~741행
params = {
    "CANO": cano, "ACNT_PRDT_CD": acnt,
    "INQR_DVSN": "00", "SLL_BUY_DVSN_CD": sll_buy_dvsn_cd,
    "INQR_STRT_DT": "", "INQR_END_DT": "", "PDNO": "", "ORD_GNO_BRNO": "", "ODNO": "",
    "INQR_DVSN_1": "0", "INQR_DVSN_2": "0",
    "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
}
r = requests.get(url, headers=headers, params=params, timeout=10)
r.raise_for_status()  # ← 여기서 500 발생
```

- `INQR_STRT_DT`, `INQR_END_DT`가 빈 문자열(`""`)로 전달됨
- KIS OpenAPI 공식 문서상 해당 필드는 날짜(YYYYMMDD) 형식이 필요할 수 있음
- 07:50 시점은 장 개시 전이므로 KIS 서버 측에서 빈 날짜 파라미터를 처리하지 못해 500 반환 가능
- 다만 500은 서버 측 에러이므로, KIS API 서버의 일시적 장애(장 개시 전 서버 준비 미완료)일 가능성도 있음

**추가 발견 - 파싱 버그 (ws_realtime_trading.py:815)**

```python
# 815행
targets = [o for o in orders if int(float(str(o.get("psbl_qty") or o.get("PSBL_QTY") or 0)).replace(",", "") or 0) > 0]
```

`.replace(",", "")`가 `float()` 반환값(float 타입)에 호출됨. `float` 객체에는 `.replace()` 메서드가 없으므로 `AttributeError` 발생함. 단, 이번 에러는 API 호출 자체(745행 `raise_for_status()`)에서 발생했으므로 이 파싱 코드에 도달하지 않았음. API가 정상 응답을 반환하면 이 버그가 발현된다.

올바른 코드는:
```python
int(float(str(o.get("psbl_qty") or o.get("PSBL_QTY") or 0).replace(",", "") or "0"))
```
(`str(...)` 결과에 `.replace()` 적용 후 `float()` → `int()` 순서)

### 수정 방안

1. **날짜 파라미터 채우기**: `INQR_STRT_DT`/`INQR_END_DT`에 당일 날짜(YYYYMMDD) 지정
2. **500 에러 재시도**: `raise_for_status()` 전에 500일 경우 1~2회 재시도 로직 추가
3. **파싱 버그 수정**: 815행과 823행의 `int(float(str(...)).replace(...))` → `int(float(str(...).replace(...)))` 로 괄호 순서 수정

---

## Issue 2: NXT 프리마켓 0종목 -- nxt=Y 필터에 의한 전량 탈락 (심층 분석)

### 현상
```
[260408_080000_TR] NXT 프리마켓 시작 (08:00~08:50): 0종목 모니터링 시작
```
- NXT 프리마켓 진입 시 대상 종목이 0개로 표시됨
- Select_Tr_target_list.csv에는 2026-04-07 날짜로 10종목이 존재함

### 전체 프로세스 흐름

**1단계: CSV 생성 -- Select_Tr_target_list_symulation_pdy_ctrt.py**
- `_load_morning_target_codes()` (ws_realtime_trading.py:3272행)에서 subprocess로 실행
- 이 함수는 모듈 로드 시점에 1회 호출됨 (3503행: `_morning_target_codes = _load_morning_target_codes()`)
- 실행 결과로 `/home/ubuntu/Stoc_Kis/symulation/Select_Tr_target_list.csv` 갱신

**2단계: NXT 프리마켓 종목 로드 -- _load_nxt_target_codes_pre()**
- scheduler_loop에서 07:50~08:00 사이에 1일 1회 호출 (4136~4144행)
- 이 함수는 스크립트를 재실행하지 않고 이미 존재하는 CSV만 읽음 (3343행)
- CSV에서 마지막 날짜(date 컬럼 max값)의 종목만 추출 (3348~3350행)

**3단계: nxt=Y 필터 적용 (3353~3365행)**
- KRX_code.csv에서 nxt=Y인 종목 set을 구함
- CSV 종목과 교집합 → `upper_codes`
- 보유종목(`_nxt_held_codes`)을 합집합 → 최종 대상

### 원인 (ws_realtime_trading.py:3338~3394)

**0종목의 직접 원인: nxt=Y 필터에서 10종목 전량 탈락**

```python
# ws_realtime_trading.py:3361-3365
if nxt_set:
    upper_codes = {c for c in raw_codes if c in nxt_set}  # <-- 여기서 0개
else:
    logger.warning("[nxt] KRX_code에 nxt 컬럼 없음 -> 전일 상한가 전체를 NXT 대상으로 사용")
    upper_codes = set(raw_codes)
```

실제 검증 결과 -- CSV 마지막 날짜(2026-04-07)의 10종목이 전부 KRX_code.csv에서 nxt=N:

| 종목코드 | 종목명 | nxt | group |
|----------|--------|-----|-------|
| 017900 | 광전자 | N | ST |
| 019660 | 글로본 | N | ST |
| 092600 | 앤씨앤 | N | ST |
| 109080 | 옵티시스 | N | ST |
| 121850 | 코이즈 | N | ST |
| 140430 | 카티스 | N | ST |
| 215790 | 이노인스트루먼트 | N | ST |
| 288980 | 모아데이타 | N | ST |
| 308100 | 형지글로벌 | N | ST |
| 452300 | 캡스톤파트너스 | N | ST |

**KRX_code.csv nxt 분포 통계:**
- 전체 ST그룹: 2726종목
- nxt=Y: 690종목 (25%)
- nxt=N: 2036종목 (75%)
- ST 이외 그룹(BC, DR, EF, EN 등): 전부 nxt=N

NXT 시장은 전체 상장종목의 일부만 거래 가능하므로, 전일 상한가 종목 10개가 모두 NXT 비대상(nxt=N)인 것은 충분히 발생 가능한 정상 상황이다.

**애프터마켓도 동일 구조 (3400~3442행)**
`_refresh_nxt_target_codes_after()`도 같은 nxt=Y 필터를 적용. 당일 `_last_prdy_ctrt`가 비어있으면 `_load_nxt_target_codes_pre()`로 폴백하므로 동일하게 0종목이 된다.

### 수정 방안

- nxt=Y 필터는 정상. 해당 날 NXT 거래 가능 상한가 종목이 없는 것은 자연스러운 상황
- 현재 로그에 0종목이라는 정보가 나오므로 기능상 문제 없음. => 다만, 다음과 같이 탈락사유를 구체화할 필요

**개선방안: 로그 개선 -- 탈락 사유 표시**
- `_load_nxt_target_codes_pre()` 내부에서 0종목일 때 CSV raw_codes 수와 nxt 필터 결과를 명시:
```python
# 3391행 부근, result가 비거나 upper_codes가 0일 때
if not upper_codes and raw_codes:
    _notify(f"{ts_prefix()} [nxt] 프리마켓: CSV {len(raw_codes)}종목 중 NXT 대상 없어 모니터링 제외함.", tele=True)
```

## Issue 3: 08:30~08:40 종료 후 status=ordered 종목 표시

### 현상
```
[260408_084100_TR] [08:30~08:40 종료] 주문내역·결과
  옵티시스(109080) 주문수량=1 매수수량=0 미매수=1 status=ordered
  ...
```
- 매수를 스킵했을 것 같은 종목이 status=ordered로 표시됨
- 실제로 주문이 나갔는지 불분명

### 원인 (ws_realtime_trading.py:2696~2714, 3125~3146, 4176~4182)

**status=ordered의 의미:**
- 2704행: 종가매수 주문(`_run_closing_buy_orders`)이 실행되어 KIS API에 실제 주문 요청을 보낸 종목에 `"status": "ordered"` 설정
- 2712행: 주문을 보내지 못한 종목은 `"status": "skipped"` 설정
- 즉, **status=ordered는 실제로 KIS API에 매수주문이 나간 종목**을 의미

**08:30~08:40 주문 흐름:**
1. 4171~4173행: `_run_morning_extra_closing_buy()` → 전일 미체결 종목에 장전시간외종가(ORD_DVSN=05) 추가 매수
2. 이 함수는 전일 `closing_buy_state_{yymmdd}.json`을 읽어서 미체결분에 대해 추가 주문 발송
3. 4177~4182행: 08:41에 `_log_closing_result_after_window("08:30~08:40", yesterday)` 호출

**표시 로직 (3125~3146행):**
```python
def _log_closing_result_after_window(window_label, yymmdd=None):
    # ...전일 state 로드 후 모든 orders를 순회하며 출력
    for o in orders:
        # status 필터 없이 전체 출력
        status = o.get("status", "unknown")
        lines.append(f"  {name}({code}) 주문수량={ord_q} 매수수량={filled} 미매수={remain} status={status}")
```

- **모든 orders를 필터 없이 출력**함. status=ordered든 status=skipped든 전부 나열됨
- status=ordered + 매수수량=0 + 미매수=1은 "전일에 주문은 나갔으나 체결되지 않았고, 08:30~08:40 추가매수에서도 체결되지 않은 상태"를 의미

**사용자 혼란 원인:**
- 08:30~08:40 결과 로그에 전일 주문 상태(status=ordered)가 그대로 표시됨
- 08:30~08:40에서 추가 주문이 실제로 나갔는지, 아니면 전일 상태를 그대로 보여주는 것인지 구분되지 않음

### 수정 방안

1. **status=skipped 종목 제외**: `_log_closing_result_after_window`에서 status=skipped인 종목은 출력에서 제외하거나 별도 구분
2. **08:30~08:40 자체 주문 결과 구분**: 전일 원래 주문 vs 당일 아침 추가 주문을 구분하는 필드 추가 (예: `morning_extra_ordered: true`)
3. **로그 메시지 개선**: "전일 주문 상태"임을 명시

```python
# 예시: skipped 제외 + 구분 표시
for o in orders:
    status = o.get("status", "unknown")
    if status == "skipped":
        continue  # 주문되지 않은 종목은 생략
    morning = " [아침추가]" if o.get("morning_extra") else " [전일주문]"
    lines.append(f"  {name}({code}) 주문수량={ord_q} 매수수량={filled} 미매수={remain}{morning}")
```

---

## Issue 4: 15:20/15:30 구독 해제/추가 실패 (TR_ID=unknown, WebSocket send timeout)

### 현상
```
[15:20:05] 구독 해제 실패 - TR_ID=unknown, 여러 종목, err=WebSocket send timeout (5s)
[15:20:26] 구독 해제 실패 - TR_ID=unknown, 여러 종목
[15:20:31] 구독 해제 실패 - TR_ID=unknown, 여러 종목, err=WebSocket send timeout (5s)
[15:30:05] 구독 해제 실패 - TR_ID=unknown, 여러 종목
[15:30:07] 구독 해제 실패 - TR_ID=unknown, 여러 종목
[15:30:13] 구독 추가 실패 - TR_ID=unknown, 여러 종목
[15:31:01] 구독 해제 실패 - TR_ID=unknown, 여러 종목
```
- 모두 TR_ID=unknown
- 15:20과 15:30에 집중 발생
- 구독 해제뿐 아니라 구독 추가도 실��� (항목 6)
- 일부는 WebSocket send timeout (5s) 에러, 일부는 에러 메시지 없음

### 원인 (ws_realtime_trading.py + kis_auth_llm.py)

**원인 1: TR_ID=unknown — 모든 구독 함수에서 동일하게 발생**

`_send_subscribe()` (ws_realtime_trading.py:8524~8540)에서 `_register_trid(req)` → `_extract_tr_id(req)` 호출:

```python
# ws_realtime_trading.py:8045-8056
def _extract_tr_id(req) -> str | None:
    for key in ("tr_id", "trid", "trId", "TRID", "header_tr_id"):
        if hasattr(req, key):
            v = getattr(req, key)
            if isinstance(v, str) and v:
                return v
    if isinstance(req, dict):
        for key in ("tr_id", "trid", "trId", "TRID"):
            v = req.get(key)
            if isinstance(v, str) and v:
                return v
    return None
```

`req`는 `ccnl_krx`, `exp_ccnl_krx` 등의 **일반 Python 함수** (domestic_stock_functions_ws.py:322, 617 등)이다. 함수 객체에는 `tr_id` 속성이 없으므로 `_extract_tr_id`는 항상 `None`을 반환한다. 결과적으로 `rid or 'unknown'` → `"unknown"`.

이 문제는 260402 research Issue 1에서 이미 분석된 것과 동일한 근본 원인이다. TR_ID=unknown은 기능 장애가 아닌 **로그 표시 문제**일 뿐이다.

**원인 2: WebSocket send timeout (5s) — 15:20 ���점**

15:20 트리거 경로:
1. `_top_rank_loop()` → `_switch_to_closing_codes()` (ws_realtime_trading.py:6478)
2. 6534행: `_request_ws_close(_active_kws)` ��� **WS 재연결 시작**
3. 재연결 중 `_active_kws`가 `None`이 되고 새 KWS가 생성됨 (8800~8801행)

동시에:
- `scheduler_loop()` (ws_realtime_trading.py:4452~4457)에서 매 루프마다 `_apply_subscriptions(_active_kws, desired)` 호출
- 재연결 중 _active_kws가 아직 이전(닫히는 중인) 객체를 참조하면, 닫히는 중인 WS에 send 시도 → asyncio 이벤트 루프가 close 처리 중이므로 코루틴이 스케줄되지 못함 → **5초 timeout**

15:20:05 (첫 실패)는 WS 닫기 직후, 15:20:26/15:20:31은 재연결 시도 중 구독 갱신 재시도에 해당한다.

**원인 3: 15:30 시점 실패**

15:30에 모드가 `CLOSE_EXP` → `CLOSE_REAL`로 전환된다 (ws_realtime_trading.py:3632).

`scheduler_loop()` 4452~4457행에서:
```python
with _kws_lock:
    if _active_kws is not None:
        if not (_pre_unsub_closing_done and new_mode == RunMode.REGULAR_REAL):
            desired = _desired_subscription_map(now)
            _apply_subscriptions(_active_kws, desired)
```

15:30 시점 `_desired_subscription_map()` (8658~8664행)은:
- `len(_regular_real_seen) >= len(codes)` 이면 `{}` (�� dict) 반환 → 기존 ��독 전부 해제
- 15:31 이후에도 `{}` 반환

따라서 15:30:05, 15:30:07은 종가 체결 수신 완료 후 exp_ccnl_krx 구독 해제 시도이다. 15:30:13 구독 추가 실패는 ccnl_krx 실시간 체결가 구독 전환 시도로 보인다 (15:30~15:40 구간에서 `_regular_real_seen`이 아직 부족할 때).

15:31:01은 `_apply_subscriptions`에서 `_close_force_stopped` 전에 마지막으로 해제를 시도하는 경로이다.

**timeout이 발생하는 근본 원인 (kis_auth_llm.py:907~915):**

```python
# kis_auth_llm.py:907-915
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

`scheduler_loop` (별도 스레드)에서 `send_request` 호출 → asyncio 루프에 코루틴 제출 → `fut.result(timeout=5)` 블로킹 대기. 정상적으로는 asyncio 루프가 코루틴을 실행하지만, **WS가 닫히는 중이거나 asyncio 루프가 close 핸들링으로 바쁜 경우** 5초 내에 실행되지 못해 timeout 발생.

에러 메시지가 없는 실패(15:20:26, 15:30:05, 15:30:07, 15:31:01)는 `self._ws is None` 체크에 걸려 `"WebSocket not connected"` 에러가 발생한 경우이다.

### 수��� 방안

**1. TR_ID=unknown 로그 개선 (ws_realtime_trading.py:8504~8522)**

`_register_trid`에서 `_extract_tr_id` 대신 함수명 기반으로 TR_ID를 추출:

```python
def _register_trid(req) -> str | None:
    rid = _extract_tr_id(req)
    if not rid and callable(req) and hasattr(req, '__name__'):
        NAME_TO_TRID = {
            "ccnl_krx": "H0STCNT0",
            "exp_ccnl_krx": "H0STANC0",
            "overtime_ccnl_krx": "H0STCNT0",
            "overtime_exp_ccnl_krx": "H0STANC0",
        }
        rid = NAME_TO_TRID.get(req.__name__)
    # ... 이하 기존 로��
```

**2. WS 재연결 중 구�� 시도 방지 (ws_realtime_trading.py:4452~4457)**

`_active_kws`가 닫히는 중인지 확인하는 가드 추가:

```python
with _kws_lock:
    kws = _active_kws
    if kws is not None and not getattr(kws, '_close_requested', False):
        desired = _desired_subscription_map(now)
        _apply_subscriptions(kws, desired)
```

**3. send_request에서 graceful 실패 (kis_auth_llm.py:907~915)**

WS가 닫히는 중이면 즉시 예외를 발생시켜 5초 대기를 방지:

```python
def send_request(self, request, tr_type: str, data, kwargs: dict | None = None):
    if self._ws is None or self._loop is None or self._close_requested:
        raise RuntimeError("WebSocket not connected")
    # ... 이하 동일
```

**4. 실질적 영향도 평가**

이 구독 실패들은 **기능에 영향을 주지 않는다**:
- 15:20 실패: `_switch_to_closing_codes()`가 `_request_ws_close()`로 WS 재연결을 유도하므로, 재���결 후 `open_map` 기반으로 올바른 구독이 자동 복구됨 (8704~8726행)
- 15:30 실패: 종가 체결 수신 완료 후 ��독 해제는 WS 종료와 동일한 효과이므로, 실패해도 곧바로 WS가 유지/종료됨
- 따라서 WARNING → DEBUG 레벨 변경으로 로그 노이즈를 줄이는 것이 유효한 방법

---

## Issue 2 구현 완료: NXT 대상 종목 소스 변경 (nxt=Y → ST+500원)

### 변경 내용

| 함수 | 기존 필터 | 변경 후 필터 |
|------|-----------|-------------|
| `_load_nxt_target_codes_pre()` | nxt=Y | group=ST + close >= 500원 |
| `_load_morning_target_codes()` | group=ST | group=ST + close >= 500원 |
| `_refresh_nxt_target_codes_after()` | nxt=Y | group=ST + _last_stck_prpr >= 500원 |

### 로그 개선 (단계별 건수)
모든 함수에 파이프라인 단계별 로그 추가:
```
[nxt] (1) CSV(2026-04-07) 전일 상한가: 10종목
[nxt] (2) ST 필터: 10 -> 8종목
[nxt] (3) 500원 미만 제외: XXX(000000)=300원
[nxt] (3) 500원 필터: 8 -> 7종목
[nxt] (4) 최종 프리마켓 대상: 9종목 (상한가+ST+500원=7 + 보유=2)
```

### 필터 파이프라인 (공통)
```
CSV 전일 상한가 -> ST 필터 -> 500원 이상 필터 -> + 보유종목 -> 최종 대상
```

---
