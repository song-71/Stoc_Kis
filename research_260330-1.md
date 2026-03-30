# Research 260330-1: 로그 문제 3건 + 종가매수 대상 리스트 개선

## Issue 1: 재구독 로그 부실 — 종목 정보 없음

### 현상
```
[260330_090029_TR] 10초 데이터 미수신 (연결 유지, 데이터 스트림 끊김) -> 재구독 시도
[260330_090034_TR] [ws] no data for 15s -> resubscribe
```
- "재구독 시도"만 표시, **어떤 종목을 몇 개, 어떤 구독타입으로** 재구독하는지 정보 없음

### 원인 (ws_realtime_trading.py)
- 경고 로그 (4184행): 단순 문자열만 출력
- force 재구독 (7861~7872행): `_apply_subscriptions(force=True)` 내부에서 로그 없이 UNSUBSCRIBE→SUBSCRIBE 수행
- `_desired_subscription_map()`의 결과(구독타입별 종목 set)를 로그에 반영하지 않음

### 수정 방안
- 재구독 경고 시 `desired` 맵에서 종목 수/타입 추출하여 로그
- `_apply_subscriptions(force=True)` 진입 시 구독타입별 종목명 리스트 로그 추가
```
[260330_090029_TR] 15초 데이터 미수신 → force 재구독: ccnl_krx 15종목 [동화약품, KR모터스, 경방, ...]
```

---

## Issue 2: 시작잔고 누락 보충 — 전전일 상한가 종목이 잡히는 문제

### 현상
```
[260330_084201_TR] [str1_sell] 시작잔고 누락 보충: 4종목 [엔비알모션(0004V0), 세우글로벌(013000), 한국ANKOR유전(152550), 본느(226340)]
```
- 이 4종목은 **전전일 상한가** 종목이며, 전일에는 본느가 -13% 등 급락
- 종목코드 `0004V0`도 비정상 (엔비알모션 = `000490`)

### 원인 (ws_realtime_trading.py:3863~3878)
보충 로직 트리거 조건:
```python
if (not _sell_state_supplement_done
        and _morning_extra_closing_done
        and not _str1_sell_state      # ← 시작잔고에서 로딩 실패한 경우
        and STR1_SELL_ENABLED and CLOSE_BUY):
    fresh_balance = _get_balance_holdings()  # ← 현재 잔고 재조회
```
- 프로그램 시작 시 `_print_startup_balance()` (1024행, `trade_only=True`) → 보유종목 없음 반환
- `_load_str1_sell_state_on_startup({})` → `not balance_map` → 아무것도 안 함 → `_str1_sell_state = {}`
- 08:42 보충 로직: `not _str1_sell_state` 조건 만족 → 잔고 재조회 → **다른 계좌에 있던 4종목** 발견

**근본 원인**: 현재 종가매수 대상은 전일 15:19 시점의 top30 기반 "예상 상한가 종목"인데, 이 시점의 판단이 부정확하여 실제 상한가가 아닌 종목이 매수되고 있음

---

## Issue 3: 초기 구독 리스트 로그 누락

### 현상
```
[260330_085000_TR] 08:50 예상체결가 구독 시작
```
- 어떤 종목을 구독하는지 리스트가 없음
- 이후 `[top] 구독 추가: [...]` 로그는 나오지만, **최초 구독 시점의 전체 리스트**가 빠져 있음

### 원인 (ws_realtime_trading.py:3425)
```python
_notify(f"{ts_prefix()} 08:50 예상체결가 구독 시작", tele=True)
```
- 모드 전환 로그만 출력, `codes` 리스트 내용을 포함하지 않음

### 수정 방안
- 모드 전환 시 현재 `codes` 목록과 종목명을 함께 로그
```
[260330_085000_TR] 08:50 예상체결가 구독 시작: 15종목 [동화약품, KR모터스, ...]
```

---

## Issue 4 (핵심 개선): 종가매수 대상 리스트를 당일 아침 정확한 전일 상한가로 교체

### 현재 흐름
```
전일 15:19 → top30 기반 "예상 상한가" 선정 → closing_buy_state JSON 저장
          → 당일 08:30 시간외종가 매수 시도
          → 08:50 예상체결가 구독
          → 09:00 실시간체결가 구독 (→ 15:19까지 유지)
```
**문제**: 15:19 시점의 예상이 부정확 → 실제 상한가가 아닌 종목이 매수/구독됨

### 개선 흐름 (제안)
```
당일 08:30 이전 프로그램 시작 시:
  1) Select_Tr_target_list_symulation_pdy_ctrt.py 실행 (전일까지의 정확한 OHLCV 기반)
  2) Select_Tr_target_list.csv에서 마지막 날짜 + group="ST" 종목 추출
  3) 이 종목으로 종가매수 대상 갱신
  4) 08:30 시간외종가 매수 → 08:50 예상체결가 구독 → 09:00 실시간체결가 구독
```

### 관련 파일
- `ws_realtime_trading.py` — 메인 트레이딩 로직
- `symulation/Select_Tr_target_list_symulation_pdy_ctrt.py` — 전략별 종목 선정 스크립트
  - `main()`: 독립 실행 가능, parquet → CSV 출력
  - `get_closing_buy_candidates()`: 외부 호출용 API (현재 import만 되어 있고 실제 사용 안 함)
  - 현재 설정: `strategy_name = "str3"`, `target_pdy_ctrt = 0.28` (당일수익률 28% 이상)
- `symulation/Select_Tr_target_list.csv` — 선정 결과 (date, symbol, name, group 등 23컬럼)

### Select_Tr_target_list.csv 구조
```
date,symbol,name,pdy_close,tdy_ctrt,pdy_ctrt,...
2026-03-26,013000,세우글로벌,1137.0,0.300,-0.024,...
2026-03-26,014160,대영포장,1084.0,0.300,0.014,...
```
- `group` 컬럼: "ST"(일반주식), "EF"(ETF) 등 — **ST만 대상**

### 현재 코드 구조 (ws_realtime_trading.py)
- `codes` 초기화 (3299~3316행): `_load_prev_closing_codes()` → 거래장부 parquet에서 "종가매수" 이력 로드
- `_base_codes` (3312행): top_rank에 의한 해제 방지 보호셋
- `_ensure_code_structs()` (5477~5501행): 새 종목 → `codes` 추가 + 카운터 초기화
- `_load_str1_sell_state_on_startup()` (1285행): 전일 매수 종목 매도상태 복원
- `_run_morning_extra_closing_buy()` (2694행): 08:30 시간외종가 매수 (전일 JSON 기반)
- 15:19 선정 (6030~6070행): top50에서 인라인 필터링 — `get_closing_buy_candidates()` 미사용

### 결정 사항
1. **매수 + 구독 모두 교체** — 08:30 매수 대상과 08:50~/09:00~ 구독 대상 모두 CSV 기반으로 통일
2. CSV에 group 컬럼 없음 → **읽을 때 KRX_code.csv 직접 로드하여 ST 필터링**
3. 최초 시작 시 CSV 갱신 실행 → `wss_subscribe_codes`에 저장. 재시작 시 당일자면 config에서 복원
4. `_base_codes = set(_morning_target_codes)` — morning target은 top30과 무관하게 하루 종일 보호

### 구현 완료 (260330)
- `_load_morning_target_codes()` 함수 추가 (ws_realtime_trading.py:3249행)
- `_prev_closing_codes` → `_morning_target_codes`로 변수명 변경
- `_base_codes` = morning target 종목으로 고정 (top30 추가/삭제와 무관)
- 재구독 로그에 구독타입/종목 수/종목명 추가
- 초기 구독 로그에 종목 리스트 추가
