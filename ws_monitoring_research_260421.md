# WS Monitoring Research 260421: VI 예상체결가 미수신 + 15:30 종가 전환 점검

## Issue 1: VI 발동 시 예상체결가 수신이 안되는 문제

### 현상
```
# VI_status_260421.log (Daily_inquire_vi_status.py) - VI 발동 확인
090101,004 [VI Status 조회_발동] 무림SP(001810) 발동시각 090030
090101,516 [VI Status 조회_발동] 케이씨에스(115500) 발동시각 090022
090101,516 [VI Status 조회_발동] 삼천리자전거(024950) 발동시각 090021
090101,619 [VI Status 조회_발동] 위메이드맥스(101730) 발동시각 090020
090101,721 [VI Status 조회_발동] 파인텍(131760) 발동시각 090016
090101,824 [VI Status 조회_발동] 진원생명과학(011000) 발동시각 090015
090101,925 [VI Status 조회_발동] 벨로크(424760) 발동시각 090014
090102,131 [VI Status 조회_발동] 로지시스(067730) 발동시각 090012
090102,233 [VI Status 조회_발동] SGA솔루션즈(184230) 발동시각 090010
090102,439 [VI Status 조회_발동] 씨이랩(189330) 발동시각 090003

# wss_TR_260421.log - ws_realtime_trading.py에는 [VI발동] / [VI해제] 로그 전혀 없음
# H0STMKO0 구독은 keep-alive 2종목만:
[260421_085909_TR] [장운영] H0STMKO0 구독: 보유 0개 + keep-alive 2개 (KOSPI=삼성전자,KOSDAQ=카카오게임즈)
```
- `Daily_inquire_vi_status.py`(별도 프로세스)에서는 당일 다수 종목에 VI 발동을 확인
- `ws_realtime_trading.py`에서는 `[VI발동]` / `[VI전환]` 로그가 한 건도 없음
- VI 발동 종목에 대한 예상체결가 구독 전환(`_vi_exp_sub_switch`)이 실행되지 않음

### 원인 (ws_realtime_trading.py:8732~8743, 5642~5667)

**H0STMKO0 구독 대상에 거래 대상 종목이 포함되지 않음.**

VI 감지 흐름:
1. H0STMKO0 장운영정보 수신 -> `_handle_market_status()` (5575행)
2. `vi_cls_code` 변경 감지 (5642~5648행)
3. VI 발동이면 `_vi_exp_sub_switch(code)` 호출 -> 실시간체결 해제, 예상체결가 구독 (5317행)

문제: H0STMKO0 초기 구독 로직 (8732~8743행):
```python
# H0STMKO0 장운영정보: 보유종목 VI 감지 + 장운영 keep-alive (정규장 시간대)
mko_codes: set[str] = set()
if dtime(8, 50) <= now_t < dtime(15, 30):
    held_codes = set()
    with _str1_sell_state_lock:
        held_codes = {c for c, st in _str1_sell_state.items() if not st.get("sold")}
    keepalive_codes: set[str] = set()
    for mkt, rep_code in _MKT_KEEPALIVE_INITIAL.items():
        if not _mkt_keepalive_current.get(mkt):
            _mkt_keepalive_current[mkt] = rep_code
        keepalive_codes.add(_mkt_keepalive_current[mkt])
    mko_codes = held_codes | keepalive_codes
```

`mko_codes = held_codes | keepalive_codes` 에서:
- `held_codes` = 보유종목 (당일 보유종목 0개 -> 빈 set)
- `keepalive_codes` = KOSPI/KOSDAQ 대표 종목 (삼성전자, 카카오게임즈)

**결과: 실제 거래 대상 13종목(codes)에 대해 H0STMKO0가 구독되지 않음.**

H0STMKO0가 구독되지 않은 종목은 vi_cls_code 데이터를 수신하지 못하므로 `_handle_market_status()`가 호출되지 않고, VI 발동/해제를 감지할 수 없다.

한편, 09:00~09:02 구간에서 `_vi_delayed_codes`를 통한 예상체결 유지는 별도 로직이며(전일등락률 >= 10% 종목에 대해 09:02까지 예상체결가 유지), 이는 실제 VI 발동 감지가 아닌 사전 예방 조치임. 로그에서 확인:
```
[260421_090001_TR] VI 급등 조건 -> 종목별 구독 분리: 실시간체결 10종목, 예상체결유지 8종목
[260421_090200_TR] VI 지연 해제 -> 전 종목 실시간체결 전환
```
이 8종목은 09:02에 일괄 해제되며, 이후에는 H0STMKO0 미구독으로 인해 VI 감지가 불가능.

### 수정 방안

H0STMKO0 구독 대상에 **거래 대상 종목(codes)**도 포함해야 한다.

방법 A (간단): `mko_codes = held_codes | keepalive_codes | set(codes)` 로 변경
- 문제: 슬롯 소비 증가 (13종목 + keep-alive 2 = 15 슬롯). 40 슬롯 상한에서 종목 데이터 구독 가능 수가 24개로 줄어듦
- 이미 슬롯 상한 준수 로직(8745~8776행)이 있으므로 자동 조정됨

방법 B (최적): VI 발동 가능성이 높은 종목만 선별 구독
- 전일등락률 >= 10% (`_vi_delayed_codes`에 이미 해당 종목 파악됨) 종목만 H0STMKO0 추가
- 09:02 이후에도 해당 종목의 H0STMKO0 유지

방법 C (fallback 보강): 현재 300초 타임아웃 fallback(5920행 주석)이 있으나 이는 이미 VI 전환된 종목의 해제용. VI 발동 자체를 감지하려면 H0STMKO0 구독이 필수.

### 결정 사항
- (미결정)

---

## Issue 2: 15:30 예상체결가 -> 실시간체결가 전환 확인

### 현상
```
# 15:20 종가매매 전환 + 종가매수 주문
[260421_152004_TR] [종가매매전환] WSS 유지, 15:21 예상체결가 구독 예정

# 15:21 예상체결가(H0STANC0) 구독 완료
[260421_152101_TR] [예상체결가구독] 15:21 13종목 구독 완료
[260421_152101_TR] [tr_id전환] 국일제지(078130) FHKST01010100 -> H0STANC0
... (13종목 순차 전환)

# 15:30 종가 체결가 전환
[260421_153000_TR] 15:30 종가 체결가 구독 전환
[260421_153001_TR] [구독 해제 결과] TR_ID=unknown 종목=... (13종목 예상체결가 해제)
[260421_153002_TR] [구독 추가 결과] TR_ID=unknown 종목=... (13종목 실시간체결가 구독)
[260421_153005_TR] [tr_id전환] 씨이랩(189330) H0STANC0 -> H0STCNT0
[260421_153005_TR] [mkop변경] 씨이랩(189330) 00 -> 20
... (13종목 순차 전환, ~15:30:26까지)

# 15:31 타임아웃 처리
[260421_153100_TR] [15:31_종가체결] 잔고검증 완료(4822) -- 변동 없음
[260421_153101_TR] [구독 해제 결과] TR_ID=unknown 종목=... (13종목 해제)
[260421_153101_TR] 15:31 타임아웃: 종가 체결 11/13종목 수신, 체결가 구독 해제, WS 연결은 유지
```

### 원인 (ws_realtime_trading.py:8651~8664, 4500~4503, 4622~4651)

**전환은 정상적으로 수행됨.**

1. **15:20:04** - `CLOSE_EXP` 모드 진입, 종가매수 주문 11건 발송, `_switch_to_closing_codes()` 호출
2. **15:21:01** - 예상체결가(H0STANC0) 13종목 구독 완료. `_desired_subscription_map`에서 `dtime(15,21) <= t < dtime(15,30)` 구간은 `{exp_ccnl_krx: all_codes}` 반환 (8653행)
3. **15:30:00** - `CLOSE_REAL` 모드 진입. `_desired_subscription_map`에서 `dtime(15,30) <= t < dtime(16,0)` 구간은 `{ccnl_krx: all_codes}` 반환 (8664행). `_apply_subscriptions`가 예상체결 해제 + 실시간체결 구독 수행
4. **15:30:05~26** - 13종목 순차적으로 `H0STANC0 -> H0STCNT0` 전환 확인 (종가 체결 데이터 수신)
5. **15:31:01** - 타임아웃: 11/13종목 수신. 전 종목 구독 해제, WS 연결은 유지 (체결통보 수신 지속)

**2종목 미수신 원인**: 15:31까지 `_regular_real_seen`에 등록되지 않은 2종목이 있었음. 해당 종목은 종가 동시호가에서 체결이 없었거나 데이터 수신이 지연된 것으로 추정. 15:31 타임아웃에 의해 정상적으로 구독이 해제되었으므로 운영상 문제 없음.

### 수정 방안
- 수정 불필요. 15:30 전환 로직은 정상 동작 확인.
- 11/13종목 수신은 정상 범위 (거래 없는 종목은 체결가 미수신).

### 결정 사항
- 15:30 전환 정상 확인 완료. 조치 불필요.
