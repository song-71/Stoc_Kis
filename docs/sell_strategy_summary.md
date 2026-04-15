# 매도 전략 정리 (트레일스톱 / 손절가)

> 기준 파일: `ws_realtime_trading.py` (4.1 복원 + a2 분리), `ws_realtime_tr_str1.py`
> 작성일: 2026-04-15 (매도 즉시 스레드 전환 반영)

## 매도 주문 실행 아키텍처 (2026-04-15 변경)

**변경 전**: `ingest_loop → _enqueue_str1_sell → _str1_sell_queue → _str1_sell_worker(0.5s poll) → REST API`
**변경 후**: `ingest_loop → _enqueue_str1_sell → 즉시 daemon 스레드(_do_sell_order) → REST API`

- **모든 매도** (트레일스톱, wghn하락, 데드크로스, 손절, VI매도, 일괄매도 등)가 조건 충족 즉시 daemon 스레드에서 REST 호출 (기존 sell_worker 큐 0.5s 폴링 제거)
- **29%+ 긴급매도만 추가로** on_result에서 `stck_prpr` 기반 즉시 감지 (ingest_loop 지표 계산 불필요 → 완전 우회)
- 다른 매도는 EMA/가���평균 등 지표 의존 → ingest_loop에서 지표 계산 후 조건 판단 → 즉시 스레드 REST (지표 계산은 틱당 수십ms, 주문 큐 대기 0.5s가 제거된 것이 핵심)
- sell_worker는 **취소(cancel) 전용**으로 유지
- 중복 방지: `_str1_sell_state_lock`으로 state 선점 후 스레드 생성

---

## 1. Str1 일반 포지션 (source != "vi")

`check_realtime_sell()` (`ws_realtime_tr_str1.py:82`) + `ws_realtime_trading.py:7371~7401`

| 순서 | 조건 | 기준 | 비고 |
|------|------|------|------|
| **⓪ 트레일스톱** | 최고가가 매수가 대비 **+3%** 이상 도달 후, 최고가 대비 **-3%** 하락 | `trail_activate_pct=0.03`, `trail_stop_pct=0.03` | 최고가 = `highest_since_buy` (bidp1 vs 일중최고가 중 큰 값) |
| **① 가중평균 하락** | `bidp1 < wghn_avrg * 0.995` | 가중평균 대비 **-0.5%** | `stck_oprc > 0` 필요 (장 시작 전 미동작) |
| **② 정배열 데드크로스** | up_trend 상태에서 `ma500 < ma2000` | EMA 데드크로스 | 매도 성공까지 재시도 (`_ema_sell_cond`) |
| **③ 매수가 손절** | `bidp1 < buy_price * 0.97` | 매수가 대비 **-3%** | 위 조건 모두 미해당 시 적용 |

**판단 흐름**: ⓪ → ① → ② → ③ 순서로 평가, 먼저 충족된 조건으로 매도.

---

## 2. VI 매수 포지션 (source == "vi")

`check_vi_sell()` (`ws_realtime_tr_str1.py:200`) + `ws_realtime_trading.py:7346~7369`

| 순서 | 조건 | 기준 |
|------|------|------|
| **① 매수가 하회** | `bidp1 < buy_price` | 즉시 매도 (0% 손절) |
| **② 트레일스톱** | 최고가가 매수가 대비 **+3%** 이상 도달 후, 최고가 대비 **-3%** 하락 | Str1과 동일 파라미터 |
| **③ 10분 이내** | `ma50 < ma200` | 단기 EMA 역전 |
| **④ 10분 초과** | up_trend 상태에서 `ma500 < ma2000` | 장기 EMA 데드크로스 |

**VI vs Str1 차이점**:
- VI는 매수가 하회 시 **즉시 손절** (Str1은 -3%까지 허용)
- VI 최고가는 **bidp1 기준만** 사용 (일중최고가 미사용)

---

## 3. 상한가 근접 스톱 (클라이언트 스톱)

`ws_realtime_trading.py:7291~7344`

| 단계 | 조건 | 동작 |
|------|------|------|
| 모니터링 진입 | `prdy_ctrt >= 29%` | 조건가 = 전일종가 × 1.29, 매도가 = 전일종가 × 1.28 설정 |
| 이탈 확인 | `bidp1 < 조건가` N틱 연속 (`STOP_LIMIT_CONFIRM_TICKS`) | **지정가 매도** (시장가 아님, 매도가=전일종가×1.28) |
| 복귀 | `bidp1 >= 조건가` | 이탈 카운트 리셋 |

---

## 4. 15:19:30 일괄 매도

`_run_sell_1518()` (`ws_realtime_trading.py:1892`)

| 유지 조건 (AND) | 매도 |
|-----------------|------|
| 전일대비 **+20%** 이상 | 조건 미충족 시 시장가 매도 |
| 당일 최고가 대비 **-5%** 이내 | |

- 하루 1회만 실행 (`_sell_1518_done`)
- REST API로 현재가/고가 조회하여 판단

---

## 5. 주기적 3% 손절 체크 (백업)

`_check_sell_by_prdy_ctrt_periodic()` (`ws_realtime_trading.py:2029`)

- **60초 주기**로 REST API 현재가 조회
- `bidp1 < buy_price * 0.97` 확인 → 매도
- 실시간 틱 놓침 방지용 백업 로직

---

## 6. 장전 시간대 매도 (08:29~09:00)

`check_opening_call_auction_sell()` / `check_opening_call_auction_cancel()` (`ws_realtime_tr_str1.py`)

- 예상체결가(`antc_prce`)의 `prdy_ctrt < 0` → 시초가 하락 예상 시 시장가 매도 선제 등록
- 예상가 복귀 시 취소 (`check_opening_call_auction_cancel`)

---

## 최고가(highest) 갱신 방식

| 유형 | 갱신 기준 | 코드 위치 |
|------|----------|----------|
| **VI** | `bidp1` 기준만 (일중최고가 미사용) | `ws_realtime_trading.py:7352` |
| **Str1** | `max(bidp1, 기존highest)` + 일중최고가(`stck_hgpr`)도 반영 | `ws_realtime_trading.py:7377`, `6953` |

---

## 파라미터 요약

| 파라미터 | 값 | 적용 대상 |
|----------|-----|----------|
| `trail_activate_pct` | 3% (매수가 대비) | Str1, VI 공통 |
| `trail_stop_pct` | 3% (최고가 대비) | Str1, VI 공통 |
| 매수가 손절 (Str1) | -3% | Str1 일반 |
| 매수가 손절 (VI) | 0% (하회 즉시) | VI |
| 가중평균 하락 | -0.5% | Str1 일반 |
| 상한가 스톱 진입 | +29% | 공통 |
| 상한가 스톱 매도가 | +28% | 공통 |
| **긴급매도 활성화** | `EMERGENCY_29PCT_SELL_ENABLED=True` | 29%+ 종목 |
| **긴급매도 호가 오프셋** | `EMERGENCY_SELL_HOGA_OFFSET=5` (현재가-5호가 지정가) | 29%+ 종목 |
| 일괄매도 유지 기준 | +20% AND 고가 -5% 이내 | 15:19:30 |

---

## 7. 29%+ 긴급매도 Fast-Path (2026-04-15 추가)

`ws_realtime_trading.py` — `_fast_29pct_sell_check()`, `_fire_emergency_sell()`

### 배경
상한가(29%+) 도달 후 급락 시 기존 경로(ingest_loop → sell_worker)는 틱 폭주로 ~5초 지연 발생.
그 사이 VI 발동 → 시장가 주문이 VI 종료가에 체결되어 큰 손실 확대 (에프알텍 사례: 29.97% → 20.48%).

### 동작 방식
1. **감지**: WSS on_result 콜백에서 직접 `stck_prpr` 확인 (ingest_loop 우회)
2. **조건**: `sl_monitoring=True` (bidp1이 29%+ 도달하여 모니터링 중) AND `stck_prpr < sl_cndt_price`
3. **주문**: 별도 daemon 스레드 즉시 생성 → **지정가** 매도 (현재가 - 5호가)
4. **중복 방지**: state 선점 (`sold=True`, `sell_ordered=True`) 후 스레드 생성, 기존 경로는 자동 스킵
5. **실패 시**: state 롤백 → 기존 30틱 경로에서 재시도

### 시장가 대신 지정가인 이유
- 시장가 주문은 VI 기간에 걸리면 VI 종료가에 체결 → 큰 손실
- 지정가(현재가-5호가)는 급락 시 즉시 체결 + VI 시 지정가 이하 체결 방지

### 기존 30틱 경로와의 관계
- Fast-path가 먼저 선점하면 30틱 경로는 `sell_ordered=True`로 자동 스킵
- Fast-path 실패 시 state 롤백되어 30틱 경로가 백업으로 동작
