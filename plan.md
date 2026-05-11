# 개선 계획: 매도 PNL 오류 + 중복 텔레그램 알림 + 1분봉 데이터 파이프라인

> research_260323-1.md 분석 결과 기반

---

## 우선순위 및 타이밍

| 단계 | 이슈 | 난이도 | 소요시간 | 타이밍 |
|------|------|--------|---------|--------|
| 1️⃣ | **PNL 마이너스 버그** (라인 1677, 1790) | 낮음 | ~5분 | 🔴 **즉시** |
| 2️⃣ | **중복 텔레그램 알림** (라인 2202) | 낮음 | ~2분 | 🔴 **즉시** |
| 3️⃣ | **체결통보 PNL 표시** (라인 7229~7233) | 중간 | ~15분 | 🟡 우선 |
| 4️⃣ | **1분봉 파이프라인** (신규 함수들) | 높음 | ~2시간 | 🟢 차후 |

---

## Phase 1: 즉시 수정 (버그 픽스)

### Task 1-1: PNL 계산 버그 수정
**파일**: `ws_realtime_trading.py`
**위치**: 라인 1677, 1790

**문제**: `calc_sell_pnl(ref_price, ref_price, qty)` — buy_price를 2번째 인자 대신 ref_price 전달
- 매수가와 매도가를 동일 값으로 계산 → 수수료+세금만큼 항상 마이너스

**수정 방법**:
```python
# BEFORE (라인 1677)
pnl_info = calc_sell_pnl(ref_price, ref_price, qty) if ref_price > 0 else {}
with _str1_sell_state_lock:
    st = _str1_sell_state.get(code, {})
    buy_price = st.get("buy_price") or ref_price

# AFTER
with _str1_sell_state_lock:
    st = _str1_sell_state.get(code, {})
    buy_price = st.get("buy_price") or ref_price
pnl_info = calc_sell_pnl(buy_price, ref_price, qty) if buy_price > 0 else {}
```

**영향**:
- ✅ 매도 체결 시 정확한 PNL 계산
- ✅ 로그/텔레그램에서 올바른 수익률 표시
- ✅ 라인 1790(재주문 로직)도 동일 방식 적용

---

### Task 1-2: 중복 텔레그램 알림 제거
**파일**: `ws_realtime_trading.py`
**위치**: 라인 2202~2203

**문제**: 동일 체결 1건이 `_on_result()` + `_on_ccnl_notice_filled()`에서 각각 텔레그램 발송

**수정 방법**:
```python
# BEFORE (라인 2202~2203) — 중복 텔레그램
fill_msg = f"{ts_prefix()} [체결통보] Str1 동시호가매도 체결 {name_fill}({code}) 수량={qty}주"
_notify(fill_msg, tele=True)

# AFTER (로그만 유지)
fill_msg = f"{ts_prefix()} [체결통보] Str1 동시호가매도 체결 {name_fill}({code}) 수량={qty}주"
logger.info(fill_msg)
```

**영향**:
- ✅ 범용 알림(라인 7233 `_on_result()`)에만 텔레그램 발송
- ✅ 텔레그램 중복 제거
- ✅ 상태 전환 로그는 유지 (sold=True 확인 용도)

---

## Phase 2: 체결통보 개선

### Task 2-1: 매도체결 시 PNL 표시 추가
**파일**: `ws_realtime_trading.py`
**위치**: 라인 7229~7233 (매도체결 분기)

**목표**: 매도 체결 시 수익률을 텔레그램 알림에 함께 표시

**수정 방법**:
```python
# 라인 7226 이후 매도 조건 분기에서
else:  # 체결 (cntg_yn == "2")
    qty = int(float(str(row.get("cntg_qty", 0) or 0).replace(",", "") or 0))
    price = int(float(str(row.get("cntg_unpr", 0) or 0).replace(",", "") or 0))
    rmn = int(float(str(row.get("rmn_qty", 0) or 0).replace(",", "") or 0))

    # ✨ NEW: 매도체결 시 PNL 계산
    pnl_txt = ""
    if seln in ("01", "1") and price > 0:  # 매도 주문
        with _str1_sell_state_lock:
            st = _str1_sell_state.get(code, {})
            bp = float(st.get("buy_price") or 0)
        if bp > 0:
            pnl_info = calc_sell_pnl(bp, price, qty)
            pnl_txt = f" | PNL≈{pnl_info['pnl']:+,.0f}원({pnl_info['ret_pct']:+.2f}%)"

    _notify(
        f"{ts_prefix()} [체결통보] {side}체결 {label} {qty}주 x {price:,}원"
        f" | 잔여={rmn} | 주문번호={oder_no}{pnl_txt}",
        tele=True,
    )
```

**영향**:
- ✅ 매도체결 텔레그램에 수익률 표시
- ✅ 실시간 거래 성과 한눈에 확인
- ✅ Task 1-1(PNL 버그 수정) 완료 후 구현 필수

---

## Phase 3: 1분봉 데이터 파이프라인 (고급)

### Task 3-1: 데이터 구조 및 기본 함수
**파일**: `ws_realtime_trading.py` (전역 변수 + 함수)

**전역 변수 추가**:
```python
_candle_1m: dict[str, pl.DataFrame] = {}              # code → 1m OHLCV
_candle_builder: dict[str, dict] = {}                 # code → 현재 분봉 빌더
_api_1m_fetch_history: dict[str, str] = {}            # code → 마지막 API 수신 시각
_1m_indicators: dict[str, dict] = {}                  # code → 1m 지표 저장소
```

**추가 함수**:
```python
def _load_historical_1m(code: str) -> pl.DataFrame:
    """과거 2일치 1m parquet 로드"""
    # /home/ubuntu/Stoc_Kis/data/1m_data/{YYYYMMDD}_1m_chart_DB_parquet.parquet
    # 최근 2일치 파일 로드 → code 필터링

def _fetch_intraday_1m(code: str) -> pl.DataFrame:
    """당일 1m REST API 증분 조회"""
    # KIS API FHKST03010200 활용
    # _api_1m_fetch_history[code] 이후 데이터만 조회
    # 마지막 1분봉은 미완성 가능 → 항상 재조회로 갱신
```

**호출 시점**:
- 장 시작 시 `_top_rank_loop()` 초기화 단계: 구독 종목별로 일괄 로드

---

### Task 3-2: WSS 틱 → 1분봉 실시간 집계
**파일**: `ws_realtime_trading.py` (`ingest_loop()` 내부, 라인 7456 부근)

**추가 함수**:
```python
def _update_candle_builder(code: str, price: float, volume: int, recv_ts: str):
    """틱 1개 → 현재 분봉에 반영, 분 전환 시 완성 캔들 생성"""
    # recv_ts[11:16] → "0930" (분 단위)
    # 분 경계 도달 시:
    #   1. 이전 분봉 → _candle_1m[code]에 append
    #   2. 새 분봉 시작

def _finalize_candle(code: str, builder: dict):
    """완성 캔들을 DataFrame에 추가 → 1m 지표 재계산"""
```

**호출 위치**:
- `ingest_loop()` 라인 7456~7473: `_calc_indicators()` 호출 직전

---

### Task 3-3: 누락 감지 및 보충
**파일**: `ws_realtime_trading.py`

**추가 함수**:
```python
def _check_candle_gap(code: str) -> bool:
    """2분 이상 데이터 공백 감지 (구독 해제→재개 시)"""
    # 마지막 완성 캔들 시각 vs 현재 시각
    # 타 종목은 정상 수신 중인지 확인 (거짓 양성 방지)

def _fill_candle_gap(code: str):
    """REST API로 누락 구간 1m 데이터 보충"""
    # _fetch_intraday_1m() → 증분 조회
    # 기존 데이터와 병합 (마지막 1건은 갱신)
```

**호출 시점**:
- Top30 구독 재개 시: `_check_candle_gap()` True → `_fill_candle_gap()`

---

### Task 3-4: 1분봉 기반 지표 계산
**파일**: `ws_realtime_trading.py`

**추가 함수**:
```python
def _calc_1m_indicators(code: str) -> dict:
    """1m OHLCV 기반 MA, BB 계산"""
    # MA: 5m, 10m, 20m, 60m, 120m
    # BB: 20분봉 (upper, mid, lower)
    # Return: {"ma5m": ..., "ma10m": ..., "bb_mid_1m": ..., ...}
```

**호출 시점**:
- `_finalize_candle()` 내부: 1분봉 완성 후 호출
- 결과 저장: `_1m_indicators[code]` 업데이트

---

### Task 3-5: Top30 변동 통합
**파일**: `ws_realtime_trading.py` (`_top_rank_loop()` 내부)

**작업 내용**:
- 종목 새 구독 시: `_load_historical_1m(code)` 호출
- 종목 구독 해제 시: `_candle_1m[code]`, `_api_1m_fetch_history[code]` 정리
- 구독 재개 시: `_check_candle_gap()` + `_fill_candle_gap()` 흐름

---

## 구현 순서

```
🔴 Phase 1 (즉시, ~7분)
├─ Task 1-1: PNL 버그 수정
├─ Task 1-2: 중복 텔레그램 제거
└─ Commit: "fix: PNL 계산 및 중복 텔레그램 알림"

🟡 Phase 2 (우선, ~15분)
├─ Task 2-1: 매도체결 PNL 표시
└─ Commit: "feat: 매도체결 알림에 수익률 표시"

🟢 Phase 3 (차후, ~2시간)
├─ Task 3-1: 1m 데이터 구조 + 초기 로드
├─ Task 3-2: WSS 틱 → 1m 집계
├─ Task 3-3: 누락 감지 + 보충
├─ Task 3-4: 1m 지표 계산
├─ Task 3-5: Top30 변동 통합
└─ Commit: "feat: 실시간 1분봉 데이터 파이프라인"
```

---

## 참고: 함수 시그니처 및 데이터 구조

### calc_sell_pnl()
```python
def calc_sell_pnl(buy_price: float, sell_price: float, qty: int) -> dict:
    """Return: {'pnl': int (원), 'ret_pct': float (%)}"""
```

### _notify()
```python
def _notify(msg: str, tele: bool = False) -> None:
    """tele=True 시 텔레그램 전송"""
```

### 1m 데이터 경로
```
로드: /home/ubuntu/Stoc_Kis/data/1m_data/{YYYYMMDD}_1m_chart_DB_parquet.parquet
컬럼: date, time, code, name, open, high, low, close, volume, acml_volume, acml_value, tdy_ret, pdy_close, pdy_ret
```

### REST API
```
기존 파일: inquire_time_itemchartprice.py 재활용
API: KIS FHKST03010200 (주식당일분봉조회)
호출: inquire_time_itemchartprice(code, "1", start_time, end_time)
```
