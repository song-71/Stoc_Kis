# 거래 기록 운영 프로세스 정리

## 1. 현재 기록 위치 요약

| 대상 | 경로 | 담당 프로그램 | 비고 |
|------|------|---------------|------|
| **거래장부 (주문/체결)** | `data/trades/trades_all.parquet` | kis_Trading_1_LmUp_str.py (mode4) | **ws_realtime_subscribe_to_DB-1.py는 사용 안 함** |
| 시세 틱 데이터 | `data/wss_data/{yymmdd}_wss_data.parquet` | ws_realtime_subscribe_to_DB-1 | 실시간/예상체결가 시세만 |
| 체결통보 raw 로그 | `logs/ccnl_notice_{yymmdd}.log` | ws_realtime_subscribe_to_DB-1 | H0STCNI0 텍스트 (parquet 미포함) |
| 종가매수 체결 수량 | `data/fetch_top_list/closing_filled_{yymmdd}.json` | ws_realtime_subscribe_to_DB-1 | code→체결수량 |
| 종가매수 주문/상태 | `data/fetch_top_list/closing_buy_state_{yymmdd}.json` | ws_realtime_subscribe_to_DB-1 | 주문 목록, filled_qty 등 |

---

## 2. 장별 거래 기록 처리 현황

### 2.1 ws_realtime_subscribe_to_DB-1.py (DB-1 메인 프로그램)

#### 08:30~08:40 시간외 종가 (전일 미매수 추가 매수)

| 항목 | 기록 위치 | 형식 |
|------|-----------|------|
| 주문 | closing_buy_state JSON | orders[] 내 status, order_qty 등 |
| 체결 | closing_filled JSON, _closing_filled_by_code | code→수량 |
| raw 체결통보 | ccnl_notice_{yymmdd}.log | 텍스트 |
| trades_all.parquet | **기록 안 함** | - |

#### 09:00~15:20 정규장

| 항목 | 기록 위치 | 형식 |
|------|-----------|------|
| 시세 틱 | wss_data parquet | 실시간 체결가 (SAVE_REAL_REGULAR 시간대별) |
| 주문/체결 | **없음** (DB-1은 정규장 매매 없음) | 종가매수만 수행 |
| trades_all.parquet | **기록 안 함** | - |

#### 15:20~15:30 종가 동시호가 (15:30 일괄 체결)

| 항목 | 기록 위치 | 형식 |
|------|-----------|------|
| 주문 | closing_buy_state JSON | 15:20 시장가 주문 |
| 체결 | closing_filled JSON, H0STCNI0 | code→체결수량 |
| raw 체결통보 | ccnl_notice_{yymmdd}.log | 텍스트 |
| 시세 틱 | wss_data parquet | 예상체결가(15:20~15:30), 체결가(15:30~) |
| trades_all.parquet | **기록 안 함** | - |

#### 15:40~16:00 시간외 종가 (당일 미매수 추가 매수)

| 항목 | 기록 위치 | 형식 |
|------|-----------|------|
| 주문 | closing_buy_state JSON | ORD_DVSN=02 당일종가 지정가 |
| 체결 | closing_filled JSON, H0STCNI0 | code→체결수량 |
| raw 체결통보 | ccnl_notice_{yymmdd}.log | 텍스트 |
| trades_all.parquet | **기록 안 함** | - |

#### 16:00~18:00 시간외 단일가 (10분 단위 재주문)

| 항목 | 기록 위치 | 형식 |
|------|-----------|------|
| 주문 | closing_buy_state JSON | ORD_DVSN=06 시장가(상한가) |
| 체결 | closing_filled JSON, H0STCNI0 | code→체결수량 |
| raw 체결통보 | ccnl_notice_{yymmdd}.log | 텍스트 |
| 시세 틱 | wss_data parquet | 예상/체결 (SAVE_REAL/EXP_OVERTIME) |
| trades_all.parquet | **기록 안 함** | - |

### 2.2 H0STCNI0 체결통보 처리 (ws_realtime_subscribe_to_DB-1)

```
체결통보 수신 → _write_ccnl_notice_log(텍스트 로그) + _on_ccnl_notice_filled(메모리/JSON)
              → ingest_loop 진입 전 return → parquet 미포함
```

- **parquet에 저장되지 않음**: `on_result` 내 `if trid == "H0STCNI0": ... return` 으로 ingest_queue에 넣지 않음
- wss_data parquet는 FHKST01010100(실시간체결), FHKST01010600(예상체결) 등 시세 TR만 포함

### 2.3 kis_Trading_1_LmUp_str.py (Top Trading mode4)

| 시점 | stage | trades_all.parquet 기록 |
|------|-------|-------------------------|
| 주문 시 | order | side, code, ord_no, req_qty, ord_dvsn, req_price, pre_qty |
| 체결 확인 시 | fill | side, code, ord_no, fill_qty, fill_price, fill_amt, pre_qty, post_qty |
| 취소 시 | cancel | side, code, ord_no, cancel_qty |

- **조건**: ENABLE_TRADE_LEDGER=True, CURRENT_RUN_MODE=4
- DB-1은 이 프로그램과 별개로 동작 → DB-1의 종가매수 거래는 trades_all.parquet에 기록되지 않음

---

## 3. 핵심 이슈

1. **trades_all.parquet**는 Top Trading(mode4) 전용이며, **ws_realtime_subscribe_to_DB-1.py는 이 파일에 기록하지 않음**
2. DB-1의 모든 주문/체결(08:30, 15:20, 15:40, 16:00~18:00)은 JSON + ccnl_notice 로그에만 의존
3. 시세 parquet(wss_data)는 **가격/호가/거래량 틱**이며, **주문/체결 장부**와는 성격이 다름

---

## 4. 개선 권장 사항

### 4.1 ws_realtime_subscribe_to_DB-1에 trades_all.parquet 기록 추가

**방향**: DB-1에서 주문·체결 발생 시 `data/trades/trades_all.parquet`에 append

| 시점 | stage | 기록 필드 예시 |
|------|-------|----------------|
| 주문 성공 시 | order | ts, stage, side=BUY, code, ord_no, req_qty, ord_dvsn, req_price, session (08:30/15:20/15:40/16:xx) |
| H0STCNI0 체결 수신 시 | fill | ts, stage, code, fill_qty, fill_price(체결통보에서 추출 가능 시), ord_no(가능 시) |

**구현 위치 후보**:

1. **`_buy_order_cash` 반환값 활용**: 주문 성공 시 ord_no 등으로 order row 기록
2. **`_on_ccnl_notice_filled`**: 체결 시 fill row 기록 (체결가격은 H0STCNI0 스키마에 따라 추가)
3. **공통 함수**: `_append_trade_to_ledger(stage, **kwargs)` → trades_all.parquet append (기존 kis_Trading 로직과 유사)

### 4.2 통합 trades_all.parquet 스키마

여러 프로그램이 한 파일에 기록할 때 스키마 통일 권장:

```
ts, stage, side, code, ord_no, req_qty, req_price, ord_dvsn,
fill_qty, fill_price, fill_amt, session, source
```

- `source`: "DB1" | "TopTrading" 등 구분
- `session`: "08:30종가" | "15:30종가" | "15:40종가" | "16:xx단일가" 등

### 4.3 체결가격 보완

- H0STCNI0 체결통보에 체결가 필드가 있으면 바로 사용
- 없으면: 당일 해당 슬롯의 wss_data parquet에서 해당 code·시간대 체결가 조회하여 보완

### 4.4 우선순위 제안

1. **1단계**: DB-1 주문 시(order) trades_all.parquet에 order row 기록
2. **2단계**: H0STCNI0 체결 시 fill row 기록 (가능한 필드만)
3. **3단계**: kis_Trading과 스키마/소스 구분 정리, 중복/충돌 방지
