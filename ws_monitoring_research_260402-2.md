# WS Monitoring Research 260402-2: VI 예상체결가 미수신 + 장운영상태 관리 개선

**날짜:** 2026-04-02
**관련:** ws_monitoring_research_260402.md (Issue 1~3: 데드락, vi_cls 분기, 반복 실패)

---

## Issue 4: `_desired_subscription_map()`에서 `_vi_active_codes` 누락

### 현상
장중 VI 발동 시 예상체결가(H0STANC0) 구독이 `_apply_subscriptions()`에 의해 해제됨.

### 원인
`_desired_subscription_map()` line 8053:
```python
vi_codes = _vi_delayed_codes & all_codes  # _vi_active_codes 누락
```
- `_vi_delayed_codes`: 09:00~09:02 VI 의심 종목만 포함
- `_vi_active_codes`: 장중 VI 발동 종목 (H0STMKO0 감지)
- `_vi_exp_sub_add()`가 직접 소켓 구독하지만, `_desired_subscription_map()`이 해당 종목을 `ccnl_krx`(rest_codes)에 배치
- `_apply_subscriptions()` 실행 시 exp_ccnl_krx에서 해제됨

### 수정
```python
vi_codes = (_vi_delayed_codes | _vi_active_codes) & all_codes
```

---

## Issue 5: 15:20~15:30 종가 동시호가 예상체결가 미수신

### 현상 (2026-03-31 로그)
```
15:20:16 [ws] kws.start returned (mode=PREOPEN_EXP)
15:20:16 [ws] reconnect in 2s...
15:20:18 [ws] 보유종목 없음 → WSS 구독 생략, 18:00까지 대기
→ 이후 15:30까지 REST보충만 반복 (가격 변동 없음)
```

### 원인
`run_ws_forever()` line 8102~8114: WSS 재연결 시 보유종목 체크.
- 보유종목 0건 → WSS 구독 건너뛰기 → 종가매수 예상체결가 모니터링 불가
- `_price_watchdog_loop()`의 REST 보충만 동작 (FHKST01010100은 동시호가 중 가격 변동 없음)

### 수정
종가매매 시간(15:20~15:30) 또는 `_closing_codes`가 있으면 보유종목 없어도 WSS 유지.

---

## Issue 6: 사이드카/서킷브레이크 미처리

### 현상
- 사이드카 발동 시 데이터 미수신으로 오판 → 불필요한 REST 보충 반복
- 사이드카 이벤트 기록 없음 → 사후 데이터 분석 불가

### 원인
H0STMKO0 핸들러(`_on_market_status_krx`)에서 `mkop_cls_code` 필드 미처리.

### MKOP_CLS_CODE 값 (KIS API PDF 확인)
| 코드 | 의미 |
|---|---|
| 110 | 장전 동시호가 개시 |
| 112 | 장개시 |
| 121 | 장후 동시호가 개시 |
| 129 | 장마감 |
| 130 | 장개시전시간외개시 |
| 139 | 장개시전시간외종료 |
| 140 | 시간외 종가 매매 개시 |
| 146 | 장종료후시간외 체결지시 |
| 149 | 시간외 종가 매매 종료 |
| 150 | 시간외 단일가 매매 개시 |
| 156 | 시간외단일가 체결지시 |
| 159 | 시간외 단일가 매매 종료 |
| **164** | **시장임시정지** |
| **174** | **서킷브레이크 발동** |
| **175** | **서킷브레이크 해제** |
| 182 | 서킷브레이크 장종동시마감 |
| **184** | **서킷브레이크 개시** |
| **185** | **서킷브레이크 해제** |
| **187** | **사이드카 발동** |
| **388** | **사이드카 레이트발동해제** |
| **397** | **사이드카 매수발동** |
| **398** | **사이드카 매수발동해제** |
| ??? | 단전거래시 |
| ??? | 서킷브레이크 단일가접수 |
| F01~F09 | 장개시 10초전~3분전 |
| F11~F18 | 장마감 10초전~3분전 |
| P01~P09 | 장개시 10초전~30분전 |
| P11~P18 | 장마감 10초전~3분전 |

### VI_CLS_CODE (확정)
- `Y`: VI적용된 종목
- `N`: VI적용되지 않은 종목
- **기존 코드 `vi_cls != "0"` 비교는 완전히 잘못됨**

### ANTC_MKOP_CLS_CODE (예상장운영구분코드)
- `112`: 장전예상종료
- `121`: 장후예상시작
- `129`: 장후예상종료
- `311`: 장전예상시작

### 수정 방향
1. `mkop_cls_code` 캐싱 + 사이드카/서킷브레이크 감지 시 알림
2. `market_event` 컬럼으로 parquet 저장
3. 사이드카 발동 시 REST 보충 "미수신" 경고 억제

---

## Issue 7: 15:19~15:20 불필요한 구독 해제/재구독

### 현상
```
15:19:55 사전구독해제: 26종목 해제
15:19:59 _apply_subscriptions: 방금 해제한 26종목 다시 구독 (REGULAR_REAL desired map)
15:20:00 _switch_to_closing_codes: WSS 재연결 → 전체 해제 후 9종목만 구독
15:20:09~15 재연결 후 다시 해제/추가
```

### 원인
`_사전구독해제`가 `_subscribed` dict를 갱신하지만, `_desired_subscription_map()`은 아직 REGULAR_REAL 모드(15:20 미만). 차분 계산 시 해제된 종목을 다시 구독 시도.

### 수정 방향
`_사전구독해제` 시점에 플래그를 설정하여 `_apply_subscriptions` 재구독 억제, 또는 CLOSE_EXP 모드 전환을 15:19:55로 앞당김.

---

## Issue 8: REST 보충 행에 장운영상태 컬럼 부재

### 현상
- H0STCNT0 실시간 데이터: `new_mkop_cls_code` 포함 (parquet 저장)
- REST 보충 행(`_enqueue_rest_price_row`): 11개 컬럼만, `new_mkop_cls_code` 없음
- H0STMKO0 장운영정보: parquet 미포함 (early return)

### 수정 방향
REST 보충 행에 `new_mkop_cls_code` + `market_event` 컬럼 추가.

---

## FHKST01010100 REST API 정보 (참고)

| 필드 | 설명 | VI 관련 |
|---|---|---|
| stck_prpr | 현재가 | X |
| acml_vol | 누적거래량 | X |
| prdy_ctrt | 전일대비율 | X |
| temp_stop_yn | 일시정지 여부 | 간접 (VI와 다름) |
| iscd_stat_cls_code | 종목상태코드 | X (KRX DB와 다른 의미) |

**VI 상태 필드 없음.** VI 조회는 별도 `FHPST01390000` API 필요.

---

## 구현 완료 요약 (2026-04-02)

### 수정 1: vi_cls_code 분기 수정 (Issue 2 해결)
- `vi_cls != "0"` → `vi_cls == "Y"` / `elif vi_cls == "N"` / `else` 미정의값 로깅
- **파일:** `ws_realtime_trading.py` line 5131

### 수정 2: _vi_exp_sub_add 데드락 해결 (Issue 1 해결)
- `_send_subscribe` 호출을 `_vi_exp_sub_worker()` 별도 스레드(daemon)로 위임
- on_result 콜백 내 asyncio 이벤트 루프 데드락 방지
- **파일:** `ws_realtime_trading.py` line 4854~4885

### 수정 3: _desired_subscription_map()에 _vi_active_codes 반영 (Issue 4 해결)
- `vi_codes = _vi_delayed_codes & all_codes` → `(_vi_delayed_codes | _vi_active_codes) & all_codes`
- `_apply_subscriptions()`가 장중 VI 종목의 H0STANC0 구독을 해제하지 않도록 함
- **파일:** `ws_realtime_trading.py` line 8057

### 수정 4: run_ws_forever() 종가매매 구간 WSS 유지 (Issue 5 해결)
- 보유종목 없어도 15:20~15:31 또는 `_closing_codes`가 있으면 WSS 연결 유지
- **파일:** `ws_realtime_trading.py` line 8110~8123

### 수정 5: 장운영상태 기록 + 사이드카/서킷브레이크 감지 (Issue 6 해결)
- H0STMKO0 `mkop_cls_code` 처리 추가: 사이드카(187,388,397,398), 서킷브레이크(174,175,182,184,185), 임시정지(164)
- `_market_event` dict로 종목별 이벤트 상태 관리
- REST 보충 행에 `new_mkop_cls_code` + `market_event` 컬럼 추가
- **파일:** `ws_realtime_trading.py` line 5073~5086 (상수), line 5108~5124 (핸들러), line 4795~4796 (REST행)

### 수정 6: 15:19~15:20 구독 패턴 최적화 (Issue 7 해결)
- `_pre_unsub_closing_done` 후 REGULAR_REAL 모드에서 `_apply_subscriptions` 호출 억제
- **파일:** `ws_realtime_trading.py` line 4208~4210

### 수정 7: VI 타임아웃 단축 + 해제 시 즉시 전환
- fallback 타임아웃 300초 → 150초 (VI 발동~해제 통상 2:03~2:20초)
- VI 해제 시 `_vi_exp_sub_worker`에서 즉시 ccnl_krx(실시간체결가) 구독

### 수정 8: FHPST01390000 REST 폴링 기반 전체 VI 감지
- `_vi_poll_check()`: WSS 5초 미수신 시 FHPST01390000으로 전체 VI 현황 1회 조회
- 발동 감지 → 구독종목이면 예상체결가(H0STANC0) 전환
- 발동시각 + 2분 경과 or API 응답에서 사라짐 → 실시간체결가(H0STCNT0) 복귀
- WSS 슬롯 0개 사용, H0STMKO0 구독 확대 불필요
- `_price_watchdog_loop`의 `not wss_alive` 구간에서 호출

### TODO: a2 계좌 별도 WSS (보류)
- 고객센터에 H0STMKO0 WSS 구독 카운트 포함 여부 확인 중 (2026-04-02)
- 결과에 따라 kis_auth_llm.py 인스턴스별 open_map/approval_key 분리 검토
- ws_realtime_trading.py 파일 상단에 TODO로 기록
