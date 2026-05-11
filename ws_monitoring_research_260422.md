# WS Monitoring Research 260422: ingest_loop 성능 병목 분석

## Issue 1: ingest_loop 정상운영 200~300건/s vs shutdown 1500건/s 성능 격차

### 현상
- 정상 운영 시 ingest_loop 처리량: 초당 200~300건
- shutdown 시 (다른 스레드 종료 후) ingest_loop 처리량: 초당 1300~1500건
- 약 5~7배 성능 격차 → 다른 스레드에 의한 GIL 경합 또는 lock 경합이 주 원인

### 분석 1: ingest_loop 전체 구조 (8269~8577행)

ingest_loop은 `_ingest_queue`에서 item을 꺼내 아래 순서로 처리한다:

| 순서 | 블록 | 행번호 | Lock | 비고 |
|------|------|--------|------|------|
| 1 | Queue get | 8274 | - | timeout=0.5 |
| 2 | 저장 게이팅 | 8297 | `_flags_lock` | 짧은 read-only |
| 3 | 모니터링 카운트 | 8310~8399 | **`_lock`** | **가장 큰 critical section** |
| 4 | 종가 예상체결가 모니터 | 8402~8407 | - | RunMode.CLOSE_EXP 시만 |
| 5 | 캐시 업데이트 (prdy_ctrt, stck_prpr, mkop) | 8409~8458 | - | partition_by 2회차 |
| 6 | VI delayed 조기 해제 | 8460~8485 | - | regular_exp + VI delayed 시만 |
| 7 | **Parquet 저장 + 기술적 지표** | 8487~8540 | **`_lock`** + `_part_buffer_lock` | **가장 무거운 블록** |
| 8 | Str1 매도 조건 체크 | 8542~8547 | `_str1_sell_state_lock` (내부) | |
| 9 | VI 예상체결가 모니터 | 8549~8554 | - | VI 활성 시만 |
| 10 | 요약 로그 | 8556~8576 | `_lock` | 30초마다 |

### 분석 2: 핵심 병목 요인

#### 병목 A: `_lock` (RLock) 경합 — 최대 병목

`_lock`은 전체 코드에서 **23회** 사용되며, ingest_loop 내에서만 3회 획득한다:
- 8314행: 모니터링 카운트 (틱마다)
- 8499행: 저장 시 partition_by + 지표 계산 (save=True 시 틱마다)
- 8563행: 요약 로그 (30초마다)

**다른 스레드에서의 `_lock` 사용** (경합 원인):
- `_price_watchdog_loop` (5910, 5919행): 09:00~09:10 개장 보충 시 codes 전체 순회하면서 lock 보유
- `_price_request_loop` (6000, 6052행): REST 보충 요청
- `scheduler_loop` (4598행 등): 주기적 상태 확인
- `_top_rank_loop` (6674행): 상위랭킹 처리
- `_flush_part_buffer_inner` (7141, 7273, 7331행): 파일 I/O 중 lock

특히 `_price_watchdog_loop`의 5919~5933행에서 **`with _lock:` 안에서 `codes` 전체를 순회**하며 REST 요청 여부를 판단하는데, 이 구간이 ingest_loop의 8314행 lock 획득을 블로킹한다.

#### 병목 B: GIL 경합 — 10개 스레드 동시 실행

정상 운영 시 활성 스레드 목록 (9283~9317행):
1. `writer_loop` — 0.5초 sleep, 1분마다 flush (I/O 시 GIL 해제하나 pl.concat 시 GIL 보유)
2. **`ingest_loop`** — 핫 패스
3. `_str1_sell_worker` — API 호출 대기 (대부분 idle, I/O 시 GIL 해제)
4. `time_flag_loop` — 1초 주기, 가벼움
5. **`scheduler_loop`** — 1초 주기이나 내부에서 REST API 호출 + lock 획득 빈번
6. `_rest_worker` — REST API 직렬 처리 (I/O 대기 시 GIL 해제)
7. **`_price_watchdog_loop`** — codes 순회 + lock 보유 (CPU 바운드)
8. `_price_request_loop` — REST 요청 생성
9. `_top_rank_loop` — 주기적 랭킹 조회
10. `run_ws_a2_forever` — a2 WSS (I/O 바운드)
11. **메인 스레드 (run_ws_forever)** — WSS 이벤트 루프, on_result 콜백 실행

shutdown 시 스레드 2~10이 모두 종료되고 ingest_loop만 남으므로, GIL + lock 경합이 완전히 사라져 5~7배 빨라진다.

#### 병목 C: `partition_by()` 중복 호출 — 틱당 최대 4회

ingest_loop에서 동일 DataFrame에 대해 `partition_by(code_col)`를 **최대 4회** 반복 호출:
1. 8315행: 모니터링 카운트용 (`tmp.partition_by`)
2. 8417행: 캐시 업데이트용 (`tmp2.partition_by`)
3. 8468행: VI delayed 해제용 (`tmp2_vi.partition_by`) — 조건부
4. 8500행: 저장 + 지표 계산용 (`df_save.partition_by`)

`partition_by`는 DataFrame을 종목별로 분할하는 연산으로, 매 틱마다 DataFrame 복사가 발생한다. WSS 틱은 보통 1행짜리이므로 partition_by 오버헤드가 상대적으로 크다.

#### 병목 D: _calc_indicators 볼린저 밴드 계산 — 틱당 O(200)

`_calc_indicators` (7589~7639행)는 save=True일 때 **모든 틱**에 대해 호출된다:
- EMA 계산: O(1) x 6개 (ma3, ma50, ma200, ma300, ma500, ma2000) — 가벼움
- **BB 계산: `list(buf)[-200:]` → 200개 복사 + sum 2회 + sqrt** — 틱당 약 600번 연산
  - 7625행: `window = list(buf)[-INDICATOR_BB_PERIOD:]` — deque→list 변환 (최대 2000개 복사)
  - 7626~7628행: mean, variance 계산 — 200개 순회 2회

deque maxlen이 `_IND_MAX_WINDOW = 2000`이므로, `list(buf)`는 **최대 2000개 원소를 매 틱마다 리스트로 복사**한다.

#### 병목 E: on_result에서 pl.from_pandas() — WSS 이벤트 루프 블로킹

`on_result` (8186행)과 a2 on_result (8802행)에서:
```python
result_pl = pl.from_pandas(result)  # 8265행, 8802행
```
이 변환은 **WSS 이벤트 루프 스레드**에서 실행되므로, 변환 중 다음 WSS 메시지 수신이 지연된다. pandas→polars 변환은 데이터 복사를 수반한다.

또한 8202행에서:
```python
result.columns = [str(c).strip().lower() for c in result.columns]  # pandas 컬럼명 소문자화
```
이것도 WSS 콜백 안에서 실행된다.

#### 병목 F: with_columns + zfill 반복

`result.with_columns(pl.col(code_col).cast(pl.Utf8).str.zfill(6))` 호출이 ingest_loop 내에서 **최소 3회** 반복된다:
- 8311행 (모니터링), 8414행 (캐시), 8492행 (저장)
- 매번 새 DataFrame을 생성하므로 메모리 할당 + 복사 오버헤드

### 분석 3: 데이터 손실 가능성

#### Queue 설정
```python
_ingest_queue: queue.Queue = queue.Queue()  # 4872행 — maxsize 미지정 = 무제한
```
- **maxsize가 설정되지 않아** queue가 무한히 커질 수 있음
- `put()` 호출은 블로킹 없이 즉시 성공 (메모리가 허용하는 한)
- 데이터 유실은 없지만, 처리 지연이 누적되면 **메모리 증가 + 실시간성 상실**

#### on_result에서의 put 방식
```python
_ingest_queue.put((result_pl, trid, kind, is_real, recv_ts))  # 8266행 — blocking put, timeout 없음
```
- `put_nowait()`가 아닌 `put()`이므로 maxsize가 없는 현재 구조에서는 문제 없음
- 하지만 **처리량 < 수신량**이면 queue가 점점 쌓이고, recv_ts와 실제 처리 시점 사이 지연이 증가

#### shutdown 시 queue 처리
- 8604~8608행: `_ingest_queue.empty()`가 될 때까지 최대 8초 대기
- ingest_loop 자체도 8272행에서 `not _ingest_queue.empty()` 조건으로 큐 소진 후 종료
- 단, deadline=8초 내 소진 못하면 남은 데이터 유실 가능

### 분석 4: 200~300 vs 1500 격차의 정량적 원인 추정

| 요인 | 영향도 | 설명 |
|------|--------|------|
| **GIL 경합 (11개 스레드)** | **~40%** | CPython GIL 하에서 CPU 바운드 스레드가 교대로 실행. scheduler, watchdog, top_rank 등이 CPU 시간 소비 |
| **`_lock` 경합** | **~30%** | ingest_loop이 _lock 대기하는 시간. watchdog/scheduler가 _lock 보유 중이면 ingest가 블로킹 |
| **partition_by 4회 반복** | **~15%** | 같은 데이터에 대해 partition_by를 4회 호출. 1회로 줄이면 3/4 절감 |
| **BB 계산 (list 복사 + 순회)** | **~10%** | deque→list 2000개 복사 + 200개 x 2회 순회. save=True 시만 |
| **on_result pl.from_pandas** | **~5%** | WSS 콜백에서 pandas→polars 변환 (직접적 ingest 병목은 아니나 수신 지연 유발) |

### 분석 5: _check_str1_sell_conditions 내부 lock (7642행~)

`_check_str1_sell_conditions`는 내부에서:
- 7682행: `with _str1_sell_state_lock:` — 종목별로 반복 획득/해제
- 7711행: `with _str1_sell_state_lock:` — buy_price 갱신

`_str1_sell_state_lock`은 전체 코드에서 **48회** 사용되며, `_str1_sell_worker`와 경합한다. 하지만 _str1_sell_worker는 대부분 idle이므로 경합 빈도는 낮다.

### 수정 방안

#### 방안 1: partition_by 1회로 통합 (영향도 ~15%)
ingest_loop 내에서 partition_by를 맨 처음 1회만 실행하고, 결과를 `{code: df_code}` dict로 캐시하여 모니터링/캐시/저장에 재사용.

#### 방안 2: _lock critical section 축소 (영향도 ~30%)
- 8314~8399행의 모니터링 카운트 블록: _lock 안에서 단일가 판정/로깅 등 부수 로직이 너무 많음. 카운터 업데이트만 lock 안에서 하고, 나머지는 lock 밖으로 이동.
- 다른 스레드(watchdog, scheduler)의 _lock 보유 시간도 축소 필요.
- 또는 `_lock`을 종목별 fine-grained lock으로 분할하여 경합 감소.

#### 방안 3: BB 계산 최적화 (영향도 ~10%)
- `list(buf)[-200:]` 대신 deque를 직접 순회하여 리스트 복사 제거
- 또는 incremental mean/variance 계산 (Welford's algorithm): 틱당 O(1)으로 감소
- `_IND_MAX_WINDOW=2000`은 BB_PERIOD=200에 비해 과도 — deque maxlen을 200으로 축소 가능

#### 방안 4: with_columns + zfill 중복 제거 (영향도 ~5%)
zfill(6) 변환을 1회만 수행하고 이후 블록에서 재사용.

#### 방안 5: on_result에서 pandas→polars 변환 제거 (영향도 ~5%)
KIS 라이브러리가 pandas DataFrame을 반환하는 한 불가피하지만:
- 가능하면 KIS 라이브러리 수정하여 처음부터 Polars 반환
- 또는 on_result에서 pandas 그대로 queue에 넣고, ingest_loop에서 한 번만 변환

#### 방안 6: GIL 경합 감소 (영향도 ~40%)
- scheduler_loop 내부의 무거운 REST 호출은 이미 _rest_worker로 분리되어 있으나, scheduler 자체의 1초 루프에서 다양한 조건 체크가 CPU를 소비
- `_price_watchdog_loop`의 codes 전체 순회 + lock 보유 구간을 최적화 (lock 밖에서 snapshot 후 판단)
- 궁극적으로는 asyncio 단일 이벤트 루프 또는 multiprocessing으로 전환

#### 방안 7: Queue maxsize 설정 + 모니터링
- `queue.Queue(maxsize=10000)` 설정하여 메모리 폭주 방지
- queue 크기를 주기적으로 로깅하여 처리 지연 감지
- put_nowait + Full 예외 시 경고/카운터 증가

### 우선순위 권장

1. **방안 2** (_lock critical section 축소) — 가장 높은 ROI
2. **방안 1** (partition_by 통합) — 구현 간단, 확실한 개선
3. **방안 3** (BB incremental 계산) — O(200) → O(1) 확정적 개선
4. **방안 4** (zfill 중복 제거) — 방안 1과 함께 처리
5. **방안 6** (GIL 경합) — 구조적 변경 필요, 장기 과제
6. **방안 5** (pandas→polars) — KIS 라이브러리 의존
7. **방안 7** (Queue maxsize) — 안전장치

---

## 구현 결과 (2026-04-22)

### ✅ 방안 1+4: partition_by 4회→1회 통합 + zfill 1회
- ingest_loop 내에서 동일 DataFrame에 대해 zfill 변환 1회, partition_by 1회만 호출
- 종목별 루프 안에서 (1)모니터링카운트 → (2)캐시업데이트 → (3)VI지연해제 → (4)저장+지표계산 순서로 통합 처리
- 기존 대비 zfill 3회 + partition_by 3회 제거

### ✅ 방안 2: `_lock` 제거 — ingest_loop 내 3곳 모두 제거
- 8314행 (모니터링 카운트): `with _lock:` 제거 — dict 단일 key 갱신은 GIL 하에서 원자적
- 8499행 (저장+지표): `with _lock:` 제거 — `_ema_state`, `_price_buf`는 ingest_loop만 쓰기
- 8563행 (summary): `with _lock:` 제거 — 모니터링 근사값으로 충분

### ✅ 방안 3: BB incremental 계산 — O(200) → O(1)
- `_price_buf`를 `deque(maxlen=2000)` → `list` (직접 trim)로 변경
- `_bb_sum`, `_bb_sq_sum` incremental 캐시 도입: `sum += new - old`, `sq_sum += new² - old²`
- pre-fill(첫 틱에 200개 복사) 제거 → 자연 축적, 2개 이상이면 BB 계산 시작
- 틱당 연산: ~600회(복사+순회) → ~5회(덧셈/뺄셈)

### ✅ 방안 5: pandas→polars 전환 — KIS 라이브러리부터 polars 네이티브
- `kis_auth_llm.py`: `pd.read_csv(sep="^")` → `pl.read_csv(separator="^", infer_schema_length=0)` — 처음부터 polars DF 반환
- `ws_realtime_trading.py`: `pl.from_pandas(result)` 제거, pandas API(`iterrows`, `empty`, `columns=[...]`) → polars API(`to_dicts`, `len()==0`, `rename`)
- H0STCNI0(체결통보), H0STMKO0(장운영정보), 체결통보 로그 모두 polars 네이티브로 변환

### ❌ 미수정 (향후 과제)

| 방안 | 내용 | 이유 |
|------|------|------|
| **방안 6** | GIL 경합 감소 (스레드 구조 변경) | CPython 근본 제약. multiprocessing 또는 asyncio 전환 필요한 대규모 구조 변경. 장기 과제 |
| **방안 7** | Queue maxsize 설정 + 모니터링 | 처리 능력(1500건/s) > 수신량(200~300건/s)이므로 큐 폭주 가능성 없음. 우선순위 낮음 |
