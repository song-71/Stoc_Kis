# Stoc_Kis 프로그램 개선 이력

---

## [2026-04-07] 5596e38

### 1. fix: 보유종목 없을 때 WSS 연결 건너뛰는 버그 수정
- **카테고리**: fix
- **파일**: `ws_realtime_trading.py`, `ws_monitoring_research_260407.md`
- **사유**: 260403(수), 260406(월) 양일 WSS 실시간 데이터 0건 수신 현상 원인 분석 결과, `run_ws_forever()` 내 "보유종목 없으면 WSS 구독 생략 → 30분 sleep" 블록이 장 시작 시 보유종목이 없을 경우 WSS 연결 자체를 건너뛰도록 동작하는 것이 근본 원인으로 확인됨. 보유종목이 없어도 장중 신규 VI 매수 체결을 받아야 하므로 WSS는 항상 유지되어야 함
- **주요 변경**:
  1. **`run_ws_forever()` 내 조건 블록 제거** (기존 line 8648~8667): `_has_held` 판단 → WSS 구독 생략 → `time.sleep(1800)` 루프 전체 삭제
  2. 이제 `RunMode.EXIT`가 아닌 이상 보유종목 유무와 관계없이 항상 WSS 연결 시도
- **영향**: 장 시작 시 보유종목이 없어도 WSS가 정상 연결되어 VI 실시간 체결 수신 가능. 260403·260406 재발 방지

---

## [2026-04-06] 5f928a5

### 1. feat: NXT 애프터마켓 대상 없을 시 조기 종료 옵션 추가 (NXT_AFTER_EARLY_EXIT)
- **카테고리**: feat
- **파일**: `ws_realtime_trading.py`
- **사유**: 상한가 종목이 없고 보유종목도 없는 날 NXT 애프터마켓(15:40~20:00)을 불필요하게 대기하는 낭비를 제거. 조기 종료 옵션으로 즉시 정리 프로세스를 실행할 수 있도록 함
- **주요 변경**:
  1. **`NXT_AFTER_EARLY_EXIT` 옵션 추가** (상단 상수 영역): True이면 애프터마켓 진입 시 대상 0종목일 경우 즉시 종료 프로세스 실행, False이면 20:00까지 대기 (기존 동작)
  2. **`_nxt_after_early_exit` 전역 플래그 추가**: `_log_mode_transition()` NXT_AFTER 분기에서 대상 0종목 + 옵션 활성 시 True로 설정하고 텔레그램 통보
  3. **`_run_exit_shutdown(reason)` 함수 신설**: EXIT 및 조기 종료 공통 종료 프로세스를 단일 함수로 통합 (단일가 집계 → WS 닫기 → flush → parts 병합 → 거래장부 갱신 → 최종 요약 → 텔레그램 종료 안내 → `_stop_event.set()`)
  4. **`scheduler_loop()` NXT_AFTER 진입 직후**: `_nxt_after_early_exit` 플래그 감지 시 `_run_exit_shutdown()` 즉시 호출 후 루프 탈출
  5. **기존 EXIT 인라인 종료 블록 제거**: `_run_exit_shutdown("20:00 NXT 애프터마켓 종료")` 단일 호출로 대체
- **영향**: 애프터마켓 대상이 없는 날 20:00까지 프로세스가 idle 상태로 유지되던 문제 해결. 종료 로직 중복 제거로 코드 유지보수성 향상

---

## [2026-04-06] 1072143

### 1. fix: NXT 1분봉 개장 전 더미 데이터 저장 방지 + crontab 스케줄 수정
- **카테고리**: fix
- **파일**: `kis_1m_API_to_Parquet_NXT.py`, crontab
- **사유**: 4.6(월) KST 06:30에 crontab이 NXT 다운로드를 실행했으나, NXT 프리마켓(08:00) 전이라 KIS API가 전일종가 기반 더미 데이터(volume=0, O=H=L=C)를 반환하여 빈 20260406 파일이 생성됨
- **주요 변경**:
  1. **volume=0 가드 추가**: 병합 후 저장 전 전체 volume 합계=0이면 저장 스킵 + 경고 로그
  2. **crontab 시간 변경**: `30 21 * * 0-4` (KST 06:30) → `01 11 * * 1-5` (KST 20:01, NXT 애프터마켓 종료 직후)
  3. **crontab 중복 제거**: 동일 NXT 명령이 `&nohup`으로 2번 실행되던 문제 수정
  4. **잘못된 파일 삭제**: `20260406_1m_chart_DB_parquet_NXT.parquet` 삭제
- **영향**: NXT 거래 없는 시간대에 빈 parquet 파일이 생성되는 문제 방지. 애프터마켓 종료 후 정상 수집

---

## [2026-04-06] 2177ea8

### 1. feat: NXT 프리마켓 로그 상세화 + 보유종목 NXT 구독 추가
- **카테고리**: feat
- **파일**: `ws_realtime_trading.py`
- **사유**: NXT 프리/애프터마켓 시작 시 로그가 종목명 나열만 하여 가독성이 낮고, 보유종목이 NXT 대상에 포함되지 않아 프리마켓 중 보유종목 가격 감시가 불가능했음
- **주요 변경**:
  1. **`_nxt_held_codes` / `_nxt_target_info` 전역변수 추가**: 보유종목 코드셋과 종목별 메타(이름·전일종가·등락률·출처) 저장
  2. **`_run_balance_0758()` 신설**: 07:58 구간 1회 잔고조회로 NXT 프리마켓 시작 전 보유종목을 `_nxt_held_codes`에 저장
  3. **`scheduler_loop()` 07:58 구간 추가**: `_run_balance_0758()` 호출 후 `_nxt_held_codes` 갱신 및 로그 출력
  4. **`_load_nxt_target_codes_pre()` 개선**: 상한가(nxt=Y) 종목 정보를 `_nxt_target_info`에 저장하고, `_nxt_held_codes`를 합산하여 `_nxt_target_codes` 구성
  5. **`_refresh_nxt_target_codes_after()` 개선**: 동일 패턴으로 `_nxt_target_info` 갱신 + 보유종목 합산
  6. **`_log_mode_transition(NXT_PRE)` 개선**: 상한가 종목(전일종가·등락률 포함)과 보유종목을 구분하여 멀티라인 출력
  7. **`_log_mode_transition(NXT_AFTER)` 개선**: 동일 패턴으로 애프터마켓 로그 상세화
- **영향**: NXT 프리/애프터마켓 시작 시 어떤 종목이 왜 구독되는지 명확히 파악 가능. 보유종목이 NXT 대상에 포함되어 프리마켓 중 가격 변동 감시 가능

---

## [2026-04-04] 4671c78

### 1. feat: VI/사이드카/써킷브레이크 이벤트 정보를 parquet 컬럼으로 저장
- **카테고리**: feat
- **파일**: `ws_realtime_trading.py`, `ws_monitoring_research_260404.md` (신규)
- **사유**: 실시간 VI 발동/해제 시각 및 마켓 전체 이벤트(사이드카/써킷브레이크)를 parquet에 기록하여 백테스트·분석에서 이벤트 구간을 식별할 수 있도록 함
- **주요 변경**:
  1. **메모리 변수 6개 추가**: `_vi_start_ts`, `_vi_end_ts` (종목별 VI 발동·해제 ISO timestamp), `_code_market_map` (code→KOSPI/KOSDAQ), `_market_wide_event`, `_market_wide_mkop`, `_market_wide_start_ts` (마켓 전체 이벤트 전파용)
  2. **symbol_master 초기화**: `code→market` 매핑(`_code_market_map`) 기동 시 로드
  3. **`_on_market_status_krx`**: `EXCH_CLS_CODE` 파싱으로 마켓 식별; 사이드카/써킷 발동 시 `_market_wide_*` 전파, 해제 시 클리어; VI 발동·해제 시각 `_vi_start_ts`/`_vi_end_ts` 기록
  4. **`_vi_poll_check` / `_vi_poll_expire_check`**: REST 폴링 경로에서도 VI 발동·해제 시각 기록
  5. **WSS `ingest_loop`**: `vi_yn`, `vi_start_ts`, `vi_end_ts`, `market_event`, `new_mkop_cls_code` 5개 컬럼을 parquet에 추가
  6. **REST `_enqueue_rest_price_row`**: 동일 5개 컬럼 추가, `market_wide` 데이터 우선 적용 후 종목별 값 fallback
- **영향**: 모든 1분봉 parquet에 VI/마켓 이벤트 컬럼이 포함되어 사후 분석 시 VI 발동 구간, 사이드카/써킷 발동 시간대를 직접 조회 가능

---

## [2026-04-02] 8885254

### 1. feat: NXT 프리마켓/애프터마켓 1분봉 다운로드 스크립트 추가
- **카테고리**: feat
- **파일**: `kis_1m_API_to_Parquet_NXT.py` (신규)
- **사유**: 넥스트레이드(NXT) 프리마켓(08:00~08:50)과 애프터마켓(15:40~20:00) 구간의 1분봉 OHLCV 데이터를 KRX 정규장 데이터와 분리 수집할 필요. 기존 `kis_1m_API_to_Parquet_all_code.py`는 J(KRX) 마켓 전용이므로 NXT 전용 스크립트를 별도 신설
- **주요 변경**:
  1. **MARKET_CODE="NX"** — `inquire_time_itemchartprice` 호출 시 `fid_cond_mrkt_div_code="NX"` 고정
  2. **QUERY_TIMES 분리** — 프리마켓 08:00~08:50, 애프터마켓 15:40~20:00 구간을 30분 단위로 분할 조회
  3. **`_in_nxt_time()` 필터** — API 응답 중 NXT 시간 범위 외 캔들 제거 (정규장 데이터 혼입 방지)
  4. **출력 파일명** — `{date}_1m_chart_DB_parquet_NXT.parquet` (기존 `_all_code` 파일과 완전 분리)
  5. **S3 경로 분리** — `layout.s3_1m_date()`의 `/1m/` 경로를 `/1m_nxt/`로 치환하여 업로드
  6. **temp 디렉토리** — `temp_1m_nxt/` 사용 (기존 `temp_1m/`과 충돌 없음)
  7. **로컬 NXT 파일 정리** — `*_NXT.parquet` 최근 2일치만 유지, 이전 파일 자동 삭제
  8. **체크포인트** — `temp_1m_nxt/checkpoints/{date}_nxt_checkpoint.csv`로 중단/재개 지원
- **영향**: NXT 거래 시간대 1분봉 데이터를 독립 parquet 파일 및 S3 경로(`market_data/1m_nxt/`)에 저장. `Daily_fetch_auto_run.sh`에 실행 구문 추가 시 자동화 완성

---

## [2026-04-02] a07e6e2

### 1. fix: NXT 재시작 시 대상 종목 비어있는 버그 수정
- **카테고리**: fix
- **파일**: `ws_realtime_trading.py`
- **사유**: 프로세스 재시작 시 `_last_prdy_ctrt`가 전부 0.0으로 초기화되어 당일 상한가 기준 필터(>=29.5%)를 통과하는 종목이 없어지면서 `_nxt_target_codes`가 빈 set으로 확정되던 문제 수정
- **주요 변경**:
  1. **_refresh_nxt_target_codes_after() 폴백 추가** — `candidates`(당일 상한가 후보)가 비어있으면 빈 set을 반환하던 기존 동작 대신 `_load_nxt_target_codes_pre()`(전일 상한가 CSV)를 호출해 대상을 복원
  2. **_log_mode_transition NXT_AFTER 자동 갱신** — `_nxt_target_codes`가 비어있을 경우 `_refresh_nxt_target_codes_after()` 자동 호출 후 텔레그램 알림 출력
- **영향**: NXT 애프터마켓 진입 시 재시작 여부와 무관하게 대상 종목이 정상 설정됨. 재시작 직후 NXT_AFTER 모드에 진입해도 구독/매수 대상이 비어있지 않음

---

## [2026-04-02] b54e369

### 1. feat: NXT(넥스트레이드) 틱데이터 WSS 수신 1단계 구현
- **카테고리**: feat
- **파일**: `ws_realtime_trading.py`
- **사유**: 넥스트레이드(NXT) 프리마켓(08:00~08:50)과 애프터마켓(15:40~20:00) 틱데이터 수신 기반 마련. 한국거래소(KRX) 외 NXT 거래소 종목의 실시간 체결가를 WSS로 수신하여 parquet 저장까지 1단계 구현
- **주요 변경**:
  1. **RunMode 추가** — `NXT_PRE`(08:00~08:50 프리마켓), `NXT_AFTER`(15:40~20:00 애프터마켓) 신규 추가. 기존 OVERTIME_EXP/OVERTIME_REAL은 NXT_AFTER로 대체
  2. **END_TIME 연장** — 18:00 → 20:01 (NXT 애프터마켓 20:00 종료 + 1분 여유)
  3. **calc_mode() 개편** — NXT 시간대 분기 추가. 15:30~15:40 CLOSE_REAL, 15:40~20:00 NXT_AFTER, 20:00 이후 EXIT
  4. **KNOWN_TRID_MAP 추가** — `H0NXCNT0`(nxt_real 실시간체결), `H0NXANC0`(nxt_exp 예상체결) 등록
  5. **_desired_subscription_map()** — NXT 모드에서 `ccnl_nxt(H0NXCNT0)`로 NXT 대상 종목 구독. 기존 overtime_ccnl_krx 구독 제거
  6. **_load_nxt_target_codes_pre()** — 07:50 실행. `Select_Tr_target_list.csv` 전일 상한가 중 `KRX_code.csv`의 `nxt=Y` 종목 필터링
  7. **_refresh_nxt_target_codes_after()** — 15:30 실행. `_last_prdy_ctrt >= 29.5%`(당일 상한가)이면서 `nxt=Y`인 종목으로 갱신. `_nxt_target_codes` 전역변수에 반영
  8. **ingest_loop parquet 저장** — `kind in ("nxt_real", "nxt_exp")`이면 `save=True`로 항상 저장
  9. **run_ws_forever WSS 유지** — NXT 모드에서 보유종목이 없어도 `_nxt_target_codes`가 있으면 WSS 연결 유지
  10. **시간외단일가 텔레그램 알림 제거** — `_run_overtime_buy_orders()`의 비활성화 알림을 `_notify(tele=True)` → `logger.info`로 전환 (이전 커밋 보완)
  11. **EXIT 처리 문구 수정** — "18:00 이후 시간외 단일가 종료" → "20:00 NXT 애프터마켓 종료"
- **영향**: NXT 프리/애프터마켓 틱데이터 수신 및 parquet 저장 가능. 운영 시간이 20:01까지 연장되어 전체 장 커버리지 확대. NXT 매수/매도 로직은 미구현(2단계 예정)

---

## [2026-04-02] 45ac0b8

### 1. refactor: 상한가 종목 장전/장후 시간외 매수 로직 비활성화
- **카테고리**: refactor
- **파일**: `ws_realtime_trading.py`
- **사유**: 상한가 종목은 매도 물량이 나오지 않다가 하락 시 손실 위험. 장전시간외종가(08:30)와 시간외단일가(16:00~18:00) 매수를 모두 비활성화. 코드는 주석으로 보존하여 전략 재활성화 시 즉시 복원 가능
- **주요 변경**:
  1. **`_run_morning_extra_closing_buy()`** — 08:30 장전시간외종가 매수 루프 전체 주석 처리. 스킵 알림 메시지(`[08:30시간외종가] 매수 비활성화`)는 유지하여 실행 여부를 로그에서 확인 가능
  2. **`_run_overtime_buy_orders()`** — 16:00~18:00 시간외단일가 매수 루프 전체 주석 처리. 스킵 대상 종목명/코드 목록을 텔레그램으로 알림
- **영향**: 상한가 추종 매수 전략 완전 중단. 기존 구독/모니터링 로직에는 영향 없음. 주석 해제만으로 전략 재활성화 가능

---

## [2026-04-02] 14594bf

### 1. feat: FHPST01390000 REST 폴링 기반 전체 시장 VI 감지 추가
- **카테고리**: feat
- **파일**: `ws_realtime_trading.py`, `ws_monitoring_research_260402-2.md`
- **사유**: WSS(H0STMKO0)는 구독 슬롯 한계(40개)로 전체 시장 VI를 감지할 수 없음. WSS 미수신 구간에서도 비구독 종목의 VI 발동을 감지하기 위해 REST API 폴링 방식을 추가 구현. 또한 a2 계좌 별도 WSS 연결 검토를 위한 TODO 추가 (고객센터 H0STMKO0 슬롯 카운트 확인 대기 중)
- **주요 변경**:
  1. **`_vi_poll_check()` 신규 함수** — FHPST01390000 API로 전체 종목 VI 현황 1회 조회. `_price_watchdog_loop`에서 WSS 5초 이상 전체 미수신 시 호출. 5초 이내 재호출 방지(중복 호출 차단)
  2. **신규 VI 발동 감지 → 예상체결가 전환** — API 응답에서 vi_cls_code가 Y/1/2/3인 종목 추출. 구독 중인 종목이면 `_vi_exp_sub_add()` 호출로 즉시 예상체결가 전환. 비구독 종목은 모니터링 로그만 출력
  3. **발동시각+2분 자동 해제** — `_vi_poll_expire_check()` 신규 함수. `_VI_DURATION_SEC=120`초 경과 시 `_vi_exp_sub_unsub()` 호출로 실시간체결가 복귀
  4. **REST 응답 소멸 종목 즉시 해제** — 이전 폴링에 있던 종목이 다음 응답에서 사라지면(해제 확정) `_vi_poll_active_codes`에서 제거 후 구독 복귀
  5. **TODO 주석 추가** — H0STMKO0가 WSS 구독 40슬롯에 포함되는지 고객센터 확인 중. 결과에 따라 kis_auth_llm.py 인스턴스별 open_map/approval_key 분리 검토
- **영향**: WSS 구독 슬롯 외 전체 종목에 대한 VI 발동을 REST 폴링으로 보완. WSS 연결 단절 구간에서도 VI 감지 지속 가능

---

## [2026-04-02] 7feecd3

### 1. fix: VI 예상체결가 타임아웃 단축 + 해제 시 실시간체결가 즉시 전환
- **카테고리**: fix
- **파일**: `ws_realtime_trading.py`
- **사유**: VI 발동~해제 통상 소요 시간이 2:03~2:20초임을 확인. 기존 타임아웃 300초는 과도하여 150초(2분30초)로 단축. 또한 VI 해제 수신 시 실시간 체결가 구독이 즉시 복원되지 않아 일시적 데이터 공백이 발생하는 문제 수정
- **주요 변경**:
  1. **예상체결가 자동해제 타임아웃 단축** — `_price_watchdog_loop`의 fallback 타임아웃 300초 → 150초로 변경. H0STMKO0 해제 통보 미수신 시에도 2분30초 내 자동 해제
  2. **VI 해제 후 실시간체결가 즉시 전환** — `_vi_exp_sub_worker`에서 tr_type="2"(해제) 처리 후 REGULAR_REAL 모드이고 구독 대상 종목이면 즉시 ccnl_krx(H0STCNT0) 재구독. VI 해제~실시간체결가 수신 사이 공백 제거
  3. **name 변수 위치 이동** — try 블록 밖으로 이동하여 except 분기에서도 종목명 로그 출력 가능하도록 정리
- **영향**: VI 해제 후 실시간 체결가 복원 지연 없음. 예상체결가 fallback 타임아웃이 줄어 비정상 구독 잔존 시간 단축

---

## [2026-04-02] ae92c53

### 1. fix: VI감지/구독/장운영 안정성 6건 개선
- **카테고리**: fix
- **파일**: `ws_realtime_trading.py`, `ws_monitoring_research_260402-2.md`
- **사유**: 260402 모니터링 리서치(Issue 4~8)에서 발견된 VI 감지 오동작, 데드락, 구독 누락, 종가매매 WSS 조기종료, 사이드카/서킷브레이크 미감지, 사전구독해제 후 재구독 문제 6건을 일괄 수정
- **주요 변경**:
  1. **vi_cls_code 분기 수정** — `!= "0"` 비교를 `== "Y"` / `== "N"` 으로 교체. H0STMKO0 PDF 확인 결과 실제 값은 Y/N (숫자 아님). 미정의 값은 별도 info 로그로 처리
  2. **_vi_exp_sub_add/unsub 데드락 해결** — asyncio 이벤트 루프 내 on_result 콜백에서 `_kws_lock` 직접 취득 시 데드락 발생. `_vi_exp_sub_worker()` 함수 신규 추가, 구독/해제를 `threading.Thread(daemon=True)`로 위임하여 해결
  3. **_desired_subscription_map에 _vi_active_codes 반영** — VI 발동 중 종목(`_vi_active_codes`)이 구독맵에서 누락되어 WSS 재연결 시 예상체결가 구독이 사라지는 문제 수정. `vi_codes = (_vi_delayed_codes | _vi_active_codes) & all_codes`
  4. **run_ws_forever 종가매매 구간 WSS 유지** — 보유종목이 없을 때 WSS를 즉시 종료하던 로직에서, `15:20~15:31` 또는 `_closing_codes` 존재 시 예외 처리 추가. 종가매매 예상체결가 모니터링 지속
  5. **H0STMKO0 mkop_cls_code 처리 추가** — 사이드카(187/388/397/398), 서킷브레이크(174/175/182/184/185), 임시정지(164) 코드 감지 시 `logger.warning` + 텔레그램 알림. 해제 코드 수신 시 `_market_event` 자동 클리어. `_enqueue_rest_price_row`에 `new_mkop_cls_code`, `market_event` 컬럼 추가
  6. **15:19~15:20 사전구독해제 후 재구독 방지** — `_pre_unsub_closing_done=True` 이후 `RunMode.REGULAR_REAL` 상태에서 scheduler가 구독을 재적용하는 문제 차단
- **영향**: VI 발동/해제가 정확히 감지되고 예상체결가 구독이 안정적으로 유지됨. 사이드카/서킷브레이크 등 장운영 이벤트 텔레그램 알림 추가. 종가매매 구간 모니터링 안정성 향상

---

## [2026-03-31] ed281b2
- **Category**: fix
- **Title**: 종가매수 주문 로그 즉시 출력 + 일일 리뷰 로그 순서 역전 감지 추가
- **Files**: `ws_realtime_trading.py`, `ws_log_monitor.py`
- **Changes**:
  1. `ws_realtime_trading.py`: `_run_closing_buy_orders` 내 `deferred_logs` 방식 폐기
     → `as_completed` 루프 내에서 `_notify` 즉시 호출, 체결통보 WSS 콜백보다 주문send 로그가 먼저 기록되도록 출력 시점 수정
     → ledger 기록(`_append_ledger`)만 `ledger_records`로 후처리 유지, 병렬전송 구조 변경 없음
  2. `ws_log_monitor.py`: 일일 리뷰 프롬프트에 "로그 순서 역전 감지" 분석 항목 추가
     → 주문send/체결통보 등 논리적 선후관계가 뒤바뀐 로그 쌍을 식별하고 원인/개선방안 제시 요청
- **Impact**: 종가매수 주문 로그가 체결통보보다 먼저 기록되어 로그 흐름의 논리적 순서 복원, 일일 분석 품질 향상

---

## [2026-03-31] 6eb749c
- **Category**: fix
- **Title**: 260331 모니터링 이슈 3건 개선
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. `_send_subscribe` except절에 종목코드/이름 추가 — 구독 실패 시 어떤 종목인지 로그에 기록
  2. `ingest_loop` partition_by 루프에 `None`/`000000` 코드 필터링 추가 — mkop변경 처리 중 유령 코드 유입 방지
  3. `_rest_fail_backoff.pop` 시 REST 복구 확인 로그 추가 — 거래정지 의심 해제 후 복구 추적 가능
- **Impact**: 구독 실패/REST 복구 디버깅 가능성 향상, `mkop변경 None(None)` 유령 로그 제거

---

## 2026-03-31

### 1. analysis: 거래 로그 정밀 분석 (wss_TR_260331.log)
- **파일**: `out/monitor/review_260331.md` (신규)
- **분석 대상**: `wss_TR_260331.log` (18,305줄)
- **발견 이슈**:
  1. REST RemoteDisconnected ×1, Read Timeout ×1, WS send timeout ×1 — 모두 자동 복구 확인
  2. 구독 해제 close frame 경합 ×3 (15:20 종가전환 시점)
- **LOG_WEAK 발견**:
  1. `mkop변경 None(None)` 유령 로그 — ws_realtime_trading.py:7718 필터링 필요
  2. 구독 실패 시 종목코드 미기록 — 디버깅 불가
  3. REST 실패 복구 확인 로그 부재
- **코드 개선 방안 4건 제안**:
  1. mkop None 필터링 (ws_realtime_trading.py:7718)
  2. 구독 실패 로그에 종목코드 추가
  3. REST 실패 복구 로그 추가
  4. 15:20 종가전환 WS close 경합 방지
- **종합**: CRITICAL 에러 없음, 종가매수 8건 정상, 시간외 1건 체결, 691,522행 데이터 정상
- **영향**: 4건의 코드 개선 방안이 도출되어 향후 로그 품질 및 안정성 향상 예정

---

## [2026-03-30] de281f6
- **Category**: feat
- **Title**: 실시간 거래 로그 모니터링 스크립트 추가 (log_monitor.py)
- **Files**: `log_monitor.py` (신규), `.claude/agent-memory/changelog-manager/MEMORY.md` (신규), `.claude/agent-memory/changelog-manager/feedback_changelog_file.md` (신규)
- **Changes**:
  - `tail -F`로 `wss_TR_{yymmdd}.log` 실시간 감시
  - CRITICAL / MODERATE / LOG_WEAK 심각도 3단계 분류
  - CRITICAL 발생 시 즉시 텔레그램 알림 + Claude CLI로 긴급 수정안 파일 생성
  - MODERATE는 30분 배치 집계 후 텔레그램 + Claude 일일 수정안 생성
  - 18:10 자동으로 Claude가 전체 로그 정밀 분석 실행
  - 5분 윈도우 중복 억제로 알림 스팸 방지
  - 18:35 자동 종료, `--dry-run` 옵션으로 테스트 모드 지원
  - 수정안 및 리뷰 파일은 `out/monitor/` 디렉토리에 저장
- **Impact**: 장중 이상 로그 발생 시 자동으로 감지·분류·알림하여 수동 모니터링 부담 감소. CRITICAL 이슈는 즉시 수정안까지 자동 생성

---

## 2026-03-23

### 1. fix: PNL 계산 버그 수정
- **파일**: `ws_realtime_trading.py` (라인 1677, 1790 부근)
- **문제**: `calc_sell_pnl(ref_price, ref_price, qty)` — buy_price 대신 ref_price를 2번 전달하여 수수료+세금만큼 항상 마이너스 PNL 표시
- **수정**: `_str1_sell_state`에서 buy_price를 먼저 조회한 후 `calc_sell_pnl(buy_price, ref_price, qty)` 호출. 2곳(일반매도, 재주문) 모두 수정
- **영향**: 매도 체결 시 정확한 PNL/수익률 계산

### 2. fix: 중복 텔레그램 알림 제거
- **파일**: `ws_realtime_trading.py` (라인 2202~2203 부근)
- **문제**: 동일 매도체결 1건에 대해 `_on_result()` + `_on_ccnl_notice_filled()` 양쪽에서 텔레그램 발송 → 중복 알림
- **수정**: `_on_ccnl_notice_filled()` 내 `_notify(tele=True)` → `logger.info()` 변경
- **영향**: 텔레그램 중복 알림 제거, 로그는 유지

### 3. feat: 매도체결 알림에 수익률(PNL) 표시
- **파일**: `ws_realtime_trading.py` (라인 7237~7245 부근, `_on_result` 함수)
- **내용**: 매도체결(`seln=="01"/"1"`) 시 `_str1_sell_state`에서 buy_price 조회 → `calc_sell_pnl()` 호출 → 텔레그램 메시지에 `PNL≈+1,234원(+2.50%)` 형태 추가
- **영향**: 매도체결 시 실시간 수익률 확인 가능

### 4. feat: 외부주문 target_price에 "상한가"/"하한가" 키워드 지원
- **파일**: `ws_realtime_trading.py` (`_exec_external_order` 함수)
- **내용**: `target_price` 필드에 숫자 대신 `"상한가"` 또는 `"하한가"` 문자열을 입력하면 REST API(`inquire_price`)로 해당 종목의 `stck_mxpr`(상한가) 또는 `stck_llam`(하한가)을 자동 조회하여 지정가(00) 주문
- **사용 예시**: `{"symbol": "005930", "order": 1, "qty": 10, "target_price": "상한가"}`
- **영향**: 상한가/하한가 가격을 몰라도 키워드로 간편 주문 가능

---

## 2026-03-24

### 1. fix: 외부주문 시간 체크 추가 — 장 시간 전 주문 방지
- **파일**: `ws_realtime_trading.py` (`_exec_external_order` 함수)
- **문제**: 외부주문이 접수되면 시간 체크 없이 즉시 KIS API에 주문 전송 → 장 시간 전(예: 08:28)에 시장가 주문 시 거부됨
- **수정**: ord_dvsn별 주문 가능 시간대 체크 추가. 시간 미도래 시 `result="접수"` 유지하며 매 사이클 재시도
  - 시장가(01): 09:00~15:30
  - 지정가(00): 08:30~15:30 (동시호가 포함)
  - 장전시간외(05): 08:30~08:40
  - 장후시간외(06): 15:40~16:00
  - 시간외단일가(07): 16:00~18:00
- **영향**: 프로그램 시작 전에 외부주문을 미리 설정해도 적절한 시간까지 자동 대기 후 주문

### 2. fix(치명적): 매수 체결 시 매도전략 자동등록 + 실시간 구독 추가
- **파일**: `ws_realtime_trading.py` (`_on_ccnl_notice_filled` 함수, 라인 2226 부근)
- **문제**: 외부주문/장전시간외/장후시간외/시간외단일가 등 VI매수 이외의 경로로 매수된 종목이 `_str1_sell_state`에 등록되지 않아 트레일스톱/손절이 전혀 작동하지 않음. 또한 `codes` 리스트에 추가되지 않아 H0STCNT0 실시간 체결가 구독도 안 됨 (WSS 틱 0건 수신 확인됨)
- **실제 사례**: 192410(오늘이엔엠) 매수가 4,290원 → 고가 4,780원(+11.4%) → 저가 3,965원(-7.6%) — 트레일스톱 미발동
- **수정**: 체결통보(H0STCNI0) 매수체결 수신 시 일괄 처리:
  1. `_str1_sell_state[code]` 자동 등록 (미등록 시 신규, 기등록 시 수량 누적)
  2. `_ensure_code_structs([code])` + `_base_codes.add(code)` → `codes` 리스트 추가
  3. `_trigger_ws_rebuild()` → H0STCNT0 실시간 구독 즉시 갱신
- **영향**: 어떤 경로(종가/VI/외부/시간외 등)로 매수되든 체결통보 수신 시 자동으로 매도전략 등록 + 실시간 구독. 트레일스톱/손절 정상 작동 보장

### 3. feat: 외부주문 디버그 로그 강화
- **파일**: `ws_realtime_trading.py` (`_check_external_orders`, `_exec_external_order`)
- **내용**: config.json에서 읽은 raw order dict를 접수 시점 + 실행 시작 시점에 로그 출력
- **영향**: ord_type/target_price/target 등 값 파싱 문제를 즉시 진단 가능

### 4. fix: 잔고조회 시 미구독 보유종목 자동 감지 + 구독 추가
- **파일**: `ws_realtime_trading.py` (`_run_closing_balance_verification` 함수)
- **문제**: 잔고에 보유 중이지만 `codes` 리스트에 없는 종목은 실시간 체결가(H0STCNT0) 구독 대상에서 제외 → 트레일스톱 미작동
- **수정**: 잔고조회 결과에서 `codes`에 없는 종목 감지 → `_str1_sell_state` 등록 + `_ensure_code_structs` + `_trigger_ws_rebuild()` → 실시간 구독 즉시 추가 + 텔레그램 알림
- **영향**: 어떤 경로로 매수되든 잔고조회 시점에 미구독 종목이 자동으로 구독에 포함

### 5. fix: "시작잔고 누락 보충" 로그에 종목 상세 추가
- **파일**: `ws_realtime_trading.py` (라인 3805 부근)
- **문제**: "3종목 발견"이라고만 표시되고 어떤 종목인지 알 수 없었음
- **수정**: 종목코드/종목명 목록을 로그 및 텔레그램에 포함
- **영향**: 로그만으로 어떤 종목이 누락 보충되었는지 즉시 확인 가능

### 6. fix: 30분 단일가 판정 — 첫 틱 즉시 확정 → 15초 관찰 후 확정으로 변경
- **파일**: `ws_realtime_trading.py` (라인 7455~7495 부근, scheduler_loop 라인 4165 부근)
- **문제**: KRX DB에 59(단기과열)로 등록된 종목이 Top30 추가 → 정시 윈도우에 첫 틱 도착 → 1틱만 보고 즉시 "30분 단일가 확정". 실제로는 매분 연속 틱이 오는 일반 연속 거래였음 (스코넥 276040 사례: 10:00~10:59 매분 2~4건 연속 틱)
- **수정**: `_single_price_observe` 딕셔너리 도입. 정시 윈도우 첫 틱 → 15초간 관찰 → 3건 이상 틱 수신 시 "연속거래 판정(일반종목)", 15초 경과 + 3건 미만이면 "단일가 확정". scheduler_loop에서 타임아웃 체크도 추가
- **영향**: 단기과열 종목이 실제로 연속 거래 중인 경우 오판 방지. 예상체결가 대신 실시간체결가 정상 구독

---

## 2026-03-30

### 1. feat: 당일 아침 전일 상한가 종목 기반 구독/매수 대상 교체 + 로그 개선
- **커밋**: `05c257a`
- **파일**: `ws_realtime_trading.py`, `ws_realtime_tr_str1.py`, `research_260330-1.md`
- **사유**: 기존 15:19 시점 top30 기반 "예상 상한가"가 부정확하여 실제 상한가가 아닌 종목이 매수/구독됨. 재구독/초기구독 로그에 종목 정보가 없어 디버깅이 어려웠던 문제도 함께 해결
- **주요 변경**:
  1. **`_load_morning_target_codes()` 신규 함수** — 프로그램 시작 시 `Select_Tr_target_list_symulation_pdy_ctrt.py`를 subprocess로 실행 → `Select_Tr_target_list.csv`에서 마지막 날짜 + group=ST 종목 추출. 기존 전일 15:19 예상 상한가 대신 정확한 전일 상한가 종목을 사용. 실패 시 기존 `_load_prev_closing_codes()` fallback
  2. **`_base_codes` 보호 로직 변경** — `_base_codes = set(_morning_target_codes)`로 고정. morning target 종목은 top30 추가/삭제와 무관하게 하루 종일(15:19까지) 구독 보호
  3. **재구독 로그 개선** — force 재구독 시 구독타입/종목 수/종목명 리스트 출력. 미수신 경고 로그에도 구독타입별 종목 수 추가
  4. **초기 구독 리스트 로그 추가** — 08:50 예상체결가/09:00 실시간체결가 모드 전환 시 구독 종목명 리스트 표시
- **영향**: 매수/구독 대상이 정확한 전일 상한가 종목으로 교체되어 불필요한 매수 방지. 구독 상태 디버깅 용이

---

## 2026-03-30

### 1. feat: 당일 아침 전일 상한가 종목 기반 구독/매수 대상 교체 + 로그 개선
- **커밋**: `05c257a`
- **파일**: `ws_realtime_trading.py`, `ws_realtime_tr_str1.py`, `research_260330-1.md`
- **사유**: 기존 15:19 시점 top30 기반 "예상 상한가"가 부정확하여 실제 상한가가 아닌 종목이 매수/구독됨. 재구독/초기구독 로그에 종목 정보가 없어 디버깅이 어려웠던 문제도 함께 해결
- **주요 변경**:
  1. **`_load_morning_target_codes()` 신규 함수** — 프로그램 시작 시 `Select_Tr_target_list_symulation_pdy_ctrt.py`를 subprocess로 실행 → `Select_Tr_target_list.csv`에서 마지막 날짜 + group=ST 종목 추출. 기존 전일 15:19 예상 상한가 대신 정확한 전일 상한가 종목을 사용. 실패 시 기존 `_load_prev_closing_codes()` fallback
  2. **`_base_codes` 보호 로직 변경** — `_base_codes = set(_morning_target_codes)`로 고정. morning target 종목은 top30 추가/삭제와 무관하게 하루 종일(15:19까지) 구독 보호
  3. **재구독 로그 개선** — force 재구독 시 구독타입/종목 수/종목명 리스트 출력. 미수신 경고 로그에도 구독타입별 종목 수 추가
  4. **초기 구독 리스트 로그 추가** — 08:50 예상체결가/09:00 실시간체결가 모드 전환 시 구독 종목명 리스트 표시
- **영향**: 매수/구독 대상이 정확한 전일 상한가 종목으로 교체되어 불필요한 매수 방지. 구독 상태 디버깅 용이

### 2. feat: 월별 구독 이력 CSV + research-analyst/commit-manager 에이전트 추가
- **커밋**: `3aa8fa6`
- **파일**: `ws_realtime_trading.py`, `.claude/agents/research-analyst.md`, `.claude/agents/commit-manager.md`
- **사유**: 날짜가 지나면 config의 구독 리스트가 덮어써져서 이전 일자의 base_codes와 top30 변동 이력이 유실됨. 월별 CSV로 영구 보관하여 사후 분석 가능하도록 함
- **주요 변경**:
  1. **월별 구독 이력 CSV** — `wss_subscription_log_YYMM.csv` (`data/fetch_top_list/`)
     - `_log_subscription_event(code, name, sub_type, action)`: date, time, type, code, name, action 컬럼
     - `_log_base_codes_on_startup()`: 시작 시 base_codes를 "base/시작"으로 기록
     - `_log_top_sub_event()`에서 기존 일별 CSV + 월별 CSV 동시 기록 (type=top30)
     - 기존 일별 `wss_sub_add_top10_list_{yymmdd}.csv`는 그대로 유지
  2. **research-analyst 에이전트** — 로그/문제 분석 → `research_YYMMDD-N.md` 작성, 구현 후 결과 요약 추가
  3. **commit-manager 에이전트** — diff 분석 → 커밋 → `history_code_dev.md` 일괄 처리
- **영향**: 구독 변동 이력이 월 단위로 영구 보관되어 사후 분석 가능. 에이전트 기반 워크플로우 자동화
