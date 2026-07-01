# Stoc_Kis 프로그램 개선 이력

---

## [2026-06-30] f6aad4e
- **Category**: feat
- **Title**: 시장데이터 공유메모리 버스 도입 · 호가기록기 생산자 배선 · VI전환 검증 테스트
- **Files**: `market_bus.py` (신규), `ws_orderbook_recorder.py` (수정), `test_vi_switch_exp_to_real.py` (신규)
- **Changes**:
  1. **`market_bus.py` 신규 — 락 없는 시장데이터 공유메모리 버스**
     - `multiprocessing.shared_memory` + `numpy` 구조화 슬롯으로 프로세스 간 데이터 공유
     - seqlock(쓰기 홀수→짝수, 읽기 전후 seq 동일+짝수 검증)으로 torn read 방지
     - latest-wins 방식: 생산자 비차단, 소비자는 항상 최신 호가 상태만 읽음
     - 헤더 `generation` 필드로 구독목록 변경 시 소비자 code→슬롯 매핑 자동 재구성
     - 슬롯 구성: 코드/종목명/recv_epoch/거래소시각(bsop_hour) + 10호가(매도·매수) + 10잔량 + 매도/매수 총잔량
     - `MarketDataBus.create(name, capacity)` / `attach(name)` / `update_orderbook` / `read_orderbook` / `close(unlink)` API
     - `__main__` 자체검증(생산자→소비자 라운드트립, torn read 없음) 통과
     - 목적: 호가를 한 번 수신해 공유메모리에 올리고 프로덕션·다계좌·다인 소비자가 읽는 단일출처. 다계좌 수평확장 토대.
  2. **`ws_orderbook_recorder.py` 수정 — 생산자 배선**
     - `--bus` 옵션 추가(기본 `"stockbus"`, 빈 문자열이면 버스 비활성)
     - `on_result` 에서 parquet 저장과 동시에 `MarketDataBus.update_orderbook` 으로 종목별 최신 1건 발행
     - `_publish_bus` / `_rec_int` 헬퍼 추가(문자열 호가 필드 → 정수 변환)
     - 종료 시 `_bus.close(unlink=True)` 로 공유메모리 회수
     - 버스 생성 실패 시 예외 포착 후 저장 전용 모드로 계속 진행
  3. **`test_vi_switch_exp_to_real.py` 신규 — VI 해제 기준 실시간체결 전환 검증 테스트**
     - 프로덕션 미적용 상태에서 "VI 해제 순간 예상체결→실시간체결 전환" 타당성 검증
     - a2 독립 WSS 접속(프로덕션 approval_key 무효화 방지)
     - 09:00:05 상승률 상위 조회 → 전일대비 +15% 최상위 1종목 선정
     - H0STANC0(예상체결) + H0STMKO0(장운영정보) 동시 구독
     - H0STMKO0 의 `vi_cls_code` 로 VI 발동(비0)/해제(0) 추적
     - VI 해제 순간 워커 스레드에서 H0STANC0 해제 + H0STCNT0(실시간체결) 전환(`send_request`)
     - `is_holiday` 가드 보유, 장운영 프레임 `data/vi_switch_test/` CSV 저장
     - 08:55 평일 크론 등록됨
     - 목적: 현재 프로덕션의 "09:01:59 시각 기준 전환"을 "VI 해제 기준 전환"으로 바꾸는 게 타당한지 실거래일 검증
- **Impact**: 호가 데이터를 공유메모리 버스로 추상화하는 인프라 확보. 향후 프로덕션이 ws_orderbook_recorder 의 공유메모리를 구독하면 WSS 슬롯 절약 + 다계좌 확장 시 수신 중복 제거 가능. VI 해제 전환 테스트 결과에 따라 프로덕션 09:01:59 전환 로직 개선 여부 결정 예정.

## [2026-06-30] ca18b55
- **Category**: feat
- **Title**: TOP5_ADD=False 데이터수집 모드 전환 · VI텔레그램 제거 · 실시간 호가기록기 신규
- **Files**: `ws_realtime_trading.py`, `orderbook_recorder.py` (신규), `test_nxt_pre_after_market.py`
- **Changes**:
  1. **`ws_realtime_trading.py` — TOP5_ADD True→False 전환**
     - top30 동적 WSS 구독 추가/제거 중단(전일 상한가 데이터수집 모드)
     - base_codes(=전일 상한가 morning_target) 보호 구독·저장은 그대로 유지
     - False 시 비활성: top30 30분 5개 추가 / 20%↓ 해제 / LRU 해제, 미수신 10분 경과 해제, 종가매매 종목 선정(15:19)·구독 전환(15:20) → 종가매매 매수 사실상 중단
     - True 복원 시 기존 동작 전체 자동 복원
  2. **`ws_realtime_trading.py` — VI 발동·해제 텔레그램 제거**
     - `[VI발동]`/`[VI해제]` 알림: `_notify(msg, tele=True)` → `_notify(msg)` (로그·콘솔만 유지)
     - 서킷브레이커·사이드카·`[VI매도]` 텔레그램은 그대로 유지
  3. **`orderbook_recorder.py` 신규 — 실시간 호가(H0STASP0) 기록기**
     - a2 계정 독립 접속(프로덕션 a1 approval_key 무효화 방지)
     - 전일 상한가 ST 전체 다종목 구독(`_select_codes`, `--code` 쉼표 다중 지정)
     - parquet 조각+병합 저장(수신→메모리 버퍼→flush 주기 조각 기록, 종료 시 단일 parquet 병합, 조각은 parts/ 백업 보존)
     - 출력: `data/wss_data/orderbook/{YYMMDD}_orderbook_{code}.parquet`
  4. **`test_nxt_pre_after_market.py` — 프리마켓·애프터마켓 지정가 지원**
     - `use_limit=True` 옵션 추가: 기준가 ±1% 시장가성 지정가(NXT 프리마켓 시장가 불가 대응)
     - 프리마켓·애프터마켓 단일 실행 기본값을 `use_limit=True` 로 전환
  5. **기타**: `docs/KIS H0STMKO0 장운영코드 260626` 문의 문서, `ws_monitoring_research_260626.md` 추가
- **Impact**: 단기과열 상한가 종목 전종목 호가 데이터 수집 인프라 확보. 프로덕션 WSS는 전일 상한가 기반 구독만으로 안정 운영. VI 텔레그램 노이즈 제거로 중요 알림(서킷·VI매도) 가시성 향상.

## [2026-06-26] 638af7b
- **Category**: feat
- **Title**: 워치독 기동 시 휴장일 확인 — 공휴일 빈 가동 방지
- **Files**: `ws_realtime_watchdog.py`
- **Changes**:
  1. **`is_holiday()` 호출로 개장일 여부 사전 확인**
     - `main()` 진입 직후 `kis_utils.is_holiday()`(국내휴장일조회 API opnd_yn + config `today_open_chk` 캐시, 메인 트레이딩과 동일 소스) 호출
     - 휴장일이면 감시 루프 없이 즉시 `return 0` (텔레그램 알림 포함)
  2. **fail-open 안전 처리**
     - API/토큰 오류로 예외 발생 시: 개장일로 간주하고 정상 가동 (안전망 우선)
     - `is_holiday` 임포트 자체 실패 시: 동일하게 정상 가동, 로그만 기록
  3. **모듈 도크스트링에 `[개장일에만 가동]` 절 추가**: 동작 원리·fail-open 정책 명시
- **Impact**: cron(평일 KST 월~금)이 한국 공휴일을 거르지 못해 공휴일에도 하루 종일 빈 점검 로그를 남기던 노이즈 제거. 공휴일엔 기동 즉시 종료. 판단 불가 시에는 안전망(감시)을 유지.

## [2026-06-26] f060b17
- **Category**: refactor
- **Title**: 워치독 자체 종료 로직 추가 — 장 마감 후 야간 빈 점검 로그 노이즈 제거
- **Files**: `ws_realtime_watchdog.py`
- **Changes**:
  1. **`_check_once()` 반환값 None → bool 로 변경**
     - 기존: 반환값 없이 항상 루프 지속 → 장 마감 후 새벽까지 5분마다 `pids=[] status=2` 빈 로그 무한 생성
     - 변경: `True` 반환 시 main 루프가 `return 0` 으로 정상 종료
  2. **20:00(NIGHT_KILL_HOUR) 이후 자체 종료 조건**
     - 잔존 프로세스 있으면 기존 안전망(SIGTERM→SIGKILL) 수행 후 재점검
     - 재점검 후에도 잔존하면 `False` 반환 — 다음 주기에 재시도
     - 감시 대상이 없으면 텔레그램 알림 후 `True` 반환 → 워치독 자체 종료
  3. **기존 안전망(규칙2) 완전 유지**: 20:00 이후 좀비 강제종료 로직 변경 없음
  4. **도크스트링 [자체 종료] 절 추가**: 동작 원리·재기동 경로 명시
- **Impact**: 장 마감 후 새벽까지 쌓이던 빈 점검 로그(`pids=[] status=2`) 노이즈 제거. 20:00 좀비 강제종료 안전망 유지. 다음 영업일 08:25 cron 으로 재기동.

## [2026-06-26] 4bc7cb7
- **Category**: feat
- **Title**: NXT 프리마켓·애프터마켓·개장전취소 실거래 점검 스크립트 추가
- **Files**: `test_nxt_pre_after_market.py` (신규)
- **Changes**:
  1. **3단계 시나리오 독립 테스트 스크립트 신규 작성**
     - ① 프리마켓(NXT 08:05): 시장가 1주 매수 → 1분 후 매도
     - ② 개장 전 취소점검(08:50~09:00): 지정가 매수 접수 후 취소 — A·B·C 3회 반복으로 KRX 동시호가 시간대 취소 정상처리 검증
     - ③ 애프터마켓(NXT 15:45): 시장가 1주 매수 → 1분 후 매도
  2. **NXT 통합주문 규격 적용**
     - 주문 본문에 `EXCG_ID_DVSN_CD` 필드 포함 (KRX/NXT 구분)
     - 신규 TR_ID 사용: 매수 `TTTC0012U` / 매도 `TTTC0011U` / 취소 `TTTC0013U`
     - 취소 본문: `ORGN_ODNO` + `QTY_ALL_ORD_YN` + `EXCG_ID_DVSN_CD` 포함
  3. **안전장치**
     - 계좌 a1 고정, 기존 토큰 캐시 재사용 (신규 토큰 발급 없음 → 카카오 알림톡 미발생)
     - 수량 1주 고정
     - `--dry-run` 옵션으로 API 호출 없이 흐름 점검 가능
     - 프로덕션 `ws_realtime_trading.py` 일체 미수정
- **Impact**: NXT 거래소 프리마켓·애프터마켓 주문 및 개장 전 취소 기능을 실거래 1주로 안전하게 검증할 수 있는 독립 점검 도구 확보.

## [2026-06-26] d05e799
- **Category**: refactor
- **Title**: 1분봉 다운로드 텔레그램 알림 노이즈 제거 + NXT 아침 실행 시각 앞당김
- **Files**: `kis_1m_API_to_Parquet_all_code.py`, `kis_1m_API_to_Parquet_NXT.py`
- **Changes**:
  1. **kis_1m_API_to_Parquet_all_code.py — 중간 진행 텔레그램 제거 + 완료 메시지 통합**
     - 50% 도달 시 보내던 "다운로드 중" 중간 진행 텔레그램 제거 → `_log_info_only` (로그 전용) 전환.
     - 사용하지 않게 된 `progress_state["tele_sent"]` 키 제거.
     - "다운로드 완료" + "파일병합 완료" 2건이던 완료 텔레그램을 1건으로 통합: 종목수·건수·소요시간 포함.
  2. **kis_1m_API_to_Parquet_NXT.py — 진행 단계 텔레그램 전면 로그 전용 전환 + 완료 메시지 정리**
     - 진행 단계 전용 `_log_q()` (화면+로그, 텔레그램 없음) 함수 추가.
     - 세션 안내·10% 진행률·병합 시작·중복제거·저장·S3업로드·temp 정리 등 중간 단계를 `_log_q`로 전환.
     - 휴일 종료 시 `_log` + `send_telegram` 중복 전송 → `_log` 1건으로 정리 (`send_telegram` 호출 및 미사용 import 제거).
     - 종료 경로마다 "프로그램 종료" 텔레그램 2건 → 1건으로 정리, 완료 메시지에 세션·건수 포함.
     - 남은 텔레그램: 시작 / 완료 / 휴일·API장애 조기종료·데이터없음 등 비정상 종료.
  3. **NXT 아침 실행 시각 09:05 → 08:51 KST (프리마켓 종료 직후) 반영**
     - 파일 헤더 주석 09:05 KST → 08:51 KST (프리마켓 종료 직후) 갱신.
     - 폴백 영업일 판정 임계값 `now_kst.hour >= 9` → `>= 8` 변경: 08:51(8시대) 실행 시 전일 라벨링 오류 방지. API 응답에 거래일이 있으면 그 값 우선이라 정상 케이스 무영향.
  4. **기타 정리 (일괄 커밋)**
     - 구형 프로덕션 백업본 7개 삭제 (ws_realtime_trading_0422 등), 구형 ws_monitoring_research md 9개 삭제.
     - 폐기된 시뮬레이션 12개 삭제, EarlyPop-Trail 시뮬레이션·요약 추가.
     - `CLAUDE.md`·에이전트 메모리·scripts·test 파일 등 신규 파일 초기 추적 등록.
- **Impact**: 텔레그램 알림이 시작/완료/비정상종료만 수신되어 노이즈 대폭 감소. NXT 아침 다운로드가 프리마켓 종료(08:50) 직후 즉시 실행되어 데이터 취득 타이밍 개선.

## [2026-06-25] 1f1cd04
- **Category**: refactor
- **Title**: 날짜별 텔레로그 기록을 telegMsg.tmsg() 단일 지점으로 통합
- **Files**: `telegMsg.py`, `ws_realtime_trading.py`
- **Changes**:
  1. **문제**: 날짜별 텔레그램 로그(out/logs/{YYMMDD}_telegram.log) 기록 코드가 프로덕션의 `_notify` / `_notify_async` 안에만 존재했음. 실제 텔레그램 발송은 모든 프로그램이 `telegMsg.tmsg()` 한 곳을 거치지만 `tmsg()` 자체는 기록을 하지 않아, 프로덕션 종료 후 `ws_realtime_watchdog.py` / `ws_log_monitor.py` / `Daily_inquire_vi_status.py` 등 9개+ 프로그램이 보내는 텔레 메시지는 날짜별 텔레로그에 남지 않았음.
  2. **telegMsg.py — `_append_tele_log()` 추가**: KST(UTC+9) 기준 날짜·시각으로 `out/logs/{YYMMDD}_telegram.log` 에 `'YYYY-MM-DD HH:MM:SS | 메시지'` 형식 기록. 기존 프로덕션 형식과 동일. 기록 실패는 텔레그램 전송을 막지 않도록 조용히 무시.
  3. **telegMsg.py — `tmsg()` 내부 호출 위치**: 네트워크 전송보다 먼저 `_append_tele_log()` 를 호출. `_notify_async` 의 데몬 스레드 fire-and-forget 경로에서도 기록이 전송 시도에 앞서 남아 누락 위험 최소화.
  4. **ws_realtime_trading.py — 중복 기록 코드 제거**: `_notify` / `_notify_async` 내 파일 기록 블록 삭제. `tmsg()` 가 유일한 기록 지점이 되어 프로덕션 텔레 메시지 중복 두 줄 문제 해소. 관련 주석 갱신.
- **Impact**: 프로덕션 종료 후에도 `tmsg()` 를 호출하는 모든 프로그램의 텔레 메시지가 같은 날짜별 텔레로그에 자동으로 빠짐없이 기록됨. 앞으로 추가될 프로그램도 별도 작업 없이 자동 적용. 프로젝트 규칙(텔레 메시지는 자체 로그에 시간정보와 함께 반드시 기록) 완전 준수.

## [2026-06-25] 6a2b2d2
- **Category**: fix
- **Title**: 로그 모니터 — 시스템종료 분류 로직 개선 (비정상강제종료만 CRITICAL)
- **Files**: `ws_log_monitor.py`
- **Changes**:
  1. **문제**: 6.24 일일 리포트에서 "CRITICAL_시스템강제종료: 5" 로 과장 집계. 실제 비정상 강제종료는 1건이고 나머지는 정상 재시작 os._exit(0) 2건, 90초 예고문 "강제 종료 예정" 1건, 중복 1건이었음.
  2. **_classify_exit() 신설**: `os._exit` / "강제 종료" 류 로그 줄을 종료코드·문맥으로 4가지로 세분.
     - `os._exit(0)` + `[scheduler] EXIT` → INFO 정상종료 (EOD 정리)
     - `os._exit(0)` (좀비 thread 차단 등 재시작 절차) → INFO 정상재시작
     - "~ 강제 종료 예정" 예고문 → INFO 강제종료예고 (실제 종료 아님)
     - `os._exit(N≠0)` (watchdog 무수신 자폭 등) → CRITICAL 비정상강제종료
  3. **기존 시각(15:30) 기반 강등 로직 제거**: 종료코드 기반으로 대체해 더 견고하게 개선. 시각 의존성 제거로 재시작 시간이 15:30 이전이어도 정확히 분류.
  4. **_dedup_key() 개선**: "비정상강제종료" 는 분(分) 단위로 묶어, 예고문·실행 로그 등 여러 줄이 같은 분 안에 찍혀 중복 집계되던 문제 해결.
  5. **process_line 개선**: INFO 생명주기 이벤트(정상종료/정상재시작/강제종료예고)를 `daily_stats[INFO_*]` 에 집계, 텔레그램·Claude 경보는 미발동. 일일 리포트에 건수 표시만 됨.
  6. **6.24 로그 검증 결과**: CRITICAL_비정상강제종료 1, INFO_정상재시작 2, INFO_정상종료 1, INFO_강제종료예고 1 (기존 오탐 CRITICAL_시스템강제종료 5 → 정상화).
- **Impact**: 실제 비정상 강제종료(watchdog 자폭 등)에만 CRITICAL 경보 + Claude 긴급 호출이 발동. 정상 재시작/예고문은 소음 없이 INFO 로 기록만 됨. 일일 리포트의 신뢰도 향상.

## [2026-06-24] 1b55b64
- **Category**: feat
- **Title**: NXT 1분봉 다운로드 하루 2회 세션 분리 (아침 프리마켓 / 저녁 애프터마켓)
- **Files**: `kis_1m_API_to_Parquet_NXT.py`, crontab(시스템 설정, 미커밋)
- **Changes**:
  1. **목적**: 기존 하루 1회(저녁 일괄) 구조에서는 당일 오전 분석 시 당일 NXT 프리마켓 데이터를 확보할 수 없었음. 아침 실행(09:05 KST)을 추가해 프리마켓 봉(08:00~08:50)을 당일 오전에 바로 취득.
  2. **_resolve_session() 추가**: `--session=morning|evening|full` 인자를 우선 적용, 미지정 시 KST 12시 기준 자동판정(이전=morning, 이후=evening).
  3. **_build_query_times(session) 추가**: morning=프리마켓만, evening=애프터마켓만, full=둘 다(수동 보강용).
  4. **PREMARKET_QUERY_TIMES에 081000 추가** (3회 조회: 081000·083000·085000).
     - 기존 083000·085000 2회는 30봉 윈도우 경계상 08:00 정각 봉이 누락됨을 20260622·20260623 실측 파일로 확인(최소 time=080100).
     - 081000 기준점 추가로 08:00 봉까지 완전 커버.
  5. **체크포인트 경로 세션별 분리**: `{date}_{session}_nxt_checkpoint.csv` — 아침(morning) 완료 플래그가 저녁(evening) 실행에서 전 종목을 skip 시키던 문제 방지.
  6. **최종 저장을 덮어쓰기 → 병합으로 변경**: 기존 parquet이 있으면 읽어 frames에 합쳐 date/time/code 기준 중복제거. 아침 프리마켓과 저녁 애프터마켓 데이터를 하나의 파일에 누적.
  7. **모듈 docstring 갱신**: 2회 실행 방법 및 `--session=full` 수동 보강 방법 명시.
  8. **crontab 변경** (시스템 설정, 미커밋):
     - 추가: `05 0 * * 1-5 ... --session=morning` (09:05 KST, 프리마켓 전용)
     - 변경: 기존 `01 11 * * 1-5` 저녁 크론에 `--session=evening` 부여 (애프터마켓 전용)
     - 원본 백업: `out/crontab.ORIG_260623`
- **Impact**: 당일 오전 분석 시 당일 NXT 프리마켓 데이터(08:00~08:50)를 09:05 이후 즉시 활용 가능. 세션 분리로 아침/저녁 실행이 서로 간섭 없이 독립 동작하며, parquet 병합 방식으로 프리+애프터 전 구간 데이터가 단일 파일에 누적 보존.

## [2026-06-20] aae7d7e
- **Category**: fix
- **Title**: 로그 모니터 — 장 마감 후 EOD 정상종료 CRITICAL 오탐 수정 + 모니터 자동 정상 종료
- **Files**: `ws_log_monitor.py`
- **Changes**:
  1. **근본 원인**: `CRITICAL_PATTERNS_EXTRA`의 `os._exit` 패턴에 장 마감 시각 가드가 없어, 프로덕션이 매일 16:01에 정상 종료할 때 출력하는 `[scheduler] EXIT 모드 정리 완료 → os._exit(0)` 줄이 CRITICAL 텔레그램 + 긴급 Claude 호출로 오탐. 06/05·08·12·15·16·17·18·19 등 거의 매 거래일 반복 발생(`out/monitor/issues_*.jsonl` 확인).
  2. **classify() 수정**: `CRITICAL_PATTERNS_EXTRA` 루프에서 `cat=="시스템강제종료"` 이고 로그 시각 >= `MARKET_CLOSE`(15:30) 이면 `("INFO","정상종료")`로 강등. 장중 `os._exit`·watchdog FATAL·재시작은 CRITICAL 유지.
  3. **NORMAL_EOD_EXIT_PATTERN 상수 추가**: `re.compile(r"\[scheduler\] EXIT 모드 정리 완료")` — 프로덕션 정상 종료의 확정 표식.
  4. **process_line() 수정**: 로그 시각 >= 15:30 이고 `NORMAL_EOD_EXIT_PATTERN` 매치 시 `_handle_normal_eod()` 호출 후 return.
  5. **_handle_normal_eod() 추가**: INFO 알림 출력·텔레그램 → 미처리분 flush(`_flush_traceback`, `_flush_moderate`) → 일일 리뷰 즉시 실행 → `stop()`. 18:35 하드 종료는 안전망으로 유지.
  6. **검증**: `py_compile` 통과. `classify` 단위 테스트 5종 통과(정상 EOD→INFO, 장중 좀비 `os._exit`→CRITICAL, 주문실패/watchdog FATAL→CRITICAL). DRY-RUN 통합 테스트: `[scheduler] EXIT` 줄 처리 직후 INFO 출력 → 일일 리뷰 호출 → `_stop=True` 확인.
- **Impact**: 매 거래일 16:01 발생하던 오탐 CRITICAL 알림 및 불필요한 Claude 긴급 호출 완전 차단. 프로덕션 정상 종료 직후 모니터도 일일 리뷰를 마치고 깔끔하게 종료되어 18:35까지 idle 상태로 잔여 알림이 발생하는 문제 해소.

## [2026-06-04] 7e3893e
- **Category**: refactor
- **Title**: 계좌 식별자/토큰 캐시 a1/a2 체계 통일 + REST access_token 공유 표준 확립
- **Files**: `ws_realtime_trading.py`, `kis_utils.py`, `kis_auth.py`, `kis_API_ohlcv_download_Utils.py`, `ws_realtime_subscribe_to_DB-2.py`, `Daily_inquire_vi_status.py`, `fetch_top30_each_1m.py`, `check_market_status.py`, `inquire_vi_status.py`, `inquire_vi_status_simple.py`, `kis_inquire_balance_simple.py`, `kis_Trading_1_LmUp_str.py`, `fetch_foreign_investor_daily.py`, `ws_a2_subprocess.py`, `symulation/fetch_investor_history.py`, `symulation/investor_db_pilot.py`, `test_ccnl_notice.py`, `test_inquire_price.py`, `test_vi_wss_cycle.py`, `test_wss_close_cycle.py`, `test_wss_simple.py`, `rules/trading_project_main_plan.md`, `rules/LOCAL_appkey_token_sharing_prompt_260604.md` (신규)
- **Changes**:
  1. **발생 원인**: 260604 08:20·08:28·09:00 알림톡 다발 원인 분석. 같은 appkey임에도 프로그램마다 토큰 캐시 파일명이 달라(main: `kis_token.json` vs `kis_token_main.json`; syw_2: `kis_token_syw2.json` vs `kis_token_syw_2.json`, 프로덕션 내부 라인 6541 vs 6613 불일치) 서로 캐시를 인식 못하고 각자 재발급 → 알림톡 반복.
  2. **계좌 식별자 통일**: 전 파일에서 `main` → `a1`, `syw_2`/`syw2` → `a2` 로 일원화. config.json 키도 `['a1', 'a2']` 체계.
  3. **토큰 캐시 파일명 통일**: `kis_token_a1.json` / `kis_token_a2.json` 단일 체계로 통일. appkey당 1발급 후 전 프로그램 공용 재사용 보장. 구파일(`kis_token.json`, `kis_token_main.json`, `kis_token_syw2.json`, `kis_token_syw_2.json`)은 watchdog 등 참조 해소 후 23:28 cron 재시작 시 정리 예정.
  4. **기본경로 통일**: `./kis_token.json` (기본값) → `./kis_token_a1.json` 로 통일하여 무계정 호출 시에도 a1 캐시 사용.
  5. **approval_key vs access_token 명문화**: `rules/trading_project_main_plan.md` 에 REST access_token 공유 표준 섹션 추가. approval_key(WSS, `/oauth2/Approval`, 발급 알림 없음)와 access_token(REST, `/oauth2/tokenP`, 24h 유효, 6h 내 동일토큰 반환, 발급 시 알림톡 발생)은 완전히 다른 키임을 명문화.
  6. **신규 규칙 파일**: `rules/LOCAL_appkey_token_sharing_prompt_260604.md` — 로컬 개발 환경 전달용 appkey/token 공유 표준 프롬프트.
  7. **검증**: 잔여 구참조 grep 0건, config 키 `['a1','a2']` 확인, 수정 `.py` 전부 `py_compile` 통과.
- **Impact**: 알림톡 다발 원인 근본 해소. appkey당 REST access_token이 단일 캐시 파일로 통합되어 24h 내 재발급 없이 전 프로그램 공유 재사용. 새 프로그램 추가 시에도 a1/a2 식별자만 사용하면 자동으로 동일 캐시 참조.

## [2026-06-02] b2ab9b3
- **Category**: feat
- **Title**: WSS close/재접속 사이클 독립 검증 테스트 신규 추가
- **Files**: `test_wss_close_cycle.py` (신규)
- **Changes**:
  1. **목적**: 재시작 직후 1006(invalid approval) 원인·수정을 메인 프로덕션(a1) 무중단으로 독립 검증. a2(syw_2) approval_key + 실제 KISWebSocket(kis_auth_llm) 사용 → 프로덕션과 동일한 close 경로 재현.
  2. **측정 항목**: ① close() 소요시간(close frame 실제 송신 완료 시간) ② UNSUBSCRIBE 단계 소요시간(mode=unsubclose, `_request_ws_close` 모사) ③ close 직후 동일 approval_key 즉시 재접속 시 1006(dwell<15s) 발생 여부.
  3. **모드**: `unsubclose`(UNSUBSCRIBE 후 close) / `closeonly`. CLI: `--account --code --cycles --mode --reconnect-delay`.
  4. **로그 캡처**: KISWebSocket 내부 로그(close frame 송신 완료 Nms / 1006 / SUBSCRIBE)를 logging 핸들러로 캡처하여 사이클별 측정값 출력.
- **Impact**: 프로덕션 무중단 상태로 WSS close 메커니즘 단독 검증 가능. [260602 17:46 실측] close=8ms(ConnectionClosedOK code=1000), 즉시 재접속 1006 없이 15초 유지 → close 코드 자체는 정상·즉시 동작 확인. 프로덕션의 5초 타임아웃/1006은 close 버그가 아니라 당일 4회 빠른 연속 재시작으로 인한 rapid-restart 연쇄 부작용(정상 일일 단일 기동/종료에선 미발생) 결론.

## [2026-06-02] 2df1910
- **Category**: fix
- **Title**: 재시작 1006 구조적 원인 수정 — UNSUBSCRIBE 예산 cap + close 실행 보장
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **배경**: 앞선 band-aid(f2f05d7, 5s→13s 외곽 timeout) 재검토. 구조적 결함은 UNSUBSCRIBE(최대 5타입×1s)가 외곽 예산을 모두 소진하면 close()가 호출조차 안 되는 것. close frame 미전송 → KIS 세션 잔재 → 재시작 직후 동일 approval_key 로 접속 시 1006 반복.
  2. **`_shutdown`**: `_request_ws_close` join 외곽 timeout 13.0s → 6.0s (band-aid 되돌림). 단계별 예산 관리는 내부로 이전. 정상 종료 시 추가비용 없음.
  3. **`_request_ws_close`**: UNSUBSCRIBE 총예산 `_UNSUB_BUDGET = 2.0s` cap 추가. 예산 소진 시 남은 종목 skip하고 즉시 close 진행 → close frame 전송 항상 보장. UNSUBSCRIBE 타입별 ok/timeout(+Ns), 단계 종료(+Ns), close() 반환(+Ns) 로깅 추가. 예외 로그도 debug→warning으로 승격.
- **Impact**: close frame 전송을 구조적으로 보장 → KIS 세션 즉시 반환 → 재시작 직후 1006 해소 기대. 다음 재시작 종료 로그의 'close() 반환 (+Ns)' 와 단계별 시간으로 원인 확정 가능. 잔여 1006은 기존 self-heal(dwell<15s → force_new approval_key)이 복구.

## [2026-06-02] f2f05d7
- **Category**: fix
- **Title**: 재시작 1006 근본수정 — graceful close timeout 5s→13s, settle 처방 철회
- **Files**: `restart_wss_trading.sh`, `ws_realtime_trading.py`
- **Changes**:
  1. **원인 재규명**: 직전 커밋(93f2c51)에서 재시작 1006을 "KIS 서버측 세션 해제 지연"으로 보고 settle 7초 대기를 추가했으나 재검증 결과 실제 원인은 "우리 close 미완료"로 확인. 모든 재시작 종료 로그에서 `_request_ws_close 5s timeout` 발생 — UNSUBSCRIBE(타입별 1s) + close frame(최대 10s) 합이 5s를 초과하여 close frame이 매번 잘렸고, KIS가 abnormal close로 인식 → 세션 잔재 → 재시작 직후 새 접속 1006.
  2. **ws_realtime_trading.py `_shutdown`**: `_request_ws_close` 스레드 join timeout 5.0s → 13.0s. 충분한 시간을 부여하여 close frame 완료 → KIS 정상 close 인식 → 세션 즉시 반환. 정상 케이스는 빠르게 끝나면 join 즉시 반환되므로 추가비용 없음. timeout 경고 메시지도 13s로 갱신.
  3. **restart_wss_trading.sh**: 직전 커밋에서 추가한 settle `sleep 7` 및 관련 echo 제거(잘못된 처방 철회). do_restart()→do_stop() 선행 종료(이중실행 방지) 로직은 유효하여 유지.
- **Impact**: close frame 미완료 → KIS abnormal close → 세션 잔재 → 재시작 1006 사이클 근본 차단. 검증 포인트: 다음 재시작 종료 로그가 `5s/13s timeout` 대신 `close frame 송신 완료`로 바뀌면 해결 확인.

## [2026-06-02] 93f2c51
- **Category**: fix
- **Title**: 재시작 시 WSS 1006 핸드셰이크 실패 방지 + 이중실행 원천 차단
- **Files**: `restart_wss_trading.sh`, `ws_realtime_trading_Restart.py`
- **Changes**:
  1. **배경**: 260602 빠른 수동 재시작 4회 모두 새 프로세스에서 WSS 1006(dwell≈1.12s, "no close frame received or sent") 핸드셰이크 실패 발생. 원인은 이전 프로세스 종료 직후 같은 a1 approval_key 로 곧바로 재접속 → KIS 서버측 WSS 세션 해제가 완료되기 전에 충돌. self-heal(force_new)로 복구되긴 했으나 재시작마다 반복.
  2. **restart_wss_trading.sh**: 이전 프로세스 종료 확인 후 새 runner 기동 직전에 `sleep 7` 및 안내 echo 추가. KIS 서버가 직전 approval_key WSS 슬롯을 반환할 시간 확보 → 재시작 1006 재발 방지.
  3. **ws_realtime_trading_Restart.py**: `do_restart()` 진입 시 `do_stop()` 먼저 호출. 기존에는 restart 스크립트 내부에서만 종료 처리하여 Restart.py 레벨 보장이 없었음. 변경 후: SIGTERM→graceful shutdown→필요시 SIGKILL(스크립트+runner 모두) 수행 완료 후 재기동 → 이중 실행/이중 매매 원천 차단.
  4. 참고: WSS graceful close(SIGTERM→_handle_signal→_shutdown→_request_ws_close: 활성 구독 UNSUBSCRIBE + close frame 송신)는 이미 구현돼 있어 이번 수정 범위 외.
- **Impact**: 재시작마다 반복되던 WSS 1006 → self-heal 사이클 제거. 이중 프로세스로 인한 이중 매매 가능성 차단.

## [2026-06-02] bd864ca
- **Category**: feat
- **Title**: OHLCV 다운로드 텔레그램 4단계 축소 + 완료 소요시간 추가
- **Files**: `kis_1d_Daily_ohlcv_fetch_manager.py`, `kis_1m_API_to_Parquet_all_code.py`
- **Changes**:
  1. **헬퍼 추가**: `_fmt_elapsed(sec)` — 초→시간/분/초 문자열 변환 (양쪽 파일에 각각 추가). `_t0` / `_t_start` — 프로그램 시작 시각 기록.
  2. **텔레그램 유지 4건(1d)**: ① [start] 실행 ② [download] 다운로드 중(fetch 시작, 문구 변경) ③ [done] 다운로드 완료(count + 소요시간) ④ [done] 파일병합 완료(문구 변경). 에러/경고(조회결과없음/통합parquet실패) 유지.
  3. **텔레그램 유지 4건(1m)**: ① 프로그램 시작 ② 다운로드 중(50% 도달 1회만) ③ 다운로드 완료(총 종목수 + 소요시간) ④ 파일병합 완료(문구 변경). 에러 2건(데이터없음/임시파일없음) 유지.
  4. **로그 전용 강등(1m)**: `_log_info_only` 헬퍼 신설. 병합 시작, 덮어쓰기 모드, 중복제거 시작, parquet 저장 시작, S3 업로드 완료, 프로그램 종료(3곳), 진행률 10%~40%/60%~90% 알림.
  5. **로그 전용 강등(1d)**: `log_tm` → `log` 강등 — 중복 'OHLCV download 시작', 'S3 업로드 완료', '[end] 종료'.
  6. **검증**: py_compile 양쪽 파일 OK. 소요시간 포맷 예: 45초 / 6분 12초 / 1시간 2분 5초.
- **Impact**: 다운로드 안정화 이후 텔레그램 과다 알림 해소. 완료 메시지에 소요시간이 포함되어 성능 추이 파악 가능.

## [2026-06-02] 9883a6d
- **Category**: fix
- **Title**: _get_balance_page 기동 직후 transient 500 재시도 추가
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **증상**: 재시작 직후 [시작 잔고조회] 첫 호출(CTX_AREA_FK100=빈값)에서 KIS inquire-balance 가 간헐 500 Internal Server Error 반환. 260602 재시작 3회 실측(13:21, 15:03 포함) 모두 a1(43444822) 첫 잔고호출에서 동일 패턴.
  2. **수정**: `_get_balance_page` 의 `requests.get + raise_for_status` 를 최대 3회 재시도(1.5s 간격) 루프로 감쌈. `requests.RequestException`(HTTPError·Timeout·ConnectionError) 모두 재시도 대상. 3회 모두 실패 시 마지막 예외 raise → 기존 오류 전파 동작 유지.
  3. idempotent GET 이므로 재시도 안전. tr_cont 페이지네이션 버그 수정과는 별개(이건 첫 호출 자체 실패 대응).
- **Impact**: 기동 직후 transient 500 흡수 → hold_map/시작잔고 정상화, str1 매도 모니터링 누락 방지.

## [2026-06-02] 4126c4e
- **Category**: feat
- **Title**: 08:59 prdy>9.8% 종목 H0STMKO0 관찰구독 → vi_cls_code 정합성 검증 로그 추가
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. 전역 `_vi_observe_codes(set)` / `_vi_observe_done(bool)` 추가 — 09:00 VI 지연 변수(`_vi_delayed_codes`) 옆에 선언
  2. `scheduler_loop`: 08:59:00~08:59:05 구간에 `_last_prdy_ctrt > 9.8%` & 구독 중(`codes`) 종목을 `_mkstatus_sub_add` 로 H0STMKO0 관찰구독 + 텔레그램 통지
  3. `scheduler_loop`: 09:05 도달 시 관찰용 H0STMKO0 해제(보유/keepalive/VI활성 제외) + `_vi_observe_codes` 비움
  4. `_on_market_status_krx`: 관찰 종목(`_vi_observe_codes`)은 `[VI관찰-0859]` 로그(vi_cls_code / ovtm_vi / mkop / antc_mkop / trht / iscd_stat)만 남기고 `continue` → 실제 VI 전환/상태 로직은 건너뜀(기존 09:00 타이머 연장 동작 유지, 동작 변경 0)
- **Impact**: 장 오픈 연장(VI 예측) 기준을 09:02 고정 타이머에서 H0STMKO0 vi_cls_code 이벤트 기반으로 전환하기 전, 09:00 전후 vi_cls_code 수신 정합성을 하루 관찰해 검증. 매매 동작 변경 없음. H0STMKO0_MAX_SLOTS=5(a1 공유)로 대상 6개+ 시 일부만 관찰됨(별도 a2 WSS 이전 시 해소 예정)

## [2026-06-02] c1dfba1
- **Category**: refactor
- **Title**: 경로B VI 감지 로그 간소화 — VI현황조회 REST 추가호출 제거 (옵션 B 채택)
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **배경**: 직전 커밋(fa7d25d)에서 경로B(REST 현재가조회) VI 감지 시 `_inquire_vi_status_single`
     (FHPST01390000)을 즉시 호출해 발동시각·VI종류(정적/동적)를 로그에 표기하도록 추가했음.
  2. **판단**: 해당 vi_time·vi_cls 정보는 `[VI감지-REST]` 로그에만 쓰이고,
     매매 로직·parquet 저장(`_vi_start_ts=now()`)에는 미사용 확인.
     정확 발동시각·VI종류는 vi_status CSV(Daily_vi, 양방향)에 권위있게 남으므로,
     감지마다 REST 1회 추가하는 비용 대비 실익이 없음 → 사용자 옵션 B 채택.
  3. **수정**: `_handle_stale_check_result` (~L8395) 에서
     - `_inquire_vi_status_single` 호출 블록 제거.
     - `_VI_CLS_NAMES` dict 제거.
     - 감지 로그를 `vi_cls_code={Y} 감지시각=HH:MM:SS → 예상체결 전환` 으로 간소화.
  4. **부수효과(무해)**:
     - `_inquire_vi_status_single`: 호출처 없어져 dead code, 유틸로 보존.
     - `_vi_trigger_info`: set 하는 곳 없어져 사실상 미사용 (기존 pop은 무해 no-op).
- **Impact**:
  - 변동성 장세에서 VI 감지마다 REST 1회 절약.
  - 매매 로직·parquet 저장에 변경 없음.
  - 로그 가독성 유지(감지시각 근사값 표기).

## [2026-06-02] fa7d25d
- **Category**: fix
- **Title**: 경로B(REST 현재가조회) VI 감지를 vi_cls_code 기준으로 전환 + Daily VI 조회 방향 전체로
- **Files**: `ws_realtime_trading.py`, `Daily_inquire_vi_status.py`
- **Changes**:
  1. **근본원인(ws_realtime_trading.py)**: 틱 끊김 후 경로B(REST 현재가조회)에서 VI 판단을
     `temp_stop_yn='Y'` 게이트로만 했으나, 260602 실측 결과 VI 동시호가 중엔
     `temp_stop_yn='N'` 이고 `vi_cls_code='Y'` 로 온다. → 일반 구독 종목의 VI를 경로B에서
     완전히 놓쳐 예상체결 전환이 누락됨.
  2. **수정(ws_realtime_trading.py)**: `_handle_stale_check_result`에서
     - `vi_cls_code` 선검사 분기를 `temp_stop_yn` 검사보다 앞에 추가.
     - `vi_cls_code`가 비정상값('N'/'0'/'' 아님)이면 `_inquire_vi_status_single`로
       발동시각/종류를 보강하고 `[VI감지-REST]` 로그(텔레그램) + `_vi_exp_sub_switch` 수행.
       해제는 이후 경로A(H0STMKO0 `vi_cls='N'`)가 처리하므로 별도 해제 로직 불필요.
     - 기존 `temp_stop_yn=Y` 처리는 '거래정지' 전용 분기로 분리, 로그에 `vi_cls_code` 병기.
  3. **수정(Daily_inquire_vi_status.py)**: `OPT_DIRECTION` `"1"`(상승만) → `"0"`(전체).
     하락 VI가 vi_status CSV에서 누락되던 문제 해소.
     (프로덕션 단일조회 `_inquire_vi_status_single`은 이미 `FID_DIV_CLS_CODE="0"` 전체였음.)
- **Impact**:
  - VI 동시호가 진입 시 경로B에서도 VI가 즉시 감지되어 예상체결 전환 누락 없어짐.
  - Daily VI 상태 CSV에 하락 VI 종목도 정상 포함됨.
  - 실거래 핵심 경로 변경 — 다음 재시작 후 라이브 검증 필요.

## [2026-06-02] 80c8f44
- **Category**: fix
- **Title**: inquire-balance 페이지네이션 500 에러 수정 — tr_cont 헤더 기반 연속 판정
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **증상**: 재시작 직후 [시작잔고조회] 500 Server Error.
     URL에 `CTX_AREA_FK100=63614390^01^N^N^01^01^N^+공백패딩` 값으로 2차 연속호출 발생.
  2. **근본원인**: KIS `inquire-balance`는 다음 페이지가 없어도 body의 `ctx_area_fk100`에
     `CANO^ACNT^...`+공백 패딩을 채워 반환 → `strip()` 후에도 비어있지 않아
     body ctx 기반 종료 판정 실패 → 쓰레기 ctx로 2차 조회 → 500.
     (미체결취소 `_inquire_psbl_rvsecncl`과 동일 클래스 버그. 아침 단일페이지 호출 시엔 패딩 미발생하여 잠복.)
  3. **수정**:
     - `_get_balance_page`에 `tr_cont` 요청헤더 송신 파라미터 추가 (초기 `""`, 연속 `"N"`).
     - 반환 튜플을 5요소로 확장: `(out1, out2, body_ctx_fk, body_ctx_nk, resp_tr_cont)`.
     - 두 페이지네이션 루프(시작잔고 `_query_and_print_balance`, str1_sell 잔고 `_get_balance_holdings`)에서
       응답 `tr_cont`가 `'F'`/`'M'`일 때만 연속 진행, body ctx 패딩 신뢰 제거.
     - 단일페이지 호출부 2곳(`_run_closing_balance_verification`, `_get_account_available_cash`)도
       5요소 unpack으로 갱신.
  4. **점검**: `ws_realtime_trading.py` 내 ctx 기반 페이지네이션은 두 함수뿐,
     둘 다 tr_cont 기반으로 통일됨 (다른 취약 루프 없음).
- **Impact**:
  - 재시작 시 [시작잔고조회] 500 에러 제거. 페이지네이션 연속 판정이 KIS 응답 헤더 기반으로 일원화됨.
  - 단일페이지 호출부도 5요소 unpack 통일로 향후 리팩토링 오류 방지.

## [2026-06-02] 55b47ae
- **Category**: fix / feat
- **Title**: VI 발동/해제 감지 근본수정 (상태전이 판정) + vi_cls_code parquet 저장
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **fix — VI 발동/해제 감지 로직 근본수정** (`_on_market_status_krx`, ~L8187):
     - 근본원인: KIS vi_cls_code 실측값은 발동='Y'/해제='N' 인데, 기존 코드는 0/1/2/3 숫자를
       가정해 `vi_cls != "0"` 으로 발동 판정 → 해제값 'N' 도 `!= "0"` 이라 발동으로 오판.
       결과: [VI해제]/_vi_exp_sub_restore 가 한 번도 안 타고, 5분 fallback(_vi_exp_sub_ts 300초)
       으로만 실시간체결 복귀 → VI 해제 후 최대 ~5분간 예상체결에 머무름.
       증거: 260529 로그 — [VI발동] 4건 / [VI해제] 0건, 발동로그에 vi_cls=N 2건 혼입.
     - 수정: 단순 문자열변화 비교 대신 '상태전이' 판정 도입.
       `is_active = vi_cls not in ("N","0","")` / `was_active = prev_vi not in ("N","0","")`.
       비활성→발동(is_active and not was_active) = _vi_exp_sub_switch(예상체결),
       발동→비활성(was_active and not is_active) = _vi_exp_sub_restore(실시간체결).
       효과: (a) 해제 즉시 정상복귀, (b) 재발동(Y→N→Y) 자연 처리, (c) keepalive 선캐싱('0')·
       반복 동일프레임 허위 전이 방지.
     - 검증근거: test_vi_wss_cycle.py 260602 라이브 확인(011300·013720·145670 사이클).
     - ※ **재시작 후 라이브 재검증 필요** (커밋 시점 실행 중 프로세스는 구코드).
  2. **feat — vi_cls_code parquet 컬럼 추가** (ingest_loop, ~L12630):
     - 기존 vi_yn(Y/공백, _vi_active_codes 기반)만 저장, vi_cls_code 원값 미보존.
     - `pl.lit(_vi_cls_cache.get(code,"")).alias("vi_cls_code")` 추가.
       H0STMKO0 마지막 수신값 그대로 보존 (발동='Y'/해제='N', 미구독 종목="").
     - 스키마 안전: diagonal_relaxed 병합 → 기존 part 와 혼재 시 null 채움 자동 처리.
- **Impact**:
  - VI 해제 시 실시간체결 즉시 복귀(기존: 5분 fallback). 매도 타이밍 지연 해소.
  - 재발동 사이클(발동→해제→재발동) 자연 처리 — 추가 코드 불필요.
  - parquet에 vi_cls_code 원값 보존 → VI 분석 데이터 품질 향상.

## [2026-06-02] 23b8471
- **Category**: feat
- **Title**: test_vi_wss_cycle.py — 멀티종목 동시 모니터링 + 실전 VI 감지 절차화 + 라이브 검증 확장
- **Files**: `test_vi_wss_cycle.py`
- **Changes**:
  1. **멀티종목 모니터링**: `latest_active_vi_code()` 대체 → `active_vi_rows()` 신설.
     시작 시점 vi_status CSV '발동중 전 종목' 스냅샷 1회 고정(실행 중 새 VI 추가 없음, 무한 누적 방지).
     종목별 독립 상태머신(mode: exp/real, released_at, done, await_first_real) 도입.
  2. **실전 VI 감지 절차화**: `run()` 루프 전에 종목별 3단계 흐름 절차화.
     - ① `rest_price_check()`: 현재가조회(FHKST01010100) `vi_cls_code` 반환 + 감지판정 로깅 (기존 best-effort 로깅 → 반환값 있는 감지 함수로 변경)
     - ② CSV 발동시각/종류/발동가/기준가/괴리율 확인 로깅
     - ③ 장운영정보(H0STMKO0) + 예상체결(H0STANC0) 구독
  3. **종목별 사이클 상태전환**: 발동→예상체결(H0STANC0), 해제(vi_cls='N')→실시간체결(H0STCNT0) 전환,
     재발동(vi_cls='Y')→예상체결 재전환. 해제 후 POST_RELEASE_HOLD=60초 관찰 뒤 종목별 완료,
     전 종목 완료 시 WSS 종료.
  4. **#2 단일가 일괄체결 명시 로그**: 해제 직후 첫 H0STCNT0 틱 = 동시호가 단일가 일괄체결.
     "N주 @ 단가 (금액) | ACML_VOL/VI_STND_PRC" 형식으로 1회 명시.
  5. **#1 파싱이상 진단**: 멀티레코드인데 필드 수가 컬럼 수의 배수가 아니면(정렬 어긋남/손상 프레임) raw 덤프.
     단일 레코드 짧음(H0STMKO0 10/11, EXCH_CLS_CODE 미전송)은 정상 패딩 처리(노이즈 제외).
  6. **`_send_sub(tr_type)` 헬퍼**: 기존 `subscribe()` 대체, tr_type '1'=구독/'2'=해제 통합.
     `_rec_code()` 헬퍼로 ANC/MKO(소문자)/CNT(대문자) 레코드에서 종목코드 추출 통일.
- **Impact**:
  - 단일 종목 → 발동중 전 종목 동시 모니터링으로 일괄 사이클 검증 가능.
  - 현재가조회 vi_cls_code 로 REST 기반 VI 실시간 감지 절차 실전 검증 완료.
  - 단일가 일괄체결 체적/금액 자동 출력으로 해제 순간 수량 확인 용이.
  - **260602 라이브 검증**: 013720 THE CUBE&(정적VI) — vi_cls_code='Y' 감지 → 구독 → 해제(vi_cls='N') → 단일가 일괄체결 3,106주@460(1,428,760원) → 60초 후 완료 종료.
    vi_cls_code sticky 아님 확정(발동중='Y'/정상='N', 005930 반대검증).

## [2026-06-02] 3b46a5b
- **Category**: fix / feat
- **Title**: test_vi_wss_cycle.py — VI 사이클 WSS 라이브 검증, 버그 3건 수정 + 기능 2건 추가 + 파싱 정확화
- **Files**: `test_vi_wss_cycle.py`
- **Changes**:
  1. **fix — `pick_account` KeyError('users')**: `load_config()`(가공 dict, raw "users" 없음) 대신 `config.json` 원본을 직접 `json.load` → V2 멀티계좌(`users→default_user→accounts`) 구조 파싱 정상화.
  2. **fix — `AttributeError: kis_auth.data_fetch` 없음**: `domestic_stock_functions_ws` import 전에 `sys.modules["kis_auth"] = ka` 별칭 등록 (메인 ws_realtime_trading.py:676 · Daily_inquire_vi_status.py:44 와 동일 패턴).
  3. **fix — stale 종목 자동선택**: live 디렉토리의 `vi_status_merged_*.csv`가 사전순으로 분당 파일보다 뒤라 `sorted()[-1]`에 잘못 걸리던 문제 → `_latest_vi_csv()` 신설, 당일 `vi_status_YYMMDD_HHMM.csv` 패턴만 선택 + merged 파일 제외.
  4. **feat — 장중 자동선택**: 09:00~15:20(KST) 실행 시 최신 VI 리스트의 마지막 발동중 종목을 즉시 자동 선택(`_is_market_hours`, KST timezone 적용). `--code` 지정 시 우선.
  5. **feat — 해제 1분 후 WSS 자동종료**: VI 해제 감지 시각 기준 `POST_RELEASE_HOLD=60.0`초 경과 시 연결 종료(해제→실시간체결 전환만 관찰 후 마무리).
  6. **fix — `parse_data_frame` 컬럼 미달 패딩**: 메인(`kis_auth_llm`)과 동일하게, KIS가 마지막 컬럼을 생략해 보낼 때(H0STMKO0 정의 11 / 실수신 10 — EXCH_CLS_CODE 미전송) 빈값 패딩 → 이름 기반 파싱이 `_raw_fields`로 깨지지 않도록. 멀티건은 ncols 단위 분할+패딩.
  7. **fix — VI 해제 판정 조건**: 실측상 해제 시 `vi_cls_code='N'`(빈값/0 아님 — 260602 라이브 확인). 기존 `vi in ("","0")`이 'N'을 놓치던 것을 `vi in ("N","0","")` 로 수정('Y'=발동중).
  8. **docs — docstring 갱신**: approval_key/동시실행 섹션을 260602 현행으로 수정 — a2 WSS 슬롯 비어있음(260601 Daily_vi WSS 비활성), a1(`--account main`) 실행 금지 사유 명시.
- **Impact**:
  - 실행 즉시 죽던 3가지 오류(KeyError/AttributeError/stale 종목) 모두 수정 → 정상 실행 가능.
  - 장중 실행 시 최신 VI 종목 자동선택 → 수동 `--code` 없이 바로 검증 가능.
  - VI 해제 후 60초 후 자동종료로 불필요한 장시간 대기 없이 사이클 완결.
  - **260602 라이브 검증 결과**: 011300 우성머티리얼스 full cycle 확인 — 예상체결(H0STANC0) 수신 → `vi_cls_code='N'` 해제감지 → H0STCNT0 실시간체결 전환 → 60초 후 자동종료. H0STMKO0 named 파싱 정상. approval_key 충돌 0.

## [2026-06-02] 0e4dfdf
- **Category**: fix / diag / feat
- **Title**: 미체결취소 연속조회 500 에러 수정 + decode-skip 근본원인 진단로깅 + 08:45 리마인더
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **fix — `_inquire_psbl_rvsecncl`: 미체결취소 조회 500 에러 근본수정**:
     - 근본원인: 연속조회 종료 판정을 응답 body의 `ctx_area_fk100` 유무로 했는데, KIS는 마지막 페이지에도 `ctx_area_fk100`에 `"CANO^ACNT^"` + 공백패딩 같은 쓰레기 값을 채워 반환 → 코드가 다음 페이지 있다고 오판 → 가짜 연속키로 재조회 시 KIS가 500 Internal Server Error 반환.
     - 로그 증상: `[미체결취소] 실패: 500 Server Error ... CTX_AREA_FK100=43444822^01^...`
     - 수정: 연속조회 종료 판정을 응답 헤더 `tr_cont` 기반으로 변경. 초기 요청 `tr_cont=""`, 응답 `tr_cont`가 `F`/`M`일 때만 다음 페이지 존재 → 연속 요청 `tr_cont="N"`. `D`/`E`/공백이면 종료. body의 ctx 값은 연속 요청 파라미터로만 활용.
  2. **diag — `_lenient_assembler_get`: decode skip(UTF-8 깨짐) 근본원인 진단 로깅 추가**:
     - 배경: decode skip 이 260511 경 발생 시작, 260601 급증. 전 일자 예외 없이 08:50~08:59 장전 예상체결(H0STANC0) 구간에만 집중(09:00 이후 0건). 에러 사유(invalid start byte / unexpected end of data)가 CP949 한글 유입 정황과 일치 → 확정용 진단 필요.
     - 추가: `UnicodeDecodeError` 발생 시 최초 40건은 rate-limit 무시하고 상세 기록 — `tr_id`(프레임 헤더 추출) / `len` / `reason` / `offset` / 깨진바이트 hex / 주변 ±16바이트 hex / `cp949` 재디코드 샘플.
     - 40건 소진 후엔 기존 30초 1회 요약으로 폴백. **근본수정 아님 — 진단 단계**.
     - 로그 패턴: `[ws][utf8-diag #N] tr_id=H0STANC0 ... cp949='...'`
  3. **feat — `scheduler_loop`: 08:45 진단로그 확인 TODO 리마인더 (1일 1회)**:
     - 모듈 플래그 `_diag_reminder_done` 추가.
     - 08:45~08:50 구간에서 1회 텔레그램 알림 발송 — "08:50~08:59 H0STANC0 구간의 `[ws][utf8-diag #N]` 라인에서 `cp949='...'` 값 확인(멀쩡한 한글=CP949 인코딩 유입 확정)" 안내.
- **Impact**:
  - `_inquire_psbl_rvsecncl` 호출 시 더 이상 500 에러 발생하지 않음. 미체결취소 조회 안정화.
  - decode skip 발생 시 다음날 08:50~08:59 구간에서 `[ws][utf8-diag]` 상세 로그 생성 → CP949 유입 확정 또는 단순 truncation 판별 가능. 이후 근본수정 방향 결정 예정.
  - 08:45 리마인더로 진단 결과 확인 누락 방지.

## [2026-06-01] 05144e0
- **Category**: fix
- **Title**: Daily_vi 장운영정보 파싱오류 수정 + WSS 중복구독 비활성화, VI 사이클 점검 테스트 추가
- **Files**: `Daily_inquire_vi_status.py`, `test_vi_wss_cycle.py` (신규)
- **Changes**:
  1. **Daily_inquire_vi_status.py — `_on_ws_result` 파싱오류 수정**:
     - 기존 `df.iterrows()` (pandas API) → SDK가 넘기는 df는 polars DataFrame이라 매 수신마다 `'DataFrame' object has no attribute 'iterrows'` 예외 발생.
     - `df.iter_rows(named=True)` (polars) + pandas 폴백으로 교체. 기존엔 장운영정보 수신 0건 + 구독 해제 불가 → MAX SUBSCRIBE OVER 누적이 근본 원인이었음.
     - 수신 시 전체 필드(dict) 로깅 추가 — H0STMKO0 실제 컬럼 구조 파악 및 VI 발동 필드 확인용.
  2. **Daily_inquire_vi_status.py — `_init_ws_a2()` 호출 비활성화**:
     - H0STMKO0 수신·처리는 `ws_realtime_trading.py`(`_on_market_status_krx`, `result.to_dicts()`, 슬롯관리)가 담당. 260529 정상일 로그로 메인 핸들러 파싱예외 0건 확인.
     - 이 파일에서 중복 구독 불필요 → `_init_ws_a2()` 주석처리. 이 파일은 REST VI현황 CSV 저장 전담.
     - `_kws=None`이므로 `_mk_subscribe_add`도 자동 no-op. WSS 함수들은 dormant 상태(추후 완전삭제 가능).
  3. **test_vi_wss_cycle.py 신규 (219줄)**:
     - VI 발동/해제 사이클 WSS 검증 전용 테스트. 운영 무영향.
     - 최신 vi_status CSV 발동중 종목 자동선택 → REST 현재가 `temp_stop_yn` 등 VI 관련 필드 확인 → H0STMKO0(장운영정보) + H0STANC0(예상체결) 구독 → 수신 전체 필드 타임스탬프 로깅 → VI 해제 감지 시 H0STCNT0(실시간체결) 전환 → 해제 직후 체결 도착 검증.
     - 계정 기본 a2(syw_2), `out/logs/test_vi_wss_*.log`로 기록.
     - approval_key 충돌 주의사항 주석 명시 (단독 실행 권장).
- **Impact**:
  - Daily_inquire_vi_status.py 장운영정보 수신 0건 버그 수정 — 단, 메인이 담당하므로 이 파일의 WSS는 비활성화 상태.
  - MAX SUBSCRIBE OVER 누적 원인 제거 (파싱 실패로 `_mk_subscribe_remove` 미호출되던 문제 해소 — 단 WSS 비활성화로 완전 무관).
  - test_vi_wss_cycle.py로 VI 발동/해제 전체 사이클을 라이브 환경에서 점검 가능한 독립 도구 확보.

## [2026-06-01] 6babd24
- **Category**: fix
- **Title**: 260601 WSS 종일 두절 근본수정 — approval_key 계좌별 캐시 분리 + 무효키 self-heal
- **Files**: `kis_auth_llm.py`, `ws_realtime_trading.py`, `rules/trading_project_main_plan.md`
- **Changes**:
  1. **근본원인 (260601 격리 재현 확정)**:
     - KIS는 같은 appkey로 새 approval_key 발급 시 직전 키를 즉시 무효화(최신 1개만 유효).
     - 기존 캐시가 날짜별 단일 파일(`KIS_approval_YYYYMMDD`)이라 a1/a2 두 계좌가 동일 파일을 서로 덮어써 키 충돌 → 무효 키로 WSS 접속 시도 → `invalid approval` → bare 1006 (dwell≈1.1s) 영구 반복. 재시작해도 동일 캐시 파일을 재사용하므로 복구 불가.
  2. **kis_auth_llm.py**:
     - approval_key 캐시를 **appkey(계좌)별+날짜별 분리** 저장(`config/approval/KIS_approval_{md5(appkey)[:12]}_{YYYYMMDD}`).
     - **원자적 쓰기(temp→os.replace)** 적용 — 반쪽 읽기 방지.
     - `auth_ws(force_new=True)` 파라미터 추가: 재발급 전 파일 재읽기 가드 — 다른 주체가 이미 갱신 시 채택(교차무효화 핑퐁 방지), 파일이 stale과 동일할 때만 실제 발급.
     - 모듈 lock(`_auth_ws_lock`) 추가로 동일 프로세스 내 발급 직렬화.
     - `_approval_appkey_tag` / `_approval_tmp_path` / `save_approval_key` / `read_approval_key` 가 appkey 인자 받도록 수정.
  3. **ws_realtime_trading.py**:
     - `run_ws_forever`: `dwell<15s` (무효 키 의심) 감지 시 `run_ws_forever._force_new_approval=True` 세트 → 다음 재접속에서 `ka.auth_ws(force_new=True)` 강제 재발급 (self-heal).
     - `[REST보충]` 로그에 `temp_stop_yn` 값 출력 추가 (VI 감지 진단용).
  4. **rules/trading_project_main_plan.md**:
     - "WSS approval_key 관리 표준 (260601 사고 후 수립) — ★WSS 두절 시 1순위 점검" 섹션 추가 (증상식별 방법 + 원리 + 4규칙).
- **Impact**:
  - a1/a2 계좌가 서로의 approval_key를 무효화하는 교차 충돌 영구 제거.
  - dwell<15s 반복 시 다음 재접속에서 자동 강제 재발급 → invalid approval 상태에서 수동 개입 없이 self-heal 가능.
  - 원자적 쓰기로 동시 쓰기 경쟁 상태 제거. 모듈 lock으로 동일 프로세스 내 중복 발급 차단.
  - **검증**: py_compile 통과(본체 + 의존 스크립트 3종). 격리테스트: appkey별 캐시 발급/재사용/force_new 신규발급→접속 SUBSCRIBE SUCCESS (5초 생존, 1006 없음) / 핑퐁가드 정상.

## [2026-05-26] dc4a9fa
- **Category**: feat
- **Title**: 휴일 가드 추가 — 휴장일 idle 프로세스 방지
- **Files**: `kis_1m_API_to_Parquet_NXT.py`, `ws_log_monitor.py`
- **Changes**:
  1. **kis_1m_API_to_Parquet_NXT.py**: main() 진입 직후 `is_holiday()` 검사.
     휴장일이면 다운로드 생략 + 텔레그램 통지 후 return.
     `is_holiday()` 예외 시 경고 로그만 남기고 정상 진행(안전망).
  2. **ws_log_monitor.py**: `run()` 시작부에 `is_holiday()` 검사.
     휴장일이면 텔레그램 통지 후 즉시 return — 종일 idle 루프 방지.
- **Impact**: 휴장일에 불필요하게 기동된 다운로드/모니터 프로세스가 아무 것도 하지 않고 깔끔하게 종료됨.

## [2026-05-26] 04f9e92
- **Category**: fix
- **Title**: 5/26 09:01 WSS 56분 두절 근본수정 — scheduler 정지 시에도 watchdog/재시작 보장
- **Files**: `kis_auth_llm.py`, `ws_realtime_trading.py`
- **Changes**:
  1. **kis_auth_llm.py — send_request timeout 추가** (line 1069):
     `fut.result()` → `fut.result(timeout=10.0)`.
     asyncio 루프 wedge 시 10초 후 TimeoutError 발생, `_send_subscribe` 의 try/except 가 잡아 락 해제.
  2. **ws_realtime_trading.py — 4가지 수정**:
     - `_trigger_ws_rebuild` 연속실패 카운터(`_rebuild_consecutive_fail_count` / `_REBUILD_FAIL_EXIT_LIMIT=5`).
       임계 초과 시 텔레그램 통지 + `os._exit(2)` → runner 자동재시작 유도 (rebuild 무한루프 탈출).
     - 무수신 watchdog `_wss_norecv_watchdog_loop` 을 독립 데몬 스레드로 분리.
       scheduler_loop 정지와 무관하게 `_last_any_recv_ts` 만 보고 독자 판정.
     - watchdog 활성 시간창 `dtime(9,30)` → `dtime(9,0)` 확대 (개장 직전 구독 분리 사각지대 제거).
     - [체결통보-체결] 매도 발송부에 매수가/PNL/사유 첨부.
       `_str1_sell_state[code]["buy_price"]` / `["sell_reason"]` 조회 후 텔레그램 본문에 추가.
- **Impact**:
  - scheduler_loop 가 wedge 되어도 watchdog 독립 스레드가 180초 무수신 시 `os._exit(2)` 보장.
  - send_request timeout 으로 락 점유 무한 블로킹 제거 → 정상 케이스에서 10초 내 복구.
  - 매도 텔레그램에 사유/매수가/PNL 이 함께 표시되어 수동 확인 불필요.

## [2026-05-26] 6f0c2b6
- **Category**: chore
- **Title**: ws_realtime_watchdog.py 좀비 인스턴스 reaper 스크립트 추가
- **Files**: `scripts/cron_start_watchdog.sh` (신규)
- **Changes**:
  - 매일 KST 08:25(UTC 23:25, 월~금) cron 실행 wrapper.
  - 기존 누적 인스턴스를 SIGTERM → 5초 대기 → SIGKILL 로 정리 후 새 인스턴스 nohup 기동.
  - cron_start_log_monitor.sh 패턴을 그대로 차용.
  - crontab 등록(`25 23 * * 0-4`)은 시스템 crontab이라 git 추적 외.
- **Impact**: 워치독 프로세스 좀비 누적 차단 — 매일 교체 기동으로 메모리 누수·슬롯 충돌 예방.

## [2026-05-26] 54b9be6
- **Category**: docs
- **Title**: 5/26 WSS 56분 두절 근본원인 분석 문서 추가
- **Files**: `ws_monitoring_research_260526.md` (신규)
- **Changes**:
  - research-analyst 산출물. 사고 로그(PID 1495038) 기반 원인 4개 분류:
    Issue 1 — `send_request` 영구 블로킹 (핵심 원인).
    Issue 2 — watchdog 이 scheduler 와 같은 스레드여서 동반 정지.
    Issue 3 — `_trigger_ws_rebuild` timeout 반복 무한루프 탈출 조건 없음.
    Issue 4 — 광진실업 매도 체결 통보에 사유/매수가 누락.
  - fix(wss-recovery) 커밋의 설계 근거 문서로 보관.
- **Impact**: 재발 방지 설계 검증 자료 및 향후 유사 사고 진단 참고 기록.

## [2026-05-22] 521f211
- **Category**: refactor
- **Title**: kis_utils 공용유틸 일원화 — 수수료 상수·손익계산을 단일 출처로 이전
- **Files**: `kis_utils.py`, `ws_realtime_tr_str1.py`, `ws_realtime_trading.py`
- **Changes**:
  1. **kis_utils.py — 공용 심볼 신규 추가**
     - `FEE_RATE_MARKET_BUY/SELL`, `FEE_RATE_LIMIT_BUY/SELL` 상수 (수수료·세금·슬리피지, 전략·실행·시뮬 공용 단일 출처)
     - `get_tick_size(price, market)` 공개 래퍼 신설 (`_kr_tick_size` 내부 함수 노출)
     - `calc_sell_pnl(buy_price, sell_price, qty, market)` 함수 str1에서 이전 (price_plus_n_ticks 다음에 배치)
  2. **ws_realtime_tr_str1.py — 전략 판단만 원칙 적용**
     - `FEE_RATE_MARKET/LIMIT_BUY/SELL` 상수 정의 4개 제거 (kis_utils 이전)
     - `calc_sell_pnl` 함수 정의 제거 (kis_utils 이전)
     - import 주석·모듈 docstring 갱신 (round_to_tick/price_plus_n_ticks만 유지)
  3. **ws_realtime_trading.py — import 정비 + 핫스왑 정리**
     - kis_utils import에 `calc_sell_pnl` 추가 (직접 참조)
     - str1 import 블록에서 `calc_sell_pnl`, `FEE_RATE_MARKET_SELL` dead-import 제거
     - `_check_strategy_swap` 핫스왑에서 `calc_sell_pnl` 재바인딩 제거 (공용 유틸은 전략 스왑 대상 아님)
- **Impact**: Str2 서버 포팅 선결 조건 충족. 전략 교체 시 수수료·손익 계산 결합 문제 제거. 향후 신규 전략 파일은 kis_utils에서 import만 하면 되며 중복 정의 불필요.

## [2026-05-22] fbe62d3
- **Category**: fix
- **Title**: H0STMKO0 keepalive 활성화 + WSS 연결 후 재구독 + CB/사이드카 처리 분리
- **Files**: `ws_realtime_trading.py`, `rules/trading_project_main_plan.md`
- **Changes**:
  1. **[A] `_mkstatus_sub_add` 보류 처리 (`_mkstatus_pending` 신설)**
     - WSS 미연결 시점(예: 08:28:10 보유종목 H0STMKO0 구독 호출)에 `_active_kws is None`이면
       조용히 no-op이 되어 구독이 영영 안 됐던 버그 수정.
     - WSS 미연결 시 신규 세트 `_mkstatus_pending`에 보류 후 `[H0STMKO0] WSS 미연결 → N건 보류` 로깅.
     - 연결 시 `run_ws_forever` open_map 빌드에서 흡수.
  2. **[B] `run_ws_forever` open_map 빌드에 H0STMKO0 명시 (재)구독 추가**
     - `_MKT_KEEPALIVE_INITIAL`(삼성전자/카카오게임즈)은 정의만 있고 실제 구독 코드가 없는
       dead config였음. 부팅·재연결마다 keepalive(KOSPI/KOSDAQ 각 1) + 보류분 + 기존 동적코드를
       `H0STMKO0_MAX_SLOTS(5)` cap·우선순위(보유>VI>keepalive>기타) 적용해 `market_status_krx`로 구독.
     - keepalive 부작용 방지: `_code_market_map.setdefault`, 허위 `[VI해제]` 차단용
       `_vi_cls_cache.setdefault(c, "0")`, `_last_mkop_event.setdefault` 추가.
  3. **[C] `_on_market_status_krx` CB/사이드카 처리 분리**
     - 사이드카(187/397=매수·매도 프로그램매매 호가효력정지, 388/398 해제)는 개인 매매와 무관.
       정보성 로그·텔레그램 알림만 남기고 시장 전파(REST 폴링 생략/재개) 발동을 제거.
     - 시장 전파는 서킷브레이커류(174=발동, 184=개시, 164=시장임시정지 / 해제: 175·185)만 발동.
     - 사이드카 해제(388/398) 수신 시 CB 시장전파 해제 로직에 진입하던 잠재버그 수정.
  4. **`rules/trading_project_main_plan.md`**: H0STMKO0 운영 원칙(CB 감지 목적, 사이드카 vs CB 처리 분리) + mkop별 처리 기준 표 추가(intent-tracker).
- **Impact**:
  - 부팅 후·재연결 후 항상 H0STMKO0 최소 2종목(keepalive) 구독 보장 → CB 감지 채널 단절 근원 차단.
  - 사이드카 수신 시 REST 폴링 생략이 발동되던 오동작 제거 → 사이드카 중에도 시장 정상 시세 수신 유지.
  - 5/21·5/22 코스닥 매수 사이드카 미감지 원인(H0STMKO0 장중 0) 3중 버그 수정.

---

## [2026-05-22] 7ca46d3
- **Category**: refactor
- **Title**: `[시간외] 체결가 수신 완료` 로그 문구 정리 → `[시간외] 10분단위 실시간체결가 구독 종료`
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **시간외 `OVERTIME_REAL→OVERTIME_EXP` 전환 로그(`:4664`) 문구 변경**
     - `[시간외] 체결가 수신 완료 → 예상체결가 복귀`
       → `[시간외] 10분단위 실시간체결가 구독 종료 → 예상체결가 복귀`
     - `체결가`(내 주문 체결 `[체결통보-체결]`과 혼동) 제거, `실시간체결가`(정해진 시장 피드 용어) + 10분단위 단일가 슬롯 의미 명확화.
- **Note**: 해당 경로(`OVERTIME_REAL`)는 `OVERTIME_SINGLE_PRICE_BUY=False`(`:148`)로 **현재 비활성**(프로그램 16:01 조기종료, `:4596`). 런타임 미출력 — 재활성화 대비 가독성 목적의 코드 정리. 점검 중 발견(실제 런타임 로그 아님).

## [2026-05-21] f236308
- **Category**: refactor
- **Title**: `[체결가 수신]` 로그 용어 명확화 → `[WSS전환후 첫 시세 정상수신]`
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **구독 전환 후 첫 실시간 시세 수신 확인 로그 (`:11613`) 용어 변경**
     - 라벨: `[체결가 수신]` → `[WSS전환후 첫 시세 정상수신]`
     - 가격 필드: `체결가=` → `현재가=` (필드 소스 `stck_prpr`, `[REST보충]` 로그와 일관)
     - 주석에 "시장 시세(현재가), 내 주문 체결[체결통보-체결]과 무관" 명시.
- **Impact**: 시장 시세 관찰 로그가 내 주문 체결가(`cntg_unpr`)로 오인되던 혼동 제거.
  배경: 5/21 녹십자엠에스(142280) — 내 시초가 체결 5,820(체결통보) vs 직후 시장 시세 5,840(`[체결가 수신]`)을 같은 "체결가"로 봐서 혼동.

## [2026-05-21] ab5465c
- **Category**: perf
- **Title**: 매도 발동→주문 내부 지연 계측([sell_latency]) + 핫패스 운영 원칙 KPI 명문화
- **Files**: `ws_realtime_trading.py`, `rules/trading_project_main_plan.md`
- **Changes**:
  1. **[계측] `_enqueue_str1_sell` — `enq_ts: time.time()` 큐 항목에 추가**
     - 매도 발동 시각을 큐 dict 에 기록. 워커 측 지연 계산의 기준점.
  2. **[계측] `_str1_sell_worker` — `_sell_order_cash` 호출 직전 [sell_latency] 로그 출력**
     - `[sell_latency] {종목}({코드}) 발동→주문 {ms}ms` 형식으로 ms 단위 계측.
     - enq_ts 가 없는 기존 잔여 항목은 안전하게 무시(`.get("enq_ts")` 조건 분기).
     - 목적: 핫패스에 블로킹 REST/텔레그램 동기 호출이 재유입되는 회귀를 상시 감시.
  3. **[문서] `rules/trading_project_main_plan.md` — "주문 핫패스 지연 금지" 운영 원칙 추가**
     - 금지 항목 명시: REST 잔고조회 동기 호출, 텔레그램 동기 전송, 기타 블로킹 네트워크 작업.
     - KPI: 발동→주문 내부 지연 목표 ≤50ms. 수백 ms 이상 = 블로킹 회귀 WARN.
     - 관측점: `[sell_latency]` 로그, log_monitor 비정상 대기값 감지 시 WARN.
     - KPI 모니터링 표에 `[sell_latency] 수백 ms 이상 → WARN` 항목 추가.
     - 변경 이력 섹션에 원칙 수립 경위(배경: 아이진 2.5초 지연, 커밋 25f936e) 추가.
- **Impact**: 핫패스 블로킹 회귀를 실시간 수치로 탐지 가능. 주문 지연 원인 불명 구간 없음. "금지 원칙 + KPI" 가 문서화되어 향후 코드 리뷰 기준으로 활용 가능.

## [2026-05-21] 25f936e
- **Category**: perf
- **Title**: str1 매도 핫패스 지연 ~2.5초 제거 (REST 재조회 + 텔레그램 동기전송 제거)
- **Files**: `ws_realtime_trading.py`, `telegMsg.py`
- **Changes**:
  - **근본 원인**: 260521 아이진(185490) 매도 로그 분석 → 발동(큐 등록) 28.136 → KIS 주문 ~30.660, 내부 ~2.5초 직렬 지연. 원인 = 주문 직전 핫패스의 REST 잔고 재조회 ~1.5초 + 텔레그램 동기 전송 ~1.0초.
  - **[ws_realtime_trading.py] REST 잔고 재조회 제거**: `_str1_sell_worker` 매도 직전 `_get_balance_holdings()` 호출(TTTC8434R 페이지네이션 ×활성계좌) 삭제. 매도 수량은 `req["qty"]`(`_str1_sell_state`/시작 `balance_map` 시드값) 그대로 사용. 수량초과(APBK0400) 안전망(미체결 매도주문 취소→재주문 except 경로)은 유지.
  - **[ws_realtime_trading.py] 주문 직전 텔레그램 동기전송 제거**: `_notify(api_call_msg, tele=True)` + 중복 `logger.info` → `logger.info` 1회로 교체.
  - **[ws_realtime_trading.py] 주문 후 통지 비동기화**: 접수완료 통지를 `_notify_async` (신규 헬퍼)로 변경. 파일 로그 + TELE_LOG_PATH(시간정보 포함) 기록은 동기 즉시 유지, 텔레그램 HTTP 전송만 데몬 스레드 fire-and-forget → 워커가 다음 매도 요청 즉시 처리.
  - **[ws_realtime_trading.py] `_notify_async` 신규**: 스레드 생성 실패 시 동기 fallback 경로 포함. 데몬 스레드이므로 프로세스 종료 시 자동 회수.
  - **[telegMsg.py] `tmsg` timeout 추가**: `requests.post(...)` 에 `timeout=(3, 3)` (connect 3초/read 3초) 추가. 텔레그램 장애 시 어떤 호출처에서도 무한 블록되지 않도록 방어.
- **Impact**: 발동→KIS 주문 구간에서 ~2.5초 직렬 블로킹 제거. 동시다발 매도 신호 발생 시 다음 종목 처리 지연도 해소. 텔레그램 통지 및 파일 로그 누락 없음(동기 기록 보장).

## [2026-05-19] 57a1405
- **Category**: feat
- **Title**: 15:30 hot path 보호 + 08:30 시간외종가 재작성 + 계측 로그
- **Files**: `kis_auth_llm.py`, `ws_realtime_trading.py`
- **Changes**:
  - **본 커밋의 실질 내용**: 15:30 hot path 보호 (1499ms spike 해소)
  - **[15:30 hot path 보호]** `_calc_indicators`: CLOSE_REAL/OVERTIME_*/STOP/EXIT 모드 즉시 return. 잔고검증 15:31 → 15:32 지연 (구독전환 후 안정화). parquet flush 가드 15:30:00~15:31:00 (ingest+writer 양쪽).
  - **[08:30 시간외종가 재작성]** 어제 state → 오늘 `_morning_target_codes`로 전환. False 시 결과 표도 함께 스킵. `_log_morning_extra_result` / `_morning_extra_placed` 신규 추가.
  - **[print_table]** 전일 상한가 모니터링 대상 종목 print_table 시작 시 즉시 출력 (09:00 이전 텔레그램 발송 가드).
  - **[hold_map 분리]** 시작 잔고조회 `hold_map` 분리: 표시=전계좌, hold_map=trade_enabled 만 (`hold_map_trade_only` 파라미터 신규).
  - **[WSS / kis_auth_llm.py]** H0STCNI0 diag WARNING → INFO. `pl.read_csv quote_char=None` (따옴표 페이로드 차단). parse error 로그에 tr_id + raw_prefix 추가.
  - **[계측 로그]** `[ingest_lag]` 30s, `[ind_calc]` 60s 성능 계측 로그 추가 (토글 가능).
  - **[가독성]** `[종가매수주문완료]` 총 소요시간 표시, "rows save done" 통일.
- **Impact**: 15:30 hot path에서 발생하던 1499ms spike 해소. 이 버전이 현재 안정적으로 운영되고 있는 파일.

## [2026-05-13] db98da9
- **Category**: fix
- **Title**: 260513 1007 frame error 차단 + SDK 폭주 재시도 차단 (좀비 세션 근본 원인 차단)
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  - **인과 chain**: 오전 1007은 자동 회복. 15:26 이후 1007 직후 SDK `__runner`가 동일 `approval_key`로 1초 간격 8회 폭주 재시도 → KIS throttle(invalid approval storm) → 슬롯 잠금 → 자기보호중단. 1007 자체가 아니라 *후속 재시도 패턴*이 진짜 원인.
  - **[A] UTF-8 lenient monkey-patch (L422~484)**: `WebSocketCommonProtocol.read_message` wrap → `UnicodeDecodeError` 발생 시 경고 로그 후 빈 문자열 반환. 추가로 `_ws_lp.codecs.getincrementaldecoder`를 lenient 버전으로 로컬 치환(글로벌 codecs 무변경) → fragmentation 경로에서도 `errors="replace"` 강제. 효과: 깨진 한글 1글자가 `?` 치환되어 1007 close 자체 발생 안 함.
  - **[B] max_retries=10 → 1 (L11973)**: `KISWebSocket(max_retries=1)` — SDK `__runner`의 retry 루프가 인스턴스 수명 동안 리셋 안 됨. 1007 후 10초 폭주 발생 경로 차단. 이제 exception 즉시 `kws.start()` 반환 → outer `run_ws_forever` backoff+`auth_ws` 재발급 루프 진입.
  - **[C] invalid approval → kws.close() 즉시 호출 (L12020~12035)**: `_on_system` 내 `"invalid approval"` 분기 끝에 `kws.close()` 추가. `_close_requested=True` 세팅으로 SDK `__runner` while 루프 즉시 종료 → KIS throttle 추가 악화 방지. max_retries=1의 이중 안전망.
- **Impact**: 이 패치 적용 후엔 1007 발생 시에도 appkey 재발급 의식(매일 수동 확인) 없이 자동 회복되어야 함. 잔여 검증: 실제 1007 케이스에서 운영 로그의 `[utf8-lenient] decode skip` 출현 여부 모니터링.

## [2026-05-13] 386ded6
- **Category**: fix
- **Title**: 260513 잠복 NameError 2건 복구 (round_to_tick import / v5 qualify 호가 누적값)
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  - **[L454] `round_to_tick` import 누락 복구**: `_setup_stop_limit_orders` 함수에서 상한가 29.5% 도달 시 스톱 지정가 주문 가격 계산(29%/28%/27% 호가 반올림)에 사용. 호출 첫 줄에서 즉시 NameError로 실패했을 코드 경로.
  - **[L10283~84] `check_uplimit_v5_qualify` 호출 키워드·변수명 교정**: `bid_sum_top5=bid_sum` → `bid_total=bid_total`, `ask_sum_top5=ask_sum` → `ask_total=ask_total`. 함수 실제 시그니처(`ws_realtime_tr_str1.py:691-692`)가 `bid_total`/`ask_total` 키워드를 받으며, `_check_uplimit_v4_from_tick` 내부 10142~10143에서 이미 `bid_total`/`ask_total`로 정의된 변수를 사용하도록 수정.
  - 두 건 모두 live trading 경로, try/except 마스킹으로 조용히 실패해왔을 가능성 있음. Pylance 진단으로 사전 발견 (py_compile PASS).
- **Impact**: 스톱 지정가 주문 가격 계산 정상화 + v5 매수 qualify 호가 누적값 정상 전달. 기존엔 해당 경로 진입 시 NameError로 try/except 흡수 후 매수 보류 상태였을 것.

## [2026-05-13] e3da250
- **Category**: fix
- **Title**: 260513 1007 → invalid approval throttle 사건 대응 — 장 중반 사망 + 재시작 불가 복구
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  - **사건 개요 (260513 15:26~15:28)**: KIS WSS 1007 frame error (`unexpected end of data at position 702`) → 동일 approval_key(`c91b6f6c-...`)로 invalid approval 8회 연속 수신 → handshake_fail 2회 → `_wss_self_stop` 호출 → `os._exit(0)` → `runner.sh`가 정상종료로 오인, 재시작 안 함 → 장 중반 프로세스 사망 방치.
  - **직접 사망 원인**: `os._exit(0)` — runner.sh의 exit_code 분기에서 0은 "정상종료"로 처리되어 watchdog 재시작 로직 비진입.
  - **수정 1** (`_wss_self_stop`, L≈11453): `os._exit(0)` → `os._exit(2)`. runner.sh의 `exit_code -eq 2` watchdog 강제종료 분기 진입 → 프로세스 재시작 보장.
  - **수정 2** (`_on_system` 내부, L≈11934): `"invalid approval"` 키워드 감지 시 `_main_last_invalid_approval_ts` 기록 + 30s 스팸가드 텔레그램 알림 발송.
  - **수정 3** (`run_ws_forever`, L≈11785): `attempt > 1` 재접속 분기에서 `auth_ws()` 호출 직전, `_main_last_invalid_approval_ts` 기준 60s 이내면 잔여 시간 backoff (0.1s 단위 stop_event 응답).
  - **수정 4** (`HANDSHAKE_FAIL_SELF_STOP_LIMIT`, L≈4130): 2 → 4 완화. 1007 throttle 폭주가 2 burst 안에 회복 불가한 상황에서 4 burst(약 60초+)로 invalid approval backoff 동작 시간 확보.
  - **수정 5** (전역, L≈4120): `_main_last_invalid_approval_ts: float = 0.0` 및 `MAIN_WSS_BACKOFF_INVALID_APPROVAL_SEC: float = 60.0` 신규 추가.
- **Impact**: 1007 frame error → invalid approval 폭주 패턴에서 (a) 프로세스 종료 후 runner가 반드시 재시작, (b) 재접속 시 throttle 해소까지 대기하여 새 approval_key 발급 유도. 잔여 리스크: 60s backoff가 불충분할 경우 운영 모니터링 후 `MAIN_WSS_BACKOFF_INVALID_APPROVAL_SEC` 값 상향 조정 필요.

## [2026-05-12] 2ec4aae
- **Category**: fix
- **Title**: 외부주문 auto 시간대 자동 매핑 + failed 처리 강건화
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **`_exec_external_order`**: `ord_type="auto"` 또는 빈값 시 현재 시각 기준 자동 분기. `<08:55` → 대기, `08:55~15:30` → 시장가(01)/지정가(00), `15:30~15:40` → 대기, `15:40~16:00` → 장후시간외종가(06), `16:00~18:00` → 시간외단일가(07, target_price 없으면 REST inquire_price 로 stck_mxpr 상한가 자동조회), `>=18:00` → 대기. 명시 ord_type(`pre_market`/`post_market`/`overtime`)은 기존 동작 보존. `_last_error` 필드로 계좌별 마지막 에러 보존 → `failed` result 에 사유 합산.
  2. **`_check_external_orders`**: `result.startswith("failed")` 분기 신규 추가 — `_failed_notified` 플래그로 1회만 텔레그램+로그 발송, config 유지. 실패 메시지에 종목명/수량/금액/result/재주문 가이드 포함. `result == "주문완료"` pop 직전 결과 로그 추가. 빈 result 재진입 시 `_failed_notified`/`_time_wait_logged` 플래그 reset.
  3. **`_cleanup_failed_external_orders_at_startup` (신규)**: 프로그램 시작 시 1회, 08:30 이전이면 config.json 의 `failed` 외부주문 삭제 (각 건 로그, 0건이면 무로그). `scheduler_loop` 진입 직후 호출.
- **Impact**: 사용자 의도한 동시호가(08:55~09:00) 매수가 시장가로 즉시 발주됨. failed 주문은 1회 텔레그램 알림 + config 유지로 추적 가능. 장 전 기동 시 전날 failed 잔재 자동 정리.

## [2026-05-12] afd1fa2
- **Category**: chore(ops)
- **Title**: 운영 토글 기본값 복원 (TARGET_DATE, OPTION)
- **Files**: `ws_merge_wss_parts.py`, `ws_realtime_trading_Restart.py`
- **Changes**: TARGET_DATE "260507" → "" (오늘 날짜 자동), OPTION 2 → 1 (재시작 모드) — 5/7 임시 설정 완료 후 기본값 복원
- **Impact**: 운영 설정 복원. 코드 로직 변경 없음.

## [2026-05-12] 09380eb
- **Category**: fix
- **Title**: H0STCNI0 체결통보 복호화 누락 버그 수정 — 복호화 분기 강건화 + 진단 로그 추가
- **Files**: `kis_auth_llm.py` (수정), `ws_realtime_trading.py` (수정), `test_wss_simple.py` (수정), `test_wss_subscribe.py` (신규)
- **Changes**:
  1. **버그 원인**: KIS H0STCNI0 (실시간 체결통보) 는 스펙상 항상 암호화 전송. 그러나 기존 코드는 `dm["encrypt"] == "Y"` 분기로만 복호화를 판단해, SUBSCRIBE 응답 파싱이 실패하면 `dm["encrypt"]` 가 미설정 상태가 되어 복호화를 건너뜀. 결과적으로 Base64 암호문이 `^` split 없이 첫 컬럼 cust_id 에 통째로 들어가고 나머지 필드는 모두 None.
  2. **증상 (2026-05-12 종일)**: 체결통보 텔레그램 알림 미발송. 종가매수 9건 접수 후 체결 여부를 알 수 없는 상태. 잔고검증 폴백(`_run_closing_balance_verification`)이 자동 보완 — `[15:31_종가체결] 잔고검증 완료(4822) — 변동 없음` 정상 출력.
  3. **그 외 WSS 정상 동작 확인**: H0STMKO0 시장상태 수신, ccnl_krx 체결가, exp_ccnl_krx 예상체결가 등 나머지 WSS 채널은 종일 정상 동작.
  4. **복호화 분기 강건화 (`kis_auth_llm.py` `__subscriber`)**: `should_decrypt = (raw[0] == "1") or (dm["encrypt"] == "Y")` — KIS 프레임 헤더 raw[0]가 "1" 이면 dm 상태 무관하게 복호화 시도. key/iv 미등록 시 `logging.error` (tr_id 별 1회, `_decrypt_warn_emitted` 플래그) 후 `continue`. 복호화 예외도 `logging.error` 후 `continue`.
  5. **진단 로그 (`kis_auth_llm.py`)**: `system_resp()` 전체 try/except 감싸기 + 파싱 실패 시 raw 첫 200자 로깅. H0STCNI0 SUBSCRIBE 응답 시 rt_cd/encrypt/iv_len/ekey_len 1회 로깅 (개인키 노출 방지 — 길이만). `add_data_map()` 호출 시 before/after 상태 진단 로그. `__subscriber` H0STCNI0 첫 프레임 도착 시 1회 진단 로깅 (`_h0stcni0_diag_logged` 플래그).
  6. **H0STCNI0 핸들러 가드 (`ws_realtime_trading.py`)**: `_h0stcni0_decrypt_warn_sent` 플래그 추가. 핸들러 진입부에서 cust_id 가 Base64 형태(길이>20 또는 `/+=` 포함)이면 복호화 실패 정황으로 보고 텔레그램 1회 경고 발송.
  7. **외부주문 체결 매칭 (`ws_realtime_trading.py`)**: `_external_order_track` / `_external_order_track_lock` 추가. 체결통보 수신 시 ordno 기반으로 외부주문 매칭 → 텔레그램 알림. `_format_external_order_intake_msg()` 헬퍼 함수 추가.
  8. **진단 도구 (`test_wss_simple.py`, `test_wss_subscribe.py`)**: `ws_subscribe_send_test()` (핸드셰이크 + subscribe + 5초 응답 모니터링), `ws_force_cleanup_attempt()` (ALREADY IN USE 잔재 UNSUBSCRIBE → 재시도). `test_wss_subscribe.py` 신규 — 운영 본체와 동일 흐름으로 2종목/15초 dwell 독립 검증.
- **Impact**: H0STCNI0 체결통보가 정상적으로 복호화되어 체결 알림 텔레그램 발송 재개. 향후 SUBSCRIBE 응답 파싱 실패가 발생해도 raw 헤더 기반으로 복호화 수행하므로 동일 사고 방지. 잔고검증 폴백은 여전히 백업 수단으로 유지.

## [2026-05-11] a8e1f49
- **Category**: feat
- **Title**: strategy_lab Phase 2.1 — 모멘텀 데이트레이딩 전략 + DAY_TRADE 액션
- **Files**: `strategy_lab/local_backtest/engine.py` (수정), `strategy_lab/local_backtest/types.py` (수정), `strategy_lab/local_backtest/examples/momentum_day_trade.py` (신규)
- **Changes**:
  1. 도입 배경: SMA 크로스오버는 스윙 전략이라 보유 기간이 길어 데이트레이딩 불가. 사용자 요청 — "당일 매도" + "학술·실무적으로 검증된 단순 전략". "전일 강한 상승 + 거래대금 폭증 → 다음날 시가 매수 → 종가 매도" 패턴 채택.
  2. `momentum_day_trade.py`: `MomentumDayTradeStrategy` 구현.
     - 편입 조건: `pdy_ctrt` 3~27%, `value / vema20 ≥ 1.5`.
     - 매일 거래대금 상위 5종목 균등 분배 (strength=0.2).
     - universe 가 매일 동적이라 `generate_signals()` 인터페이스 우회 → 자체 `backtest()` 메서드 보유.
     - `build_name_map()` 헬퍼: symbol → 종목명 매핑.
  3. `engine.py`: `DAY_TRADE` 액션 처리 추가.
     - 같은 날 시가(open) 매수 + 종가(close) 매도, pnl 즉시 확정.
     - strength < 1.0 이면 해당 값, 아닌 경우 당일 DAY_TRADE 종목 수로 균등 분배.
     - 포지션 미보유 (당일 청산) — positions 딕셔너리 변동 없음.
  4. `types.py`: `Action` Literal 에 `"DAY_TRADE"` 추가.
- **Validation** (2025년 풀 백테스트):
  - Total Return -62.90%, MDD 66.69%, Sharpe -1.88
  - Win Rate 42.59% (509승 / 686패), Profit Factor 0.80
  - 거래 수 2,390 (매수 1,195 + 매도 1,195)
  - 해석: 단순 일간 모멘텀은 한국시장에서 손실. 수수료·세금 누적 + 시가 갭 변동성이 불리. 학술적 모멘텀 효과는 1~12개월 단위에서 발견됨.
- **Impact**: 데이트레이딩 첫 전략 검증 완료. 운영 코드(ws_realtime_trading 계열) 무수정, 무영향. 향후 동적 universe 전략은 동일 패턴(자체 backtest()) 따를 예정.

## [2026-05-11] b54017b
- **Category**: feat
- **Title**: strategy_lab Phase 2 — 자체 Polars 백테스트 엔진 + SMA 크로스오버 예제
- **Files**: `strategy_lab/local_backtest/__init__.py`, `strategy_lab/local_backtest/types.py`, `strategy_lab/local_backtest/strategy.py`, `strategy_lab/local_backtest/metrics.py`, `strategy_lab/local_backtest/engine.py`, `strategy_lab/local_backtest/examples/__init__.py`, `strategy_lab/local_backtest/examples/sma_crossover.py`, `strategy_lab/tests/test_local_backtest.py` (전부 신규, 8개)
- **Changes**:
  1. 방향 전환 배경: quantconnect/lean:latest Docker pull 중 루트 파티션 여유 12GB 부족(압축 해제 10GB+ 추정)으로 디스크 소진. 사용자 결정으로 자체 Polars 기반 백테스트 엔진으로 전환. Docker 0 의존, 검증은 symulation/ SQL 결과와 교차검증으로 보완 예정.
  2. `types.py`: Signal(날짜/종목/action/strength), Trade(매수-매도 쌍 + PnL), BacktestResult(equity_curve + trades + metrics dict) 데이터 타입 정의.
  3. `strategy.py`: Strategy ABC — `generate_signals(bars: pl.DataFrame) → pl.DataFrame[date, symbol, action, strength]` 인터페이스.
  4. `metrics.py`: Total Return / CAGR / MDD / Sharpe / Win Rate / Profit Factor 6종 산출. equity_curve에 drawdown_pct 컬럼 추가.
  5. `engine.py`: BacktestEngine — provider + initial_cash + fee_buy(0.015%) + fee_sell(0.015%+0.18% 거래세) + slippage_rate. 흐름: get_history() → bars → generate_signals() → 날짜 순회 시뮬레이션 → BacktestResult.
  6. `examples/sma_crossover.py`: SmaCrossoverStrategy — 단기/장기 SMA 크로스오버 첫 검증 예제.
  7. pytest 3건 PASSED: initial_cash 정합성, equity_curve 일관성, 다종목 동작.
- **Validation**:
  - 005930 SMA(10/30) 2025-01-01~09-30: CAGR 13.55%, MDD 21.71%, Sharpe 0.52, Trades 9
  - 005930+000660 SMA(5/20) 6개월: Total Return +57.73%, CAGR 153.42%, MDD 12.93%, Sharpe 3.36, Trades 16
- **Impact**: Docker 없는 경량 자체 백테스트 기반 확보. 운영 코드(ws_realtime_trading 계열) 무수정, 무영향. 다음(Phase 3): symulation/ SQL 백테스트 결과와 교차검증 → 운영 연결.

## [2026-05-11] 0bd6f6d
- **Category**: feat
- **Title**: strategy_lab Phase 1 — LocalParquetProvider (우리 parquet → kis_backtest DataProvider 어댑터)
- **Files**: `strategy_lab/data_adapters/local_parquet_provider.py` (신규), `strategy_lab/tests/test_local_parquet_provider.py` (신규), `strategy_lab/data_adapters/__init__.py` (+3줄)
- **Changes**:
  1. AI Extensions plugin 설치 보류 결정: cwd 강제(strategy_builder/backtester/), kis-prod-guard.sh의 /api/orders 차단, kis_devlp.yaml 충돌 회피. 대신 Claude 가 strategy_builder/backtester 코드를 직접 호출하는 구조로 전환.
  2. `LocalParquetProvider` 구현: `kis_backtest.providers.base.DataProvider` Protocol 충족.
     - `get_history(symbol, start, end, resolution=DAILY)`: polars lazy scan, symbol/date 필터, Bar 리스트 반환.
     - `get_quote(symbol)`: 마지막 종가를 더미 호가(bid/ask)로 채워 반환.
     - `subscribe_realtime(...)`: NotImplementedError (실시간은 ws_realtime_trading.py 담당).
  3. symbol zfill(6) 자동 처리, 미존재 종목은 `[]` 또는 KeyError.
  4. KIS API 인증 불필요, kis_devlp.yaml 불필요. 의존성: polars, pydantic.
  5. 데이터 소스: `data/1d_data/kis_1d_unified_parquet_DB.parquet` (22컬럼 기존 파일).
  6. pytest 7건 전부 PASSED: OHLC 정합성, zfill 자동화, 미존재 종목, MINUTE 미지원 등.
  7. `strategy_lab/.venv` 분리: pandas 3.0.2, polars 1.40.1, pydantic 2.13.4, pytest 9.0.3.
- **Impact**: 파케이 데이터를 backtester 어댑터로 공급하는 기반 완성. 운영 코드(ws_realtime_trading 계열) 무수정, 무영향. 다음(Phase 2): .kis.yaml 전략 작성 + LeanClient 백테스트 실행(Docker).

## [2026-05-11] af19bc9
- **Category**: feat
- **Title**: strategy_lab Phase 0 — KIS open-trading-api subtree 흡수 + 골격
- **Files**: strategy_lab/.gitignore, README.md, requirements.txt, strategies_local/__init__.py, data_adapters/__init__.py, tests/__init__.py, backtests/{configs,results,reports}/.gitkeep
- **Changes**: strategy_lab/ 신규 폴더 생성. KIS 공식 GitHub(open_trading_api/)는 a42a7ec squash merge 로 별도 완료. 이번 commit 은 골격 파일(init, gitkeep, README, requirements) 9개.
- **Impact**: 자연어 기반 전략 설계/백테스트 환경 기반 마련. 운영 코드(ws_realtime_trading 계열) 무수정, 단방향 데이터 공유 구조.

## [2026-05-08] 3078c8e
- **Category**: refactor
- **Title**: 잔고조회 출력을 계좌별 블록으로 분리
- **Files**: `kis_utils.py`, `ws_realtime_trading.py`
- **Changes**:
  1. `print_table`에 `header_sep: bool = True` 파라미터 추가. `False` 지정 시 헤더와 데이터 사이 `─` 구분선 생략. 기존 호출자는 기본값 `True`로 동작 무변경.
  2. `_tele_balance_summary` 시그니처 변경: 단일 집계값 `(dnca/ord_cash/tot_eval/eval_pfls/valid_rows)` → `per_account: list[dict]` 수신으로 변경. 계좌별 블록 메시지 구성.
  3. `_query_and_print_balance` 리팩터링: 계좌별 데이터를 수집한 후 계좌별 블록으로 출력. 첫 계좌 요약만 잡히던 버그 해소(`if not summary` 가드 제거). `◆ [<alias> (<cano>)]   <═×54>` 헤더 + `보유종목 N건`/`보유종목 없음` 라벨. 요약/보유 테이블 모두 `header_sep=False` 호출.
  4. 부수적으로 이미 staged 상태였던 `fetch_investor_daily.py → fetch_foreign_investor_daily.py` 리네임(내용 변경 없음, R100) 포함.
- **Impact**: a1(43444822, 보유 없음) / a2(63614390) 두 계좌 잔고조회 로그가 계좌별 블록으로 명확히 분리됨. a2 요약 영구 누락 버그 해소. 텔레그램 잔고 메시지도 계좌별 블록으로 전송.

## [2026-04-28] bff4039
- **Category**: cleanup
- **Title**: kis_auth_llm.py — a2 흔적 (approval_key 파라미터 + _use_global_open_map) 제거
- **Files**: `kis_auth_llm.py`
- **Changes**:
  1. `KISWebSocket.__init__` 시그니처에서 `approval_key` 파라미터 제거: a2-WSS가 main과 다른 approval_key를 쓰기 위해 도입했던 파라미터로, 선행 commit(a2 제거)으로 사용처 0건
  2. `_use_global_open_map` 분기 단순화: `if _use_global_open_map: init_map = open_map` 분기 → `init_map = open_map` 직접 할당
  3. `send_multiple` docstring의 multiprocessing/multi-appkey 흔적 일괄 정리
- **Impact**: a2 제거 완전 마무리. dead code 0건. `KISWebSocket` 생성자 시그니처 단순화.

## [2026-04-28] 76323d8
- **Category**: revert
- **Title**: a2 자식 WSS 제거 + 단일 main 41슬롯 시간대별 분산 운영 복원
- **Files**: `ws_realtime_trading.py` (대규모 수정), `ws_a2_subprocess.py` (삭제)
- **Changes**:
  1. **배경**: 2026-04-27 도입한 a2 multiprocessing 패턴(KIS 공식 multi_processing_sample_ws.py 기반)이 ALREADY IN USE storm을 유발, 2026-04-28 운영 정지까지 이어짐
  2. **사용자 결정**: 단일 main 계좌 WSS 한 세션 41슬롯 시간대별 분산 운영으로 복원
  3. **삭제 내용 (392라인)**:
     - a2 전역 변수 4종: `_a2_kws_lock`, `_a2_active_kws`, `_a2_subscribed`, `_a2_approval_key`, `_a2_last_already_in_use_ts`
     - a2 multiprocessing 전역 변수 6종: `_a2_proc`, `_a2_data_queue`, `_a2_cmd_queue`, `_a2_alive`, `_a2_subscribed_local`
     - a2 전용 함수 9개: `_get_a2_approval_key`, `_a2_cmd_send`, `_a2_data_router_loop`, `_start_a2_subprocess`, `_stop_a2_subprocess`, `run_ws_a2_forever`, `run_ws_a2_forever_LEGACY`, `_a2_mkstatus_sub_add`, `_a2_mkstatus_sub_remove`
     - `_shutdown`의 a2 정리부 (`_stop_a2_subprocess` 호출, `_a2_active_kws` 잔재)
     - `t_a2_wss = threading.Thread(...)` + start 두 줄
  4. **_vi_exp_sub_switch / _vi_exp_sub_restore**: a2 분기 제거, a1 fallback을 본 경로로 승격 → 단일 `_active_kws`에서 ccnl↔exp swap 직접 수행
  5. **H0STMKO0(장운영정보)**: main 직접 구독 + `H0STMKO0_MAX_SLOTS=5` hard cap 도입 (보유→VI→기타 우선순위). 이유: 최악의 경우 5슬롯 점유 시에도 데이터 슬롯 ≥35 확보
  6. **슬롯 회계 보정**: `run_ws_forever`의 슬롯 회계에 `mkstatus_slots = len(_mkstatus_sub_codes)` 포함, `_top_rank_loop` available_slots 계산에서 `_mkstatus_sub_codes` 차감
  7. **ws_a2_subprocess.py 삭제**: a2 entry point 파일 통째 제거
- **Impact**:
  - 핵심 운영 흐름 보존: 08:50 예상체결가 → 09:00 실시간체결가 → VI 시 ccnl↔exp 1:1 swap → 15:21 예상체결가 → 15:30 실시간체결가 → 16:00 overtime
  - `_desired_subscription_map` 시간대별 시뮬 9개 케이스 모두 plan 표와 일치 확인
  - a2 multiprocessing으로 인한 ALREADY IN USE storm 재발 가능성 제거
  - syntax OK, module import OK, NameError 0건, a2 잔존 attribute 0건 검증 완료

## [2026-04-27] f6a9a2d
- **Category**: fix
- **Title**: WSS 재시작 시 ALREADY IN USE storm 본질 원인 제거
- **Files**: `kis_auth_llm.py`, `ws_realtime_trading.py`
- **Changes**:
  1. **Fix 1 — `KISWebSocket.close` timeout 2s → 10s** (`kis_auth_llm.py`): shutdown 시 `[shutdown] (1/4) WSS 종료 완료 (+2.00s)` 로그가 정확히 2초임을 포착 → `fut.result(timeout=2.0)` timeout exception 강제종료 확인. `except Exception: pass` 로 silent 처리되어 close frame 미완료 → KIS 가 abnormal close 인식 → appkey 해제 지연(정상=즉시, abnormal=30초~수분) → 재시작 시 ALREADY IN USE storm. timeout 10s 로 상향하고 성공/실패 시 `logging.info` / `logging.warning` 으로 가시화.
  2. **Fix 2 — `_request_ws_close` 에 명시적 UNSUBSCRIBE 선행** (`ws_realtime_trading.py`): close frame 만으로는 KIS 측 구독 정리가 늦을 수 있음. `_subscribed` 의 모든 active 구독에 `tr_type="2"` UNSUBSCRIBE 송신 후 0.5s dwell 후 close. 부모 main(`_active_kws`) 만 대상, a2 자식은 자체 cmd_queue stop 으로 처리.
- **Impact**: 직전 commit 들(max_retries=10, long backoff, dwell 안전망)은 storm 발생 후 차단 보험. 이 commit 은 storm 자체를 발생시키지 않는 본질 fix. 검증: 다음 재시작 시 `[ws] close frame 송신 완료 (XXms)` 로그 출력, ALREADY IN USE 미발생이면 정상.

## [2026-04-27] 44a5525
- **Category**: fix
- **Title**: 8808542 storm 패치 follow-up — UnboundLocalError 및 dwell 임계 보정
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **Fix 1 — UnboundLocalError** (`run_ws_forever`, line 11635): `_main_last_already_in_use_ts` 를 함수 본체에서 read/write 하면서 `global` 선언이 누락 → Python 이 local 변수로 인식 → 첫 read 시 UnboundLocalError → traceback → runner 자동 재시작 → 루프 반복. `global _close_force_stopped, _main_last_already_in_use_ts` 로 수정.
  2. **Fix 2 — dwell 임계 5s → 15s** (line 11821 부근): `max_retries=10` + retry 1초 sleep 환경에서 storm 패턴 dwell 상한이 약 10~12초. 실측 dwell 10.16s 가 5초 임계를 통과하지 못해 안전망 미작동. 임계를 15s 로 완화 (정상 connect 는 분~시간 단위이므로 false trigger 위험 없음). 주석·경고 로그 메시지도 갱신.
- **Impact**: UnboundLocalError 재발 0건 (12:34 이후). dwell=10.16s < 15s → long backoff 90s 진입 로그 확인. Storm self-feeding loop 차단 (분당 ~1회 시도로 감소).

## [2026-04-27] a578ba0
- **Category**: fix
- **Title**: stale 프로세스 누적 방지를 위한 cron wrapper 스크립트 2종 추가
- **Files**: `scripts/cron_start_log_monitor.sh`, `scripts/cron_start_vi_status.sh`
- **Changes**:
  1. `cron_start_log_monitor.sh`: ws_log_monitor.py 기동 전 기존 인스턴스 SIGTERM → 5초 대기 → SIGKILL fallback 후 nohup 새 인스턴스 시작
  2. `cron_start_vi_status.sh`: Daily_inquire_vi_status.py 동일 패턴 적용
  3. 인라인 cron에서 pkill 사용 시 cron sh 자체가 TARGET 패턴에 매칭되어 SIGKILL 위험 → wrapper 스크립트 분리로 자기참조 문제 해결
  4. crontab 변경(git 외부, 백업: `out/crontab_backup_260428.txt`): 23:20/23:58 두 줄을 각 wrapper 스크립트 경로로 교체
- **Impact**: ws_log_monitor.py의 tail -F 블로킹 버그(440-450라인)로 11일치 14개 stale 프로세스 누적된 문제 재발 방지. 매일 cron 기동 시 이전 인스턴스를 안전하게 정리 후 새 인스턴스 시작

## [2026-04-27] 8808542
- **Category**: fix
- **Title**: 부모 WSS reconnect storm 진단·방지 패치 (260428)
- **Files**: `kis_auth_llm.py`, `ws_realtime_trading.py`
- **Changes**:
  1. **`kis_auth_llm.py` line 842** — 연결 예외 로그 `logging.debug` → `logging.warning` 승격, `e.code` / `e.reason` 노출
     (1,975건 storm 이 DEBUG 레벨에 묻혀 close 사유가 전혀 안 보이던 가시화 fix)
  2. **`kis_auth_llm.py` line 829** — `ping_interval=None` → `ping_interval=20, ping_timeout=10` (합리적 default 유지)
  3. **`ws_realtime_trading.py` line 11757** — `KISWebSocket(api_url="", max_retries=10)` (기존 1회 → 10회)
     (첫 close 즉시 외부 reconnect 폭주하던 근본 원인 완화)
  4. **`ws_realtime_trading.py` line 4062~** — `_main_last_already_in_use_ts` 전역 변수 + `MAIN_WSS_BACKOFF_ALREADY_IN_USE_SEC=90.0` 신설 (a2 와 동일 패턴)
  5. **`_on_system` 콜백** — KIS 시스템 메시지 `ALREADY IN USE` 감지 시 `_main_last_already_in_use_ts` 기록
  6. **`run_ws_forever` reconnect 분기** — 직전 30초 내 ALREADY IN USE 감지 시 backoff 90초로 강제 (기존 2초). storm self-feeding loop 차단
  7. **`kws.start` dwell 측정** — 5초 미만 종료 시 `_main_last_already_in_use_ts` 강제 설정 (`_on_system` 콜백이 못 잡는 케이스 대비)
  8. **`SKIP_CCNI0_ON_INIT=1` env toggle** (line 11709, `_ccnl_notice_sub_add`) — H0STCNI0 init 구독 및 종목별 sub_add 우회. default off, 진단 전용.
- **Impact**:
  - WSS storm 의 close 사유가 WARNING 레벨로 즉시 가시화됨
  - max_retries=10 + long backoff 90s 조합으로 ALREADY IN USE self-feeding loop 차단
  - main appkey 가 외부(KIS HTS/모바일)에서 점유된 상태에서 재접속 폭주 방지
  - SKIP_CCNI0_ON_INIT 환경변수로 H0STCNI0 충돌 가설 현장 검증 가능

## [2026-04-27] ed5e342
- **Category**: feat
- **Title**: 매수가 손절 단일-틱 fake 가격 보호 (SK증권우 260428 사례)
- **Files**: `ws_realtime_trading.py`, `rules/trading_project_main_plan.md`
- **Changes**:
  1. **신규 상수 3개** (line 92, 95, 96):
     - `LOSS_CONFIRM_TICKS = 30` — 매수가 손절 N틱 연속 이탈 확인
     - `OPENING_GRACE_SEC = 30` — 09:00:00~09:00:30 grace period
     - `OPENING_GRACE_LOSS_PCT = 0.07` — grace 구간 임계 -7%
  2. **`_str1_sell_state` 초기화 5곳에 `loss_below_count: 0` 필드 추가** (line 1490, 1568, 1594, 2503, 6010)
  3. **매수가 손절 분기 교체** (`_check_str1_sell_conditions` line 9489):
     - 단일 `elif bidp1 < _buy_p * 0.97` → grace 임계 분기 + N틱 누적 카운터 + 회복 시 자동 리셋 패턴
     - 09:00:00~09:00:30 grace 구간: 임계 -3% → -7% (우선주/thin liquidity 첫 틱 1주 fake 가격 보호)
     - `LOSS_CONFIRM_TICKS` 미달 시 debug 로그만 기록, 달성 시 매도 발동 (기존 `STOP_LIMIT_CONFIRM_TICKS` 패턴 차용)
     - loss-pending 중 데드크로스 재시도 보류 (loss 우선 의미 유지)
     - buy_p/bidp1 미확보 시 ema fallback 분기 보존
  4. **`rules/trading_project_main_plan.md`**: 09:00~15:20 섹션에 매수가 손절 가드 항목 추가 (3개 상수 설명 + SK증권우 사례 언급)
- **Impact**:
  - 우선주 등 thin liquidity 종목의 09:00 첫 틱 1주 fake 가격으로 즉시 손절되는 버그 해소
  - SK증권우 검증: 첫 틱 14,280 > grace 임계 13,922 → 신규 로직에서 카운터 미증가, 손절 미발동. 이후 +25% 회복 포착 가능

## [2026-04-27] f4ee6bf
- **Category**: fix
- **Title**: ws_a2_subprocess V2 config nested 구조 대응 (syw_2 계정 로드 버그)
- **Files**: `ws_a2_subprocess.py`
- **Changes**:
  - `cfg["accounts"]["syw_2"]` flat path 참조 → `get_account_config(cfg, "syw_2")` 로 교체
    (V2 config 구조: `users.<default_user>.accounts.syw_2` nested)
  - `htsid`는 user 레벨 fallback: `syw2.get("htsid") or user_cfg.get("htsid", "")`
  - `acnt_prdt_cd`도 `or "01"` fallback 명시
- **Impact**: flat path 참조 오류로 syw_2 계정 로드 실패 → a2 자식 프로세스 무한 재시작되던 버그 해소

## [2026-04-26] ecc4333
- **Category**: feat
- **Title**: 외인/기관/개인 일자별 순매수 순위 컬럼 + fetch_investor_daily.py 루트 이동
- **Files**: `compute_investor_ranks.py` (신규), `fetch_investor_daily.py` (symulation/ → 루트 이동)
- **Changes**:
  1. **compute_investor_ranks.py 신규** — `data/investor_data/kis_investor_unified_parquet_DB.parquet` 의 일자별 4종 rank 컬럼 산출
     - `frgn_rank` (외인 순매수 수량), `orgn_rank` (기관), `prsn_rank` (개인), `frgn_orgn_rank` (외인+기관 합산)
     - 1=최대 순매수, method='min', ascending=False, na_option='bottom'
     - CLI: `python3 compute_investor_ranks.py` (전 일자) / `--date 20260427` (단일)
     - 모듈 import: `from compute_investor_ranks import compute_ranks_for_dates`
  2. **fetch_investor_daily.py 루트 이동** — `symulation/` → 프로젝트 루트 (daily fetch 류 스크립트 일관성)
     - SCRIPT_DIR: `parent.parent` → `parent`
     - docstring 사용법/cron 경로 갱신
  3. **fetch_investor_daily.py 통합** — parquet 저장 직전 `compute_ranks_for_dates()` 호출, try/except로 rank 실패 시에도 데이터 저장 보장
  4. **271일 backfill 완료** — 전 일자 일괄 처리, parquet shape (715,740, 31) 확인
- **Impact**: 시뮬레이션/분석 시 일자별 외인·기관·개인 순매수 순위를 직접 join 없이 parquet에서 바로 필터링 가능

## [2026-04-26] d015a87
- **Category**: feat
- **Title**: 외인/기관 매매현황 DB 스크립트 3종 (파일럿/1년 backfill/일일 cron) 추가
- **Files**: `symulation/investor_db_pilot.py`, `symulation/fetch_investor_history.py`, `symulation/fetch_investor_daily.py`
- **Changes**:
  - KIS 공식 `investor-trade-by-stock-daily` API 채택 (TR_ID `FHPTJ04160001`): 일자 지정 가능, 30 영업일/페이지
  - `investor_db_pilot.py` (Step 1): 1종목 호출로 응답 schema, 1페이지 일수, tr_cont 동작, 비용 추정 검증. 단독 실행 (`python3 investor_db_pilot.py`)
  - `fetch_investor_history.py` (Step 2): 전종목 (KRX_code.csv group=ST) 9개 입력일자로 1년치 backfill. `--limit N` 시범 모드, `--code XXX` 단일 종목 모드. 27 컬럼 (외인/기관/개인 + 기관 세부5종 + OHLCV + symbol/name). 출력: `data/investor_data/kis_investor_unified_parquet_DB.parquet`. 기존 parquet 자동 merge (date+symbol dedup)
  - `fetch_investor_daily.py` (Step 3): 매일 1회 cron 용 — 오늘 영업일 1행/종목만 수집. 2500종목 × 1 호출 = 약 2.5분. cron 등록 명령 docstring 명시
  - 파일럿 검증: 005930 단일 종목 240행/240일자 성공. 100종목 시범 25,260행/271일자/2.2분. 전체 2500종목 약 55분 추정
  - output schema 검증: 외인 (수량+금액), 기관 (수량+금액+세부5종), 개인, OHLCV. 동화약품(000020) prdy_ctrt/frgn_ntby_qty/orgn_ntby_qty/prsn_ntby_qty 정상
- **Impact**:
  - 백테스트 + v5 F13 (외인 양수 매수 정책) 사후 검증을 위한 종목별 일별 외인/기관 매매 데이터 영속 DB 기반 마련
  - Step 4 백테스트 join 은 별도 task. 전체 2500종목 backfill 및 cron 등록은 사용자 승인 후 실행 예정

## [2026-04-27] a8bf83d
- **Category**: feat
- **Title**: v5 매수 path 진단 카운터 추가 (5분 주기 dump) — 04-27 미호출 원인 식별용
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  - 글로벌 `_v5_diag_counters` (20개 카운터) 추가: fn_called, kind_not_regular_real, rows_processed, code_invalid, in_holdover, in_daily_bought, in_buy_pending, in_evaluated, limitup_reached, low_liquidity, diversify_full, ctrt_below_25, ctrt_at_or_above_30, ctrt_in_observe_band, ctrt_in_buy_band, qualify_called, qualify_passed, qualify_failed, trigger_evaluated, trigger_buy_fired, in_position_path
  - `_v5_diag_dump_if_due()` 함수: 5분 주기로 카운터 INFO 로그 출력 + 리셋, 모두 0건이면 "v5 path 미호출 가능성" 경고
  - `_check_uplimit_v4_from_tick` 함수 진입 첫 줄에 fn_called++ 및 dump 호출, 모든 분기 차단/통과 지점에 카운터 삽입
  - `_restore_uplimit_state_on_startup`: daily_bought 카운트 로그 추가 + 비어있지 않으면 차단 종목 리스트 INFO 출력
- **Impact**: 다음 운영부터 5분마다 `[v5_diag]` 로그로 매수 path 진입/차단 현황 파악 가능. 0건이면 ingest_loop 분기 의심, ctrt_at_or_above_30 높으면 즉시상한가 가설 확정

## [2026-04-27] d3efec2
- **Category**: feat
- **Title**: 15:31 종가매수 결과 정리 + 잔고검증 + 15:40 대기 메시지 강제 실행
- **Files**: `ws_realtime_trading.py` (run_ws_forever() wait loop 진입부, line ~11682)
- **Changes**:
  - wait loop 진입 직후(`종가 체결 수신 완료` 로그 다음) 강제 실행 블록 추가
  - `_run_closing_buy_filled_notify()` 호출 — 종가매수 체결 결과 통보 (체결통보 매칭 정보 정리)
  - `_closing_balance_1531_done` 플래그 확인 후 미실행 시 `"15:31_종가체결"`, 이미 실행된 경우 `"15:31_종가체결_재검증"` 라벨로 `_run_closing_balance_verification()` 호출
  - `_notify("[wait] 종가매수 정리 완료 → 15:40 시간외 종가 매수까지 대기", tele=True)` 텔레그램 알림
  - 각 블록 try/except 처리로 실패 시 warning 로그만 남기고 계속 진행
- **Impact**:
  - 배경: 04-27 운영 사고 — SK증권우 3주 실체결됐으나 시스템 로그에 `15:31_종가체결` 흔적 0건 (main monitor 분기 미도달로 잔고검증 미실행)
  - main monitor 분기 누락이 있어도 wait 진입 시점에 무조건 실행 보장
  - 사용자가 텔레그램 + 콘솔 양쪽으로 종가매수 결과 + 실잔고 확인 가능
  - `"15:40 시간외 매수까지 대기 중"` 명시 → idle 정상/멎음 즉시 분간 가능
  - 미해결: H0STCNI0 체결통보 수신 정상에도 SK증권우 3주 체결 미감지 정확한 원인 — 별도 진단 필요

## [2026-04-26] eeeed9f
- **Category**: feat
- **Title**: 15:30~16:00 idle 구간 1분 주기 heartbeat 로그 추가
- **Files**: `ws_realtime_trading.py` (run_ws_forever() wait loop, line ~11682)
- **Changes**:
  - wait loop 진입 전 `_last_hb = time.time()` 타이머 초기화
  - 루프 내 `time.time() - _last_hb >= 60.0` 체크 → 매 1분마다 heartbeat 로그 출력
  - 로그 형식: `[ws] idle 중 — 16:00 시간외까지 N분 남음`
  - 기존 진입 로그(`종가 체결 수신 완료, 16:00 시간외까지 대기`) / 종료 로그(`16:00 도달 → 시간외 단일가 연결 시작`) 유지
- **Impact**:
  - 사용자 원칙 "모든 상황 변화는 반드시 로그로 남겨야 한다" 적용
  - 기존 04-01 이후 동일 코드: 30분간 로그 없음 → 멎음/정상 idle 구분 불가 문제 해소
  - 15:30~16:00 구간에 최대 30개 heartbeat 로그 생성 → idle 정상 작동 확인 가능

## [2026-04-26] 0299cfb
- **Category**: refactor
- **Title**: send_multiple 의 race code 제거 — multiprocessing 전환 후 dead code cleanup
- **Files**: `kis_auth_llm.py` (`send_multiple` 함수, line 868-913)
- **Changes**:
  - `send_multiple` docstring 및 본문을 KIS 공식 샘플 형태로 단순화
  - `with KISWebSocket._approval_key_lock:` 블록 제거
  - `if self._approval_key:` 분기 및 `_base_headers_ws` swap 로직 제거
  - 단순 `await self.send(ws, request, tr_type, d, kwargs)` 호출로 회귀 (32줄 → 9줄)
  - 호환을 위해 `__init__` 의 `approval_key` 인자, `_approval_key_lock` 클래스 변수, `_use_global_open_map` 속성은 보존
- **Impact**:
  - Phase 7 (c985f0e) build/send 분리 코드가 a2-WSS multiprocessing 전환 (a75ff0b) 이후 완전 dead code 가 됨 → 제거
  - a2 자식 프로세스 namespace 격리로 race condition 자체가 발생할 수 없는 구조 → 방어 코드 불필요
  - 코드 복잡도 감소, KIS 공식 패턴과 일치

## [2026-04-26] a75ff0b
- **Category**: feat
- **Title**: a2-WSS multiprocessing 패턴 전환 (KIS 공식 샘플 기준, race condition 근본 제거)
- **Files**: `ws_a2_subprocess.py` (신규, ~200줄), `ws_realtime_trading.py` (수정)
- **Changes**:
  - `ws_a2_subprocess.py` 신규 생성: `multiprocessing.Process` target, `data_queue` (a2→main), `cmd_queue` (main→a2) IPC, `cmd_loop` 별도 thread로 동적 sub/unsub 지원
  - syw_2 계정 정보 자체 로드 후 `ka.auth_ws()` 로 approval_key 발급 → 자식 프로세스 namespace 내 `_base_headers_ws` 격리 사용
  - `ws_realtime_trading.py`: 신규 globals (`_a2_proc`, `_a2_data_queue`, `_a2_cmd_queue`, `_a2_alive`), 신규 함수 (`_a2_cmd_send`, `_a2_data_router_loop`, `_start_a2_subprocess`, `_stop_a2_subprocess`)
  - `run_ws_a2_forever`: thread 패턴 폐기 → 자식 프로세스 spawn + alive 모니터 loop 로 변경, 이전 코드는 `run_ws_a2_forever_LEGACY` 로 보존
  - `_a2_mkstatus_sub_add/remove`, `_vi_exp_sub_switch/restore`: a2 send 부분을 `_a2_cmd_send()` 호출로 변경
  - `_shutdown`: `_stop_a2_subprocess()` 호출 추가
  - `kis_auth_llm.py` 의 legacy fallback 코드(`_approval_key_lock`, `_base_headers_ws` swap)는 검증 후 cleanup 예정
- **Impact**:
  - 04-22 thread 패턴의 `_base_headers_ws` 글로벌 swap race condition 근본 제거
  - 04-23~04-27 누적된 ALREADY IN USE 오류(23건/270건/110+건)의 원인 해소
  - main appkey / syw_2 appkey 가 OS 프로세스 단위로 완전 격리 → KIS 1 appkey=1 세션 정책 정상 대응
  - 멀티계좌 확장성 확보 (향후 a3/a4 동일 패턴으로 추가 가능)

## [2026-04-26] f83e903
- **Category**: fix
- **Title**: a2-WSS appkey 를 syw_2 → main 으로 변경 (syw_2 KIS 점유 우회)
- **Files**: `ws_realtime_trading.py` (`_get_a2_approval_key`, line ~11038)
- **Changes**:
  - `_get_a2_approval_key()` 함수에서 계정 키 소스를 `syw_2` → `main` 으로 변경
  - docstring 에 변경 사유 및 배경 명시 ([260427])
  - 에러 메시지 "syw_2 appkey/appsecret/base_url 누락" → "main appkey/appsecret/base_url 누락"
- **Impact**:
  - syw_2 appkey 가 KIS 서버에 ALREADY IN USE 로 영구 점유된 상태(110건+) 우회 시도
  - KIS 1 appkey=1 세션 정책상 main WSS 와 충돌 가능성 있음 — 거부 시 H0STMKO0 자체 비활성 후속 검토 필요
  - 사용자 직접 지시 ("a1 으로 옮겨봐")

---

## [2026-04-26] c985f0e
- **Category**: fix
- **Title**: send_multiple race condition 수정 — main 으로 syw_2 key 누출 방지 (a2-WSS ALREADY IN USE 근본 원인)
- **Files**: `kis_auth_llm.py` (line 868~897 → 868~911)
- **Changes**:
  - 기존 버그: a2 path 만 `_approval_key_lock` 획득, main path 는 lock 미획득 상태로 `_base_headers_ws` 직접 read.
    a2 가 `_base_headers_ws["approval_key"] = syw_2_key` swap 후 `await` 시 yield 하면, main 이 동시에
    `syw_2_key` 를 읽어 main connection 으로 KIS 에 전송 → KIS 가 syw_2 appkey 를 main 세션이 점유한 것으로 인식.
    이후 a2 가 자기 connection 에서 syw_2 등록 시도 시 ALREADY IN USE 반환 (04-23~04-27 110건).
  - 수정: 모든 path (a2 + main) 가 `_approval_key_lock` 안에서 `request()` 호출 (메시지 build).
    `await ws.send()` 는 lock 외부에서 실행 → asyncio yield 시 lock 미보유로 deadlock 방지.
  - `add_data_map` 도 lock 블록 안으로 이동 (글로벌 dict 일관성 보호).
  - 시그니처/반환값 변경 없음 → 모든 호출자 영향 없음.
- **Impact**:
  - main 이 자기 main_appkey 로만 subscribe (syw_2 key 누출 경로 완전 차단).
  - a2 가 syw_2_appkey 로 정상 subscribe → ALREADY IN USE 110건 → 0건 예상 (재시작 후 검증 필요).
  - Phase 6 워밍업 sleep + Phase 6-B long backoff 는 옛 KIS 점유 자연 timeout 까지 보조.

## [2026-04-26] 5647452
- **Category**: feat
- **Title**: a2-WSS 워밍업 sleep + ALREADY IN USE long backoff (점유 자연 회복 강화)
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  - 모듈 전역 `_a2_last_already_in_use_ts: float = 0.0` 추가 — ALREADY IN USE 감지 시각 기록용.
  - 상수 `A2_WSS_INITIAL_WARMUP_SEC = 30.0`, `A2_WSS_BACKOFF_ALREADY_IN_USE_SEC = 90.0` 추가.
  - (A) `run_ws_a2_forever` 메인 루프 진입 전 30초 워밍업 sleep: 옛 프로세스 잔존 점유 KIS timeout 확보. `_stop_event` 0.1초 단위 분할 대기로 즉시 종료 응답 보장. main WSS(체결가) 는 별도 스레드라 영향 없음.
  - (B) `_on_system_a2` 콜백에서 "ALREADY IN USE" 메시지 감지 시 `_a2_last_already_in_use_ts = time.time()` 기록. reconnect 직전 30초 이내 감지 여부로 분기: 발생이면 90초 long backoff, 미발생이면 기존 지수 backoff(2~60초). 두 경우 모두 `_stop_event` 0.1초 단위 분할 sleep.
  - 모니터링 키워드: `[a2-WSS] 워밍업 sleep 30s`, `[a2-WSS] ALREADY IN USE 감지 → long backoff 90s`, 정상 회복 시 `[a2-WSS] H0STMKO0 구독 완료: N종목`.
- **Impact**:
  - 배경: Phase 6 (close frame 동기 전송) 는 미래 재시작을 보호하지만, 옛 코드(04-27 pid 677384) 가 close frame 없이 죽으면서 KIS 서버에 syw_2 appkey 점유 잔존 → KIS 자연 timeout 까지 ALREADY IN USE 반복.
  - 시작 시 30초 워밍업으로 잔존 점유 자동 정리 시간 확보.
  - ALREADY IN USE 감지 시 짧은 backoff 폭주/로그 스팸 방지 + 90초 한 번 대기 후 KIS 자연 회복. v5 매수/매도 및 main WSS 체결가 흐름 영향 없음.

---

## [2026-04-27] 24f747b
- **Category**: feat
- **Title**: 기동 시 git commit 버전 표시 — 옛 코드 운영 사고 방지
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  - `_get_code_version()` 함수 신규 추가: `git rev-parse --short=10 HEAD` 로 commit hash, `git log -1 --format=%ci` 로 커밋 날짜, `git status --porcelain` 으로 핵심 파일(ws_realtime_trading.py, ws_realtime_tr_str1.py, kis_auth_llm.py) dirty 여부 확인. 서브프로세스 timeout 2초, 실패 시 "unknown" 반환.
  - 출력 형식: `"7a949d212d (2026-04-27)"` 또는 미커밋 변경 존재 시 `"+dirty"` 접미.
  - `__main__` 진입점에서 `_CODE_VERSION` 산출 후 휴일/개장일 양쪽 시작 메시지에 `[v=...]` 삽입, 텔레그램(tele=True)으로도 발송.
- **Impact**:
  - 동기: 04-27 운영 중 재시작 후 사용자가 옛 프로세스(Phase 6 미적용)로 착각하게 해 잘못된 분석 제공. 버전 명시가 있었으면 즉시 식별 가능했음.
  - 매 기동마다 로그/텔레그램에 commit hash 기록 → 운영 중 버전 원격 확인 가능.
  - `+dirty` 표시 시 미커밋 변경 있음을 경고하여 실수 예방.

---

## [2026-04-27] 1a3b112
- **Category**: fix
- **Title**: WSS close frame 동기 전송 + shutdown telegram 비동기화 (a2-WSS ALREADY IN USE 방지)
- **Files**: `kis_auth_llm.py`, `ws_realtime_trading.py`
- **Changes**:
  - `KISWebSocket.close(timeout=2.0)`: `fut.result(timeout)` 으로 close frame 전송 완료까지 동기 대기. `shutdown()` 도 동일 인자 전달. 기존 `kws.close()` 인자 없는 호출 완전 호환.
  - `ws_realtime_trading.py` finally 블록: `_notify(종료)` 를 `daemon=True` 스레드로 wrap → Telegram API 지연이 shutdown 흐름을 차단하지 않음. logger/파일 기록은 thread 안에서 즉시 수행되어 로그 손실 없음.
- **Impact**:
  - 근본 원인(04-27 11:19 사고): `asyncio.run_coroutine_threadsafe(ws.close())` 결과 대기 없이 즉시 리턴 → SystemExit 으로 asyncio 루프 종료 → KIS 서버 close frame 미수신 → 60~120초 appkey 점유 → 신규 a2-WSS 4회 "ALREADY IN USE" + H0STMKO0 구독 실패.
  - 수정 후: shutdown 시 close frame 정상 전송(최대 2초) → KIS 즉시 appkey 해제 → 재시작 직후 H0STMKO0 첫 구독 성공 → restart 갭 30초 이내 해소, SIGKILL 미발동.

---

## [2026-04-26] a410692
- **Category**: refactor
- **Title**: v5 외인 일괄 fetch 제거 — Top30 신규/lazy fetch 만으로 충분
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  `_precache_uplimit_signals` 내 `target_codes[:40]` 외인 REST 일괄 fetch 블록 삭제.
  v5 F13 외인 필터는 25%+ 도달 종목에만 적용되므로 보유/전일 closing_buy 후보 등 일반 구독 종목의 사전 fetch 는 대부분 무의미.
  보유 종목은 F2(이미 보유)로 차단되고, 전일 후보도 대부분 25% 미만.
  자리에 사유 주석(`[260427]`) 추가.

- **Impact**:
  - 장 시작 직후 REST 호출 ~40회 절감 (약 2초 단축)
  - 외인 quota 낭비 제거
  - 외인 데이터 공급 경로: (a) 재시작 시 parquet load, (b) Top30 신규 25%+ 도달 시 사이클당 10개 fetch, (c) 매수 판정 시 lazy fetch — 세 경로로 충분

## [2026-04-26] 9c70ab4
- **Category**: feat
- **Title**: v5 F13 외인 캐시 영속화 + lazy fetch (재시작 안전성)
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  **문제 배경**: v5 F13 강화 (외인 양수만 매수) 이후 재시작 시 `_uplimit_frgn_3d` 메모리 휘발 → 재시작 직후 모든 종목 `v5_F13_외인데이터없음` 차단.
  사전 캐시 40종목 한도 + 장중 추가 사이클당 10종목 한도로 Top30 변화 빠를 때 누락 문제도 겸 해결.

  **신규 전역 set**:
  - `_uplimit_frgn_3d_fetch_attempted` — 종목당 lazy fetch 1회 시도 보장 추적

  **신규 헬퍼 함수**:
  - `_frgn_3d_cache_path()` — `data/frgn_cache/YYMMDD.parquet` 경로 반환 + 디렉토리 생성
  - `_load_frgn_3d_cache_today()` — 당일 parquet → `_uplimit_frgn_3d` 메모리 적재 (재시작 시)
  - `_save_frgn_3d_cache_today()` — `_uplimit_frgn_3d` → parquet 덮어쓰기 저장
  - `_lazy_fetch_frgn_3d(code)` — 캐시 miss 시 동기 fetch (200~500ms) + parquet 갱신

  **호출 지점**:
  - `_precache_uplimit_signals` 시작부: `_load_frgn_3d_cache_today()` (당일 기존 데이터 복원)
  - `_precache_uplimit_signals` 종료부: `_save_frgn_3d_cache_today()` (precache 결과 누적 저장)
  - `_precache_uplimit_for_new_codes` fetch 후: `_save_frgn_3d_cache_today()` (신규 종목 즉시 저장)
  - `_check_uplimit_v4_from_tick` qualify 단계: 캐시 miss 시 `_lazy_fetch_frgn_3d(code)` 호출

  **parquet 사양**: 컬럼 `code`(str 6자리) / `frgn_3d_net`(float) / `fetched_ts`(ISO str), YYMMDD 파일명으로 당일 데이터만 적재

  **로그 키워드**: `[v5_frgn_load]` (parquet→메모리) / `[v5_frgn_save]` (메모리→parquet) / `[v5_frgn_lazy]` (캐시 miss 동기 fetch)

- **Impact**:
  - 재시작 후 즉시 외인 데이터 복원 → F13 차단 없이 매수 판정 재개
  - lazy fetch 종목당 1회 보장 → TPS 영향 미미
  - parquet 손상 시 빈 dict 진행 후 precache 가 다시 채움 (무중단)

## [2026-04-26] 1c91397
- **Category**: feat
- **Title**: v5 종목 선정/매수 트리거 분리 + 5필터 완화 + BB 반등 트리거
- **Files**: `ws_realtime_tr_str1.py`, `ws_realtime_trading.py`
- **Changes**:
  **설계 변경 배경**: 기존 13필터 단일 판정 구조에서 "종목 선정(12필터) + 매수 트리거(ma10/BB)" 2단계로 분리.
  종목 선정 후 일시 하락 구간에서 BB 하단 반등 시 저점 매수 가능하도록 설계.

  **신규 함수**:
  - `check_uplimit_v5_qualify(...)` — 12필터 AND (F1, F2, F4-F13). F3 제거. qualified 상태 반환
  - `check_uplimit_v5_buy_trigger(...)` — ma10 상회 OR BB 하단 반등 트리거 판정
  - `_get_bb_lower_v5(code)` — 현재 캐시(_bb_sum/_bb_sq_sum/_price_buf) 기준 bb_lower 계산

  **5필터 완화** (일평균 5~10종목 통과 목표):
  | 필터 | 이전 | 신규 |
  |---|---|---|
  | F6 시가갭 | ≥5% | ≥2% |
  | F8 거래량서지 | ≥3배 | ≥2배 |
  | F10 10일변동성 | ≤0.05 | ≤0.08 |
  | F11 매도벽 | ask < bid×3 | ask < bid×5 |
  | F12 체결강도 | ≥120 | ≥100 |

  **매수 트리거 2종**:
  - ma10 분기: tick_count≥10 + stck_prpr>ma10
  - BB 반등 분기: breach_flag + prev_bidp1>bb_lower + bidp1>bb_lower + bidp1>prev_bidp1

  **10분 만료**: UPLIMIT_V5_QUALIFY_EXPIRY_MIN=10, qualified 후 10분 초과 시 트리거 비활성 (1회 로그)

  **신규 상태변수** (ws_realtime_trading.py):
  - `_uplimit_v5_qualified_ts: dict[str, datetime]` — 12필터 통과 시각
  - `_uplimit_v5_last_bidp1: dict[str, float]` — 직전 틱 bidp1 (BB 반등 prev 기준)
  - `_uplimit_v5_lower_breached: set[str]` — qualified 이후 bb_lower 이탈 이력

  **호환 처리**: `check_uplimit_v5_instant_buy` → qualify wrapper로 alias 유지

- **Impact**:
  - 종목 선정 후 매수 타이밍을 ma10 OR BB 저점 반등 중 빠른 것으로 포착 가능
  - 5필터 완화로 일평균 통과 종목 수 증가 (1~2종목 → 5~10종목 기대)
  - 10분 만료로 늦은 진입 차단 — 진입 품질/리스크 관리 균형

## [2026-04-26] 1e56bf3
- **Category**: fix
- **Title**: v5 F13 강화 — 외국인 수급 양수 종목만 매수 (None=차단)
- **Files**: `ws_realtime_tr_str1.py`
- **Changes**:
  **사용자 명시 의도**: "외국인 매도종목 차단"이 아니라 "외국인 수급이 들어오는 종목에만 매수" — 양수 확인이 조건.

  **정책 변화 (check_uplimit_v5_instant_buy F13 분기)**:
  | 조건 | 이전 | 신규 |
  |---|---|---|
  | frgn_3d_net > 0 | 통과 | 통과 |
  | frgn_3d_net == 0 | 통과 | 차단 (v5_F13_외인수급없음) |
  | frgn_3d_net < 0 | 차단 | 차단 (v5_F13_외인수급없음) |
  | frgn_3d_net is None | 통과 | 차단 (v5_F13_외인데이터없음) |

  **데이터 미수집 종목 처리**:
  - `_precache_uplimit_for_new_codes` 가 10코드/사이클로 외인 데이터 일괄 fetch
  - ma10 누적(10틱) 완료 전에 수집되면 정상 판정 가능
  - Top30 신규 코드 10종목 초과 시 일부 누락 → 다음 사이클에 재시도, 그 전까지 None 차단으로 안전 대기

- **Impact**:
  - 외인 순매수 확인 불가 종목은 진입 불가 — 진입 품질 상향
  - 외인 데이터 0(보합)도 차단 — 순매수 확신 있는 종목만 허용
  - c755632(13필터 복원) 의 후속 강화 커밋

## [2026-04-26] c755632
- **Category**: fix
- **Title**: Strategy A-v5 매수 판정에 13필터 복원 — F3만 ma10 상회로 교체
- **Files**: `ws_realtime_tr_str1.py`, `ws_realtime_trading.py`
- **Changes**:
  **문제**: 직전 v5 도입(4d97760) 시 13필터를 누락하고 25%+ma10 단 2조건만 적용함.
  사용자 의도("13필터 통과 종목에 대해 ma10 상회 시 매수")가 반영되지 않은 채 커밋됨.

  **ws_realtime_tr_str1.py — check_uplimit_v5_instant_buy**:
  - 함수 파라미터 5개 → 21개로 확장 (prev_close, stck_oprc, acml_vol, day_avg_vol_per_min 등)
  - uplimit_approach_buy_signal 의 F1~F13 전 필터 AND 조건으로 복원
    - F3 (28% edge-trigger) 만 ma10 상회 트리거로 교체
    - F4: 전일 등락률 < 10%, F5: 저가주(≥1000원), F6: 시가 갭 ≥ +5%
    - F7: 누적거래량 ≥ 100K, F8: 거래량 서지(5분 ≥ 일평균×3), F9: 25% 돌파 후 60분 이내
    - F10: 10일 변동성 ≤ 5%, F11: 매도벽 차단, F12: 체결강도 ≥ 120, F13: 외인 3일 순매도 차단
  - 차단 사유 prefix v5_F{n}_* 으로 통일 (가시성)

  **ws_realtime_trading.py — _check_uplimit_v4_from_tick 매수부**:
  - col_map 에서 stck_oprc, acml_vol, bidp_rsqn{1~5}, askp_rsqn{1~5} 추출
    (WSS 필드명 오타 bidp{i}_rsqn → bidp_rsqn{i} 수정 포함)
  - prev_close, volume_power, frgn_3d, day_avg, last5m_avg, min_since_cross 수집
  - 새 21파라미터 시그니처로 check_uplimit_v5_instant_buy 호출
  - v5_skip 로그 종목당 10초 쿨다운 추가

- **Impact**:
  - 04-24 매수 실패 6종목 시뮬 결과: 한화갤러리아(F13 차단), 피엠티(F13 차단), CSA코스믹/한화갤러리아우(저거래 차단) ✓
  - 고영·아스플로는 필터 통과 후 ma10 상회 시 매수 대상으로 남음
  - v5 즉시매수가 진입 품질 기준 없이 동작하던 버그 해소

## [2026-04-26] 4d97760
- **Category**: feat
- **Title**: Strategy A-v5 도입 — 구독 즉시 ma10 상회 시 시장가 매수 + 저거래/상한가도달/트레일3% 차단
- **Files**: `ws_realtime_trading.py`, `ws_realtime_tr_str1.py`, `symulation/Query_Krx_code.py`, `symulation/Query_str_bb_expansion_simulation.py`, `symulation/Query_str_ma_trend_simulation.py`
- **Changes**:
  **v4 폐기 사유 (04-24 실전 3대 치명 버그)**:
  1. 매수 직후 -5% 하드손절 오발동 — 당일 전체 stck_lwpr 를 post_buy_low 로 사용하는 버그
  2. 상한가 초과 주문 실패 3,425건/48분 — 지정가 +2틱 방식이 가격제한(1.30) 초과
  3. 04-24 매수 실패 6종목 중 5종목 이후 상한가 도달 → 5분 유지 조건 자체가 기회 손실

  **v5 신규 사양 (F1~F4)**:
  - F1: 30% 도달 종목 당일 차단 (`_uplimit_v5_limitup_reached` set)
  - F2: max_prdy_ctrt ≥ 29% 도달 후 cur_price ≤ highest × 0.97 시 트레일 3% 청산 (`check_uplimit_v5_trail_exit`)
  - F3: 25%~30% 구간 + tick_count ≥ 10 + stck_prpr > ma10 → 시장가 즉시 매수 (5분 유지 폐지, `check_uplimit_v5_instant_buy`)
  - F4: 전일 거래대금 / 당일 acml < 50억 종목 차단 (`_is_low_liquidity_v5`, 한화갤러리아우 패턴 방지)

  **ws_realtime_trading.py 주요 변경**:
  - 신규 상수: `UPLIMIT_V5_ENABLED`, `UPLIMIT_V5_MIN_TICKS=10`, `UPLIMIT_V5_TRAIL_ARM_CTRT=29.0`, `UPLIMIT_V5_TRAIL_PCT=0.03`, `UPLIMIT_V5_LOW_LIQ_VALUE=50억`, 관찰밴드(20~25%), 매수밴드 상한(30%)
  - 신규 state: `_uplimit_v5_evaluated`, `_uplimit_v5_limitup_reached`, `_uplimit_v5_observe_log`, `_uplimit_prev_day_amount`, `_uplimit_v5_last_acml`
  - 신규 함수: `_populate_prev_day_amount(codes)` — unified parquet value 컬럼에서 전일 거래대금 적재
  - 신규 함수: `_is_low_liquidity_v5(code, cur_acml)` — 저거래 종목 차단
  - `_check_uplimit_v4_from_tick` 전면 재작성: 28% sustain 추적 폐지, v5 로직 통합
  - `_try_v5_buy` 신규: ord_dvsn="01" 시장가, 가격 0, stck_prpr 기반 수량 (클램프 제거)
  - `_try_v4_buy` → `_try_v5_buy` alias 유지
  - `_process_uplimit_notice` 체결 시 `pos["post_buy_low"] = fill_pr` 추가 (시장가 체결가로 하드손절 기준 자동 정상화)

  **ws_realtime_tr_str1.py 주요 변경**:
  - 신규 함수: `check_uplimit_v5_instant_buy(prdy_ctrt, stck_prpr, ma10, tick_count, now_hm)` — 09:30~14:30 + 25%≤ctrt<30% + tick≥10 + stck_prpr>ma10
  - 신규 함수: `check_uplimit_v5_trail_exit(buy_price, highest_since_buy, max_prdy_ctrt, cur_price)`
  - `closing_crash_filter_signal` 차단 로직 제거

  **시뮬레이션 파일 개선**:
  - `Query_Krx_code.py`: CODE_TEXT 한화갤러리아우로 교체 (저거래 검증용)
  - `Query_str_bb_expansion_simulation.py`: BB 압축/확장 백테스트 실구현 (deque, polars 로드 추가)
  - `Query_str_ma_trend_simulation.py`: MA 추세 시뮬레이션 개선

- **Impact**:
  - Strategy A-v4 완전 폐기, v5로 전면 전환
  - 시장가 주문으로 가격제한 초과 문제 구조적 해결
  - 5분 유지 조건 제거로 상한가 직전 종목 즉시 포착
  - 저거래 종목(한화갤러리아우 패턴) 사전 차단으로 유동성 리스크 제거
  - post_buy_low 체결가 자동 정상화로 오발동 하드손절 구조적 해결

## [2026-04-22] 54476be
- **Category**: feat
- **Title**: Strategy A-v4 실전 반영 — 28% 5분유지 + 당일홀드 + 상한가 익일MA매도
- **Files**: `ws_realtime_trading.py`, `ws_realtime_tr_str1.py`
- **Changes**:
  1. **백테스트 근거**: `symulation/Query_str_uplimit_5min_hold_nextday_v2_simulation.py` — 59일 863거래, 승률 38.1%, 총 PnL +846%, 평균 +1.56%/거래
  2. **신규 전략 상수** (`ws_realtime_tr_str1.py`): `UPLIMIT_V4_SUSTAIN_MINUTES=5`, `UPLIMIT_V4_HARD_LOSS=0.05`, `UPLIMIT_V4_OVERNIGHT_CTRT=0.295`, `UPLIMIT_V4_NEXTDAY_GAP_LOSS=0.03`
  3. **신규 전략 함수 3종** (`ws_realtime_tr_str1.py`):
     - `check_uplimit_v4_sustain_buy()`: 09:30~14:30 구간, 28%≤prdy_ctrt<30%, 5분 유지 확인 후 매수 판정
     - `check_uplimit_v4_overnight_hold()`: 14:55 에 29.5%+ 이면 익일 홀드 전환 판정
     - `check_uplimit_v4_nextday_sell()`: 갭손절(-3%) / MA데드크로스(ma_fast<ma_slow) / 14:55 마감
  4. **활성화 플래그** (`ws_realtime_trading.py`): `UPLIMIT_V4_ENABLED = True`
  5. **상태 dict 4종 신규**: `_uplimit_v4_28pct_first_ts` (28% 최초 감지 시각), `_uplimit_v4_holdover` (익일 보유 포지션), `_uplimit_v4_daily_bought_codes` (중복매수 방지), `_uplimit_v4_last_sustain_log` (로그 쿨다운)
  6. **신규 함수 6종** (`ws_realtime_trading.py`):
     - `_check_uplimit_v4_from_tick()`: 매 틱 v4 메인 로직 (28% 감지·유지·리셋, 보유 관리)
     - `_try_v4_buy()`: v4 매수 주문 (`source="v4"` 로 `_uplimit_positions` 등록)
     - `_try_v4_market_sell()`: 당일 시장가 매도 (하드손절 / 14:55 당일청산)
     - `_move_v4_to_holdover()`: 당일 포지션 → `_uplimit_v4_holdover` 전환
     - `_check_v4_nextday_exit()`: 익일 overnight 포지션 매도 판정 (매 틱)
     - `_try_v4_holdover_sell()`: 익일 시장가 매도
  7. **분기 처리**: `_check_uplimit_conditions_from_tick()` 초입에 `UPLIMIT_V4_ENABLED=True` 면 v4만 수행, 기존 13필터 edge-trigger 건너뜀
  8. **상태 저장/복원**: `_save_uplimit_state()`에 `v4_holdover`, `v4_daily_bought` 추가. `_restore_uplimit_state_on_startup()`에 날짜 체크 포함 복원.
  9. **ingest_loop**: overnight 포지션 있을 때 `_check_v4_nextday_exit()` 호출 추가
- **Impact**:
  - 기존 uplimit_approach_buy_signal (13필터 edge-trigger)는 `UPLIMIT_V4_ENABLED=True`인 동안 비활성. 내일(04-24) 08:28 cron 기동 시 즉시 반영.
  - 익일 홀드 포지션은 상태파일에 저장되어 재시작 후에도 복원됨
  - 긴급 비활성: `UPLIMIT_V4_ENABLED=False` 재시작
  - MA 데드크로스는 틱 기반 ma500(≈1분봉MA5 근사) / ma2000(≈1분봉MA20 근사) 사용. 정확한 1분봉 aggregator는 별도 작업 예정.
- **로그 키워드**: `[v4_waiting]`, `[v4_매수]`, `[v4_익일홀드]`, `[v4_매도]`, `[v4_익일매도]`, `[v4_holdover복원]`

## [2026-04-23] 6e3619b
- **Category**: feat
- **Title**: Strategy B/C 신호 관찰 추가 + WSS watchdog + 종가매수 거래대금 정렬
- **Files**: `ws_realtime_trading.py`, `ws_realtime_tr_str1.py`, `symulation/Query_str_ma_trend_simulation.py`, `symulation/Query_str_bb_expansion_simulation.py`, `symulation/Query_str_combined_simulation.py`
- **Changes**:
  1. **WSS 무수신 watchdog** (`scheduler_loop`): 09:30~15:20 장 시간대 + 시작 120초 유예. 90초 무수신 → 텔레그램 경고 1회. 180초 무수신 → `os._exit(2)` 강제종료 (runner 재시작 유도). `_wss_recv_alert_sent` 플래그로 중복 알림 방지.
  2. **종목별 WSS 수신 분포 주기 로그** (`scheduler_loop`): 5분 간격, 구독 종목 활성(≤60s)/stale(>60s) 분류 출력. 내일 수신 감소 원인 진단용.
  3. **종가매수 거래대금 정렬** (`_prepare_closing_buy_orders`): 선정 종목을 `acml_tr_pbmn` 기준 내림차순 정렬. 자금 부족 시 거래대금 큰 종목 우선 배정 (기존 차단 방식 → 우선순위 방식으로 전환). 즉시 활성.
  4. **INDICATOR_MA_LINE에 ma10 추가**: `[3,10,50,200,300,500,2000]` — Strategy B MA trend용.
  5. **Strategy B (MA trend) 함수 신규** (`ws_realtime_tr_str1.py`):
     - `ma_trend_buy_signal()`: ma50>ma500 골든크로스 edge-trigger + 15~29% 구간 + 전일 <10% + 거래량 ≥100k
     - `ma_trend_exit_signal()`: ma10<ma500 데드크로스 / 손절 -3% / 트레일 -3% / 14:55 마감
  6. **Strategy C (BB expansion) 함수 신규** (`ws_realtime_tr_str1.py`):
     - `bb_expansion_buy_signal()`: BB 폭 압축(min<20) → 확장(cur>24) + 하단 이탈-복귀 + 첫 양틱
     - `bb_expansion_exit_signal()`: bb_mid 하회 절반 / bb_lower 재이탈 전량 / 손절 / 트레일 / 마감
  7. **Strategy B/C 관찰 상태 dict 신규** (`ws_realtime_trading.py`): `_ma_trend_prev_ma50/500`, `_bb_width_history`, `_bb_lower_cross_history`, `_bb_last_bidp1` 등.
  8. **`_check_strategy_bc_from_tick()`** 신규 함수: ingest_loop에서 매 틱 호출, 신호 발생 시 로그만 출력 (쿨다운 10초). `MA_TREND_ENABLED / BB_EXPANSION_ENABLED = False` 기본값.
  9. **백테스트 skeleton 3종 신규**: `Query_str_ma_trend_simulation.py`, `Query_str_bb_expansion_simulation.py`, `Query_str_combined_simulation.py` — docstring + TODO 구조 + main().
- **Impact**: 오늘(04-23) 거래 0건 원인 3가지 대응:
  ① WSS 무수신 → watchdog으로 자동 재시작 유도
  ② 종가매수 종목 선정 → 거래대금 우선순위로 자원 효율화
  ③ 전략 단일화 위험 → Strategy B/C 신호 관찰 개시 (백테스트 후 실전 전환 예정)
  Strategy B/C는 `ENABLED=False`이므로 실매매 없음. 내일 08:28 cron 기동 시 즉시 반영.
- **알려진 제약**: Strategy B/C 백테스트 미완. 승률 55%+ & 평균수익 +0.5%+ 확인 후 True 전환 예정.

## [2026-04-23] ab9e3cc
- **Category**: feat
- **Title**: 상한가 근접 전략 전면 재설계 — 28% edge-trigger + 3종목 분산 + KIS 스톱 지정가 Exit
- **Files**: `ws_realtime_trading.py`, `ws_realtime_tr_str1.py`, `ws_realtime_trading_260422.py`(백업), `ws_realtime_tr_str1_260422.py`(백업)
- **Changes**:
  1. **Top30 조회 주기 재구성** (`_build_top_rank_schedule`): 09:00:10~09:05:00 10초 주기 + 09:06~14:50 1분 주기. 기존 30분 주기 폐기. 15:18/15:20 종가매매 스케줄 유지.
  2. **Top30 구독 기준 변경** (`_top_rank_loop`): "상위 N" 방식 폐기 → prdy_ctrt ≥ 25% 모든 종목 자동 편입. 미매수 구독 종목 20% 미만 하락 시 자동 해제. MAX_WSS_SUBSCRIBE 초과 시 LRU 해제 (매수/매도 pending 종목 보호). `_top_added_ts` dict 신규 추가 (LRU 관리).
  3. **28% 상향 돌파 edge-trigger** (`uplimit_approach_buy_signal` F3): `prev_ctrt` 파라미터 추가. `prev_ctrt < 28.0 ≤ prdy_ctrt < 29.5` 인 경우만 통과. 이미 28% 이상 유지 중인 종목 중복 진입 방지.
  4. **시드 분산 3종목** (`_try_uplimit_buy`): `UPLIMIT_DIVERSIFY_N=3`, `UPLIMIT_MAX_POSITIONS=1→3`. `_uplimit_seed_base = INIT_CASH / 3` (세션 시작 시 1회 계산). 각 매수금액 = `min(seed_base, avail_cash)`.
  5. **KIS 서버 스톱 지정가 Exit** (`_setup_stop_limit_orders`): 29.5% 도달 시 `ord_dvsn=22` 스톱 지정가 주문 2건 발주. ①절반: CNDT=29%, UNPR=28% / ②나머지: CNDT=28%, UNPR=27%.
  6. **25% 하회 시장가 긴급 청산** (`_try_uplimit_market_sell`): 25% 미만 감지 시 기존 스톱 주문 전부 취소 + 시장가 전량 매도. `_uplimit_blacklist` 미추가 → 29% 재상회 시 재매수 허용.
  7. **자동 구독 해제** (`_auto_unsub_code`): 미매수 구독 종목 20% 하회 시 `_remove_code_structs` + WSS 재구성. 쿨다운 30초 (반복 해제 방지).
  8. **`uplimit_approach_exit` @deprecated 처리**: 스톱 지정가가 단계적 청산 로직 대체. 함수 몸체 호환성 유지.
  9. 신규 상수: `UPLIMIT_SUBSCRIBE_MIN_CTRT=25.0`, `UPLIMIT_UNSUBSCRIBE_CTRT=20.0`, `UPLIMIT_DIVERSIFY_N=3`, `UPLIMIT_REACH_UPPER_PCT=29.5`, `UPLIMIT_EXIT_TRIGGER_CTRT=28.0`, `UPLIMIT_EXIT_MARKET_SELL_CTRT=25.0`
  10. 신규 함수: `_setup_stop_limit_orders`, `_try_uplimit_market_sell`, `_auto_unsub_code`, `uplimit_should_cancel_and_market_sell`, `uplimit_should_setup_stop_orders`
- **Impact**: 04-23 첫 실전 운영 매수 0건 원인(30분 폴링 지연 + 구간 필터 AND 시점 지연)을 근본 해결. 장 시작 직후 10초 주기 폴링으로 급등 포착 강화. 28% 돌파 순간만 매수 시도하여 노이즈 제거. 시드 분산으로 단일 종목 집중 리스크 완화. KIS 서버 측 스톱 주문으로 연결 단절 시에도 Exit 보장.
- **알려진 제약**: KIS 스톱 지정가(ord_dvsn=22) 실전 미검증. 재매수 무제한 → 향후 일일 누적 손실 한도 추가 필요.

## [2026-04-22] e7d8e12
- **Category**: feat
- **Title**: 로그 파일 운영 모드 개선 — cron fresh vs 수동재시작 append 구분
- **Files**: `ws_realtime_trading_runner.sh`, `restart_wss_trading.sh`
- **Changes**:
  1. **[ws_realtime_trading_runner.sh]** 첫 인자로 LOG_MODE 수신 (기본값 `fresh`)
     - `fresh`: 기존 `.out` 파일이 있으면 `wss_realtime_trading_{yymmdd_HHMM}.out` 으로 rotate 후 새 파일 시작. 없으면 빈 파일 생성
     - `append`: 기존 `.out` 에 이어서 기록 — 수동 재시작 시 당일 로그 연속성 유지
     - 알 수 없는 모드 입력 시 `fresh` 로 폴백 + stderr 경고 출력
     - runner_log 및 Telegram 알림 메시지에 `log_mode` 표시
  2. **[restart_wss_trading.sh]** runner 호출 시 `append` 인자 명시 전달
     - `nohup bash "$RUNNER_SCRIPT"` → `nohup bash "$RUNNER_SCRIPT" append`
     - 에코 메시지에 log_mode 표시 추가
- **Impact**:
  - cron (인자 없음) → `fresh` 기본값 → 매일 자동 실행 시 전날 `.out` 자동 rotate, 날짜별 로그 분리로 분석 편의 개선
  - `Restart.py` → `restart.sh` → `runner.sh append` → 수동 재시작 시 기존 `.out` 에 이어서 기록하여 당일 운영 맥락 유지
  - crontab 수정 불필요 (기본값이 `fresh`)
  - Python 로거 파일 (`out/logs/wss_TR_{yymmdd}.log` 등 날짜 기반 파일) 에는 영향 없음

## [2026-04-22] 87695d0
- **Category**: chore
- **Title**: _1 백업 파일을 정식 파일로 승격 및 _0422 날짜 백업 보존
- **Files**: `ws_realtime_trading.py`, `ws_realtime_tr_str1.py`, `ws_realtime_trading_0422.py` (신규), `ws_realtime_tr_str1_0422.py` (신규), `ws_realtime_trading_1.py` (삭제), `ws_realtime_tr_str1_1.py` (삭제)
- **Changes**:
  1. **파일 승격**: 1197b66 커밋에서 `_1` 백업으로 구현한 파일을 정식 운영 파일로 대체
     - `ws_realtime_trading_1.py` → `ws_realtime_trading.py` 덮어쓰기 (상한가 근접 매수 + 종가 폭락 필터 포함)
     - `ws_realtime_tr_str1_1.py` → `ws_realtime_tr_str1.py` 덮어쓰기
  2. **날짜 백업 보존**: 직전 정식 파일을 `_0422` 날짜 스탬프로 보존
     - `ws_realtime_trading.py` → `ws_realtime_trading_0422.py`
     - `ws_realtime_tr_str1.py` → `ws_realtime_tr_str1_0422.py`
  3. **내부 참조 정리**:
     - `ws_realtime_trading.py` import 경로 `ws_realtime_tr_str1_1` → `ws_realtime_tr_str1` 복구
     - `ws_realtime_tr_str1.py` docstring 내 `(v_1)` 라벨 및 `_1` 파일 참조 정식 이름으로 정정
     - 본문 `[v_1 신규]` 주석 태그는 이력 추적 용도로 의도적 보존 (다음 리팩토링 시 정리 예정)
  4. **컴파일 검증**: `python3 -m py_compile` 통과, import 정상 확인
- **Impact**: 향후 운영은 `ws_realtime_trading.py` 정식 파일로 진행. 기존 `_1` 파일 제거로 파일 구조 정리.

## [2026-04-22] 1197b66
- **Category**: feat
- **Title**: 상한가 근접 매수 전략 (_1 백업 파일 분리 구현)
- **Files**: `ws_realtime_trading_1.py` (신규), `ws_realtime_tr_str1_1.py` (신규), `kis_signal_apis.py` (신규), `kis_utils.py`
- **Changes**:
  1. **[ws_realtime_trading_1.py]** 원본 복사 후 상한가 근접 전략 통합 11포인트
     - 상단 옵션 플래그 15개: `UPLIMIT_TRADE_ENABLED`, `UPLIMIT_MAX_POSITIONS`, `UPLIMIT_SEED_KRW`, `UPLIMIT_APPROACH_PCT_LOW/HIGH`, `UPLIMIT_EXIT_*` 등
     - 상태 dict 10개: `uplimit_holdings`, `uplimit_entry_price`, `uplimit_entry_qty`, `uplimit_peak_price`, `uplimit_half_exited`, `uplimit_signal_cache` 등
     - 신규 함수 8개: `_try_uplimit_buy`, `_try_uplimit_exit`, `_check_uplimit_exit_for_tick`, `_check_uplimit_conditions_from_tick`, `_handle_uplimit_ccnl_fill`, `_precache_uplimit_signals`, `_save_uplimit_state`, `_restore_uplimit_state_on_startup`
     - `ingest_loop` 내 전일대비 25~28% 구간 진입 감지 분기 추가
     - uplimit 보유 종목은 기존 `EMERGENCY_29PCT_SELL` (check_sell_by_prdy_ctrt_periodic) 대상 제외
     - 종가매수 `_prepare_closing_buy_orders`에 폭락 필터 통합 (`closing_crash_filter_signal`)
     - `_on_ccnl_notice_filled` 체결통보에 uplimit 체결 분기 추가
     - startup 시 `_restore_uplimit_state_on_startup` 호출로 재시작 연속성 보장
  2. **[ws_realtime_tr_str1_1.py]** 원본 복사 후 순수 판단 함수 6개 추가
     - `uplimit_approach_buy_signal`: 13필터 AND (등락률 범위, 시총, 거래대금, Volume Power, frgnmem 순매수, BB 위치, 시간, iscd_stat_cls_code 정상, VI 이력 없음 등)
     - `uplimit_approach_exit`: 5단계 우선순위 청산 판단 (대폭락 -5% > 손절 -3% > 단계적 29%→28% 절반→27% 전량 > 타임아웃 > 트레일링)
     - `closing_crash_filter_signal`: 종가매수 전 당일 급락 종목 차단 (-5% 이상 하락 또는 VP 급감)
     - `closing_next_day_exit`: 종가매수 익일 청산 판단 (갭상승/갭하락/시간 기반)
     - `calc_uplimit_qty`: 시드 기반 수량 계산 (호가단위 반올림)
     - `calc_entry_price`: 진입 기준가 계산 (현재가 + N틱)
  3. **[kis_signal_apis.py]** 신규 모듈
     - `get_volume_power`: `inquire_price` 응답의 `tday_rltv` 필드 재활용 Volume Power 조회
     - `get_frgnmem_net_buy_3d`: 3일 외국인+기관 순매수 집계 래퍼
     - `get_ranking_wrapper`: 거래량/등락률 랭킹 조회 공통 래퍼
  4. **[kis_utils.py]** `price_plus_n_ticks(price, n, market)` 헬퍼 추가
     - `_kr_tick_size` / `round_to_tick` 기반으로 `price_minus_one_tick`과 대칭 구조
- **Impact**:
  - 원본 `ws_realtime_trading.py` / `ws_realtime_tr_str1.py` 완전 무수정 — 두 버전 나란히 실행 가능
  - 시드 460K 고정, UPLIMIT_MAX_POSITIONS=1 (1종목 집중 투자)
  - 단계적 청산으로 상한가 도달 시 수익 극대화 + 대폭락 방지 손실 방어 우선
  - py_compile 4개 파일 통과, 신규 함수 6개 import 검증 완료, Buy/Exit 시나리오 sanity 통과
- **Related Docs**: `docs/01. up_limit_buy_str_260420.md`, `docs/02. Top30_str.md`, `docs/str_exec_research_0422.md`, `.claude/plans/1-elegant-tome.md`

## [2026-04-22] ecbd05c
- **Category**: docs
- **Title**: WSS 연결 안정화 변경 이력 주석 추가
- **Files**: `kis_auth_llm.py`
- **Changes**:
  1. `__runner` 메서드 내부에 WSS 연결 안정화 변경 이력 주석 8줄 추가
  2. 변경 전/후 상태 명시: `ping_interval` 기본값(20s) → `None` 비활성화
  3. PINGPONG 응답 방식 변경: `ws.pong()` → `ws.send(raw)` (KIS JSON 프레임 기대)
  4. 끊김 재발 시 복원 방법 가이드 포함
- **Impact**:
  - 코드 동작 변경 없음 (주석 추가만)
  - 향후 담당자가 WSS 설정을 롤백하거나 비교 실험할 때 근거 자료로 활용 가능

## [2026-04-22] 9295fa7
- **Category**: perf
- **Title**: ingest_loop 성능 최적화 및 Polars 전환 + 완료 리서치 파일 정리
- **Files**: `ws_realtime_trading.py`, `kis_auth_llm.py`, `fetch_top30_each_1m.py`, `kis_1d_unified_parquet.py`, `kis_1d_Daily_ohlcv_fetch_manager.py`, `Daily_inquire_vi_status.py`, `Daily_fetch_auto_run.sh`, `restart_wss_trading.sh`, `ws_realtime_trading_runner.sh`, `symulation/Query_Krx_code.py`, `symulation/Select_Tr_target_list_symulation_pdy_ctrt.py`
- **Changes**:
  1. **[ingest_loop 성능 최적화]**: partition_by 통합, `_lock` 제거, BB(볼린저밴드) incremental 처리, zfill 통합으로 불필요한 반복 연산 제거
  2. **[Pandas → Polars 전환]**: `ws_realtime_trading.py` 핵심 루프 및 `kis_auth_llm.py`의 `pd.read_csv` → `pl.read_csv` 전환으로 처리 속도 개선
  3. **[종가 결과 로그 개선]**: `_log_closing_result_after_window`를 print_table 정렬 출력 방식으로 변경하여 가독성 향상
  4. **[스크립트 개선]**: `restart_wss_trading.sh`, `ws_realtime_trading_runner.sh`, `Daily_fetch_auto_run.sh` 운영 개선
  5. **[리서치 파일 정리]**: 완료된 리서치 파일 5개 삭제 (research_260330-1.md, ws_monitoring_research_260402-2.md, 260404.md, 260407.md, 260408.md)
  6. **[기타]**: symulation 파일, Daily 운영 스크립트 업데이트
- **Impact**:
  - ingest_loop 처리 속도 향상으로 실시간 틱 데이터 처리 지연 감소
  - Polars 전환으로 대용량 데이터 로딩/처리 성능 개선
  - 완료된 리서치 파일 정리로 프로젝트 구조 간소화

## [2026-04-22] 5c5f403
- **Category**: feat
- **Title**: 듀얼 WSS 구조 도입 + VI 감지 개선 + 잔고조회 버그 수정
- **Files**: `ws_realtime_trading.py`, `ws_realtime_trading_v0421.py`
- **Changes**:
  1. **[체결가 수신 로그 개선]**: tr_id 전환 메시지를 실제 체결가 수신 + 체결가 출력 형태로 개선
  2. **[실시간체결통보 로그]**: mkop 변경 로그에 코드 의미 표시 (00=장중, 20=장마감 등)
  3. **[VI 감지 개선]**:
     - `_recent_tick_ts` 딕셔너리로 종목별 최근 5개 틱 타임스탬프 추적
     - 틱 간격 추적 기반 "갑자기 끊김" 감지 로직 추가 (`_avg_tick_interval`)
     - 끊김 감지 시 REST VI 조회(`FHPST01390000`, `_inquire_vi_status_single`) → 발동 확인 후 H0STMKO0 동적 구독
     - `_vi_trigger_info` 딕셔너리로 REST 조회 확인된 VI 발동 정보 캐싱
  4. **[듀얼 WSS 구조]**:
     - `run_ws_a2_forever` 함수 신설: syw_2(a2) 계좌 전용 WSS 데몬 스레드
     - a2 전담: H0STMKO0(장운영정보) 전 종목 + VI 발동 시 예상체결가(H0STCNI0)
     - a1에서 H0STMKO0 구독 제거 → 종목 데이터 슬롯 추가 확보 (a1은 체결가/VI 예상체결 전담)
     - `_a2_active_kws`, `_a2_kws_lock`, `_a2_subscribed`, `_a2_approval_key` 전역 변수 추가
     - `_vi_exp_sub_switch` / `_vi_exp_sub_restore`: a1/a2 역할 명확 분리
     - a2 미연결 시 a1 fallback 방어 로직 적용
     - `_mkstatus_sub_add` / `_mkstatus_sub_remove` → a2 위임 함수로 전환
     - `_a2_mkstatus_sub_add` / `_a2_mkstatus_sub_remove` 함수 신설
     - `__main__` 진입점에서 `t_a2_wss` 데몬 스레드 시작
  5. **[잔고조회 버그 수정]**: 보유종목이 없을 때 output2(잔고요약) 미저장 → 출금가능/주문가능 금액이 0으로 표시되던 문제 수정 (summary 저장 시점을 rows 반복 전으로 이동)
- **Impact**:
  - a1 WSS 슬롯 여유 확보로 더 많은 종목 실시간 체결 수신 가능
  - VI 발동 감지 신뢰성 향상 (WSS 끊김 시 REST 보완)
  - 보유종목 없는 상태에서도 출금가능/주문가능 금액 정상 표시
  - syw_2 계좌가 H0STMKO0 전담으로 장운영정보 모니터링 역할 분리

## [2026-04-21] dffdf65
- **Category**: fix
- **Title**: V2 멀티계좌 config 구조에서 is_holiday() API 호출 실패 버그 수정
- **Files**: `kis_utils.py`
- **Changes**:
  - `_is_open_day_via_api()` 함수에서 config 최상위에 appkey/appsecret이 없는 경우(V2 멀티계좌 구조) `default_user → users[default_user].accounts.main` 경로로 fallback하여 인증키를 가져오도록 수정
  - fallback 실패 시(KeyError/TypeError) 기존 RuntimeError 경로로 자연스럽게 넘어가도록 처리
- **Impact**: V2 멀티계좌 config 사용 환경에서 `is_holiday()` 호출 시 발생하던 RuntimeError가 해소됨. `Daily_inquire_vi_status.py`가 2일간 미실행된 버그 해결

## [2026-04-15] 34e8225
- **Category**: refactor
- **Title**: 4.1 안정 버전 복원 + a2 WSS 최소 추가 (0415 불안정 롤백)
- **Files**: `ws_realtime_trading.py`, `kis_auth_llm.py`
- **Changes**:
  1. **배경**: 0415 버전이 KIS 서버 throttling + a2 send_request 블로킹 + watchdog 반복종료 문제로 불안정. 안정적이었던 4.1 코드를 기반으로 a2 분리를 최소 범위로 재구현
  2. **[kis_auth_llm.py] 4.1 복원 + 4가지 수정**:
     - approval_key 파라미터 전달 및 threading.Lock 보호 유지
     - 인스턴스별 open_map(`_use_global_open_map` 플래그) 유지
     - H0STCNI0 디버그 로그 제거 → logging.debug 단순화
     - csv QUOTE_NONE 제거, 복잡한 암호화 분기 제거 → `dm.get("encrypt") == "Y"` 단순화
     - `fut.result(timeout=5)` → `fut.result()` (5초 타임아웃 제거로 블로킹 방지)
  3. **[ws_realtime_trading.py] 4.1 복원 + a2 추가 (~120줄)**:
     - `faulthandler`/`os` import 제거, `_watchdog_loop` 스레드 제거 (watchdog 반복종료 원인 원천 제거)
     - NXT 관련 코드 전량 제거 (RunMode.NXT_PRE/NXT_AFTER, ccnl_nxt, `_run_balance_0758` 등)
     - `_antc_prce_timeline`, `_print_antc_prce_timeline`, `_FUNC_NAME_TO_TRID` 역매핑 제거
     - `_query_and_print_balance`: `fetch_balance_simple` 경유 → 기존 `_get_balance_page` 직접 호출 방식 복원
     - `_get_balance_holdings`: `effective_qty = psbl if psbl > 0 else qty` (T+2 결제 전 hldg_qty 활용 복원)
     - VI 예상체결가: a2 우선 → a1 fallback (`_desired_subscription_map` 4.1 구조 복원, a2 전담 분기 제거)
     - 체결통보(H0STCNI0): a2 활성 시 a2 우선, 미연결 시 a1 fallback
     - 장운영정보(H0STMKO0): a2 온디맨드 유지(상한5개, 30초 타임아웃) + 매도 상태 로드 시 `_mkstatus_sub_add` 복원
     - `_shutdown`: a2 종료를 a1 앞으로 순서 조정, `global _active_kws` 선언 복원
     - `run_ws_forever`: 보유종목 없을 시 WSS 구독 생략 + 18:00까지 대기 로직 추가
     - `_switch_to_closing_codes`: `_a2_apply_subscriptions` 호출 제거 (4.1 구조 복원)
     - `_desired_subscription_map`: 15:30~16:00 구독 종료 구간 복원, NXT 시간대 제거
     - 건드리지 않은 것: `_desired_subscription_map` 핵심 분기, `_apply_subscriptions`, `scheduler_loop`, `_switch_to_closing_codes` 구조 — 4.1 그대로
- **Impact**: 0415 버전의 세 가지 불안정 요인(KIS throttling → a2 연결 지연 → send_request 블로킹 → ingest heartbeat 정체 → watchdog 강제종료) 원천 해소. watchdog 스레드 제거로 오판 종료 없음. 4.1 기반으로 a2 체결통보/H0STMKO0 온디맨드가 최소 범위로 동작하여 a1 슬롯 절약 및 안정성 유지

## [2026-04-15] 6dcd871
- **Category**: fix
- **Title**: a2 WSS 미연결 시 send_request 블로킹으로 인한 watchdog 강제종료 방지
- **Files**: `ws_realtime_trading.py`, `ws_merge_wss_parts_snapshot.py` (신규)
- **Changes**:
  1. **`_a2_is_connected()` 함수 추가**: `_kws_a2 is not None and _kws_a2._ws is not None` 조건으로 실제 WebSocket 연결 여부 확인. 기존 `_kws_a2 is not None` 패턴(12곳)을 전량 교체
  2. **a2 send_request 호출부 전체 가드 교체**: `_a2_wss_subscribe`, `_a2_subscribe_ccnl_notice`, `_a2_wss_subscribe_batch`, `_a2_mkstatus_subscribe`, `_a2_mkstatus_unsubscribe`, `_a2_apply_subscriptions`, `_desired_subscription_map`(3곳), `run_ws_forever`, `scheduler_loop`, `_ccnl_notice_sub_add`, `_shutdown`
  3. **`_desired_subscription_map` fallback 보장**: a2 미연결 시 a1에서 예상체결가(exp_ccnl_krx) 구독하도록 분기 처리
  4. **`ws_merge_wss_parts_snapshot.py` 추가**: 중간 병합 스냅샷 스크립트 보관
- **Impact**: KIS 서버 throttling으로 a2 WebSocket 연결이 수립되지 않은 상태(`_ws=None`)에서 send_request가 5초씩 반복 블로킹되어 ingest_loop heartbeat가 갱신되지 않고 watchdog이 127초 후 프로세스를 강제종료하던 문제 해소. a2 연결 시도 중에도 a1에서 예상체결가 fallback 구독이 정상 동작

## [2026-04-15] e22125e
- **Category**: refactor
- **Title**: _init_open_map 플래그화 + approval_key 스왑 threading.Lock 보호
- **Files**: `kis_auth_llm.py`
- **Changes**:
  1. **`_use_global_open_map` 플래그 도입**: `__init__`에서 스냅샷(`_init_open_map = dict(open_map)`) 방식을 폐기하고, approval_key 유무에 따라 플래그만 설정. a1(None)은 글로벌 `open_map` 직접 참조, a2(지정)는 빈 맵 사용
  2. **`__runner` 로직 단순화**: `hasattr` 방어 코드 제거, `open_map if self._use_global_open_map else {}` 한 줄로 대체
  3. **`send_multiple` approval_key 스왑 Lock 보호**: `threading.Lock`(`_approval_key_lock`, 클래스 변수)으로 임시 교체 블록을 감싸 a1/a2 동시 send 시 글로벌 `_base_headers_ws["approval_key"]` 경합 방지. approval_key 미사용 경로는 lock 밖에서 직접 send
  4. **`threading` import 추가**
- **Impact**: 스냅샷 방식에서 발생하던 인스턴스 생성 시점 고정 문제 해소. a1/a2 동시 send 시 approval_key가 서로 덮어쓰이던 잠재적 경합 조건 제거로 WSS 구독/해제 요청 안정화

## [2026-04-15] 41912ed
- **Category**: fix
- **Title**: a2 H0STMKO0 슬롯 오버플로우 + _shutdown UnboundLocalError 수정
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **`_a2_mkstatus_expire()` 신설**: 구독 시각(`_a2_mkstatus_ts`)을 기록해두고 30초(`_A2_MKSTATUS_TIMEOUT`) 경과 시 자동 해제. `_a2_mkstatus_subscribe`와 `_price_watchdog_loop`에서 주기적으로 호출하여 누적 방지
  2. **`_A2_MKSTATUS_MAX=5` 동시 구독 상한**: 상한 초과 시 신규 구독을 스킵하여 슬롯 고갈 방어
  3. **`_shutdown` UnboundLocalError 수정**: 함수 진입부에 `global _active_kws` 선언 추가. 일부 코드 경로에서 선언 없이 `_active_kws`를 재할당하여 UnboundLocalError 발생하던 문제 해소
  4. **`run_ws_forever` 체결통보 중복 로그 제거**: `_a2_subscribe_ccnl_notice` 호출 후 중복된 `logger.info` 제거 (이미 구독 시 스킵 처리로 충분)
- **Impact**: stale_check마다 H0STMKO0 구독이 쌓여 a2 40슬롯 초과 → MAX SUBSCRIBE OVER → 연결 반복 끊김 현상 해소. _shutdown 경로에서 발생하던 UnboundLocalError 제거로 종료 처리 안정화

## [2026-04-15] 335593f
- **Category**: fix
- **Title**: a2 WSS 재연결 시 글로벌 open_map 오염 + on_system 데드락 2건 수정
- **Files**: `kis_auth_llm.py`, `ws_realtime_trading.py`
- **Changes**:
  1. **[kis_auth_llm.py] Bug 1 — open_map 글로벌 참조 오염**: `KISWebSocket.__init__`에서 `_init_open_map` 스냅샷 저장 (approval_key 지정 시 빈 맵으로 초기화). `__runner`에서 글로벌 `open_map` 대신 인스턴스별 `_init_open_map`을 사용하여 a2 재연결 시 a1 구독 목록을 a2 approval_key로 전송하던 문제 제거
  2. **[ws_realtime_trading.py] Bug 2 — on_system 콜백 데드락**: `_a2_on_system`(asyncio 루프 내 콜백)에서 `send_request`(`run_coroutine_threadsafe`) 직접 호출 시 같은 루프에 코루틴이 추가되지만 현재 콜백이 반환되기 전까지 실행되지 않아 5초 timeout 발생. 구독 요청 로직을 별도 daemon thread로 분리(1초 대기 후 실행)하여 해결
- **Impact**: a2 WSS 재연결 시 서버가 잘못된 구독 요청을 받아 연결을 끊는 반복 현상 해소. on_system 콜백에서의 timeout으로 인한 연결 불안정 제거 → a2 WSS 안정적 운영 가능

## [2026-04-15] 2ccc3ac
- **Category**: fix
- **Title**: a2 WSS 수신 DataFrame Polars 미변환으로 인한 ingest_loop AttributeError 수정
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **`_a2_on_result` 버그 수정**: kis_auth_llm이 반환하는 pandas DataFrame을 `pl.from_pandas()`로 변환 후 `_ingest_queue`에 투입. 기존에는 pandas DataFrame을 그대로 큐에 넣어 ingest_loop에서 Polars API 호출 시 AttributeError 발생
  2. **컬럼명 소문자 통일**: `df.columns = [str(c).strip().lower() for c in df.columns]` 처리를 변환 전 추가 (a1 on_result와 동일 방식)
  3. **recv_ts 포맷 통일**: 기존 `"%H%M%S%f"[:9]` 형식에서 a1과 동일한 `"%Y-%m-%d %H:%M:%S.%f"` 형식으로 변경
- **Impact**: 미수정 시 a2 WSS(예상체결가/시간외 등) 수신 데이터 전량 유실 → watchdog 강제종료로 이어지는 치명적 버그 해소. a2 연결 데이터가 정상적으로 ingest_loop에서 처리됨

## [2026-04-14] dea17d1
- **Category**: refactor
- **Title**: H0STMKO0 상시구독 제거 → a2 WSS 온디맨드 방식으로 전환
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **a1 상시구독 완전 제거**: `run_ws_forever()` 초기 구독 블록, `_load_str1_sell_state_on_startup()` `_mkstatus_sub_add()` 2곳, `_str1_sell_worker()` 매도 완료 시 `_mkstatus_sub_remove()` 2곳 삭제
  2. **a2 온디맨드 함수 신설**: `_a2_mkstatus_subscribe()` — 미수신 종목 일시 구독, `_a2_mkstatus_unsubscribe()` — 수신 재개 또는 VI 해제 시 자동 해제
  3. **_a2_on_result에 H0STMKO0 분기 추가**: `_on_market_status_krx(df)` 직접 호출
  4. **트리거 포인트 3곳**: `_price_watchdog_loop` stale_check 시 구독 개시, `ingest_loop` WSS 수신 재개 시 자동 해제, `_on_market_status_krx` VI 해제 시 자동 해제
  5. **`_a2_mkstatus_codes` 상태 셋 추가**: a2 H0STMKO0 구독 중인 종목 추적
- **Impact**: a1 WSS 슬롯 보유종목 5~10개 절약 → a1을 순수 실시간체결가(H0STCNT0) 전용으로 확보. VI/거래정지/사이드카 감지는 stale 시에만 일시적으로 a2에서 확인하는 방식으로 효율화

## [2026-04-14] 41564da
- **Category**: feat
- **Title**: a2 WSS 역할 확장 — VI 전용 → 예상체결가 전체 + 체결통보 겸용
- **Files**: `ws_realtime_trading.py`, `ws_merge_wss_parts.py`, `test_ccnl_notice.py`
- **Changes**:
  1. `_init_a2_wss` 콜백 분기: H0STANC0/H0STOAC0 → ingest_queue, H0STCNI0 → on_result 직접 호출
  2. `_a2_on_system`: 연결 완료 즉시 체결통보(`_a2_subscribe_ccnl_notice`) + 현재 시간대 예상체결가(`_a2_apply_subscriptions`) 자동 구독
  3. `_a2_subscribe_ccnl_notice()` 신규: a2 WSS에서 H0STCNI0 1회 구독 (`_a2_ccnl_notice_done` 플래그로 중복 방지)
  4. `_a2_wss_subscribe_batch()` 신규: 배치 구독/해제 헬퍼
  5. `_a2_apply_subscriptions()` 신규: 시간대별 예상체결가 구독 전환 로직
     - 08:50~09:00: 전 종목 장전 예상체결가
     - 09:00~15:20: VI + 57/59 예상체결가 (30분 단일가 실시간 윈도우 제외)
     - 15:20~15:30: 전 종목 장마감 예상체결가
  6. `_desired_subscription_map`: a2 활성 시 위 3개 시간대에서 a1 예상체결가 구독 제거 → a1 슬롯 절약
  7. `_ccnl_notice_sub_add`: a2 우선 구독, a1 fallback
  8. `scheduler_loop` 08:29:59 체결통보 조기 구독: a2 우선
  9. `run_ws_forever` 체결통보 구독: a2 활성 시 a2에서 구독
  10. `_switch_to_closing_codes`: 15:20 전환 시 a2 구독 갱신 호출
  11. `_trigger_ws_rebuild`: a2 구독도 함께 갱신
  12. `ws_merge_wss_parts.py`: parquet 병합 시 컬럼 타입 충돌(string vs double) 자동 해소 로직 추가
- **Impact**: a1은 실시간체결(H0STCNT0) + 장운영정보(H0STMKO0) 전용으로 슬롯 여유 확보. a2는 예상체결가(H0STANC0/H0STOAC0) + 체결통보(H0STCNI0) 독립 40슬롯 운영 → 구독 슬롯 경합 해소

## [2026-04-14] 9d56507
- **Category**: feat
- **Title**: a2 계좌 별도 WSS로 VI 예상체결가 구독 분리 (lock 경합 제거 + a1 슬롯 보존)
- **Files**: `kis_auth_llm.py`, `ws_realtime_trading.py`
- **Changes**:
  - `KISWebSocket.__init__`에 `approval_key` 파라미터 추가. `send_multiple`에서 인스턴스별 key를 글로벌과 임시 스왑 후 복원 (finally 보장)
  - 프로그램 시작 시 a2(syw_2) WSS 인증 + `_a2_approval_key` 발급 (a1 상태 복원 후 진행)
  - `_init_a2_wss()`: a2 전용 KISWebSocket 생성/시작, H0STANC0 수신 시 `_ingest_queue`에 `regular_exp` 타입으로 합류
  - `_a2_wss_subscribe()`: a2 WSS로 예상체결가 동적 구독/해제, `_a2_subscribed_codes` 관리
  - `_vi_exp_sub_worker`: a2 우선 처리 → 성공 시 즉시 반환 (a1 건드리지 않음), a2 미사용 시 a1 fallback
  - `_desired_subscription_map`: a2 활성 시 `vi_codes = set()` → a1에서 VI 종목 예상체결가 구독 불필요
  - `_shutdown`: a2 WSS 종료 추가
- **Impact**: a1 WSS는 실시간체결가(H0STCNT0)를 항상 유지. VI 발동 시 a2가 예상체결가만 구독/해제하므로 `_kws_lock` 경합 원천 제거 + a1 슬롯 보존

## [2026-04-14] 35a4524
- **Category**: fix
- **Title**: lock ordering deadlock 해소 + watchdog 스킵 구간 heartbeat 리셋 (04-14 연속 crash 4회 원인)
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  - **[Fix 1] `_vi_exp_sub_worker` lock ordering deadlock 해소**
    - `scheduler_loop`는 `_mode_lock → _kws_lock` 순서로 획득
    - 기존 코드는 `_kws_lock` 내부에서 `_get_mode()` → `_mode_lock` 획득 → 교차 데드락 발생
    - `_get_mode()` 호출을 `with _kws_lock` 바깥으로 이동하여 lock 획득 순서 통일
    - ae92c53(04-02) 커밋에서 도입된 버그, 04-14 watchdog 4회 강제종료의 근본 원인
  - **[Fix 2] watchdog 스킵 구간(15:31~16:00) 종료 후 heartbeat 리셋**
    - 스킵 구간(continue) 동안 `_last_ingest_tick_ts`가 갱신되지 않음
    - 16:00 감시 재개 시 idle 시간이 1740초로 누적 → 즉시 FATAL 판정 → `os._exit(2)`
    - 스킵 구간 내 `globals()['_last_ingest_tick_ts'] = time.time()` 추가로 해소
    - 04-14 16:00 4번째 crash 원인
  - **[Fix 3] `_switch_to_closing_codes`: `_active_kws` 선행 None 처리**
    - `_active_kws = None` 먼저 세팅 후 `_request_ws_close(kws_ref)` 호출
    - 다른 스레드의 send 시도를 close 이전에 차단하여 경쟁 조건 방지
  - **[Fix 4] `_run_open_buy_order_cancel_on_startup`: 15:30~16:00 취소 스킵 추가**
    - 정규장 종료(15:30) 후 주문이 자동 실효되는 구간에서 불필요한 취소 API 호출 방지
- **Impact**:
  - 04-14 장중 4회 발생한 watchdog 강제종료(`os._exit(2)`) 재발 방지
  - VI 예상체결가 구독/해제 시 lock 교차 데드락 완전 해소
  - 종가 전환 후 다른 스레드의 WebSocket send 충돌 방지

## [2026-04-13] 19f94cc
- **Category**: fix
- **Title**: NXT 필터 누락·OVERTIME 중복·watchdog 오판 4개 버그 수정 (Issue 6~9)
- **Files**: `ws_realtime_trading.py`, `ws_log_monitor.py`
- **Changes**:
  - **[Issue 6,7]** `_load_nxt_target_codes_pre`, `_refresh_nxt_target_codes_after`에서
    KRX_code.csv `nxt=Y` 필터 추가. 기존에는 ST 필터만 적용되어 비NXT 종목이 포함될 수 있었음.
    `nxt` 컬럼 없으면 경고 로그 후 스킵(하위 호환 유지).
  - **[Issue 8]** OVERTIME 슬롯 전환 조건에 `NXT_AFTER`, `NXT_PRE` 모드 가드 추가.
    NXT 모드에서 불필요한 슬롯 전환이 발생해 동일 배너가 23회 반복 출력되던 버그 차단.
  - **[Issue 9]** watchdog가 15:31~16:00 구간에 WS 데이터 미수신을 freeze로 오판하여
    `os._exit(2)` 호출하는 버그 수정. 해당 구간 감시 스킵 추가.
    종가 체결 구독 해제 시 `_last_ingest_tick_ts` heartbeat 갱신하여 오판 방지.
  - **ws_log_monitor.py**: watchdog 강제종료·시스템재시작 CRITICAL 패턴 추가,
    NXT 대상 점검·반복 출력 MODERATE 패턴 추가, 일일 리뷰 프롬프트에 분석 관점 4가지 보강.
- **Impact**:
  - NXT 프리마켓/애프터마켓 거래 대상이 실제 NXT 적격 종목으로 정확하게 필터링됨
  - NXT 모드 중 OVERTIME 배너 폭주 차단 → 로그 노이즈 제거
  - 15:31~16:00 구간 watchdog 오판에 의한 시스템 강제 종료 방지

## [2026-04-10] d564b23
- **Category**: feat
- **Title**: ws_realtime_trading 자동 재시작 래퍼 스크립트 추가
- **Files**: `ws_realtime_trading_runner.sh` (신규)
- **Changes**:
  - `ws_realtime_trading.py`를 foreground로 실행하는 bash 래퍼 스크립트 추가
  - exit code로 종료 사유 추정(SIGSEGV=139, SIGKILL=137, SIGTERM=143, watchdog=2) → Telegram 알림
  - exit 0(정상 종료)이면 재시작 없이 루프 종료
  - 20:10 이후 재시작 중단, 하루 최대 20회 제한, 60초 미만 조기사망 시 10초 백오프
  - 런너 로그: `out/logs/runner_YYMMDD.log`
  - **crontab 변경** (git 대상 아님, 수동 적용 필요):
    - 기존: `50 22 * * 0-4 nohup python ws_realtime_trading.py > out/wss_realtime_trading.out 2>&1 &`
    - 변경: `50 22 * * 0-4 nohup /bin/bash ws_realtime_trading_runner.sh > /dev/null 2>&1 &`
- **Impact**: SIGSEGV/데드락 등 비정상 사망 시 수동 개입 없이 자동 복구. 사망 원인을 Telegram으로 즉시 파악 가능

## [2026-04-10] e6c04b9
- **Category**: fix
- **Title**: SIGSEGV 원인(dump_traceback_later) 제거 + fut.result 데드락 근본 수정
- **Files**: `ws_realtime_trading.py`, `kis_auth_llm.py`
- **Changes**:
  - `ws_realtime_trading.py`: `faulthandler.dump_traceback_later(60, repeat=True)` 1줄 삭제
    - 이유: 60초마다 polars/pandas C 확장 실행 중 Python 스택 순회 → SIGSEGV
    - 4/10 apport.log signal 11 × 3회 확인. `faulthandler.enable()` 은 유지
  - `kis_auth_llm.py`: `send_request()`의 `fut.result()` → `fut.result(timeout=5)` + TimeoutError 처리
    - 이유: `_kws_lock` 내부에서 asyncio future를 무한 대기 → 데드락 → 4/9 09:12 silent hang 9시간 원인
  - `kis_auth_llm.py`: H0STCNI0 encrypt/key/iv 상태 디버그 로그 추가
  - `kis_auth_llm.py`: `pd.read_csv`에 `quoting=csv.QUOTE_NONE` 추가 (따옴표 파싱 오류 방어)
  - `kis_auth_llm.py`: `is_encrypted` 로직 개선 — `d1[0]=="1"` 조건 병행 체크, key/iv 없을 때 skip 처리
- **Impact**:
  - 원인 체인: fut.result() 무한대기 → _kws_lock 데드락(4/9) → 진단 목적 dump_traceback_later 추가(9103d8b) → C 확장 스택 순회 SIGSEGV(4/10 3회)
  - 이번 수정으로 SIGSEGV(dump_traceback_later 제거)와 데드락(timeout=5) 양쪽 모두 해소

---

## [2026-04-10] b08c3df

### fix: 잔고조회 out2 all-zero 문제를 fetch_balance_simple() 경로로 치환
- **카테고리**: fix
- **파일**: `ws_realtime_trading.py`, `kis_inquire_balance_simple.py`
- **목적**: `_query_and_print_balance()` 내부의 `_iter_enabled_accounts + _init_account_client + _get_balance_page` 루프가 토큰/클라이언트 초기화 추정 원인으로 out2 all-zero를 반환하는 증상 해소
- **주요 변경**:
  1. `kis_inquire_balance_simple.py`: 모듈 최상위 스크립트를 `fetch_balance_simple(config_path, account_id) -> dict` 재사용 함수로 리팩터링. CLI 진입점은 `_cli_main()` + `if __name__ == "__main__"` 보존
  2. `ws_realtime_trading.py`: `_query_and_print_balance()` 내부를 `fetch_balance_simple()` 단일 호출로 치환
- **검증**: main(43444822) 예수금 13,667,957 정상 반환 실측 확인
- **영향**: 잔고조회 결과가 항상 0으로 표시되던 문제 해소. 심플 경로 단일화로 유지보수 용이

### fix: NXT 프리마켓 전일 상한가 표시 퍼센트 버그 수정
- **카테고리**: fix
- **파일**: `ws_realtime_trading.py` (L3447, `_load_nxt_target_codes_pre()`)
- **목적**: CSV 생성기(str3)가 "해당 date에 tdy_ctrt≥0.28인 종목"을 다음 거래일 대상으로 저장하는 구조에서, 표시 퍼센트를 `pdy_ctrt`(그 전날 ratio)가 아닌 `tdy_ctrt`(전일 상한가 판정 기준값)로 교체
- **주요 변경**: `row.get("pdy_ctrt")` → `row.get("tdy_ctrt")` 로 1줄 수정
- **영향**: 퍼스텍(-1.8%), 부국철강(+1.5%), 레이(+3.6%) 등 정상 상한가 종목이 비상한가처럼 오해를 유발하던 표시 버그 해소

---

## [2026-04-10] d4bfbbe

### feat: 메인 플랜 문서 + log_monitor Claude 자동분석 재작성 + intent-tracker 에이전트
- **카테고리**: feat
- **파일**: `docs/trading_project_main_plan.md` (신규), `log_monitor.py` (재작성), `.claude/agents/changelog-manager.md` (확장), `.claude/agents/intent-tracker.md` (신규)
- **목적**: 운영 의사결정 기준을 단일 문서(main plan)로 집약하고, 로그 모니터링을 Claude CLI 기반 자동분석 체계로 전환
- **주요 변경**:
  1. `docs/trading_project_main_plan.md`: §0 원칙 / §1 시간대별 / §2 불변설정 / §3 관측포인트 / §4 변경이력 구조로 초안 작성. log_monitor 판정 기준 SOT
  2. `log_monitor.py`: 정규식 패턴 매칭(CRITICAL/MODERATE/LOG_WEAK) 방식 제거. 5분 청크 수집 + offset 유지 + `claude -p` 호출 + research md 자동 작성 + summary 로그 append + CRITICAL만 Telegram. `--dry-run / --once / --interval` 옵션 제공
  3. `.claude/agents/intent-tracker.md`: 사용자 의도 추출 → main plan 섹션 갱신 에이전트 신규 정의
  4. `.claude/agents/changelog-manager.md`: 커밋 시 main plan §4 변경이력 append + 영향받는 섹션 동시 갱신 책임 추가
- **영향**: 로그 분석 기준이 main plan 문서로 일원화. log_monitor가 LLM 판단 기반으로 전환되어 미정의 패턴도 탐지 가능

---

## [2026-04-09] b70a06e

### feat: VI 현황 폴링에 H0STMKO0 실시간 교차 로깅 기능 추가
- **카테고리**: feat
- **파일**: `Daily_inquire_vi_status.py`
- **목적**: 폴링 기반 VI 현황 수집에 더해, 실시간 장운영정보(H0STMKO0)의 수신 신뢰성·지연시간을 검증하기 위한 교차 로깅 기능 추가
- **주요 변경**:
  1. **별도 WSS 연결 (a2/syw_2 계좌)**: `kis_auth_llm` + `market_status_krx`를 이용해 syw_2 계좌 approval_key로 독립 WSS 연결. `_init_ws_a2()` 함수가 auth → KISWebSocket 백그라운드 스레드 시작 (빈 open_map에서 시작, 동적 구독). ws_realtime_trading.py(a1/main 계좌)와 approval_key가 달라 40슬롯 한도가 완전 분리됨
  2. **VI 발동/해제 교차 로깅** (`out/log/VI_status_{YYMMDD}.log`): `_vi_log()` 함수가 `HHMMSS,mmm [태그] ...` 형식으로 append (thread-safe). 폴링에서 신규 VI 감지 → `[VI Status 조회_발동]` 기록 후 즉시 H0STMKO0 구독. 폴링 응답에 해제시각이 채워지면 → `[VI Status 조회_해제]` 기록 (1회만). H0STMKO0 실시간 수신 → `[장운영정보] {종목} VI_CLS_CODE: Y/N` 기록. 구독 ack → `[장운영정보 구독응답]` 기록
  3. **동적 구독 관리**: `_mk_subscribe_add()` 발동 시 `send_request(tr_type="1")`, `_mk_subscribe_remove()` VI_CLS_CODE=N 실시간 수신 시 `send_request(tr_type="2")` → 40슬롯 회전. `_seen_vi` dict로 `code_발동시각` key 단위 중복 방지
  4. **상태 추적 자료구조**: `_seen_vi`, `_code_name`, `_subscribed_codes`, `_log_lock`, `_vi_state_lock`, `_ws_lock` 추가
- **기존 동작 유지**: 1분 주기 CSV 저장, 15:20 병합/backup, 휴일 종료 로직 변경 없음
- **파급효과**: Daily_inquire_vi_status.py 프로세스가 a2 계좌로 WSS를 하나 점유. ws_realtime_trading.py의 a1 WSS와는 독립이므로 기존 매매 로직에 영향 없음

---

## [2026-04-09] 51a2687

### 1. chore: faulthandler 덤프를 전용 일자별 로그파일로 분리 (9103d8b 보완)
- **카테고리**: chore (refactor)
- **파일**: `ws_realtime_trading.py`
- **사유**: 직전 커밋(9103d8b)에서 추가한 `faulthandler.dump_traceback_later(60, repeat=True)` 가 기본 출력으로 stderr → `out/wss_realtime_trading.out` 에 쓰여, 정상 가동 중에도 60초마다 스택 덤프가 메인 out 파일에 쌓여 가독성이 크게 저하되는 문제 발생. 전용 일자별 파일로 분리하여 메인 out 파일 오염 방지
- **주요 변경**:
  1. **faulthandler 초기화 위치 이동** (L158→L271 이후): `import faulthandler` 만 상단에 남기고, `enable()` + `dump_traceback_later()` 호출을 `LOG_DIR` 정의 이후로 이동. 파일 경로(`FAULTHANDLER_LOG_PATH`) 구성이 `LOG_DIR` / `today_yymmdd` 에 의존하기 때문
  2. **전용 로그 파일 오픈** (L271~283): `FAULTHANDLER_LOG_PATH = LOG_DIR / f"faulthandler_{today_yymmdd}.log"` 생성. `_faulthandler_log_fp` 를 모듈 전역으로 유지 — faulthandler 내부에서 파일 객체를 참조하므로 GC 방지 필수. 시작 시 `pid` 포함 헤더 기록으로 프로세스 재기동 시점 구분 가능
  3. **watchdog 강제 덤프 경로 변경** (L8572~): `faulthandler.dump_traceback(sys.stderr)` → `faulthandler.dump_traceback(_faulthandler_log_fp)` 로 통일. 강제 덤프 직전 타임스탬프 + idle 경과시간 헤더 추가. `stall`(미정의 변수 오타) → `idle` 로 수정
  4. **docstring 수정**: `_watchdog_loop` 함수 설명의 stderr 참조 → `_faulthandler_log_fp` 참조로 갱신
- **기대 효과**:
  - `out/wss_realtime_trading.out` 는 정상 출력만 유지, 60초 주기 덤프 오염 없음
  - freeze 진단 증거는 `out/logs/faulthandler_{yymmdd}.log` 에 집중 — `tail out/logs/faulthandler_{yymmdd}.log` 한 줄로 원인 스택 확인 가능
  - watchdog 강제 덤프도 동일 파일에 기록되어 진단 증거가 한 곳에 모임

---

## [2026-04-09] 9103d8b

### 1. feat: burst 재접속 silent hang 방지 — faulthandler + watchdog + 배치 크기 상향
- **카테고리**: feat
- **파일**: `ws_realtime_trading.py`
- **사유**: 2026-04-09 09:12 WSS Connection exception 재접속 직후 초당 1000~2000건 burst 유입으로 약 2분 15초간 모든 로그가 멈추는 silent hang 발생. dmesg/journalctl 에 OOM·SIGKILL 증거 없음. `_kws_lock` 데드락(가설 A, 60%) 추정이나 증거 부족. 재발 시 원인 자동 수집 + 자동 재기동을 목표로 다층 방어 패치 적용.
- **주요 변경**:
  1. **배치 크기 상향** (L414~417): `PART_FLUSH_THRESHOLD` 1500→3000, `PART_FLUSH_SAVE` 1000→2000. burst 유입 시 flush 호출 빈도를 절반으로 줄여 lock 경합 부담 완화. flush 로그 reason 태그 하드코딩 제거 → `f"{PART_FLUSH_SAVE}rows"` 동적화, docstring 일반화
  2. **faulthandler 상시 활성화** (L153~159): `import faulthandler` + `faulthandler.enable()` + `dump_traceback_later(60, repeat=True, exit=False)` 추가. freeze 발생 시 60초마다 전 스레드 스택을 stderr(→`out/wss_realtime_trading.out`)에 자동 덤프하여 원인 진단 가능
  3. **ingest_loop heartbeat** (L4685~4686, L8227): 전역 `_last_ingest_tick_ts: float = 0.0` 선언. `_ingest_queue.get()` 직후 `globals()['_last_ingest_tick_ts'] = time.time()` 갱신. globals() 동적 접근으로 Pylance "_last_ingest_tick_ts is not accessed" 경고는 정상 오탐
  4. **_watchdog_loop() 신규 daemon thread** (L8519~): 10초 주기로 heartbeat 정체 감시. 60초 초과 → 텔레그램 경고 + logger.error (시간정보 포함). 120초 초과 → 치명 경고 + `faulthandler.dump_traceback(sys.stderr)` + `os._exit(2)` 강제종료(systemd/nohup 재기동 유도). `_safe_notify` 래퍼로 watchdog 자체 hang 방지. startup 지점에서 daemon thread 기동
- **기대 효과**:
  - 재발 시 nohup out 파일에 전 스레드 스택 자동 덤프 → 데드락 원인 특정 가능
  - 120초 이상 hang 시 `os._exit(2)` + 재기동으로 장시간 정지(2분 이상) 방지
  - 배치 크기 상향으로 burst 구간 flush 빈도 절감
- **2차 작업 TODO**: `_kws_lock` 데드락 근본 원인 확인 후 timeout 리팩터 (lock acquire 시 timeout 적용, 실패 시 연결 재시작) — 이번 패치로 수집한 faulthandler 덤프 분석 후 진행

---

## [2026-04-08] 578c5aa

### 1. feat: NXT/모닝 대상 필터를 nxt=Y에서 ST+500원이상으로 변경
- **카테고리**: feat
- **파일**: `ws_realtime_trading.py`, `ws_monitoring_research_260408.md`
- **사유**: KRX_code.csv의 `nxt` 컬럼이 NXT 시장 거래 가능 여부를 나타내지만, 실제 거래에서 의미 있는 필터는 시장구분(group=ST)과 단가 기준(500원 이상)임이 로그 분석을 통해 확인됨. 동전주·관리종목 유입 방지 목적으로 필터 기준 변경
- **주요 변경**:
  1. **`_load_morning_target_codes()`**: nxt=Y 대신 group=ST + close>=500 이중 필터 적용
  2. **`_load_nxt_target_codes_pre()`**: 프리마켓 대상 선정을 ST+500원↑ 기준으로 통일, `usecols=["code","group"]` 최적화
  3. **`_refresh_nxt_target_codes_after()`**: 애프터마켓도 nxt 대신 ST+500원 필터 적용 (`_last_stck_prpr` 기반 실시간 단가 사용)
  4. **단계별 로그**: ①CSV로드 → ②ST필터 → ③500원필터 → ④최종 순서로 각 단계 종목 수 기록
  5. **500원 미만 제외 로그**: 제외 종목별 종목명·코드·단가를 상세 로그로 출력
  6. **주석/변수명 정리**: `nxt_set` → `st_set`, `upper_codes` 설명 문구 갱신
- **영향**: 동전주(500원 미만) 및 비ST 종목이 NXT 프리/정규/애프터마켓 대상에서 자동 제외됨. 필터 로직이 세 함수 모두 동일 기준으로 통일되어 일관성 확보

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

## [2026-04-20] af3e7b2
- **Category**: feat
- **Title**: 종가매매 전환 안정화 + 텔레그램 로그 파일 추가
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. qty=0 제외 종목 로그 출력: `[종가매수준비]` 태그로 상한가 > 배정액 사유 명시
  2. 15:20 종가전환 시 WSS 재연결 제거 → WSS 유지, subscribe/unsubscribe만 사용 (`_switch_to_closing_codes`)
  3. 구독 해제 타이밍 15:19:55 → 15:19:10으로 앞당김 + ccnl_krx 전종목 해제 (종가대상 유지 방식 폐기)
  4. 15:21에 예상체결가(`exp_ccnl_krx`) 신규 구독 추가 + `_exp_sub_done` 플래그로 중복 방지
  5. 텔레그램 전송 메시지 전용 로그: `out/logs/YYMMDD_telegram.log` 자동 기록 (`_notify(tele=True)` 경로)
  6. `OVERTIME_SINGLE_PRICE_BUY` 플래그 신설: False 시 16:01 조기 종료 + 관련 시간외 로직 전부 게이팅
  7. `_backup_log_file()` 추가: 종료 시 `wss_realtime_trading_YYMMDD.out` 날짜별 백업
- **Impact**: 종가매매 전환 시 WSS 재연결 없이 안정적으로 구독 전환. 텔레그램 메시지 별도 파일로 사후 추적 가능

## [2026-05-23] d1864da
- **Category**: feat
- **Title**: 전일 상한가 Str2 전략 서버 라이브 wiring — 기본 비활성(STR2_ENABLED=False)
- **Files**: `ws_realtime_tr_str2.py` (신규), `ws_realtime_trading.py`
- **Changes**:
  1. **ws_realtime_tr_str2.py 신규**: per-tick 판단 함수 14개 + `Str2State`(dataclass) + 상수. kis_utils 의존만. 로컬 검증 완료 코드 서버에 그대로 이식.
  2. **STR2_ENABLED=False 플래그(145행)**: config로만 활성화 가능. 기본 OFF 상태에서 str1/v5/VI/종가 거동 100% 불변 격리 확인.
  3. **신규 스트리밍 지표**: `_str2_compute_stoch_k`(1500틱), `_str2_update_bb_hi_max`(rolling600), `_str2_baked_bb`(ddof=0).
  4. **상태 영속화**: `_save_str2_state` / `_restore_str2_state_on_startup` — 재시작 시 Str2 포지션 복원.
  5. **동기 주문**: `_str2_place_buy` / `_str2_place_sell` / `_str2_place_cancel` / `_str2_qty` + 체결처리 `_handle_str2_ccnl_fill`.
  6. **드라이버**: `_check_str2_from_tick`(11480) — tick 수신 시마다 매수/매도/취소 판단. `_check_str2_carry_premarket`(11721) — 장 시작 전 익일 carry 처리. ingest dispatch 게이트(12549) — STR2_ENABLED OFF 시 dispatch 자체 스킵.
  7. **체결통보 hook 분기**: `_on_ccnl_notice_filled`(2728) str2 종목 분기, str2 종목을 str1에 자동등록 차단(2805, `_is_str2_code` 가드) — STR2_ENABLED=False 시 기존과 동일.
  8. **게이트**: `_refresh_str2_enabled`(4778) + `_check_strategy_swap`(4831) — config 변경 감지 후 str2 모듈핸들 재바인딩.
  9. **검증**: py_compile 3파일(ws_realtime_trading / ws_realtime_tr_str2 / kis_utils) 통과, `import ws_realtime_tr_str2` OK, 게이트 전수 확인.
- **Impact**: 전일 상한가 종목 대상 Str2 전략을 서버 라이브에 완전 wiring. STR2_ENABLED=False 기본값으로 기존 전략 완전 격리. config 한 줄로 전략 활성화 가능.

## [2026-05-20] e120fef
- **Category**: fix
- **Title**: 1007 근원 차단 — websockets asyncio API lenient 패치 + 시초 backoff 단축
- **Files**: `ws_realtime_trading.py`, `ws_monitoring_research_260520.md` (신규)
- **Changes**:
  1. **[Part A] websockets asyncio API UTF-8 lenient 패치 (1007 실효 차단)**
     - 기존 legacy(read_message) 패치는 KIS SDK 가 실제로 쓰는 경로를 잡지 못함.
       SDK(kis_auth_llm.py:818)는 websockets 15.x 의 asyncio API (`websockets.connect()` → `ws.recv()`)를 사용.
       260513~260520 실측: legacy 패치 catch 0건, 1007 오류 9건 → 경로 불일치 확정.
     - 1007 발생 경로: `Assembler.get()` → `data.decode()` strict → `UnicodeDecodeError`
       → `Connection.recv`가 `protocol.fail(CloseCode.INVALID_DATA)` 호출 → 연결 종료.
     - `Assembler.get` monkey-patch 신설: 정상 프레임은 strict 그대로(동작·성능 불변),
       깨진 프레임만 `errors="replace"` 로 ? 치환 + 30s rate-limit 경고 로그 후 연결 유지.
     - `get_iter` 안전망: 모듈 전역 `UTF8Decoder` 도 lenient factory 로 치환.
     - 버전 가드: `websockets 15.x` 에서만 적용, 구조 불일치 위험 차단.
     - legacy(read_message) 패치는 `[legacy]` 태그 + 현 SDK 미사용 주석으로 유지(무해 안전망).
  2. **[Part B] 시초 near-open 핸드셰이크 실패 backoff 단축 (90s → 15s)**
     - 260520 사건: 08:59:46 handshake-fail(dwell<15s) → 90s long backoff → 09:01:16 까지 무수신.
       시초 VI 후보 구간 전체를 잃음.
     - `MAIN_WSS_BACKOFF_NEAR_OPEN_SEC = 15.0` 상수 신설.
     - 핸드셰이크 실패 다발 분기에서만 `08:55 ≤ now < 09:02` 구간 backoff 15s 로 단축, `near-open 단축` 로그.
     - `ALREADY IN USE` 분기는 KIS 측 stale session 정리에 실제 시간 필요 → 단축 제외.
  3. **분석 문서 추가**: `ws_monitoring_research_260520.md` — 260520 handshake storm 원인 분석,
     legacy 패치 0-catch 실증, asyncio API 경로 확인, Part A/B 검증 기록.
- **Impact**: 깨진 UTF-8 바이트로 인한 WSS 1007 단절 근원 차단. 개장 직전 handshake 실패 시 시초 구간 손실 방지.

## [2026-07-01] 23fc7c4
- **Category**: feat
- **Title**: VI 공식 해제(vi_cls 'Y'→'N') 순간 실시간체결 전환 신호 추가
- **Files**: `ws_realtime_trading.py`
- **Changes**:
  1. **VI_SWITCH_BY_VICLS 옵션 신설 (~100번 줄)**
     - True: H0STMKO0 장운영정보에서 vi_cls_code 'Y'→'N' 수신 즉시 실시간체결 전환
     - False: 기존 방식만 (예상체결 stck_oprc 시가형성 + 09:02 타이머)
     - True여도 stck_oprc·09:02 타이머는 폴백으로 유지 (H0STMKO0 누락/미발동 대비)
  2. **_on_market_status_krx VI해제 분기 추가 (~8550번 줄)**
     - `_vi_delayed_codes`에 있는 종목이 vi_cls 'Y'→'N' 되면 즉시 discard
     - delayed 셋이 빌 때 `_trigger_ws_rebuild()` 1회만 호출 (재구독 스톰 방지)
     - 기존 stck_oprc 경로(:13021)와 동일한 rebuild 1회 정책 적용
     - 260701 실측: 코퍼스코리아 09:02:12 vi_cls 'Y'→'N' 확인 → 기존 방식 대비 더 빠르고 정확
  3. **안전 설계**
     - H0STMKO0 는 08:59 [VI관찰-0859]가 이미 구독 중 (delayed ⊆ observed) → 별도 구독 불필요
     - 260609 붕괴 영역: MKO 전용 스레드에서 실행(메인 체결 수신 비차단), rebuild 1회 제한
     - 현재 실행 중인 프로세스에 영향 없음, 다음 기동부터 적용
- **Impact**: 09:00 예상체결 연장 종목의 실시간체결 전환이 VI 공식 해제 시점(vi_cls)에 즉각 이뤄져 구간 진입 타이밍 개선. 기존 폴백 유지로 안전성 확보.
