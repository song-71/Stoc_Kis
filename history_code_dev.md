# Stoc_Kis 프로그램 개선 이력

---

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
