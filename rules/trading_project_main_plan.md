# Stoc_Kis Trading Project — Main Plan

_Last updated: 2026-05-26 (by: intent-tracker)_
_Source: `ws_realtime_trading.py` 운영 흐름 + 사용자 의도_

이 문서는 Stoc_Kis 프로젝트의 **운영 의도·방향·판정 기준**의 단일 소스다.
- `intent-tracker` 에이전트: 사용자 의도 변경 시 해당 섹션을 직접 갱신
- `changelog-manager` 에이전트: 커밋 시 `## 4. 변경 이력 요약`에 append하고, 운영 방향에 영향 주는 변경은 해당 시간대 섹션도 함께 갱신
- `log_monitor.py`: 5분 주기로 이 문서를 컨텍스트에 포함하여 Claude CLI에 로그 청크와 함께 전달. Claude는 이 기준과 비교해 이상 여부를 판정

---

## 0. 프로젝트 목적 및 원칙

### 목적
- KIS Open API + WebSocket 기반으로 실시간 체결 데이터를 수신·매매하는 개인 트레이딩 봇
- VI(변동성완화장치) 발동 / 전일 상한가 / 종가 매수 등 이벤트 기반 전략 수행
- NXT(넥스트레이드) 프리마켓/애프터마켓 동시 운영

### 불변 원칙
1. **종목상태 판단은 KRX code DB** (`kis_utils.KRX_code_batch`) 로만. KIS REST API 의 `iscd_stat_cls_code` 는 의미가 달라 사용 금지
2. **종목코드는 항상 6자리 `zfill(6)`**
3. **텔레그램 알림은 자체 로그에도 동시 기록** (시간 prefix 필수)
4. **거래용 계좌 순회는 `_iter_enabled_accounts(trade_only=True)` 로 `trade_enabled=false` 제외**
5. **VI 매수는 `exclude_cash` 무시, `INIT_CASH` 기반 수량 계산** (test_mode=1주 고정)
6. **잔고조회는 `kis_inquire_balance_simple.fetch_balance_simple()` 경로로 통일** (검증된 심플 경로)

### 데일리 스크립트 휴일 가드 의무

크론으로 매일 실행되는 모든 Python 데일리 스크립트는 **시작 직후 `kis_utils.is_holiday()` 가드를 자체 보유**해야 한다 (휴장일이면 즉시 종료 + 텔레그램 통지).

**예외**: 쉘 래퍼(`Daily_fetch_auto_run.sh`)가 이미 휴일 체크 후 호출하는 경우, `.py` 내부 가드는 불필요.
- 래퍼 보호 대상(내부 가드 생략 허용): `kis_1m_API_to_Parquet_all_code.py`, `kis_1d_Daily_ohlcv_fetch_manager.py`
- 직접 cron 호출 또는 standalone 실행 대상은 반드시 `.py` 내부 가드 보유

**현재 가드 적용 확인 목록**: `ws_realtime_trading.py`, `fetch_daily_KRX_code.py`, `fetch_top30_each_1m.py`, `Daily_inquire_vi_status.py`, `kis_1m_API_to_Parquet_NXT.py`, `ws_log_monitor.py` (커밋 dc4a9fa)

---

### 절대 금지사항
- 커밋 시 `.env`, `config.json`, 토큰 파일(`kis_token_*.json`) 푸시
- `git add -A` / `git add .` 남용 (민감 파일 혼입 위험)
- 매도완료 T+2 결제 전 `hldg_qty` 만으로 보유 판단 (반드시 `ord_psbl_qty` 체크)

### 장운영정보(H0STMKO0) 구독 원칙

- **실질 목적은 서킷브레이커(CB) 감지다.** keepalive 구독(KOSPI/KOSDAQ 각 1종목 상시 유지)도 CB 감지 보장을 위한 것이며, WSS 연결 후 재구독 포함.
- **사이드카(mkop 187=매수/397=매도/388·398 등)는 프로그램매매 호가효력 정지이며 개인 매매에는 영향이 없다.**
  - 처리: 정보성 로그 + 텔레그램 알림만 남긴다.
  - **시장 단위 동작(REST 폴링 생략/재개 등)을 발동시키지 않는다.** 사이드카 중에도 시장은 정상 거래되므로 폴링을 끊으면 안 된다.
- **서킷브레이커류(mkop 174=발동, 184=개시, 164=시장임시정지 / 해제: 175·185)는 시장 단위 동작을 발동시킨다.**
  - CB 발동 시 REST 시세가 갱신되지 않으므로 폴링 생략이 타당하다.
  - CB 해제 수신 시 폴링 재개.

### 전략 배포/선택 원칙

1. **신규 전략 모듈은 "기본 비활성"으로 라이브에 wiring한다.**
   - 예: str2 는 `STR2_ENABLED=False` 기본. config 으로만 활성화.
   - 미활성 상태에서 기존 전략(str1/v5/VI/종가)의 거동은 바이트 동일이어야 한다. 신규 모듈 import 자체가 기존 경로에 side-effect 를 주면 안 된다.

2. **전략 활성화는 config 로 선택한다.**
   - `config["str2"]["enabled"] == true` 또는 `config["strategy_swap"] = {"module": "ws_realtime_tr_str2", "status": "requested"}` 방식.
   - 전략 교체는 모듈핸들(`import ws_realtime_tr_str2 as strat2`) + reload 방식. 프로세스 재시작 불요.

3. **신규 전략 활성 첫날은 test_mode(1주) 페이퍼 우선.** 검증 후 run_mode 전환.
   - 활성 전 백테스트 = 라이브 파리티 대조 필수.

4. **전략파일은 순수 per-tick 판단만 소유한다.**
   - 공용 계산(호가단위/수수료/상한가/손익)은 `kis_utils` 단일 출처.
   - 손절·트레일·스탑 로직은 전략파일 소유. 메인(`ws_realtime_trading.py`)에 중복 금지.

5. **로컬↔서버 지표 파리티 기준 = 서버 baked 값(BB σ ddof=0).**
   - 로컬 백테스트/뷰어가 서버 baked MA/BB 를 그대로 채택해야 backtest=live 파리티 성립.
   - 서버가 baked 값을 쓰는 한 로컬이 ddof 등 파라미터를 임의 변경 금지.

### 좀비 reaper (`ws_realtime_watchdog.py`) 운영 의무

- **기동**: 매일 08:25 KST 크론 자동 기동 (`scripts/cron_start_watchdog.sh` wrapper + crontab `25 23 * * 0-4`)
- **5분 주기 2가지 종료 규칙**:
  1. `config["ws_realtime_trading_status"] == 2` 이고 트레이딩 프로세스가 alive → SIGTERM → 5s 대기 → SIGKILL
  2. 20:00 이후 프로세스가 alive 이면 status 무관 강제 종료
- **배경**: 2026-05-20 발생한 좀비 트레이딩 프로세스가 6일간 CLOSE-WAIT 누수 + runner 영구 wait 점유 (2026-04/05-06 사건과 동일 패턴). 커밋 6f0c2b6

---

### WSS 자동복구 표준 패턴 (260526 09:01 사고 후 수립)

1. **`send_request` 는 timeout 필수**: `kis_auth_llm.py` `KISWebSocket.send_request` 의 `fut.result(timeout=10.0)`. 타임아웃 미설정은 scheduler_loop 영구 wedge 원인이므로 절대 무한대기 금지.
2. **무수신 watchdog 은 scheduler_loop 와 분리된 독립 데몬 스레드**: `_wss_norecv_watchdog_loop`. scheduler 정지 시에도 동작 보장하여 무수신 임계(180s) 도달 시 `os._exit(2)` → runner 재시작.
3. **무수신 watchdog 활성 시간창**: 09:00~15:20 (구 09:30~ 에서 확대 — 개장 직후 사각지대 제거).
4. **`_trigger_ws_rebuild` 연속실패 에스컬레이션**: 연속실패 카운터(`_REBUILD_FAIL_EXIT_LIMIT=5`) 초과 시 `os._exit(2)` 에스컬레이션. 진단 메시지("dead socket 의심")가 진짜 원인(락 점유)을 가리는 케이스에 대한 안전판.
- 커밋 04f9e92 / 근거 문서: `ws_monitoring_research_260526.md` (커밋 54b9be6)

---

### WSS approval_key 관리 표준 (260601 사고 후 수립) — ★WSS 두절 시 1순위 점검

**증상 식별**: WSS 가 `code=1006` (no close frame) 으로 **연결 직후 ~0.1초(dwell≈1.1s)** 만에 끊김이 **반복**되는데, 로그에 `ALREADY IN USE`(OPSP8996)·`invalid approval`·`[ws][system]` 이 **0건**이면 → **approval_key 무효화**를 1순위로 의심한다. (슬롯잠금 OPSP8996 과 **다름**.)

**근본 원리 (260601 재현 확정)**:
1. KIS 는 **같은 appkey 에 새 approval_key 를 발급하면 직전 키를 즉시 무효화**한다 (최신 1개만 유효).
2. 무효화된 키로 (재)접속/(재)구독하면 → `invalid approval` → bare 1006, ~0.1초 사망.
3. **이미 붙어있는 연결은 키가 무효화돼도 죽지 않는다** (재접속/재구독 시점에만 키 검증).

**관리 규칙 (절대 준수)**:
1. **approval_key 캐시는 appkey(계좌)별 + 날짜별 분리** 저장 (`config/approval/KIS_approval_{md5(appkey)[:12]}_{YYYYMMDD}`). **날짜별 단일 파일 금지** (a1/a2 키 충돌 원인 — 과거 단일계좌 복원 사고의 근본).
2. **발급은 계좌별 1회 + 공용 재사용** (KIS 24h 유효 — 불필요한 재발급 금지). 모든 프로그램은 `kis_auth_llm.auth_ws()` 중앙 함수만 사용하고 **`/oauth2/Approval` 직접 호출 금지**(캐시 우회 = 타 소비자 키 무효화).
3. **재발급 전 파일 재읽기 가드**: 무효 감지 후 재발급하기 전에 캐시를 다시 읽어, 다른 주체가 이미 갱신했으면 그걸 채택(재발급 생략) → 교차 무효화 핑퐁 차단. 파일 쓰기는 **원자적(temp→replace)**.
4. **[C] self-heal**: `run_ws_forever` 가 `dwell<15s` 감지 시 다음 재접속에서 `auth_ws(force_new=True)` 강제 재발급 (`run_ws_forever._force_new_approval`). 무효 캐시 키를 영구 재사용하던 버그의 자동 회복.

**구현**: `kis_auth_llm.py` (`_approval_tmp_path`/`save_approval_key`/`read_approval_key`/`auth_ws(force_new)`), `ws_realtime_trading.py` (dwell<15s → force_new). 근거: 메모리 `project_wss_approval_key_invalidation.md`, 백업 `kis_auth_llm.py.bak_260601_approval`.

---

### 매도 체결 텔레그램 필수 정보

매도 체결 텔레그램 메시지에는 다음 항목 **모두** 포함 의무:
- 종목명(코드), 수량, **매수가**, 매도가, **사유(reason)**, PNL(원/%), 시간정보(`ts_prefix`)

구현: `ws_realtime_trading.py` `[체결통보-체결]` 발송부에서 `_str1_sell_state[code]` 의 `buy_price` / `sell_reason` 조회 후 메시지 본문 첨부. 매수체결/외부매도/사유 미기록 케이스는 빈 문자열 허용.

**배경**: 2026-05-26 광진실업 트레일스톱 매도 시 텔레그램에 사유 누락 → 사용자가 매도 근거 인지 불가. 커밋 04f9e92

---

_(배경: str2 전략(전일 상한가 종목 매수/매도)을 서버 라이브에 전체 wiring 완료, 기본 비활성으로 배포 — 커밋 d1864da. stoch_k+bb_hi_max 스트리밍 지표 추가. kis_utils 일원화(FEE_RATE/get_tick_size/calc_sell_pnl) 선행 완료.)_

---

### 성능 원칙 — 주문 핫패스 지연 금지

**원칙**: 매도/매수 **주문 발동 → KIS 주문 전송** 사이의 핫패스에는 **블로킹 동기 호출을 두지 않는다.**

**금지 항목**:
- REST 잔고조회(`_get_balance_holdings` 등 TTTC8434R)를 주문 직전 동기 호출 금지. 수량 검증은 사후 APBK0400 예외처리 안전망에 위임
- 텔레그램 동기 전송(`_notify(tele=True)`, `requests.post`)을 주문 직전 호출 금지. 통지는 주문 **이후** `_notify_async`(비동기, fire-and-forget)로
- 기타 네트워크/디스크 블로킹 작업을 주문 직전 경로에 추가 금지

**KPI**: 발동(매도 큐 등록) → 주문 전송 내부 지연 **목표 ≤ 50ms**. 수백 ms 이상이면 블로킹 회귀로 간주하고 즉시 조사. _(임계값은 운영자가 조정 가능)_

**관측 로그**: 매도 워커가 `[sell_latency] {종목}({코드}) 발동→주문 {ms}ms` 를 주문 직전 출력 (`_enqueue_str1_sell` 의 `enq_ts` 기준). `log_monitor` 가 비정상 대기값 감지 시 WARN.

**배경**: 2026-05-21 아이진(185490) 매도 — 발동 28.136 → 실제 주문 ~30.660 (~2.5초). 원인 = 주문 직전 REST 잔고 재조회 (~1.5초) + 텔레그램 동기 전송 (~1.0초). 커밋 25f936e 에서 핫패스 제거 확정.

---

## 1. 운영 시간대별 운영 방향 (`ws_realtime_trading.py`)

### 07:50 — 시작 잔고조회 (`_print_startup_balance` → `_query_and_print_balance`)
- **목적**: main 계좌 예수금/보유종목 파악 → 이후 VI 매수 예산·보유종목 매도 로직 초기화
- **경로**: `kis_inquire_balance_simple.fetch_balance_simple()` 사용 (2026-04-10 치환됨)
- **정상 기준**:
  - `dnca_tot_amt > 0` 또는 `tot_evlu_amt > 0` 중 하나 이상 참
  - 로그 테이블에 `[시작 잔고조회]` 블록이 출력
- **이상 판정**:
  - 출금가능/주문가능/총평가 모두 0 → **CRITICAL** (외부 출금/토큰/앱키 이슈 의심)
  - `조회 실패` 로그 → **CRITICAL**

### 07:58 — NXT 프리마켓 직전 잔고 재조회 (`_run_balance_0758`)
- **목적**: NXT 프리마켓 진입 전 현재 보유종목 최종 확인
- **정상 기준**: `[07:58 잔고조회(NXT)]` 로그 출력 + 텔레그램 전송
- **이상 판정**: 07:50 과 `dnca_tot_amt` 차이가 ±10% 초과 시 **WARN**

### 08:00~08:50 — NXT 프리마켓 (`RunMode.NXT_PRE`)
- **대상 선정** (`_load_nxt_target_codes_pre`):
  1. `symulation/Select_Tr_target_list.csv` 최신 date 행 로드
  2. KRX `group=ST` 필터
  3. CSV `close ≥ 500원` 필터
  4. 보유종목 무조건 추가 (방어)
- **표시 퍼센트**: **`tdy_ctrt` 컬럼** (해당 date 당일 ratio, 2026-04-10 수정됨). `pdy_ctrt` 사용 금지
- **매매 규칙**:
  - `VI_TRADE_MODE=off` → 모니터링만
  - `VI_TRADE_MODE=test_mode` → 1주 고정
  - `VI_TRADE_MODE=run_mode` → `INIT_CASH` 기반 수량 계산
- **이상 판정**:
  - 대상 종목 표시 퍼센트가 **+28% 미만**인 것 포함 → **WARN** (표시 버그 또는 CSV 오염)
  - NXT WSS 구독 실패 → **CRITICAL**

### 08:50~09:00 — 동시호가 예상체결 (`RunMode.PREOPEN_EXP`)
- 구독을 **예상체결가**로 전환
- 이상: 예상체결 수신 없음 → WARN

### 09:00~15:20 — 정규장 실시간 (`RunMode.REGULAR_REAL`)
- **구독**: 예상체결 → 실시간 체결
- **VI 감지**: `FHPST01390000` REST 폴링 기반 전체 시장 VI 감지 (`_vi_poll_check`)
- **매수 조건**: `ws_realtime_tr_str1.vi_buy_strategy` (VI 해제 직후 실시간체결가 기준)
- **매도 조건**: `check_vi_sell` — TP/SL/VI재발동/종가 조건 조합
- **매수가 손절 가드** (`ws_realtime_trading.py` `_check_str1_conditions_and_sell`):
  - `LOSS_CONFIRM_TICKS=30` 틱 연속 이탈 시에만 매도 발동 (단일 틱 fake 가격 차단)
  - `OPENING_GRACE_SEC=30` 초간(09:00:00~09:00:30) 임계 강화: -3% → `OPENING_GRACE_LOSS_PCT=-7%` (우선주 등 thin liquidity 첫 틱 보호 — 260428 SK증권우 사례)
  - 임계 위 회복 시 카운터 자동 리셋. loss-pending 동안엔 데드크로스 재시도도 보류 (loss 우선)
- **이상 판정**:
  - `매[수도]주문.*실패` → **CRITICAL**
  - `Traceback` → **CRITICAL**
  - `RemoteDisconnected` / WS 끊김 → **WARN** (재접속 로직이 복구 성공하면 OK)
  - 5분간 체결 데이터 수신 0건 → **CRITICAL**

### 15:20~15:30 — 종가 동시호가 (`RunMode.CLOSE_EXP`)
- `_switch_to_closing_codes()` 로 종가매수 대상 교체
- 종가 매수 조건 충족 시 주문
- `closing_buy_limit_up_only=true` 면 당일 상한가 종목만 매수

### 15:30~15:40 — 종가 실시간 (`RunMode.CLOSE_REAL`)
- 종가 체결 수신
- 종가매수 주문 로그 즉시 출력

### 15:40~16:00 — 장후 시간외종가 (`CLOSE_REAL` 지속, NXT 진입 전)
- 시간외종가 매매 구간
- `45ac0b8` 이후 상한가 종목 장전/장후 시간외종가 매수는 **비활성**

### 16:00~20:00 — NXT 애프터마켓 (`RunMode.NXT_AFTER`)
- 대상: 당일 `prdy_ctrt ≥ 29.5%` + ST + 500원↑
- 재시작 시 `_last_prdy_ctrt` 비어있으면 전일 상한가 CSV 로 폴백
- 대상 0종목이고 `NXT_AFTER_EARLY_EXIT=true` 면 조기 종료

### 20:00~20:01 — 종료 (`RunMode.EXIT`)
- `END_TIME = 20:01` 에 정상 종료
- 일일 리뷰 로그 작성
- **비정상 종료 기준**: `=== WSS END ===` 가 15:30 이전에 출력되면 CRITICAL

---

## 2. 계좌/환경 불변 설정

- **main**: cano `43444822`, alias `a1`, `acnt_prdt_cd=01`, `trade_enabled=true`, `exclude_cash=13,200,000`, `max_invest=300,000`
- **syw_2**: cano `63614390`, alias `a2`, `trade_enabled=false` (거래 제외)
- **config.json** 은 `.gitignore` 대상 — git 커밋 대상 아님
- **VI_TRADE_MODE**: `off` / `test_mode` / `run_mode`
- **no_sell_codes**: 매도 금지 종목 리스트 (config.json)
- **`trade_enabled` 필드 값**: JSON 소문자 `true` / `false`

---

## 3. 관측 포인트 & 이상 판정 기준 (Claude 검토 기준)

`log_monitor.py` 가 5분마다 Claude CLI 에 이 섹션을 컨텍스트로 전달한다. Claude 는 해당 시간대 규칙에 어긋나는 라인이 있으면 severity 를 매긴다.

### 공통 CRITICAL 패턴
- `매[수도]주문.*실패` · `취소\s*실패`
- `Traceback`
- `=== WSS END ===` (15:30 이전에 발생 시)
- `프로그램\s*종료` (예정 시각 외)
- `조회\s*실패` (잔고조회 실패)
- 잔고 테이블 all-zero (`출금가능 0` + `주문가능 0` + `총 평가금액 0`)

### 공통 WARN 패턴
- `RemoteDisconnected`
- `WebSocket.*(closed|lost|disconnect)` (재접속 복구되지 않을 때)
- `구독.*실패`
- `no data for.*resubscribe`
- `| WARNING |` 레벨 로그

### 장운영정보(H0STMKO0) 감지 기준
| mkop 코드 | 의미 | 처리 방향 |
|---|---|---|
| 174 | 서킷브레이커 발동 | CRITICAL 알림 + REST 폴링 생략 |
| 184 | 서킷브레이커 개시 | CRITICAL 알림 + REST 폴링 생략 |
| 164 | 시장임시정지 | CRITICAL 알림 + REST 폴링 생략 |
| 175 / 185 | 서킷브레이커 해제 | INFO 알림 + REST 폴링 재개 |
| 187 / 397 / 388 / 398 | 사이드카(매수/매도) | INFO 로그 + 텔레그램 알림만. 시장 동작 변경 없음 |

- H0STMKO0 수신 0건(keepalive 종목도 포함해서)이 장중 지속되면 → **WARN** (WSS 구독 누락 의심)

### 시간대별 특이 체크
| 시간대 | 체크 포인트 | 위반 severity |
|---|---|---|
| 07:50 | 시작 잔고 테이블 출력 + 금액 > 0 | 미출력/all-zero = CRITICAL |
| 07:58 | `[07:58 잔고조회(NXT)]` 출력 | 미출력 = WARN |
| 08:00 | NXT 프리마켓 대상 표기 tdy_ctrt ≥ +28% | 미달 종목 포함 = WARN |
| 09:00~15:20 | 분당 체결 수신 건수 > 0 | 5분 0건 = CRITICAL |
| 09:00~15:20 | `[sell_latency]` 수백 ms 이상 | WARN (블로킹 회귀 의심) |
| 15:20 | 종가매수 대상 전환 로그 | 미출력 = WARN |
| 20:00~20:01 | 정상 종료 로그 | 15:30 이전 종료 = CRITICAL |

---

## 4. 변경 이력 요약

_신규 항목은 `changelog-manager` 에이전트가 append._

- 2026-04-10 (initial draft) — 메인 플랜 문서 초안 작성. 운영 시간대별 섹션 + 판정 기준 수립
- 2026-04-10 — 이슈 1: 잔고조회 경로를 `kis_inquire_balance_simple.fetch_balance_simple()` 로 치환 (기존 `_iter_enabled_accounts + _get_balance_page` 경로 폐기)
- 2026-04-10 — 이슈 2: NXT 프리마켓 대상 표시 퍼센트를 `pdy_ctrt` → `tdy_ctrt` 로 수정
- 2026-04-10 — 이슈 3 (e6c04b9): SIGSEGV 원인 제거 — `faulthandler.dump_traceback_later(60)` 삭제 (C 확장 스택 순회 → signal 11, 4/10 3회 확인)
- 2026-04-10 — 이슈 4 (e6c04b9): 데드락 근본 수정 — `kis_auth_llm.send_request()` `fut.result()` → `fut.result(timeout=5)` (`_kws_lock` 내 asyncio 무한대기 → 4/9 silent hang 9시간 원인)
- 2026-04-10 — 이슈 5 (d564b23): 자동 재시작 래퍼 `ws_realtime_trading_runner.sh` 추가 — SIGSEGV/데드락 등 비정상 사망 시 Telegram 알림 + 자동 복구 (20:10 이후/20회 초과 시 중단). crontab을 runner.sh 경유로 변경 필요
- 2026-05-21 (intent-tracker) — 주문 핫패스 지연 금지 원칙 수립 (목표 ≤50ms, 수백 ms = 블로킹 회귀 WARN). 배경: 아이진 매도 2.5초 지연 (커밋 25f936e)
- 2026-05-22 (intent-tracker) — H0STMKO0 운영 원칙 수립: 실질 목적=CB 감지. 사이드카는 정보성 로그/알림만(시장 동작 변경 없음), CB류만 REST 폴링 생략/재개 발동. mkop별 처리 기준 표 추가
- 2026-05-23 (intent-tracker) — 전략 배포/선택 원칙 5개 항목 신설 (str2 기본 비활성 wiring, config 활성화, test_mode 첫날 페이퍼, 전략파일=per-tick 판단 전용, 서버 baked 파리티 기준)
- 2026-05-26 (intent-tracker) — 데일리 스크립트 휴일 가드 의무 원칙 수립. 직접 cron 호출 스크립트는 .py 내부 is_holiday() 가드 필수, 래퍼 보호 시 예외 허용 (커밋 dc4a9fa)
- 2026-05-26 (intent-tracker) — 좀비 reaper ws_realtime_watchdog.py 운영 의무 수립. 08:25 기동, 5분 주기 status=2 강제종료 + 20:00 이후 무조건 종료 (커밋 6f0c2b6)
- 2026-05-26 (intent-tracker) — WSS 자동복구 표준 패턴 4개 항목 수립 (send_request timeout 필수, 무수신 watchdog 독립 스레드, 활성 시간창 09:00~15:20, 연속실패 os._exit 에스컬레이션). 커밋 04f9e92
- 2026-05-26 (intent-tracker) — 매도 체결 텔레그램 필수 정보 원칙 수립 (매수가+사유+PNL 포함 의무). 커밋 04f9e92
