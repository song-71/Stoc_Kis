# Stoc_Kis Trading Project — Main Plan

_Last updated: 2026-05-21 (by: intent-tracker)_
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

### 절대 금지사항
- 커밋 시 `.env`, `config.json`, 토큰 파일(`kis_token_*.json`) 푸시
- `git add -A` / `git add .` 남용 (민감 파일 혼입 위험)
- 매도완료 T+2 결제 전 `hldg_qty` 만으로 보유 판단 (반드시 `ord_psbl_qty` 체크)

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
