# Stoc_Kis Trading Project — Main Plan

_Last updated: 2026-04-10 (by: initial draft)_
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
| 15:20 | 종가매수 대상 전환 로그 | 미출력 = WARN |
| 20:00~20:01 | 정상 종료 로그 | 15:30 이전 종료 = CRITICAL |

---

## 4. 변경 이력 요약

_신규 항목은 `changelog-manager` 에이전트가 append._

- 2026-04-10 (initial draft) — 메인 플랜 문서 초안 작성. 운영 시간대별 섹션 + 판정 기준 수립
- 2026-04-10 — 이슈 1: 잔고조회 경로를 `kis_inquire_balance_simple.fetch_balance_simple()` 로 치환 (기존 `_iter_enabled_accounts + _get_balance_page` 경로 폐기)
- 2026-04-10 — 이슈 2: NXT 프리마켓 대상 표시 퍼센트를 `pdy_ctrt` → `tdy_ctrt` 로 수정
