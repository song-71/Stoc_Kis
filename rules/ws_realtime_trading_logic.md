# 이 파일은 `ws_realtime_trading.py`(프로덕션)의 작동 로직 파일 — 별칭: "로직 파일"

> **별칭 정의:** 이 파일(`rules/ws_realtime_trading_logic.md`)은 `ws_realtime_trading.py` 의 작동 로직을
> 정리한 문서이며, 그 별칭은 **"로직 파일"** 이다. 사용자가 "로직 파일" 이라고 하면 **이 문서(`rules/ws_realtime_trading_logic.md`)를 지칭**하는 것으로 이해한다.

> **이 문서의 목적/사용 규칙**
> - 이 문서의 존재 이유: `ws_realtime_trading.py` 는 방대해 매번 전체를 다시 읽어 로직을 재확인하기 어렵고,
>   그 과정에서 단편만 보고 나머지를 **지레짐작·단정**하는 오판이 반복된다. 그 헛수고와 오판을 없애기 위해
>   **로직을 한 번 정확히 정리해 두고, 이후엔 이 문서를 신뢰 소스로 삼는다.**
> - 그래서 **이 문서에는 코드로 검증된 것만** `파일:라인` 근거와 함께 적는다. **미검증은 "미검증"으로 표시**하고
>   상상으로 채우지 않는다(지레짐작을 여기 적으면 그 자체가 오염원이 된다).
> - **코드 검토·수정 전에 반드시 이 문서를 먼저 읽는다.** (이미 함께 만든 로직을 잊고 뒤엎는 오판 방지)
> - **`ws_realtime_trading.py` 를 검토·수정하다가 이 문서에 없던 새 로직을 파악하면, 그때마다 이 문서의
>   관련 섹션과 맨 아래 "변경 이력" 을 함께 갱신한다.** (코드만 고치고 로직 파일을 방치하지 않는다.)
> - 운영 의도/원칙의 최상위 기준은 `rules/trading_project_main_plan.md` 이며, 이 문서는 그 의도가 코드에서
>   **어떻게 구현되어 있는지**를 설명한다.

작성 시작: 2026-07-03. 아래 섹션 중 "핵심 검증됨" 표기가 있는 부분만 실측/코드로 확인된 내용이며, 나머지는 점진 보강한다.

---

## 0. 계좌·연결 구조 — 핵심 검증됨

- **계좌 구성(config.json, `default_user=sywems12`)**: 개인1 = **a1**(cano 43444822, `trade_enabled=True`, 메인)·**a2**(cano 63614390, `trade_enabled=False`). 현재 config 에는 a1/a2 두 계좌만 존재. (개인2 b1/b2… 는 향후 확장 개념 — 현 config 미존재, 미검증)
- **프로덕션 = a1 계좌 단일 프로세스·단일 WSS 연결.** 체결(H0STCNT0)·예상체결(H0STANC0)·**장운영정보(H0STMKO0)**·체결통보(H0STCNI0) 를 **모두 a1 한 연결**에서 tr_id 로 멀티 수신한다. (근거: `ws_orderbook_recorder.py:19~24`)
  - 이유: 라이브러리 `kis_auth_llm` 이 approval_key·open_map·data_map 을 모듈 전역으로 관리 → **"1프로세스=1연결"** 구조. 한 프로세스에서 a1·a2 두 연결을 띄우면 전역 상태가 엉켜 서로 구독을 침범(과거 이중연결 실패 근본원인).
  - **따라서 물음의 답: 장운영정보(H0STMKO0) 구독 = a1. VI 발생 시 예상체결 전환(ccnl→exp)도 = a1.** (프로덕션 단일 연결이라 전부 a1에서 일어남)
- **호가(orderbook, H0STASP0) 기록 = a2 계좌, 별도 프로세스**(`ws_orderbook_recorder.py`). 프로덕션(a1)과 독립. (근거: `ws_orderbook_recorder.py:17,28`)
  - 별도 프로세스인 이유: 같은 a1 appkey 로 새 approval_key 를 발급하면 직전 키가 무효화되어 프로덕션이 즉사(메모리: WSS approval_key 무효화). 그래서 기록기는 a2 appkey 로만 접속.
- 모듈레벨(import 시점)에서 a1 실인증을 수행: `ws_realtime_trading.py:763~764` (`ka.auth`/`ka.auth_ws`). **→ 이 파일을 단순 import 하면 새 approval_key 발급으로 가동 중 프로덕션이 즉사할 수 있다. 오프라인 검증 시 주의(§검증 참조).**

## 1. 구독 생명주기 (전일 상한가 종목) — 핵심 검증됨

- 대상: **전일(직전 거래일) 종가 상한가 마감 종목**이 익일 구독 대상이 된다. (검증 전략 EarlyPop-Trail 계열)
- 08:50 — 대상 종목을 **예상체결가(H0STANC0)** 로 구독 시작. 로그: `08:50 예상체결가 구독 시작: N종목 [...]`
- 09:00 — 예상체결가 구독 중지 → **실시간 체결가(H0STCNT0)** 구독으로 전환. 로그: `09:00 예상체결가 구독 중지 -> 실시간 체결가 구독 시작`
- VI 발동 시 해당 종목만 실시간체결(ccnl) → 예상체결(exp) 로 스왑, 해제 시 복귀 (§3 참조).
- 근거: 7/2 로그 실측 (보해양조·금호건설 = 7/1 상한가 → 7/2 구독). `_trigger_ws_rebuild()` = `ws_realtime_trading.py:4851`.

### 1.1 아침 장전 구독 대상 결정 — 핵심 검증됨
구독 대상은 **`symulation/Select_Tr_target_list.csv` 의 "가장 최근 일자" 행 + 주식(group=ST)** 으로 정해진다.

- `_load_morning_target_codes()` (`ws_realtime_trading.py:4459~4522`):
  1. **CSV 갱신:** `Select_Tr_target_list_symulation_pdy_ctrt.py` 를 subprocess 로 실행(`:4471~4482`).
  2. **최근일자 필터:** `last_date = df["date"].max()` → 그 날짜 행의 `symbol` 을 6자리 zfill 로 추출(`:4493~4495`).
  3. **주식 필터:** `data/admin/symbol_master/KRX_code.csv` 에서 `group=="ST"` 인 코드만 통과(`:4498~4508`).
  4. **실패 시 폴백:** `_load_prev_closing_codes()`(거래장부 '종가매수' 마지막 날짜 code) (`:4520~4522`).
- 대상 선정 스크립트(`Select_Tr_target_list_symulation_pdy_ctrt.py`): 현재 전략 `str3` = 당일 등락률(tdy_ctrt) ≥ `0.28`(상한가) & 전일종가 ≥ 500원 → **다음 거래일 적용대상**. 상장폐지예정(iscd 58) 제외. 출력 = `Select_Tr_target_list.csv`.
- 모듈 로드 시 반영: `_morning_target_codes = _load_morning_target_codes()` (`:4722`) → `codes = _load_codes_from_cfg(...)` (`:4732`).
- **config.json `wss_subscribe_codes`/`wss_subscribe_date` 는 구독대상 소스가 아니다.** `TOP5_ADD`(`:84`, 현재 `False`)가 True 이고 **당일 장중 재시작** 일 때만 복구 캐시로 쓰인다(`_load_codes_from_cfg:4597~4642`). 현재 설정(TOP5_ADD=False)에서는 항상 아침 CSV 결과(base_codes)로 시작하며 config 캐시는 무시된다.

## 2. VI 감지 — 2개 경로 (병행) — 핵심 검증됨

VI 발동/해제는 **두 경로**로 감지된다. 둘은 상호보완(중복 방지 로직 포함).

| 경로 | 트리거 소스 | 코드 위치 | 로그 태그 | 텔레그램 |
|---|---|---|---|---|
| A | H0STMKO0 장운영정보 `vi_cls_code` 상태전이('Y'↔'N') | `ws_realtime_trading.py:8520~` | `[VI발동]`/`[VI해제]` | **미발송**(260630부터 로그만, `_notify(msg)` tele 없음, :8544) |
| B | 현재가조회(REST) `vi_cls_code` = 'Y' | `ws_realtime_trading.py:8758~8778` | `[VI감지-REST]` | **발송**(`_notify(msg, tele=True)`, :8777) |

- 발동상태 판정: `vi_cls_code` 가 `"N"`/`"0"`/`""` 가 아니면 발동. 단순 문자열변화가 아니라 **상태전이**로 판정(경로 A, :8530~8532). 실측상 KIS 는 발동='Y'/해제='N' 으로 보냄(:8521 주석).
- 경로 B가 발동을 먼저 감지하면 `_vi_cls_cache[code]='Y'` 로 맞춰, 직후 H0STMKO0 'Y' 프레임이 경로 A에서 중복 [VI발동] 발사되는 것을 차단(:8766~8772).
- **텔레 정책 요지: 사용자에게 수시로 오는 "VI 발동" 텔레는 경로 B `[VI감지-REST]` 이며, 구독 종목(=전일 상한가 종목)에 한정된다.** (별도 스크립트 `Daily_inquire_vi_status.py` 는 REST 1분 폴링을 CSV/로그로만 남기고 종목별 텔레는 보내지 않음 — 시작/종료 메시지만.)
- **경로 B 의 REST 폴링을 트리거하는 실체 = `_price_watchdog_loop`(`:8848~`).** 상시 폴링이 아니라, **활발히 거래되던 종목(`_avg_tick_interval`<5s)이 갑자기 멈추면 VI 로 의심**해 `stale_check`(현재가조회)를 큐잉하고, 그 결과 `vi_cls_code='Y'` 면 경로 B 가 발동한다. **[260714] 이 "갑자기 끊김→VI 의심" 감지는 종전 09:10 게이트 안에만 있어 개장 직후(09:00~09:10)에 상승 VI 가 걸린(이미 시가를 받은) 종목을 놓쳤다**(260713 디와이에이 09:04 사건). → 감지 블록을 09:00 가드 직후로 이동해 **09:00~15:30 상시 동작**하도록 확장(`_vi_delayed_codes` 09:00~09:02 예상체결연장 종목은 제외). 배경: `ws_monitoring_research_260713.md`.
- **경로 A(H0STMKO0)의 전제 = 해당 종목이 H0STMKO0 구독 중일 것.** H0STMKO0 는 슬롯 상한 안에서만 구독되며(§6 슬롯 배분), **[260714] 이 상한이 정적 5 → 동적(`_h0stmko0_slot_cap`)** 으로 바뀌어 장초 고등락 종목이 관찰구독에서 탈락하지 않게 됐다(260713: 데이터 8종목뿐인데 cap 5 로 002880 탈락 → 경로 A 무력화됐던 사건).

## 3. 장운영 이벤트 (서킷브레이커 / 사이드카 / VI단일가) — 핵심 검증됨 + 260703 변경

핸들러: `_on_market_status_krx` (`ws_realtime_trading.py:8398~`). H0STMKO0 는 **보유·VI·관찰 종목 단위로만** 구독된다(시장 전체 구독 아님).

### 3-1. 원값 로깅
- 세 필드(`mkop_cls_code`, `antc_mkop_cls_code`, `trht_yn`) 중 하나라도 변하면 `[장운영정보]` 로 원값 기록(:8461).

### 3-2. 이벤트 코드 매핑 (매뉴얼 숫자코드 + 실측 A/B 접두코드 **병행**)
**[260703] 정의·판정은 부작용 없는 순수 모듈 `mkop_events.py` 로 분리**(리플레이/단위테스트용, §7-B). 프로덕션은 `from mkop_events import classify_mkop_event`(`ws_realtime_trading.py:6593`)로 위임. `classify_mkop_event(mkop, antc_mkop)` = 매뉴얼+실측 병행 판정 → `{evt_code, evt_src, event_name, cb_trigger, cb_release, clear_market_event}`.

| 코드 | 이벤트명 | 처리 |
|---|---|---|
| 174 / 184 / 164 | 서킷발동 / 서킷개시 / 시장임시정지 | 시장 전파(신규매수 차단·REST 폴링 생략·전종목 ccnl→exp·30분 자동복귀 예약) |
| **AF8** (실측) | 서킷브레이크발동 | 174/184/164 와 동일하게 **시장 전파** |
| 175 / 185 | 서킷해제 | 시장 전파 해제(REST 폴링 재개·전종목 exp→ccnl) |
| **AF1** (실측) | 서킷브레이크해제 | 175/185 와 동일하게 **시장 전파 해제** |
| 187 / 397 / 388 / 398 | 사이드카(매수/매도 발동·해제) | **정보성 로그 + 텔레만.** 시장 동작 변경 없음 |
| **BF9** (실측) | VI단일가(의미 미확정) | **정보성 로그 + 텔레만.** 시장 전파 없음 |

- **[260703] 병행 감지 도입 배경(중요):** 260626 실측 결과, KIS 운영 WSS 는 **매뉴얼의 숫자 서킷/사이드카 코드(174/187 등)를 실제로는 보내지 않고** 서킷 발동=`AF8` / 해제=`AF1` / VI단일가=`BF9` 로 보낸다. 매뉴얼 숫자코드는 한 번도 수신되지 않았다. 이를 KIS 고객센터에 문의(`docs/KIS문의_H0STMKO0_장운영코드_260626.md`)한 상태이며, 답신으로 확정되기 전까지 **숫자코드와 A/B 접두코드를 함께 매핑**해 어느 쪽이 와도 감지한다.
- 핸들러 분기(`_on_market_status_krx`): 시장 전파(발동) = `_evt["cb_trigger"]`(:8487), 시장 전파 해제 = `_evt["cb_release"]`(:8509), 종목 이벤트 클리어 = `_evt["clear_market_event"]`(:8505). 각 코드집합 정의는 `mkop_events.py`(CB_TRIGGER_CODES/CB_RELEASE_CODES/MARKET_EVENT_CLEAR_CODES).
- 사이드카 한계(미해결): 260626 실측상 **사이드카는 H0STMKO0 로 0건 푸시** → 이 경로로는 원천 감지 불가. 시장 전체 매도/매수 사이드카를 잡으려면 지수/별도 TR 기반 감지가 필요(향후 과제).

### 3-3. 서킷 자동복귀 안전장치
- CB 발동 시 `_cb_resume_at[market] = now + CB_AUTO_RESUME_SEC(1800s)` 예약. 해제코드를 놓쳐도 발동+30분에 강제 복귀(:6247~, :8491). 해제코드가 먼저 오면 즉시 복귀.
- CB 활성 판정: `_is_cb_active(code)` = 해당 시장 `_market_wide_event` 존재 여부(:6581~6583). 신규매수 금지에 사용.

## 4. 텔레그램 발신 정책 — 핵심 검증됨
- 유지(텔레 발송): 서킷브레이커류(§3), 사이드카(§3), `[VI감지-REST]`(§2 경로 B), `[VI매도]` 등.
- 제거(로그만): VI 발생현황 경로 A(260630~, :8543).
- 모든 텔레 메시지는 자체 로그(`out/logs/YYMMDD_telegram.log`)에도 기록되며 시간정보를 포함한다.

## 5. 호가(orderbook) 레코더 — 핵심 검증됨 + 260715 비활성
- **[260715] `ORDERBOOK_ENABLED=False`(:109) 로 전환 → 프로덕션이 레코더를 기동/소비하지 않는다.** 호가 전략 당분간 미사용 + a2 를 다계좌 WSS **구독 연결**로 전용하기 위함(다계좌 구독 리팩터, §0). `_start_orderbook_recorder`(:14055)·`_orderbook_bus_consumer_loop`·`_get_orderbook`(:14110) 모두 이 플래그를 가드하므로 플래그 하나로 경로 전체 비활성.
- (이하 활성 시 동작, 참고 보존) 프로덕션에서 **별도 subprocess** 로 기동(커밋 41d3211, 260702). 생산물: `data/wss_data/orderbook/`. 산출: 일자별 `YYMMDD_orderbook.parquet` + `parts/` 조각 + `run_YYMMDD.log`. 소스 파일: `ws_orderbook_recorder.py`. 시장데이터 공유메모리 버스 소비.

## 6. 스케줄 / 시간대별 모드·구독 구성 — 핵심 검증됨

모드 판정 = `calc_mode(now)` (`ws_realtime_trading.py:4967~4981`), 모드 enum `RunMode`(`:4762~4771`).
시각별 구독 TR = `_desired_subscription_map(now)` (`:13479~13560`), 적용 = `_apply_subscriptions(..., reason="모드전환")` (`:6217~6220`).
`END_TIME = 18:00`(시간외단일가매수 ON) 또는 `16:01` (`:4927`).

| 시각(KST) | 모드 | 구독 TR |
|---|---|---|
| ~08:50 | PREOPEN_WAIT | 없음 (`:13487`) |
| 08:50~09:00 | PREOPEN_EXP | 예상체결 H0STANC0 전종목 (`:13495`). 단 08:59:29+ 30분단일가 종목은 실시간 ccnl (`:13490~13494`) |
| 09:00~15:20 | REGULAR_REAL | 기본 실시간체결 H0STCNT0; VI/단일가/서킷 종목만 예상체결 exp (`:13497~13526`) |
| 15:20~15:21 | CLOSE_EXP | 없음(주문발송만) → 15:21 exp 전종목 (`:13528~13530`) |
| 15:30~16:00 | CLOSE_REAL | 실시간체결 전종목; 전종목 수신완료/15:31 경과 시 해제 (`:13532~13541`) |
| 16:00~END | OVERTIME_EXP/REAL | 시간외 H0STOUP0 계열 (`:13543~13545`) |
| END 이후 | EXIT | 없음 |

**정시 이벤트(scheduler_loop, `:5775~6270`, 시각 가드 if):**
- 08:29:59 체결통보(H0STCNI0) 구독(`:5809~5821`) · 08:58 잔고조회(`:5864`) · 08:59~09:05 prdy>9.8% 종목 H0STMKO0 관찰구독(`:6103~6131`)
- 09:00 prdy_ctrt≥10% 종목 예상체결 유지(`_vi_delay_until=09:02`, `:6224~6242`) → **09:02 전종목 실시간 전환**(`:6243~6252`)
- 15:19:10 사전 구독해제(ccnl 전종목 해제, H0STCNI0/H0STMKO0 유지, `:5904~5922`) · 15:19:28 조건부 일괄매도(`:5893~5898`)
- 15:20 종가매수 발송(`_switch_to_closing_codes`, `:6041~6048`) · 15:21 exp 구독(`:5922~5932`) · 15:29:30/50 취소·재분배(`:5934~5945`)
- 15:30 종가 실시간체결(`:6049~6052`) · 15:30:40 체결알림(`:5947`) · 15:40~16:00 장후 시간외종가 매수(`:5991~5995`) · 15:58 당일 상한가 재선정(`:5984~5989`)
- 16:00~18:00 시간외 단일가 매수/집계(`:6006~6021`) · 18:00(EXIT) 종료·flush·`os._exit(0)`(`:6055~6099`)

주의(미검증): 위 시각 상당수는 상단 상수 블록이 아니라 인라인 `dtime(...)` 리터럴이다(일부만 상수: `CLOSE_STOP_TIME=15:31 :798`, `TOP_RANK_END=15:20 :826`, `UPLIMIT_CLOSE_TIME_HM=(15,10) :133`, `END_TIME :4927`).

### 6-1. WSS 슬롯 배분 (총 `MAX_WSS_SUBSCRIBE=40` :832) — 핵심 검증됨 + 260714 변경
- 총 슬롯 = **H0STCNI0(체결통보) 1 + H0STMKO0(장운영정보) N + 종목데이터(ccnl/exp) M ≤ 40**. rebuild 말미 '구독 상한 준수' 블록이 초과 시 **종목데이터를 축소**해 하드 보장(`:13710~`).
- **H0STMKO0 슬롯 상한 = 동적 `_h0stmko0_slot_cap()`(:8606, 260714):** `가용 = 40 − 1(CNI0) − 현재데이터구독수(len(codes)) − 예약여유(H0STMKO0_DATA_RESERVE=5)`, 하한 `H0STMKO0_MIN_SLOTS=5`. **종전 정적 `H0STMKO0_MAX_SLOTS=5` 폐기.** 데이터 구독이 적을 땐(예 260713 8종목 → 가용 26) 장초 고등락(VI 급등) 종목을 관찰구독에 넉넉히 수용해 경로 A(§2)를 살린다.
- 두 집행 지점 모두 동적 cap 적용: 런타임 추가 `_mkstatus_sub_add`(:8640), open_map rebuild(:13719). 초과 시 우선순위 **보유 > VI > keepalive > 기타**.

## 7. 검증 방법 (백테스트 / 리플레이 / 라이브) — 핵심 검증됨

세 가지가 서로 다르다. **바꾼 코드가 "매매전략"이면 (A), "이벤트 핸들러"(VI/CB/사이드카 등)면 (B)** 로 검증한다.

- **(A) 전략(매매) 백테스트** — `symulation/Query_str_*_simulation.py`. S3 1분봉(`s3://tfttrain/KIS_DB/market_data/1m/date=*/ohlcv.parquet`)을 duckdb 로 조회 + 대상 `Select_Tr_target_list.csv`. 진입/청산 규칙의 과거 손익·승률 계산. (EarlyPop-Trail 검증이 이 산물)
- **(B) 이벤트 핸들러 리플레이 검증 — 구현됨(260703).** 과거 실제 수신 프레임을 판정 로직에 재생.
  - ⚠ 배경: `ws_realtime_trading.py` 는 import 시 `:763~764` 에서 a1 실인증 → 가동 중 프로덕션 approval_key 무효화(즉사) 위험. 그래서 판정 로직을 **부작용 없는 순수 모듈 `mkop_events.py`** 로 분리(§3-2)했고, 테스트는 프로덕션을 import 하지 않고 `mkop_events` 만 import 한다.
  - 실행: `python3 test_mkop_replay.py`. 260626 서킷일 `AF8/AF1/BF9` 원본 7건(`out/logs/wss_TR_260626.log`) 파싱 + 매뉴얼 숫자코드 합성 프레임 + 상태기계(AF8→BF9→AF1) 재생. 260703 기준 **PASS=46 / FAIL=0**.
- **(C) 라이브 검증** — `test_vi_switch_exp_to_real.py`(a2 별도 WSS, 장중 관측). 서킷/사이드카는 드물어 on-demand 재현 불가.

---

## 8. WSS 틱 저장 · 병합 · recv_ts 스킴 — 260706 신설/수정 (핵심 검증됨)

**수신→저장 흐름:** WSS 프레임 1개엔 국내주식 실시간체결(H0STCNT0)이 **최대 20건**까지 묶여 온다(260706 실측, 하드캡). `on_result`가 프레임 도착 시 `recv_ts` 를 **프레임당 1번** 찍고(`:12788`) `_ingest_queue` 에 넣으면, `ingest_loop`(`:12793~`)가 종목별로 partition(`:12860`) → 지표를 **틱마다** 계산(`:13011~13013`, `_calc_indicators`) → part 버퍼에 적재. part 저장은 초과분만 떼어 저장하며 **중복제거를 하지 않는다**(원본 온전). 18:00(또는 16:01) 종료 시 `_merge_parts_to_final`(`:10260~`)이 part 들을 daily `YYMMDD_wss_data.parquet` 로 병합하고 원본 part 는 삭제가 아니라 `out/.../backup/` 으로 이동(`:10355~`).

**recv_ts 유일화(260706 수정):** 프레임당 1번 찍은 recv_ts 가 `:12852`에서 그 프레임 전 행(최대 20행)에 **똑같이 복사**된다. 그래서 종목별로 프레임 내 순번(00~19, acml_vol 오름차순=수신순)을 소수점 뒤 **2자리로 붙여 6자리→8자리로 유일화**한다(`:13006~`, save 경로). 8자리는 문자열 정렬·차트(`Fplt_...:283` `str.slice` 고정위치)·pandas `to_datetime`(나노초 보존) 모두 안전. **주의:** 파이썬 순정 `datetime.strptime(…, "%f")`(6자리 고정)로 파싱하면 에러 → 파싱 필요 시 pandas `to_datetime`(포맷 불필요) 사용.

**recv_ts 유일화·dedup 흐름 — 3사이트, 공용 로직 1개 (헷갈리기 쉬우니 반드시 인지):**

| 사이트 | 파일:위치 | 방식 | 공용모듈 사용 |
|---|---|---|---|
| ① 프로덕션 실시간 ingest (저장 시점, 원천) | `ws_realtime_trading.py:13006~` | Polars 인라인으로 종목별 순번 2자리 붙여 8자리 생성 | ✗ (인라인) |
| ② 프로덕션 자동 병합 (EOD parts→daily) | `ws_realtime_trading.py:10340` | 인라인 `drop_duplicates([code, recv_ts])` — ①이 8자리 유일화했으므로 실체결 삭제 없음(진짜 재병합 중복만) | ✗ (인라인) |
| ③ 오프라인 단독 병합 | `ws_merge_wss_parts.py:160~` | **공용 `wss_recv_ts_util.unique_recv_ts`** | ✓ |
| ④ 오프라인 과거 복구 | `recover_wss_daily_lossless.py` | **공용 `wss_recv_ts_util.unique_recv_ts`** | ✓ |

- **공용 모듈 `wss_recv_ts_util.unique_recv_ts`** (③·④가 공유): 완전 동일 행(진짜 재병합 중복)만 제거하고, 같은 `(code, recv_ts)` 실체결은 삭제 없이 수신순(acml_vol) 순번을 붙여 유일화. **구 6자리 데이터는 순번을 복원해 8자리로, 신규 8자리는 그대로 통과(멱등·자기적응).** 오프라인 경로는 이 한 함수만 고치면 둘 다 동일하게 바뀐다.
- **주의:** ①·②(프로덕션)는 공용 모듈을 쓰지 않고 각자 인라인이다. 실시간 경로는 Polars 네이티브 hot-path라 pandas 유틸을 끼우지 않았다. recv_ts 유일화 규칙을 바꾸려면 **①(생성)과 ③④(공용 모듈) 양쪽**을 함께 봐야 한다(②는 ①에 의존하므로 대개 불변).

**★ 데이터 형식 차이(중요):** **~2026-07-05 의 daily parquet 은 프레임당 1행으로 축약되어 하루 수신의 약 54%가 소실**되어 있다(260706 진단: 301,031행→137,079행). **2026-07-06 부터는 무손실**(recv_ts 8자리). 과거 파일 복구는 backup 조각이 남아 있을 때만 `recover_wss_daily_lossless.py` 로 가능(현재 backup 은 260706 만 존재 → 그 이전은 원본 조각이 정리되어 복구 불가). 따라서 **날짜 간 틱수·거래량·가격경로를 비교할 땐 260706 경계 전후가 다른 정밀도임을 반드시 감안**한다.

**지표 계산 원칙:** 저장 경로에서 지표는 **틱마다** 계산한다(`:13011~`, 20틱≈1ms로 저렴). "마지막 틱만 계산해 프레임 전체에 브로드캐스트"는 **금지** — 프레임 내 가격경로(순간 고점/저점)가 소실되어 트레일링/손절 판정이 틀어진다. 브로드캐스트는 프레임 내내 상수인 저장·표시 컬럼(vi_yn 등, `:13027~`)에만 허용. 다중 체결에 대한 실행 제어는 "틱을 버리는" 게 아니라 **주문을 프레임당 1회로 게이팅**(예: `_uplimit_v5_evaluated`)한다.

---

## 운영/진단 주의 (도구 관련) — 핵심 검증됨
- **프로덕션 로그 파일(`out/logs/wss_TR_*.log`)은 널바이트를 포함해 `file` 이 `data`(이진)로 인식한다.** 따라서 `grep` 이 매칭을 통째로 건너뛰므로, 로그 검색 시 **반드시 `grep -a`** 를 쓴다. (`grep` 만 쓰면 실제로 있는 내용을 "0건"으로 오판함 — 260703 실사고)

---

## 변경 이력 (ws_realtime_trading.py 수정 시마다 추가)

| 날짜 | 변경 | 관련 섹션 | 코드 위치 |
|---|---|---|---|
| 2026-07-16 | **다계좌 WSS 구독 리팩터 S3·S4 (전부 `MULTIACCT_WSS_ENABLED` 플래그 OFF → 프로덕션 동작 불변).** **S4 배정**: `_build_sub_map(now, subset)` 로 구독맵 본문 추출 + `_desired_subscription_map` 은 플래그 OFF 시 전체 codes(기존 동일)·ON 시 `_a1_share()`. `_partition_a1_a2()` = 보유>VI>전일상한가>나머지 우선순위로 a1(≤39 데이터)·a2(오버플로) 분할. **S4a sticky**: `_uplimit_watch_loop`(등락률 +28~30% 주권 30초 조회 → `codes` sticky 편입, EOD까지), watchdog sticky-skip. **S3 H0STMKO0→a2**: a1 rebuild 에서 H0STMKO0 구독/슬롯 제외(플래그 ON), `_a2_desired_subscription` 에 `market_status_krx` 합류, `_mkstatus_sub_add/remove` 가 a2 연결(`_active_kws_a2`)로 라우팅. a2 데이터 동적 재구독 `_apply_a2_data_subs`(재연결 없이, BB 연속성). **미검증(라이브)**: 실제 2연결 동시 수신·H0STMKO0 a2 수신은 장중 관측 필요 → 검증 후 플래그 ON. | §0·§2·§6-1 | `_build_sub_map`·`_partition_a1_a2`·`_uplimit_watch_loop`·`_a2_desired_subscription`·`_apply_a2_data_subs` |
| 2026-07-16 | **a2 1006 근본원인 해결 + 다계좌 정상 가동(flags ON).** 원인: `sys.modules["kis_auth"]=글로벌(a1)` 별칭 → 요청함수 data_fetch 가 a2 구독 메시지를 **a1 approval_key** 로 생성 → 불일치 bare 1006. 수정: domestic_stock_functions_ws 를 a2 바인딩 사본(`_dsfw_a2`)으로 로드, `_a2_req` 로 치환. **디커플**: `_a2_active()` — a2 실연결 시에만 H0STMKO0·데이터 a2 배정, 미연결 시 a1 폴백(VI 보호). `_setup_ka2` auth 20s 타임아웃 가드. 실측(12:27~) a2 H0STMKO0 연결 3.5분+ 무중단·1006 0건. (in-process 초기 미연결은 a2 appkey 일시 ALREADY-IN-USE 였고 해소됨.) | §0·§2 | `_a2_req`·`_a2_active`·`_setup_ka2`·`_dsfw_a2` |
| 2026-07-15 | **다계좌 WSS 구독 리팩터 S1·S2.** **S1**: `ORDERBOOK_ENABLED=True→False`(:109) — 호가 전략 미사용 + a2 를 구독 연결로 전용(레코더 기동 제거). **S2**: a2 격리 연결 인프라 추가 — `MULTIACCT_WSS_ENABLED`(플래그, 현재 **OFF**), `_import_ka_copy`(importlib 사본)·`_setup_ka2`(a2 자격 인증)·`run_ws_forever_a2`(경량 연결 루프, on_result 공유). **격리 검증 완료**(open_map/_cfg/approval헤더/클래스 사본별 독립). 플래그 OFF 라 프로덕션 동작 불변. 미결: **S4** 구독 총괄관리자(전일상한가 ∪ 당일28%↑ sticky, a1우선→a2오버플로), **S3** H0STMKO0→a2 이관(최고위험, VI경로 재배선). 설계: `docs/multiacct_wss_subscribe_plan_260715.md`. | §5·§0 | S1 `:109` / S2 `MULTIACCT_WSS_ENABLED`·`_import_ka_copy`·`run_ws_forever_a2` |
| 2026-07-14 | **개장 직후(09:00~09:10) VI 예상체결가 미수신 사건 개선(2건).** ① **H0STMKO0 슬롯 cap 정적 5 → 동적**(`_h0stmko0_slot_cap()`): `가용 = MAX_WSS_SUBSCRIBE−1(CNI0)−현재데이터구독수−예약여유(5)`, 하한 `H0STMKO0_MIN_SLOTS=5`. 데이터 구독이 적을 때(예 260713 8종목) 장초 고등락 종목이 관찰구독에서 탈락하던 문제 해결 → `VI_SWITCH_BY_VICLS` 주력 조기전환 경로 복원. ② **REST "갑자기 끊김→VI 의심" 감지의 09:10 게이트 → 09:00 확장**: 감지 블록을 `_price_watchdog_loop` 섹션(C)에서 09:00 가드 직후로 이동, 섹션(A) early-return 과 무관하게 09:00~15:30 상시 동작. 09:00~09:02 예상체결연장(`_vi_delayed_codes`) 종목은 제외. (배경·근거: `ws_monitoring_research_260713.md`) | §2·§6 | `:8606`(`_h0stmko0_slot_cap`), `:8640`, `:8655`, `:13719~`, watchdog `:8864~`(신규 블록)·`:9004~`(C 중복 제거) |
| 2026-07-06 | **병합 중복제거 데이터 소실 버그 수정.** 한 WSS 프레임(최대 20체결)에 복사된 동일 recv_ts 를 종목별 프레임 내 순번 2자리로 **8자리 유일화** → 병합 `drop_duplicates([code,recv_ts])` 가 실체결을 삭제하던 문제 해결(하루 ~54% 소실→무손실). 공용 모듈 `wss_recv_ts_util.py` 신설, 단독 병합 `ws_merge_wss_parts.py` 동일 적용, 과거 복구 `recover_wss_daily_lossless.py` 신설. **~260705 daily 는 축약본, 260706+ 는 무손실(형식 차이 존재).** | §8 | `:13006~13020`, `:10340`, `wss_recv_ts_util.py`, `ws_merge_wss_parts.py:160`, `recover_wss_daily_lossless.py` |
| 2026-07-03 | 장운영 이벤트 **AF8/AF1/BF9 병행 감지** 추가. AF8=서킷발동(시장전파), AF1=서킷해제(시장전파 해제), BF9=VI단일가(정보성). 매뉴얼 숫자코드(174/187 등) 실측 미수신 확인에 따른 보완. | §3-2 | `:6590~6606`, `:8493`, `:8511`, `:8515` |
| 2026-07-03 | 로직 파일 신설 + 확장(별칭 정의, §0 계좌·연결 구조, §1.1 아침 구독대상 결정, §6 스케줄/모드, §7 검증방법). 코드 변경 아님(로직 정리). | §0·§1.1·§6·§7 | (문서) |
| 2026-07-03 | 장운영코드 정의·판정을 **부작용 없는 `mkop_events.py`** 로 분리 → 프로덕션은 `classify_mkop_event` 위임. 리플레이 검증 `test_mkop_replay.py` 신설(260626 실프레임+합성+상태기계, **PASS=46/FAIL=0**). | §3-2·§7 | `:6593`, `:8461`, `mkop_events.py`, `test_mkop_replay.py` |
