# 다계좌(a1/a2/a3…) WSS 구독 총괄관리 — 수정 방향(설계안)

작성: 2026-07-15 · 브랜치 `feature/multiacct-wss-subscribe` · 백업 커밋 `7f45494`
상태: **컨펌 대기 (아직 프로덕션 코드 미수정)**

---

## 1. 목적 / 배경

- **왜 필요한가:** 당일 상한가 도달 종목(하루 20~30건)을 눌림목 매수 전략의 대상으로 삼으려면 **그 종목들을 모두 실시간 구독**해야 한다. 이유 두 가지:
  1. **지표 연속성** — MA10·볼린저밴드는 연속 틱 이력이 있어야 유지된다(도달 순간부터 끝까지 물고 있어야 함).
  2. **백데이터 확보** — 상한가 종목의 틱을 저장해 이후 백테스트 소스로 쓴다.
- **문제:** 전일 상한가 종목 + 당일 상한가 종목을 합치면 **단일 WSS 연결의 41슬롯(코드상 40) 한도를 초과**한다.
- **요구:** **하나의 프로세스** 안에서 a1/a2/a3… 다계좌의 WSS 연결을 **총괄 관리**하여, 구독을 계좌 연결들에 분산해 총 슬롯을 `40 × 계좌수`로 확장한다.

> 참고: 이 문서가 다루는 것은 **①시세 구독(WSS)** 확장뿐이다. **②주문 실행(REST, 계좌별 동시 매매)** 은 이미 구현되어 있고(`_iter_enabled_accounts` :6773, 계좌별 매수 :4189~4238) 이 리팩터와 **독립**이다. 구독을 어느 계좌 연결로 받든, 주문은 기존대로 각 계좌가 자기 잔고로 낸다.

---

## 2. 현재 구조 (코드로 검증된 사실 · `파일:라인`)

### 2.1 `kis_auth_llm.py` = 모듈 전역 기반 "1프로세스 = 1연결"
- 전역 상태: `open_map`(:748, 구독맵), `data_map`(:770, TR 스키마), `_base_headers_ws["approval_key"]`(:632, 현재 승인키), `_cfg`(appkey/secret 원천), `_appkey`.
- **슬롯 상한 검사:** `if len(open_map.keys()) > 40: raise ValueError("Subscription's max is 40")` (:1006) — **전역 open_map 기준**.
- **연결 클래스** `class KISWebSocket`(:834): `on_result` 콜백은 **인스턴스 속성**(:836 선언, :1120 설정, :981 호출). 그러나 `subscribe()`(:1096)는 **전역 `open_map` 에 기록**(`add_open_map`), 실제 프로덕션은 클래스에 직접 호출(`ka.KISWebSocket.subscribe(...)` :13698/:13781/:13786). 연결 구동은 `__runner`(:997)/`__subscriber`(:856)가 **전역 open_map 을 소비**(:1019).
- **계좌 선택 = `_cfg["my_app"]/["my_sec"]`**(:588). `auth_ws()`가 이 값으로 승인키 발급/재사용.
- **승인키 캐시는 이미 appkey별**(`_approval_tmp_path(appkey)` :160, `save/read_approval_key(..., appkey)` :172/:184) → **계좌 간 키 파일 충돌 없음**.

### 2.2 계좌 전환의 검증된 패턴 (orderbook 레코더)
- `ws_orderbook_recorder.py:_setup_a2_auth`: **`ka._cfg["my_app"]=a2 appkey` 로 덮어쓴 뒤 `ka.auth_ws()`** → a2 승인키를 a1 과 독립으로 발급. **별도 프로세스**라 전역이 안 엉킴.
- 즉 "계좌 전환"은 이미 실전에서 쓰는 패턴이다. 다만 **한 프로세스에 두 연결**을 띄우면 전역(open_map/approval_key)이 엉켜 서로 구독을 침범 — 과거 a1/a2 이중연결 실패의 근본원인(`ws_orderbook_recorder.py:23~24`, 로직 §0).

### 2.3 프로덕션(a1) 구독 경로
- import 시 a1 인증: `ws_realtime_trading.py:763~764`(`ka.auth`/`ka.auth_ws`).
- 구독 재구성 `_apply_subscriptions()`(:13449): `ka.open_map.clear()`(:13685) → `ka.KISWebSocket.subscribe(req, codes)`(:13698 등) → `kws = ka.KISWebSocket(...)`(:13805) 로 단일 연결 구동. 활성 인스턴스 `_active_kws`.
- 슬롯 배분(로직 §6-1): `MAX_WSS_SUBSCRIBE=40`(:832). 총 = `H0STCNI0(1) + H0STMKO0(N) + 종목데이터(M) ≤ 40`. H0STMKO0 동적 cap `_h0stmko0_slot_cap()`(:8606).

---

## 3. 설계 방향

### 3.1 채택안 — **격리된 모듈 사본 방식** (계좌당 kis_auth_llm 사본 1개)

`importlib` 로 `kis_auth_llm.py` 를 **계좌 수만큼 독립 로드**한다. 각 사본은 자기만의 `open_map/data_map/_base_headers_ws/_cfg` 를 가지므로 전역이 **완전 격리**된다(과거 실패의 근본원인 = 공유 전역, 이 방식은 애초에 공유가 없음).

```
ka_a1 = 기존 import kis_auth_llm  (프로덕션 기본 연결 = a1, 그대로 유지)
ka_a2 = _load_ka_instance("kis_auth_llm__a2")   # 독립 전역
ka_a3 = _load_ka_instance("kis_auth_llm__a3")   # (a3 appkey 준비 시)
```
- 각 사본에 `ka_aX._cfg["my_app"]=계좌appkey` 설정 후 `ka_aX.auth()/auth_ws()` — **orderbook 레코더 패턴을 한 프로세스 안에서 복제**.
- 각 사본의 `KISWebSocket` 을 **자기 스레드 + 자기 asyncio 루프**로 구동(현재 단일 연결 스레드 구동 :13805~ 을 계좌별로 복제).
- 모든 사본의 `on_result` 를 **동일한 중앙 처리 파이프라인**(기존 `_ingest_queue`/지표계산/전략)으로 연결 → 어느 연결로 받은 틱이든 한 곳에서 처리.

**채택 이유:** ① `kis_auth_llm` 내부(취약한 심장부)를 **거의 안 고침** → 리스크 최소. ② 이미 검증된 계좌전환 패턴 재사용. ③ 승인키 캐시가 이미 appkey별이라 충돌 없음.

### 3.2 대안 (참고, 미채택)
- **B. `kis_auth_llm` 인스턴스화 리팩터** — `open_map/data_map/approval_key` 를 인스턴스 속성으로 이동. 장기적으로 깔끔하나 **심장부 대수술**이라 고위험(과거 실패가 이 전역 공유 탓). → 미채택.
- **C. 다중 프로세스**(a2를 별도 프로세스로, market_bus IPC) — 사용자가 "한 프로세스" 를 명시 → 미채택. 단 최후 폴백으로 유효(레코더가 이미 이 형태).

---

## 4. 새 컴포넌트 — 구독 총괄 관리자(SubscriptionManager)

한 곳에서 **"어느 종목을 어느 계좌 연결에 실을지"** 를 결정·집행한다.

- **입력(구독 대상):** 전일 상한가(기존) + **당일 상한가 도달 종목**(신규, 도달 순간 추가).
- **연결별 예산:** 각 계좌 연결마다 `H0STCNI0(1, 해당 계좌 체결통보) + H0STMKO0(동적) + 종목데이터 ≤ 40`.
- **배정 규칙(초안, 컨펌 대상):**
  1. a1 연결부터 채우고, 40 근접 시 a2, 그다음 a3… **오버플로 순차 배정**.
  2. **한 종목은 정확히 한 연결에만** 구독(중복 구독 = 중복 틱 → 금지). 배정표 유지.
  3. 보유/VI/전일상한가 = 고정 우선(가급적 a1), 신규 당일상한가 = 잔여 슬롯에 배정.
  4. 청산·구독해제 시 슬롯 반납 → 이후 종목이 회수분에 배정.
- **집행:** 계좌 연결별로 `open_map` 을 각각 rebuild(현재 단일 `_apply_subscriptions` 를 **연결 파라미터화**).
- **저장/지표:** 모든 연결의 틱이 기존 저장 스킴(§8, recv_ts 8자리 유일화)·지표계산으로 합류. 저장 parquet 는 종목 단위라 연결 구분 불필요(단, 어느 계좌로 받았는지 진단 컬럼은 선택).

---

## 5. 상세 변경 목록(파일별, 예정)

| 파일 | 변경 | 비고 |
|---|---|---|
| `kis_auth_llm.py` | **거의 불변.** 필요 시 `_cfg`/전역을 함수 인자로 주입 가능하게 소폭 보강(하위호환 유지). 40-cap(:1006)은 연결당 유지. | 심장부 최소 손 |
| `ws_realtime_trading.py` | ① 계좌별 `ka` 사본 로더 `_load_ka_instance()` 신설. ② `_apply_subscriptions()`(:13449)를 **연결(사본) 파라미터화** — `ka.open_map`/`ka.KISWebSocket` 직접참조를 연결객체 경유로. ③ SubscriptionManager 신설(배정·집행). ④ 계좌 연결별 스레드 구동(:13805 복제). ⑤ 당일 상한가 도달 종목 구독 추가 훅. | 가장 큰 작업 |
| `ws_realtime_tr_str1/2.py` | 매매 로직은 계좌-무관이라 대개 불변. 구독소스 참조가 있으면 조정. | 소폭/불변 |
| `kis_utils.py` | 계좌 목록/설정 헬퍼 재사용. 대개 불변(사용자 지시로 백업엔 포함). | 불변 예상 |
| `config.json` | a3 이상 확장 시 `users.*.accounts.aN` 추가. 현재 a1/a2만 존재 → **당장 2연결(최대 80슬롯)**. | 비밀정보, git 제외 |
| 로직 파일 `rules/ws_realtime_trading_logic.md` | §0·§6-1 갱신 + 변경이력 추가(수정과 동시). | 규칙상 필수 |

---

## 6. 주의 / 리스크

1. **approval_key 무효화:** a1/a2 는 appkey가 달라 상호 무효화 없음(검증). **같은 appkey 로 두 연결 금지**(그건 무효화 유발). 사본은 반드시 서로 다른 계좌.
2. **import 시 실인증 부작용:** `ws_realtime_trading.py:763~764` 는 import 만 해도 a1 승인키를 발급 → 가동 중 프로덕션 즉사 위험(로직 §0/§7). 사본 로더도 **auth 시 같은 위험** — 오프라인 검증은 프로덕션 미기동 시간대에만.
3. **H0STCNI0(체결통보)는 계좌별:** a2가 실제로 매매하면 a2 체결통보는 a2 연결에서 받아야 한다. 구독 배정과 별개로 **각 연결은 자기 계좌 CNI0** 를 갖는다(슬롯 1 소비).
4. **스레드/asyncio 다중 루프:** 연결당 스레드+루프. 종료/재접속·flush 시 N개 정리 필요(현재 단일 종료 :6055~ 를 다연결로 확장).
5. **중복 구독 절대 금지:** 한 종목 두 연결 = 틱 2배·저장 오염. 배정표로 하드 차단.
6. **틱 병합 부하:** 구독 종목이 2배가 되면 저장/지표 부하도 증가 — 무손실 스킴(§8) 유지 확인 필요.

---

## 7. 검증 계획

- **(단위/오프라인)** 사본 로더가 실제로 격리된 전역을 갖는지: `ka_a2.open_map is not ka_a1.open_map`, 서로 다른 approval_key. **장 시간 외**에만(인증 부작용 때문).
- **(라이브 관측)** `test_vi_switch_exp_to_real.py` 계열로 a2 2번째 연결이 a1 과 독립 수신하는지, 중복 틱 없는지.
- **(회귀)** 기존 단일 계좌(a1만)일 때 동작 불변 — 계좌 1개면 기존과 동일 경로.
- **(전략 무영향)** 매매 백테스트는 이 변경과 무관(구독≠매매).

## 8. 롤백
- 백업: `backup/prod_pre_multiacct_260715/`(커밋 `7f45494`). 파일 단위 복원 가능.
- 작업 브랜치 `feature/multiacct-wss-subscribe` 에서만 진행 → main 무영향. 문제 시 브랜치 폐기.

---

## 9. 확정 사항 (2026-07-15 사용자 컨펌 + 2차 지시)

1. **a2 = 구독 전용, 매매는 a1만.** `a2.trade_enabled=False` 유지. a2 연결은 시세만, 체결통보(H0STCNI0)·주문·자본분리 없음.
2. **범위 = 우선 2연결(a1+a2), 최대 80슬롯.** 코드는 N계좌 일반화, 실검증은 2연결.
3. **구독 ≠ 매매(재확인).** 어느 연결로 구독하든 주문은 기존 REST 계좌별.

### 9-1. 2차 지시 (2026-07-15)
4. **a2 orderbook 수신 제거.** 호가(orderbook) 전략은 당분간 미사용 → 프로덕션이 `ws_orderbook_recorder.py` 를 **기동/정리하지 않도록** 제거(`_start_orderbook_recorder` 호출 :14285, `_stop_orderbook_recorder` 호출 :13280, 플래그 :103). a2 가 orderbook 에서 해방되어 신규 구독 연결로 전용.
5. **장운영정보(H0STMKO0)는 a2 연결로만.** a1 루프에서 H0STMKO0 (재)구독 블록 제거(:13706~13747, :13784~13787), a2 경량 루프로 이관. VI 감지 경로 A(§2)·서킷 판별의 H0STMKO0 수신을 a2 on_result 가 **동일 프로세스의 기존 `_on_market_status_krx` 큐**(:6507~)로 라우팅.
6. **top30 28%↑ 종목 구독 추가(상한가 도달 관리).** 개선된 `fetch_top30_each_1m.py`(등락률 +28~30%, 주권 ST) 결과를 프로덕션이 주기적으로 읽어 **구독 유니버스에 편입**. 목적: 상한가 도달 여부를 실시간 추적 + 눌림목 진입 후보 확보.
7. **구독 sticky(당일 유지).** 한 번 구독에 들어온 종목은 28% 아래로 되돌아가도 **당일 장 종료까지 계속 구독**(중도 해제 금지). EOD 에 일괄 정리.

### 9-2. 배정 규칙 확정본 (2연결)
- **a1 연결** = `H0STCNI0(1, a1 체결통보) + 종목데이터(ccnl/exp)`. 데이터 예산 ≈ 39. 보유·VI·전일상한가 우선.
- **a2 연결** = `H0STMKO0(장운영정보 N) + 종목데이터 오버플로`. CNI0·orderbook 없음. 40 예산.
- 한 종목 종목데이터는 **정확히 한 연결에만**(중복 구독 금지). a1 우선 → 초과분 a2.
- 구독 유니버스 = 전일상한가(기존) ∪ 당일 28%↑(top30) — **sticky, EOD 해제**.

### 9-3. 구현 단계 (전부 `MULTIACCT_WSS_ENABLED` 플래그 OFF 상태로 커밋 — 프로덕션 동작 불변)
- **S1. orderbook 제거**(지시 4) — ✅ 완료(`ORDERBOOK_ENABLED=False`).
- **S2. a2 격리 모듈 사본 로더 + a2 경량 연결 루프** — ✅ 완료(`_import_ka_copy`/`_setup_ka2`/`run_ws_forever_a2`, 격리 검증).
- **S4. 구독 총괄관리자**(유니버스·sticky·a1/a2 배정, 지시 6·7) — ✅ 완료(`_build_sub_map`/`_partition_a1_a2`/`_uplimit_watch_loop`/`_apply_a2_data_subs`).
- **S3. H0STMKO0 를 a1→a2 이관**(지시 5) — ✅ 완료(a1 rebuild 제외 + `_a2_desired_subscription` 합류 + `_mkstatus_sub_add/remove` a2 라우팅).
- **S5(남음). 라이브 검증 후 플래그 ON.** 장중 2연결 동시 수신·H0STMKO0 a2 수신·sticky 편입 관측 → 이상 없으면 `MULTIACCT_WSS_ENABLED=True`, `UPLIMIT_WATCH_ENABLED=True`. **검증 전에는 플래그 OFF 유지(프로덕션 무영향).**

### 9-4. 활성화 방법(라이브 검증 후)
1. 장 시작 전(프로덕션 미가동 시간) `MULTIACCT_WSS_ENABLED=True` + `UPLIMIT_WATCH_ENABLED=True` 로 변경.
2. 재기동 후 로그에서 `[a2-wss] a2 격리 연결 인증 완료`, `[상한가감시] 신규 N종목 편입`, H0STMKO0 a2 수신, VI 감지 정상 확인.
3. 이상 시 즉시 플래그 OFF 복귀(단일 a1 동작으로 안전 회귀).

### 9-5. 260716 라이브 검증 결과 (중요)
- **S1(orderbook OFF)·S4a(28%↑ sticky)·a1 리팩터: 정상.** 재시작 후 a1 단일 경로 무결(1006 0건), 28%↑ 신규 편입 실측(8~14종목).
- **a2 1006 근본원인 규명·수정(격리 검증 완료):** `ws_realtime_trading.py:708 sys.modules["kis_auth"]=글로벌(a1)` 별칭 → `domestic_stock_functions_ws` 요청함수의 `data_fetch` 가 항상 **a1 approval_key** 로 메시지 생성 → a2 연결(핸드셰이크=a2 키)과 불일치 → **bare 1006 storm**. 수정: 요청함수를 a2 사본(`_dsfw_a2`)에 바인딩(`_a2_req`). 격리 재현 테스트 = 48건 수신·1006 0건 ✅ (커밋 fd529ed).
- **미해결(다음 안전창):** ① **in-process 에서 a2 스레드가 연결 안 됨**(로그 전무·approval 파일 미갱신 — 격리는 성공하나 전체 프로세스에선 `_setup_ka2` 가 조용히 멈춤/실패). ② 09:27 런에서 **`[a2-wss][system] ALREADY IN USE appkey`**(OPSP8996) 관측 — a2 appkey 사용중 충돌(자연해제 ~10h). ③ 플래그 ON 이 H0STMKO0→a2 이관까지 묶여 있어, a2 미연결 시 **VI/서킷 감지가 꺼짐**.
- **설계 보완 방향:** 플래그 **디커플**(28% sticky/a1 = 독립, a2 오버플로·H0STMKO0 이관 = a2 연결 확인된 뒤에만) + a2 스레드 in-process 미연결 원인 규명(스레드 예외 표면화·`_setup_ka2` 단계 로깅 강화) + a2 appkey ALREADY-IN-USE 해소 후 재검증.
- **현재 상태: 플래그 전부 OFF, 프로덕션 안정(단일 a1).**

### 9-6. 260716 12:27 — 다계좌 정상 가동 확인 (해결)
- **①in-process a2 미연결 원인 = a2 appkey 의 일시적 ALREADY-IN-USE(OPSP8996)** (앞선 1006 폭주 잔재). 코드 버그 아님. 해소 후 재시작하니 `_setup_ka2` step1~4 ~100ms 정상 완료, a2 연결 유지.
- **실측(12:27~ flags ON):** a2 kws.start(H0STMKO0 2건) **106초+ 무중단·1006 0건**, a1 정상(32종목 데이터), 28%↑ sticky 15종목 편입. **dsfw a2 바인딩 수정으로 a2 가 자기 approval_key 로 구독 성공**(불일치 1006 소멸).
- **②디커플 적용:** `_a2_active()` — a2 실연결 시에만 H0STMKO0·데이터 배정을 a2 로. a2 미연결이면 전부 a1 폴백(VI/서킷 안 꺼짐). a2 끊김 시 `_active_kws_a2=None`+rebuild 로 a1 재확보.
- **로버스트:** `_setup_ka2` auth 20s 타임아웃 가드(POST 무timeout hang 방지) + 단계 로깅([a2-diag]).
- **현재: flags ON 정상 가동.** 남은 실검증(장중 이벤트 필요): VI 이벤트의 a2 경로 A 감지, 유니버스>39 시 a2 데이터 오버플로.

### ⚠ 미해결/후속 검증 포인트
- **a2 재접속 시 H0STMKO0 재구독**: 현재 a2 데이터는 `_apply_a2_data_subs` 로 무재연결 동적 갱신하나, H0STMKO0 는 `_mkstatus_sub_add`(라이브) + a2 reconnect 시 `_a2_desired_subscription` 재구독에 의존. a2 가 자주 끊기면 H0STMKO0 재구독 지연 가능 → 장중 관측 필요.
- **a1/a2 배정 경계**: 보유·전일상한가가 a1 39슬롯을 이미 채운 극단 상황의 오버플로 동작 확인.
- **중복구독 방지**: `_partition_a1_a2` 가 한 종목을 정확히 한쪽에만 넣는지(교집합 0) 라이브 확인.
