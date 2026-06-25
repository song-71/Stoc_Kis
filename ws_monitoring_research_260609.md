# ws_monitoring_research_260609 — 재접속 폭주 / 서킷브레이커 미감지 / VI 발동시각 분석

_작성: 2026-06-09 (Claude 분석). 대상: `ws_realtime_trading.py` (프로덕션 a1 main)_

---

## 0. 한 줄 요약 (확정)

오늘(6/9) 정오에 **프로덕션이 완전 정지**했다. **진짜 시발점은 한산장도 부하도 아니라, H0STMKO0(장운영정보) 처리가 이벤트 루프 스레드에서 인라인 실행되며 그 안의 VI 구독전환 `send_request` 가 같은 이벤트 루프를 self-deadlock 시켜(send 당 10초 타임아웃) 수신이 10~30초씩 멎은 것**이다. 이 freeze 가 무수신 워치독의 재구독·강제재접속을 촉발 → approval_key 자기무효화 핸드셰이크 실패 → 자기보호중단(exit=2) 반복 → runner 재시작 한도 초과로 11:59 종료. 더불어 서킷브레이커 감지·전환 설계가 골격만 있고 **배선이 끊긴(orphaned) 미완성** 상태임을 확인했다.

> ※ 분석 정정 이력: 초기에 "한산장 무체결 오탐"(틀림) → "고부하 back-pressure"(틀림) 으로 오판했다. 실측 검증 결과 **이벤트 루프 self-deadlock** 이 확정 원인. 근거: 정지 구간 09:15:31/41/51 에 정확히 10초 간격 "구독 실패 err=(빈값)" 3건 = `send_request(timeout=10.0)` 타임아웃 × 3 = 30초 = "30초 미수신" 과 일치.

---

## 1. 재접속 폭주 → 프로덕션 정지 (CRITICAL)

### 타임라인 (6/9)
| 시각 | 사건 |
|---|---|
| 09:01 | 장초반 정상 폭주 359/초 (피크 883/초) — WSS·파이프라인 정상 |
| 10:22:10 | **첫 자기보호중단(exit=2)** |
| 10:23~ | `10초/20초 데이터 미수신 → 재구독` 연속 (총 **245회**) |
| 11:48~ | `handshake_fail_count` 1→2→3→4 사이클 반복 (dwell≈1.02초 즉사) |
| ~5분 주기 | 자기보호중단 → runner 재시작, **21회 반복(42 로그라인)** |
| 11:59:30 | runner `restart limit exceeded` → `runner end` |
| 이후 | **트레이딩 프로세스 다운** (watchdog 만 생존). 장 후반·시간외 전체 손실 |

### 원인 사슬 (확정 — 두 문제의 연쇄)

**문제 1 (만성 도화선) — H0STMKO0 이벤트 루프 self-deadlock**
1. `on_result`(12402, 이벤트 루프 스레드)가 H0STMKO0 만 큐 거치지 않고 **루프에서 인라인 처리**(`12492: _on_market_status_krx`). 일반 데이터는 `_ingest_queue`(워커)로 가는 것과 대조.
2. 처리 중 VI 발동/해제 → `_vi_exp_sub_switch/_restore`(6844/6864, `_kws_lock`) → `_send_subscribe` → `send_request`(`kis_auth_llm.py:1119-1131`, `run_coroutine_threadsafe(coro, self._loop).result(timeout=10.0)`).
3. 호출 스레드가 곧 이벤트 루프 → 루프가 `fut.result(timeout=10)` 로 자기를 막아 send 코루틴 실행 불가 → **send 당 10초 자기교착.** VI해제 1건 = exp해제+ccnl추가+H0STMKO0해제 = **30초 수신정지**.
4. 물증: 09:15:31/41/51 정확히 10초 간격 "구독 실패 err=(빈값)" 3건 = `send_request` timeout × 3. 빈도: 6/8 34건·6/9 14건(매일 routine).
5. 이 30초 정지 → 무수신 워치독 "30초 미수신 → 재구독" 발동 → 재구독·강제재접속.

**문제 2 (치명 증폭) — KIS 연결(IP)레벨 핸드셰이크 throttle**
6. 문제1발 재접속 폭주가 단시간 다발 연결시도 → KIS 가 **WebSocket 핸드셰이크(HTTP 업그레이드)를 거부**: `InvalidMessage: did not receive a valid HTTP response`. 죽음의나선 동안 **85건 전부 이 에러**, dwell≈1.01초(=SDK 가 핸드셰이크 1회 실패 후 `await asyncio.sleep(1)` 하고 반환 → 연결 자체가 성립 못 함).
7. **이 사망은 approval_key 와 무관** — SDK 연결 URL(`kis_auth_llm.py:1016 url=env.my_url_ws+api_url`)에 approval_key 없음. KIS 는 approval_key 를 구독 메시지 본문에 실음. 핸드셰이크는 IP/연결레벨에서 거부됨.
8. 그런데 코드는 dwell<15s 를 **"invalid approval_key 의심"으로 오진단**(13568) → 매 재접속 `auth_ws(force_new=True)` 강제 재발급(wss_TR 로그상 매번 "강제 재발급 완료" 실행됨) → **새 키를 받아도 다음 연결이 1초 만에 또 InvalidMessage.** 즉 재발급은 무효(+ 발급 endpoint 스팸으로 throttle 악화 우려).
9. 90초 backoff + 4회 실패 시 자기보호중단(exit=2) → runner 재시작(~5분 주기) → 즉시 재접속 → **여전히 throttle 창 안이라 또 거부** → 21회 반복 → 11:59 runner 재시작 한도 초과 → 완전정지.
- OPSP8996(슬롯잠금) **아님**(0건). approval_key 무효화도 핵심 아님(핸드셰이크 단계라 키 무관).

### 설계 결함 (확정)
1. **H0STMKO0 처리가 이벤트 루프에서 인라인** → 그 안의 동기 `send_request` 가 루프 self-deadlock. (문제1 근본)
2. **dwell<15s 를 approval 문제로 오진단** → 실제는 연결레벨 throttle 인데 무관한 approval 재발급으로 대응. (문제2 오진단)
3. **throttle 회복 backoff(90초)·재시작주기(~5분)가 너무 짧음** → throttle 창을 못 벗어나고 계속 두드림.

### 적용 수정 (260609)
- **[적용] 문제1 해소**: H0STMKO0 처리를 전용 워커 스레드(`_mkstatus_recv_loop` + `_mkstatus_recv_queue`)로 이관 → `send_request` 가 이벤트 루프와 분리되어 self-deadlock·10초 타임아웃 소멸. (`on_result` 는 큐에 put 만)
- **[미적용] 문제2**: §5 후속과제 — (a) InvalidMessage 핸드셰이크 실패에는 approval 재발급 금지(연결레벨이라 무관), force_new 은 실제 "invalid approval" 시스템메시지에만. (b) 핸드셰이크 throttle 회복 backoff 를 분 단위 점증으로. (c) 자기보호중단 후 재시작이 즉시 재접속하지 않도록 throttle-aware 쿨다운.

---

## 2. 서킷브레이커 감지·전환 — 설계는 있으나 배선 끊김

### 살아있는 부분 ✓
- 시장별 keepalive H0STMKO0 상시 구독: `_MKT_KEEPALIVE_INITIAL{KOSPI:삼성전자005930, KOSDAQ:카카오게임즈293490}` → `13362`/`13393` 에서 매 연결 구독. 6/8 삼성전자 VI 가 이 채널로 감지됨 → 채널 생존.

### 끊긴 부분 (orphaned)
- `_CB_ACTIVE_EVENTS`(6385) → **읽는 코드 0건**.
- `_last_cb_replay_ts`(6368, "CB 중 가격 복제") → **읽는 코드 0건**.
- `_market_wide_event` 소비처는 (a) REST 폴링 생략(8260), (b) parquet `market_event` 컬럼(12739) 뿐 → **전 종목 예상체결 전환 코드 없음**.
- `ws_a2_subprocess.py`(a2 전담 장운영/예상체결) → main 에 `run_a2_subprocess`/`data_queue`/`cmd_queue`/`mp.Process` 배선 **0건** = 미통합 orphan. 실제로는 **a1 main WSS 가 H0STMKO0 직접 수신**(`_send_subscribe(_active_kws, market_status_krx,…)` 8360).

### 감지 필드 문제 (유력)
- 핸들러는 `mkop_cls_code`(8232)만 봄. 그러나 H0STMKO0 스키마상 `mkop_cls_code`(현재 장운영)와 `antc_mkop_cls_code`(예상 장운영)는 별개 필드(`domestic_stock_functions_ws.py:1143-1144`).
- 6/8 실측 진단라인: `mkop='None' antc_mkop='311'`(×4), `mkop='None' antc_mkop='112'`(×1) → **현재필드는 비고 3자리 상태는 예상필드에 실려 옴**. CB/단일가 상태가 antc_mkop 로만 오면 기존 감지는 놓침.

### 로깅 공백 (근본 원인)
- 구독/해제는 로깅(`[H0STMKO0] 구독 추가/해제`), 이벤트 전이도 로깅(`[장운영]`) — 단 `_MKOP_EVENT_NAMES` 매칭 변화일 때만.
- **일반 장운영정보 수신(원시 mkop/antc_mkop 값)은 로그·parquet 모두 미기록**(12491 "parquet 미포함"). → "6/8 CB 때 어떤 코드가 실제로 왔는지" 확인 불가. 이것이 §2 감지 필드 확정을 막는 원인.

### 적용 수정 (260609)
1. **장운영정보 수신 로깅**: `mkop_cls_code`·`antc_mkop_cls_code`·`trht_yn` 세 값이 하나라도 바뀌면 원값 모두 로깅(`[장운영정보]`).
2. **감지 필드 보강**: mkop 우선, 비었으면 antc_mkop 으로도 감지. 감지 경로(`_evt_src`)를 로그에 명시.
3. **전 종목 예상체결 전환 배선**: CB 시장 종목을 `_desired_subscription_map` 에서 exp 로 매핑 + 발동/해제 시 `_trigger_ws_rebuild()` 즉시 적용. 해제(175/185) 시 자동 ccnl 복귀.
4. **워치독 CB 억제**(§1).

### 검증 대기
실제 CB 코드값(174/184 vs antc 의 다른 값)·단계별(거래정지→동시호가) 코드 천이는 **다음 실발동 시 §2.1 로깅으로 확정** 후 필요 시 감지셋·전환 타이밍 보정.

### 아키텍처 방향 (사용자 확정)
a2 분리는 과거 장운영 수신 불안정 + 실시간↔예상 전환난 때문이었으나, 현재 장운영 수신 양호 + VI 해제 즉시 실시간 복귀로 충분 → **a1 단일 통합**: a1 이 실시간체결가 상시 + VI/CB 시 예상체결가도 수신. (a2 subprocess 는 미사용 orphan 으로 유지)

---

## 3. VI 발동시각 버그 — 수정 완료 ✓

REST 감지 경로(`_handle_stale_check_result`)가 `_vi_exp_sub_switch()`만 호출하고 `_vi_start_ts` 미갱신 → 한 종목 복수 VI 시 2회차가 1회차 발동시각으로 잘못 스탬프. H0STMKO0 경로(8279~)와 대칭으로 발동시각·해제시각·발동횟수·상태캐시(`vi_cls_cache='Y'`) 기록하도록 수정(중복 [VI발동] 차단 포함).

---

## 4. decode skip (`[ws][utf8-diag]`) — KIS 서버측, 매매무해

장전 08:50~08:59 H0STANC0 프레임 후반 필드(오프셋 495~)에 `60 c2 80 80`/NUL 등 깨진 바이트. NUL 은 정상 텍스트 불가 → **KIS 서버측 프레임 깨짐 확정**("CP949 한글 유입" 가설 반증). `errors="replace"` 로 행 유실 없이 후반 1필드만 치환, 매매 영향 없음. 후반 필드 부분 활용 가능성은 후속 검토 과제.

---

## 5. 후속 과제 (미적용)

1. **무수신 워치독 판정 분리**: 한산장 무체결을 장애로 오인하지 않도록, 소켓 생존 양성증거(ping/pong·최근 핸드셰이크 dwell) 기반으로 재구독/재접속을 게이트. 오늘 정지의 일반 케이스 근본 대응.
2. **CB 단계 코드 확정**: §2.1 로깅으로 거래정지/동시호가/재개 코드 천이 캡처 후 전환 타이밍 정밀화.
3. **decode 후반필드 부분 활용** 검토.
4. **a2 subprocess** 정식 제거 또는 문서상 deprecated 명시.
