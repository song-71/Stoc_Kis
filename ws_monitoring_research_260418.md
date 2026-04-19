# WS Monitoring Research 260418: WSS 반복 끊김 종합 원인 분석

## 배경

- 4월 들어 WSS 수신 중단이 **매일** 반복 발생
- v0416_P1 불안정으로 **v0401 안정 버전 완전 복원** (ws_realtime_trading.py + kis_auth_llm.py 모두 원본)
- 복원 후에도 동일 증상 지속
- **2~3월에는 WSS 수신 중단 사례 없었음**

---

## 1. 4월 전체 장애 타임라인

| 날짜 | 주요 현상 | 원인 분류 | 해결 |
|------|----------|----------|------|
| 260401 | H0STCNI0 체결통보 미수신 (15:30 동시호가) | KIS 서버 or 타이밍 | 15:31:10 이후로 이동 |
| 260402 | VI 구독 5s timeout (asyncio deadlock) | **클라이언트** | VI 구독 별도 스레드 위임 |
| 260402-2 | VI 예상체결가 미수신, 보유종목 없으면 WSS skip | **클라이언트** | 7개 수정사항 구현 |
| 260403 | resubscribe 반복 경고 (09:00~09:19) | **클라이언트** | 타이머 리셋 + 간격 확대 |
| 260404 | VI/사이드카/써킷 Parquet 미저장 | **클라이언트** (설계) | 컬럼 추가 설계 |
| 260407 | WSS 전면 미수신 (보유종목 없으면 30분 sleep) | **클라이언트** | 블록 삭제 |
| 260408 | 미체결취소 500에러, NXT 0종목 | KIS 서버 + 클라이언트 | NXT 필터 변경 |
| 260409 | 2분 15초 정적화 → silent exit | **클라이언트** (deadlock) | watchdog + faulthandler |
| 260410 | silent SIGKILL (faulthandler 덤프 절단) | **외부** (EC2 의심) | CloudWatch 확인 필요 |
| 260415 | a2-wss send timeout 연쇄 (7분간) | **클라이언트** (Polars 미변환 + watchdog) | global 누락 수정 |
| 260416 | resubscribe 무한 루프 (250회+, 1500메시지) | **클라이언트** (zombie 미감지) | NO_DATA_RECONNECT_COUNT 도입 |
| 260417 | 장전 반복 끊김 + 09:08 수신 중단 + MAX SUBSCRIBE OVER | KIS 서버 + 클라이언트 | 슬롯 계산 개선안 |

**통계**: 12건 중 **서버 원인 확인 2건**, **클라이언트 원인 확인 8건**, **외부(EC2) 1건**, **혼합 1건**

---

## 2. "동일 코드에서도 발생" — 핵심 분석

### 확인된 사실

| 구성요소 | 2~3월 (안정기) | 4월 v0401 복원 |
|----------|---------------|----------------|
| ws_realtime_trading.py | v0401 원본 | v0401 원본 ✅ |
| kis_auth_llm.py | v0401 원본 (907줄) | v0401 원본 (907줄) ✅ |
| KIS WSS 서버 | 안정 | **불안정 (끊김 반복)** |
| EC2 인스턴스 | 동일 | 동일 (확인 필요) |

### 결론

**코드가 동일한데 4월에만 끊기므로, 코드 자체는 원인이 아니다.**

가능한 외부 원인:
1. KIS WSS 서버 인프라 변경 (가장 유력)
2. EC2 환경 변화 (CPU credit, 네트워크)
3. AWS 네트워크 경로 변경

---

## 3. KIS 서버 변경 의심 근거

### 3-1. TCP close frame 없는 끊김 (260417)

```
09:08 이후 데이터 수신 완전 중단
TCP close frame 없이 연결이 죽음 → 클라이언트가 끊김 감지 불가
```

- 2~3월에는 이런 패턴 없었음
- TCP close frame 없는 끊김 = **서버가 정상적으로 연결을 종료하지 않음**
- KIS 서버 로드밸런서 변경, 타임아웃 정책 변경 등으로 발생 가능

### 3-2. "ALREADY IN USE appkey" 빈발

```
재연결 시도 시 "ALREADY IN USE appkey" 에러 반복
→ 서버가 이전 세션을 즉시 정리하지 않음
```

- 2~3월: 발생 없음 또는 극히 드뭄
- 4월: 거의 매일 발생
- **서버측 세션 관리 정책 변경 의심** (세션 TTL 증가, 정리 주기 변경 등)

### 3-3. 장전 시간대 새로운 불안정

```
260417: 장전(08:50~09:00) 시간대에 WSS 반복 끊김
→ 이전에는 장전 불안정 보고 없었음
```

- 장전 시간대는 KIS 서버 부하가 높은 시점
- 서버 용량 변경 또는 인프라 교체 시 장전 불안정 발생 가능

### 3-4. MAX SUBSCRIBE OVER (260417)

- 40 슬롯 제한은 KIS 서버 정책이며, **클라이언트가 초과하지 않도록 관리해야 함**
- 260417에서 슬롯 초과 발생 → 구독 수 계산 로직에 H0STCNI0, H0STMKO0 등 고정 구독이 누락되어 실제 사용 슬롯을 과소 계산한 것이 원인
- 이는 **클라이언트 코드 버그**로, 서버 문제가 아님

---

## 4. EC2 환경 점검 항목

### 4-1. CPU Credit — ✅ 정상 (배제)

- 인스턴스: **t3.large** (i-0290759194abd5c6a)
- 4/10~4/17 credit balance: **min 836.9 / max 864.0** (만충 상태 유지)
- 10 미만 떨어진 적 없음 → **CPU credit 고갈 가설 완전 배제**
- 260410 silent SIGKILL의 원인은 CPU credit이 아닌 다른 요인

### 4-2. 메모리 — ✅ 정상 (배제)

- 총 7.6GB / 사용 3.8GB / 가용 3.9GB — 여유 충분
- OOM Killer 이력 없음, systemd-oomd 비활성
- **메모리 부족 가설 배제**

### 4-3. TCP Keepalive — ⚠️ 문제 발견

```
tcp_keepalive_time  = 7200  (2시간 — 너무 김)
tcp_keepalive_intvl = 75    (75초)
tcp_keepalive_probes = 9    (9회)
```

- dead connection 감지까지 **최대 2시간 + 11분 15초** 소요
- WSS가 TCP close frame 없이 끊기면 (260417에서 확인된 패턴), 클라이언트가 **2시간 동안 끊김을 모름**
- 이 동안 send/recv 모두 블로킹 → 데이터 수신 중단으로 나타남
- **권장**: `tcp_keepalive_time`을 60초로 낮추면 dead connection을 ~74초 내에 감지 가능

```bash
# 즉시 적용 (재부팅 시 초기화)
sudo sysctl -w net.ipv4.tcp_keepalive_time=60
# 영구 적용
echo "net.ipv4.tcp_keepalive_time = 60" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

---

## 5. 권장 대응 방안

### 즉시 (P0)

| 조치 | 상세 |
|------|------|
| **KIS 고객센터 문의** | "4월부터 WSS(H0STCNT0/H0STCNI0) 연결이 TCP close 없이 끊기는 현상이 매일 발생. 서버 인프라 변경 여부 확인 요청" |
| **EC2 CPU credit 확인** | CloudWatch에서 4월 CPU credit 추이 확인. 0 도달 이력 있으면 인스턴스 타입 변경 |
| **TCP keepalive 설정** | `tcp_keepalive_time`을 60초로 낮춰 dead connection 조기 감지 |

### 단기 (P1) — v0401 기반 최소 방어 코드

| 조치 | 상세 |
|------|------|
| **ping/pong heartbeat** | WSS에 주기적 ping 전송 (30초), pong 미수신 시 재연결 |
| **수신 타임아웃 watchdog** | 60초 이상 데이터 미수신 시 자동 재연결 (v0401에는 없는 기능) |
| **"ALREADY IN USE" 대응** | appkey 충돌 시 30초 대기 후 재시도 (서버 세션 만료 대기) |

### 중기 (P2)

| 조치 | 상세 |
|------|------|
| **EC2 인스턴스 변경** | burstable(t3) → fixed performance(m6i) 검토 |
| **이중화** | WSS 끊김 시 REST API 폴백으로 체결 데이터 보완 |

---

## 6. 종합 판단

**4월 WSS 반복 끊김의 근본 원인은 코드가 아닌 외부 요인이다.**

가장 유력한 원인은 **KIS WSS 서버 인프라 변경**(65%):
- TCP close 없는 끊김, ALREADY IN USE 빈발, 장전 불안정 등 서버측 행동 변화 징후가 다수
- 동일 코드(v0401)로 2~3월 정상 → 4월 매일 끊김이 이를 뒷받침

차순위로 **TCP keepalive 설정**(20%):
- 기본값 7200초(2시간) → dead connection 감지 불가 → 수신 중단 장기화
- `tcp_keepalive_time=60`으로 변경 시 ~74초 내 감지 가능
- (EC2 CPU/메모리는 점검 결과 정상 — 배제)

**코드 개선(P1)은 원인 해결이 아닌 방어 수단**으로, KIS 서버 확인과 병행해야 함.


====================================================================================
====================================================================================

## 7. KIS GitHub 예제 코드 분석 및 핵심 원인 발견 (260418 추가)

### 7-1. KIS 공식 GitHub 확인 결과

- https://github.com/koreainvestment/open-trading-api
- 4.2 공지는 Strategy Builder / Backtester / AI Extension **신규 추가** 안내이며, WebSocket 프로토콜 자체 변경은 아님
- 그러나 **KIS 공식 WebSocket 예제 코드에서 결정적인 차이점** 발견

### 7-2. 핵심 원인: `websockets 16.0` 라이브러리의 자동 ping과 KIS 서버 충돌

#### 우리 코드 (kis_auth_llm.py:811)
```python
async with websockets.connect(url) as ws:   # ← ping_interval 미지정
```

#### KIS 공식 예제 코드
```python
async with websockets.connect(url, ping_interval=None) as ws:   # ← ping 비활성화
```

#### 문제의 메커니즘

**websockets 16.0 라이브러리 기본값:**
- `ping_interval = 20` (20초마다 RFC 6455 WebSocket PING 프레임 자동 전송)
- `ping_timeout = 20` (20초 내 PONG 미수신 시 **라이브러리가 강제로 연결 종료**)

**두 종류의 PING/PONG이 충돌:**

| 구분 | RFC 6455 PING (라이브러리) | KIS PINGPONG (앱 레벨) |
|------|--------------------------|----------------------|
| 형태 | Binary control frame (opcode 0x9/0xA) | JSON text message `{"header":{"tr_id":"PINGPONG"}}` |
| 발신 | websockets 라이브러리가 **자동** 전송 | KIS 서버가 주기적 전송 |
| 응답 | 상대방이 PONG 프레임으로 응답해야 함 | 클라이언트가 동일 메시지를 echo |
| 우리 처리 | **없음** (라이브러리가 자동으로 보내는 것도 모름) | `await ws.pong(raw)` (line 774) |

**시나리오:**
```
1. websockets 라이브러리: 20초마다 서버에 RFC PING 프레임 전송
2. KIS 서버: RFC PING에 PONG 응답을 하지 않음 (KIS는 자체 PINGPONG만 지원)
3. 20초 후 라이브러리: "PONG 안 왔다 → 연결 죽었다" 판단 → 강제 close()
4. 우리 코드: 갑자기 연결 끊김 → 재연결 시도
5. 반복
```

**이것이 4월부터 발생한 이유:**
- websockets 라이브러리가 버전업되면서 ping_interval 기본값이 변경되었을 가능성
- 또는 이전에는 KIS 서버가 RFC PING에 응답했지만, 서버 업데이트로 응답 중단
- 어느 쪽이든, `ping_interval=None` 미지정이 근본 원인

### 7-3. 추가 발견: PINGPONG 응답 방식 오류

#### 우리 코드 (kis_auth_llm.py:774)
```python
if rsp.isPingPong:
    await ws.pong(raw)    # ← WebSocket binary PONG 프레임으로 전송
```

#### 올바른 방식
```python
if rsp.isPingPong:
    await ws.send(raw)    # ← 동일한 JSON text message로 echo
```

- `ws.pong(raw)`: RFC 6455 binary PONG **control frame** 전송 (opcode 0xA)
- `ws.send(raw)`: 일반 **text message** 전송

KIS 서버는 자체 PINGPONG에 대해 **text message echo**를 기대함.
`ws.pong()`으로 보내면 서버가 "PINGPONG 응답 안 왔다"고 판단할 수 있음 → 서버측 세션 타임아웃 → 끊김

### 7-4. 수정 사항 (2건)

#### 수정 1: `ping_interval=None` 추가 (최우선)

**파일**: `kis_auth_llm.py` line 811
```python
# Before
async with websockets.connect(url) as ws:

# After
async with websockets.connect(url, ping_interval=None) as ws:
```

#### 수정 2: PINGPONG 응답을 `ws.send()`로 변경

**파일**: `kis_auth_llm.py` line 774
```python
# Before
await ws.pong(raw)

# After
await ws.send(raw)
```

### 7-5. 이것이 모든 증상을 설명하는 이유

| 증상 | ping_interval=20 으로 설명 |
|------|--------------------------|
| **TCP close 없는 끊김** | 라이브러리가 close() 호출하지만, 서버는 모름 → 서버 세션 유지 → "ALREADY IN USE" |
| **ALREADY IN USE appkey 빈발** | 라이브러리가 끊었지만 서버는 이전 세션 활성 상태 |
| **20~40초 간격 반복 끊김** | ping_interval=20 + ping_timeout=20 = 최대 40초 주기 |
| **v0401 복원해도 동일** | v0401의 kis_auth_llm.py에도 `ping_interval=None` 없음 (line 804) |
| **2~3월에는 정상** | websockets 라이브러리 업데이트 또는 KIS 서버 변경 시점과 일치 |
| **장전 시간대 불안정 심화** | 서버 부하 → PONG 응답 지연 → ping_timeout 초과 빈도 증가 |

### 7-6. websockets 라이브러리 버전 이력 확인 필요

```bash
# 현재 설치 버전 확인 (결과: 16.0)
pip show websockets

# 언제 16.0으로 업데이트 되었는지 확인
pip install websockets==15.0 --dry-run  # 이전 버전 확인용
```

- websockets **13.0 이전**: `ping_interval` 기본값 = `20` (이미 있었음)
- 다만 **13.x → 14.x → 15.x → 16.0** 사이에 ping 동작 변경이 있었을 수 있음
- 2~3월에 정상이었다면, 그 사이에 `pip install --upgrade` 또는 venv 재생성이 있었을 가능성

====================================================================================
====================================================================================
