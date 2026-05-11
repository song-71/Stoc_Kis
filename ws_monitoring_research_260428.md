# WSS 부모 무한 재접속 root cause (260428)

## Timeline (정확한 시간/이벤트)

- **08:28:03** `=== WSS START ===` (1차 기동) — `out/logs/wss_TR_260428.log:1`
- **08:28:11** `[ws] connect attempt=1 mode=PREOPEN_WAIT` — log:59 (정상)
- **08:50:33~08:58:46** PREOPEN_EXP 시도 #2~#14 (간격 30초~수분, 종목구독 수신 0개 환경에서 자연스런 timeout). 동시에 `[a2-mp] 자식 프로세스 종료 감지 — 재시작 시도` 가 5초마다 폭주 — log:63~ (이후 f4ee6bf 로 a2 V2 config 로딩 버그 fix)
- **09:22:58** `signal=15` 종료, `09:23:14` `=== WSS START ===` (2차 기동)
- **09:23:22.445** `[ws] connect attempt=1 mode=REGULAR_REAL` — log:2641
- **09:23:23.571** `[ws] kws.start returned (mode=REGULAR_REAL)` — log:2645 (**1.1초 후 정상 return**)
- **09:23:23.571** `[ws] reconnect in 2s...` — log:2646
- 이후 attempt=2부터 매 ~3.16s 간격으로 동일 패턴 반복 (`auth_ws 재발급 → H0STCNI0 구독 → open_map prepared → start on_result → 1.1s 후 returned → reconnect in 2s`)
- **현재 (11:09:43)**: attempt=183 진행 중. 단순 산술: 09:23:22 ~ 11:09:43 ≈ 6,381초 / 3.16s/attempt ≈ 2,019회 — 실제 1,975회 (log grep `[ws] connect attempt`)와 거의 일치 → **kws.start 가 거의 매번 1.1초 만에 정상 return** 하고 있음
- 도중 두 차례 `signal=15` 재시작 (10:40:16, 10:59:23), 매 재시작 후 attempt=1 부터 다시 storm 시작 → **재시작이 해결책이 아님**
- **에러/예외 로그 없음**: 09:23~현재 grep `WARNING|ERROR|Traceback|RemoteDisconnected|ALREADY IN USE` 결과 0건 (REST 500, [stale] 등 무관 항목 제외)

### 라이브 socket 상태 (`ss -tnp`)

```
ESTAB      0      0   172.31.0.49:35910 210.107.75.39:21000 pid=758751 (a2 child)   ← 정상 ESTAB
ESTAB      0      0   172.31.0.49:45354 210.107.75.39:21000 pid=737901            ← 4/27 起 stale Daily_inquire_vi_status.py
CLOSE-WAIT 10629  0   172.31.0.49:45390 210.107.75.39:21000 pid=307106            ← 4/16 起 stale
CLOSE-WAIT 1704   0   172.31.0.49:36684 210.107.75.39:21000 pid=373961            ← 4/19 起 stale
CLOSE-WAIT 6398   0   172.31.0.49:52502 210.107.75.39:21000 pid=430025            ← 4/20 起 stale
CLOSE-WAIT 886    0   172.31.0.49:54918 210.107.75.39:21000 pid=460908            ← 4/21 起 stale
(부모 758584 의 21000 ESTAB 은 ss snapshot 시점에 없음 — 1.1s 마다 끊겨 잡히지 않음)
```

부모 758584 의 9443(REST) 소켓만 CLOSE-WAIT 로 보이고, 21000(WSS) 은 매번 1초 만에 끊겨 ESTAB 으로 잡히지 않음.

---

## 코드 추적 (file_path:line — 인용)

### 1) 부모 WSS 메인 루프 — `ws_realtime_trading.py`

```python
# ws_realtime_trading.py:11625~11815 (요지)
def run_ws_forever():
    while not _stop_event.is_set():
        ...
        kws = ka.KISWebSocket(api_url="")            # line 11757 — max_retries 인자 미지정 → 기본값 사용
        ...
        kws.start(on_result=on_result)                # line 11794 — 블로킹 호출
        logger.info(f"... kws.start returned (mode={mode.name})")  # line 11796
        ...
        # finally: line 11803~11812 — _active_kws 정리
        ...
        logger.info(f"... reconnect in {backoff}s...") # line 11867
        time.sleep(backoff)                           # backoff=2
```

### 2) `KISWebSocket.start()` — `kis_auth_llm.py`

```python
# kis_auth_llm.py:698~700
def __init__(self, api_url: str, max_retries: int = 1, approval_key: str | None = None):
    self.api_url = api_url
    self.max_retries = max_retries  # 기본 1: 내부 재시도 없이 즉시 반환 → 외부 run_ws_forever가 최신 구독으로 재연결
```

```python
# kis_auth_llm.py:803~847 (__runner)
async def __runner(self):
    ...
    while self.retry_count < self.max_retries:           # line 827
        try:
            async with websockets.connect(url, ping_interval=None) as ws:   # line 829
                self._ws = ws
                for name, obj in init_map.items():
                    await self.send_multiple(ws, obj["func"], "1", obj["items"], obj["kwargs"])  # 22회 send (50ms sleep × 22 ≈ 1.1s)
                await asyncio.gather(self.__subscriber(ws))                  # line 838
        except Exception as e:                                               # line 841
            logging.debug(f"[ws] connection exception: {e}")                 # line 842 ★ DEBUG 로 swallow
            self.retry_count += 1                                            # line 843
            await asyncio.sleep(1)
        finally:
            self._ws = None
```

### 3) 로거 설정 — `ws_realtime_trading.py`

```python
# ws_realtime_trading.py:230
logging.basicConfig(level=logging.WARNING, force=True)    # 루트 로거 = WARNING
# ws_realtime_trading.py:381
logger.setLevel(logging.INFO)                              # `wss` 로거 = INFO
# ws_realtime_trading.py:392
fh.setLevel(logging.INFO)                                  # 파일 핸들러 = INFO
```

→ `kis_auth_llm.py:842` 의 `logging.debug(...)` 는 **루트 로거(WARNING)로 보내지므로 모두 drop**. 매 끊김의 실제 사유 (e.g. `ConnectionClosedError`, `ConnectionClosedOK`, KeyError, …) 가 한 줄도 기록 안 됨.

### 4) `__subscriber` — 1.1s 시점에 raise 가 발생함을 추정

```python
# kis_auth_llm.py:708~724
async def __subscriber(self, ws):
    _RECV_CHECK_INTERVAL = 60.0
    while True:
        if self._close_requested:
            await ws.close(); return
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=_RECV_CHECK_INTERVAL)
        except asyncio.TimeoutError:
            continue
        except Exception:
            raise              # line 723 → asyncio.gather → __runner 의 except Exception 으로 전파
        ...
```

`websockets.exceptions.ConnectionClosed*` 는 TimeoutError 가 아니므로 line 723 `raise` 로 전파. → `__runner` 의 line 841 `except Exception` 캐치 → `retry_count=1` → `while retry_count < max_retries=1` False → 루프 종료 → `start()` 정상 return → 외부 `run_ws_forever` 가 2초 후 재연결.

### 5) 22 메시지 송신 = 1.1초 — `kis_auth_llm.py`

```python
# kis_auth_llm.py:181
_smartSleep = 0.05    # auth() 시 prod 모드에서 설정
# kis_auth_llm.py:866
await asyncio.sleep(_smartSleep)   # send 마다 50ms
```

매 connect 직후 22 회 send (`H0STCNI0` 1 + `ccnl_krx` 20 + `exp_ccnl_krx` 1) → 22 × 50ms ≈ 1.1초. **로그상 connect → returned 간격 정확히 1.1s** 와 일치.

---

## Root cause 단정

**아키텍처 결함 + 환경 오염의 조합**:

### A. 결정적 코드 결함 — 진단 불가 상태로 만든 부분
1. `kis_auth_llm.py:842` — WSS 재연결 사유를 `logging.debug` 로 처리 (루트 WARNING 정책 하에서 **완전 무음**). 1,975회의 close 사유가 한 줄도 안 남음.
2. `kis_auth_llm.py:700` — `KISWebSocket.__init__` 의 `max_retries=1` 기본값. 부모 (`ws_realtime_trading.py:11757`) 가 인자 미지정 → **단 1회 예외에 즉시 종료**. 운영자 시각으론 storm 으로 보이지만 코드 의도는 "외부 루프가 최신 구독으로 재연결" — 외부 루프 (`run_ws_forever`) 가 끊김 사유 식별 없이 backoff=2s 고정 → **무한 재시도**.
3. `ws_realtime_trading.py:11796` — `kws.start returned` 로그가 정상 return 인지 예외 후 return 인지 구분 안 됨. INFO 레벨 단일 메시지로 1,975번 똑같이 출력.

### B. 환경 오염 — KIS 서버가 close 보내는 가능성 있는 트리거
* `Daily_inquire_vi_status.py` 4/27 기동분 (pid=737901) 가 **여전히 살아있고 `KISWebSocket(max_retries=100)` 으로 syw_2 appkey WSS ESTAB 유지 중** (`ss` 결과). 같이 syw_2 를 쓰는 a2 child (758751) 와 충돌 가능 — 그러나 부모 (main appkey) 자체와 직접 appkey 충돌은 없음.
* 4/16~4/21 기동된 stale `Daily_inquire_vi_status.py` 4건도 CLOSE-WAIT 로 syw_2 측 소켓을 잡고 있음.
* 부모는 **main appkey** 라 위 stale 들과 직접 충돌 안 함. 다만 KIS 서버가 단일 client IP 에서 동시 9개 WSS 시도를 비정상 행위로 간주하여 close 를 던질 가능성은 **추정 단계** — 결정적 단서는 코드/로그만으로는 잡을 수 없음 (KIS 서버 응답 frame 캡처 필요).

### 단정 가능한 부분
- **무음 storm 의 직접 원인**: `max_retries=1` + DEBUG-level 예외 swallow + 2s 고정 backoff 의 조합. 끊김 사유가 무엇이든 부모는 매 3.16초마다 무한 재접속.
- **추가 측정 필요한 부분**: KIS 서버가 close frame 을 던지는 *진짜 사유*. 단서 부족 — `kis_auth_llm.py:842` 를 WARNING 으로 격상하거나 traceback 을 남겨야 다음 단계 단정 가능.

---

## 권장 fix 옵션 (코드 변경 위치 + 의사코드)

### Fix #1 (필수, 즉시) — close 사유 가시화
`kis_auth_llm.py:841~843`
```python
except Exception as e:
    # 변경: DEBUG → WARNING + traceback 1회 / N회 주기로 출력
    logging.warning(f"[ws] connection exception: {type(e).__name__}: {e}")
    if isinstance(e, (websockets.exceptions.ConnectionClosed,)):
        logging.warning(f"[ws] close code={getattr(e,'code',None)} reason={getattr(e,'reason',None)!r}")
    self.retry_count += 1
    await asyncio.sleep(1)
```
→ 1회 storm 발생 시 KIS 서버의 close code/reason 이 즉시 로그에 남음 (e.g. 1011=server error, 1008=policy violation, 4xxx=KIS custom).

### Fix #2 (필수) — storm 자체 차단 (지수 backoff + 한도)
`ws_realtime_trading.py:11625~` `run_ws_forever()` 의 backoff 로직을 누적 실패율 기반으로 개선:
```python
# 의사코드: 10초 안에 재연결 5회 이상이면 storm 으로 간주
_recent_returns = deque(maxlen=10)
...
kws.start(on_result=on_result)
_recent_returns.append(time.time())
if len(_recent_returns) >= 5 and (_recent_returns[-1] - _recent_returns[0]) < 10.0:
    logger.error(f"{ts_prefix()} [ws] ★ storm 감지: 10s 내 {len(_recent_returns)}회 reconnect → 60s backoff + telegram 알림")
    _notify(f"WSS reconnect storm — KIS 서버 close 폭주", tele=True)
    time.sleep(60)
else:
    time.sleep(backoff)
```

### Fix #3 (권장) — stale 프로세스 청소 가드
`ws_realtime_trading.py` 기동부 (`__main__` 진입 직후) 에서 `pgrep -f Daily_inquire_vi_status.py` / `ws_realtime_trading.py` 같은 alias 로 살아있는 stale 프로세스 검사 → 발견 시 텔레그램 경고 + 자동 SIGTERM (선택). 4/16 부터 누적된 stale 6 건이 KIS 측 점유율을 끌어올려 close 를 유발했을 가능성.

### Fix #4 (선택) — `max_retries` 의도 명시
`ws_realtime_trading.py:11757`
```python
kws = ka.KISWebSocket(api_url="", max_retries=1)   # 명시화 (기본값에 의존하지 말고 의도 표시)
```
→ 또는 `max_retries=3` 으로 올려 단발 ConnectionClosed 는 내부 재시도로 해결, 누적 실패만 외부 루프로 노출.

---

## 부수 정리 사항

1. **stale 프로세스 6 개 즉시 SIGTERM** (사용자 확인 후): pid 307106, 373961, 430025, 460908, 737900, 737901. KIS 서버 side 의 syw_2 점유 상태가 자연 회복될 때까지 대기 시간 단축.
2. `Daily_inquire_vi_status.py` 의 종료 로직 검토 — 왜 16/19/20/21/27 다섯 차례 모두 정상 종료 안 되고 좀비처럼 살아남았는지 별도 root cause 필요. 의심: `ka.KISWebSocket(max_retries=100)` 무한 재시도 + `_stop_event` 누락.
3. `ws_a2_subprocess.py` 와 `Daily_inquire_vi_status.py` 가 둘 다 syw_2 appkey 로 WSS 등록 — 동시 동작 시 ALREADY IN USE 가능. 기동 시 한쪽만 쓰도록 (또는 daily 스크립트 종료 후 a2 시작) 가드 필요.
4. `ws_realtime_trading.py:11663~11695` 의 auth_ws 재발급 로직 — 매 attempt 마다 `_base_headers_ws["approval_key"]` 갱신. KIS 서버가 새 key 발급 시 이전 key 무효화 정책일 경우, 직전 connection 의 close frame 이 다음 attempt 의 첫 send 후에 도달 → 또 close 패턴 가능성. 이번 단서는 부족하지만 Fix #1 적용 후 close code 보면 단정 가능.
5. **본 reconnect storm 은 어제 a2 multiprocessing 도입(`a75ff0b`) 그 자체와 직접 인과는 약함**. 사용자 가설대로 어제 도입된 코드의 race 가 원인이라면, syw_2 fix(`f4ee6bf`) 이후에는 a2 가 안정 ESTAB 인 현재 부모도 안정화되어야 하나 그렇지 않음 → **부모-측 결함 (Fix #1, #2) 와 환경 오염 (정리 사항 1) 의 별도 사슬**.
