# KIS OpenAPI WebSocket — `OPSP8996 ALREADY IN USE appkey` 좀비 세션 미해제 문의

## 1. 환경
- 계정: sywems12, cano 4344****22
- 서버: 실전(prod) — REST `openapi.koreainvestment.com:9443`, WSS `ops.koreainvestment.com:21000`
- 클라이언트: Python `websockets` 15.0.1, 한투 공식 샘플 기반
- 용도: 자체 알고리즘 트레이딩 (장 시간 상시 연결)

## 2. 증상 (재현 시나리오)
1. 장 중 WSS 텍스트 프레임에 invalid UTF-8 byte 가 섞여 들어와 client 가 close frame **1007** 송신
2. **KIS 서버가 close ack 를 보내지 않음** → TCP 가 ack 없이 끊김 (`code=1006`)
3. 직후 동일 appkey 로 재접속 시 `OPSP8996 ALREADY IN USE appkey` 반복
4. 시간이 경과해도 슬롯이 자연 해제되지 않음 (수 시간 잠김 사례 다수)
5. 매일 신규 appkey 를 발급받아 회피 중 — 지속 불가능

## 3. 시도한 조치 (모두 무효)
| 시도 | 결과 |
|------|------|
| `/oauth2/Approval` 새 approval_key 발급 | 슬롯 해제 안 됨 |
| `/oauth2/revokeP` access_token 폐기 | 200 OK, WSS 슬롯 영향 없음 |
| UNSUBSCRIBE (`tr_type=2`) 송신 | `OPSP0003 UNSUBSCRIBE ERROR(not found!)` 응답 |
| 시간 대기 후 재접속 | 만료 시간 일정치 않음 (수십 초~수 시간) |

## 4. 받은 응답 원문
```
{"body":{"rt_cd":"9","msg_cd":"OPSP8996","msg1":"ALREADY IN USE appkey"}}

{"body":{"rt_cd":"1","msg_cd":"OPSP0011","msg1":"invalid approval : 062f61cb-c756-4b8f-9fab-92113c6fbea4"}}

{"body":{"rt_cd":"1","msg_cd":"OPSP0003","msg1":"UNSUBSCRIBE ERROR(not found!)"}}
```

## 5. 실제 운영 로그 (2026-05-13 발생분)

### (a) 오전 — 1007 발생 후 자동 회복된 케이스 (정상 패턴, 약 18초 만에 회복)
```
[08:52:07] [ws] connection exception: ConnectionClosedError:
           sent 1007 (invalid frame payload data)
           invalid start byte at position 480; no close frame received code=1006
[08:52:18] 10초 데이터 미수신 → 재구독 시도 (exp_ccnl_krx:11)
[08:52:23] [ws] no data for 15s -> resubscribe (1/3)
[08:52:25] (초당 11건/수신 종목 11개) ← 정상 수신 재개

[08:53:41] [ws] connection exception: ConnectionClosedError:
           sent 1007 ... invalid start byte at position 482; no close frame received code=1006
[08:54:02] (초당 16건/수신 종목 11개) ← 자동 회복

[08:54:46] [ws] connection exception: ConnectionClosedError:
           sent 1007 ... invalid start byte at position 485; no close frame received code=1006
[08:56:02] (정상 수신 재개) ← 자동 회복
```
→ 같은 1007 이지만 짧은 시간 안에 KIS 가 슬롯을 풀어주어 정상 재접속 가능했음

### (b) 오후 15:26 — 1007 발생 후 슬롯 잠금 + 회복 실패 (사망 케이스)
```
[15:26:12] [ws] connection exception: ConnectionClosedError:
           sent 1007 (invalid frame payload data)
           unexpected end of data at position 702; no close frame received code=1006

[15:26:13] [ws][system] tr_id=H0STCNI0 tr_key=sywems12
           msg=invalid approval : c91b6f6c-e923-4dc0-b122-78ac73cd85eb
WARNING:root:[ws] connection exception: ConnectionClosedError: no close frame received or sent code=1006

[15:26:14] [ws][system] tr_id=H0STCNI0 tr_key=sywems12
           msg=invalid approval : c91b6f6c-e923-4dc0-b122-78ac73cd85eb   ← 같은 approval_key
[15:26:16] (같은 approval_key 로 invalid approval)
[15:26:17] (같은 approval_key 로 invalid approval)
[15:26:18] (같은 approval_key 로 invalid approval)
[15:26:19] (같은 approval_key 로 invalid approval)
... 동일 approval_key c91b6f6c-... 로 8회 연속 invalid approval

[15:26:33] [ws] kws.start dwell=11.15s < 15s
           → 핸드셰이크 실패 다발 → long backoff 90s

[15:28:14] [ws] kws.start dwell=11.17s < 15s
           → 자기보호중단 (프로세스 종료)
```
→ 1007 직후 ~1초 간격 폭주 재시도가 발생, 동일 approval_key 가 8회 거절됨. 이후 새 approval_key 발급해도 슬롯이 잠겨 있어 동일하게 ALREADY IN USE 응답.

### (c) 진단 스크립트 실행 결과 (같은 날 사후 검증)
```
[main]   REST Approval OK / WSS handshake OK
         WSS subscribe : FAIL (ConnectionClosedError code=1006)
         [resp] {"body":{"rt_cd":"9","msg_cd":"OPSP8996","msg1":"ALREADY IN USE appkey"}}

[syw_2]  REST Approval OK / WSS handshake OK
         WSS subscribe : OK_RESP   ← 다른 appkey 는 정상

[main]   /oauth2/revokeP 호출 → 200 OK ("접근토큰 폐기에 성공하였습니다.")
         새 approval_key 발급 → 다른 키 값 발급됨
         WSS subscribe 재시도 → 여전히 OPSP8996 ALREADY IN USE
```
→ revokeP 가 WSS 슬롯에는 영향 없음을 확인

## 6. 질문 (핵심 3가지)

**Q1.** `OPSP8996 ALREADY IN USE appkey` 상태에서 **client 측이 슬롯을 즉시 해제할 수 있는 공식적인 방법**이 있습니까? (예: 슬롯 release 전용 엔드포인트, force 옵션 등)

**Q2.** ALREADY IN USE 상태의 **자연 해제 시간 기준**과 **한 appkey 당 동시 WSS 세션 한도**를 알려주세요.

**Q3.** 동일 사례에 대한 **공식 권장 처리 방법**(client 측 close handshake, 재접속 backoff, 다중 appkey 권장 여부 등) 을 안내 부탁드립니다.

## 7. 참고
- 동일 시점 별도 appkey 의 다른 계좌는 정상 동작 → IP/네트워크 문제 아님, **특정 appkey 슬롯의 KIS 서버측 상태 이상** 으로 판단
- 오전과 오후 1007 의 차이는 *후속 재시도 빈도* — 오전엔 ~18초 침묵 후 1회 재시도(자동 회복), 오후엔 ~1초 간격 8회 폭주 재시도(슬롯 잠금). KIS 서버측에서 동일 appkey 의 단기간 다회 invalid approval 발생 시 throttle 모드로 진입하는 정책이 있는지 함께 확인 부탁드립니다.
