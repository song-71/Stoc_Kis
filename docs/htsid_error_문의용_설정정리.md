# "htsid가 잘못되었습니다" 오류 문의용 설정 정리

## 0. 결론 (우선 점검)

- **오류는 구독 실패가 아니라 WebSocket 접속 인증 단계 실패**
- **H0STCNI0 tr_key = my_htsid (HTS 로그인 ID)** ← 계좌번호/종목코드 아님
- **kis_devlp.yaml의 `my_htsid`**에 실제 HTS 로그인 ID(영문/숫자)를 정확히 입력해야 함

### 점검 체크리스트
1. kis_devlp.yaml의 `my_htsid` = HTS 로그인 ID (이메일X, 계좌번호X, AppKeyX)
2. approval_key 재발급 후 연결하는지 (재연결 시 auth_ws 호출)
3. 실전/모의 TR 혼용 없는지
4. approval_key 발급 계정 = my_htsid 계정인지

---

## 1. 오류 발생 시점

- **시간대**: 16:00~18:00 시간외 단일가 구간
- **트리거**: "예상체결가 → 체결가 전환" 직후 (`[시간외] 예상체결가 → 체결가 전환` 로그 출력 직후)
- **응답 형식**: `tr_id=(null) tr_key= msg=ERROR : htsid가잘못되었습니다`
- **연결**: `Connection exception >>  no close frame received or sent` 이후 재연결 시도 반복

---

## 2. 현재 구독 구성 (open_map)

| 순서 | TR_ID | tr_key | 용도 |
|------|-------|--------|------|
| 1 | H0STCNT0 / H0GICN0 | 종목코드들 | overtime_ccnl_krx 또는 overtime_exp_ccnl_krx (시간외 체결가/예상체결가) |
| 2 | **H0STCNI0** | **my_htsid** (HTS 로그인 ID) | **주식 체결통보 (ccnl_notice)** |

**공식 정답**: tr_key = trenv.my_htsid (kis_devlp.yaml의 my_htsid와 일치해야 함)

---

## 3. H0STCNI0(ccnl_notice) 관련 설정

### 3-1. tr_key 시도한 값들 (모두 동일 오류)

| 시도 | tr_key 값 | 결과 |
|------|-----------|------|
| 1 | `cano+acnt` (예: 4344482201) | htsid 오류 |
| 2 | `trenv.my_htsid` (getTREnv) | htsid 오류 |
| 3 | 종목코드 (예: codes[0], 005930) | htsid 오류 |

### 3-2. ccnl_notice 구독 조건

```python
# 08:58~18:00 사이에 구독
if dtime(8, 58) <= now_t < END_TIME:  # END_TIME = 18:00
    acc_key = _get_ccnl_notice_tr_key()  # my_htsid 반환 (kis_devlp.yaml 설정값)
    if acc_key:
        ka.KISWebSocket.subscribe(ccnl_notice, [acc_key])
```

### 3-3. domestic_stock_functions_ws.py ccnl_notice

- **TR_ID**: H0STCNI0 (실전), H0STCNI9 (모의)
- **params**: `{"tr_key": tr_key}` 만 전달
- **정확한 tr_key**: my_htsid (HTS 로그인 ID, 종목코드 아님)
- **암호화**: 수신 데이터 AES256 복호화 (encrypt="Y")

---

## 4. 인증/연결 설정

### 4-1. auth_ws() (접속키 발급)

- **URL**: `{prod}/oauth2/Approval`
- **응답**: `approval_key` 사용
- **헤더**: `_base_headers_ws["approval_key"]` = approval_key
- **재발급**: 연결 끊김 후 재연결 시 `auth_ws(svr="prod")` 재호출

### 4-2. 웹소켓 URL

- **실전**: `ws://ops.koreainvestment.com:21000` (kis_devlp.yaml의 `ops`)
- **api_url**: `""` (기본 경로)

### 4-3. kis_auth_llm 설정 경로

- **주 설정**: `config/kis_devlp.yaml`
- **보조**: `config.json` → appkey, appsecret, base_url 병합
- **my_htsid**: `kis_devlp.yaml`의 `my_htsid: "사용자 HTS ID"` (직접 설정값)

---

## 5. 구독 전송 순서 (KISWebSocket.__runner)

```python
for name, obj in open_map.items():
    await self.send_multiple(ws, obj["func"], "1", obj["items"], obj["kwargs"])
```

- **overtime_exp_ccnl_krx** 또는 **overtime_ccnl_krx**: N개 종목코드
- **ccnl_notice**: 1개 tr_key (my_htsid)

---

## 6. 오류 가능성 정리

1. **htsid가 연결 단계 검증**  
   - tr_id=(null) → subscribe 응답이 아닌 연결/핸드셰이크 단계 오류일 가능성

2. **H0STCNI0 단독 사용 시**  
   - 체결통보는 다른 TR과 동시 구독 시 제약이 있는지 여부

3. **tr_key 형식**  
   - **정답**: my_htsid (HTS 로그인 ID)  
   - 계좌번호·종목코드 아님

4. **시간외 구간**  
   - 16:00~18:00에 H0STCNI0 구독 제한/제약 여부

5. **approval_key / htsid**  
   - approval_key는 연결 시 사용, my_htsid는 kis_devlp.yaml 설정  
   - Approval API 응답에 htsid 포함 여부 및 사용처 확인 필요

---

## 7. 질문 예시 (타 커뮤니티/고객센터 문의용)

> 한국투자증권 Open API 웹소켓 사용 중 "htsid가 잘못되었습니다" 오류가 발생합니다.
>
> - **발생 시점**: 16:00~18:00 시간외 단일가 구간, 예상체결가→체결가 전환 직후
> - **구독 TR**: H0STCNT0(시간외체결가) + H0STCNI0(체결통보)
> - **H0STCNI0 tr_key**: 공식은 my_htsid인데 동일 오류 → kis_devlp.yaml my_htsid 값 정확히 맞는지 점검 필요
> - **응답**: tr_id=(null), tr_key= 빈값
>
> H0STCNI0 체결통보 구독 시 tr_key에 어떤 값을 넣어야 하며, htsid 오류의 정확한 원인을 알 수 있을까요?
