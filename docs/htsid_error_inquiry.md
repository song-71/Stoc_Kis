# H0STCNI0 체결통보 htsid 에러 문의

## 현상
- 일시: 2026-03-06 09:02:01
- WSS 연결 후 H0STCNI0 구독 시 에러 응답 수신
- 에러 메시지: `ERROR : htsid가잘못되었습니다`
- 응답 헤더: tr_id=(null), tr_key=(비어있음)
- 이후 체결통보 수신 불가 (주문접수/체결 모두 미수신)

## 현재 설정값
- appkey: PSqbw6L... (main 계좌 43444822)
- my_htsid (tr_key): "sywems12"
- 구독 요청 JSON:
```json
{
  "header": {"approval_key": "...", "tr_type": "1", "custtype": "P", "content-type": "utf-8"},
  "body": {"input": {"tr_id": "H0STCNI0", "tr_key": "sywems12"}}
}
```

## 질문

### 1. HTS ID 확인
- "sywems12"가 현재 유효한 HTS 로그인 ID가 맞는지?
- KIS Developers 포탈에서 이 appkey에 연결된 HTS ID를 확인하는 방법은?

### 2. htsid 형식
- H0STCNI0 구독 시 tr_key에 넣어야 하는 값이 정확히 무엇인지?
  (HTS 로그인 ID? KIS Developers 회원 ID? 계좌번호?)
- 대소문자를 구분하는지?

### 3. 에러 원인
- "htsid가잘못되었습니다" 에러가 발생하는 조건은?
  (ID 불일치? 만료? appkey-htsid 불일치?)
- 이 에러 시 서버가 WSS 연결을 끊는지, 아니면 연결은 유지되고 구독만 실패하는지?

### 4. appkey와 htsid 관계
- 실전투자 appkey에 대해 htsid는 1:1 대응인지?
- 한 계정에 appkey가 여러개면 동일 htsid를 사용하는지?

### 5. 이전 정상 동작 → 갑자기 에러
- htsid가 만료되거나 무효화되는 경우가 있는지?
- 비밀번호 변경, 보안 설정 변경 등과 관련이 있는지?

### 6. 체결통보와 REST API 주문 관계
- 체결통보(H0STCNI0) 구독 없이도 REST API 주문은 정상 동작하는지?
  (맞다면 htsid 에러가 주문 자체에는 영향 없음을 확인)
