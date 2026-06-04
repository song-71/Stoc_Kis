# 로컬(노트북/외부 PC) 전달용 규칙 프롬프트 — appkey / access_token 공유

> 작성 260604. 서버(AWS) Stoc_Kis 운영 원칙을 로컬 viewer/보조 프로그램에 동일 적용하기 위한 프롬프트.
> 로컬에서 Claude(또는 작업자)에게 그대로 붙여넣어 사용.

---

## 붙여넣을 프롬프트

```
[KIS appkey / access_token 공유 규칙 — 반드시 준수]

배경: KIS 는 키가 2종류이며 서로 완전히 다르다.
- approval_key  : /oauth2/Approval, 웹소켓(WSS) 접속용. 같은 appkey 에 새로 발급하면
                  직전 키를 즉시 무효화한다(최신 1개만 유효). → 공유 주체끼리 새로 발급하면
                  상대 WSS 가 bare 1006(0.1초 즉사)으로 끊긴다.
- access_token  : /oauth2/tokenP, REST 호출용. 유효 24시간. 6시간 이내 재요청 시 같은 토큰을
                  반환하고, 신규 발급 시 KIS 가 카톡 알림톡을 보낸다. (approval_key 와 달리
                  신규 발급해도 기존 토큰을 즉시 무효화하진 않음 → WSS 두절로 이어지진 않고,
                  영향은 "중복 알림톡"에 한정.)

이 로컬 프로그램(viewer 등)이 지켜야 할 것:

1. [appkey 분리 — 1순위] 서버 메인(a1)이 쓰는 appkey 로 WSS approval_key 를 절대 새로
   발급하지 말 것. 서버 a1 의 WSS 가 즉시 끊긴다. 로컬은 반드시 서버와 다른 appkey
   (예: a2) 를 쓰거나, WSS 가 필요 없으면 approval_key 를 아예 발급하지 말 것.

2. [access_token 공유] REST access_token 은 appkey 당 하루 1발급을 목표로 한다.
   - 직접 POST /oauth2/tokenP 호출 금지. 반드시 캐시 파일을 경유한다.
   - 캐시 파일명은 계좌 식별자 a1/a2 체계로 통일: kis_token_a1.json / kis_token_a2.json.
     (변종 파일명 kis_token.json, kis_token_main.json, kis_token_syw2.json,
      kis_token_syw_2.json 등 금지 — 파편화되면 각자 재발급 → 카톡 알림 폭발.)
   - 캐시가 유효하면(만료 전) 무조건 재사용. 만료/없을 때만 1회 발급 후 파일 저장.

3. [발급 직접 호출 금지] approval_key·access_token 모두 중앙 인증 함수
   (kis_auth*.auth() / auth_ws())만 사용하고, /oauth2/Approval, /oauth2/tokenP 를
   코드에서 직접 호출하지 말 것.

요약: 로컬은 "서버와 다른 appkey(a2)" + "kis_token_a2.json 캐시 공유" 를 기본으로 한다.
같은 appkey 를 공유해야만 한다면 WSS approval_key 는 절대 새로 발급하지 말고,
access_token 도 서버 캐시 파일과 동일 토큰을 재사용하라.
```

---

## 참고 (서버측 근거)

- 규칙 원문: `rules/trading_project_main_plan.md`
  - "WSS approval_key 관리 표준 (260601 사고 후 수립)"
  - "REST access_token 공유 표준 (260604 수립)"
- 260604 사고: 토큰 캐시 파일명 파편화로 08:20·08:28·09:00 카톡 알림톡 다발.
- 계좌 식별자 매핑: `main → a1`, `syw_2/syw2 → a2`.
