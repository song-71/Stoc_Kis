---
name: h0stcni0-acnt-no-identifies-account
description: H0STCNI0 체결통보의 acnt_no 필드가 공유 htsid 환경에서도 실제 체결 계좌(cano)를 식별한다는 KIS API 동작
metadata:
  type: project
---

H0STCNI0 체결통보(WSS)의 `acnt_no` 필드는 `cano(8자리) + acnt_prdt_cd(2자리)` 형식(예: a2 → "6361439001")으로, **실제 체결이 일어난 계좌를 식별 가능**하다.

**Why:** htsid(HTS 로그인 ID)는 a1/a2 두 계좌가 공유하므로 체결통보는 같은 WSS 구독으로 양 계좌가 함께 도착한다. 따라서 "어느 계좌 체결인지"는 구독 키로는 못 가르고, row 의 `acnt_no` 로만 구분된다. 260623 `out/logs/ccnl_notice_260623.csv` 실측으로 확인(402340 SK스퀘어 체결의 acnt_no=6361439001=a2). 두 cano(43444822/63614390)는 prefix 충돌 없음 → `acnt_no.startswith(cano)` 매칭이 안전.

**How to apply:** 종목→보유계좌 매핑(`_code_account_map`)을 잔고조회 경로뿐 아니라 매수 체결통보 시점에 `acnt_no`로 채울 수 있다. 매도/취소 시 보유계좌 식별이 필요하면 우선 이 방식을 고려. 0보유 계좌로 임의 발사하면 APBK0400(주문가능수량 초과) 무한실패가 난다(260623 1,803회 폭주 사례) → 보유계좌 못 찾으면 매도 스킵이 원칙.
