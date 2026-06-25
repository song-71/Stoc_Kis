---
name: project-external-orders-failed-policy
description: 외부주문(external_orders) failed 처리 정책 — 자동삭제 금지, 1회 알림 후 config 유지, 08:30 이전 시작 시에만 cleanup
metadata:
  type: project
---

`ws_realtime_trading.py` 의 외부주문(`external_orders`) failed 처리 운영 정책.

## 정책

- `result.startswith("failed")` 항목은 **자동 삭제 금지**. config 에 그대로 남겨두고 1회 텔레그램 알림(`_failed_notified` 플래그로 중복 방지) 후 운영자가 수동 처리.
- 운영자 재주문 절차: config 의 해당 항목 `result` 를 빈값(`""`)으로 수정 → 다음 사이클에 자동 재실행.
- 단, 프로그램 시작 직후 **08:30 이전**에는 `_cleanup_failed_external_orders_at_startup()` 이 1회 실행되어 누적 failed 항목을 일괄 삭제 (전일자 잔재 정리).
- 08:30 이후 시작이면 cleanup 동작 안 함 — 장 시간 내 발생한 failed 는 알림으로 인지해야 하므로 보존.

## Why

2026-05-12 08:28 외부주문 #2 (006345, 시장가) 가 "접수" 상태로 묶이고 그 이후 추적 불가 → 사용자가 결과 누적 좌절 상태. failed 를 자동삭제하면 **왜 실패했는지 사용자가 영영 모름**. 시작 시 무조건 삭제도 안전하지 않아서, 장 시작 전(08:30 이전)에만 정리하고 그 외엔 알림 + 수동 처리로 결정.

## How to apply

- 외부주문 관련 분기를 추가/변경할 때 failed 항목을 무조건 pop 하지 말 것. `_failed_notified` 플래그를 통해 알림 idempotency 보장.
- cleanup 시점을 바꿀 때 (08:30 → 다른 시각) 사용자에게 한 번 확인 필요. 현재 컷오프 시각은 KST `dtime(8, 30)`.
- `ord_type="auto"` 시간대별 매핑 동작은 [[project-external-orders-auto-mapping]] 참고 (없으면 코드 `_exec_external_order` 의 auto 분기 직접 참조).
