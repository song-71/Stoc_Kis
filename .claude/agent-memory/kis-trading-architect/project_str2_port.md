---
name: str2-port
description: str2 전략 서버 포팅 — 모듈/스펙 위치, ddof 결정, 지표 갭, 통합 방식
metadata:
  type: project
---

str2(전일 상한가 종목 b1/b2/b3 매수 + 손절/트레일/sell_ready 매도) 전략을 서버(ws_realtime_trading.py)에 라이브 통합하는 작업.

**구성**
- 순수 판단 모듈: `ws_realtime_tr_str2.py` (per-tick 스칼라 입력 → signal/reason. API/IO/주문 없음, kis_utils 만 의존). 노출 함수: classify_premarket, is_limit_up, compute_trends, buy_filter_allowed, buy_b1b3_origin, buy_escalation, sell_stop_trail, compute_ma500gap, sell_ready_set, sell_ready_release, sell_limit_place_ok, sell_order_resolve, sell_deadcross, carry_nextday_premarket_sell + Str2State.
- 크로스팀 스펙: `temp/260523 str2_port_local_reply(to_server).md` (로컬 회신 v2 = 정본 수식 §2/§3), `temp/260522 str2_port_server_review(to_local).md` (서버 검토).
- 백테스트 드라이버: Fplt_kis_ws_trading_engine_str2.py (로컬, 아직 서버 repo 에 없음).

**Why (핵심 결정)**
- ddof: 로컬 회신 §2/§3 은 BB σ ddof=1 명시하나, 서버 기존 `_calc_indicators`(line~10012) 가 이미 모집단분산(ddof=0)으로 baked BB 산출 중. 사용자 결정 4 = **서버는 ddof=0 (baked) 채택**, bb_lo/bb_hi/bb_hi_max 전부 동일 baked bb(ddof=0) 기반. ddof 불일치는 cross-team 조정 항목 — 로컬 뷰어/백테스트가 baked BB(ddof=0) 를 그대로 써야 backtest=live 파리티 성립.
- 별도 `_str2_state: dict[str,Str2State]` 신설 (str1 sell_state 확장 아님).
- str2 전용 동기 주문 함수 (_str2_place_buy/_str2_place_sell/_str2_cancel, 프리미티브 `_buy_order_cash`/`_sell_order_cash`/`_cancel_order_generic` 직접 호출, v5 `_try_v5_buy` 패턴). str1 큐/worker 재사용 안 함.
- stoch_k = `_price_buf[code]` 마지막 1500개로 산출 (raw %K, diff==0/warmup→NaN, clip0..100, M평활 미적용).
- bb_hi_max = rolling_max(bb_upper, 600). 손절/트레일은 strat.sell_stop_trail 로만 (메인 중복 금지). carry_hold 는 v4 overnight/nextday 와 배타.

**How to apply**
- 기본 비활성 불변: config 가 str2 를 지정하지 않으면 str1/v5/VI/종가매수 거동 바이트 동일. str2 게이트 밖으로 부작용 누출 금지.
- WSS H0STCNT0 스키마에 askp1/bidp1 존재 확인됨 (domestic_stock_functions_ws.py:667-668). str2 docstring 의 bidp0/askp0 = WSS bidp1/askp1.

관련: [[external-orders-failed-policy]]
