# 전략 구현 방법 검토 — 모의투자 기반 상한가 근접 / Top30 전략 적용

> 작성일: 2026-04-22
> 목적: `01. up_limit_buy_str_260420.md` + `02. Top30_str.md` 전략을 `ws_realtime_tr_str1.py` 에 적용하되, **매매는 KIS 모의투자 계정으로만 실행**하고 WSS 데이터 수신은 기존 a1/a2 계정을 유지하는 구조의 가능성·설계·제약 정리
> 관련 문서: `docs/01. up_limit_buy_str_260420.md`, `docs/02. Top30_str.md`, `docs/KIS New_str_api_260402.md`

---

## 0. 핵심 결론 (TL;DR)

| 질문 | 답 | 근거 |
|------|-----|------|
| 두 전략을 실제로 돌릴 수 있는가 | **가능** | 판단 필터는 기존 WSS 스트림 + REST 보조로 실시간 산출 가능 |
| 모의투자 계좌로 매매만 분리할 수 있는가 | **가능** | `_buy_order_cash / _sell_order_cash`가 이미 `client+cano+tr_id`를 인자로 받음 → 별도 `KisClient` 하나 추가로 충분 |
| a1/a2 WSS 수신 유지 + 모의투자 주문 라우팅은 충돌하는가 | **충돌 없음** | KIS는 실전 토큰과 모의 토큰을 독립적으로 발급 가능. 접속 도메인/TR_ID만 분리하면 됨 |
| 전략 함수를 `ws_realtime_tr_str1.py` 에 추가하는 것이 맞는가 | **맞음** | str1.py 주석에 "순수 전략 판단 함수만 포함, 주문 실행은 ws_realtime_trading.py" 명시됨. 기존 `vi_buy_strategy`, `check_vi_sell` 과 동일 패턴 |

### 제약 (반드시 인지 후 진행)
1. 모의투자용 **WSS 체결통보(H0STCNI9)** 를 모의 approval_key로 **별도 WSS 구독** 필요 — 주문 체결/취소 이벤트 수신용
2. 일부 보조 REST(`frgnmem_pchs_trend`, `capture_uplowprice`, `volume-power`) 는 모의 미지원 가능성 있음 → **시세성 조회는 실전 계정 유지** 권장
3. 모의투자는 **종목/호가단위/거래정지** 등 실전과 상태가 다를 수 있음 (일부 종목 주문 거부 가능)
4. **상태 파일 네임스페이스 분리 필수**: `closing_buy_state_*.json`, `str1_sell_state_*.json`, `vi_buy_state_*.json` 등이 실전 기록과 섞이면 다음날 청산/재시작 시 데이터 오염
5. 시드 **1천만원 고정**, 하루 주문 횟수·금액 한도 있음 → `INIT_CASH`, `max_invest`, 1종목 수량 상한 별도 설정 필요

---

## 1. 전략 문서 요약

### 1-1. 상한가 근접 매수 전략 (`01. up_limit_buy_str_260420.md`)
- **진입**: WSS 실시간 `prdy_ctrt` **25~28%** 구간 진입 감지
- **Signal Strength** (0.0~1.0): 체결강도/호가잔량/외국인동향/변동성/VI해제반응 가산·감산, 0.5+ 시 매수, 0.7+ 시 정상 수량
- **수량**: `INIT_CASH × 10%` 기준, 강도에 따라 1/3 or full
- **Exit**: 상한가 도달 시 → 익일 전략 / 미도달 시 → -3% 손절, 10분 타임아웃, 트레일링 -2%, 15:10 청산
- **백테스트 최적**: +28% 진입 + 상한가 트레일(29% 이탈 매도) + 필터(거래량서지 3배, 갭 +5%, 60분 이내 도달, 전일등락 <10%) → 승률 56.9%, 상한가 도달 75.9%
- **충돌 방지**: 이 전략으로 매수한 종목은 기존 `EMERGENCY_29PCT_SELL` 대상에서 제외

### 1-2. Top30 장 오픈 + 종가매매 익일 폭락 필터 (`02. Top30_str.md`)

#### Part A — 장 오픈 매수
- **Phase 1 (09:00~09:05)**: 시초가 갭 ≥ +3%, Signal Strength ≥ 0.6 → 지정가 (시초가 + 2호가) 매수, 최대 2종목
- **Phase 2 (09:05~09:30)**: 5분 관찰 후, prdy_ctrt ≥ +5% 유지, Signal Strength ≥ 0.5 → 지정가 매수
- **Phase 3 (09:30~)**: 기존 top_rank_loop + 상한가 근접 전략 연계
- **손절/익절**: -3% 손절, 트레일링(+3% 활성화, -2% 하락), 09:30까지 +1% 미도달 시 타임아웃

#### Part B — 종가매매 폭락 필터
- 15:18 선정 시 외국인 동향/체결강도/거래대금/상한가 잔량/변동성 Signal Strength 0.5+ 통과 종목만 매수
- 익일 08:30 예상시초가 < 매수가 × 0.97 → 시초가 시장가 매도

---

## 2. KIS 모의투자(Paper Trading) 구조 정리

### 2-1. 도메인/인증
| 구분 | 실전(prod) | 모의(vps) |
|------|-----------|-----------|
| REST 도메인 | `https://openapi.koreainvestment.com:9443` | `https://openapivts.koreainvestment.com:29443` |
| WSS 도메인 | `ops.koreainvestment.com:31000` | `ops.koreainvestment.com:21000` |
| 앱키/시크릿 | `my_app` / `my_sec` | `paper_app` / `paper_sec` (별도 발급 필요) |
| 접근토큰 | 실전 토큰 (`kis_token_main.json`) | 모의 토큰 (별도 파일) |
| 시드 자금 | 계좌 실제 예수금 | 1천만원 (초기), 리셋 시 갱신 |

`kis_auth_llm.py:458` 에 이미 **자동 TR_ID 전환 로직** 존재:
```python
if ptr_id[0] in ("T", "J", "C"):     # 실전투자 TR ID
    if isPaperTrading():             # 모의투자 전환 시
        tr_id = "V" + ptr_id[1:]
```

### 2-2. 주요 TR_ID 매핑 (실전 → 모의)

| 용도 | 실전 TR_ID | 모의 TR_ID | 현재 사용 위치 |
|------|-----------|-----------|--------------|
| 매수 | `TTTC0802U` | `VTTC0802U` | `ws_realtime_trading.py:2762`, `:3012`, `:3181`, `:3282`, `:4250` |
| 매도 | `TTTC0801U` | `VTTC0801U` | `_sell_order_cash()` (:674, tr_id 하드코딩됨) |
| 정정/취소 | `TTTC0803U` | `VTTC0803U` | `_cancel_order_generic()` (:705) |
| 잔고조회 | `TTTC8434R` | `VTTC8434R` | `_get_balance_page()` (:527) |
| 미체결조회 | `TTTC0084R` | `VTTC0084R` | `_inquire_psbl_rvsecncl()` (:732) |
| WSS 체결통보 | `H0STCNI0` | `H0STCNI9` | `domestic_stock_functions_ws.py:433` |
| WSS 체결/호가/예상체결 | `H0STCNT0/H0STASP0/H0STANC0` | 동일 | 그대로 사용 (수신은 실전 WSS) |

### 2-3. 모의투자 한계 (KIS 공식)
- **일일 주문 건수/금액 제한** 있음 (공식 문서 기준 보수적으로 하루 수백 건)
- 시간외 단일가/장전·후 시간외 일부 제한
- **시세 데이터는 실전과 동일** (실시간) — 단, 대상 종목에 따라 매매 거부 가능 (ETN/정리매매/관리종목 등)
- 일부 REST TR은 **모의 미지원** (문서에 "실전만"으로 명시된 것들 — `frgnmem-pchs-trend`가 대표 사례)

---

## 3. 설계: "수신은 실전, 매매는 모의" 이중 채널 구조

### 3-1. 전체 흐름

```
[WSS 수신 채널 — 실전]           [REST 주문 채널 — 모의]
┌────────────────────────┐       ┌────────────────────────┐
│ a1 (main) appkey/sec   │       │ paper appkey/sec       │
│ https://openapi...9443 │       │ https://openapivts...  │
│ kis_token_main.json    │       │   ...:29443            │
│                        │       │ kis_token_paper.json   │
│ WSS ops:31000          │       │                        │
│ H0STCNT0 체결           │       │ 매수: VTTC0802U         │
│ H0STASP0 호가           │       │ 매도: VTTC0801U         │
│ H0STANC0 예상체결       │       │ 취소: VTTC0803U         │
│ (모든 종목 실시간 수신)  │       │ 잔고: VTTC8434R         │
└────────────┬───────────┘       └────────────▲───────────┘
             │                                │
             ▼                                │
      [ingest_loop]                           │
   WSS 프레임 파싱/지표 계산                    │
             │                                │
             ▼                                │
   전략 판단 (ws_realtime_tr_str1.py):        │
     - uplimit_buy_strategy()                 │
     - opening_buy_strategy()                 │
     - closing_buy_filter()                   │
     - 기존 check_vi_sell / check_realtime_sell│
             │                                │
             └───► 주문 실행 (모의 client) ────┘

[WSS 체결통보 — 모의 별도]
   paper approval_key → ops:21000 → H0STCNI9 구독
   → 모의 주문 체결/취소 이벤트 수신 (필수)
```

### 3-2. 옵션 플래그 (파일 상단)

`ws_realtime_trading.py` 최상단 사용자 설정 영역에 추가:

```python
# ─────────────────────────────────────────────────────────────
# 모의투자(Paper Trading) 옵션
#   True  : 모든 매수/매도/잔고조회/취소를 모의 계정으로 라우팅
#           단, WSS 실시간 체결·호가·예상체결 수신은 a1(실전) 유지
#   False : 기존과 동일 — 실전 계정으로 매매
# ─────────────────────────────────────────────────────────────
MOCK_TRADE = True        # 초기 검증은 True 로 시작
MOCK_INIT_CASH = 10_000_000   # 모의 시드 (1천만원 고정)
MOCK_MAX_INVEST_PER_STOCK = 1_000_000   # 1종목 최대 투자금 (10%)
MOCK_MAX_POSITIONS = 3   # 동시 보유 최대 종목 수
```

### 3-3. config.json 확장

```json
{
  "users": {
    "sywems12": {
      ...
      "accounts": {
        "main": { "appkey": "...", "appsecret": "...", "cano_alias": "a1", "trade_enabled": true, ... },
        "syw_2": { ..., "trade_enabled": false, ... },
        "paper": {
          "appkey": "PAPER_APPKEY_HERE",
          "appsecret": "PAPER_APPSECRET_HERE",
          "cano_alias": "p1",
          "cano": "MOCK_CANO_8DIGIT",
          "acnt_prdt_cd": "01",
          "trade_enabled": true,
          "is_paper": true,
          "base_url_override": "https://openapivts.koreainvestment.com:29443",
          "exclude_cash": "0",
          "max_invest": "1000000"
        }
      }
    }
  }
}
```

- 신규 키: `is_paper: true` → `_iter_enabled_accounts()` 에서 식별
- 신규 키: `base_url_override` → 모의 도메인 강제 적용 (전역 `base_url` 은 실전 유지)

### 3-4. 계정 라우팅 유틸

새 함수 추가 (`ws_realtime_trading.py`, `_init_top_client` 근처):

```python
_paper_client: KisClient | None = None

def _load_paper_cfg() -> dict:
    """config.json → accounts.paper 키를 읽어 모의 계정 설정 반환."""
    cfg = load_config(str(SCRIPT_DIR / "config.json"))
    for uid, user in cfg.get("users", {}).items():
        acct = user.get("accounts", {}).get("paper", {})
        if acct.get("appkey") and acct.get("is_paper"):
            return {
                "appkey": acct["appkey"],
                "appsecret": acct["appsecret"],
                "cano": acct["cano"],
                "acnt_prdt_cd": acct.get("acnt_prdt_cd", "01"),
                "base_url": acct.get("base_url_override") or "https://openapivts.koreainvestment.com:29443",
                "custtype": "P",
            }
    raise ValueError("config.json 에 accounts.paper (is_paper=true) 계정이 없습니다.")


def _init_paper_client() -> KisClient:
    p = _load_paper_cfg()
    return KisClient(KisConfig(
        appkey=p["appkey"], appsecret=p["appsecret"],
        base_url=p["base_url"], custtype=p["custtype"],
        market_div="J",
        token_cache_path=str(SCRIPT_DIR / "kis_token_paper.json"),
    ))


def _get_trade_client():
    """매매용 클라이언트 — MOCK_TRADE 여부에 따라 모의/실전 라우팅."""
    global _paper_client
    if MOCK_TRADE:
        if _paper_client is None:
            _paper_client = _init_paper_client()
        return _paper_client
    return _top_client or _init_top_client()


def _get_trade_tr_id(prod_tr_id: str) -> str:
    """주문/조회 TR_ID — 모의 모드면 T→V 변환."""
    if MOCK_TRADE and prod_tr_id and prod_tr_id[0] == "T":
        return "V" + prod_tr_id[1:]
    return prod_tr_id


def _get_trade_account() -> tuple[str, str]:
    """매매 대상 (cano, acnt_prdt_cd) — 모의 모드면 모의 계좌."""
    if MOCK_TRADE:
        p = _load_paper_cfg()
        return p["cano"], p["acnt_prdt_cd"]
    # 실전: 기존 로직 (_iter_enabled_accounts 등)
    accts = _iter_enabled_accounts(trade_only=True)
    if not accts:
        raise ValueError("실전 trade_enabled 계좌 없음")
    a = accts[0]
    return a["cano"], a["acnt_prdt_cd"]
```

### 3-5. 기존 주문 호출 부위 수정

기존 호출:
```python
_buy_order_cash(client, cano, acnt, "TTTC0802U", code, qty, price, ord_dvsn)
```

수정 후 (전역 변환 헬퍼 한 줄 추가):
```python
client = _get_trade_client()
cano, acnt = _get_trade_account()
_buy_order_cash(client, cano, acnt, _get_trade_tr_id("TTTC0802U"), code, qty, price, ord_dvsn)
```

또는 래퍼 함수를 도입해서 호출부 수정을 최소화:

```python
def _trade_buy(code, qty, price, ord_dvsn="00", prod_tr_id="TTTC0802U"):
    client = _get_trade_client()
    cano, acnt = _get_trade_account()
    return _buy_order_cash(client, cano, acnt, _get_trade_tr_id(prod_tr_id),
                           code, qty, price, ord_dvsn)

def _trade_sell(code, qty, price=0.0, ord_dvsn="01"):
    client = _get_trade_client()
    cano, acnt = _get_trade_account()
    # _sell_order_cash 내부 TR_ID도 _get_trade_tr_id 경유 필요
    return _sell_order_cash(client, cano, acnt, code, qty, price, ord_dvsn=ord_dvsn)
```

> `_sell_order_cash`(:674) 는 현재 TR_ID `TTTC0801U`를 **함수 내부에서 하드코딩** 중.
> 함수 시그니처에 `tr_id` 파라미터를 추가하거나, 내부에서 `_get_trade_tr_id("TTTC0801U")` 호출하도록 수정 필요.

### 3-6. WSS 체결통보(H0STCNI9) 이중 구독

**문제**: 모의투자 주문 체결은 실전 approval_key 로는 수신 불가.
**해법**: 모의 계정 approval_key 로 **별도 WSS 세션** 구성 (별도 스레드) 하여 `H0STCNI9` 만 구독.

- 기존 WSS: 실전 approval_key, 종목 체결/호가 수신
- 신규 WSS: 모의 approval_key, `H0STCNI9` 1슬롯 (모의 HTS ID 필요)

`kis_auth_llm.auth_ws(svr="vps")` 호출로 모의 approval_key 발급 가능. 단, 현재 `ka._base_headers_ws["approval_key"]` 는 싱글톤이라 **두 번째 접속을 위한 래퍼 필요**. `domestic_stock_functions_ws.py` 는 모듈 전역 WSS 클라이언트를 가정하므로, 모의 WSS는 별도 최소 구현으로 진행하거나 모의 체결통보 대신 **주문 후 `inquire_psbl_rvsecncl` 폴링**으로 대체 가능 (검증 단계에선 후자가 더 단순).

**권장**: 1단계에서는 체결통보 WSS 대신 **주문 응답(ODNO) + 주기 폴링**으로 처리, 2단계에서 H0STCNI9 별도 세션 추가.

### 3-7. 상태 파일 네임스페이스 분리

모의 모드에서 파일 접두사 분리:

| 실전 파일 | 모의 파일 (MOCK_TRADE=True 일 때) |
|-----------|----------------------------------|
| `data/fetch_top_list/closing_buy_state_{yymmdd}.json` | `..._paper_{yymmdd}.json` |
| `data/fetch_top_list/vi_buy_state_{yymmdd}.json` | `..._paper_{yymmdd}.json` |
| `data/fetch_top_list/str1_sell_state_{yymmdd}.json` | `..._paper_{yymmdd}.json` |
| `data/fetch_top_list/uplimit_buy_state_{yymmdd}.json` (신규) | `..._paper_{yymmdd}.json` |

구현: 파일 경로 생성 유틸에 `MOCK_TRADE` 체크 후 `_paper` 접미 삽입.

---

## 4. 전략 함수를 `ws_realtime_tr_str1.py` 에 추가

### 4-1. 현재 파일 구조 (확인됨)
- 모듈 주석: "순수 전략 판단 함수만 포함 (API 호출 없음). 실제 주문·상태 관리는 ws_realtime_trading.py 에서 수행"
- 기존 함수: `check_opening_call_auction_sell/_cancel`, `check_realtime_sell`, `vi_buy_strategy`, `vi_should_cancel`, `check_vi_sell`, `calc_sell_pnl`
- 핫스왑: `__main__` 실행 시 `config.json` 에 `strategy_swap` 요청 기록 → 메인 프로그램이 리로드

→ **신규 함수도 동일 규칙으로 추가 가능** (부작용 없음, import-only)

### 4-2. 추가할 함수

#### 4-2-1. 상한가 근접 매수 (`01.` 문서)

```python
def uplimit_approach_buy_signal(
    prdy_ctrt: float,
    stck_prpr: float,
    stck_oprc: float,            # 시가 (갭 계산)
    prev_close: float,           # 전일종가
    acml_vol: float,             # 당일 누적 거래량
    prev_vol_5m_avg: float,      # 직전 5분 평균 거래량 (WSS 집계)
    volume_power: float | None,  # 체결강도 (REST 1회 조회, None 허용)
    ask_vol_sum: float,          # 매도 잔량 합 (WSS 호가)
    bid_vol_sum: float,          # 매수 잔량 합 (WSS 호가)
    vola_10d: float | None,      # 10일 변동성 (사전 계산)
    frgn_3d_net: float | None,   # 외국인 3일 순매수 (사전 캐시)
    prev_prdy_ctrt: float | None, # 전일 등락률 (연속 급등 필터)
    minutes_since_open: float,   # 09:00 이후 경과 분
    vi_released_recent: bool,    # VI 해제 후 1분 이내 재상승 감지 여부
    already_holding: bool,
    now_hm: tuple[int, int],     # (hour, minute) — 14:50 이후 매수 금지
) -> tuple[bool, str, float, int]:
    """
    상한가 근접 매수 판단.

    Returns (should_buy, reason, strength, qty_multiplier):
      qty_multiplier: 3 = 정상(1/1), 1 = 축소(1/3), 0 = 보류
    """
    # 필수 컷: 시간대, 보유 중, 진입 구간
    if already_holding:
        return False, "", 0.0, 0
    h, m = now_hm
    if h >= 15 or (h == 14 and m >= 50):
        return False, "uplimit_장후반_금지", 0.0, 0
    if prdy_ctrt < 25.0 or prdy_ctrt >= 29.0:
        return False, "", 0.0, 0

    # Signal Strength
    s = 0.40
    reasons = [f"기본({prdy_ctrt:.1f}%)"]

    # 거래량 서지 (직전 5분 > 당일 평균의 3배) — 백테스트 1순위 필터
    if prev_vol_5m_avg > 0 and acml_vol > 0:
        avg_per_min = acml_vol / max(1.0, minutes_since_open)
        if prev_vol_5m_avg / 5.0 >= avg_per_min * 3.0:
            s += 0.15
            reasons.append("vol_서지3배")

    # 갭률
    if prev_close > 0 and stck_oprc > 0:
        gap_pct = (stck_oprc / prev_close - 1) * 100
        if gap_pct >= 5.0:
            s += 0.10
            reasons.append(f"갭+{gap_pct:.1f}%")

    # 호가 잔량
    if bid_vol_sum > 0 and ask_vol_sum > 0:
        if bid_vol_sum >= ask_vol_sum * 2.0:
            s += 0.15
            reasons.append("매수벽")
        elif ask_vol_sum >= bid_vol_sum * 3.0:
            s -= 0.20
            reasons.append("매도벽")

    # 체결강도
    if volume_power is not None:
        if volume_power >= 150.0:
            s += 0.20
            reasons.append(f"VP{volume_power:.0f}")
        elif volume_power < 90.0:
            s -= 0.15
            reasons.append(f"VP약{volume_power:.0f}")

    # 외국인
    if frgn_3d_net is not None:
        if frgn_3d_net > 0:
            s += 0.10
        elif frgn_3d_net < 0:
            s -= 0.10

    # 변동성
    if vola_10d is not None:
        if vola_10d < 0.02:
            s += 0.10
            reasons.append("저변동돌파")
        elif vola_10d > 0.05:
            s -= 0.10

    # VI 해제 후 반응
    if vi_released_recent:
        s += 0.10
        reasons.append("VI해제재상승")

    # 전일 등락률 — 연속급등(작전주) 제외: 백테스트 1순위 필터
    if prev_prdy_ctrt is not None and prev_prdy_ctrt >= 10.0:
        s -= 0.30   # 사실상 차단
        reasons.append("전일급등")

    # 장 후반 감산
    if h == 14 and m >= 30:
        s -= 0.05

    # 결정
    if s >= 0.70:
        return True, "uplimit_매수(" + ",".join(reasons) + f")/s={s:.2f}", s, 3
    if s >= 0.50:
        return True, "uplimit_축소(" + ",".join(reasons) + f")/s={s:.2f}", s, 1
    return False, f"uplimit_보류/s={s:.2f}", s, 0


def uplimit_approach_exit(
    prdy_ctrt: float,
    bidp1: float,
    buy_price: float,
    highest_since_buy: float,
    minutes_since_buy: float,
    now_hm: tuple[int, int],
) -> tuple[bool, str]:
    """상한가 근접 매수 포지션의 매도 조건."""
    # 상한가 도달 → 익일 전략 (매도 안 함)
    if prdy_ctrt >= 29.5:
        return False, "uplimit_상한가도달_익일전환"

    # 15:10 이후 미도달 청산
    h, m = now_hm
    if h == 15 and m >= 10:
        return True, "uplimit_마감청산"

    # 손절 -3%
    if buy_price > 0 and bidp1 > 0 and bidp1 <= buy_price * 0.97:
        return True, f"uplimit_손절-3%({bidp1}/{buy_price})"

    # 10분 타임아웃 + 25% 미만
    if minutes_since_buy >= 10 and prdy_ctrt < 25.0:
        return True, f"uplimit_타임아웃({prdy_ctrt:.1f}%)"

    # 트레일링 -2% (상한가 근접은 타이트)
    if highest_since_buy > 0 and bidp1 > 0 and bidp1 <= highest_since_buy * 0.98:
        return True, f"uplimit_트레일링-2%(고가{int(highest_since_buy):,})"

    return False, ""
```

#### 4-2-2. 장 오픈 매수 (`02.` 문서, Phase 1/2)

```python
def opening_buy_phase1_signal(
    prdy_ctrt: float,
    stck_prpr: float,
    stck_oprc: float,
    prev_close: float,
    sec_since_open: float,          # 09:00 이후 초
    antc_volume_rank: float | None, # 08:50~09:00 예상거래량 상위 % (0.0~1.0)
    frgn_3d_net: float | None,
    vola_10d: float | None,
    was_prev_day_uplimit: bool,     # 전일 상한가 종목 여부
    already_holding: bool,
    position_count: int,
    max_positions: int = 2,
) -> tuple[bool, str, float]:
    """
    09:00:00~09:00:10 시초가 갭 매수 판단.
    Signal Strength ≥ 0.6 → 매수.
    """
    if already_holding or position_count >= max_positions:
        return False, "", 0.0
    if sec_since_open > 10.0:
        return False, "", 0.0
    if prev_close <= 0 or stck_oprc <= 0:
        return False, "", 0.0

    gap_pct = (stck_oprc / prev_close - 1) * 100
    if gap_pct < 3.0:
        return False, f"갭부족({gap_pct:+.1f}%)", 0.0

    # 09:00:05 시점 체결가 >= 시초가 확인 (상승 지속)
    if sec_since_open >= 5.0 and stck_prpr < stck_oprc:
        return False, "시초가_하락전환", 0.0

    s = 0.40
    if was_prev_day_uplimit:
        s += 0.15
    if gap_pct >= 7.0:
        s += 0.15
    if antc_volume_rank is not None and antc_volume_rank <= 0.30:
        s += 0.10
    if frgn_3d_net is not None and frgn_3d_net > 0:
        s += 0.10
    if vola_10d is not None:
        if vola_10d < 0.02: s += 0.10
        elif vola_10d > 0.05: s -= 0.10
    if gap_pct < 3.0: s -= 0.15

    return (s >= 0.60), f"opening_ph1/s={s:.2f},갭{gap_pct:+.1f}%", s


def opening_buy_phase2_signal(
    prdy_ctrt: float,
    min_since_open: float,                     # 09:00 이후 분
    low_since_open: float, stck_oprc: float,   # 하락 없이 유지 체크
    volume_power: float | None,
    bid_vol_sum: float, ask_vol_sum: float,
    frgn_3d_net: float | None,
    was_prev_day_uplimit: bool,
    vola_10d: float | None,
    rank_rising: bool,                         # 09:00→09:05 순위 상승 여부
    already_holding: bool,
    position_count: int,
    max_positions: int = 3,
) -> tuple[bool, str, float]:
    """09:05~09:30 확인 후 진입."""
    if already_holding or position_count >= max_positions:
        return False, "", 0.0
    if not (5.0 <= min_since_open <= 30.0):
        return False, "", 0.0
    if prdy_ctrt < 5.0:
        return False, "", 0.0
    if low_since_open < stck_oprc:
        return False, "시초_하락이력", 0.0
    if bid_vol_sum <= ask_vol_sum:
        return False, "매수잔량약", 0.0

    s = 0.35
    s += 0.15  # 등락률 유지
    if volume_power is not None and volume_power >= 120.0: s += 0.15
    if frgn_3d_net is not None and frgn_3d_net > 0: s += 0.10
    if was_prev_day_uplimit: s += 0.10
    if vola_10d is not None and vola_10d < 0.02: s += 0.10
    if rank_rising: s += 0.05
    if ask_vol_sum >= bid_vol_sum * 3: s -= 0.10
    if frgn_3d_net is not None and frgn_3d_net < 0: s -= 0.10

    return (s >= 0.50), f"opening_ph2/s={s:.2f}", s


def opening_buy_exit(
    bidp1: float, buy_price: float,
    highest_since_buy: float,
    minutes_since_buy: float,
    min_since_open: float,
    phase: int,   # 1 또는 2
) -> tuple[bool, str]:
    """장 오픈 매수 포지션 Exit."""
    if buy_price <= 0 or bidp1 <= 0:
        return False, ""
    # 손절 -3%
    if bidp1 <= buy_price * 0.97:
        return True, f"opening_손절-3%"
    # Phase 1 타임아웃: 09:30까지 +1% 미도달
    if phase == 1 and min_since_open >= 30.0 and bidp1 < buy_price * 1.01:
        return True, "opening_ph1_타임아웃"
    # 트레일링 +3% 활성, -2% 하락
    if highest_since_buy >= buy_price * 1.03:
        if bidp1 <= highest_since_buy * 0.98:
            return True, f"opening_트레일링(고가{int(highest_since_buy):,})"
    return False, ""
```

#### 4-2-3. 종가매매 폭락 방지 필터 (`02.` Part B)

```python
def closing_buy_filter_signal(
    prdy_ctrt: float,
    stck_prpr: float, stck_hgpr: float,
    prdy_sign: str,                   # "1"(상한가) "2"(상승)
    volume_power: float | None,
    acml_tr_pbmn: float,              # 누적거래대금(원)
    uplimit_bid_ratio: float | None,  # 상한가 잔량 / 전일 거래량
    frgn_3d_net: float | None,
    vola_10d: float | None,
    institution_net_today: float | None,
) -> tuple[bool, str, float]:
    """
    15:18 종가매수 후보에 대한 폭락 방지 필터 (Signal Strength).
    0.6+ 정상 수량, 0.5~0.6 최소 수량, 0.5 미만 보류.
    """
    # 기본 조건 (기존)
    if prdy_sign not in ("1", "2"): return False, "prdy_sign", 0.0
    if prdy_ctrt < 20.0: return False, "등락률부족", 0.0
    if stck_hgpr > 0 and (stck_prpr / stck_hgpr) < 0.97:
        return False, "고가대비약", 0.0
    # 거래대금 최소
    if acml_tr_pbmn < 5_000_000_000:
        return False, "거래대금부족", 0.0

    s = 0.50
    if frgn_3d_net is not None and frgn_3d_net >= 0: s += 0.15
    elif frgn_3d_net is not None and frgn_3d_net < 0: s -= 0.20   # 강한 제외
    if volume_power is not None:
        if volume_power >= 150.0: s += 0.10
        elif volume_power < 100.0: s -= 0.15
    if uplimit_bid_ratio is not None:
        if uplimit_bid_ratio >= 0.20: s += 0.10
        elif uplimit_bid_ratio < 0.05: s -= 0.10
    if vola_10d is not None:
        if vola_10d < 0.02: s += 0.10
        elif vola_10d > 0.05: s -= 0.10
    if institution_net_today is not None and institution_net_today > 0: s += 0.05

    if s >= 0.60: return True, f"closing_정상/s={s:.2f}", s
    if s >= 0.50: return True, f"closing_축소/s={s:.2f}", s
    return False, f"closing_보류/s={s:.2f}", s


def closing_buy_next_day_exit(
    antc_prce: float,
    buy_price: float,
    is_opening_tick: bool,   # 09:00 시초가 체결 직후 여부
) -> tuple[bool, str]:
    """익일 동시호가/시초가 폭락 감지 손절."""
    if buy_price <= 0 or antc_prce <= 0:
        return False, ""
    # 08:30~09:00 동시호가: 예상가 < 매수가 × 0.97 → 시초 시장가 매도
    if not is_opening_tick and antc_prce < buy_price * 0.97:
        return True, f"closing_익일갭손절예상({antc_prce}/{buy_price})"
    # 09:00 시초가 < 매수가 × 0.95 → 즉시 시장가 매도
    if is_opening_tick and antc_prce < buy_price * 0.95:
        return True, f"closing_익일시초가손절({antc_prce}/{buy_price})"
    return False, ""
```

> 이 함수들은 **순수 판단 함수** — `ws_realtime_tr_str1.py` 규약 준수.
> 실제 호출(주문/상태 관리)은 `ws_realtime_trading.py` 의 ingest_loop, top_rank_loop, `_prepare_closing_buy_orders` 등에서 수행.

---

## 5. 위험·제약·Open Question

### 5-1. 기술적 제약

| # | 항목 | 내용 | 대응 |
|---|------|------|------|
| 1 | 모의 REST 일부 미지원 TR | `frgnmem-pchs-trend` 등은 실전만 가능 가능성 | 시세성 조회는 **실전 계정(`_top_client`)** 유지, 매매 TR만 모의 |
| 2 | 모의 WSS 체결통보 | approval_key 실전/모의 독립 → 단일 WSS 세션으로 양쪽 구독 불가 | 1단계: 폴링(`_inquire_psbl_rvsecncl` 주기 호출) / 2단계: 모의 WSS 별도 세션 |
| 3 | 하루 주문 한도 | 모의는 건수/금액 한도 엄격 | 1종목 1주부터 테스트, 한도 모니터링 로그 추가 |
| 4 | 종목 가용성 차이 | 모의에서 거부되는 종목 존재 (ETN, 관리, 거래정지 등) | 주문 실패 시 `rt_cd != 0` + msg 파싱 → 상세 로깅 |
| 5 | 상태 파일 오염 | 실전/모의 동시 운영 시 같은 파일에 기록 | `_paper_` 접미로 네임스페이스 완전 분리 |
| 6 | 텔레그램 알림 혼동 | 실전/모의 알림이 동일 채널 | 메시지 prefix에 `[MOCK]` 추가 |
| 7 | 잔고/현금 계산 | `exclude_cash`, `INIT_CASH` 실전 기준 → 모의 시드에 맞게 재조정 | `MOCK_TRADE=True` 면 `MOCK_INIT_CASH=1천만원`, `exclude_cash=0` 적용 |
| 8 | `EMERGENCY_29PCT_SELL` 충돌 | 상한가 근접 매수 종목은 29% 긴급매도 대상에서 제외 필요 | `uplimit_buy_state` 보유 종목은 긴급매도 skip |

### 5-2. 운영 이슈

- **데이터 수신 계정과 매매 계정 분리 → 확인용 잔고 조회가 두 계정 모두 필요**
  - a1 실전: 기존 잔고 (보유 종목) → WSS 구독 판단/중복 보유 회피
  - paper 모의: 매매 잔고 → 수량/현금 계산
  - 두 잔고를 혼동하지 않도록 내부 `hold_map` 도 **분리된 딕셔너리** 필요
- **재시작 시 미체결 취소**: `OPEN_BUY_ORDER_CANCEL=True` 이면 재시작 시 전량 취소 — 모의 모드도 동일하게 모의 계정 기준으로 수행 (교차 취소 금지)
- **Top5 동적 구독 / CODE_TEXT 종목**: 실전 WSS로 계속 구독되므로 **시세는 정확**. 단, 모의에서 거부되는 종목이 리스트에 포함되면 매수 시도 → 실패 반복 가능 → 사전 필터 or 거부 횟수 기반 블랙리스트

### 5-3. Open Question (사용자 확인 필요)

1. **모의투자 계정 개설 여부**: KIS 모의투자 계정 앱키/앱시크릿이 아직 없다면 먼저 [한국투자증권 모의투자 서비스](https://securities.koreainvestment.com) 에서 발급 필요. (`appkey`, `appsecret`, `cano` 8자리)
2. **H0STCNI9 구독 방식**: 별도 WSS 세션을 지금 구현할지, 1단계는 폴링으로 갈지? (권장: 1단계 폴링)
3. **VI 매수 전략 (`VI_TRADE_MODE`) 도 모의 라우팅 적용할 것인가**: 기존 `off` 상태인데, 모의에서는 `test_mode` 로 켜서 같이 검증할지
4. **종가매매(`CLOSE_BUY`) 는 모의에서도 활성화할 것인가**: 모의 일일 한도/종목 수 제약 → 동시 운영 시 Phase 1/2/상한가 근접과 주문 수 충돌 가능
5. **기존 Str1 매도(전일 종가매수 체결분)** 는 실전 보유 자산 → **모의에 매도 주문하면 실패**. `MOCK_TRADE=True` 면 Str1 매도 경로는 어떻게 할지:
   - (A) 실전 그대로 유지 (수신은 실전, 매도도 실전) — 과거 보유분 정리용
   - (B) 신규 진입만 모의, 기존 실전 보유분은 그대로 실전 매도
   → **권장: (A/B 공통) 모의 모드에선 전일 실전 매수 보유분 매도 경로는 실전 client 유지, 신규 진입만 모의 client**

---

## 6. 단계별 구현 계획

### Phase 0 — 사전 확인 (코드 변경 0)
- [ ] KIS 모의투자 앱키/앱시크릿 발급 확인 (사용자)
- [ ] 모의 계좌 cano 확인
- [ ] `curl` 로 모의 도메인 `oauth2/tokenP` 직접 호출 → 토큰 발급 확인

### Phase 1 — 라우팅 인프라 (매매 1건이라도 성공시키기)
- [ ] `config.json` `accounts.paper` 추가 (is_paper, base_url_override)
- [ ] `ws_realtime_trading.py` 상단에 `MOCK_TRADE`, `MOCK_INIT_CASH`, `MOCK_MAX_INVEST_PER_STOCK` 플래그
- [ ] `_init_paper_client()`, `_get_trade_client()`, `_get_trade_tr_id()`, `_get_trade_account()` 유틸 추가
- [ ] `_sell_order_cash()` 시그니처에 `tr_id` 파라미터 추가 (기본값 유지, 하위호환)
- [ ] 기존 주문 호출부(7곳, 위 grep 결과) 를 래퍼 함수로 교체 OR `_get_trade_*` 경유
- [ ] 잔고조회/미체결조회 TR_ID도 동일 경유
- [ ] 상태 파일 경로 유틸에 `_paper_` 접미 로직 추가
- [ ] 텔레그램 알림 `_notify()` 에 `[MOCK]` prefix 주입 (MOCK_TRADE=True 시)
- [ ] **검증**: `external_order_input` 으로 1주 수동 매수 → 모의 잔고 반영 확인

### Phase 2 — 체결 감지 (폴링)
- [ ] 매수 주문 후 `ODNO` 기록 → 30초 주기로 `_inquire_psbl_rvsecncl` 폴링 + `inquire-balance` 비교 → 체결/취소/부분체결 감지
- [ ] 체결 완료 시 `uplimit_buy_state` / `opening_buy_state` 파일에 반영

### Phase 3 — 상한가 근접 매수 전략 적용
- [ ] `ws_realtime_tr_str1.py` 에 `uplimit_approach_buy_signal`, `uplimit_approach_exit` 추가
- [ ] `ws_realtime_trading.py` ingest_loop (:7658 근방) 에 `prdy_ctrt 25~28%` 감지 분기 추가
- [ ] Signal Strength 계산에 필요한 실시간 상태 집계 (WSS 호가 잔량 sum, 직전 5분 거래량 avg) 추가
- [ ] 보조 REST 호출 래퍼 (체결강도, 외국인) — **실전 client (`_top_client`) 유지**
- [ ] 일봉 변동성은 장 시작 전 1회 사전 계산 (기존 `kis_1d_Daily_ohlcv_fetch_manager` 활용)
- [ ] `_fire_emergency_sell()` / `EMERGENCY_29PCT_SELL` 에서 uplimit 보유 종목 skip
- [ ] 보유 모니터링 틱마다 `uplimit_approach_exit` 호출 → 매도 조건 충족 시 모의 매도

### Phase 4 — 장 오픈 매수 (Phase 1/2)
- [ ] `opening_buy_phase1_signal`, `opening_buy_phase2_signal`, `opening_buy_exit` 추가
- [ ] `top_rank_loop` 09:00 초반에 Phase 1, 09:05~09:30 에 Phase 2 분기
- [ ] 전일 상한가 종목 리스트 사전 준비 (이미 `closing_buy_state` 에서 일부 가능)

### Phase 5 — 종가매매 폭락 필터
- [ ] `closing_buy_filter_signal`, `closing_buy_next_day_exit` 추가
- [ ] `_prepare_closing_buy_orders` (:2622) 에서 후보 선정 후 필터 통과 종목만 주문
- [ ] 08:30 동시호가에 `closing_buy_next_day_exit` 모니터링 루프 추가

### Phase 6 — 운영 안정화
- [ ] 하루 주문 한도 카운터 + 초과 시 신규 매수 중단
- [ ] 모의/실전 잔고 혼동 방지 로그 prefix (`[MOCK-bal]`, `[REAL-bal]`)
- [ ] 모의 WSS 체결통보(H0STCNI9) 별도 세션 (선택)

---

## 7. 구체적 수정 포인트 요약

| 파일 | 위치 | 변경 |
|------|------|------|
| `config.json` | `users.sywems12.accounts.paper` | 모의 계정 추가 (`is_paper: true`, `base_url_override`) |
| `ws_realtime_trading.py` | :65~90 (사용자 설정) | `MOCK_TRADE`, `MOCK_INIT_CASH`, `MOCK_MAX_INVEST_PER_STOCK` 추가 |
| `ws_realtime_trading.py` | :5003 `_load_main_cfg` 근처 | `_load_paper_cfg`, `_init_paper_client`, `_get_trade_client`, `_get_trade_tr_id`, `_get_trade_account` 추가 |
| `ws_realtime_trading.py` | :547 `_buy_order_cash` | (시그니처는 그대로) 호출부만 교체 |
| `ws_realtime_trading.py` | :674 `_sell_order_cash` | `tr_id` 파라미터 추가 (기본 `TTTC0801U`), 내부 헤더 생성에 사용 |
| `ws_realtime_trading.py` | :705 `_cancel_order_generic` | 동일 — TR_ID 파라미터화 |
| `ws_realtime_trading.py` | :2762, :3012, :3181, :3282, :4250 (매수 호출부) | `_get_trade_client/tr_id/account` 경유로 교체 |
| `ws_realtime_trading.py` | :1711, :1829 (매도 호출부) | 동일 |
| `ws_realtime_trading.py` | :527 `_get_balance_page` 호출부 | TR_ID 파라미터화 (`TTTC8434R` → `_get_trade_tr_id()`) |
| `ws_realtime_trading.py` | ingest_loop (~7658) | `prdy_ctrt 25~28%` 감지 + Phase 1/2 시초가 감지 분기 |
| `ws_realtime_trading.py` | `EMERGENCY_29PCT_SELL` (~7691) | uplimit 보유 종목 skip |
| `ws_realtime_trading.py` | `_prepare_closing_buy_orders` (:2622) | 폭락 방지 필터 호출 |
| `ws_realtime_trading.py` | 상태 파일 경로 유틸 | `_paper_` 접미 분기 |
| `ws_realtime_trading.py` | `_notify` | `[MOCK]` prefix 분기 |
| `ws_realtime_tr_str1.py` | 말미 | `uplimit_approach_buy_signal/_exit`, `opening_buy_phase1/2_signal`, `opening_buy_exit`, `closing_buy_filter_signal`, `closing_buy_next_day_exit` 추가 |

---

## 8. 최초 운영 체크리스트 (Phase 1 완료 시점)

1. `MOCK_TRADE=True`, `VI_TRADE_MODE="off"`, `CLOSE_BUY=False`, `STR1_SELL_ENABLED=False` 로 시작 — 다른 전략 간섭 배제
2. `CODE_TEXT` 에 테스트 종목 2~3개만 (거래량 큰 코스피 대형주 + 변동성 있는 소형주 1)
3. `external_order_input.py` 로 **1주 지정가 매수 수동 실행** → 로그에 `tr_id=VTTC0802U`, `base_url=openapivts...:29443` 확인
4. 모의 HTS/웹에서 잔고 반영 확인 (수초 지연 가능)
5. 수동 매도 1주 → 체결 확인
6. 24시간 운영 후 모의 잔고/실전 잔고가 **독립적으로 움직이는지** 확인
7. Phase 2 (폴링 체결 감지) → Phase 3 (상한가 근접 전략) 순차 활성화

---

## 참고: 근거 코드 위치

| 확인 사항 | 파일:라인 |
|-----------|----------|
| TR_ID T→V 자동 변환 (실전→모의) | `kis_auth_llm.py:458` |
| WSS TR_ID 모의 분기 | `domestic_stock_functions_ws.py:54, :357, :429` |
| 모의 WSS 체결통보 TR_ID | `domestic_stock_functions_ws.py:433` (`H0STCNI9`) |
| 계좌별 클라이언트 초기화 패턴 | `ws_realtime_trading.py:5020, :5056` (`_init_top_client`, `_init_top_client_2`) |
| 매수 주문 함수 | `ws_realtime_trading.py:547` (`_buy_order_cash`) |
| 매도 주문 함수 | `ws_realtime_trading.py:674` (`_sell_order_cash`) |
| 잔고 조회 | `ws_realtime_trading.py:527, :883` |
| Str1 전략 함수 규약 | `ws_realtime_tr_str1.py:18-19` ("순수 전략 판단 함수만") |
| V2 계좌 순회 | `ws_realtime_trading.py:5084` (`_iter_enabled_accounts`) |
| 백테스트 결과 | `docs/01. up_limit_buy_str_260420.md:366` (진입+필터 조합 승률) |

---

## 결론

**두 전략을 `ws_realtime_tr_str1.py` 판단 함수 + `ws_realtime_trading.py` 주문 실행** 구조로 기존 패턴에 맞춰 추가 가능하며, **모의투자 계정으로의 매매 라우팅은 기존 `KisClient` 다중 계정 인프라를 그대로 확장하면 된다.** 가장 리스크가 큰 지점은 (1) 모의 WSS 체결통보 이중 세션과 (2) 상태 파일 네임스페이스 오염이므로 Phase 1 에서 플래그/라우팅/파일 분리를 완성한 후 전략 로직을 얹는 순서를 엄수한다.

다음 단계는 **사용자의 모의 계정 앱키/앱시크릿 확보 + Phase 1 라우팅 구현**.
