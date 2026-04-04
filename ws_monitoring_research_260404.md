# VI/사이드카/써킷브레이크 이벤트 Parquet 저장 방안 연구

**작성일**: 2026-04-04
**대상 파일**: `ws_realtime_trading.py`
**목적**: 백테스트 시 fplt 차트에 장운영 이벤트를 배경색으로 시각화하기 위한 parquet 컬럼 추가

---

## 1. 현재 상태 분석

### 1-1. Parquet 저장 경로 (2개)

| 경로 | 소스 | 위치 | market_event 컬럼 |
|------|------|------|-------------------|
| **WSS** (H0STCNT0/H0STANC0) | 실시간체결/예상체결 | line 8095-8130 | **없음** |
| **REST** (_enqueue_rest_price_row) | FHKST01010100 | line 4901-4922 | **있음** (line 4915-4916) |

**문제**: WSS 경로에 `market_event`, `new_mkop_cls_code` 컬럼이 누락되어 있어 대부분의 체결 데이터에 이벤트 정보가 없음. VI 발동/종료 시각 컬럼은 어디에도 없음.

### 1-2. 기존 메모리 변수

| 변수 | 타입 | 용도 |
|------|------|------|
| `_vi_active_codes` | set[str] | 현재 VI 발동 중인 종목 |
| `_vi_cls_cache` | dict[str, str] | code → 마지막 vi_cls_code (Y/N) |
| `_vi_trigger_count` | dict[str, int] | code → 당일 VI 발동 횟수 |
| `_vi_poll_active_codes` | dict[str, float] | code → VI 발동 epoch (REST) |
| `_vi_stnd_prc` | dict[str, float] | code → VI 기준가 |
| `_vi_exp_sub_ts` | dict[str, float] | code → 예상체결 구독 시작 epoch |
| `_market_event` | dict[str, str] | code → 이벤트명 (종목 단위) |
| `_last_mkop_cls_code` | dict[str, str] | code → mkop_cls_code |
| `_last_mkop_event` | dict[str, str] | code → 이전 mkop_cls_code |

### 1-3. H0STMKO0 장운영정보 필드 구조

**수신 컬럼** (domestic_stock_functions_ws.py line 1139-1151):

| 컬럼 | 한글명 | 길이 | 설명 |
|------|--------|------|------|
| `mksc_shrn_iscd` | 유가증권단축종목코드 | 9 | 종목코드 |
| `trht_yn` | 거래정지여부 | 1 | Y/N |
| `tr_susp_reas_cntt` | 거래정지사유내용 | 100 | |
| `mkop_cls_code` | 장운영구분코드 | 3 | 사이드카/써킷 등 (아래 표) |
| `antc_mkop_cls_code` | 예상장운영구분코드 | 3 | |
| `mrkt_trtm_cls_code` | 임의연장구분코드 | 1 | |
| `divi_app_cls_code` | 동시호가배분처리구분코드 | 2 | |
| `iscd_stat_cls_code` | 종목상태구분코드 | 2 | |
| `vi_cls_code` | VI적용구분코드 | 1 | Y=VI발동, N=미적용 |
| `ovtm_vi_cls_code` | 시간외단일가VI적용구분코드 | 1 | |
| **`EXCH_CLS_CODE`** | **거래소구분코드** | **1** | **마켓 식별 키** |

### 1-4. MKOP_CLS_CODE 주요 값

**사이드카**:
| 코드 | 설명 |
|------|------|
| 187 | 사이드카 매도 발동 |
| 388 | 사이드카 레이트 해제 |
| 397 | 사이드카 매수 발동 |
| 398 | 사이드카 매수 해제 |

**써킷브레이크**:
| 코드 | 설명 |
|------|------|
| 174 | 서킷브레이크 발동 |
| 175 | 서킷브레이크 해제 |
| 182 | 서킷브레이크 장종동시마감 |
| 184 | 서킷브레이크 개시 |
| 185 | 서킷브레이크 해제 |

**기타**: 164=시장임시정지

### 1-5. EXCH_CLS_CODE (거래소구분코드)

H0STMKO0에 포함된 `EXCH_CLS_CODE` 필드(길이 1)로 해당 이벤트가 어느 마켓인지 식별 가능.
현재 코드에서는 이 필드를 **전혀 사용하지 않음** → 파싱 추가 필요.

> **핵심**: 사이드카/써킷브레이크는 마켓 단위 이벤트. 한 종목에서 수신되면 EXCH_CLS_CODE로 마켓(KOSPI/KOSDAQ)을 식별하여 해당 마켓 전체 종목에 일괄 적용.

---

## 2. 설계안

### 2-1. 신규 Parquet 컬럼 (5개)

| 컬럼 | 타입 | 설명 | 예시값 |
|------|------|------|--------|
| `vi_yn` | Utf8 | VI 발동 중 여부 | "Y" / "" |
| `vi_start_ts` | Utf8 | 현재 VI 발동 시각 (ISO) | "2026-04-04 09:05:23" |
| `vi_end_ts` | Utf8 | VI 해제 시각 (ISO) | "2026-04-04 09:07:23" |
| `market_event` | Utf8 | 장운영 이벤트명 | "사이드카발동", "서킷브레이크발동" |
| `new_mkop_cls_code` | Utf8 | mkop 코드 | "187", "174" |

> `market_event`, `new_mkop_cls_code`는 REST 경로에 이미 존재하지만 WSS 경로에 추가 필요.

### 2-2. VI 발동 시각 관리

**원칙**: VI 발동~해제 기간의 **모든 row에 동일한 vi_start_ts 값**을 기록.

```
vi_yn="Y", vi_start_ts="09:05:23", vi_end_ts=""       ← 발동 중
vi_yn="Y", vi_start_ts="09:05:23", vi_end_ts=""       ← 발동 중 (동일값 유지)
vi_yn="",  vi_start_ts="09:05:23", vi_end_ts="09:07:23" ← 해제 후
vi_yn="",  vi_start_ts="",         vi_end_ts=""        ← 이후 클리어
```

**발동 시각 소스**:
- **H0STMKO0 WSS** (vi_cls_code=Y): `datetime.now(KST)` — 실시간 수신 시점
- **FHPST01390000 REST 폴링**: `cntg_vi_hour` 필드 (HHMMSS) → ISO 변환

**해제 시각 소스**:
- **H0STMKO0 WSS** (vi_cls_code=N): `datetime.now(KST)`
- **REST 폴링 만료** (120초): `datetime.now(KST)`

### 2-3. 마켓 전파 (사이드카/써킷브레이크)

```
H0STMKO0 수신 (한 종목)
    ↓
EXCH_CLS_CODE 파싱 → market (KOSPI/KOSDAQ)
    ↓
mkop_cls_code가 사이드카/써킷 발동 코드?
    ├─ Yes → _market_wide_event[market] = 이벤트명
    │        _market_wide_mkop[market] = mkop_cls_code
    │        _market_wide_start_ts[market] = datetime.now(KST) ISO
    └─ 해제 코드? → _market_wide_event.pop(market)
                    _market_wide_mkop.pop(market)
                    _market_wide_end_ts[market] = datetime.now(KST) ISO

ingest 시 (per-code loop):
    market = _code_market_map.get(code, "")
    market_event = _market_wide_event.get(market, "") or _market_event.get(code, "")
```

> **Fallback**: EXCH_CLS_CODE 파싱이 안 될 경우 `symbol_master`의 `market` 컬럼("KOSPI"/"KOSDAQ")으로 `_code_market_map` 구축 (초기화 시).

### 2-4. 사이드카/써킷브레이크 발동 시각 확인

**문제**: H0STMKO0에는 발동 시각 필드가 없음 (VI의 `cntg_vi_hour` 같은 것 없음).

**해결**: H0STMKO0 수신 시점의 `datetime.now(KST)`를 발동 시각으로 기록.
- 사이드카: 프로그램매매 5분간 중단
- 써킷브레이크: 20분 거래정지 + 10분 동시호가 (주문접수)

**써킷브레이크 기간 중 예상체결가 조회**:
- 발동 시각 + 20분 이후 10분간 동시호가 → 이 기간에 예상체결가(exp_ccnl_krx) 구독 필요
- VI와 유사한 패턴: 발동 → 예상체결가 구독, 해제(mkop=175/185) → 실시간체결가 전환

---

## 3. 수정 포인트 (ws_realtime_trading.py)

### 3-1. 신규 메모리 변수 추가 (~line 4578)

```python
# ── VI/마켓 이벤트 시각 추적 ──
_vi_start_ts: dict[str, str] = {}           # code → VI 발동 ISO timestamp
_vi_end_ts: dict[str, str] = {}             # code → VI 해제 ISO timestamp
_code_market_map: dict[str, str] = {}       # code → "KOSPI"/"KOSDAQ" (symbol_master fallback)
_market_wide_event: dict[str, str] = {}     # "KOSPI"/"KOSDAQ" → 이벤트명
_market_wide_mkop: dict[str, str] = {}      # "KOSPI"/"KOSDAQ" → mkop_cls_code
_market_wide_start_ts: dict[str, str] = {}  # "KOSPI"/"KOSDAQ" → 발동 ISO timestamp
```

### 3-2. `_code_market_map` 초기화 (~line 3458)

```python
# symbol_master에서 market 컬럼으로 code→market 매핑 구축
try:
    sdf = load_symbol_master()
    sdf["code"] = sdf["code"].astype(str).str.zfill(6)
    _code_market_map = dict(zip(sdf["code"], sdf["market"].astype(str)))
except Exception:
    pass
```

### 3-3. `_on_market_status_krx` 수정 (~line 5343)

**EXCH_CLS_CODE 파싱 추가**:
```python
exch_cls = str(row.get("EXCH_CLS_CODE", "")).strip()
# exch_cls 값으로 마켓 결정 (값 확인 필요, 미확인 시 _code_market_map fallback)
market = _exch_cls_to_market(exch_cls) or _code_market_map.get(code, "")
```

**사이드카/써킷 발동 시 마켓 전파** (line 5370-5375 이후):
```python
if mkop in ("187", "397", "174", "184", "164"):  # 발동 코드
    _market_wide_event[market] = event_name
    _market_wide_mkop[market] = mkop
    _market_wide_start_ts[market] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
if mkop in ("175", "185", "388", "398"):  # 해제 코드
    _market_wide_event.pop(market, None)
    _market_wide_mkop.pop(market, None)
    _market_wide_start_ts.pop(market, None)
```

**VI 발동/해제 시각 기록** (line 5407-5420 이후):
```python
if vi_cls == "Y":  # 발동
    _vi_start_ts[code] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    _vi_end_ts.pop(code, None)
elif vi_cls == "N":  # 해제
    _vi_end_ts[code] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
```

### 3-4. `_vi_poll_check` 수정 (~line 5100)

**신규 VI 감지 시** (cntg_vi_hour → vi_start_ts):
```python
vi_time_iso = datetime.fromtimestamp(vi_ts, tz=KST).strftime("%Y-%m-%d %H:%M:%S")
_vi_start_ts[code] = vi_time_iso
_vi_end_ts.pop(code, None)
```

**VI 해제/만료 시**:
```python
_vi_end_ts[code] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
```

### 3-5. WSS ingest 컬럼 추가 (~line 8103 per-code loop)

기존 indicator 계산 후, `chunks.append(df_code)` 직전에:
```python
market = _code_market_map.get(code, "")
mkt_evt = _market_wide_event.get(market, "") or _market_event.get(code, "")
mkt_mkop = _market_wide_mkop.get(market, "") or _last_mkop_cls_code.get(code, "")

df_code = df_code.with_columns([
    pl.lit("Y" if code in _vi_active_codes else "").alias("vi_yn"),
    pl.lit(_vi_start_ts.get(code, "")).alias("vi_start_ts"),
    pl.lit(_vi_end_ts.get(code, "")).alias("vi_end_ts"),
    pl.lit(mkt_evt).alias("market_event"),
    pl.lit(mkt_mkop).alias("new_mkop_cls_code"),
])
```

### 3-6. REST ingest 컬럼 추가 (~line 4901)

`row` dict에 추가:
```python
market = _code_market_map.get(code, "")
row.update({
    "vi_yn": "Y" if code in _vi_active_codes else "",
    "vi_start_ts": _vi_start_ts.get(code, ""),
    "vi_end_ts": _vi_end_ts.get(code, ""),
    "market_event": _market_wide_event.get(market, "") or _market_event.get(code, ""),
    "new_mkop_cls_code": _market_wide_mkop.get(market, "") or _last_mkop_cls_code.get(code, ""),
})
```

### 3-7. Parquet 머지 호환성

`ws_merge_wss_parts.py`는 `pl.concat(..., how="diagonal_relaxed")`로 스키마 불일치 자동 처리 → **변경 불필요**. 신규 컬럼은 기존 part 파일에서 null로 채워짐.

---

## 4. 로컬 fplt 시각화 가이드

### 4-1. 신규 컬럼 스키마

parquet 로드 후 사용 가능한 이벤트 컬럼:

| 컬럼 | 의미 | 시각화 용도 |
|------|------|------------|
| `vi_yn` | "Y"이면 VI 발동 중 | 배경색 트리거 |
| `vi_start_ts` | VI 발동 시각 | 구간 시작점 |
| `vi_end_ts` | VI 해제 시각 | 구간 종료점 |
| `market_event` | 사이드카/써킷 이벤트명 | 배경색 트리거 + 라벨 |
| `new_mkop_cls_code` | mkop 코드 | 이벤트 종류 판별 |

### 4-2. 배경색 표시 규칙

| 이벤트 | 조건 | 배경색 |
|--------|------|--------|
| **VI 발동** | `vi_yn == "Y"` 구간 | 반투명 보라색 (`purple`, alpha=0.15) |
| **써킷브레이크** | `market_event`에 "서킷브레이크" 포함 구간 | 반투명 적색 (`red`, alpha=0.15) |
| **사이드카** | `market_event`에 "사이드카" 포함 구간 | 반투명 적색 (`red`, alpha=0.15) |

### 4-3. fplt 구현 예시

```python
import finplot as fplt
import polars as pl

df = pl.read_parquet("data/wss_data/260404_wss_data.parquet")
df_code = df.filter(pl.col("mksc_shrn_iscd") == "005930")

# VI 구간 추출 (vi_yn == "Y" 연속 구간)
vi_mask = df_code["vi_yn"] == "Y"
vi_groups = vi_mask.ne(vi_mask.shift(1)).cum_sum()
for _, grp in df_code.filter(vi_mask).group_by(vi_groups):
    t0 = grp["recv_ts"][0]
    t1 = grp["recv_ts"][-1]
    fplt.add_band(t0, t1, ax, color='#800080', alpha=0.15)  # 반투명 보라

# 써킷/사이드카 구간 추출
mkt_mask = df_code["market_event"].str.len_chars() > 0
for _, grp in df_code.filter(mkt_mask).group_by(...):
    t0 = grp["recv_ts"][0]
    t1 = grp["recv_ts"][-1]
    label = grp["market_event"][0]
    fplt.add_band(t0, t1, ax, color='#FF0000', alpha=0.15)  # 반투명 적색
    fplt.add_text((t0, max_price), label, color='red', anchor=(0,0))
```

> **참고**: `fplt.add_band()` 사용 불가 시 matplotlib의 `ax.axvspan(t0, t1, color=..., alpha=...)` 사용.

---

## 5. 미확인 사항

| 항목 | 상태 | 비고 |
|------|------|------|
| `EXCH_CLS_CODE` 값 매핑 | 미확인 | 실제 수신 데이터로 값 확인 필요 (1=KOSPI? 2=KOSDAQ?) |
| 써킷브레이크 시 예상체결가 구독 | 미구현 | 20분 정지 후 10분 동시호가 기간 예상체결가 조회 로직 |
| VI 해제 후 vi_start_ts 클리어 시점 | 설계 필요 | 해제 직후? 다음 VI까지? (현재안: 해제 후에도 유지, 새 VI 발동 시 덮어씀) |

---

## 6. 요약

1. **WSS 경로에 이벤트 컬럼 5개 추가** (vi_yn, vi_start_ts, vi_end_ts, market_event, new_mkop_cls_code)
2. **VI 기간 전체 row에 동일한 vi_start_ts** 유지
3. **EXCH_CLS_CODE로 마켓 식별** → 사이드카/써킷브레이크를 해당 마켓 전체 종목에 일괄 적용
4. **발동 시각**: VI=cntg_vi_hour(REST) or recv_ts(WSS), 사이드카/써킷=recv_ts
5. **로컬 fplt**: vi_yn=="Y" → 반투명 보라, market_event 있는 구간 → 반투명 적색
