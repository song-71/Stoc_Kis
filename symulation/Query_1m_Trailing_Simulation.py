"""
1m 데이터 기반 트레일링 시뮬레이션 (polars + DuckDB 사용)

- code_text에 입력한 종목에 대해:
  1) 1d 통합 parquet에서 전일종가(pdy_close), atr3를 조회
  2) 1m 데이터를 조회
  3) pdy_close, atr3를 추가하고 pdy_ctrt 계산
  4) trail_v, action, ret, tot_ret 컬럼을 기록
  5) 전략별 CSV 저장 (simul_str1.csv, simul_result.csv)
* hl_diff : 당일 변동폭(가격 차이) : high - low
* hl_dif_o_rt : 당일 변동폭을 시가 대비 비율로 환산한 값입니다. : hl_diff / open

전략 옵션:
  - TRAIL_STEPS: [0.02, 0.03, ..., 0.29]
  - STOP_LOSS: 0.0
  - pdy_close_buy: True (True면 전일 종가 매수, False면 시가>=전일 종가일 때 시가 매수)
  - NEG_TDY_RET_STREAK_STOP: True (True면 tdy_ret<0 연속 5회 이상 시 거래 중지)
"""

import argparse
import sys, time
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, List, Tuple

import duckdb
import numpy as np
import pandas as pd
from math import ceil
from numba import njit  # type: ignore

sys.path.append(str(Path(__file__).resolve().parents[1]))
from kis_utils import load_symbol_master, print_table

# ===== 시뮬레이션 옵션 (내부 설정) =====
default_limit = 0  # 0이면 출력제한 rows 없음(당일자 모든 데이터 rows 출력)
show_parquet_head = False  # True면 parquet 파일 원본 head/컬럼 출력
default_save_csv = True  # True면 CSV로 저장

STRATEGY_NAME = "str1"
STRATEGY_ID = 2 if STRATEGY_NAME == "str2" else 1
pdy_close_buy = 0            # 전일 종가 매수 여부 설정, True면 전일 종가 매수, False면 시가>=전일 종가일 때 시가 매수
# TRAIL_STEPS = [0.02, 0.03, 0.04, 0.05, 0.10, 0.15, 0.20, 0.25, 0.28, 0.29]
TRAIL_STEPS = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29]
STOP_LOSS = 0.0
K = 0.3
K2 = 0.1
SLIPPAGE = 0.005  
NEG_TDY_RET_STREAK_STOP = False  # True : 전일종가 이하(tdy_ret<0)가 연속 N회 이상 시 거래 중지
NEG_TDY_RET_STREAK_LIMIT = 10    # tdy_ret<0 연속 횟수 기준
max_loss_rt = 5  # 연속 손실 한계 횟수(이 횟수가 도달하면 그 종목은 거래 종료)
"""
전략설명
str1: 전일 종가 매수 → 매수가 이탈 시 즉시 매도 → min_low + atr3*K 돌파 재매수 → 트레일링 적용/이탈 매도 
      → 15:08 종가 매도 순서로 동작하며, ret의 평균값을 tot_ret로 집계합니다
  1) 전일 종가(pdy_close)에 매수
  2) 현재 저가가 매수가보다 낮으면 매수가에 매도
  3) 매도 후 min_low + atr3*K(0.3) 돌파 시 재매수
  4) 고가가 buy_pr*(1+trail+stop_loss) 이상이면 트레일링 적용
  5) 저가가 트레일링 값 아래로 하락하면 매도
  6) 15:08 종가(close)에 매도
  7) 매도 체결가는 슬리피지(SLIPPAGE)만큼 보정
str2: 0931분의 시가에 매수해서 이후는 str1을 전략을 적용함
"""

default_date = "20260220"  # 예: "20260123" 또는 "2026-01-23", 비우면 당일

#260220 수신대상 종목(업종) 목록 : 26개
CODE_TEXT = """
유투바이오
상상인증권
SK증권우
에쎈테크
SK증권
아스플로
코리아에셋투자증권
와토스코리아
미래에셋증권우
한화투자증권
광동제약
현대차증권
한화투자증권우
선익시스템
부광약품
한화솔루션
티씨머티리얼즈
유진투자증권
동아엘텍
엘앤씨바이오
마이크로컨텍솔
링크솔루션
포바이포
피에스케이홀딩스
다올투자증권
NHN벅스
제일바이오
케어젠

"""

# 결과 파일
BASE_DIR = Path(__file__).resolve().parent
DETAIL_OUT = BASE_DIR / f"simul_{STRATEGY_NAME}.csv"
SUMMARY_OUT = BASE_DIR / "simul_result.csv"


def _normalize_biz_date(text: str | None) -> str:
    if not text:
        return datetime.now().strftime("%Y%m%d")
    cleaned = str(text).strip()
    if "-" in cleaned:
        return datetime.strptime(cleaned, "%Y-%m-%d").strftime("%Y%m%d")
    if len(cleaned) == 8 and cleaned.isdigit():
        return cleaned
    return datetime.now().strftime("%Y%m%d")


def _resolve_1m_parquet_path(biz_date: str) -> str:
    out_dir = BASE_DIR.parent / "data" / "1m_data"
    if not out_dir.exists():
        raise FileNotFoundError(f"1m_data 폴더가 없습니다: {out_dir}")
    matches = sorted(out_dir.glob(f"{biz_date}*_1m_chart_DB_parquet.parquet"))
    if not matches:
        raise FileNotFoundError(f"parquet 파일이 없습니다: {out_dir}/{biz_date}*_1m_chart_DB_parquet.parquet")
    return str(matches[0])


def _resolve_1d_parquet_path() -> str:
    path = BASE_DIR.parent / "data" / "1d_data" / "kis_1d_unified_parquet_DB.parquet"
    if not path.exists():
        raise FileNotFoundError(f"1d parquet 파일이 없습니다: {path}")
    return str(path)


def _build_name_code_map() -> Tuple[Dict[str, str], Dict[str, str]]:
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"]))
        code_to_name = dict(zip(sdf["code"], sdf["name"].astype(str)))
        return name_to_code, code_to_name
    except Exception:
        return {}, {}


def _parse_codes(text: str, name_to_code: Dict[str, str]) -> List[str]:
    codes = []
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        if raw.isdigit():
            codes.append(raw.zfill(6))
            continue
        code = name_to_code.get(raw)
        if code:
            codes.append(code)
    return codes


def _load_1d_info(con: duckdb.DuckDBPyConnection, date_str: str, codes: List[str]) -> Dict[str, Dict[str, float]]:
    if not codes:
        return {}
    codes_sql = ",".join(f"'{c}'" for c in codes)
    query = f"""
    SELECT symbol, name, pdy_close, atr3, pdy_ctrt, tdy_ctrt, open, high, low, close
    FROM read_parquet('{_resolve_1d_parquet_path()}')
    WHERE date = '{date_str}' AND symbol IN ({codes_sql})
    """
    df = con.execute(query).df()
    info = {}
    for _, row in df.iterrows():
        pdy_close = float(row.get("pdy_close", 0) or 0)
        pdy_ctrt = float(row.get("pdy_ctrt", 0) or 0)
        info[str(row["symbol"])] = {
            "name": str(row.get("name", "") or ""),
            "pdy_close": pdy_close,
            "atr3": float(row.get("atr3", 0) or 0),
            "pdy_ctrt": pdy_ctrt,
            "tdy_ctrt": float(row.get("tdy_ctrt", 0) or 0),
            "open": float(row.get("open", 0) or 0),
            "high": float(row.get("high", 0) or 0),
            "low": float(row.get("low", 0) or 0),
            "close": float(row.get("close", 0) or 0),
        }
    return info


def _format_optional(value: float, fmt: str) -> str:
    if value == value:
        return fmt.format(value)
    return ""


def _format_hhmmss(value: int) -> str:
    if not value:
        return ""
    s = f"{int(value):06d}"
    return f"{s[0:2]}:{s[2:4]}:{s[4:6]}"


@njit(cache=True)
def _round_up_tick(price: float) -> float:
    if price <= 0:
        return price
    if price < 1000:
        step = 1
    elif price < 5000:
        step = 5
    elif price < 10000:
        step = 10
    elif price < 50000:
        step = 50
    elif price < 100000:
        step = 100
    elif price < 500000:
        step = 500
    else:
        step = 1000
    return ceil(price / step) * step


@njit(cache=True)
def _simulate_core_nb(
    open_arr: np.ndarray,
    high_arr: np.ndarray,
    low_arr: np.ndarray,
    close_arr: np.ndarray,
    time_arr: np.ndarray,
    volume_arr: np.ndarray,
    tdy_ret_arr: np.ndarray,
    pdy_close: float,
    atr3: float,
    trail_steps: np.ndarray,
    slippage: float,
    k: float,
    k2: float,
    strategy_id: int,
    pdy_close_buy: bool,
    neg_tdy_ret_streak_stop: int,
    max_loss_rt: int,
):
    n = len(close_arr)
    buy_pr_arr = np.zeros(n)
    sell_pr_arr = np.zeros(n)
    trail_stop_r_arr = np.empty(n)
    trail_stop_arr = np.empty(n)
    ret_arr = np.empty(n)
    tot_ret_arr = np.zeros(n)
    mark_arr = np.zeros(n, dtype=np.int8)
    dy_low_arr = np.empty(n)
    new_low_arr = np.empty(n)
    atr3_arr = np.empty(n)
    rk_arr = np.empty(n)
    rk2_arr = np.empty(n)
    breakout_arr = np.empty(n)
    stop_time_arr = np.empty(n)
    for i in range(n):
        trail_stop_r_arr[i] = np.nan
        trail_stop_arr[i] = np.nan
        ret_arr[i] = np.nan
        dy_low_arr[i] = np.nan
        new_low_arr[i] = np.nan
        atr3_arr[i] = np.nan
        rk_arr[i] = np.nan
        rk2_arr[i] = np.nan
        breakout_arr[i] = np.nan
        stop_time_arr[i] = 0

    buy_pr = 0.0
    in_position = False
    pending_buy = False
    buy_idx = -1
    if strategy_id == 2:
        first_idx = -1
        for i in range(n):
            if time_arr[i] >= 93100 and volume_arr[i] > 0:
                first_idx = i
                break
        if first_idx >= 0:
            buy_pr = open_arr[first_idx]
            buy_idx = first_idx
            pending_buy = True
    elif pdy_close_buy:
        if pdy_close > 0:
            buy_pr = pdy_close
            buy_idx = 0
            pending_buy = True
    else:
        if pdy_close > 0:
            first_idx = -1
            for i in range(n):
                if time_arr[i] >= 90000 and volume_arr[i] > 0:
                    first_idx = i
                    break
            if first_idx >= 0 and open_arr[first_idx] >= pdy_close:
                buy_pr = open_arr[first_idx]
                buy_idx = first_idx
                pending_buy = True

    peak_price = high_arr[0]
    trail_stop_price = 0.0
    trail_step_applied = 0.0
    sell_count = 0
    total_ret = 1.0
    current_tot_ret = 0.0
    dy_low = np.nan
    dy_high = np.nan
    new_low = np.nan

    neg_streak = 0
    stop_trading = False
    stop_time = 0
    loss_streak = 0
    for i in range(n):
        price = close_arr[i]
        high_price = high_arr[i]
        low_price = low_arr[i]
        open_price = open_arr[i]
        tot_ret_arr[i] = current_tot_ret
        stop_time_arr[i] = stop_time
        if stop_trading:
            dy_low_arr[i] = np.nan
            new_low_arr[i] = np.nan
            atr3_arr[i] = np.nan
            rk_arr[i] = np.nan
            rk2_arr[i] = np.nan
            breakout_arr[i] = np.nan
            continue
        if not stop_trading and max_loss_rt > 0 and loss_streak >= max_loss_rt:
            stop_trading = True
        if strategy_id == 2 and time_arr[i] < 93100:
            continue
        if time_arr[i] >= 90000 and volume_arr[i] > 0:
            if dy_low == dy_low:
                if low_price < dy_low:
                    dy_low = low_price
            else:
                dy_low = low_price
            if dy_high == dy_high:
                if high_price > dy_high:
                    dy_high = high_price
            else:
                dy_high = high_price
            if new_low != new_low:
                new_low = low_price
        # 검증용 컬럼은 포지션/중단 여부와 무관하게 기록
        atr3_eff = atr3
        if atr3_eff <= 0 and dy_low == dy_low and dy_high == dy_high:
            atr3_eff = dy_high - dy_low
        rk1 = atr3_eff * k
        rk2 = atr3_eff * k * k2
        atr3_arr[i] = atr3_eff
        breakout1 = dy_low + rk1 if dy_low == dy_low else np.nan
        breakout2 = new_low + rk2 if new_low == new_low else np.nan
        dy_low_arr[i] = dy_low
        new_low_arr[i] = new_low
        rk_arr[i] = rk1
        rk2_arr[i] = rk2
        if breakout1 == breakout1 and breakout2 == breakout2:
            breakout_arr[i] = breakout1 if breakout1 >= breakout2 else breakout2
        if in_position and buy_idx >= 0 and i > buy_idx:
            breakout_arr[i] = np.nan
        if pending_buy and i == buy_idx and buy_pr > 0:
            in_position = True
            pending_buy = False
            buy_pr_arr[buy_idx] = buy_pr
            mark_arr[buy_idx] = 1
            peak_price = high_price
        if volume_arr[i] > 0 and open_price == high_price == low_price == price:
            if in_position and buy_pr > 0 and buy_idx >= 0:
                sell_pr = price * (1 - slippage)
                trade_ret = (sell_pr / buy_pr - 1) if buy_pr > 0 else 0.0
                sell_pr_arr[i] = sell_pr
                mark_arr[i] = -1
                ret_arr[i] = trade_ret
                total_ret *= (1 + trade_ret)
                current_tot_ret = total_ret - 1
                tot_ret_arr[i] = current_tot_ret
                sell_count += 1
                if trade_ret < 0:
                    loss_streak += 1
                else:
                    loss_streak = 0
                in_position = False
                buy_pr = 0.0
                buy_idx = -1
                trail_stop_price = 0.0
                trail_step_applied = 0.0
                new_low = low_price
            stop_trading = True
        if neg_tdy_ret_streak_stop > 0:
            if tdy_ret_arr[i] < 0:
                neg_streak += 1
            else:
                neg_streak = 0
            if neg_streak >= neg_tdy_ret_streak_stop and not stop_trading:
                if in_position and buy_pr > 0 and buy_idx >= 0:
                    sell_pr = price * (1 - slippage)
                    trade_ret = (sell_pr / buy_pr - 1) if buy_pr > 0 else 0.0
                    sell_pr_arr[i] = sell_pr
                    mark_arr[i] = -1
                    ret_arr[i] = trade_ret
                    total_ret *= (1 + trade_ret)
                    current_tot_ret = total_ret - 1
                    tot_ret_arr[i] = current_tot_ret
                    sell_count += 1
                    if trade_ret < 0:
                        loss_streak += 1
                    else:
                        loss_streak = 0
                    in_position = False
                    buy_pr = 0.0
                    trail_stop_price = 0.0
                    trail_step_applied = 0.0
                stop_trading = True
        if stop_trading:
            if stop_time == 0:
                stop_time = time_arr[i]
            stop_time_arr[i] = stop_time
            tot_ret_arr[i] = current_tot_ret
            continue

        if in_position:
            if buy_pr <= 0 or buy_idx < 0:
                in_position = False
                continue

            if high_price > peak_price:
                peak_price = high_price

            peak_ret = (peak_price / buy_pr - 1) if buy_pr > 0 else 0.0
            new_step = 0.0
            for j in range(trail_steps.size):
                if peak_ret >= trail_steps[j]:
                    new_step = trail_steps[j]
            if new_step > trail_step_applied:
                trail_step_applied = new_step
                trail_stop_price = buy_pr * (1 + trail_step_applied)

            if trail_stop_price > 0:
                trail_stop_r_arr[i] = trail_step_applied
                trail_stop_arr[i] = trail_stop_price

            # 매수 봉 포함: 트레일스탑 이탈 시 트레일가 매도
            if (
                trail_stop_price > 0
                and low_price <= trail_stop_price
                and open_price != price
            ):
                if open_price == low_price and trail_stop_price > low_price:
                    sell_pr = low_price * (1 - slippage)
                else:
                    sell_pr = trail_stop_price * (1 - slippage)
                trade_ret = (sell_pr / buy_pr - 1) if buy_pr > 0 else 0.0
                sell_pr_arr[i] = sell_pr
                mark_arr[i] = -1
                ret_arr[i] = trade_ret
                total_ret *= (1 + trade_ret)
                current_tot_ret = total_ret - 1
                tot_ret_arr[i] = current_tot_ret
                sell_count += 1
                if trade_ret < 0:
                    loss_streak += 1
                else:
                    loss_streak = 0
                in_position = False
                buy_pr = 0.0
                buy_idx = -1
                trail_stop_price = 0.0
                trail_step_applied = 0.0
                min_low = low_price
                if max_loss_rt > 0 and loss_streak >= max_loss_rt:
                    stop_trading = True
                continue
            if buy_idx == i and price < buy_pr:
                sell_pr = buy_pr * (1 - slippage)
                trade_ret = (sell_pr / buy_pr - 1) if buy_pr > 0 else 0.0
                sell_pr_arr[i] = sell_pr
                mark_arr[i] = -1
                ret_arr[i] = trade_ret
                total_ret *= (1 + trade_ret)
                current_tot_ret = total_ret - 1
                tot_ret_arr[i] = current_tot_ret
                sell_count += 1
                if trade_ret < 0:
                    loss_streak += 1
                else:
                    loss_streak = 0
                in_position = False
                buy_pr = 0.0
                buy_idx = -1
                trail_stop_price = 0.0
                trail_step_applied = 0.0
                min_low = low_price
                if max_loss_rt > 0 and loss_streak >= max_loss_rt:
                    stop_trading = True
                continue
            if buy_idx != i and low_price < buy_pr:
                sell_pr = buy_pr * (1 - slippage)
                trade_ret = (sell_pr / buy_pr - 1) if buy_pr > 0 else 0.0
                sell_pr_arr[i] = sell_pr
                mark_arr[i] = -1
                ret_arr[i] = trade_ret
                total_ret *= (1 + trade_ret)
                current_tot_ret = total_ret - 1
                tot_ret_arr[i] = current_tot_ret
                sell_count += 1
                if trade_ret < 0:
                    loss_streak += 1
                else:
                    loss_streak = 0
                in_position = False
                buy_pr = 0.0
                buy_idx = -1
                trail_stop_price = 0.0
                trail_step_applied = 0.0
                new_low = low_price
                if max_loss_rt > 0 and loss_streak >= max_loss_rt:
                    stop_trading = True
                continue

        if not in_position:
            if breakout1 == breakout1 and breakout2 == breakout2:
                entry = breakout1 if breakout1 >= breakout2 else breakout2
                if entry > 0 and breakout1 > 0 and breakout2 > 0 and high_price >= breakout1 and high_price >= breakout2:
                    buy_pr = _round_up_tick(entry)
                    in_position = True
                    peak_price = high_price
                    trail_stop_price = 0.0
                    trail_step_applied = 0.0
                    buy_pr_arr[i] = buy_pr
                    mark_arr[i] = 1
                    buy_idx = i
                    peak_ret = (peak_price / buy_pr - 1) if buy_pr > 0 else 0.0
                    new_step = 0.0
                    for j in range(trail_steps.size):
                        if peak_ret >= trail_steps[j]:
                            new_step = trail_steps[j]
                    if new_step > 0:
                        trail_step_applied = new_step
                        trail_stop_price = buy_pr * (1 + trail_step_applied)
                        trail_stop_r_arr[i] = trail_step_applied
                        trail_stop_arr[i] = trail_stop_price

                    if trail_stop_price > 0 and price < trail_stop_price and open_arr[i] != price:
                        sell_pr = trail_stop_price * (1 - slippage)
                    elif price < buy_pr:
                        sell_pr = buy_pr * (1 - slippage)
                    else:
                        continue

                    trade_ret = (sell_pr / buy_pr - 1) if buy_pr > 0 else 0.0
                    sell_pr_arr[i] = sell_pr
                    mark_arr[i] = -1
                    ret_arr[i] = trade_ret
                    total_ret *= (1 + trade_ret)
                    current_tot_ret = total_ret - 1
                    tot_ret_arr[i] = current_tot_ret
                    sell_count += 1
                    if trade_ret < 0:
                        loss_streak += 1
                    else:
                        loss_streak = 0
                    in_position = False
                    buy_pr = 0.0
                    buy_idx = -1
                    trail_stop_price = 0.0
                    trail_step_applied = 0.0
                    new_low = low_price
                    if max_loss_rt > 0 and loss_streak >= max_loss_rt:
                        stop_trading = True
                    continue

    if in_position and buy_pr > 0 and buy_idx >= 0:
        close_idx = n - 1
        for i in range(n):
            if time_arr[i] >= 150800:
                close_idx = i
                break
        sell_pr = close_arr[close_idx] * (1 - slippage)
        trade_ret = (sell_pr / buy_pr - 1) if buy_pr > 0 else 0.0
        sell_pr_arr[close_idx] = sell_pr
        mark_arr[close_idx] = -1
        ret_arr[close_idx] = trade_ret
        total_ret *= (1 + trade_ret)
        current_tot_ret = total_ret - 1
        tot_ret_arr[close_idx] = current_tot_ret
        sell_count += 1
        if trade_ret < 0:
            loss_streak += 1
        else:
            loss_streak = 0

    tot_ret = total_ret - 1 if sell_count > 0 else 0.0
    return (
        buy_pr_arr,
        sell_pr_arr,
        trail_stop_r_arr,
        trail_stop_arr,
        ret_arr,
        tot_ret_arr,
        mark_arr,
        dy_low_arr,
        new_low_arr,
        atr3_arr,
        rk_arr,
        rk2_arr,
        breakout_arr,
        stop_time_arr,
        sell_count,
        tot_ret,
    )


def _simulate_strategy(
    df: pd.DataFrame,
    pdy_close: float,
    pdy_ctrt: float,
    atr3: float,
    pdy_close_buy: bool,
    trail_steps: List[float],
    neg_tdy_ret_streak_stop: bool,
    max_loss_rt: int,
) -> Tuple[pd.DataFrame, int, float]:
    if df.empty:
        return df, 0, 0.0

    df = df.copy()
    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["pdy_close"] = pdy_close
    df["pdy_ctrt"] = pdy_ctrt
    df["atr3"] = atr3
    if "close" in df.columns and pdy_close > 0:
        df["tdy_ret"] = (df["close"].astype(float) - pdy_close) / pdy_close
    else:
        df["tdy_ret"] = 0.0

    open_arr = df["open"].to_numpy(dtype=np.float64)
    high_arr = df["high"].to_numpy(dtype=np.float64)
    low_arr = df["low"].to_numpy(dtype=np.float64)
    close_arr = df["close"].to_numpy(dtype=np.float64)
    if "time" in df.columns:
        time_series = df["time"].astype(str)
        time_series = time_series.str.replace(":", "", regex=False).str.replace(".", "", regex=False)
        time_series = time_series.str.strip().str.zfill(6)
        time_arr = (
            pd.to_numeric(time_series, errors="coerce")
            .fillna(0)
            .astype(int)
            .to_numpy(dtype=np.int64)
        )
    else:
        time_arr = np.zeros(len(df), dtype=np.int64)
    tdy_ret_arr = df["tdy_ret"].to_numpy(dtype=np.float64)
    if "volume" in df.columns:
        volume_arr = pd.to_numeric(df["volume"], errors="coerce").fillna(0).to_numpy(dtype=np.float64)
    else:
        volume_arr = np.zeros(len(df), dtype=np.float64)

    trail_steps_arr = np.array(sorted(trail_steps), dtype=np.float64)
    streak_limit = int(NEG_TDY_RET_STREAK_LIMIT) if neg_tdy_ret_streak_stop else 0
    (
        buy_pr_arr,
        sell_pr_arr,
        trail_stop_r_arr,
        trail_stop_arr,
        ret_arr,
        tot_ret_arr,
        mark_arr,
        dy_low_arr,
        new_low_arr,
        atr3_arr,
        rk_arr,
        rk2_arr,
        breakout_arr,
        stop_time_arr,
        sell_count,
        tot_ret,
    ) = _simulate_core_nb(
        open_arr,
        high_arr,
        low_arr,
        close_arr,
        time_arr,
        volume_arr,
        tdy_ret_arr,
        pdy_close,
        atr3,
        trail_steps_arr,
        SLIPPAGE,
        K,
        K2,
        STRATEGY_ID,
        pdy_close_buy,
        streak_limit,
        int(max_loss_rt),
    )

    df["buy"] = np.where(buy_pr_arr > 0, np.vectorize(lambda v: f"{v:.0f}")(buy_pr_arr), "")
    df["sell"] = np.where(sell_pr_arr > 0, np.vectorize(lambda v: f"{v:.0f}")(sell_pr_arr), "")
    df["trail_r"] = [_format_optional(v, "{:.2f}") for v in trail_stop_r_arr]
    df["trail_v"] = [_format_optional(v, "{:.0f}") for v in trail_stop_arr]
    df["action"] = np.where(mark_arr == 1, "BUY", np.where(mark_arr == -1, "SELL", ""))
    df["ret"] = [_format_optional(v, "{:.4f}") for v in ret_arr]
    df["tot_ret"] = [f"{v:.4f}" for v in tot_ret_arr]
    df["dy_low"] = [_format_optional(v, "{:.0f}") for v in dy_low_arr]
    df["new_low"] = [_format_optional(v, "{:.0f}") for v in new_low_arr]
    df["atr3"] = [_format_optional(v, "{:.4f}") for v in atr3_arr]
    df["Rk"] = [_format_optional(v, "{:.4f}") for v in rk_arr]
    df["Rk2"] = [_format_optional(v, "{:.4f}") for v in rk2_arr]
    df["break_pr"] = [_format_optional(v, "{:.0f}") for v in breakout_arr]
    df["stop_time"] = [_format_hhmmss(v) for v in stop_time_arr]
    for old_col in ("trail_stop_r", "trail_stop", "BreakOut", "min_low"):
        if old_col in df.columns:
            df.drop(columns=[old_col], inplace=True)

    # 컬럼 순서 정리: break_pr을 pdy_ctrt 다음에 배치, 나머지(검증용)는 우측 배치
    cols = list(df.columns)
    tail_cols = [c for c in ("dy_low", "new_low", "Rk", "Rk2") if c in cols]
    cols = [c for c in cols if c not in tail_cols]
    if "break_pr" in cols:
        cols.remove("break_pr")
        if "pdy_ctrt" in cols:
            idx = cols.index("pdy_ctrt") + 1
            cols.insert(idx, "break_pr")
        else:
            cols.append("break_pr")
    cols.extend(tail_cols)
    df = df[cols]

    return df, int(sell_count), float(tot_ret)


def main() -> None:
    start_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    buy_mode = "전일 종가 매수" if pdy_close_buy else "당일 시가 매수"
    print(f"[{start_ts}] 프로그램 시작__buy_mode: [{buy_mode}] K: [{K}] K2: [{K2}] ==============================================")
    parser = argparse.ArgumentParser(description="1m 트레일링 시뮬레이션")
    parser.add_argument("--date", dest="biz_date", help="기준일 (YYYYMMDD 또는 YYYY-MM-DD)")
    parser.add_argument("--limit", dest="limit", type=int, default=None, help="출력 행 제한")
    args = parser.parse_args()

    date_arg = args.biz_date or default_date
    limit_arg = default_limit if args.limit is None else args.limit

    biz_date = _normalize_biz_date(date_arg)
    date_str = datetime.strptime(biz_date, "%Y%m%d").strftime("%Y-%m-%d")
    parquet_1m = _resolve_1m_parquet_path(biz_date)
    limit_sql = f"LIMIT {int(limit_arg)}" if isinstance(limit_arg, int) and limit_arg > 0 else ""

    name_to_code, code_to_name = _build_name_code_map()
    codes = _parse_codes(CODE_TEXT, name_to_code)
    if not codes:
        print("CODE_TEXT에 종목코드가 없습니다.")
        return
    codes_sql = ",".join(f"'{c}'" for c in codes)
    code_filter = f"WHERE code IN ({codes_sql})"

    con = duckdb.connect()
    try:
        schema_df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_1m}')").df()
        all_cols = schema_df["column_name"].tolist() if "column_name" in schema_df.columns else []
        # 원본 DB head 출력은 사용하지 않습니다.

        preferred_cols = [
            "date",
            "time",
            "code",
            "name",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "acml_value",
            "tdy_ret",
            "pdy_close",
        ]
        select_cols = [c for c in preferred_cols if c in all_cols]
        if not select_cols:
            select_cols = ["*"]
        select_cols_sql = ", ".join(select_cols)

        query = f"""
        SELECT {select_cols_sql}
        FROM read_parquet('{parquet_1m}')
        {code_filter}
        ORDER BY date, code, time
        {limit_sql}
        """
        df_1m = con.execute(query).df()

        summary_map: Dict[str, Dict[str, float]] = {}
        if {"code", "time", "open", "high", "low", "close"}.issubset(set(all_cols)):
            try:
                summary_query = f"""
                SELECT
                    code,
                    min_by(CAST(open AS DOUBLE), CAST(time AS BIGINT)) AS day_open,
                    max_by(CAST(close AS DOUBLE), CAST(time AS BIGINT)) AS day_close,
                    max(CAST(high AS DOUBLE)) AS day_high,
                    min(CAST(low AS DOUBLE)) AS day_low
                FROM read_parquet('{parquet_1m}')
                {code_filter}
                GROUP BY code
                """
                summary_df = con.execute(summary_query).df()
                for _, row in summary_df.iterrows():
                    summary_map[str(row["code"])] = {
                        "day_open": float(row.get("day_open", 0) or 0),
                        "day_close": float(row.get("day_close", 0) or 0),
                        "day_high": float(row.get("day_high", 0) or 0),
                        "day_low": float(row.get("day_low", 0) or 0),
                    }
            except Exception:
                summary_map = {}

        info_map = _load_1d_info(con, date_str, codes)
    finally:
        con.close()

    if "name" in df_1m.columns:
        if not code_to_name:
            _, code_to_name = _build_name_code_map()
        if code_to_name:
            code_name = df_1m["code"].map(code_to_name)
            blank_name = df_1m["name"].isna() | df_1m["name"].astype(str).str.strip().eq("")
            df_1m.loc[blank_name, "name"] = code_name
            df_1m["name"] = df_1m["name"].fillna("")

    detail_frames: List[pd.DataFrame] = []
    summary_rows: List[Dict[str, Any]] = []
    summary_print_rows: List[Dict[str, str]] = []

    for code in codes:
        df_code = df_1m[df_1m["code"] == code].copy()
        if df_code.empty:
            continue

        info = info_map.get(code, {})
        pdy_close = float(info.get("pdy_close", 0))
        pdy_ctrt = float(info.get("pdy_ctrt", 0))
        atr3 = float(info.get("atr3", 0))
        name = info.get("name", "") or (df_code["name"].iloc[0] if "name" in df_code.columns else "")

        df_sim, sell_cnt, tot_ret = _simulate_strategy(
            df_code,
            pdy_close,
            pdy_ctrt,
            atr3,
            pdy_close_buy,
            TRAIL_STEPS,
            NEG_TDY_RET_STREAK_STOP,
            max_loss_rt,
        )
        df_sim["name"] = name
        detail_cols = [
            "date",
            "time",
            "code",
            "name",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "acml_value",
            "tdy_ret",
            "pdy_close",
            "pdy_ctrt",
            "break_pr",
            "buy",
            "sell",
            "trail_r",
            "trail_v",
            "ret",
            "tot_ret",
            "stop_time",
            "action",
            "atr3",
            "min_low",
            "Rk",
            "acml_volume",
        ]
        ordered_cols = [c for c in detail_cols if c in df_sim.columns]
        remaining_cols = [c for c in df_sim.columns if c not in ordered_cols]
        df_sim = df_sim[ordered_cols + remaining_cols]
        detail_frames.append(df_sim)

        day_open = float(info.get("open", 0) or 0)
        day_close = float(info.get("close", 0) or 0)
        day_high = float(info.get("high", 0) or 0)
        day_low = float(info.get("low", 0) or 0)
        if code in summary_map:
            if day_open <= 0:
                day_open = summary_map[code]["day_open"]
            if day_close <= 0:
                day_close = summary_map[code]["day_close"]
            if day_high <= 0:
                day_high = summary_map[code]["day_high"]
            if day_low <= 0:
                day_low = summary_map[code]["day_low"]
        if day_open <= 0:
            day_open = float(df_code.iloc[0]["open"])
        if day_close <= 0:
            day_close = float(df_code.iloc[-1]["close"])
        if day_high <= 0:
            day_high = float(df_code["high"].max()) if "high" in df_code.columns else 0.0
        if day_low <= 0:
            day_low = float(df_code["low"].min()) if "low" in df_code.columns else 0.0
        tdy_oc_rt = (day_close / day_open - 1) if day_open > 0 else 0.0
        tdy_ctrt = (day_close / pdy_close - 1) if pdy_close > 0 else 0.0
        prdy_ctrt = float(info.get("pdy_ctrt", 0))
        stop_time_val = ""
        if "stop_time" in df_sim.columns:
            non_empty = df_sim["stop_time"].astype(str).str.strip()
            non_empty = non_empty[non_empty != ""]
            if not non_empty.empty:
                stop_time_val = non_empty.iloc[-1]
        if not stop_time_val:
            stop_time_val = "15:30:00"
        summary_rows.append(
            {
                "date": date_str,
                "code": code,
                "name": name,
                "pdy_close": pdy_close,
                "open": day_open,
                "high": day_high,
                "low": day_low,
                "close": day_close,
                "tdy_oc_rt": tdy_oc_rt,
                "tdy_ctrt": tdy_ctrt,
                "prdy_ctrt": prdy_ctrt,
                "sell_cnt": sell_cnt,
                "tot_ret": tot_ret,
                "stop_time": stop_time_val,
            }
        )
        summary_print_rows.append(
            {
                "date": date_str,
                "code": code,
                "name": name,
                "pdy_close": f"{pdy_close:.0f}",
                "open": f"{day_open:.0f}",
                "high": f"{day_high:.0f}",
                "low": f"{day_low:.0f}",
                "close": f"{day_close:.0f}",
                "tdy_oc_rt": f"{tdy_oc_rt:.6f}",
                "prdy_ctrt": f"{prdy_ctrt:.6f}",
                "tdy_ctrt": f"{tdy_ctrt:.6f}",
                "sell_cnt": f"{sell_cnt}",
                "tot_ret": f"{tot_ret:.6f}",
                "stop_time": stop_time_val,
            }
        )

    detail_saved_msg = ""
    if detail_frames and default_save_csv:
        detail_df = pd.concat(detail_frames, ignore_index=True)
        detail_df.to_csv(DETAIL_OUT, index=False, encoding="utf-8-sig", float_format="%.9f")
        detail_saved_msg = f"[csv] saved: {DETAIL_OUT}"

    if summary_rows:
        avg_tot_ret = sum(r.get("tot_ret", 0.0) for r in summary_rows) / len(summary_rows)
        avg_tdy_oc_rt = sum(r.get("tdy_oc_rt", 0.0) for r in summary_rows) / len(summary_rows)
        avg_prdy_ctrt = sum(r.get("prdy_ctrt", 0.0) for r in summary_rows) / len(summary_rows)
        avg_tdy_ctrt = sum(r.get("tdy_ctrt", 0.0) for r in summary_rows) / len(summary_rows)
        summary_print_rows.append(
            {
                "date": "average",
                "code": "",
                "name": "",
                "pdy_close": "",
                "open": "",
                "high": "",
                "low": "",
                "close": "",
                "tdy_oc_rt": f"{avg_tdy_oc_rt:.6f}",
                "prdy_ctrt": f"{avg_prdy_ctrt:.6f}",
                "tdy_ctrt": f"{avg_tdy_ctrt:.6f}",
                "sell_cnt": "",
                "tot_ret": f"{avg_tot_ret:.6f}",
                "stop_time": "",
            }
        )
        columns = [
            "date",
            "code",
            "name",
            "pdy_close",
            "open",
            "high",
            "low",
            "close",
            "tdy_oc_rt",
            "prdy_ctrt",
            "tdy_ctrt",
            "sell_cnt",
            "tot_ret",
            "stop_time",
        ]
        align = {
            "date": "left",
            "code": "right",
            "name": "left",
            "pdy_close": "right",
            "open": "right",
            "high": "right",
            "low": "right",
            "close": "right",
            "tdy_oc_rt": "right",
            "prdy_ctrt": "right",
            "tdy_ctrt": "right",
            "sell_cnt": "right",
            "tot_ret": "right",
            "stop_time": "right",
        }
        print("분석결과 출력(고정폭 텍스트 테이블 형식(kis_utils/print_table))")
        print_table(summary_print_rows, columns, align)

    if summary_rows and default_save_csv:
        if detail_saved_msg:
            print(detail_saved_msg)
        summary_df = pd.DataFrame(summary_rows)
        summary_df.to_csv(SUMMARY_OUT, index=False, encoding="utf-8-sig", float_format="%.9f")
        print(f"[csv] saved: {SUMMARY_OUT}")
    end_ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    elapsed = time.perf_counter() - t0
    print(f"[{end_ts}] 프로그램 종료 (총 소요시간: {elapsed:.3f}초)")


if __name__ == "__main__":
    main()
