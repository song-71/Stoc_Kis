"""
KIS 트레이딩 시뮬레이션 엔진 - 전략1 (Str1)
- 전일 종가에 전액 매수 완료 상태로 시작
- 하락: bidp1 < wghn_avrg_stck_prc → bidp1에 즉시 매도
- 상향: ma50 < ma500 → bidp1에 매도
- 각 종목당 1회 매도 후 종료
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import os
import numpy as np
import polars as pl


@dataclass
class TradeResult:
    buy_tick: int
    buy_price: float
    buy_reason: str
    sell_tick: int
    sell_price: float
    sell_reason: str
    qty: int
    pnl: float
    ret_pct: float
    tot_ret_pct: float = 0.0
    actual_sell_price: float = 0.0


def _get_tick_size(price, market="KOSPI"):
    if price < 1000: return 1
    elif price < 5000: return 5
    elif price < 10000: return 10
    elif price < 50000: return 10 if market == "KOSDAQ" else 50
    elif price < 100000: return 100
    elif price < 500000: return 500
    else: return 1000


def _round_to_tick(price, market="KOSPI"):
    tick = _get_tick_size(price, market)
    return round(price / tick) * tick


def _get_prev_close(df: pl.DataFrame) -> float:
    """전일 종가 계산: stck_prpr / (1 + prdy_ctrt/100)"""
    for i in range(min(100, len(df))):
        row = df.row(i, named=True)
        pr = row.get("stck_prpr")
        pct = row.get("prdy_ctrt")
        if pr is None or (isinstance(pr, float) and (np.isnan(pr) or pr <= 0)):
            continue
        try:
            pr_f = float(pr)
            if pct is not None and not (isinstance(pct, float) and np.isnan(pct)):
                pct_f = float(pct)
                if abs(pct_f) < 100:
                    return pr_f / (1 + pct_f / 100)
            return pr_f
        except (ValueError, TypeError):
            continue
    return float(df["stck_prpr"][0]) if len(df) > 0 else 0.0


def run_simulation_str1(
    df: pl.DataFrame,
    init_cash: float = 10_000_000,
    slippage_market_sell: float = 0.0035,
    market: str = "KOSPI",
) -> tuple[pl.DataFrame, list[TradeResult], dict]:
    """
    Str1 전략: 전일 종가 매수 완료 → wghn 하락 또는 ma50<ma500 시 매도 (1회)
    
    Returns:
        (df_with_trade_cols, trade_results, summary_dict)
    """
    n = len(df)
    if n == 0:
        return df, [], {}

    prev_close = _get_prev_close(df)
    if prev_close <= 0:
        return df, [], {"error": "전일종가 유효하지 않음"}

    qty = int(init_cash / prev_close)
    if qty <= 0:
        return df, [], {"error": "매수 수량 0"}

    bidp1 = df["bidp1"].cast(pl.Float64).fill_null(0).to_numpy() if "bidp1" in df.columns else df["stck_prpr"].cast(pl.Float64).to_numpy()
    wghn = df["wghn_avrg_stck_prc"].cast(pl.Float64).fill_null(0).to_numpy() if "wghn_avrg_stck_prc" in df.columns else bidp1.copy()
    ma50 = df["ma50"].cast(pl.Float64).fill_null(0).to_numpy() if "ma50" in df.columns else np.zeros(n)
    ma500 = df["ma500"].cast(pl.Float64).fill_null(0).to_numpy() if "ma500" in df.columns else np.zeros(n)
    stck_oprc = df["stck_oprc"].cast(pl.Float64).fill_null(0).to_numpy() if "stck_oprc" in df.columns else np.ones(n)

    # 시초가 형성(stck_oprc>0) 시점부터 거래 시작
    first_trade_tick = 0
    for i in range(n):
        if stck_oprc[i] > 0:
            first_trade_tick = i
            break

    col_buy_order = [""] * n
    col_buy_pr = np.zeros(n)
    col_buy_qty = np.zeros(n, dtype=int)
    col_sell_order = [""] * n
    col_sell_pr = np.zeros(n)
    col_sell_qty = np.zeros(n, dtype=int)
    col_qty = np.zeros(n, dtype=int)
    col_cash = np.full(n, 0.0)
    col_ret = np.zeros(n)
    col_tot_ret = np.zeros(n)
    col_v_cash = np.full(n, init_cash)

    cash = 0.0
    position = True
    trade_results: list[TradeResult] = []
    sell_tick = -1

    for i in range(n):
        bp1 = bidp1[i]
        w = wghn[i]
        m50 = ma50[i]
        m500 = ma500[i]
        pr = float(df["stck_prpr"][i]) if "stck_prpr" in df.columns else bp1

        if not position:
            col_cash[i] = cash
            col_v_cash[i] = cash
            col_tot_ret[i] = cash / init_cash - 1 if init_cash > 0 else 0
            continue

        # 시초가 형성 전: 거래 미개시, 포지션 보유만 표시
        if i < first_trade_tick:
            col_qty[i] = qty
            col_cash[i] = 0.0
            col_v_cash[i] = qty * pr if pr > 0 else 0
            col_tot_ret[i] = col_v_cash[i] / init_cash - 1 if init_cash > 0 else 0
            continue

        if pr <= 0 or bp1 <= 0:
            continue

        sell_reason = None
        if w > 0 and bp1 < w:
            sell_reason = "str1_wghn_하락"
        elif m50 > 0 and m500 > 0 and m50 < m500:
            sell_reason = "str1_ma50_ma500_데드크로스"

        if sell_reason:
            actual_sell_pr = bp1 * (1 - slippage_market_sell)
            actual_sell_pr = _round_to_tick(actual_sell_pr, market)
            revenue = actual_sell_pr * qty
            cash = revenue
            pnl = (actual_sell_pr - prev_close) * qty
            ret = actual_sell_pr / prev_close - 1 if prev_close > 0 else 0

            col_sell_order[i] = f"market_{sell_reason}"
            col_sell_pr[i] = bp1
            col_sell_qty[i] = qty
            col_cash[i] = cash
            col_v_cash[i] = cash
            col_ret[i] = ret
            col_tot_ret[i] = ret
            col_qty[i] = 0

            tr = TradeResult(
                buy_tick=first_trade_tick, buy_price=prev_close, buy_reason="str1_전일종가_매수완료",
                sell_tick=i, sell_price=bp1, sell_reason=sell_reason,
                qty=qty, pnl=pnl, ret_pct=ret, tot_ret_pct=ret,
                actual_sell_price=actual_sell_pr,
            )
            trade_results.append(tr)
            position = False
            sell_tick = i
        else:
            col_qty[i] = qty
            col_cash[i] = 0.0
            col_v_cash[i] = qty * pr if pr > 0 else 0
            col_tot_ret[i] = col_v_cash[i] / init_cash - 1 if init_cash > 0 else 0

    col_buy_order[first_trade_tick] = "str1_전일종가_매수완료"
    col_buy_pr[first_trade_tick] = prev_close
    col_buy_qty[first_trade_tick] = qty

    col_buy_fill_qty = [qty if i == first_trade_tick else 0 for i in range(n)]
    col_sell_fill_qty = [qty if i == sell_tick else 0 for i in range(n)]
    col_remain_qty = [0] * n

    df = df.with_columns([
        pl.Series("buy_order", col_buy_order),
        pl.Series("buy_pr", col_buy_pr),
        pl.Series("buy_qty", col_buy_qty),
        pl.Series("buy_fill_qty", col_buy_fill_qty),
        pl.Series("sell_order", col_sell_order),
        pl.Series("sell_pr", col_sell_pr),
        pl.Series("sell_qty", col_sell_qty),
        pl.Series("sell_fill_qty", col_sell_fill_qty),
        pl.Series("qty", col_qty),
        pl.Series("remain_qty", col_remain_qty),
        pl.Series("cash", col_cash),
        pl.Series("ret", col_ret),
        pl.Series("tot_ret", col_tot_ret),
        pl.Series("v_cash", col_v_cash),
    ])

    if position:
        final_cash = init_cash
    else:
        final_cash = cash
    tot_ret_pct = (final_cash / init_cash - 1) * 100 if init_cash > 0 else 0
    summary = {
        "n_buys": 1,
        "n_sells": len(trade_results),
        "tot_ret_pct": tot_ret_pct,
        "init_cash": init_cash,
        "final_cash": final_cash,
        "prev_close": prev_close,
        "qty": qty,
    }
    return df, trade_results, summary


def run_str1_batch(
    data_path: str,
    date_ymd: str,
    init_cash: float = 10_000_000,
    slippage_market_sell: float = 0.0035,
    start_time: str = "09:00",
    use_ema: bool = True,
    ma_line: list = None,
) -> list[dict]:
    """
    모든 종목에 대해 Str1 시뮬레이션 실행, 종목별 성과 반환.
    ※ 08:59까지 수신된 종목만 대상 (전일종가 매수 가정 → 09:00 전 모니터링 필요)
    
    Returns:
        [{"name": str, "ret_pct": float, "sell_reason": str, "pnl": float, ...}, ...]
    """
    if ma_line is None:
        ma_line = [3, 50, 200, 300, 500]

    if not os.path.exists(data_path):
        print(f"[str1] 파일 없음: {data_path}", flush=True)
        return []

    lf = pl.scan_parquet(data_path)
    schema = lf.collect_schema().names()
    name_col = "name" if "name" in schema else None
    code_col = "mksc_shrn_iscd" if "mksc_shrn_iscd" in schema else None

    if name_col:
        all_names = lf.select(name_col).unique().collect()[name_col].to_list()
    elif code_col:
        all_names = lf.select(code_col).unique().collect()[code_col].to_list()
    else:
        return []

    all_names = [str(s).strip() for s in all_names if s and str(s).strip() and str(s).strip() not in ("=====", "전체")]
    results = []

    # 08:59까지 수신된 종목만: 전일종가 매수 가정 시 09:00 전부터 모니터링해야 하므로
    cutoff_min = 8 * 60 + 59  # 08:59 = 539분

    try:
        import Kis_local_utils as klu
    except ImportError:
        klu = None

    for nm in all_names:
        try:
            lf_sub = pl.scan_parquet(data_path)
            if name_col:
                lf_sub = lf_sub.filter(pl.col("name") == nm)
            else:
                lf_sub = lf_sub.filter(pl.col("mksc_shrn_iscd") == nm)

            # 08:59 이전 데이터 존재 여부 확인 (수신 시작이 09:00 이후면 제외)
            if "recv_ts" in schema:
                try:
                    ts_col = pl.col("recv_ts").cast(pl.Utf8)
                    lf_check = lf_sub.with_columns(
                        (ts_col.str.slice(11, 2).cast(pl.Int32) * 60 +
                         ts_col.str.slice(14, 2).cast(pl.Int32)).alias("_tm")
                    )
                    min_row = lf_check.select(pl.col("_tm").min()).collect()
                    if min_row.is_empty():
                        continue
                    min_val = min_row.item() if min_row.shape == (1, 1) else min_row[0, 0]
                    if min_val is None or min_val > cutoff_min:
                        continue
                except Exception:
                    pass

            if "recv_ts" in schema and start_time:
                try:
                    h, m = map(int, start_time.split(":")[:2])
                    start_min = h * 60 + m
                    ts_col = pl.col("recv_ts").cast(pl.Utf8)
                    lf_sub = lf_sub.with_columns(
                        (ts_col.str.slice(11, 2).cast(pl.Int32) * 60 +
                         ts_col.str.slice(14, 2).cast(pl.Int32)).alias("_tm")
                    )
                    lf_sub = lf_sub.filter(pl.col("_tm") >= start_min)
                    lf_sub = lf_sub.drop("_tm")
                except Exception:
                    pass

            df = lf_sub.collect()
            if len(df) == 0:
                continue

            for c in ["stck_prpr", "prdy_ctrt", "wghn_avrg_stck_prc", "stck_oprc", "askp1", "bidp1"]:
                if c in df.columns:
                    df = df.with_columns(pl.col(c).cast(pl.Float64, strict=False))

            market = "KOSPI"
            code = str(nm).zfill(6) if str(nm).isdigit() else nm
            stock_name = nm
            if klu:
                _c, _n, _, _ = klu.krx_code(nm)
                code = str(_c).zfill(6) if _c else code
                stock_name = str(_n) if _n else nm
                if name_col and nm not in ("=====", "전체"):
                    market = klu.get_market_for_name(nm) or "KOSPI"

            for n in ma_line:
                if use_ema:
                    prices = df["stck_prpr"].cast(pl.Float64).to_list()
                    alpha = 2 / (n + 1)
                    ema, ema_list = None, []
                    for p in prices:
                        ema = p if ema is None else ema + alpha * (p - ema)
                        ema_list.append(ema)
                    df = df.with_columns(pl.Series(f"ma{n}", ema_list))
                else:
                    df = df.with_columns(pl.col("stck_prpr").rolling_mean(n).alias(f"ma{n}"))

            _, trades, summary = run_simulation_str1(
                df, init_cash=init_cash,
                slippage_market_sell=slippage_market_sell, market=market,
            )

            sell_reason = "미매도"
            ret_pct = 0.0
            pnl = 0.0
            if trades:
                t = trades[0]
                sell_reason = t.sell_reason
                ret_pct = t.ret_pct * 100
                pnl = t.pnl

            results.append({
                "code": code,
                "name": stock_name,
                "ret_pct": ret_pct,
                "sell_reason": sell_reason,
                "pnl": pnl,
                "prev_close": summary.get("prev_close", 0),
                "qty": summary.get("qty", 0),
            })

        except Exception as e:
            results.append({"code": str(nm).zfill(6) if str(nm).isdigit() else nm, "name": nm,
                            "ret_pct": 0.0, "sell_reason": f"오류:{e}", "pnl": 0.0})

    return results


def print_str1_results(results: list[dict]):
    """종목별 Str1 성과 출력"""
    if not results:
        print("[str1] 결과 없음", flush=True)
        return
    try:
        import Kis_local_utils as klu
        sorted_res = sorted(results, key=lambda x: -x.get("ret_pct", 0))
        tbl_rows = [{"code": r.get("code", ""), "name": r.get("name", ""), "ret_pct": r.get("ret_pct", 0)} for r in sorted_res]
        print("\n" + "=" * 60, flush=True)
        print("[Str1] 종목별 성과 (전일종가 매수 → wghn하락/ma50<ma500 매도)", flush=True)
        print("=" * 60, flush=True)
        klu.print_table(tbl_rows, columns=["code", "name", "ret_pct"],
                        align={"code": "right", "ret_pct": "right"},
                        headers={"code": "종목코드", "name": "종목명", "ret_pct": "RET"})
        avg_ret = sum(r.get("ret_pct", 0) for r in results) / len(results)
        print("-" * 60, flush=True)
        print(f"  평균 ret = {avg_ret:+.2f}%  (종목수: {len(results)})", flush=True)
        print("=" * 60 + "\n", flush=True)
    except ImportError:
        for r in sorted(results, key=lambda x: -x.get("ret_pct", 0)):
            print(f"  {r.get('code',''):>8}  {r.get('name',''):18}  ret={r.get('ret_pct',0):+7.2f}%", flush=True)
