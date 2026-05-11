"""모멘텀 데이트레이딩 — 전일 강한 상승 + 거래대금 폭증 종목 다음날 당일거래.

규칙:
    - 전일 등락률 `pdy_ctrt` ∈ [pdy_ctrt_min, pdy_ctrt_max]
    - 전일 거래대금 `value` ≥ `vema20` × vol_ratio_min
    - 매일 위 조건 만족 종목 중 거래대금 큰 상위 max_codes_per_day 선택
    - 다음날 시가 매수 → 같은 날 종가 매도

KOSDAQ/KOSPI 종합 universe. 우리 parquet 의 사전계산 컬럼 그대로 사용.
"""

from __future__ import annotations

import math
from datetime import date
from pathlib import Path
from typing import Dict, List

import polars as pl

from ..metrics import attach_drawdown, cagr_pct, mdd_pct, sharpe, total_return_pct, trade_stats
from ..types import BacktestResult, Trade

DEFAULT_FEE_BUY = 0.00015
DEFAULT_FEE_SELL = 0.00015 + 0.0018


class MomentumDayTradeStrategy:
    """전일 모멘텀 + 거래대금 폭증 종목의 다음날 시가→종가 데이트레이딩."""

    name = "momentum_day_trade"

    def __init__(
        self,
        pdy_ctrt_min: float = 0.03,
        pdy_ctrt_max: float = 0.27,
        vol_ratio_min: float = 1.5,
        max_codes_per_day: int = 5,
        min_price: float = 1000.0,
    ) -> None:
        self.pdy_ctrt_min = pdy_ctrt_min
        self.pdy_ctrt_max = pdy_ctrt_max
        self.vol_ratio_min = vol_ratio_min
        self.max_codes_per_day = max_codes_per_day
        self.min_price = min_price

    def backtest(
        self,
        parquet_path: str | Path,
        start: date,
        end: date,
        initial_cash: float = 100_000_000,
        fee_buy: float = DEFAULT_FEE_BUY,
        fee_sell: float = DEFAULT_FEE_SELL,
        slippage_rate: float = 0.0,
    ) -> BacktestResult:
        df = (
            pl.scan_parquet(parquet_path)
            .filter((pl.col("date") >= start) & (pl.col("date") <= end))
            .select(
                [
                    "date", "symbol", "name", "open", "high", "low", "close",
                    "volume", "value", "pdy_ctrt", "vema20",
                ]
            )
            .collect()
        )
        if df.is_empty():
            return self._empty_result(initial_cash)

        # 다음날 진입을 위해 — 전일(t-1)의 pdy_ctrt/value/vema20 으로 오늘(t) 매수 후보 결정
        # parquet 의 pdy_ctrt 는 이미 "오늘 종가 vs 어제 종가" 등락률.
        # 즉 오늘 종가 시점에 pdy_ctrt 가 양수이면 오늘 상승. 다음날(t+1) 매수해야 함.
        # → 시그널 조건은 t 일 데이터로 평가하고 매수/매도는 t+1 에 실행.
        df = df.sort(["symbol", "date"])
        df = df.with_columns(
            [
                pl.col("open").shift(-1).over("symbol").alias("next_open"),
                pl.col("close").shift(-1).over("symbol").alias("next_close"),
                pl.col("date").shift(-1).over("symbol").alias("next_date"),
                pl.col("name").shift(-1).over("symbol").alias("next_name"),
            ]
        )

        candidates = (
            df.filter(
                (pl.col("pdy_ctrt") >= self.pdy_ctrt_min)
                & (pl.col("pdy_ctrt") <= self.pdy_ctrt_max)
                & (pl.col("vema20") > 0)
                & (pl.col("value") >= pl.col("vema20") * self.vol_ratio_min)
                & (pl.col("close") >= self.min_price)
                & pl.col("next_open").is_not_null()
                & pl.col("next_close").is_not_null()
            )
            .with_columns(
                pl.col("value")
                .rank(method="ordinal", descending=True)
                .over("date")
                .alias("rank_by_value")
            )
            .filter(pl.col("rank_by_value") <= self.max_codes_per_day)
            .sort(["next_date", "rank_by_value"])
        )

        if candidates.is_empty():
            return self._empty_result(initial_cash)

        cash = float(initial_cash)
        trades: List[Trade] = []
        equity_by_date: Dict[date, float] = {}

        for trade_date, group in candidates.group_by("next_date", maintain_order=True):
            trade_d = (
                trade_date[0]
                if isinstance(trade_date, tuple)
                else trade_date
            )
            n = group.height
            day_pnl = 0.0
            for row in group.iter_rows(named=True):
                sym = row["symbol"]
                name = row["next_name"] or row["name"]
                buy_px = float(row["next_open"]) * (1.0 + slippage_rate)
                sell_px = float(row["next_close"]) * (1.0 - slippage_rate)
                budget = cash * (1.0 / n) * 0.995
                qty = int(budget // buy_px)
                if qty <= 0:
                    continue
                buy_gross = qty * buy_px
                buy_fee = buy_gross * fee_buy
                sell_gross = qty * sell_px
                sell_fee = sell_gross * fee_sell
                cash -= buy_gross + buy_fee
                cash += sell_gross - sell_fee
                pnl = (sell_gross - sell_fee) - (buy_gross + buy_fee)
                day_pnl += pnl
                trades.append(
                    Trade(symbol=sym, side="BUY", date=trade_d,
                          price=buy_px, quantity=qty, fee=buy_fee, pnl=0.0)
                )
                # SELL trade 에는 종목명을 reason 처럼 stock_name 으로 못 넣으니
                # 대신 symbol 그대로 두되, 보고 시점에 외부 name_map 으로 매핑.
                trades.append(
                    Trade(symbol=sym, side="SELL", date=trade_d,
                          price=sell_px, quantity=qty, fee=sell_fee, pnl=pnl)
                )
            equity_by_date[trade_d] = cash  # 당일 청산이므로 cash == equity

        if not equity_by_date:
            return self._empty_result(initial_cash)

        equity_curve = (
            pl.DataFrame(
                [{"date": d, "equity": v} for d, v in sorted(equity_by_date.items())]
            )
            .sort("date")
        )
        equity_curve = attach_drawdown(equity_curve)

        final_equity = float(equity_curve["equity"][-1])
        n_days = (
            (equity_curve["date"][-1] - equity_curve["date"][0]).days
            if equity_curve.height >= 2
            else 0
        )
        years = max(n_days / 365.25, 1e-9)
        win_rate, pf, n_w, n_l = trade_stats(trades)

        return BacktestResult(
            initial_cash=float(initial_cash),
            final_equity=final_equity,
            total_return_pct=total_return_pct(initial_cash, final_equity),
            cagr_pct=cagr_pct(initial_cash, final_equity, years),
            mdd_pct=mdd_pct(equity_curve),
            sharpe=sharpe(equity_curve),
            win_rate_pct=win_rate,
            profit_factor=pf if not math.isinf(pf) else float("nan"),
            n_trades=len(trades),
            n_winning=n_w,
            n_losing=n_l,
            equity_curve=equity_curve,
            trades=trades,
        )

    def _empty_result(self, initial_cash: float) -> BacktestResult:
        return BacktestResult(
            initial_cash=float(initial_cash),
            final_equity=float(initial_cash),
            total_return_pct=0.0,
            cagr_pct=0.0,
            mdd_pct=0.0,
            sharpe=0.0,
            win_rate_pct=0.0,
            profit_factor=0.0,
            n_trades=0, n_winning=0, n_losing=0,
            equity_curve=pl.DataFrame(
                schema={"date": pl.Date, "equity": pl.Float64, "drawdown_pct": pl.Float64}
            ),
            trades=[],
        )

    # ------------------------------------------------------------------
    # 종목명 매핑 헬퍼 (parquet 의 name 컬럼)
    # ------------------------------------------------------------------
    @staticmethod
    def build_name_map(parquet_path: str | Path) -> Dict[str, str]:
        return dict(
            pl.scan_parquet(parquet_path)
            .select(["symbol", "name"])
            .unique(subset=["symbol"], keep="last")
            .collect()
            .iter_rows()
        )
