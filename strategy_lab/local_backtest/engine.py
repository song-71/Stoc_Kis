"""일봉 백테스트 엔진.

흐름:
    1. provider.get_history() 로 종목별 일봉 수집 → Polars DataFrame 통합
    2. strategy.generate_signals() 호출 → 시그널 DataFrame
    3. 날짜 순회하면서 가상 포트폴리오 시뮬레이션 (매수/매도)
    4. 일별 equity curve 계산
    5. metrics 모듈로 표준 지표 산출 → BacktestResult

가정/단순화:
    - 일봉 close 가격에 체결 (다음날 시가 슬리피지 X)
    - 매수: cash * strength (0~1) * 0.995(여유) → 정수 주
    - 매도: 보유 전량
    - 수수료: KRX 매수 0.015%, 매도 0.015% + 거래세 0.18% (코스피)
    - 슬리피지: slippage_rate (기본 0)
"""

from __future__ import annotations

import math
from datetime import date, datetime, time
from typing import Iterable, List

import polars as pl

from .metrics import attach_drawdown, cagr_pct, mdd_pct, sharpe, total_return_pct, trade_stats
from .strategy import Strategy
from .types import BacktestResult, Trade

DEFAULT_FEE_BUY = 0.00015
DEFAULT_FEE_SELL = 0.00015 + 0.0018  # 매도 수수료 + 거래세 (코스피 기준 단순화)


class BacktestEngine:
    def __init__(
        self,
        provider,
        initial_cash: float = 100_000_000,
        fee_buy: float = DEFAULT_FEE_BUY,
        fee_sell: float = DEFAULT_FEE_SELL,
        slippage_rate: float = 0.0,
    ) -> None:
        self._provider = provider
        self._initial_cash = float(initial_cash)
        self._fee_buy = fee_buy
        self._fee_sell = fee_sell
        self._slip = slippage_rate

    def _collect_bars(
        self, symbols: Iterable[str], start: date, end: date
    ) -> pl.DataFrame:
        from kis_backtest.models import Resolution  # type: ignore

        rows = []
        for sym in symbols:
            bars = self._provider.get_history(sym, start, end, Resolution.DAILY)
            for b in bars:
                t = b.time
                d = t.date() if isinstance(t, datetime) else t
                rows.append(
                    {
                        "date": d,
                        "symbol": sym,
                        "open": b.open,
                        "high": b.high,
                        "low": b.low,
                        "close": b.close,
                        "volume": b.volume,
                    }
                )
        if not rows:
            return pl.DataFrame(
                schema={
                    "date": pl.Date,
                    "symbol": pl.Utf8,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Int64,
                }
            )
        return pl.DataFrame(rows).sort(["symbol", "date"])

    def run(
        self,
        strategy: Strategy,
        symbols: Iterable[str],
        start: date,
        end: date,
    ) -> BacktestResult:
        symbols = list(symbols)
        bars = self._collect_bars(symbols, start, end)
        if bars.is_empty():
            return self._empty_result()

        signals = strategy.generate_signals(bars)
        # 보장: action 컬럼 유효값
        if "action" not in signals.columns:
            raise ValueError("Strategy.generate_signals 결과에 'action' 컬럼 필요")

        signals = signals.with_columns(
            [
                pl.col("action").cast(pl.Utf8),
                pl.col("strength").cast(pl.Float64)
                if "strength" in signals.columns
                else pl.lit(1.0).alias("strength"),
            ]
        )
        merged = bars.join(
            signals.select(["date", "symbol", "action", "strength"]),
            on=["date", "symbol"],
            how="left",
        ).with_columns(
            pl.col("action").fill_null("HOLD"),
            pl.col("strength").fill_null(1.0),
        )

        all_dates = merged["date"].unique().sort()
        cash = self._initial_cash
        positions: dict[str, dict] = {}  # symbol → {qty, avg_price}
        trades: List[Trade] = []
        equity_rows = []

        for d in all_dates:
            today = merged.filter(pl.col("date") == d)
            close_lookup = {row["symbol"]: row["close"] for row in today.iter_rows(named=True)}

            for row in today.iter_rows(named=True):
                sym = row["symbol"]
                action = row["action"]
                strength = float(row["strength"])
                close = float(row["close"])

                if action == "BUY" and cash > 0 and sym not in positions:
                    fill_price = close * (1.0 + self._slip)
                    budget = cash * max(0.0, min(strength, 1.0)) * 0.995
                    qty = int(budget // fill_price)
                    if qty <= 0:
                        continue
                    gross = qty * fill_price
                    fee = gross * self._fee_buy
                    cost = gross + fee
                    if cost > cash:
                        # 수수료 포함 재계산
                        qty = int((cash / (1 + self._fee_buy)) // fill_price)
                        if qty <= 0:
                            continue
                        gross = qty * fill_price
                        fee = gross * self._fee_buy
                        cost = gross + fee
                    cash -= cost
                    positions[sym] = {"qty": qty, "avg_price": fill_price}
                    trades.append(
                        Trade(
                            symbol=sym, side="BUY",
                            date=d if isinstance(d, date) else d.date(),
                            price=fill_price, quantity=qty, fee=fee, pnl=0.0,
                        )
                    )

                elif action == "SELL" and sym in positions:
                    pos = positions.pop(sym)
                    qty = pos["qty"]
                    avg = pos["avg_price"]
                    fill_price = close * (1.0 - self._slip)
                    gross = qty * fill_price
                    fee = gross * self._fee_sell
                    proceeds = gross - fee
                    cash += proceeds
                    pnl = proceeds - qty * avg
                    trades.append(
                        Trade(
                            symbol=sym, side="SELL",
                            date=d if isinstance(d, date) else d.date(),
                            price=fill_price, quantity=qty, fee=fee, pnl=pnl,
                        )
                    )

            # 일말 equity 평가
            holdings_value = sum(
                pos["qty"] * close_lookup.get(sym, pos["avg_price"])
                for sym, pos in positions.items()
            )
            equity = cash + holdings_value
            equity_rows.append({"date": d, "equity": equity})

        equity_curve = pl.DataFrame(equity_rows).sort("date")
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
            initial_cash=self._initial_cash,
            final_equity=final_equity,
            total_return_pct=total_return_pct(self._initial_cash, final_equity),
            cagr_pct=cagr_pct(self._initial_cash, final_equity, years),
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

    def _empty_result(self) -> BacktestResult:
        return BacktestResult(
            initial_cash=self._initial_cash,
            final_equity=self._initial_cash,
            total_return_pct=0.0,
            cagr_pct=0.0,
            mdd_pct=0.0,
            sharpe=0.0,
            win_rate_pct=0.0,
            profit_factor=0.0,
            n_trades=0,
            n_winning=0,
            n_losing=0,
            equity_curve=pl.DataFrame(
                schema={"date": pl.Date, "equity": pl.Float64, "drawdown_pct": pl.Float64}
            ),
            trades=[],
        )
