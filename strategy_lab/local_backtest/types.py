"""백테스트 도메인 타입."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Literal

import polars as pl

Action = Literal["BUY", "SELL", "HOLD", "DAY_TRADE"]


@dataclass(frozen=True)
class Signal:
    date: date
    symbol: str
    action: Action
    strength: float = 1.0
    reason: str = ""


@dataclass(frozen=True)
class Trade:
    symbol: str
    side: Literal["BUY", "SELL"]
    date: date
    price: float
    quantity: int
    fee: float = 0.0
    pnl: float = 0.0  # 매도 체결 시 실현손익 (매수면 0)


@dataclass
class BacktestResult:
    initial_cash: float
    final_equity: float
    total_return_pct: float
    cagr_pct: float
    mdd_pct: float
    sharpe: float
    win_rate_pct: float
    profit_factor: float
    n_trades: int
    n_winning: int
    n_losing: int
    equity_curve: pl.DataFrame  # columns: date, equity, drawdown_pct
    trades: List[Trade] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"=== BacktestResult ===\n"
            f"initial_cash:     {self.initial_cash:>15,.0f}\n"
            f"final_equity:     {self.final_equity:>15,.0f}\n"
            f"total_return:     {self.total_return_pct:>14.2f}%\n"
            f"CAGR:             {self.cagr_pct:>14.2f}%\n"
            f"MDD:              {self.mdd_pct:>14.2f}%\n"
            f"Sharpe:           {self.sharpe:>14.2f}\n"
            f"Win Rate:         {self.win_rate_pct:>14.2f}%\n"
            f"Profit Factor:    {self.profit_factor:>14.2f}\n"
            f"Trades:           {self.n_trades:>14}  "
            f"(W:{self.n_winning} / L:{self.n_losing})"
        )
