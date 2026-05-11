"""백테스트 표준 지표 계산."""

from __future__ import annotations

import math
from typing import List

import polars as pl

from .types import Trade


def total_return_pct(initial: float, final: float) -> float:
    if initial <= 0:
        return 0.0
    return (final - initial) / initial * 100.0


def cagr_pct(initial: float, final: float, years: float) -> float:
    if initial <= 0 or final <= 0 or years <= 0:
        return 0.0
    return ((final / initial) ** (1.0 / years) - 1.0) * 100.0


def mdd_pct(equity_curve: pl.DataFrame, equity_col: str = "equity") -> float:
    """최대낙폭 (음수가 아닌 양수 %로 반환). drawdown_pct 컬럼도 함께 부착해 반환할 수도 있음."""
    if equity_curve.is_empty():
        return 0.0
    eq = equity_curve[equity_col].to_numpy()
    peak = eq[0]
    mdd = 0.0
    for v in eq:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak * 100.0
            if dd > mdd:
                mdd = dd
    return mdd


def attach_drawdown(equity_curve: pl.DataFrame, equity_col: str = "equity") -> pl.DataFrame:
    """equity_curve 에 drawdown_pct 컬럼 추가."""
    return equity_curve.with_columns(
        (
            (pl.col(equity_col).cum_max() - pl.col(equity_col))
            / pl.col(equity_col).cum_max()
            * 100.0
        ).alias("drawdown_pct")
    )


def sharpe(
    equity_curve: pl.DataFrame,
    equity_col: str = "equity",
    risk_free_annual: float = 0.03,
    trading_days: int = 252,
) -> float:
    if equity_curve.height < 2:
        return 0.0
    eq = equity_curve[equity_col].to_numpy()
    daily_ret = (eq[1:] - eq[:-1]) / eq[:-1]
    if daily_ret.size == 0:
        return 0.0
    excess = daily_ret - (risk_free_annual / trading_days)
    mean = float(excess.mean())
    std = float(excess.std(ddof=1))
    if std == 0 or math.isnan(std):
        return 0.0
    return mean / std * math.sqrt(trading_days)


def trade_stats(trades: List[Trade]) -> tuple[float, float, int, int]:
    """win_rate_pct, profit_factor, n_winning, n_losing.
    매수만 있는 경우 모두 0 반환.
    """
    sells = [t for t in trades if t.side == "SELL"]
    if not sells:
        return 0.0, 0.0, 0, 0
    wins = [t.pnl for t in sells if t.pnl > 0]
    losses = [t.pnl for t in sells if t.pnl < 0]
    n_w = len(wins)
    n_l = len(losses)
    win_rate = n_w / len(sells) * 100.0
    gross_profit = sum(wins) if wins else 0.0
    gross_loss = abs(sum(losses)) if losses else 0.0
    if gross_loss == 0:
        pf = float("inf") if gross_profit > 0 else 0.0
    else:
        pf = gross_profit / gross_loss
    return win_rate, pf, n_w, n_l
