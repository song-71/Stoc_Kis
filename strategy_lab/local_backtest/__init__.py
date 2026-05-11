"""자체 일봉 백테스트 엔진 (Polars 기반).

Lean Docker 미사용. 우리 parquet 데이터로 직접 시뮬레이션.
표준 지표(Total Return/CAGR/MDD/Sharpe/Win Rate/Profit Factor) 산출.
"""

from .types import Signal, Trade, BacktestResult
from .strategy import Strategy
from .engine import BacktestEngine
from . import metrics

__all__ = [
    "Signal",
    "Trade",
    "BacktestResult",
    "Strategy",
    "BacktestEngine",
    "metrics",
]
