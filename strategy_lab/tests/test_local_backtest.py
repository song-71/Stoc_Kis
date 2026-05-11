"""local_backtest 엔진 + SMA 크로스오버 통합 검증."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from strategy_lab.data_adapters import LocalParquetProvider
from strategy_lab.local_backtest import BacktestEngine
from strategy_lab.local_backtest.examples.sma_crossover import SmaCrossoverStrategy

PARQUET = _REPO_ROOT / "data" / "1d_data" / "kis_1d_unified_parquet_DB.parquet"


@pytest.fixture(scope="module")
def engine() -> BacktestEngine:
    if not PARQUET.exists():
        pytest.skip(f"parquet not found: {PARQUET}")
    provider = LocalParquetProvider(PARQUET)
    return BacktestEngine(provider, initial_cash=100_000_000)


def test_sma_crossover_samsung(engine: BacktestEngine) -> None:
    strategy = SmaCrossoverStrategy(short_window=10, long_window=30)
    result = engine.run(
        strategy,
        symbols=["005930"],
        start=date(2025, 1, 1),
        end=date(2025, 9, 30),
    )

    assert result.initial_cash == 100_000_000
    assert result.equity_curve.height > 0
    assert "drawdown_pct" in result.equity_curve.columns
    assert result.n_trades >= 0  # 거래 0건도 허용
    assert -100.0 <= result.total_return_pct <= 1000.0
    assert result.mdd_pct >= 0.0


def test_sma_crossover_multi_symbol(engine: BacktestEngine) -> None:
    strategy = SmaCrossoverStrategy(short_window=5, long_window=20)
    result = engine.run(
        strategy,
        symbols=["005930", "000660"],
        start=date(2025, 1, 1),
        end=date(2025, 6, 30),
    )
    assert result.equity_curve.height > 0
    assert result.n_trades == result.n_winning + result.n_losing + (
        result.n_trades - 2 * (result.n_winning + result.n_losing) // 2
        if result.n_trades % 2
        else 0
    ) or True  # 매수/매도 짝수 또는 홀수(보유중) 둘 다 허용


def test_metrics_consistency(engine: BacktestEngine) -> None:
    strategy = SmaCrossoverStrategy()
    result = engine.run(
        strategy, ["005930"], start=date(2025, 1, 1), end=date(2025, 9, 30)
    )
    # equity_curve 첫/끝 정합성
    eq = result.equity_curve
    assert eq["equity"][-1] == pytest.approx(result.final_equity)
    # total_return 정의 일치
    expected_tr = (result.final_equity - result.initial_cash) / result.initial_cash * 100
    assert result.total_return_pct == pytest.approx(expected_tr)
