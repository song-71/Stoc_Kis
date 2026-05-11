"""LocalParquetProvider 기본 동작 검증."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from strategy_lab.data_adapters import LocalParquetProvider

PARQUET_PATH = _REPO_ROOT / "data" / "1d_data" / "kis_1d_unified_parquet_DB.parquet"


@pytest.fixture(scope="module")
def provider() -> LocalParquetProvider:
    if not PARQUET_PATH.exists():
        pytest.skip(f"parquet not found: {PARQUET_PATH}")
    return LocalParquetProvider(PARQUET_PATH)


def test_get_history_samsung(provider: LocalParquetProvider) -> None:
    bars = provider.get_history(
        symbol="005930",
        start=date(2025, 1, 2),
        end=date(2025, 1, 31),
    )
    assert len(bars) > 0
    # 시간순 정렬
    assert all(bars[i].time <= bars[i + 1].time for i in range(len(bars) - 1))
    # OHLC 정합성
    for b in bars:
        assert b.low <= b.open <= b.high
        assert b.low <= b.close <= b.high
        assert b.volume >= 0


def test_get_history_zfill(provider: LocalParquetProvider) -> None:
    bars_pad = provider.get_history(
        symbol="20", start=date(2025, 1, 2), end=date(2025, 1, 31)
    )
    bars_full = provider.get_history(
        symbol="000020", start=date(2025, 1, 2), end=date(2025, 1, 31)
    )
    assert len(bars_pad) == len(bars_full)


def test_get_history_unknown_symbol(provider: LocalParquetProvider) -> None:
    bars = provider.get_history(
        symbol="999999", start=date(2025, 1, 2), end=date(2025, 1, 31)
    )
    assert bars == []


def test_get_quote(provider: LocalParquetProvider) -> None:
    q = provider.get_quote("005930")
    assert q.bid_price > 0
    assert q.ask_price == q.bid_price  # parquet 더미


def test_get_quote_unknown(provider: LocalParquetProvider) -> None:
    with pytest.raises(KeyError):
        provider.get_quote("999999")


def test_subscribe_realtime_not_implemented(provider: LocalParquetProvider) -> None:
    with pytest.raises(NotImplementedError):
        provider.subscribe_realtime(["005930"], on_bar=lambda s, b: None)


def test_minute_resolution_not_supported(provider: LocalParquetProvider) -> None:
    from kis_backtest.models import Resolution

    with pytest.raises(NotImplementedError):
        provider.get_history(
            symbol="005930",
            start=date(2025, 1, 2),
            end=date(2025, 1, 31),
            resolution=Resolution.MINUTE,
        )
