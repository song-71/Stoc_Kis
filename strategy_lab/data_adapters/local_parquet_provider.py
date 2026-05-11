"""LocalParquetProvider — kis_backtest DataProvider 구현 (parquet 기반)

우리 일봉 parquet (data/1d_data/kis_1d_unified_parquet_DB.parquet) 을 읽어
kis_backtest 의 DataProvider Protocol 을 만족하는 Bar 리스트로 변환한다.

KIS API 호출 0회, 인증(kis_devlp.yaml) 불필요.
백테스트 전용 — subscribe_realtime 은 NotImplementedError.

사용:
    from strategy_lab.data_adapters import LocalParquetProvider
    from kis_backtest import LeanClient

    provider = LocalParquetProvider("data/1d_data/kis_1d_unified_parquet_DB.parquet")
    client = LeanClient(data_provider=provider)
"""

from __future__ import annotations

import sys
from datetime import date, datetime, time
from pathlib import Path
from typing import Callable, List, Optional

import polars as pl

_KIS_BACKTEST_ROOT = (
    Path(__file__).resolve().parents[1] / "open_trading_api" / "backtester"
)
if str(_KIS_BACKTEST_ROOT) not in sys.path:
    sys.path.insert(0, str(_KIS_BACKTEST_ROOT))

from kis_backtest.models import Bar, Quote, Resolution  # noqa: E402
from kis_backtest.models.trading import Subscription  # noqa: E402


class LocalParquetProvider:
    """일봉 parquet 데이터를 kis_backtest DataProvider 형태로 공급."""

    def __init__(self, parquet_path: str | Path) -> None:
        self._path = Path(parquet_path)
        if not self._path.exists():
            raise FileNotFoundError(f"parquet not found: {self._path}")
        self._lazy = pl.scan_parquet(self._path)

    def get_history(
        self,
        symbol: str,
        start: date,
        end: date,
        resolution: Resolution = Resolution.DAILY,
    ) -> List[Bar]:
        if resolution != Resolution.DAILY:
            raise NotImplementedError(
                f"LocalParquetProvider 는 DAILY 만 지원 (요청: {resolution})"
            )

        symbol = symbol.zfill(6)
        df = (
            self._lazy.filter(
                (pl.col("symbol") == symbol)
                & (pl.col("date") >= start)
                & (pl.col("date") <= end)
            )
            .select(["date", "open", "high", "low", "close", "volume"])
            .sort("date")
            .collect()
        )

        bars: List[Bar] = []
        for row in df.iter_rows(named=True):
            d = row["date"]
            t = datetime.combine(d, time.min) if isinstance(d, date) else d
            bars.append(
                Bar(
                    time=t,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=int(row["volume"]),
                )
            )
        return bars

    def get_quote(self, symbol: str) -> Quote:
        """parquet 의 마지막 종가를 bid/ask 양쪽에 채워 Quote 반환 (백테스트 더미)."""
        symbol = symbol.zfill(6)
        df = (
            self._lazy.filter(pl.col("symbol") == symbol)
            .select(["date", "close"])
            .sort("date")
            .tail(1)
            .collect()
        )
        if df.is_empty():
            raise KeyError(f"symbol not found in parquet: {symbol}")

        row = df.row(0, named=True)
        d = row["date"]
        t = datetime.combine(d, time.min) if isinstance(d, date) else d
        close = float(row["close"])
        return Quote(
            time=t,
            bid_price=close,
            bid_size=0,
            ask_price=close,
            ask_size=0,
        )

    def subscribe_realtime(
        self,
        symbols: List[str],
        on_bar: Callable[[str, Bar], None],
    ) -> Subscription:
        raise NotImplementedError(
            "LocalParquetProvider 는 실시간 구독 미지원 — 실시간은 ws_realtime_trading.py 사용"
        )

    def get_stock_info(self, symbol: str) -> Optional[object]:
        return None

    def get_financial_data(self, symbol: str) -> Optional[object]:
        return None
