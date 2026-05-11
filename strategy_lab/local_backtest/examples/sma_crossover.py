"""SMA 크로스오버 전략 — 첫 검증용 단순 일봉 전략.

규칙:
    - short SMA > long SMA 이고 전일 반대이면 BUY
    - short SMA < long SMA 이고 전일 반대이면 SELL
"""

from __future__ import annotations

import polars as pl

from strategy_lab.local_backtest.strategy import Strategy


class SmaCrossoverStrategy(Strategy):
    name = "sma_crossover"

    def __init__(self, short_window: int = 10, long_window: int = 30) -> None:
        if short_window >= long_window:
            raise ValueError("short_window < long_window 이어야 함")
        self.short = short_window
        self.long = long_window

    def generate_signals(self, bars: pl.DataFrame) -> pl.DataFrame:
        return (
            bars.sort(["symbol", "date"])
            .with_columns(
                [
                    pl.col("close")
                    .rolling_mean(self.short)
                    .over("symbol")
                    .alias("sma_short"),
                    pl.col("close")
                    .rolling_mean(self.long)
                    .over("symbol")
                    .alias("sma_long"),
                ]
            )
            .with_columns((pl.col("sma_short") > pl.col("sma_long")).alias("bullish"))
            .with_columns(pl.col("bullish").shift(1).over("symbol").alias("prev_bullish"))
            .with_columns(
                pl.when(pl.col("bullish") & ~pl.col("prev_bullish").fill_null(False))
                .then(pl.lit("BUY"))
                .when(~pl.col("bullish") & pl.col("prev_bullish").fill_null(False))
                .then(pl.lit("SELL"))
                .otherwise(pl.lit("HOLD"))
                .alias("action")
            )
            .with_columns(pl.lit(1.0).alias("strength"))
            .select(["date", "symbol", "action", "strength"])
        )
