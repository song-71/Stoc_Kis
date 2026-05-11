"""Strategy 추상 클래스 — 일봉 시그널 생성기."""

from __future__ import annotations

from abc import ABC, abstractmethod

import polars as pl


class Strategy(ABC):
    """일봉 OHLCV 입력 → 매수/매도 시그널 출력.

    구현 규약:
        generate_signals 는 bars 와 동일한 (date, symbol) 순서를 가지는 DataFrame 을 반환.
        필수 컬럼: date, symbol, action ("BUY" | "SELL" | "HOLD")
        선택 컬럼: strength (float, default 1.0), reason (str, default "")
    """

    name: str = "Strategy"

    @abstractmethod
    def generate_signals(self, bars: pl.DataFrame) -> pl.DataFrame: ...
