from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol, List, Optional


@dataclass(frozen=True)
class PriceRow:
    trade_date: date
    ticker: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    amount: float | None


class PriceProvider(Protocol):
    name: str

    def fetch_prices(
        self,
        ticker: str,
        start: date,
        end: date,
    ) -> List[PriceRow]:
        ...
