"""
Ingestion protocols and DTOs.

Defines UniverseProvider and PriceProvider interfaces plus lightweight
data transfer objects. No Yahoo or business logic here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol


@dataclass
class UniverseRow:
    ticker: str
    name: str
    market: str  # "US" / "HK"


@dataclass
class PriceRow:
    trade_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    adj_close: float | None
    volume: int | None
    amount: float | None
    currency: str | None


class UniverseProvider(Protocol):
    def fetch_universe(self, trade_date: date) -> list[UniverseRow]: ...


class PriceProvider(Protocol):
    def fetch_prices(self, ticker: str, start: date, end: date) -> list[PriceRow]: ...
