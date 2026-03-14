from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import List


@dataclass
class TradingCalendar:
    """
    Simple trading calendar for US and HK markets.

    Current implementation approximates trading days as Monday–Friday
    business days and does not model exchange-specific holidays.
    TODO: Replace with real US/HK exchange calendars (e.g. via
    pandas_market_calendars) in a later iteration.
    """

    def latest_trading_day(self, market: str = "US") -> date:
        """
        Return the latest trading day (on or before today) for the given market.
        """
        self._validate_market(market)
        today = date.today()
        d = today
        while not self._is_business_day(d):
            d -= timedelta(days=1)
        return d

    def trading_days(self, start: date, end: date, market: str = "US") -> List[date]:
        """
        Return a list of trading days between start and end (inclusive).
        """
        if end < start:
            raise ValueError("end date must be on or after start date")
        self._validate_market(market)

        days: List[date] = []
        d = start
        while d <= end:
            if self._is_business_day(d):
                days.append(d)
            d += timedelta(days=1)
        return days

    @staticmethod
    def _is_business_day(d: date) -> bool:
        # Monday=0, Sunday=6
        return d.weekday() < 5

    @staticmethod
    def _validate_market(market: str) -> None:
        if market not in {"US", "HK"}:
            raise ValueError(f"Unsupported market: {market!r}. Expected 'US' or 'HK'.")

