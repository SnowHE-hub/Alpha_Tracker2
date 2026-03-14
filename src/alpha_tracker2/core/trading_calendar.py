"""
Real US (NYSE/NASDAQ) and HK (HKEX) exchange trading calendars.

Uses pandas_market_calendars: NYSE for US, XHKG for Hong Kong.
Effective range: depends on the underlying library (typically 1990s–present).
Outside that range the library may have no data; docstring documents the contract.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List, Tuple

import pandas as pd

try:
    import pandas_market_calendars as mcal
except ImportError as e:
    raise ImportError(
        "TradingCalendar requires pandas_market_calendars. "
        "Install with: pip install pandas_market_calendars"
    ) from e


def _calendar_for_market(market: str):
    if market == "US":
        return mcal.get_calendar("NYSE")
    if market == "HK":
        return mcal.get_calendar("XHKG")
    raise ValueError(f"Unsupported market: {market!r}. Expected 'US' or 'HK'.")


def _schedule_to_dates(schedule: pd.DataFrame) -> List[date]:
    if schedule is None or schedule.empty:
        return []
    # schedule index is timezone-aware; normalize to date in UTC then convert to date
    return [pd.Timestamp(ts).date() for ts in schedule.index]


class TradingCalendar:
    """
    US (NYSE) and HK (XHKG) exchange trading calendar.

    Public interface is stable: latest_trading_day(market), trading_days(start, end, market).
    Trading days are cached per (market, start, end) to avoid repeated computation.
    """

    _CACHE: Dict[Tuple[str, date, date], List[date]] = {}
    _MAX_CACHE_ENTRIES = 256

    def latest_trading_day(self, market: str = "US") -> date:
        """
        Return the latest trading day on or before today for the given market.
        If today is a trading day, returns today.
        """
        self._validate_market(market)
        today = date.today()
        # Request a window that always contains at least one session
        start = today - timedelta(days=30)
        days = self.trading_days(start, today, market)
        if not days:
            # Fallback: go further back (e.g. long holiday)
            start = today - timedelta(days=365)
            days = self.trading_days(start, today, market)
        if not days:
            raise RuntimeError(
                f"No trading days found for {market} up to {today}. "
                "Check pandas_market_calendars date range."
            )
        return days[-1]

    def trading_days(self, start: date, end: date, market: str = "US") -> List[date]:
        """
        Return trading days in [start, end] (inclusive), ascending order.
        Supports market "US" (NYSE) and "HK" (XHKG).
        """
        if end < start:
            raise ValueError("end date must be on or after start date")
        self._validate_market(market)
        cache_key = (market, start, end)
        if cache_key in self._CACHE:
            return self._CACHE[cache_key]
        cal = _calendar_for_market(market)
        schedule = cal.schedule(
            start_date=pd.Timestamp(start),
            end_date=pd.Timestamp(end),
        )
        out = _schedule_to_dates(schedule)
        # Prune cache if too large (simple FIFO by clearing when over limit)
        if len(self._CACHE) >= self._MAX_CACHE_ENTRIES:
            self._CACHE.clear()
        self._CACHE[cache_key] = out
        return out

    @staticmethod
    def _validate_market(market: str) -> None:
        if market not in {"US", "HK"}:
            raise ValueError(f"Unsupported market: {market!r}. Expected 'US' or 'HK'.")
