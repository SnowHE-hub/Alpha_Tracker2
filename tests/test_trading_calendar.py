"""
Unit tests for TradingCalendar (I-3): real US/HK exchange calendars.

Uses known holidays: US 2024-07-04 (Independence Day), HK Lunar New Year 2024.
"""

from datetime import date

import pytest

from alpha_tracker2.core.trading_calendar import TradingCalendar


def test_trading_days_us_excludes_independence_day_2024() -> None:
    """I3-2: 2024-07-04 (US Independence Day) is not a US trading day."""
    cal = TradingCalendar()
    days = cal.trading_days(date(2024, 7, 1), date(2024, 7, 5), "US")
    assert date(2024, 7, 4) not in days
    assert date(2024, 7, 3) in days
    assert date(2024, 7, 5) in days


def test_latest_trading_day_us_on_holiday_patched(monkeypatch: pytest.MonkeyPatch) -> None:
    """I3-2: When 'today' is 2024-07-04 (US holiday), latest_trading_day('US') returns 2024-07-03."""
    from datetime import date as real_date

    class FakeDate(real_date):
        @staticmethod
        def today() -> real_date:
            return real_date(2024, 7, 4)

    monkeypatch.setattr("alpha_tracker2.core.trading_calendar.date", FakeDate)
    cal = TradingCalendar()
    result = cal.latest_trading_day("US")
    assert result == date(2024, 7, 3)


def test_trading_days_hk_excludes_lunar_new_year() -> None:
    """I3-3: At least one known HKEX holiday (e.g. Lunar New Year 2024) is excluded."""
    cal = TradingCalendar()
    # 2024-02-10 and 2024-02-12 are typical HKEX Lunar New Year closures
    days = cal.trading_days(date(2024, 2, 8), date(2024, 2, 16), "HK")
    # At least one of these should be missing
    closed = [date(2024, 2, 10), date(2024, 2, 12), date(2024, 2, 13)]
    assert any(d not in days for d in closed), "At least one HKEX LNY date should be closed"


def test_trading_days_ascending_and_inclusive() -> None:
    """trading_days returns ascending list, start/end inclusive when they are trading days."""
    cal = TradingCalendar()
    days = cal.trading_days(date(2024, 7, 1), date(2024, 7, 5), "US")
    assert len(days) >= 1
    assert days == sorted(days)
    assert date(2024, 7, 1) in days
    assert date(2024, 7, 5) in days


def test_latest_trading_day_hk_returns_date() -> None:
    """latest_trading_day('HK') returns a date instance."""
    cal = TradingCalendar()
    d = cal.latest_trading_day("HK")
    assert isinstance(d, date)


def test_trading_days_invalid_market_raises() -> None:
    """Unsupported market raises ValueError."""
    cal = TradingCalendar()
    with pytest.raises(ValueError, match="Unsupported market"):
        cal.trading_days(date(2024, 1, 1), date(2024, 1, 10), "XX")
    with pytest.raises(ValueError, match="Unsupported market"):
        cal.latest_trading_day("XX")


def test_trading_days_end_before_start_raises() -> None:
    """end < start raises ValueError."""
    cal = TradingCalendar()
    with pytest.raises(ValueError, match="end date"):
        cal.trading_days(date(2024, 1, 10), date(2024, 1, 1), "US")
