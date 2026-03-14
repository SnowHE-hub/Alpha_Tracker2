"""
Compute forward N-day returns from prices_daily.adj_close for a given signal date and ticker list.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING, List

import pandas as pd

from alpha_tracker2.core.trading_calendar import TradingCalendar

if TYPE_CHECKING:
    from alpha_tracker2.storage.duckdb_store import DuckDBStore


def compute_forward_returns(
    store: "DuckDBStore",
    as_of_date: date,
    tickers: List[str],
    horizon: int = 5,
    market: str = "US",
) -> pd.DataFrame:
    """
    Given a signal date and list of tickers, compute forward N-day return using
    prices_daily.adj_close. Uses the trading calendar to define the N-th trading day.

    **Signal-day semantics**: If as_of_date is not a trading day, the **first trading
    day on or after as_of_date** is used as the start date. The buy price is the
    close (adj_close) on that start date; end_date is the horizon-th trading day after
    start_date. So the forward return is always "start_date close → start_date + horizon
    trading days close".

    Returns a DataFrame with at least columns: ticker, fwd_ret (and optionally fwd_ret_5d
    for horizon=5). Index may be ticker. Tickers with insufficient future data get NaN
    or are omitted.
    """
    if not tickers:
        return pd.DataFrame(columns=["ticker", "fwd_ret", "fwd_ret_5d"])

    cal = TradingCalendar()
    # Window: from as_of_date enough calendar days to cover horizon+1 trading days
    end_cal = as_of_date + timedelta(days=max(horizon * 3, 30))
    days = cal.trading_days(as_of_date, end_cal, market=market)
    if len(days) < horizon + 1:
        # Not enough trading days ahead
        return _empty_result(tickers)

    start_date = days[0]
    end_date = days[horizon]
    # start_date is first trading day on or after as_of_date; end_date is horizon-th after that

    placeholders = ",".join(["?"] * len(tickers))
    params: list[object] = [start_date.isoformat(), end_date.isoformat(), *tickers]
    rows = store.fetchall(
        f"""
        SELECT trade_date, ticker, adj_close
        FROM prices_daily
        WHERE trade_date IN (?, ?)
          AND ticker IN ({placeholders})
        ORDER BY ticker, trade_date
        """,
        params,
    )

    if not rows:
        return _empty_result(tickers)

    df = pd.DataFrame(rows, columns=["trade_date", "ticker", "adj_close"])
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    out_rows = []
    for t in tickers:
        sub = df[df["ticker"] == t]
        start_row = sub[sub["trade_date"] == start_date]
        end_row = sub[sub["trade_date"] == end_date]
        if start_row.empty or end_row.empty:
            fwd_ret = float("nan")
        else:
            p0 = float(start_row["adj_close"].iloc[0])
            p1 = float(end_row["adj_close"].iloc[0])
            if p0 and p0 > 0:
                fwd_ret = (p1 / p0) - 1.0
            else:
                fwd_ret = float("nan")
        out_rows.append({"ticker": t, "fwd_ret": fwd_ret})

    out = pd.DataFrame(out_rows)
    out["fwd_ret_5d"] = out["fwd_ret"]
    return out


def _empty_result(tickers: List[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {"ticker": tickers, "fwd_ret": [float("nan")] * len(tickers), "fwd_ret_5d": [float("nan")] * len(tickers)}
    )
