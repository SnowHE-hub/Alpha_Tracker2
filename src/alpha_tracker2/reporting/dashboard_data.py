"""
Dashboard data aggregation: load nav_daily, eval_5d_daily, picks_daily from store
for a date range. Used by make_dashboard and can be reused by Streamlit etc.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from alpha_tracker2.storage.duckdb_store import DuckDBStore


def load_nav_for_dashboard(
    store: "DuckDBStore",
    start: date,
    end: date,
) -> pd.DataFrame:
    """Load nav_daily in [start, end]. Columns: trade_date, portfolio, nav, ret."""
    rows = store.fetchall(
        """
        SELECT trade_date, portfolio, nav, ret
        FROM nav_daily
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date, portfolio
        """,
        [start.isoformat(), end.isoformat()],
    )
    if not rows:
        return pd.DataFrame(columns=["trade_date", "portfolio", "nav", "ret"])
    df = pd.DataFrame(rows, columns=["trade_date", "portfolio", "nav", "ret"])
    return df


def load_eval_for_dashboard(
    store: "DuckDBStore",
    start: date,
    end: date,
) -> pd.DataFrame:
    """Load eval_5d_daily with as_of_date in [start, end]. Columns: as_of_date, version, bucket, fwd_ret_5d, n_picks, horizon."""
    rows = store.fetchall(
        """
        SELECT as_of_date, version, bucket, fwd_ret_5d, n_picks, horizon
        FROM eval_5d_daily
        WHERE as_of_date >= ? AND as_of_date <= ?
        ORDER BY as_of_date, version, bucket
        """,
        [start.isoformat(), end.isoformat()],
    )
    if not rows:
        return pd.DataFrame(
            columns=["as_of_date", "version", "bucket", "fwd_ret_5d", "n_picks", "horizon"]
        )
    df = pd.DataFrame(
        rows,
        columns=["as_of_date", "version", "bucket", "fwd_ret_5d", "n_picks", "horizon"],
    )
    return df


def load_picks_for_dashboard(
    store: "DuckDBStore",
    start: date,
    end: date,
) -> pd.DataFrame:
    """Load picks_daily with trade_date in [start, end]. Standard columns for export."""
    rows = store.fetchall(
        """
        SELECT trade_date, version, ticker, name, rank, score, score_100, reason, thr_value, pass_thr, picked_by
        FROM picks_daily
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date, version, rank NULLS LAST, ticker
        """,
        [start.isoformat(), end.isoformat()],
    )
    cols = [
        "trade_date", "version", "ticker", "name", "rank", "score", "score_100",
        "reason", "thr_value", "pass_thr", "picked_by",
    ]
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows, columns=cols)
