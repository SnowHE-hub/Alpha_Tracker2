"""
Dashboard data aggregation: load nav_daily, eval_5d_daily, picks_daily from store
for a date range. Used by make_dashboard and can be reused by Streamlit etc.

Extended (D-1): build_eval_summary aggregates eval_5d_daily + ic_series.csv into
eval_summary.csv columns: version, mean_fwd_ret_5d, mean_ic, n_dates.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
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


def build_eval_summary(
    store: "DuckDBStore",
    start: date,
    end: date,
    ic_series_csv_path: Path | None = None,
) -> pd.DataFrame:
    """
    Build eval_summary DataFrame for [start, end]: version, mean_fwd_ret_5d, mean_ic, n_dates.

    - mean_fwd_ret_5d, n_dates: from eval_5d_daily (bucket='all') aggregated by version.
    - mean_ic: from ic_series CSV if path exists and is readable; else None for that column.

    Output columns: version, mean_fwd_ret_5d, mean_ic, n_dates.
    """
    start_str = start.isoformat()
    end_str = end.isoformat()
    rows = store.fetchall(
        """
        SELECT version,
               AVG(fwd_ret_5d) AS mean_fwd_ret_5d,
               COUNT(DISTINCT as_of_date) AS n_dates
        FROM eval_5d_daily
        WHERE as_of_date >= ? AND as_of_date <= ?
          AND bucket = 'all'
        GROUP BY version
        """,
        [start_str, end_str],
    )
    if not rows:
        df = pd.DataFrame(columns=["version", "mean_fwd_ret_5d", "mean_ic", "n_dates"])
    else:
        df = pd.DataFrame(rows, columns=["version", "mean_fwd_ret_5d", "n_dates"])
        df["mean_ic"] = None

    if ic_series_csv_path is not None and ic_series_csv_path.is_file():
        try:
            ic_df = pd.read_csv(ic_series_csv_path)
            if not ic_df.empty and "version" in ic_df.columns and "ic" in ic_df.columns:
                ic_df["as_of_date"] = ic_df["as_of_date"].astype(str)
                ic_in_range = ic_df[
                    (ic_df["as_of_date"] >= start_str) & (ic_df["as_of_date"] <= end_str)
                ]
                mean_ic = ic_in_range.groupby("version")["ic"].mean().reindex(df["version"])
                df["mean_ic"] = mean_ic.values
        except Exception:
            pass

    return df
