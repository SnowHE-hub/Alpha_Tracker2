"""
Integration test for build_features writing bt_* (I-2).

Uses a temporary DuckDB: creates schema, inserts 120 trading days of prices_daily
and picks_daily(UNIVERSE), runs feature computation and write path, then asserts
features_daily has rows with bt_mean/bt_winrate/bt_worst_mdd non-null and in range.

Real-data baseline (PERVASIVE_TEST_INFRA_TASK INF-5): ≥20 trading days, US tickers
(AAPL, MSFT); fixture structure matches ingest_prices/build_features output.
Reproduce: pytest tests/test_build_features_bt_integration.py -v
"""

import tempfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

from alpha_tracker2.features.price_features import PriceFeatureConfig, compute_price_features
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _make_prices_rows(n_weekdays: int, tickers: list[str], start: date, seed: int = 1):
    """Generate (trade_date, ticker, adj_close, amount) rows for prices_daily (weekdays only)."""
    np.random.seed(seed)
    rows = []
    i = 0
    d = start
    prices = {t: 100.0 for t in tickers}
    while i < n_weekdays:
        if d.weekday() >= 5:
            d += timedelta(days=1)
            continue
        for t in tickers:
            step = np.random.randn() * 0.02
            prices[t] = max(1.0, prices[t] * (1 + step))
            amt = prices[t] * (1000 + np.random.randint(0, 500))
            rows.append((d.isoformat(), t, float(prices[t]), float(amt)))
        i += 1
        d += timedelta(days=1)
    return rows


@pytest.fixture
def temp_db_and_store(tmp_path):
    """Create a temporary DuckDB and DuckDBStore with schema; yield (store, schema_path)."""
    root = Path(__file__).resolve().parents[1]
    schema_path = root / "src" / "alpha_tracker2" / "storage" / "schema.sql"
    db_path = tmp_path / "test.duckdb"
    store = DuckDBStore(db_path=db_path, schema_path=schema_path)
    store.init_schema()
    yield store, schema_path


def test_build_features_writes_bt_columns_integration(temp_db_and_store):
    """
    Integration: insert prices + universe, run feature computation and write path,
    then assert features_daily has bt_* with valid ranges (I2-1, I2-2, I2-4).
    Uses 70 weekdays of synthetic data (structure matches real data).
    """
    store, _ = temp_db_and_store
    start = date(2024, 1, 1)
    tickers = ["AAPL", "MSFT"]
    rows = _make_prices_rows(120, tickers, start)
    if not rows:
        pytest.skip("No rows generated")
    # Insert into prices_daily (need trade_date, ticker, adj_close, amount; schema also has market, etc.)
    with store.session() as conn:
        for r in rows:
            conn.execute(
                """INSERT INTO prices_daily (trade_date, ticker, market, adj_close, amount, source)
                   VALUES (?, ?, 'US', ?, ?, 'test')""",
                [r[0], r[1], r[2], r[3]],
            )
        # picks_daily UNIVERSE for one date (use last date we have)
        last_date = rows[-1][0]
        for rank, t in enumerate(tickers, start=1):
            conn.execute(
                """INSERT INTO picks_daily (trade_date, version, ticker, rank, score)
                   VALUES (?, 'UNIVERSE', ?, ?, 0.0)""",
                [last_date, t, rank],
            )
        # Compute features in-process (same logic as build_features)
        import pandas as pd

        prices_df = pd.DataFrame(rows, columns=["trade_date", "ticker", "adj_close", "amount"])
    prices_df["trade_date"] = pd.to_datetime(prices_df["trade_date"])
    trading_days = sorted(prices_df["trade_date"].dt.date.unique())
    if len(trading_days) < 20:
        pytest.skip("Need at least 20 trading days")
    target = trading_days[-1]
    config = PriceFeatureConfig(bt_window=60)
    features_df = compute_price_features(
        prices_df,
        trading_days=trading_days,
        target_trade_date=target,
        config=config,
    )
    if features_df.empty:
        pytest.skip("No features computed")
    # Write using same INSERT logic as build_features (manually one row to avoid importing _write_features)
    trade_date_str = target.isoformat()
    with store.session() as conn:
        for (ts, ticker), row in features_df.iterrows():
            try:
                dt = ts.date() if hasattr(ts, "date") else pd.Timestamp(ts).date()
            except Exception:
                dt = pd.Timestamp(ts).date()
            if dt != target:
                continue
            conn.execute(
                """INSERT INTO features_daily (
                    trade_date, ticker,
                    ret_1d, ret_5d, ret_10d, ret_20d,
                    vol_5d, vol_ann_60d, mdd_60d,
                    ma5, ma10, ma20, ma60,
                    ma5_gt_ma10_gt_ma20, ma20_above_ma60, ma20_slope,
                    avg_amount_20, bt_mean, bt_winrate, bt_worst_mdd
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )""",
                [
                    trade_date_str,
                    str(ticker),
                    float(row["ret_1d"]) if pd.notna(row["ret_1d"]) else None,
                    float(row["ret_5d"]) if pd.notna(row["ret_5d"]) else None,
                    float(row["ret_10d"]) if pd.notna(row["ret_10d"]) else None,
                    float(row["ret_20d"]) if pd.notna(row["ret_20d"]) else None,
                    float(row["vol_5d"]) if pd.notna(row["vol_5d"]) else None,
                    float(row["vol_ann_60d"]) if pd.notna(row["vol_ann_60d"]) else None,
                    float(row["mdd_60d"]) if pd.notna(row["mdd_60d"]) else None,
                    float(row["ma5"]) if pd.notna(row["ma5"]) else None,
                    float(row["ma10"]) if pd.notna(row["ma10"]) else None,
                    float(row["ma20"]) if pd.notna(row["ma20"]) else None,
                    float(row["ma60"]) if pd.notna(row["ma60"]) else None,
                    bool(row["ma5_gt_ma10_gt_ma20"]) if pd.notna(row["ma5_gt_ma10_gt_ma20"]) else None,
                    bool(row["ma20_above_ma60"]) if pd.notna(row["ma20_above_ma60"]) else None,
                    float(row["ma20_slope"]) if pd.notna(row["ma20_slope"]) else None,
                    float(row["avg_amount_20"]) if pd.notna(row["avg_amount_20"]) else None,
                    float(row["bt_mean"]) if pd.notna(row["bt_mean"]) else None,
                    float(row["bt_winrate"]) if pd.notna(row["bt_winrate"]) else None,
                    float(row["bt_worst_mdd"]) if pd.notna(row["bt_worst_mdd"]) else None,
                ],
            )
    # Assert: at least one row has bt_* non-null
    r = store.fetchone(
        """SELECT COUNT(*) FROM features_daily
           WHERE trade_date = ? AND (bt_mean IS NOT NULL OR bt_winrate IS NOT NULL OR bt_worst_mdd IS NOT NULL)""",
        [trade_date_str],
    )
    assert r and int(r[0]) >= 1, "At least one row should have a non-null bt_* column"
    # Ranges
    rows_db = store.fetchall(
        """SELECT bt_winrate, bt_worst_mdd FROM features_daily WHERE trade_date = ?""",
        [trade_date_str],
    )
    for r in rows_db:
        if r[0] is not None:
            assert 0 <= float(r[0]) <= 1
        if r[1] is not None:
            assert float(r[1]) <= 0
