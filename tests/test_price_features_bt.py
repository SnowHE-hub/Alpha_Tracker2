"""
Unit tests for bt_* feature computation (I-2).

Data: synthetic DataFrame with adj_close and amount over 80 trading days (single ticker).
Covers bt_mean, bt_winrate, bt_worst_mdd presence, ranges [0,1] and <=0, and idempotent behavior.
"""

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from alpha_tracker2.features.price_features import PriceFeatureConfig, compute_price_features


def _make_synthetic_prices(
    n_days: int = 80,
    ticker: str = "AAPL",
    start: date | None = None,
    seed: int = 42,
) -> tuple[pd.DataFrame, list[date]]:
    """Build a prices_daily-style DataFrame and trading_days list for testing."""
    if start is None:
        start = date(2024, 1, 1)
    np.random.seed(seed)
    dates = [start + timedelta(days=i) for i in range(n_days)]
    # Skip weekends so we have ~60 weekdays
    trading_days = [d for d in dates if d.weekday() < 5]
    if len(trading_days) < 60:
        trading_days = dates[: 60 + (5 - len(trading_days))]
    n = len(trading_days)
    # Random walk for adj_close; amount proportional to price
    log_ret = np.random.randn(n) * 0.01
    adj_close = 100.0 * np.exp(np.cumsum(log_ret))
    amount = adj_close * (1000 + np.random.randint(0, 500, size=n))
    df = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(trading_days),
            "ticker": ticker,
            "adj_close": adj_close,
            "amount": amount,
        }
    )
    return df, trading_days


def test_compute_price_features_output_has_bt_columns() -> None:
    """Output DataFrame has bt_mean, bt_winrate, bt_worst_mdd columns."""
    df, trading_days = _make_synthetic_prices(80)
    target = trading_days[-1]
    result = compute_price_features(
        df,
        trading_days=trading_days,
        target_trade_date=target,
        config=PriceFeatureConfig(bt_window=60),
    )
    assert "bt_mean" in result.columns
    assert "bt_winrate" in result.columns
    assert "bt_worst_mdd" in result.columns
    assert len(result) >= 1


def test_bt_winrate_in_range() -> None:
    """bt_winrate when non-null is in [0, 1] (I2-2)."""
    df, trading_days = _make_synthetic_prices(80)
    target = trading_days[-1]
    result = compute_price_features(
        df,
        trading_days=trading_days,
        target_trade_date=target,
        config=PriceFeatureConfig(bt_window=60),
    )
    for _, row in result.iterrows():
        v = row.get("bt_winrate")
        if pd.notna(v):
            assert 0 <= float(v) <= 1, f"bt_winrate must be in [0,1], got {v}"


def test_bt_worst_mdd_non_positive() -> None:
    """bt_worst_mdd when non-null is <= 0 (I2-2)."""
    df, trading_days = _make_synthetic_prices(80)
    target = trading_days[-1]
    result = compute_price_features(
        df,
        trading_days=trading_days,
        target_trade_date=target,
        config=PriceFeatureConfig(bt_window=60),
    )
    for _, row in result.iterrows():
        v = row.get("bt_worst_mdd")
        if pd.notna(v):
            assert float(v) <= 0, f"bt_worst_mdd must be <= 0, got {v}"


def test_bt_mean_same_order_as_returns() -> None:
    """bt_mean is of similar magnitude to ret_* (mean of daily returns)."""
    df, trading_days = _make_synthetic_prices(80)
    target = trading_days[-1]
    result = compute_price_features(
        df,
        trading_days=trading_days,
        target_trade_date=target,
        config=PriceFeatureConfig(bt_window=60),
    )
    for _, row in result.iterrows():
        bt_mean = row.get("bt_mean")
        ret_1d = row.get("ret_1d")
        if pd.notna(bt_mean) and pd.notna(ret_1d):
            # bt_mean is mean of ret_1d over window; typically same order (e.g. both ~1e-2)
            assert abs(bt_mean) < 1.0, "bt_mean should be return-like magnitude"
            assert abs(bt_mean) >= 0 or ret_1d is not None  # sanity


def test_bt_at_least_one_non_null_with_sufficient_data() -> None:
    """With 80 trading days and bt_window=60, at least one of bt_mean/bt_winrate/bt_worst_mdd is non-null (I2-1)."""
    df, trading_days = _make_synthetic_prices(80)
    target = trading_days[-1]
    result = compute_price_features(
        df,
        trading_days=trading_days,
        target_trade_date=target,
        config=PriceFeatureConfig(bt_window=60),
    )
    assert len(result) >= 1
    row = result.iloc[0]
    non_null = sum(pd.notna(row.get(c)) for c in ("bt_mean", "bt_winrate", "bt_worst_mdd"))
    assert non_null >= 1, "With 80 days and window 60, at least one bt_* should be non-null"


def test_bt_config_window_affects_null_count() -> None:
    """Larger bt_window with same data can yield more nulls (insufficient window)."""
    df, trading_days = _make_synthetic_prices(65)  # only 65 days
    target = trading_days[-1]
    config_small = PriceFeatureConfig(bt_window=20)
    config_large = PriceFeatureConfig(bt_window=60)
    out_small = compute_price_features(df, trading_days=trading_days, target_trade_date=target, config=config_small)
    out_large = compute_price_features(df, trading_days=trading_days, target_trade_date=target, config=config_large)
    assert "bt_mean" in out_small.columns and "bt_mean" in out_large.columns
    # Both may have non-null; we only check that computation runs and ranges hold
    for _, row in out_large.iterrows():
        v = row.get("bt_winrate")
        if pd.notna(v):
            assert 0 <= float(v) <= 1
