from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PriceFeatureConfig:
    """
    Configuration for price feature computation windows.
    """

    ret_windows: Sequence[int] = (1, 5, 10, 20)
    vol_short_window: int = 5
    vol_long_window: int = 60
    ma_windows: Sequence[int] = (5, 10, 20, 60)
    amount_window: int = 20
    ma_slope_window: int = 20
    annualization_factor: float = 252.0


def _ensure_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise the input to a MultiIndex (trade_date, ticker) sorted by both.
    """
    required_cols = {"trade_date", "ticker"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"df must contain columns {sorted(required_cols)}")

    if not isinstance(df.index, pd.MultiIndex) or df.index.names != ["trade_date", "ticker"]:
        idx = pd.MultiIndex.from_frame(
            df[["trade_date", "ticker"]].astype(
                {"trade_date": "datetime64[ns]", "ticker": "string"}
            ),
            names=["trade_date", "ticker"],
        )
        df = df.copy()
        df.index = idx

    # Ensure datetime index level for trade_date
    if not np.issubdtype(df.index.levels[0].dtype, np.datetime64):
        df = df.copy()
        df.index = df.index.set_levels(
            pd.to_datetime(df.index.levels[0]), level=0  # type: ignore[arg-type]
        )

    return df.sort_index()


def _compute_returns(adj_close: pd.Series, windows: Iterable[int]) -> dict[str, pd.Series]:
    log_price = np.log(adj_close)
    # Use log returns for stability, then exponentiate; but window is small so plain ratio is fine.
    feats: dict[str, pd.Series] = {}
    for w in windows:
        feats[f"ret_{w}d"] = adj_close / adj_close.shift(w) - 1.0
    return feats


def _rolling_volatility(
    returns: pd.Series,
    window: int,
    annualization_factor: float | None = None,
) -> pd.Series:
    vol = returns.rolling(window=window, min_periods=1).std()
    if annualization_factor is not None:
        vol = vol * np.sqrt(annualization_factor)
    return vol


def _rolling_max_drawdown(prices: pd.Series, window: int) -> pd.Series:
    """
    Rolling max drawdown over a trailing window in terms of price / rolling peak.
    """
    # Work with a rolling window via expanding then differencing cumulative max.
    rolling_mdd = pd.Series(index=prices.index, dtype="float64")
    # For simplicity and clarity we use a groupby-style apply on a rolling object
    roll = prices.rolling(window=window, min_periods=1)
    rolling_mdd[:] = roll.apply(
        lambda x: float((x / x.cummax() - 1.0).min()),
        raw=False,
    )
    return rolling_mdd


def _rolling_ma(prices: pd.Series, window: int) -> pd.Series:
    return prices.rolling(window=window, min_periods=1).mean()


def _slope(series: pd.Series, window: int) -> pd.Series:
    """
    Simple slope over a trailing window using linear regression on index order.
    """
    idx = np.arange(window, dtype="float64")

    def _fit(y: np.ndarray) -> float:
        # y is a 1d array of length window (or shorter for initial periods)
        x = idx[: len(y)]
        x_mean = x.mean()
        y_mean = y.mean()
        denom = np.sum((x - x_mean) ** 2)
        if denom == 0.0:
            return 0.0
        return float(np.sum((x - x_mean) * (y - y_mean)) / denom)

    return series.rolling(window=window, min_periods=1).apply(_fit, raw=True)


def compute_price_features(
    df: pd.DataFrame,
    *,
    trading_days: list[date],
    target_trade_date: date | None = None,
    config: PriceFeatureConfig | None = None,
) -> pd.DataFrame:
    """
    Compute price/volume features from a prices_daily-style DataFrame.

    Parameters
    ----------
    df:
        DataFrame or MultiIndex DataFrame with at least:
        ['trade_date', 'ticker', 'adj_close', 'amount'].
    trading_days:
        Ordered list of trading days for the window (ascending).
    target_trade_date:
        The trade_date to output features for. If None, defaults to the
        latest date present in df that is also in trading_days.
    config:
        Optional configuration; defaults to PriceFeatureConfig().

    Returns
    -------
    DataFrame indexed by (trade_date, ticker) with feature columns.
    Only rows for target_trade_date are returned.
    """
    if config is None:
        config = PriceFeatureConfig()

    if not trading_days:
        raise ValueError("trading_days must be a non-empty list")

    df_idx = _ensure_index(df)

    # Determine target date
    if target_trade_date is None:
        # choose the latest intersection of trading_days and df dates
        df_dates = pd.to_datetime(df_idx.index.get_level_values("trade_date")).date
        latest_df_date = max(df_dates)
        target_trade_date = latest_df_date

    target_ts = pd.Timestamp(target_trade_date)

    # Restrict to available window within trading_days
    all_dates = pd.to_datetime(trading_days)
    window_mask = all_dates <= target_ts
    if not window_mask.any():
        raise ValueError("No trading days on or before target_trade_date")

    # Use full df_idx (already sorted); rolling operations naturally use history.
    # Work per ticker to avoid cross-talk between series.
    feature_frames: list[pd.DataFrame] = []

    # We operate on a copy with float dtype to avoid int issues
    if "adj_close" not in df_idx.columns or "amount" not in df_idx.columns:
        raise ValueError("df must contain 'adj_close' and 'amount' columns")

    grouped = df_idx[["adj_close", "amount"]].groupby(level="ticker", sort=False)

    for ticker, g in grouped:
        g = g.sort_index()
        prices = g["adj_close"].astype("float64")

        # Returns
        ret_feats = _compute_returns(prices, config.ret_windows)
        ret_1d = ret_feats["ret_1d"]

        # Volatility
        vol_5d = _rolling_volatility(ret_1d, window=config.vol_short_window, annualization_factor=None)
        vol_ann_60d = _rolling_volatility(
            ret_1d,
            window=config.vol_long_window,
            annualization_factor=config.annualization_factor,
        )

        # Max drawdown on price
        mdd_60d = _rolling_max_drawdown(prices, window=config.vol_long_window)

        # Moving averages
        ma_cols: dict[str, pd.Series] = {}
        for w in config.ma_windows:
            ma_cols[f"ma{w}"] = _rolling_ma(prices, window=w)

        ma5 = ma_cols["ma5"]
        ma10 = ma_cols["ma10"]
        ma20 = ma_cols["ma20"]
        ma60 = ma_cols["ma60"]

        ma5_gt_ma10_gt_ma20 = (ma5 > ma10) & (ma10 > ma20)
        ma20_above_ma60 = ma20 > ma60

        # Slope of ma20 over last ma_slope_window days
        ma20_slope = _slope(ma20, window=config.ma_slope_window)

        # Liquidity
        avg_amount_20 = g["amount"].astype("float64").rolling(
            window=config.amount_window,
            min_periods=1,
        ).mean()

        feats_df = pd.DataFrame(
            {
                "ret_1d": ret_feats["ret_1d"],
                "ret_5d": ret_feats["ret_5d"],
                "ret_10d": ret_feats["ret_10d"],
                "ret_20d": ret_feats["ret_20d"],
                "vol_5d": vol_5d,
                "vol_ann_60d": vol_ann_60d,
                "mdd_60d": mdd_60d,
                "ma5": ma5,
                "ma10": ma10,
                "ma20": ma20,
                "ma60": ma60,
                "ma5_gt_ma10_gt_ma20": ma5_gt_ma10_gt_ma20.astype("boolean"),
                "ma20_above_ma60": ma20_above_ma60.astype("boolean"),
                "ma20_slope": ma20_slope,
                "avg_amount_20": avg_amount_20,
            },
            index=g.index,
        )
        feature_frames.append(feats_df)

    if not feature_frames:
        return pd.DataFrame(
            columns=[
                "ret_1d",
                "ret_5d",
                "ret_10d",
                "ret_20d",
                "vol_5d",
                "vol_ann_60d",
                "mdd_60d",
                "ma5",
                "ma10",
                "ma20",
                "ma60",
                "ma5_gt_ma10_gt_ma20",
                "ma20_above_ma60",
                "ma20_slope",
                "avg_amount_20",
            ]
        )

    all_feats = pd.concat(feature_frames).sort_index()

    # Filter to target_trade_date only (normalize for robust comparison: datetime64, tz-aware, etc.)
    idx_vals = all_feats.index.get_level_values("trade_date")
    target_normalized = pd.Timestamp(target_trade_date).normalize()
    idx_normalized = pd.to_datetime(idx_vals, utc=False).normalize()
    mask_target = idx_normalized == target_normalized
    result = all_feats[mask_target].copy()
    # Ensure (trade_date, ticker) as columns for easier DB write later
    result.reset_index(inplace=True)
    result.rename(columns={"trade_date": "trade_date", "ticker": "ticker"}, inplace=True)

    return result.set_index(["trade_date", "ticker"])


__all__ = ["PriceFeatureConfig", "compute_price_features"]

