"""
Core evaluation metrics: Sharpe, MDD, IC. Pure functions for E-2, E-3, D-1.
"""

from __future__ import annotations

from typing import List, Union

import numpy as np
import pandas as pd


def sharpe(
    returns: Union[pd.Series, List[float]],
    risk_free: float = 0.0,
    ann_factor: float = 252.0,
) -> float:
    """
    Annualized Sharpe ratio from a return series.

    Args:
        returns: Period returns (e.g. daily or 5-day forward returns).
        risk_free: Per-period risk-free rate (same frequency as returns).
        ann_factor: Annualization factor (e.g. 252 for daily, 252/5 for 5-day period).

    Returns:
        Annualized Sharpe: (mean(R) - Rf) / std(R) * sqrt(ann_factor).
        Returns 0.0 if std is 0 or series is empty; no NaN in valid input.
    """
    if isinstance(returns, list):
        arr = np.asarray(returns, dtype=float)
    else:
        arr = np.asarray(returns, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return 0.0
    excess = np.nanmean(arr) - risk_free
    std = np.nanstd(arr)
    if std is None or std <= 0:
        return 0.0
    return float(excess / std * np.sqrt(ann_factor))


def mdd(
    nav_or_returns: Union[pd.Series, List[float]],
    from_returns: bool = False,
) -> float:
    """
    Maximum drawdown from a NAV series or from a return series (converted to NAV).

    Args:
        nav_or_returns: Either a NAV series (cumulative, first element typically 1.0)
            or a return series, depending on from_returns.
        from_returns: If True, nav_or_returns is treated as returns and converted
            to NAV as cumprod(1 + returns) before computing drawdown.

    Returns:
        Maximum drawdown as a fraction in [0, 1] (e.g. 0.1 = 10%).
        Drawdown at each time is (peak - current) / peak; MDD is the max of that.
        Returns 0.0 if series is empty or single point.
    """
    if isinstance(nav_or_returns, list):
        arr = np.asarray(nav_or_returns, dtype=float)
    else:
        arr = np.asarray(nav_or_returns, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return 0.0
    if from_returns:
        nav = np.cumprod(1.0 + arr)
    else:
        nav = arr.copy()
    if nav.size <= 1:
        return 0.0
    peak = np.maximum.accumulate(nav)
    dd = (peak - nav) / np.where(peak > 0, peak, np.nan)
    dd = np.nan_to_num(dd, nan=0.0, posinf=0.0, neginf=0.0)
    return float(np.max(dd))


def ic(
    score: Union[pd.Series, np.ndarray, List[float]],
    fwd_ret: Union[pd.Series, np.ndarray, List[float]],
    method: str = "pearson",
) -> float:
    """
    Information coefficient: correlation between score and forward return (single cross-section).

    Args:
        score: Cross-sectional score (e.g. model score).
        fwd_ret: Forward return for the same entities (same length/alignment).
        method: 'pearson' or 'spearman'.

    Returns:
        Correlation in [-1, 1]. Returns float('nan') if insufficient valid pairs or undefined.
    """
    s = pd.Series(score).astype(float)
    r = pd.Series(fwd_ret).astype(float)
    df = pd.DataFrame({"score": s, "fwd_ret": r}).dropna()
    if len(df) < 2:
        return float("nan")
    if method == "spearman":
        return float(df["score"].rank().corr(df["fwd_ret"].rank()))
    return float(df["score"].corr(df["fwd_ret"]))


def ic_series(
    as_of_dates: List[pd.Timestamp],
    score_series_list: List[pd.Series],
    fwd_ret_series_list: List[pd.Series],
    method: str = "pearson",
) -> pd.DataFrame:
    """
    IC time series: one IC per (as_of_date) from aligned score and fwd_ret series per date.

    Args:
        as_of_dates: List of as_of_date (length N).
        score_series_list: List of N Series (index = ticker or id, values = score).
        fwd_ret_series_list: List of N Series (same index as score for that date).

    Returns:
        DataFrame with columns as_of_date, ic. Index optional.
    """
    ics = []
    for d, sc, fr in zip(as_of_dates, score_series_list, fwd_ret_series_list):
        v = ic(sc, fr, method=method)
        ics.append({"as_of_date": d, "ic": v})
    return pd.DataFrame(ics)


def aggregate_ic_series(ic_series: pd.Series) -> dict:
    """
    Aggregate an IC time series to mean and optional std (for E-2/E-3 summary).

    Args:
        ic_series: Series of IC values (index can be date).

    Returns:
        Dict with keys: mean_ic, std_ic, count (valid count).
    """
    valid = ic_series.dropna()
    n = len(valid)
    if n == 0:
        return {"mean_ic": float("nan"), "std_ic": float("nan"), "count": 0}
    return {
        "mean_ic": float(valid.mean()),
        "std_ic": float(valid.std()) if n > 1 else 0.0,
        "count": n,
    }
