"""
E-1: Unit tests for evaluation/metrics.py (Sharpe, MDD, IC).
"""

import numpy as np
import pandas as pd
import pytest

from alpha_tracker2.evaluation.metrics import (
    aggregate_ic_series,
    ic,
    ic_series,
    mdd,
    sharpe,
)


# ---- Sharpe (E1-1) ----
def test_sharpe_zero_returns() -> None:
    """All zeros -> Sharpe 0."""
    assert sharpe([0.0, 0.0, 0.0]) == 0.0
    assert sharpe(pd.Series([0.0, 0.0])) == 0.0


def test_sharpe_known_sequence() -> None:
    """Hand-check: mean=0.01, std≈0.0122, ann=252 -> Sharpe ≈ 0.01/0.0122 * sqrt(252)."""
    rets = [0.01, -0.02, 0.015, 0.0, 0.02]
    s = sharpe(rets, risk_free=0.0, ann_factor=252.0)
    mean_r = np.mean(rets)
    std_r = np.std(rets)
    expected = (mean_r / std_r) * np.sqrt(252.0)
    assert abs(s - expected) < 1e-10


def test_sharpe_single_value() -> None:
    """Single return -> std 0 -> 0."""
    assert sharpe([0.01]) == 0.0


def test_sharpe_with_nan() -> None:
    """NaN dropped, rest used."""
    rets = pd.Series([0.01, np.nan, -0.01, 0.02])
    s = sharpe(rets)
    expected = sharpe([0.01, -0.01, 0.02])
    assert s == expected


# ---- MDD (E1-2) ----
def test_mdd_nav_known() -> None:
    """NAV 1, 1.1, 1.05, 0.9 -> peak 1.1, trough 0.9 -> DD = (1.1-0.9)/1.1."""
    nav = [1.0, 1.1, 1.05, 0.9, 0.95]
    d = mdd(nav, from_returns=False)
    assert abs(d - (1.1 - 0.9) / 1.1) < 1e-10


def test_mdd_from_returns() -> None:
    """Returns 0.1, -0.2, 0.1 -> NAV 1.1, 0.88, 0.968 -> peak 1.1, MDD = (1.1-0.88)/1.1."""
    rets = [0.1, -0.2, 0.1]
    d = mdd(rets, from_returns=True)
    nav = np.cumprod(1.0 + np.array(rets))
    peak = np.maximum.accumulate(nav)
    dd = (peak - nav) / peak
    expected = float(np.max(dd))
    assert abs(d - expected) < 1e-10


def test_mdd_empty() -> None:
    """Empty -> 0."""
    assert mdd([]) == 0.0
    assert mdd([], from_returns=True) == 0.0


# ---- IC (E1-3) ----
def test_ic_pearson_fixed() -> None:
    """Perfect correlation: score = fwd_ret -> IC = 1."""
    score = pd.Series([1.0, 2.0, 3.0])
    fwd = pd.Series([1.0, 2.0, 3.0])
    assert abs(ic(score, fwd, method="pearson") - 1.0) < 1e-10


def test_ic_negative_correlation() -> None:
    """Score and fwd_ret opposite -> IC = -1."""
    score = pd.Series([1.0, 2.0, 3.0])
    fwd = pd.Series([3.0, 2.0, 1.0])
    assert abs(ic(score, fwd, method="pearson") + 1.0) < 1e-10


def test_ic_spearman() -> None:
    """Monotonic but non-linear: Spearman 1, Pearson < 1."""
    score = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    fwd = pd.Series([1.0, 4.0, 9.0, 16.0, 25.0])
    assert abs(ic(score, fwd, method="spearman") - 1.0) < 1e-10
    assert ic(score, fwd, method="pearson") < 1.0


def test_ic_insufficient_points() -> None:
    """< 2 valid pairs -> nan."""
    assert np.isnan(ic(pd.Series([1.0]), pd.Series([0.5])))
    assert np.isnan(ic(pd.Series([1.0, np.nan]), pd.Series([0.5, 0.5])))


def test_ic_series() -> None:
    """Multiple dates -> DataFrame with as_of_date, ic."""
    dates = [pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-02")]
    s1 = pd.Series([1.0, 2.0, 3.0], index=["a", "b", "c"])
    r1 = pd.Series([1.0, 2.0, 3.0], index=["a", "b", "c"])
    s2 = pd.Series([2.0, 3.0], index=["a", "b"])
    r2 = pd.Series([2.0, 1.0], index=["a", "b"])
    df = ic_series(dates, [s1, s2], [r1, r2], method="pearson")
    assert list(df.columns) == ["as_of_date", "ic"]
    assert len(df) == 2
    assert abs(df.iloc[0]["ic"] - 1.0) < 1e-10
    assert abs(df.iloc[1]["ic"] + 1.0) < 1e-10


def test_aggregate_ic_series() -> None:
    """Mean, std, count."""
    ser = pd.Series([0.05, 0.1, -0.02, 0.03])
    out = aggregate_ic_series(ser)
    assert out["count"] == 4
    assert abs(out["mean_ic"] - ser.mean()) < 1e-10
    assert abs(out["std_ic"] - ser.std()) < 1e-10
