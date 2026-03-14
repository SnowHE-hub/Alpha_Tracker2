"""
Version comparison and factor analysis for E-3. Outputs CSV to data/out for D-1.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

import pandas as pd

from alpha_tracker2.evaluation.forward_returns import compute_forward_returns
from alpha_tracker2.evaluation.metrics import ic

if TYPE_CHECKING:
    from alpha_tracker2.storage.duckdb_store import DuckDBStore


# Output schema (for E-3 and D-1): column names and semantics documented here.
# version_compare.csv: version, mean_fwd_ret_5d, avg_n_picks, n_dates, n_pick_days
# factor_analysis.csv: factor_name, mean_ic, n_dates (optional: std_ic)


def run_version_compare(
    store: "DuckDBStore",
    start: date,
    end: date,
    versions: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Compare versions over [start, end] using eval_5d_daily and picks_daily.

    Returns a DataFrame with columns: version, mean_fwd_ret_5d, avg_n_picks, n_dates, n_pick_days.
    """
    start_str = start.isoformat()
    end_str = end.isoformat()

    # Eval: mean fwd_ret_5d and avg n_picks per version (bucket=all)
    rows = store.fetchall(
        """
        SELECT version, AVG(fwd_ret_5d) AS mean_fwd_ret_5d, AVG(n_picks) AS avg_n_picks, COUNT(DISTINCT as_of_date) AS n_dates
        FROM eval_5d_daily
        WHERE as_of_date BETWEEN ? AND ?
          AND bucket = 'all'
        GROUP BY version
        """,
        [start_str, end_str],
    )
    if not rows:
        return pd.DataFrame(
            columns=["version", "mean_fwd_ret_5d", "avg_n_picks", "n_dates", "n_pick_days"]
        )
    df = pd.DataFrame(
        rows,
        columns=["version", "mean_fwd_ret_5d", "avg_n_picks", "n_dates"],
    )

    # Picks: count of days each version has at least one pick
    pick_days = store.fetchall(
        """
        SELECT version, COUNT(DISTINCT trade_date) AS n_pick_days
        FROM picks_daily
        WHERE trade_date BETWEEN ? AND ?
        GROUP BY version
        """,
        [start_str, end_str],
    )
    pd_df = pd.DataFrame(pick_days, columns=["version", "n_pick_days"])
    df = df.merge(pd_df, on="version", how="left").fillna(0)
    df["n_pick_days"] = df["n_pick_days"].astype(int)

    if versions:
        df = df[df["version"].isin(versions)]
    return df


def run_factor_analysis(
    store: "DuckDBStore",
    start: date,
    end: date,
    factor_columns: Optional[List[str]] = None,
    horizon: int = 5,
    max_dates: int = 100,
) -> pd.DataFrame:
    """
    Factor-vs-fwd_ret IC over [start, end]. Uses picks_daily (score) + forward_returns + features_daily.

    Returns DataFrame with columns: factor_name, mean_ic, std_ic, n_dates.
    """
    from alpha_tracker2.core.trading_calendar import TradingCalendar

    cal = TradingCalendar()
    days = cal.trading_days(start, end, market="US")
    if max_dates and len(days) > max_dates:
        # Sample evenly
        step = len(days) // max_dates or 1
        days = days[::step][:max_dates]

    if factor_columns is None:
        factor_columns = ["score", "ret_5d", "bt_mean"]

    # features_daily columns we can use (must exist in schema)
    feature_cols = ["ret_5d", "ret_10d", "ret_20d", "bt_mean", "bt_winrate", "bt_worst_mdd"]
    available = [c for c in factor_columns if c in feature_cols or c == "score"]

    ics_by_factor: dict[str, List[float]] = {f: [] for f in available}
    for d in days:
        d_str = d.isoformat()
        # Picks with score for that day (all versions pooled for factor analysis, or take one version)
        rows = store.fetchall(
            """
            SELECT ticker, score FROM picks_daily
            WHERE trade_date = ? AND version = 'V1'
            ORDER BY ticker
            """,
            [d_str],
        )
        if not rows:
            continue
        picks = pd.DataFrame(rows, columns=["ticker", "score"])
        picks["ticker"] = picks["ticker"].astype(str)
        tickers = picks["ticker"].tolist()
        if not tickers:
            continue
        fr = compute_forward_returns(store, d, tickers, horizon=horizon)
        if fr.empty or fr["fwd_ret"].notna().sum() < 2:
            continue
        merged = picks.merge(fr[["ticker", "fwd_ret"]], on="ticker", how="inner").dropna(
            subset=["fwd_ret"]
        )
        if len(merged) < 2:
            continue
        for fac in available:
            if fac == "score":
                ic_val = ic(merged["score"], merged["fwd_ret"], method="pearson")
            else:
                # Load features for (d, tickers)
                placeholders = ",".join(["?"] * len(tickers))
                feats = store.fetchall(
                    f"""
                    SELECT ticker, {fac}
                    FROM features_daily
                    WHERE trade_date = ? AND ticker IN ({placeholders})
                    """,
                    [d_str, *tickers],
                )
                if not feats:
                    continue
                feat_df = pd.DataFrame(feats, columns=["ticker", fac])
                feat_df["ticker"] = feat_df["ticker"].astype(str)
                m2 = merged.merge(feat_df, on="ticker", how="inner").dropna(subset=[fac])
                if len(m2) < 2:
                    continue
                ic_val = ic(m2[fac], m2["fwd_ret"], method="pearson")
            if pd.notna(ic_val):
                ics_by_factor[fac].append(ic_val)

    out = []
    for f in available:
        arr = ics_by_factor[f]
        if not arr:
            out.append({"factor_name": f, "mean_ic": None, "std_ic": None, "n_dates": 0})
        else:
            s = pd.Series(arr)
            out.append({
                "factor_name": f,
                "mean_ic": float(s.mean()),
                "std_ic": float(s.std()) if len(s) > 1 else 0.0,
                "n_dates": len(arr),
            })
    return pd.DataFrame(out)


def run_diagnostics(
    store: "DuckDBStore",
    start: date,
    end: date,
    out_dir: Path,
    versions: Optional[List[str]] = None,
) -> dict[str, Path]:
    """
    Run version comparison and factor analysis; write CSVs to out_dir.
    Returns dict of output_name -> path.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    version_df = run_version_compare(store, start, end, versions=versions)
    version_path = out_dir / "version_compare.csv"
    version_df.to_csv(version_path, index=False)

    factor_df = run_factor_analysis(store, start, end, factor_columns=["score", "ret_5d", "bt_mean"])
    factor_path = out_dir / "factor_analysis.csv"
    factor_df.to_csv(factor_path, index=False)

    return {"version_compare": version_path, "factor_analysis": factor_path}
