from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Mapping

import pandas as pd

from alpha_tracker2.scoring.base import Scorer, ensure_score_frame
from alpha_tracker2.storage.duckdb_store import DuckDBStore


@dataclass(frozen=True)
class V1Config:
    """
    Simple linear V1 score based on cross-sectional z-scores.

    weights: mapping factor_name -> weight. Factors must exist as columns
             in features_daily.
    """

    weights: Mapping[str, float]


DEFAULT_V1_WEIGHTS: dict[str, float] = {
    # Momentum / trend proxies
    "ret_5d": 0.5,
    "ret_20d": 0.3,
    # Liquidity / quality adjustments
    "avg_amount_20": 0.2,
}


def _fetch_features(store: DuckDBStore, trade_date: date, columns: list[str]) -> pd.DataFrame:
    base_cols = ["trade_date", "ticker"]
    select_cols = base_cols + columns
    sql = f"""
        SELECT {', '.join(select_cols)}
        FROM features_daily
        WHERE trade_date = ?
    """
    rows = store.fetchall(sql, [trade_date.isoformat()])
    if not rows:
        return pd.DataFrame(columns=select_cols)
    df = pd.DataFrame(rows, columns=select_cols)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


class V1BaselineScorer(Scorer):
    """
    Baseline linear model on top of daily features.

    This scorer intentionally keeps logic simple but transparent and
    explainable via a JSON 'reason' column.
    """

    def __init__(self, cfg: V1Config | None = None) -> None:
        self._cfg = cfg or V1Config(weights=DEFAULT_V1_WEIGHTS)

    def score(self, trade_date: date, store: DuckDBStore) -> pd.DataFrame:
        factor_names = list(self._cfg.weights.keys())
        features = _fetch_features(store, trade_date, factor_names)
        if features.empty:
            return ensure_score_frame(pd.DataFrame(columns=["ticker", "score", "reason"]))

        # Work on a ticker-indexed frame
        features = features.set_index("ticker")

        # Cross-sectional z-scores per factor
        z_df = pd.DataFrame(index=features.index)
        for name in factor_names:
            series = pd.to_numeric(features[name], errors="coerce")
            mean = series.mean()
            std = series.std(ddof=0)
            if std == 0 or pd.isna(std):
                z = pd.Series(0.0, index=series.index)
            else:
                z = (series - mean) / std
            z_df[name] = z

        # Linear combination of z-scores using configured weights
        score = pd.Series(0.0, index=z_df.index, name="score")
        for name, w in self._cfg.weights.items():
            if name in z_df:
                score = score + float(w) * z_df[name].fillna(0.0)

        # Build reason JSON: include raw factors, z-scores, and weights
        out_rows: list[dict] = []
        for ticker, s in score.items():
            row_factors = {k: _safe_float(features.loc[ticker].get(k)) for k in factor_names}
            row_z = {k: _safe_float(z_df.loc[ticker].get(k)) for k in factor_names}
            payload = {
                "model": "V1",
                "factors": row_factors,
                "z": row_z,
                "weights": dict(self._cfg.weights),
            }
            out_rows.append(
                {
                    "ticker": str(ticker),
                    "score": float(s) if pd.notna(s) else None,
                    "reason": json.dumps(payload, ensure_ascii=False),
                }
            )

        result = pd.DataFrame(out_rows)
        return ensure_score_frame(result)


def _safe_float(value: object) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(f):
        return None
    return float(f)

