from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol

import pandas as pd

from alpha_tracker2.storage.duckdb_store import DuckDBStore


REQUIRED_SCORE_COLUMNS = ("ticker", "score")


class Scorer(Protocol):
    """
    Scoring interface operating on top of features_daily.

    Implementations are responsible for:
      - pulling required inputs from DuckDB (typically features_daily)
      - computing a per-ticker score for a given trade_date

    Implementations are NOT responsible for:
      - writing to picks_daily
      - thresholding / picked_by flags
      - score_100 normalisation
    """

    def score(self, trade_date: date, store: DuckDBStore) -> pd.DataFrame:  # pragma: no cover - protocol
        """
        Compute scores for the given trade_date.

        Parameters
        ----------
        trade_date:
            Target trading date.
        store:
            DuckDBStore to fetch inputs from (e.g. features_daily).

        Returns
        -------
        pd.DataFrame
            One row per ticker. Must contain at least:
              - 'ticker' (str)
              - 'score' (float)
            May additionally include:
              - 'name'
              - 'reason'
              - 'rank'
              - any other diagnostic columns
        """
        ...


@dataclass(frozen=True)
class ScoreResultSpec:
    """
    Declarative spec for minimal score result contract.

    This is mainly a convenience wrapper to centralise column semantics.
    """

    ticker_col: str = "ticker"
    score_col: str = "score"


def ensure_score_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and lightly normalise a scorer output DataFrame.

    - Ensures required columns exist.
    - Casts ticker to string and score to float where possible.
    - Returns a shallow copy to avoid mutating caller's DataFrame.
    """
    missing = [c for c in REQUIRED_SCORE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Scorer output missing required columns: {missing}")

    result = df.copy()

    # Normalise basic dtypes
    result["ticker"] = result["ticker"].astype(str)
    # Use pd.to_numeric with errors='coerce' then keep as float
    result["score"] = pd.to_numeric(result["score"], errors="coerce")

    if result["score"].isna().all():
        raise ValueError("All scores are NaN after coercion; check scorer implementation.")

    return result

