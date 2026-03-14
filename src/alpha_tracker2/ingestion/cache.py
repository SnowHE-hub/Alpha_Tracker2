"""
Lake cache for ingestion: prices (and optionally universe).

Storage layout: lake_dir / "prices" / ticker / "{start}_{end}.parquet".
Load returns None if file does not exist.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd


@dataclass
class PricesCache:
    """Parquet cache for price DataFrames keyed by ticker and date range."""

    lake_dir: Path

    def _path_for(self, ticker: str, start: date, end: date) -> Path:
        # Sanitize ticker for filesystem (e.g. 0700.HK -> 0700.HK is fine)
        safe = ticker.replace("/", "_").replace("\\", "_")
        return (
            self.lake_dir
            / "prices"
            / safe
            / f"{start.isoformat()}_{end.isoformat()}.parquet"
        )

    def save(self, ticker: str, start: date, end: date, df: pd.DataFrame) -> Path:
        p = self._path_for(ticker, start, end)
        p.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(p, index=True)
        return p

    def load(self, ticker: str, start: date, end: date) -> pd.DataFrame | None:
        p = self._path_for(ticker, start, end)
        if not p.is_file():
            return None
        return pd.read_parquet(p)


@dataclass
class UniverseCache:
    """
    Parquet cache for universe DataFrames keyed by trade_date.
    Layout: lake_dir / "universe" / "{trade_date}" / universe.parquet
    """

    lake_dir: Path

    def _path_for(self, trade_date: date) -> Path:
        return self.lake_dir / "universe" / trade_date.isoformat() / "universe.parquet"

    def save(self, trade_date: date, df: pd.DataFrame) -> Path:
        """Save universe DataFrame for the given trade_date. Returns path written."""
        p = self._path_for(trade_date)
        p.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(p, index=False)
        return p

    def load(self, trade_date: date) -> pd.DataFrame | None:
        """Load universe for trade_date if file exists, else None."""
        p = self._path_for(trade_date)
        if not p.is_file():
            return None
        return pd.read_parquet(p)

    def load_latest(self) -> pd.DataFrame | None:
        """Return the most recently saved universe (by directory date), or None if none exist."""
        base = self.lake_dir / "universe"
        if not base.is_dir():
            return None
        dates: list[date] = []
        for child in base.iterdir():
            if not child.is_dir():
                continue
            try:
                d = date.fromisoformat(child.name)
                dates.append(d)
            except ValueError:
                continue
        if not dates:
            return None
        latest = max(dates)
        return self.load(latest)
