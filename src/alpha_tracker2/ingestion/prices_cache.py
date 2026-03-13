from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import pandas as pd


@dataclass(frozen=True)
class PricesCache:
    lake_dir: Path

    def cache_path(self, ticker: str, start: date, end: date) -> Path:
        # data/lake/prices/600519.SH/2026-01-01_2026-01-14.parquet
        safe = ticker.replace(":", "_").replace("/", "_")
        return self.lake_dir / "prices" / safe / f"{start.isoformat()}_{end.isoformat()}.parquet"

    def save(self, ticker: str, start: date, end: date, df: pd.DataFrame) -> Path:
        p = self.cache_path(ticker, start, end)
        p.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(p, index=False)
        return p

    def load(self, ticker: str, start: date, end: date) -> pd.DataFrame | None:
        p = self.cache_path(ticker, start, end)
        if p.exists():
            return pd.read_parquet(p)
        return None
