from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import pandas as pd


@dataclass(frozen=True)
class UniverseCache:
    lake_dir: Path

    def cache_path(self, trade_date: date) -> Path:
        # data/lake/universe/2026-01-14/universe.parquet
        return self.lake_dir / "universe" / trade_date.isoformat() / "universe.parquet"

    def save(self, trade_date: date, df: pd.DataFrame) -> Path:
        p = self.cache_path(trade_date)
        p.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(p, index=False)
        return p

    def load(self, trade_date: date) -> pd.DataFrame | None:
        p = self.cache_path(trade_date)
        if p.exists():
            return pd.read_parquet(p)
        return None

    def load_latest(self) -> pd.DataFrame | None:
        root = self.lake_dir / "universe"
        if not root.exists():
            return None

        # 找到按日期命名的子目录，取最新一个
        dates = sorted([d for d in root.iterdir() if d.is_dir()], reverse=True)
        for d in dates:
            p = d / "universe.parquet"
            if p.exists():
                return pd.read_parquet(p)
        return None
