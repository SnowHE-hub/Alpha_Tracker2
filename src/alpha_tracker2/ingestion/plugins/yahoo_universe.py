"""
Yahoo / static universe provider: US and HK tickers.

Minimal implementation: uses a small hardcoded list of US and HK tickers.
TODO: Replace with config-driven list, CSV path, or index constituents from Yahoo/other source.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import yaml

from alpha_tracker2.ingestion.base import UniverseRow


# Default tickers when no config is provided. At least one US and one HK for acceptance.
_DEFAULT_US: list[str] = ["AAPL", "MSFT"]
_DEFAULT_HK: list[str] = ["0700.HK"]


def _load_config_universe(project_root: Path) -> list[UniverseRow] | None:
    """If config has ingestion.universe as a list, return UniverseRow list; else None."""
    config_path = project_root / "configs" / "default.yaml"
    if not config_path.is_file():
        return None
    with config_path.open("r", encoding="utf-8") as f:
        config: dict[str, Any] = yaml.safe_load(f) or {}
    raw = config.get("ingestion") or {}
    universe = raw.get("universe")
    if universe is None:
        return None
    if not isinstance(universe, list):
        return None
    rows: list[UniverseRow] = []
    for item in universe:
        ticker = str(item).strip()
        if not ticker:
            continue
        market = "HK" if ticker.endswith(".HK") else "US"
        rows.append(UniverseRow(ticker=ticker, name="", market=market))
    return rows if rows else None


def _default_universe() -> list[UniverseRow]:
    """Hardcoded default: one US and one HK ticker (and a few more for variety)."""
    rows: list[UniverseRow] = []
    for t in _DEFAULT_US:
        rows.append(UniverseRow(ticker=t, name="", market="US"))
    for t in _DEFAULT_HK:
        rows.append(UniverseRow(ticker=t, name="", market="HK"))
    return rows


class YahooUniverseProvider:
    """
    Provides US and HK universe tickers.

    Uses config ingestion.universe if present (list of tickers); otherwise
    uses built-in default list. TODO: Support CSV path or index constituents.
    """

    def __init__(self, project_root: Path | None = None):
        self._project_root = project_root

    def fetch_universe(self, trade_date: date) -> list[UniverseRow]:
        # Unused for now; kept for protocol and future date-based filtering.
        _ = trade_date
        if self._project_root is not None:
            from_config = _load_config_universe(self._project_root)
            if from_config is not None:
                return from_config
        return _default_universe()
