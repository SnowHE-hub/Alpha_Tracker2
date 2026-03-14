"""
Ingest universe into picks_daily (version='UNIVERSE').

CLI: --date YYYY-MM-DD (optional; default latest US trading day).
Idempotent: DELETE for (trade_date, version='UNIVERSE') then INSERT.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.registry import get_universe_provider
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.ingestion.base import UniverseRow
from alpha_tracker2.ingestion.cache import UniverseCache
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def _load_ingestion_config(project_root: Path) -> dict:
    path = project_root / "configs" / "default.yaml"
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg.get("ingestion") or {}


def _rows_to_universe_df(rows: list) -> pd.DataFrame:
    return pd.DataFrame([{"ticker": r.ticker, "name": r.name or "", "market": r.market} for r in rows])


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest universe into picks_daily.")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Trade date YYYY-MM-DD; default latest US trading day",
    )
    args = parser.parse_args()

    project_root = _find_project_root(Path(__file__).resolve())
    settings = load_settings(project_root)
    cal = TradingCalendar()
    trade_date = (
        date.fromisoformat(args.date)
        if args.date
        else cal.latest_trading_day("US")
    )

    ingestion_cfg = _load_ingestion_config(project_root)
    provider_name = ingestion_cfg.get("universe_provider", "yahoo_universe")
    provider_cls = get_universe_provider(provider_name)
    provider = provider_cls(project_root=project_root)
    universe_cache = UniverseCache(settings.lake_dir)

    try:
        rows = provider.fetch_universe(trade_date)
        # On success: write to lake for future fallback
        universe_df = _rows_to_universe_df(rows)
        universe_cache.save(trade_date, universe_df)
    except Exception as e:
        # Fallback: try cache for trade_date, then latest
        df = universe_cache.load(trade_date)
        if df is None:
            df = universe_cache.load_latest()
        if df is None:
            raise RuntimeError(
                f"Universe provider failed ({e}) and no cached universe in lake (data/lake/universe/)."
            ) from e
        rows = [
            UniverseRow(ticker=str(r["ticker"]), name=str(r.get("name") or ""), market=str(r.get("market", "US")))
            for _, r in df.iterrows()
        ]
        print(f"ingest_universe: provider failed, using cached universe (rows={len(rows)})")

    # Stable order for rank
    rows_sorted = sorted(rows, key=lambda r: (r.market, r.ticker))
    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    with store.session() as conn:
        conn.execute(
            "DELETE FROM picks_daily WHERE trade_date = ? AND version = ?",
            [trade_date.isoformat(), "UNIVERSE"],
        )
        for rank, row in enumerate(rows_sorted, start=1):
            conn.execute(
                """INSERT INTO picks_daily (
                    trade_date, version, ticker, name, rank, score, score_100,
                    reason, thr_value, pass_thr, picked_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    trade_date.isoformat(),
                    "UNIVERSE",
                    row.ticker,
                    row.name or None,
                    rank,
                    0.0,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
            )

    print(f"ingest_universe: wrote {len(rows_sorted)} rows")
    print(f"  trade_date={trade_date}")
    print(f"  db={settings.store_db}")
    print(f"  provider={provider_name}")


if __name__ == "__main__":
    main()
