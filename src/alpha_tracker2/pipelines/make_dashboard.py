"""
Export dashboard data from DuckDB to data/out/*.csv.

Reads nav_daily, eval_5d_daily, picks_daily (filtered by date range) and writes
CSV files. No business logic—only read table + filter + export.

CLI:
  --start YYYY-MM-DD --end YYYY-MM-DD  (export range)
  --date YYYY-MM-DD                     (single day: start=end=date)
  --out-dir PATH                        (default from config paths.out_dir)
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.reporting.dashboard_data import (
    load_eval_for_dashboard,
    load_nav_for_dashboard,
    load_picks_for_dashboard,
)
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export nav_daily, eval_5d_daily, picks_daily to data/out/*.csv.",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date YYYY-MM-DD (use with --end)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date YYYY-MM-DD (use with --start)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Single date YYYY-MM-DD (equivalent to --start X --end X)",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default=None,
        help="Output directory for CSV files; default from config paths.out_dir",
    )
    args = parser.parse_args()

    if args.date:
        start = end = date.fromisoformat(args.date)
    elif args.start and args.end:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
        if end < start:
            raise ValueError("--end must be >= --start")
    else:
        raise SystemExit("Provide either --date YYYY-MM-DD or both --start and --end.")

    project_root = _find_project_root(Path(__file__).resolve())
    settings = load_settings(project_root)
    out_dir = Path(args.out_dir) if args.out_dir else settings.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    # Nav
    nav_df = load_nav_for_dashboard(store, start, end)
    nav_path = out_dir / "nav_daily.csv"
    nav_df.to_csv(nav_path, index=False)
    print(f"make_dashboard: wrote {len(nav_df)} rows -> {nav_path}")

    # Eval 5d
    eval_df = load_eval_for_dashboard(store, start, end)
    eval_path = out_dir / "eval_5d_daily.csv"
    eval_df.to_csv(eval_path, index=False)
    print(f"make_dashboard: wrote {len(eval_df)} rows -> {eval_path}")

    # Picks (optional export)
    picks_df = load_picks_for_dashboard(store, start, end)
    picks_path = out_dir / "picks_daily.csv"
    picks_df.to_csv(picks_path, index=False)
    print(f"make_dashboard: wrote {len(picks_df)} rows -> {picks_path}")

    print(f"make_dashboard: out_dir={out_dir} range=[{start}, {end}]")


if __name__ == "__main__":
    main()
