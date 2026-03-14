"""
E-3: Run version comparison and factor analysis; write version_compare.csv and factor_analysis.csv to data/out.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import yaml

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.evaluation.diagnostics import run_diagnostics
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def _resolve_versions(arg_versions: str | None, project_root: Path) -> list[str] | None:
    if arg_versions is None or arg_versions.strip() == "":
        return None
    return [v.strip().upper() for v in arg_versions.split(",") if v.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="E-3: Version comparison and factor analysis -> data/out CSVs.",
    )
    parser.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--versions",
        type=str,
        default=None,
        help="Comma-separated versions to include (default: all from eval_5d_daily)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (default: config paths.out_dir)",
    )
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if end < start:
        raise ValueError("--end must be >= --start")

    project_root = _find_project_root(Path(__file__).resolve())
    settings = load_settings(project_root)
    out_dir = Path(args.output_dir) if args.output_dir else settings.out_dir
    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    versions = _resolve_versions(args.versions, project_root)
    paths = run_diagnostics(store, start, end, out_dir, versions=versions)
    print("run_diagnostics: wrote")
    for name, p in paths.items():
        print(f"  {name}: {p}")
    print(f"run_diagnostics: out_dir={out_dir}")


if __name__ == "__main__":
    main()
