from __future__ import annotations

from datetime import datetime
from pathlib import Path

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _find_project_root(start: Path) -> Path:
    """
    Walk up from the given path until we find a directory containing configs/default.yaml.
    """
    current = start
    for parent in [current, *current.parents]:
        config_path = parent / "configs" / "default.yaml"
        if config_path.is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def main() -> None:
    # Locate project root (directory that contains configs/default.yaml).
    project_root = _find_project_root(Path(__file__).resolve())
    settings = load_settings(project_root)

    # 1) init_schema
    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    # 2) insert into meta_runs
    now = datetime.now()
    run_id = now.strftime("%Y%m%d_%H%M%S")
    store.exec(
        "INSERT INTO meta_runs(run_id, run_ts, note) VALUES (?, ?, ?);",
        (run_id, now, "smoke"),
    )

    # 3) read back meta_runs count
    row = store.fetchone("SELECT COUNT(*) FROM meta_runs;")
    meta_runs_count = int(row[0]) if row is not None else 0

    # 4) test TradingCalendar
    cal = TradingCalendar()
    us_today = cal.latest_trading_day("US")
    hk_today = cal.latest_trading_day("HK")

    print("[OK] smoke passed.")
    print("project_name:", settings.project_name)
    print("store_db:", settings.store_db)
    print("meta_runs_count:", meta_runs_count)
    print("latest_trading_day_US:", us_today)
    print("latest_trading_day_HK:", hk_today)


if __name__ == "__main__":
    main()

