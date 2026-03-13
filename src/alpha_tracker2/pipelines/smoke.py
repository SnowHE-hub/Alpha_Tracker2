from __future__ import annotations

from pathlib import Path
from datetime import datetime

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def main() -> None:
    root = Path(__file__).resolve().parents[3]
    s = load_settings(root)

    # ensure dirs
    s.lake_dir.mkdir(parents=True, exist_ok=True)
    s.store_db.parent.mkdir(parents=True, exist_ok=True)
    s.runs_dir.mkdir(parents=True, exist_ok=True)

    store = DuckDBStore(
        db_path=s.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    store.exec(
        "INSERT INTO meta_runs(run_id, run_ts, note) VALUES (?, ?, ?);",
        (run_id, datetime.now(), "smoke"),
    )

    n = store.fetchone("SELECT COUNT(*) AS n FROM meta_runs;")[0]

    print("[OK] smoke pipeline passed (with schema).")
    print("project_name:", s.project_name)
    print("store_db:", s.store_db)
    print("meta_runs_count:", n)


if __name__ == "__main__":
    main()
