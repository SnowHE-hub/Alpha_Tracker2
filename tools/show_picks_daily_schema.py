from __future__ import annotations
from pathlib import Path

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def main():
    root = Path(__file__).resolve().parents[1]
    cfg = load_settings(root)

    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    rows = store.fetchall("PRAGMA table_info('picks_daily');")
    print("=== picks_daily schema ===")
    for r in rows:
        # PRAGMA table_info columns:
        # cid, name, type, notnull, dflt_value, pk
        print(f"{r[1]:20s} {r[2]}")

    print("db:", cfg.store_db)


if __name__ == "__main__":
    main()
