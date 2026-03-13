# tools/init_schema_and_check_nav_daily.py
# Purpose:
#   1) apply schema.sql via DuckDBStore.init_schema()
#   2) verify nav_daily table exists
#
# Run (PowerShell):
#   cd D:\alpha_tracker2
#   $env:PYTHONPATH="src"
#   .\.venv\Scripts\python.exe .\tools\init_schema_and_check_nav_daily.py

from __future__ import annotations

from pathlib import Path
import sys

import duckdb

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def main() -> int:
    root = Path(__file__).resolve().parents[1]  # project root (D:\alpha_tracker2)
    cfg = load_settings(root)

    schema_path = root / "src" / "alpha_tracker2" / "storage" / "schema.sql"
    if not schema_path.exists():
        print(f"[ERROR] schema.sql not found: {schema_path}")
        return 2

    # Apply schema
    store = DuckDBStore(cfg.store_db, schema_path)
    store.init_schema()
    print("[OK] schema initialized from schema.sql")

    # Verify tables
    con = duckdb.connect(str(cfg.store_db))
    try:
        tables = con.execute("SHOW TABLES").fetchall()
        print("\n=== SHOW TABLES ===")
        for t in tables:
            print(t)

        cnt = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'nav_daily'"
        ).fetchone()[0]

        print("\n=== nav_daily exists? ===")
        print(f"nav_daily table count in information_schema: {cnt}")

        if cnt == 1:
            # Optionally check row count (may be 0 right after creating table)
            nrows = con.execute("SELECT COUNT(*) FROM nav_daily").fetchone()[0]
            print(f"nav_daily rows: {nrows}")

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
