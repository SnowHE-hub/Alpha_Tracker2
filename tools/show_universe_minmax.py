from __future__ import annotations
from pathlib import Path

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _table_exists(store: DuckDBStore, table: str) -> bool:
    rows = store.fetchall(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?;",
        (table,),
    )
    return int(rows[0][0]) > 0


def _minmax(store: DuckDBStore, table: str):
    mn, mx, cnt = store.fetchall(
        f"SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM {table};"
    )[0]
    return mn, mx, cnt


def main():
    root = Path(__file__).resolve().parents[1]
    cfg = load_settings(root)

    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    tables = ["universe_daily_hot", "universe_daily"]

    print("db:", cfg.store_db)
    for t in tables:
        if not _table_exists(store, t):
            print(f"{t}: [MISSING TABLE]")
            continue
        mn, mx, cnt = _minmax(store, t)
        print(f"{t}: min={mn}  max={mx}  rows={cnt}")


if __name__ == "__main__":
    main()
