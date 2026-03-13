from __future__ import annotations
from pathlib import Path
from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore

def main():
    root = Path(__file__).resolve().parents[1]
    cfg = load_settings(root)
    store = DuckDBStore(db_path=cfg.store_db, schema_path=root/"src"/"alpha_tracker2"/"storage"/"schema.sql")
    store.init_schema()
    mn = store.fetchone("SELECT MIN(trade_date) FROM prices_daily;")[0]
    mx = store.fetchone("SELECT MAX(trade_date) FROM prices_daily;")[0]
    n = store.fetchone("SELECT COUNT(*) FROM prices_daily;")[0]
    print("prices_daily rows:", n)
    print("prices_daily min trade_date:", mn)
    print("prices_daily max trade_date:", mx)
    print("db:", cfg.store_db)

if __name__ == "__main__":
    main()
    