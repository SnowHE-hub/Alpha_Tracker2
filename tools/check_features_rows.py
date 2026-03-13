from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    return ap.parse_args()

def main():
    args = parse_args()
    d = pd.to_datetime(args.date).date()
    root = Path(__file__).resolve().parents[1]
    cfg = load_settings(root)
    store = DuckDBStore(db_path=cfg.store_db, schema_path=root/"src"/"alpha_tracker2"/"storage"/"schema.sql")
    store.init_schema()
    n = store.fetchone("SELECT COUNT(*) FROM features_daily WHERE trade_date=?;", (d,))[0]
    print("trade_date:", d, "rows:", n)

if __name__ == "__main__":
    main()
