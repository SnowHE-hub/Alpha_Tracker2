# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="trade_date YYYY-MM-DD")
    return ap.parse_args()


def main():
    args = _parse_args()
    root = _project_root()
    cfg = load_settings(root)

    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    d = pd.to_datetime(args.date).date()

    with store.session() as con:
        before = con.execute("SELECT COUNT(*) FROM features_daily WHERE trade_date = ?;", (d,)).fetchone()[0]
        con.execute("DELETE FROM features_daily WHERE trade_date = ?;", (d,))
        after = con.execute("SELECT COUNT(*) FROM features_daily WHERE trade_date = ?;", (d,)).fetchone()[0]

    print("[OK] purged features_daily for date:", d)
    print("rows before:", before)
    print("rows after :", after)
    print("db:", cfg.store_db)


if __name__ == "__main__":
    main()
