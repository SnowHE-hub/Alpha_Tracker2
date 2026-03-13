# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _project_root_from_here() -> Path:
    return Path(__file__).resolve().parents[1]


def _parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="trade_date YYYY-MM-DD")
    ap.add_argument("--limit", type=int, default=50)
    return ap.parse_args()


def main():
    args = _parse_args()
    root = _project_root_from_here()
    cfg = load_settings(root)

    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    d = pd.to_datetime(args.date).date()

    # overall range
    mn = store.fetchone("SELECT MIN(trade_date) FROM features_daily;")[0]
    mx = store.fetchone("SELECT MAX(trade_date) FROM features_daily;")[0]
    cnt = store.fetchone("SELECT COUNT(*) FROM features_daily;")[0]
    print("db:", cfg.store_db)
    print("features_daily rows:", cnt)
    print("features_daily min trade_date:", mn)
    print("features_daily max trade_date:", mx)

    # per-day stats
    n_day_rows = store.fetchone("SELECT COUNT(*) FROM features_daily WHERE trade_date=?;", (d,))[0]
    n_day_tickers = store.fetchone("SELECT COUNT(DISTINCT ticker) FROM features_daily WHERE trade_date=?;", (d,))[0]
    print("\ntrade_date:", d)
    print("rows on date:", n_day_rows)
    print("distinct tickers on date:", n_day_tickers)

    # duplicates check
    dup = store.fetchall(
        """
        SELECT ticker, COUNT(*) AS n
        FROM features_daily
        WHERE trade_date=?
        GROUP BY ticker
        HAVING COUNT(*) > 1
        ORDER BY n DESC, ticker
        LIMIT ?;
        """,
        (d, int(args.limit)),
    )
    if dup:
        print("\n[WARN] duplicate rows per ticker found (top):")
        for t, n in dup:
            print(f"  {t}: {n}")
    else:
        print("\n[OK] no duplicate ticker rows on this date.")

    # sample tickers
    rows = store.fetchall(
        """
        SELECT ticker
        FROM features_daily
        WHERE trade_date=?
        ORDER BY ticker
        LIMIT ?;
        """,
        (d, int(args.limit)),
    )
    sample = [r[0] for r in rows]
    print("\nsample tickers:", sample[:20], ("..." if len(sample) > 20 else ""))


if __name__ == "__main__":
    main()
