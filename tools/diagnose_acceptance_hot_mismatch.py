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
    ap.add_argument("--limit", type=int, default=203)
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
    limit = int(args.limit)

    # 1) hot universe tickers
    uni = store.fetchall(
        """
        SELECT ticker
        FROM universe_daily_hot
        WHERE trade_date = ?
        ORDER BY hot_industry_rank, stock_rank_in_industry, ticker
        LIMIT ?;
        """,
        (d, limit),
    )
    uni_tickers = [r[0] for r in uni]
    print("trade_date:", d)
    print("universe_daily_hot tickers:", len(uni_tickers))

    # 2) correct intersection check: features_daily by date only
    feat_all = store.fetchall(
        """
        SELECT ticker
        FROM features_daily
        WHERE trade_date = ?
        """,
        (d,),
    )
    feat_set = set(r[0] for r in feat_all)
    inter = [t for t in uni_tickers if t in feat_set]
    missing = [t for t in uni_tickers if t not in feat_set]
    print("features_daily tickers on date:", len(feat_set))
    print("intersection (hot in features):", len(inter))
    print("missing hot tickers:", len(missing))
    if missing:
        print("missing examples:", missing[:20])

    # 3) emulate acceptance query: trade_date + IN (tickers)
    if uni_tickers:
        placeholders = ",".join(["?"] * len(uni_tickers))
        rows = store.fetchall(
            f"""
            SELECT COUNT(DISTINCT ticker)
            FROM features_daily
            WHERE trade_date = ?
              AND ticker IN ({placeholders});
            """,
            (d, *uni_tickers),
        )
        cnt = int(rows[0][0]) if rows else 0
        print("\n[emulate acceptance] features_tickers:", cnt)

    print("\ndb:", cfg.store_db)


if __name__ == "__main__":
    main()
