# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path
import re

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


A_SHARE_RE = re.compile(r"^\d{6}\.(SZ|SH)$")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--universe-table", default="universe_daily_hot", help="e.g. universe_daily_hot")
    ap.add_argument("--out", default=None, help="output txt path (default: data/out/universe/hot_tickers_{date}.txt)")
    ap.add_argument("--filter-a-share", action="store_true", help="keep only 6-digit .SZ/.SH")
    ap.add_argument("--drop-prefix", default="900,920", help="comma prefixes to drop, default=900,920")
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

    trade_date = pd.to_datetime(args.date).date()

    rows = store.fetchall(
        f"""
        SELECT DISTINCT ticker
        FROM {args.universe_table}
        WHERE trade_date = ?
        ORDER BY ticker;
        """,
        (trade_date,),
    )
    tickers = [r[0] for r in rows]

    drop_prefixes = tuple(p.strip() for p in (args.drop_prefix or "").split(",") if p.strip())
    if drop_prefixes:
        tickers = [t for t in tickers if not any(t.startswith(p) for p in drop_prefixes)]

    if args.filter_a_share:
        tickers = [t for t in tickers if A_SHARE_RE.match(t)]

    if args.out:
        out_path = Path(args.out)
    else:
        out_path = root / "data" / "out" / "universe" / f"hot_tickers_{args.date}.txt"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    out_path.write_text("\n".join(tickers) + "\n", encoding="utf-8")

    print("trade_date:", args.date)
    print("universe_table:", args.universe_table)
    print("tickers:", len(tickers))
    print("out:", str(out_path))
    print("db:", str(cfg.store_db))


if __name__ == "__main__":
    main()
