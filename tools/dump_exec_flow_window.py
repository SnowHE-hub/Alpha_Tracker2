from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def _dump(con: duckdb.DuckDBPyConnection, title: str, sql: str, params: list):
    df = con.execute(sql, params).df()
    print(f"\n=== {title} ===")
    if df.empty:
        print("[EMPTY]")
    else:
        print(df.to_string(index=False))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    db_path = root / "data" / "store" / "alpha_tracker.duckdb"
    con = duckdb.connect(str(db_path))

    print(f"[DB] {db_path}")
    print(f"[RANGE] {args.start} ~ {args.end}  version={args.version}")

    _dump(
        con,
        "picks_daily (signals)",
        r"""
        SELECT trade_date, version, ticker, rank, score, picked_by
        FROM picks_daily
        WHERE version = ?
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date, rank;
        """,
        [args.version, args.start, args.end],
    )

    _dump(
        con,
        "orders_daily",
        r"""
        SELECT trade_date, version, ticker, side, qty, price, notional
        FROM orders_daily
        WHERE version = ?
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date, ticker;
        """,
        [args.version, args.start, args.end],
    )

    _dump(
        con,
        "trades_daily",
        r"""
        SELECT trade_date, version, ticker, side, qty, price, notional
        FROM trades_daily
        WHERE version = ?
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date, ticker;
        """,
        [args.version, args.start, args.end],
    )

    con.close()


if __name__ == "__main__":
    main()
