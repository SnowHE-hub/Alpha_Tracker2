from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def _pick_date_col(con: duckdb.DuckDBPyConnection, table: str) -> str:
    cols = con.execute(f"DESCRIBE {table}").df()["column_name"].tolist()
    # prefer semantic date columns
    for c in ["trade_date", "asof_date", "signal_date"]:
        if c in cols:
            return c
    # fallback: created_at (cast to date)
    if "created_at" in cols:
        return "created_at::DATE"
    raise RuntimeError(f"No usable date column found for table={table}, cols={cols}")


def _dump(con: duckdb.DuckDBPyConnection, title: str, sql: str, params: list) -> None:
    print(f"\n=== {title} ===")
    df = con.execute(sql, params).df()
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

    # picks_daily: has trade_date (signal day)
    _dump(
        con,
        "picks_daily (signals)",
        """
        SELECT trade_date, version, ticker, rank, score, picked_by
        FROM picks_daily
        WHERE version = ?
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date, rank
        """,
        [args.version, args.start, args.end],
    )

    # orders_daily: likely has signal_date
    orders_date = _pick_date_col(con, "orders_daily")
    _dump(
        con,
        f"orders_daily (date_col={orders_date})",
        f"""
        SELECT {orders_date} AS dt, version, ticker,
               target_weight, prev_weight, delta_weight, created_at
        FROM orders_daily
        WHERE version = ?
          AND {orders_date} BETWEEN ? AND ?
        ORDER BY dt, ticker
        """,
        [args.version, args.start, args.end],
    )

    # positions_daily: asof_date
    pos_date = _pick_date_col(con, "positions_daily")
    _dump(
        con,
        f"positions_daily (date_col={pos_date})",
        f"""
        SELECT {pos_date} AS dt, version, ticker, shares, price, market_value, cash
        FROM positions_daily
        WHERE version = ?
          AND {pos_date} BETWEEN ? AND ?
        ORDER BY dt, ticker
        """,
        [args.version, args.start, args.end],
    )

    con.close()


if __name__ == "__main__":
    main()
