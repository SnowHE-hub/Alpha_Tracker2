# tools/inspect_exec_nav_prices.py
from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=str, default="", help="DuckDB path")
    ap.add_argument("--version", type=str, default="ENS")
    ap.add_argument("--signal_date", type=str, required=True)   # 2026-01-07
    ap.add_argument("--dates", type=str, required=True, help="Comma dates e.g. 2026-01-13,2026-01-14")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    db_path = Path(args.db) if args.db else (root / "data" / "store" / "alpha_tracker.duckdb")
    con = duckdb.connect(str(db_path))

    # tickers from picks
    tickers = con.execute(
        "SELECT ticker FROM picks_daily WHERE version=? AND trade_date=? ORDER BY ticker;",
        [args.version, args.signal_date],
    ).df()

    if tickers.empty:
        con.close()
        raise RuntimeError("No tickers found for that signal_date.")

    date_list = [d.strip() for d in args.dates.split(",") if d.strip()]
    # pull raw closes for those dates (no ffill)
    q = """
    SELECT trade_date, ticker, close
    FROM prices_daily
    WHERE ticker IN (SELECT ticker FROM picks_daily WHERE version=? AND trade_date=?)
      AND trade_date IN ({})
    ORDER BY trade_date, ticker;
    """.format(",".join(["?"] * len(date_list)))

    params = [args.version, args.signal_date] + date_list
    df = con.execute(q, params).df()
    con.close()

    print(f"[DB] {db_path}")
    print(f"[VERSION] {args.version}  [SIGNAL_DATE] {args.signal_date}")
    print(f"[TICKERS] {', '.join(tickers['ticker'].tolist())}")
    print(f"[DATES] {', '.join(date_list)}")
    if df.empty:
        print("[WARN] No price rows returned for those dates.")
        return

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
