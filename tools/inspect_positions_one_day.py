from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True)
    ap.add_argument("--date", required=True)  # asof_date
    ap.add_argument("--initial_equity", type=float, default=100000.0)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    db_path = root / "data" / "store" / "alpha_tracker.duckdb"
    con = duckdb.connect(str(db_path))

    q = r"""
    SELECT
      p.asof_date,
      p.version,
      p.ticker,
      p.shares,
      -- exact close (may be null)
      (
        SELECT close
        FROM prices_daily d
        WHERE d.ticker = p.ticker
          AND d.trade_date = p.asof_date
      ) AS close_exact,
      -- ffill close (latest <= asof_date)
      (
        SELECT close
        FROM prices_daily d
        WHERE d.ticker = p.ticker
          AND d.trade_date <= p.asof_date
          AND d.close IS NOT NULL
        ORDER BY d.trade_date DESC
        LIMIT 1
      ) AS close_ffill,
      p.cash
    FROM positions_daily p
    WHERE p.version = ?
      AND p.asof_date = ?
    ORDER BY p.ticker;
    """

    df = con.execute(q, [args.version, args.date]).df()
    con.close()

    if df.empty:
        print("[EMPTY] no positions found")
        return

    df["mv_ffill"] = df["shares"] * df["close_ffill"]
    cash = df["cash"].dropna().max() if "cash" in df else 0.0
    mv_sum = df["mv_ffill"].sum()
    equity = mv_sum + (cash if cash is not None else 0.0)
    nav = equity / float(args.initial_equity)

    print(f"[DB] {db_path}")
    print(f"[DATE] {args.date}  version={args.version}")
    print("\n=== Positions (asof_date) ===")
    print(df.to_string(index=False))

    print("\n=== Equity breakdown ===")
    print(f"market_value_sum = {mv_sum:.2f}")
    print(f"cash             = {cash:.2f}")
    print(f"equity           = {equity:.2f}")
    print(f"nav (equity / initial) = {nav:.6f}")


if __name__ == "__main__":
    main()
