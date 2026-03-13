from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True)
    ap.add_argument("--signal_date", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    db_path = root / "data" / "store" / "alpha_tracker.duckdb"
    con = duckdb.connect(str(db_path))

    # tickers from picks on signal_date
    tks = con.execute(
        """
        SELECT ticker
        FROM picks_daily
        WHERE version = ? AND trade_date = ?
        ORDER BY rank
        """,
        [args.version, args.signal_date],
    ).df()["ticker"].tolist()

    if not tks:
        print("[EMPTY] no tickers from picks_daily for that signal_date")
        return

    # show exact close + ffill close used by our rule (latest <= date)
    q = """
    WITH days AS (
      SELECT DISTINCT trade_date
      FROM prices_daily
      WHERE trade_date BETWEEN ? AND ?
      ORDER BY trade_date
    ),
    grid AS (
      SELECT d.trade_date, t.ticker
      FROM days d
      CROSS JOIN (SELECT UNNEST(?) AS ticker) t
    )
    SELECT
      g.trade_date,
      g.ticker,
      (SELECT close FROM prices_daily p
        WHERE p.trade_date = g.trade_date AND p.ticker = g.ticker) AS close_exact,
      (SELECT close FROM prices_daily p
        WHERE p.trade_date <= g.trade_date AND p.ticker = g.ticker AND p.close IS NOT NULL
        ORDER BY p.trade_date DESC LIMIT 1) AS close_ffill
    FROM grid g
    ORDER BY g.trade_date, g.ticker;
    """

    df = con.execute(q, [args.start, args.end, tks]).df()
    con.close()

    print(f"[DB] {db_path}")
    print(f"[VERSION] {args.version}  [SIGNAL_DATE] {args.signal_date}")
    print(f"[TICKERS] {', '.join(tks)}")
    print(f"[RANGE] {args.start} ~ {args.end}")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
