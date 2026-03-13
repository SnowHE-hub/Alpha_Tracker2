# tools/find_missing_prices_for_nav.py
from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=str, default="", help="DuckDB path; default data/store/alpha_tracker.duckdb")
    ap.add_argument("--version", type=str, default="ENS")
    ap.add_argument("--signal_date", type=str, required=True, help="Signal date (picks_trade_date), e.g. 2026-01-07")
    ap.add_argument("--start", type=str, required=True, help="Mark-to-market start date, e.g. 2026-01-08")
    ap.add_argument("--end", type=str, required=True, help="Mark-to-market end date, e.g. 2026-01-14")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    db_path = Path(args.db) if args.db else (root / "data" / "store" / "alpha_tracker.duckdb")
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    con = duckdb.connect(str(db_path))

    # 1) tickers used by ENS signal_date
    picks_q = """
    SELECT ticker
    FROM picks_daily
    WHERE version = ?
      AND trade_date = ?
    ORDER BY ticker;
    """
    tickers = con.execute(picks_q, [args.version, args.signal_date]).df()
    if tickers.empty:
        con.close()
        raise RuntimeError(f"No picks found: version={args.version}, trade_date={args.signal_date}")

    # 2) build a date grid and left join prices
    # prices_daily expected columns: trade_date, ticker, close
    grid_q = """
    WITH tickers AS (
      SELECT ticker FROM picks_daily
      WHERE version = ? AND trade_date = ?
    ),
    dates AS (
      SELECT * FROM generate_series(CAST(? AS DATE), CAST(? AS DATE), INTERVAL 1 DAY) AS t(d)
    )
    SELECT
      dates.d AS trade_date,
      tickers.ticker AS ticker,
      p.close AS close
    FROM dates
    CROSS JOIN tickers
    LEFT JOIN prices_daily p
      ON p.trade_date = dates.d AND p.ticker = tickers.ticker
    ORDER BY 1,2;
    """
    df = con.execute(grid_q, [args.version, args.signal_date, args.start, args.end]).df()
    con.close()

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    missing = df[df["close"].isna()].copy()

    print(f"[DB] {db_path}")
    print(f"[VERSION] {args.version}")
    print(f"[SIGNAL_DATE] {args.signal_date}")
    print(f"[RANGE] {args.start} to {args.end}")
    print(f"[TICKERS] {', '.join(tickers['ticker'].tolist())}")
    print(f"[TOTAL_ROWS] {len(df)}  [MISSING_CLOSE_ROWS] {len(missing)}")

    if missing.empty:
        print("[OK] No missing close in prices_daily for these tickers/dates.")
        return

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print("\n=== Missing close rows ===")
    print(missing.to_string(index=False))


if __name__ == "__main__":
    main()
