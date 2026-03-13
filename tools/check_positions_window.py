# tools/check_positions_window.py
from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=str, default="", help="DuckDB path; default data/store/alpha_tracker.duckdb")
    ap.add_argument("--version", type=str, default="ENS")
    ap.add_argument("--start", type=str, required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", type=str, required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--show_schema", action="store_true", help="Print positions_daily schema then exit")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    db_path = Path(args.db) if args.db else (root / "data" / "store" / "alpha_tracker.duckdb")
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    con = duckdb.connect(str(db_path))

    if args.show_schema:
        print(f"[DB] {db_path}")
        print("=== DESCRIBE positions_daily ===")
        print(con.execute("DESCRIBE positions_daily").df().to_string(index=False))
        con.close()
        return

    # positions_daily uses asof_date for daily mark-to-market date
    q = """
    SELECT
      asof_date,
      version,
      COUNT(*) AS n_pos,
      ROUND(SUM(market_value), 2) AS mv_sum,
      SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS n_null_price
    FROM positions_daily
    WHERE version = ?
      AND asof_date BETWEEN ? AND ?
    GROUP BY 1,2
    ORDER BY 1;
    """
    df = con.execute(q, [args.version, args.start, args.end]).df()
    con.close()

    print(f"[DB] {db_path}")
    print(f"[RANGE] {args.start} to {args.end}  version={args.version}")
    if df.empty:
        print("[WARN] No rows found in positions_daily for this window.")
        return

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
