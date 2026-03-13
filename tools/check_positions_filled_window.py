from __future__ import annotations

import argparse
import duckdb
import pandas as pd

DB_PATH = r"D:\alpha_tracker2\data\store\alpha_tracker.duckdb"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args = ap.parse_args()

    con = duckdb.connect(DB_PATH)
    q = """
    SELECT
      asof_date, version,
      COUNT(*) AS n_rows,
      SUM(market_value_filled) AS mv_sum_filled,
      MAX(cash) AS cash_max,
      SUM(CASE WHEN price_filled IS NULL AND ticker <> '__CASH__' THEN 1 ELSE 0 END) AS n_missing_price_filled
    FROM positions_daily_filled
    WHERE version = ?
      AND asof_date BETWEEN ? AND ?
      AND ticker <> '__CASH__'
    GROUP BY 1,2
    ORDER BY 1;
    """
    df = con.execute(q, [args.version, args.start, args.end]).df()
    con.close()

    df["mv_sum_filled"] = pd.to_numeric(df["mv_sum_filled"], errors="coerce")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
