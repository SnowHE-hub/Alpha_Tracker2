# tools/check_nav_diff_detail.py
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "data" / "store" / "alpha_tracker.duckdb"))
    ap.add_argument("--version", default="ENS")
    ap.add_argument("--exec_csv", required=True, help="nav_exec_from_positions_*.csv")
    ap.add_argument("--topn", type=int, default=10)
    args = ap.parse_args()

    exec_df = pd.read_csv(args.exec_csv)
    # expect columns: trade_date, version, nav_exec
    exec_df["trade_date"] = pd.to_datetime(exec_df["trade_date"]).dt.date
    exec_df = exec_df[exec_df["version"] == args.version].copy()

    con = duckdb.connect(args.db)
    nav_df = con.execute(
        """
        SELECT trade_date, version, nav
        FROM nav_daily
        WHERE version = ?
        ORDER BY trade_date
        """,
        (args.version,),
    ).fetchdf()
    nav_df["trade_date"] = pd.to_datetime(nav_df["trade_date"]).dt.date

    m = exec_df.merge(nav_df, on=["trade_date", "version"], how="inner", suffixes=("_exec", "_db"))
    if m.empty:
        print("[ERR] no overlapping dates between exec_csv and nav_daily.")
        return

    m["abs_diff"] = (m["nav_exec"] - m["nav"]).abs()
    m = m.sort_values("abs_diff", ascending=False)

    print("=== Top diffs (exec vs nav_daily) ===")
    print(m.head(args.topn)[["trade_date", "nav_exec", "nav", "abs_diff"]].to_string(index=False))

    # quick shape check
    print("\nrows_exec_csv:", len(exec_df), "rows_nav_daily:", len(nav_df), "rows_merged:", len(m))

    con.close()


if __name__ == "__main__":
    main()
