from __future__ import annotations
import argparse
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--versions", default="V1,V2,V3,V4")
    args = ap.parse_args()

    versions = [v.strip() for v in args.versions.split(",") if v.strip()]
    con = duckdb.connect(str(ROOT / "data/store/alpha_tracker.duckdb"))
    try:
        for v in versions:
            n_days = con.execute(
                """
                SELECT COUNT(DISTINCT trade_date)
                FROM picks_daily
                WHERE version=? AND trade_date BETWEEN ? AND ?
                """,
                [v, args.start, args.end],
            ).fetchone()[0]
            n_rows = con.execute(
                """
                SELECT COUNT(*)
                FROM picks_daily
                WHERE version=? AND trade_date BETWEEN ? AND ?
                """,
                [v, args.start, args.end],
            ).fetchone()[0]
            print(f"[OK] {v}: pick_days={n_days}, pick_rows={n_rows}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
