from __future__ import annotations
import argparse
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "data/store/alpha_tracker.duckdb"))
    ap.add_argument("--strategy_id", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args = ap.parse_args()

    con = duckdb.connect(args.db)
    try:
        sid = args.strategy_id
        start = args.start
        end = args.end

        n_pos_days = con.execute("""
          SELECT COUNT(DISTINCT asof_date)
          FROM positions_daily
          WHERE strategy_id=? AND asof_date BETWEEN ? AND ?
        """,[sid, start, end]).fetchone()[0]

        n_nav_days = con.execute("""
          SELECT COUNT(*)
          FROM nav_daily
          WHERE strategy_id=? AND trade_date BETWEEN ? AND ?
        """,[sid, start, end]).fetchone()[0]

        first_last_nav = con.execute("""
          SELECT MIN(trade_date), MAX(trade_date), MIN(nav), MAX(nav)
          FROM nav_daily
          WHERE strategy_id=? AND trade_date BETWEEN ? AND ?
        """,[sid, start, end]).fetchone()

        print("[OK] strategy_id:", sid)
        print("[OK] positions days:", n_pos_days)
        print("[OK] nav rows:", n_nav_days)
        print("[OK] nav (min_date, max_date, min_nav, max_nav):", first_last_nav)

    finally:
        con.close()

if __name__ == "__main__":
    main()
