from __future__ import annotations
import argparse
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--where_model", default="V1")
    args = ap.parse_args()

    con = duckdb.connect(str(ROOT / "data/store/alpha_tracker.duckdb"))
    try:
        # strategies
        sids = con.execute(
            "SELECT strategy_id FROM strategies WHERE model_version=? ORDER BY strategy_id",
            [args.where_model],
        ).fetchall()
        sids = [r[0] for r in sids]
        print(f"[OK] strategies({args.where_model}) =", len(sids))

        nav_cnt = con.execute(
            """
            SELECT COUNT(*) FROM nav_daily
            WHERE trade_date BETWEEN ? AND ? AND strategy_id IN (SELECT UNNEST(?))
            """,
            [args.start, args.end, sids],
        ).fetchone()[0]
        print("[OK] nav_daily rows in range =", nav_cnt)

        eval_cnt = con.execute(
            """
            SELECT COUNT(*) FROM eval_5d_batch_daily
            WHERE trade_date BETWEEN ? AND ? AND strategy_id IN (SELECT UNNEST(?))
            """,
            [args.start, args.end, sids],
        ).fetchone()[0]
        print("[OK] eval_5d_batch_daily rows in range =", eval_cnt)

        top = con.execute(
            """
            SELECT strategy_id, MAX(nav) AS nav_max
            FROM nav_daily
            WHERE trade_date BETWEEN ? AND ? AND strategy_id IN (SELECT UNNEST(?))
            GROUP BY 1
            ORDER BY nav_max DESC
            LIMIT 10
            """,
            [args.start, args.end, sids],
        ).fetchall()
        print("[OK] top strategies by max(nav):")
        for r in top:
            print(" ", r)
    finally:
        con.close()

if __name__ == "__main__":
    main()
