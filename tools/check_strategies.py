# tools/check_strategies.py
from __future__ import annotations

import argparse
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "data" / "store" / "alpha_tracker.duckdb"))
    args = ap.parse_args()

    con = duckdb.connect(args.db)
    try:
        n = con.execute("SELECT COUNT(*) FROM strategies").fetchone()[0]
        print(f"[OK] strategies count: {n}")
        rows = con.execute("""
            SELECT strategy_id, model_version, trade_rule, hold_n, topk, cost_bps
            FROM strategies
            ORDER BY strategy_id
            LIMIT 10
        """).fetchall()
        for r in rows:
            print(r)
    finally:
        con.close()

if __name__ == "__main__":
    main()
