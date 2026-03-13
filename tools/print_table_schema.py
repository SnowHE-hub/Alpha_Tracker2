import argparse
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(ROOT / "data/store/alpha_tracker.duckdb"))
    try:
        rows = con.execute(f"PRAGMA table_info('{args.table}');").fetchall()
        for r in rows:
            print(r)
    finally:
        con.close()

if __name__ == "__main__":
    main()
