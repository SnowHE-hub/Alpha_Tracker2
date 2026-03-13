from pathlib import Path
import duckdb

DB_PATH = Path("data/store/alpha_tracker.duckdb")

def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))

    try:
        print("=== positions_daily columns ===")
        rows = con.execute("PRAGMA table_info('positions_daily');").fetchall()
        for r in rows:
            print(r)

        print("\n=== nav_daily columns ===")
        rows = con.execute("PRAGMA table_info('nav_daily');").fetchall()
        for r in rows:
            print(r)

    finally:
        con.close()


if __name__ == "__main__":
    main()
