from __future__ import annotations
import argparse
from pathlib import Path

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--table", default="universe_daily_hot", help="universe table name")
    ap.add_argument("--head", type=int, default=20)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    cfg = load_settings(root)

    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    cnt = store.fetchall(
        f"SELECT COUNT(*) FROM {args.table} WHERE trade_date = ?;",
        (args.date,),
    )[0][0]
    print(f"table: {args.table}")
    print(f"trade_date: {args.date}")
    print(f"rows: {cnt}")

    if cnt:
        rows = store.fetchall(
            f"""
            SELECT ticker
            FROM {args.table}
            WHERE trade_date = ?
            ORDER BY ticker
            LIMIT {int(args.head)};
            """,
            (args.date,),
        )
        sample = [r[0] for r in rows]
        print("sample tickers:", sample)

        # 如果有 name 列就顺便打印
        try:
            rows2 = store.fetchall(
                f"""
                SELECT ticker, name
                FROM {args.table}
                WHERE trade_date = ?
                ORDER BY ticker
                LIMIT {int(min(args.head, 10))};
                """,
                (args.date,),
            )
            print("sample (ticker,name):", rows2)
        except Exception:
            pass

    print("db:", cfg.store_db)


if __name__ == "__main__":
    main()
