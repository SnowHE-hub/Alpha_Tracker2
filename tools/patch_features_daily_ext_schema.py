from __future__ import annotations

import argparse
from pathlib import Path

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


NEW_COLS = [
    ("ret_5d", "DOUBLE"),
    ("ret_10d", "DOUBLE"),
    ("ret_20d", "DOUBLE"),
    ("ma_10", "DOUBLE"),
    ("ma_20", "DOUBLE"),
    ("ma_60", "DOUBLE"),
    ("ma5_gt_ma10_gt_ma20", "INTEGER"),
    ("ma20_above_ma60", "INTEGER"),
    ("ma20_slope", "DOUBLE"),
    ("vol_ann_60d", "DOUBLE"),
    ("mdd_60d", "DOUBLE"),
    ("worst_day_60d", "DOUBLE"),
    ("avg_amount_20", "DOUBLE"),
    ("limit_up_60", "INTEGER"),
    ("limit_down_60", "INTEGER"),
    ("bt_best_style", "VARCHAR"),
    ("bt_mean", "DOUBLE"),
    ("bt_median", "DOUBLE"),
    ("bt_winrate", "DOUBLE"),
    ("bt_p10", "DOUBLE"),
    ("bt_worst", "DOUBLE"),
    ("bt_avg_mdd", "DOUBLE"),
    ("bt_worst_mdd", "DOUBLE"),
]


def parse_args():
    ap = argparse.ArgumentParser(description="Patch features_daily schema with extended columns.")
    ap.add_argument("--dry-run", action="store_true", help="Print SQL only, do not execute.")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    root = Path(__file__).resolve().parents[1]
    cfg = load_settings(root)

    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    sqls = [f"ALTER TABLE features_daily ADD COLUMN IF NOT EXISTS {col} {typ};" for col, typ in NEW_COLS]

    if args.dry_run:
        print("\n".join(sqls))
        return

    with store.session() as con:
        con.execute("BEGIN;")
        try:
            for s in sqls:
                con.execute(s)
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise

        info = con.execute("PRAGMA table_info('features_daily')").fetchdf()

    print("[OK] patched features_daily columns.")
    print(info[["name", "type"]])
    print("db:", cfg.store_db)


if __name__ == "__main__":
    main()
