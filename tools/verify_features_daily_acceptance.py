from __future__ import annotations

"""
Verify the three acceptance criteria for features_daily.

Usage (after running ingest_universe, ingest_prices, and build_features):

    PYTHONPATH=src python tools/verify_features_daily_acceptance.py --date 2026-01-15

This script checks:
  1) features_daily table exists and has rows for the given trade_date.
  2) Required feature columns are present and not all NULL for at least one row.
  3) For that date, each ticker appears at most once (no duplicate (trade_date, ticker)).
"""

import argparse
from datetime import date
from pathlib import Path

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def _verify_features_daily(store: DuckDBStore, trade_date: date) -> bool:
    ok = True
    trade_date_str = trade_date.isoformat()

    # 1) table exists and has rows for trade_date
    row = store.fetchone(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = 'features_daily'
        """,
    )
    if row is None:
        print("[FAIL] features_daily table does not exist.")
        return False

    count_row = store.fetchone(
        "SELECT COUNT(*) FROM features_daily WHERE trade_date = ?",
        [trade_date_str],
    )
    num_rows = int(count_row[0]) if count_row is not None else 0
    if num_rows == 0:
        print(f"[FAIL] No rows in features_daily for trade_date={trade_date_str}.")
        ok = False
    else:
        print(f"[OK] features_daily has {num_rows} rows for trade_date={trade_date_str}.")

    # 2) required feature columns present and not all NULL for at least one row
    required_cols = [
        "ret_1d",
        "ret_5d",
        "ret_10d",
        "ret_20d",
        "vol_5d",
        "vol_ann_60d",
        "mdd_60d",
        "ma5",
        "ma10",
        "ma20",
        "ma60",
        "ma5_gt_ma10_gt_ma20",
        "ma20_above_ma60",
        "ma20_slope",
        "avg_amount_20",
    ]

    # Check columns exist in schema
    cols = store.fetchall(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'features_daily'
        """,
    )
    existing_cols = {c[0] for c in cols}
    missing = [c for c in required_cols if c not in existing_cols]
    if missing:
        print(f"[FAIL] features_daily missing required columns: {missing}")
        ok = False
    else:
        print("[OK] features_daily contains all required feature columns.")

    # Check at least one row where these columns are not all NULL
    non_null_row = store.fetchone(
        f"""
        SELECT 1
        FROM features_daily
        WHERE trade_date = ?
          AND (
            ret_1d IS NOT NULL OR
            ret_5d IS NOT NULL OR
            ret_10d IS NOT NULL OR
            ret_20d IS NOT NULL OR
            vol_5d IS NOT NULL OR
            vol_ann_60d IS NOT NULL OR
            mdd_60d IS NOT NULL OR
            ma5 IS NOT NULL OR
            ma10 IS NOT NULL OR
            ma20 IS NOT NULL OR
            ma60 IS NOT NULL OR
            ma5_gt_ma10_gt_ma20 IS NOT NULL OR
            ma20_above_ma60 IS NOT NULL OR
            ma20_slope IS NOT NULL OR
            avg_amount_20 IS NOT NULL
          )
        LIMIT 1
        """,
        [trade_date_str],
    )
    if non_null_row is None:
        print(
            "[FAIL] All required feature columns are NULL for "
            f"trade_date={trade_date_str} rows.",
        )
        ok = False
    else:
        print(
            "[OK] At least one row for trade_date has non-NULL values "
            "in required feature columns.",
        )

    # 3) uniqueness of (trade_date, ticker) for that date
    uniq_row = store.fetchone(
        """
        SELECT
            COUNT(*) AS n,
            COUNT(DISTINCT ticker) AS n_tickers
        FROM features_daily
        WHERE trade_date = ?
        """,
        [trade_date_str],
    )
    if uniq_row is None:
        print("[FAIL] Could not compute uniqueness statistics.")
        ok = False
    else:
        n, n_tickers = int(uniq_row[0]), int(uniq_row[1])
        if n != n_tickers:
            print(
                "[FAIL] Duplicate rows detected for (trade_date, ticker) on "
                f"{trade_date_str}: total_rows={n}, distinct_tickers={n_tickers}.",
            )
            ok = False
        else:
            print(
                "[OK] For trade_date, each ticker appears exactly once in "
                "features_daily (no duplicates).",
            )

    return ok


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify acceptance criteria for features_daily.",
    )
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Trade date YYYY-MM-DD to verify.",
    )
    args = parser.parse_args()

    trade_date = date.fromisoformat(args.date)
    project_root = _find_project_root(Path(__file__).resolve())
    settings = load_settings(project_root)
    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    ok = _verify_features_daily(store, trade_date)
    if ok:
        print("[PASS] All three acceptance criteria satisfied for features_daily.")
    else:
        print("[FAIL] One or more acceptance criteria failed for features_daily.")


if __name__ == "__main__":
    main()

