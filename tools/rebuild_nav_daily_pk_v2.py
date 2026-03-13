from __future__ import annotations

import argparse
from pathlib import Path
import duckdb


def table_exists(con, name: str) -> bool:
    return con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?",
        [name],
    ).fetchone()[0] > 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    con = duckdb.connect(str(db_path))
    try:
        con.execute("BEGIN;")

        if not table_exists(con, "nav_daily"):
            raise RuntimeError("nav_daily does not exist")

        # 1) Create new table with PK(trade_date, strategy_id)
        con.execute("""
        CREATE TABLE IF NOT EXISTS nav_daily_v2 (
          trade_date DATE,
          picks_trade_date VARCHAR,
          asof_date DATE,
          version VARCHAR,
          strategy_id VARCHAR,
          day_ret DOUBLE,
          nav DOUBLE,
          n_picks INTEGER,
          n_valid INTEGER,
          day_ret_gross DOUBLE,
          nav_gross DOUBLE,
          turnover DOUBLE,
          cost_bps DOUBLE,
          cost DOUBLE,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(trade_date, strategy_id)
        );
        """)

        # 2) Clear v2 table (idempotent rebuild)
        con.execute("DELETE FROM nav_daily_v2;")

        # 3) Copy data with de-dup
        #    - If old rows have NULL/empty strategy_id, build a legacy id so PK is valid
        #    - If duplicates exist for (trade_date, strategy_id), keep the latest by created_at
        con.execute("""
        INSERT INTO nav_daily_v2 (
          trade_date, picks_trade_date, asof_date, version, strategy_id,
          day_ret, nav, n_picks, n_valid,
          day_ret_gross, nav_gross,
          turnover, cost_bps, cost, created_at
        )
        SELECT
          trade_date,
          picks_trade_date,
          asof_date,
          version,
          CASE
            WHEN strategy_id IS NULL OR TRIM(strategy_id) = ''
              THEN version || '__LEGACY__H0__TOP0__C0'
            ELSE strategy_id
          END AS strategy_id_fixed,
          day_ret, nav, n_picks, n_valid,
          day_ret_gross, nav_gross,
          turnover, cost_bps, cost,
          created_at
        FROM (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY trade_date,
                           CASE
                             WHEN strategy_id IS NULL OR TRIM(strategy_id) = ''
                               THEN version || '__LEGACY__H0__TOP0__C0'
                             ELSE strategy_id
                           END
              ORDER BY created_at DESC NULLS LAST
            ) AS rn
          FROM nav_daily
        ) t
        WHERE rn = 1;
        """)

        # 4) Replace old table
        con.execute("DROP TABLE nav_daily;")
        con.execute("ALTER TABLE nav_daily_v2 RENAME TO nav_daily;")

        con.execute("COMMIT;")
        print("[OK] nav_daily rebuilt with PK(trade_date, strategy_id).")
    except Exception:
        con.execute("ROLLBACK;")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
