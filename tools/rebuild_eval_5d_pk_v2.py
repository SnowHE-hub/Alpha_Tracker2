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

        if not table_exists(con, "eval_5d_batch_daily"):
            raise RuntimeError("eval_5d_batch_daily does not exist")

        # v2: PK(trade_date, strategy_id)
        con.execute("""
        CREATE TABLE IF NOT EXISTS eval_5d_batch_daily_v2 (
          trade_date DATE,
          version VARCHAR,
          coverage DOUBLE,
          hit_rate DOUBLE,
          avg_ret_5d DOUBLE,
          median_ret_5d DOUBLE,
          eval_n_picks INTEGER,
          eval_n_valid INTEGER,
          extra VARCHAR,
          strategy_id VARCHAR,
          PRIMARY KEY(trade_date, strategy_id)
        );
        """)

        # idempotent rebuild
        con.execute("DELETE FROM eval_5d_batch_daily_v2;")

        # move + de-dup:
        # - if strategy_id empty, build legacy id so PK valid
        # - if duplicates exist, keep arbitrary one (rn=1) since we'll overwrite later anyway
        con.execute("""
        INSERT INTO eval_5d_batch_daily_v2 (
          trade_date, version, coverage, hit_rate, avg_ret_5d, median_ret_5d,
          eval_n_picks, eval_n_valid, extra, strategy_id
        )
        SELECT
          trade_date,
          version,
          coverage,
          hit_rate,
          avg_ret_5d,
          median_ret_5d,
          eval_n_picks,
          eval_n_valid,
          extra,
          strategy_id_fixed
        FROM (
          SELECT
            trade_date,
            version,
            coverage,
            hit_rate,
            avg_ret_5d,
            median_ret_5d,
            eval_n_picks,
            eval_n_valid,
            extra,
            CASE
              WHEN strategy_id IS NULL OR TRIM(strategy_id) = ''
                THEN version || '__LEGACY__H0__TOP0__C0'
              ELSE strategy_id
            END AS strategy_id_fixed,
            ROW_NUMBER() OVER (
              PARTITION BY trade_date,
                           CASE
                             WHEN strategy_id IS NULL OR TRIM(strategy_id) = ''
                               THEN version || '__LEGACY__H0__TOP0__C0'
                             ELSE strategy_id
                           END
            ) AS rn
          FROM eval_5d_batch_daily
        ) t
        WHERE rn = 1;
        """)

        con.execute("DROP TABLE eval_5d_batch_daily;")
        con.execute("ALTER TABLE eval_5d_batch_daily_v2 RENAME TO eval_5d_batch_daily;")

        con.execute("COMMIT;")
        print("[OK] eval_5d_batch_daily rebuilt with PK(trade_date, strategy_id).")
    except Exception:
        con.execute("ROLLBACK;")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
