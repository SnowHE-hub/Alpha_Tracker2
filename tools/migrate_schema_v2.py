# tools/migrate_schema_v2.py
from __future__ import annotations

import argparse
from pathlib import Path
import duckdb


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    q = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?"
    return con.execute(q, [table]).fetchone()[0] > 0


def existing_columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    if not table_exists(con, table):
        return set()
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return {r[1] for r in rows}


def add_column_if_missing(con: duckdb.DuckDBPyConnection, table: str, col: str, col_type: str) -> None:
    cols = existing_columns(con, table)
    if col in cols:
        return
    con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")


def execute_schema_without_indexes(con: duckdb.DuckDBPyConnection, sql_text: str) -> None:
    """
    Execute schema statements but skip ALL CREATE INDEX statements.
    This avoids binder errors when old tables exist without new columns.
    """
    stmts: list[str] = []
    buf: list[str] = []

    def flush():
        if not buf:
            return
        stmt = "\n".join(buf).strip()
        buf.clear()
        if not stmt:
            return
        # Skip any CREATE INDEX (case-insensitive)
        head = stmt.lstrip().upper()
        if head.startswith("CREATE INDEX") or head.startswith("CREATE UNIQUE INDEX"):
            return
        stmts.append(stmt)

    for line in sql_text.splitlines():
        buf.append(line)
        if ";" in line:
            flush()
    flush()

    for s in stmts:
        con.execute(s)


def safe_create_index(con: duckdb.DuckDBPyConnection, sql: str, table: str, needed_cols: list[str]) -> None:
    cols = existing_columns(con, table)
    if all(c in cols for c in needed_cols):
        con.execute(sql)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--schema_v2", required=True)
    args = ap.parse_args()

    db_path = Path(args.db)
    schema_path = Path(args.schema_v2)
    if not schema_path.exists():
        raise FileNotFoundError(f"Missing schema_v2.sql: {schema_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        con.execute("BEGIN;")

        # 1) Create tables (skip any indexes in schema file)
        raw_sql = schema_path.read_text(encoding="utf-8")
        execute_schema_without_indexes(con, raw_sql)

        # 2) Patch legacy tables to include v2 columns

        # nav_daily
        if table_exists(con, "nav_daily"):
            nav_cols = {
                "strategy_id": "VARCHAR",
                "day_ret_gross": "DOUBLE",
                "nav_gross": "DOUBLE",
                "turnover": "DOUBLE",
                "cost_bps": "INTEGER",
                "cost": "DOUBLE",
                "n_picks": "INTEGER",
                "n_valid": "INTEGER",
                "picks_trade_date": "DATE",
                "asof_date": "DATE",
                "created_at": "TIMESTAMP",
            }
            for c, t in nav_cols.items():
                add_column_if_missing(con, "nav_daily", c, t)

        # positions_daily (你现在这个表很可能是旧版本/临时表，列非常少)
        if table_exists(con, "positions_daily"):
            pos_cols = {
                "asof_date": "DATE",
                "strategy_id": "VARCHAR",
                "ticker": "VARCHAR",
                "avg_cost": "DOUBLE",
                "price": "DOUBLE",
                "market_value": "DOUBLE",
                "pnl_pct": "DOUBLE",
                "hold_days": "INTEGER",
                "tp_half_done": "BOOLEAN",
                "score": "DOUBLE",
                "meta_json": "VARCHAR",
                "created_at": "TIMESTAMP",
            }
            for c, t in pos_cols.items():
                add_column_if_missing(con, "positions_daily", c, t)

        # trades_daily
        if table_exists(con, "trades_daily"):
            trade_cols = {
                "trade_date": "DATE",
                "strategy_id": "VARCHAR",
                "ticker": "VARCHAR",
                "side": "VARCHAR",
                "shares": "BIGINT",
                "price": "DOUBLE",
                "notional": "DOUBLE",
                "cost": "DOUBLE",
                "reason": "VARCHAR",
                "created_at": "TIMESTAMP",
            }
            for c, t in trade_cols.items():
                add_column_if_missing(con, "trades_daily", c, t)

        # eval_5d_batch_daily
        if table_exists(con, "eval_5d_batch_daily"):
            add_column_if_missing(con, "eval_5d_batch_daily", "strategy_id", "VARCHAR")

        # 3) Create indexes safely AFTER columns exist
        safe_create_index(con,
                          "CREATE INDEX IF NOT EXISTS idx_nav_daily_trade_date ON nav_daily(trade_date);",
                          "nav_daily", ["trade_date"])
        safe_create_index(con,
                          "CREATE INDEX IF NOT EXISTS idx_nav_daily_strategy ON nav_daily(strategy_id, trade_date);",
                          "nav_daily", ["strategy_id", "trade_date"])
        safe_create_index(con,
                          "CREATE INDEX IF NOT EXISTS idx_positions_strategy_date ON positions_daily(strategy_id, asof_date);",
                          "positions_daily", ["strategy_id", "asof_date"])
        safe_create_index(con,
                          "CREATE INDEX IF NOT EXISTS idx_signals_strategy_date ON signals(strategy_id, trade_date);",
                          "signals", ["strategy_id", "trade_date"])
        safe_create_index(con,
                          "CREATE INDEX IF NOT EXISTS idx_fills_strategy_date ON fills(strategy_id, trade_date);",
                          "fills", ["strategy_id", "trade_date"])

        con.execute("COMMIT;")
        print("[OK] schema v2 migrated successfully.")
    except Exception:
        con.execute("ROLLBACK;")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
