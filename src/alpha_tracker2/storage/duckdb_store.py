from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional, Sequence

import duckdb


@dataclass(frozen=True)
class DuckDBStore:
    db_path: Path
    schema_path: Path

    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        Create a new DuckDB connection to the configured database path.

        Ensures the parent directory for the database file exists.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.db_path))

    def init_schema(self) -> None:
        """
        Initialize the database schema from the schema.sql file.

        The SQL file may contain multiple statements.
        After applying schema.sql, ensures features_daily has bt_* columns (I-1 migration).
        """
        if not self.schema_path.is_file():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")

        sql = self.schema_path.read_text(encoding="utf-8")
        with self.session() as conn:
            conn.execute(sql)
            self._migrate_features_daily_bt_columns(conn)

    @staticmethod
    def _migrate_features_daily_bt_columns(conn: duckdb.DuckDBPyConnection) -> None:
        """Add bt_mean, bt_winrate, bt_worst_mdd to features_daily if missing (idempotent)."""
        try:
            existing = conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'features_daily' AND column_name IN ('bt_mean','bt_winrate','bt_worst_mdd')"
            ).fetchall()
        except Exception:
            return
        existing_names = {row[0] for row in existing}
        for col in ("bt_mean", "bt_winrate", "bt_worst_mdd"):
            if col not in existing_names:
                conn.execute(f"ALTER TABLE features_daily ADD COLUMN {col} DOUBLE")

    def exec(self, sql: str, params: Optional[Sequence[object]] = None) -> None:
        """
        Execute a SQL statement that does not return rows.
        """
        with self.session() as conn:
            if params is not None:
                conn.execute(sql, params)
            else:
                conn.execute(sql)

    def fetchall(
        self, sql: str, params: Optional[Sequence[object]] = None
    ) -> list[tuple]:
        """
        Execute a query and return all rows as a list of tuples.
        """
        with self.session() as conn:
            if params is not None:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)
            return result.fetchall()

    def fetchone(
        self, sql: str, params: Optional[Sequence[object]] = None
    ) -> Optional[tuple]:
        """
        Execute a query and return the first row, or None if there is no result.
        """
        with self.session() as conn:
            if params is not None:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)
            row = result.fetchone()
            return row if row is not None else None

    @contextmanager
    def session(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """
        Context manager that yields a DuckDB connection and closes it on exit.
        """
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

