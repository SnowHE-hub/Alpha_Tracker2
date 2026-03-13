from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
import duckdb
from contextlib import contextmanager

@dataclass(frozen=True)
class DuckDBStore:
    db_path: Path
    schema_path: Path

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.db_path))


    @contextmanager
    def session(self):
        """
        提供一个“保持连接不关闭”的会话，用于事务/批量写入。
        用法：
          with store.session() as con:
              con.execute("BEGIN;")
              ...
        """
        con = self.connect()
        try:
            yield con
        finally:
            con.close()

    def init_schema(self) -> None:
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Missing schema file: {self.schema_path}")

        con = self.connect()
        try:
            sql = self.schema_path.read_text(encoding="utf-8")
            con.execute(sql)
        finally:
            con.close()

    # ✅ 新增：执行但不返回结果集（适合 INSERT/CREATE）
    def exec(self, sql: str, params: tuple | None = None) -> None:
        con = self.connect()
        try:
            if params is None:
                con.execute(sql)
            else:
                con.execute(sql, params)
        finally:
            con.close()

    # ✅ 新增：查询返回所有行（在 close 前 fetch 完）
    def fetchall(self, sql: str, params: tuple | None = None) -> list[tuple]:
        con = self.connect()
        try:
            rel = con.execute(sql) if params is None else con.execute(sql, params)
            return rel.fetchall()
        finally:
            con.close()

    # ✅ 新增：查询返回单行（在 close 前 fetch 完）
    def fetchone(self, sql: str, params: tuple | None = None) -> tuple | None:
        con = self.connect()
        try:
            rel = con.execute(sql) if params is None else con.execute(sql, params)
            return rel.fetchone()
        finally:
            con.close()
