"""
Schema tests (I-1): features_daily has bt_mean, bt_winrate, bt_worst_mdd.
"""

import tempfile
from pathlib import Path

import pytest

from alpha_tracker2.storage.duckdb_store import DuckDBStore


def test_features_daily_has_bt_columns() -> None:
    """I1-1: features_daily contains bt_mean, bt_winrate, bt_worst_mdd (DOUBLE, nullable)."""
    root = Path(__file__).resolve().parents[1]
    schema_path = root / "src" / "alpha_tracker2" / "storage" / "schema.sql"
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.duckdb"
        store = DuckDBStore(db_path=db_path, schema_path=schema_path)
        store.init_schema()
        rows = store.fetchall(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'features_daily' AND column_name IN ('bt_mean','bt_winrate','bt_worst_mdd')"
        )
        found = {r[0] for r in rows}
        assert "bt_mean" in found
        assert "bt_winrate" in found
        assert "bt_worst_mdd" in found
        # Type check
        types = store.fetchall(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'features_daily' AND column_name IN ('bt_mean','bt_winrate','bt_worst_mdd')"
        )
        for _name, dtype in types:
            assert "DOUBLE" in (dtype or "").upper()
