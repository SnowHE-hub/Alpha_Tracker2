"""
ENS unit and integration tests (SCR-2): score_ensemble aggregation, picks_daily(version='ENS').

Unit: aggregation logic from fixed picks_daily data.
Integration: score_all + score_ensemble on fixture features_daily; assert ENS rows and reason JSON.
Run: pytest tests/test_score_ensemble.py -v
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from alpha_tracker2.pipelines.score_all import run as score_all_run
from alpha_tracker2.pipelines.score_ensemble import run as score_ensemble_run
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture
def temp_store(tmp_path):
    """Temp DuckDB with schema."""
    root = _project_root()
    schema_path = root / "src" / "alpha_tracker2" / "storage" / "schema.sql"
    store = DuckDBStore(db_path=tmp_path / "test.duckdb", schema_path=schema_path)
    store.init_schema()
    return store


def test_ensemble_after_score_all_writes_ens(temp_store) -> None:
    """
    Unit + integration: insert minimal features_daily + UNIVERSE, run score_all then score_ensemble,
    assert picks_daily has version='ENS' with valid rows and reason JSON (SCR-2, S4-1).
    """
    root = _project_root()
    trade_date = date(2026, 1, 15)
    td_str = trade_date.isoformat()
    tickers = ["AAPL", "MSFT", "GOOGL"]

    # UNIVERSE
    with temp_store.session() as conn:
        for i, t in enumerate(tickers, start=1):
            conn.execute(
                """INSERT INTO picks_daily (trade_date, version, ticker, name, rank, score, score_100, reason, picked_by)
                   VALUES (?, 'UNIVERSE', ?, ?, ?, 0.0, 50.0, '{}', 'UNIVERSE')""",
                [td_str, t, t, i],
            )
    # features_daily: minimal rows so V1/V2/V3/V4 can score
    with temp_store.session() as conn:
        for t in tickers:
            conn.execute(
                """INSERT INTO features_daily (
                     trade_date, ticker, ret_5d, ret_20d, vol_ann_60d, mdd_60d,
                     ma5, ma20, ma60, ma20_slope, avg_amount_20
                   ) VALUES (?, ?, 0.01, 0.02, 0.2, -0.05, 100, 99, 98, 0.01, 1e6)""",
                [td_str, t],
            )

    score_all_run(root, trade_date, versions=["V1", "V2", "V3", "V4"], store=temp_store)
    score_ensemble_run(root, trade_date, versions=["V1", "V2", "V3", "V4"], store=temp_store)

    rows = temp_store.fetchall(
        "SELECT version, ticker, score, score_100, reason FROM picks_daily WHERE trade_date = ? AND version = 'ENS'",
        [td_str],
    )
    assert len(rows) >= 1, "picks_daily must have at least one ENS row (S4-1)"
    for row in rows:
        version, ticker, score, score_100, reason = row
        assert version == "ENS"
        assert ticker and score is not None and score_100 is not None
        data = json.loads(reason)
        assert data.get("method") == "mean_score_100"
        assert "input_versions" in data


def test_ensemble_idempotent(temp_store) -> None:
    """Re-running score_ensemble same date does not duplicate rows (S4-5)."""
    root = _project_root()
    trade_date = date(2026, 1, 15)
    td_str = trade_date.isoformat()
    tickers = ["AAPL", "MSFT"]

    with temp_store.session() as conn:
        for i, t in enumerate(tickers, start=1):
            conn.execute(
                """INSERT INTO picks_daily (trade_date, version, ticker, name, rank, score, score_100, reason, picked_by)
                   VALUES (?, 'UNIVERSE', ?, ?, ?, 0.0, 50.0, '{}', 'U')""",
                [td_str, t, t, i],
            )
        for t in tickers:
            conn.execute(
                """INSERT INTO features_daily (trade_date, ticker, ret_5d, ret_20d, vol_ann_60d, mdd_60d, ma5, ma20, ma60, ma20_slope, avg_amount_20)
                   VALUES (?, ?, 0.01, 0.02, 0.2, -0.05, 100, 99, 98, 0.01, 1e6)""",
                [td_str, t],
            )

    score_all_run(root, trade_date, store=temp_store)
    score_ensemble_run(root, trade_date, store=temp_store)
    n1 = temp_store.fetchone("SELECT COUNT(*) FROM picks_daily WHERE trade_date = ? AND version = 'ENS'", [td_str])
    score_ensemble_run(root, trade_date, store=temp_store)
    n2 = temp_store.fetchone("SELECT COUNT(*) FROM picks_daily WHERE trade_date = ? AND version = 'ENS'", [td_str])
    assert n1 is not None and n2 is not None
    assert int(n1[0]) == int(n2[0]), "Second run must not add duplicate ENS rows"
