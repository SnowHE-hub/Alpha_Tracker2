"""
Integration tests for score_all + score_ensemble on fixture features_daily (SCR-3).

Uses a temp DB with ≥20 trading days of features_daily (and UNIVERSE) in the same structure
as real ingest/build_features output, then runs score_all and score_ensemble and checks
picks_daily (all versions + ENS) and reason JSON.

Delivery (PERVASIVE_TEST_SCORING_TASK):
  - Data: fixture, 25 trading days from 2025-01-01, US tickers (AAPL, MSFT).
  - Source: synthetic rows written in test (no external DB).
  - Reproduce: pytest tests/test_scoring_integration.py -v
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from alpha_tracker2.pipelines.score_all import run as score_all_run
from alpha_tracker2.pipelines.score_ensemble import run as score_ensemble_run
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _trading_days(start: date, n: int) -> list[date]:
    out = []
    d = start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


@pytest.fixture
def temp_store_with_features(tmp_path):
    """Temp DuckDB with schema, features_daily and UNIVERSE for 25 trading days."""
    root = _project_root()
    schema_path = root / "src" / "alpha_tracker2" / "storage" / "schema.sql"
    store = DuckDBStore(db_path=tmp_path / "test.duckdb", schema_path=schema_path)
    store.init_schema()

    tickers = ["AAPL", "MSFT"]
    days = _trading_days(date(2025, 1, 1), 25)
    if len(days) < 20:
        pytest.skip("Need at least 20 trading days")
    target = days[-1]
    target_str = target.isoformat()

    with store.session() as conn:
        for d in days:
            ds = d.isoformat()
            for i, t in enumerate(tickers, start=1):
                conn.execute(
                    """INSERT INTO features_daily (
                         trade_date, ticker, ret_5d, ret_20d, vol_ann_60d, mdd_60d,
                         ma5, ma20, ma60, ma20_slope, avg_amount_20
                       ) VALUES (?, ?, 0.01, 0.02, 0.2, -0.05, 100, 99, 98, 0.01, 1e6)""",
                    [ds, t],
                )
        for i, t in enumerate(tickers, start=1):
            conn.execute(
                """INSERT INTO picks_daily (trade_date, version, ticker, name, rank, score, score_100, reason, picked_by)
                   VALUES (?, 'UNIVERSE', ?, ?, ?, 0.0, 50.0, '{}', 'UNIVERSE')""",
                [target_str, t, t, i],
            )
    yield store, target, target_str


def test_score_all_and_ensemble_on_fixture_features_daily(temp_store_with_features) -> None:
    """
    Integration (SCR-3): On fixture with ≥20 trading days and US tickers, run score_all
    and score_ensemble; assert picks_daily has V1/V2/V3/V4 and ENS, reason is valid JSON.
    """
    store, trade_date, td_str = temp_store_with_features
    root = _project_root()

    score_all_run(root, trade_date, store=store)
    score_ensemble_run(root, trade_date, store=store)

    for version in ("V1", "V2", "V3", "V4"):
        r = store.fetchone(
            "SELECT COUNT(*) FROM picks_daily WHERE trade_date = ? AND version = ?",
            [td_str, version],
        )
        assert r is not None and int(r[0]) >= 1, f"picks_daily must have rows for version={version}"

    r_ens = store.fetchone(
        "SELECT COUNT(*) FROM picks_daily WHERE trade_date = ? AND version = 'ENS'",
        [td_str],
    )
    assert r_ens is not None and int(r_ens[0]) >= 1, "picks_daily must have at least one ENS row"

    rows = store.fetchall(
        "SELECT version, ticker, score, score_100, reason FROM picks_daily WHERE trade_date = ?",
        [td_str],
    )
    for row in rows:
        version, ticker, score, score_100, reason = row
        assert version and ticker
        assert score is not None
        assert reason is not None and len(reason) > 0
        data = json.loads(reason)
        assert isinstance(data, dict)
