"""
E-3: Unit tests for evaluation/diagnostics.py (version compare, factor analysis).
Uses mock/fixture data; integration test on real DB is separate.
"""

import tempfile
from datetime import date
from pathlib import Path

import pytest

from alpha_tracker2.evaluation.diagnostics import (
    run_diagnostics,
    run_factor_analysis,
    run_version_compare,
)
from alpha_tracker2.storage.duckdb_store import DuckDBStore


@pytest.fixture
def schema_path() -> Path:
    return Path(__file__).resolve().parents[1] / "src" / "alpha_tracker2" / "storage" / "schema.sql"


@pytest.fixture
def store_with_eval_and_picks(schema_path: Path) -> DuckDBStore:
    """Minimal DuckDB with eval_5d_daily and picks_daily rows for [2026-01-01, 2026-01-05]."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.duckdb"
        store = DuckDBStore(db_path=db_path, schema_path=schema_path)
        store.init_schema()
        # Insert eval_5d_daily: 3 dates, 2 versions, bucket=all
        with store.session() as conn:
            conn.executemany(
                """
                INSERT INTO eval_5d_daily (as_of_date, version, bucket, fwd_ret_5d, n_picks, horizon)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    ("2026-01-01", "V1", "all", 0.01, 5, 5),
                    ("2026-01-01", "V2", "all", 0.02, 5, 5),
                    ("2026-01-02", "V1", "all", -0.01, 5, 5),
                    ("2026-01-02", "V2", "all", 0.0, 5, 5),
                    ("2026-01-05", "V1", "all", 0.015, 5, 5),
                    ("2026-01-05", "V2", "all", 0.01, 5, 5),
                ],
            )
            conn.executemany(
                """
                INSERT INTO picks_daily (trade_date, version, ticker, name, rank, score, score_100, reason, thr_value, pass_thr, picked_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("2026-01-01", "V1", "AAPL", None, 1, 0.5, 80.0, "", None, True, "BASELINE_RANK"),
                    ("2026-01-01", "V2", "AAPL", None, 1, 0.6, 90.0, "", 0.4, True, "THRESHOLD"),
                    ("2026-01-02", "V1", "MSFT", None, 1, 0.4, 70.0, "", None, True, "BASELINE_RANK"),
                    ("2026-01-02", "V2", "MSFT", None, 1, 0.45, 75.0, "", 0.35, True, "THRESHOLD"),
                ],
            )
        yield store


def test_version_compare_columns(store_with_eval_and_picks: DuckDBStore) -> None:
    """E3-1: version_compare output has required columns."""
    start = date(2026, 1, 1)
    end = date(2026, 1, 5)
    df = run_version_compare(store_with_eval_and_picks, start, end)
    required = {"version", "mean_fwd_ret_5d", "avg_n_picks", "n_dates", "n_pick_days"}
    assert required.issubset(set(df.columns)), f"Missing columns: {required - set(df.columns)}"
    assert len(df) >= 1


def test_factor_analysis_columns(store_with_eval_and_picks: DuckDBStore) -> None:
    """E3-2: factor_analysis output has factor_name, mean_ic, n_dates (may be 0 if no features)."""
    start = date(2026, 1, 1)
    end = date(2026, 1, 5)
    df = run_factor_analysis(
        store_with_eval_and_picks,
        start,
        end,
        factor_columns=["score"],
        max_dates=5,
    )
    required = {"factor_name", "mean_ic", "std_ic", "n_dates"}
    assert required.issubset(set(df.columns))


def test_run_diagnostics_writes_files(store_with_eval_and_picks: DuckDBStore) -> None:
    """E3-3: run_diagnostics writes version_compare.csv and factor_analysis.csv with fixed schema."""
    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp)
        paths = run_diagnostics(
            store_with_eval_and_picks,
            date(2026, 1, 1),
            date(2026, 1, 5),
            out_dir,
        )
        assert "version_compare" in paths
        assert "factor_analysis" in paths
        assert paths["version_compare"].exists()
        assert paths["factor_analysis"].exists()
        vc = paths["version_compare"].read_text()
        assert "version" in vc and "mean_fwd_ret_5d" in vc
        fa = paths["factor_analysis"].read_text()
        assert "factor_name" in fa
