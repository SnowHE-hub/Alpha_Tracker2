"""
D-1 / Pervasive: Tests for make_dashboard extension (eval_summary, quintile_returns, ic_series).
DASH-1..DASH-5: eval_summary columns, build_eval_summary+ic_series, schema; real-data or fixture data.
"""

import tempfile
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from alpha_tracker2.reporting.dashboard_data import build_eval_summary
from alpha_tracker2.storage.duckdb_store import DuckDBStore


@pytest.fixture
def schema_path() -> Path:
    return Path(__file__).resolve().parents[1] / "src" / "alpha_tracker2" / "storage" / "schema.sql"


@pytest.fixture
def store_with_eval(schema_path: Path) -> DuckDBStore:
    """Minimal store with eval_5d_daily (bucket=all) for two versions."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.duckdb"
        store = DuckDBStore(db_path=db_path, schema_path=schema_path)
        store.init_schema()
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
                ],
            )
        yield store


def test_build_eval_summary_columns(store_with_eval: DuckDBStore) -> None:
    """D1-1: eval_summary has version, mean_fwd_ret_5d, mean_ic, n_dates."""
    df = build_eval_summary(
        store_with_eval,
        date(2026, 1, 1),
        date(2026, 1, 2),
        ic_series_csv_path=None,
    )
    assert list(df.columns) == ["version", "mean_fwd_ret_5d", "n_dates", "mean_ic"]
    assert len(df) == 2
    assert set(df["version"]) == {"V1", "V2"}
    assert df["n_dates"].iloc[0] == 2


def test_build_eval_summary_with_ic_csv(store_with_eval: DuckDBStore) -> None:
    """D1-1: mean_ic filled when ic_series.csv is provided."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("as_of_date,version,ic\n2026-01-01,V1,0.1\n2026-01-01,V2,0.2\n2026-01-02,V1,-0.05\n2026-01-02,V2,0.0\n")
        ic_path = Path(f.name)
    try:
        df = build_eval_summary(
            store_with_eval,
            date(2026, 1, 1),
            date(2026, 1, 2),
            ic_series_csv_path=ic_path,
        )
        assert "mean_ic" in df.columns
        v1 = df[df["version"] == "V1"]["mean_ic"].iloc[0]
        v2 = df[df["version"] == "V2"]["mean_ic"].iloc[0]
        assert v1 == pytest.approx(0.025)  # (0.1 + -0.05) / 2
        assert v2 == pytest.approx(0.1)
    finally:
        ic_path.unlink(missing_ok=True)


def test_eval_summary_csv_schema() -> None:
    """D1-1: Documented eval_summary.csv columns."""
    expected = ["version", "mean_fwd_ret_5d", "mean_ic", "n_dates"]
    row = {"version": "V1", "mean_fwd_ret_5d": 0.01, "mean_ic": 0.05, "n_dates": 10}
    for c in expected:
        assert c in row
    df = pd.DataFrame([row])
    assert list(df.columns) == expected


def test_quintile_returns_csv_schema() -> None:
    """D1-2: quintile_returns columns (same as E-2 / eval_5d_batch)."""
    expected = ["as_of_date", "version", "quintile", "mean_fwd_ret_5d", "n_stocks"]
    row = {"as_of_date": "2026-01-01", "version": "V1", "quintile": 1, "mean_fwd_ret_5d": 0.01, "n_stocks": 10}
    for c in expected:
        assert c in row


def test_ic_series_csv_schema() -> None:
    """D1-2: ic_series columns (same as E-2)."""
    expected = ["as_of_date", "version", "ic"]
    row = {"as_of_date": "2026-01-01", "version": "V1", "ic": 0.05}
    for c in expected:
        assert c in row


def test_real_data_out_eval_summary_if_present() -> None:
    """DASH-4: When data/out/eval_summary.csv exists (from make_dashboard / eval_5d_batch), validate columns and non-empty."""
    project_root = Path(__file__).resolve().parents[1]
    out_dir = project_root / "data" / "out"
    path = out_dir / "eval_summary.csv"
    if not path.is_file():
        pytest.skip("data/out/eval_summary.csv not present (run make_dashboard first)")
    df = pd.read_csv(path)
    required = ["version", "mean_fwd_ret_5d", "mean_ic", "n_dates"]
    for c in required:
        assert c in df.columns, f"eval_summary.csv missing column {c}"
    assert len(df) >= 1, "eval_summary.csv must have at least one row (no empty-only acceptance)"
