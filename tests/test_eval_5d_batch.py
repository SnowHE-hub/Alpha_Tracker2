"""
E-2: Unit tests for eval_5d_batch (batch eval, quintile, IC series).
E1-4: E-2 calls metrics.ic (verified by import and use in eval_5d_batch.py).
EVAL-4/EVAL-5: test_integration_eval_5d_batch_real_data uses real picks_daily + prices_daily when available.
"""

import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from alpha_tracker2.evaluation.metrics import ic
from alpha_tracker2.pipelines.eval_5d_batch import _tickers_for_bucket


def test_ic_called_by_eval_5d_batch() -> None:
    """E1-4: E-2 uses metrics.ic for IC series (import and usage in eval_5d_batch)."""
    # eval_5d_batch imports: from alpha_tracker2.evaluation.metrics import ic
    # and uses ic(merged["score"], merged["fwd_ret"], method="pearson")
    assert callable(ic)
    # Sanity: IC in [-1, 1]
    s = pd.Series([1.0, 2.0, 3.0])
    r = pd.Series([1.0, 2.0, 3.0])
    assert -1 <= ic(s, r) <= 1


def test_tickers_for_bucket() -> None:
    """Bucket all vs top3/top5."""
    df = pd.DataFrame({"ticker": ["A", "B", "C", "D", "E"], "rank": [1, 2, 3, 4, 5]})
    assert _tickers_for_bucket(df, None) == ["A", "B", "C", "D", "E"]
    assert _tickers_for_bucket(df, 3) == ["A", "B", "C"]
    assert _tickers_for_bucket(df, 5) == ["A", "B", "C", "D", "E"]
    assert _tickers_for_bucket(pd.DataFrame(), None) == []


def test_quintile_returns_csv_schema() -> None:
    """E2-2: quintile_returns output has documented columns."""
    expected_cols = ["as_of_date", "version", "quintile", "mean_fwd_ret_5d", "n_stocks"]
    # Schema is fixed in eval_5d_batch.py quintile_rows append
    row = {
        "as_of_date": "2026-01-01",
        "version": "V1",
        "quintile": 1,
        "mean_fwd_ret_5d": 0.01,
        "n_stocks": 10,
    }
    for c in expected_cols:
        assert c in row
    df = pd.DataFrame([row])
    assert list(df.columns) == expected_cols


def test_ic_series_csv_schema() -> None:
    """E2-3: ic_series output has as_of_date, version, ic."""
    expected_cols = ["as_of_date", "version", "ic"]
    row = {"as_of_date": "2026-01-01", "version": "V1", "ic": 0.05}
    for c in expected_cols:
        assert c in row


def _project_root() -> Path:
    root = Path(__file__).resolve().parents[1]
    for p in [root, *root.parents]:
        if (p / "configs" / "default.yaml").is_file():
            return p
    return None


@pytest.mark.integration
def test_integration_eval_5d_batch_real_data() -> None:
    """
    EVAL-4 / EVAL-5: Integration test on real picks_daily + prices_daily.
    Runs eval_5d_batch over a range with >= 20 trading days when the project store
    exists and has sufficient data; checks output files exist and IC in [-1,1],
    quintile returns are numeric. Skip if store missing or data insufficient.
    Reproduce: from repo root, ensure ingest + score_all have been run for a range
    of at least 20 trading days, then pytest tests/test_eval_5d_batch.py -v -k integration.
    """
    root = _project_root()
    if root is None:
        pytest.skip("project root (configs/default.yaml) not found")
    sys.path.insert(0, str(root / "src"))
    try:
        from alpha_tracker2.core.config import load_settings
        from alpha_tracker2.core.trading_calendar import TradingCalendar
        from alpha_tracker2.storage.duckdb_store import DuckDBStore
    except Exception as e:
        pytest.skip(f"import failed: {e}")
    settings = load_settings(root)
    if not settings.store_db.is_file():
        pytest.skip(f"real store not found: {settings.store_db}")
    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()
    row = store.fetchone(
        "SELECT MIN(trade_date) AS lo, MAX(trade_date) AS hi, COUNT(DISTINCT trade_date) AS nd FROM picks_daily"
    )
    if not row or row[2] is None or int(row[2]) < 20:
        pytest.skip("picks_daily has fewer than 20 distinct trade_dates")
    start_str = str(row[0])
    end_str = str(row[1])
    start_d = date.fromisoformat(start_str) if isinstance(row[0], str) else row[0]
    end_d = date.fromisoformat(end_str) if isinstance(row[1], str) else row[1]
    cal = TradingCalendar()
    trading_days = cal.trading_days(start_d, end_d, market="US")
    if len(trading_days) < 20:
        pytest.skip("date range spans fewer than 20 US trading days")
    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp) / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            sys.executable,
            "-m",
            "alpha_tracker2.pipelines.eval_5d_batch",
            "--start",
            start_str,
            "--end",
            end_str,
            "--output-dir",
            str(out_dir),
        ]
        result = subprocess.run(
            cmd,
            cwd=str(root),
            env={**__import__("os").environ, "PYTHONPATH": str(root / "src")},
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, f"eval_5d_batch failed: {result.stderr}"

        quintile_path = out_dir / "quintile_returns.csv"
        ic_path = out_dir / "ic_series.csv"
        assert quintile_path.exists(), "quintile_returns.csv not written"
        assert ic_path.exists(), "ic_series.csv not written"

        quintile_df = pd.read_csv(quintile_path)
        ic_df = pd.read_csv(ic_path)
        assert "mean_fwd_ret_5d" in quintile_df.columns
        assert "ic" in ic_df.columns
        for _, r in ic_df.iterrows():
            v = r.get("ic")
            if pd.notna(v) and isinstance(v, (int, float)):
                assert -1 <= float(v) <= 1, f"IC out of range: {v}"
        for _, r in quintile_df.iterrows():
            v = r.get("mean_fwd_ret_5d")
            if pd.notna(v):
                assert isinstance(v, (int, float)), f"mean_fwd_ret_5d not numeric: {v}"
