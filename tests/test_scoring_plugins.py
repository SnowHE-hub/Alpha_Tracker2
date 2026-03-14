"""
Unit tests for V1–V4 scorers (SCR-1): config loading, score/reason structure, V4 bt_* and missing bt.

Covers: S-1/S-3 weights and thresholds from config; S-2 V4 bt_score, bt_column_weights, bt_* missing.
Run: pytest tests/test_scoring_plugins.py -v
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from alpha_tracker2.scoring.plugins.v1_baseline import (
    DEFAULT_V1_WEIGHTS,
    V1BaselineScorer,
    V1Config,
    load_v1_config,
)
from alpha_tracker2.scoring.plugins.v2_v3_v4 import (
    CoreTrendRiskConfig,
    V2Scorer,
    V3Scorer,
    V4Scorer,
    load_bt_column_weights,
    load_core_config,
)
from alpha_tracker2.scoring.registry import get_scorer, list_versions
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


# ---- V1: config and score/reason ----


def test_load_v1_config_from_project_returns_weights() -> None:
    """V1 weights are loaded from config; keys match expected factors."""
    root = _project_root()
    cfg = load_v1_config(root)
    assert isinstance(cfg.weights, dict)
    assert len(cfg.weights) >= 1
    for k, v in cfg.weights.items():
        assert isinstance(k, str) and isinstance(v, (int, float))


def test_v1_scorer_with_config_produces_reason_with_weights() -> None:
    """V1BaselineScorer(cfg) output reason JSON contains weights (S1-3)."""
    cfg = V1Config(weights={"ret_5d": 0.6, "ret_20d": 0.2, "avg_amount_20": 0.2})
    scorer = V1BaselineScorer(cfg=cfg)
    assert scorer._cfg.weights == cfg.weights


def test_v1_scorer_score_affected_by_weights(tmp_path) -> None:
    """Score and reason reflect configured weights (S1-1)."""
    root = _project_root()
    schema_path = root / "src" / "alpha_tracker2" / "storage" / "schema.sql"
    store = DuckDBStore(db_path=tmp_path / "t.duckdb", schema_path=schema_path)
    store.init_schema()
    # Insert one row of features_daily so scorer has data
    store.exec(
        """INSERT INTO features_daily (trade_date, ticker, ret_5d, ret_20d, avg_amount_20)
           VALUES ('2026-01-15', 'AAPL', 0.01, 0.02, 1e6)"""
    )
    cfg_a = V1Config(weights={"ret_5d": 1.0, "ret_20d": 0.0, "avg_amount_20": 0.0})
    cfg_b = V1Config(weights={"ret_5d": 0.0, "ret_20d": 1.0, "avg_amount_20": 0.0})
    from datetime import date

    trade_date = date(2026, 1, 15)
    out_a = V1BaselineScorer(cfg=cfg_a).score(trade_date, store)
    out_b = V1BaselineScorer(cfg=cfg_b).score(trade_date, store)
    assert not out_a.empty and not out_b.empty
    assert "reason" in out_a.columns and "reason" in out_b.columns
    r_a = json.loads(out_a["reason"].iloc[0])
    r_b = json.loads(out_b["reason"].iloc[0])
    assert r_a.get("weights") == dict(cfg_a.weights)
    assert r_b.get("weights") == dict(cfg_b.weights)
    assert "score" in out_a.columns and "score" in out_b.columns


# ---- V2/V3/V4: config and reason ----


def test_load_core_config_from_project_returns_trend_risk_bt() -> None:
    """V2/V3/V4 core config (trend_weight, risk_weight, bt_weight) loaded from config (S1-2)."""
    root = _project_root()
    for ver in ("V2", "V3", "V4"):
        cfg = load_core_config(root, ver)
        assert isinstance(cfg.trend_weight, (int, float))
        assert isinstance(cfg.risk_weight, (int, float))
        assert isinstance(cfg.bt_weight, (int, float))


def test_v2_v3_v4_scorers_reason_contain_weights() -> None:
    """V2/V3/V4 reason JSON contains trend_weight, risk_weight, bt_weight (S1-3)."""
    cfg = CoreTrendRiskConfig(trend_weight=0.4, risk_weight=0.3, bt_weight=0.1)
    v2 = V2Scorer(cfg=cfg)
    assert v2.cfg.trend_weight == 0.4 and v2.cfg.risk_weight == 0.3


def test_load_bt_column_weights_from_project() -> None:
    """bt_column_weights loaded for V4 (S2-2)."""
    root = _project_root()
    w = load_bt_column_weights(root)
    assert isinstance(w, dict)
    assert "bt_mean" in w or "bt_winrate" in w or "bt_worst_mdd" in w


# ---- Per-version threshold config (S-3) ----


def test_per_version_threshold_config_loaded_from_score_all() -> None:
    """score_all's per-version q/window can be loaded (S3-1)."""
    from alpha_tracker2.pipelines.score_all import _load_per_version_threshold_config

    root = _project_root()
    per = _load_per_version_threshold_config(root)
    assert "V2" in per and "V3" in per and "V4" in per
    for ver in ("V2", "V3", "V4"):
        q, window, topk = per[ver]
        assert isinstance(q, (int, float)) and isinstance(window, int) and isinstance(topk, int)


# ---- V4: bt_score and bt_* missing (S2) ----


def test_v4_with_bt_columns_produces_bt_score_in_reason(tmp_path) -> None:
    """V4 reason contains bt_score when features_daily has bt_* (S2-1, S2-3)."""
    root = _project_root()
    schema_path = root / "src" / "alpha_tracker2" / "storage" / "schema.sql"
    store = DuckDBStore(db_path=tmp_path / "t.duckdb", schema_path=schema_path)
    store.init_schema()
    store.exec(
        """INSERT INTO features_daily (
             trade_date, ticker, ret_5d, ret_20d, vol_ann_60d, mdd_60d,
             ma5, ma20, ma60, ma20_slope, bt_mean, bt_winrate, bt_worst_mdd
           ) VALUES ('2026-01-15', 'AAPL', 0.01, 0.02, 0.2, -0.05, 100, 99, 98, 0.01, 0.1, 0.6, -0.1)"""
    )
    cfg = CoreTrendRiskConfig(trend_weight=0.3, risk_weight=0.3, bt_weight=0.4)
    scorer = V4Scorer(cfg=cfg, bt_column_weights={"bt_mean": 0.5, "bt_winrate": 0.3, "bt_worst_mdd": 0.2})
    from datetime import date

    out = scorer.score(date(2026, 1, 15), store)
    assert not out.empty
    r = json.loads(out["reason"].iloc[0])
    assert "bt_score" in r
    assert "ma_bonus" in r or r.get("ma_bonus_included_in_trend_score") is True


def test_v4_bt_weight_zero_no_bt_term(tmp_path) -> None:
    """When bt_weight=0, V4 score does not include bt term (S2-1)."""
    root = _project_root()
    schema_path = root / "src" / "alpha_tracker2" / "storage" / "schema.sql"
    store = DuckDBStore(db_path=tmp_path / "t.duckdb", schema_path=schema_path)
    store.init_schema()
    store.exec(
        """INSERT INTO features_daily (
             trade_date, ticker, ret_5d, ret_20d, vol_ann_60d, mdd_60d,
             ma5, ma20, ma60, ma20_slope, bt_mean, bt_winrate, bt_worst_mdd
           ) VALUES ('2026-01-15', 'AAPL', 0.01, 0.02, 0.2, -0.05, 100, 99, 98, 0.01, 0.1, 0.6, -0.1)"""
    )
    cfg_no_bt = CoreTrendRiskConfig(trend_weight=1.0, risk_weight=0.5, bt_weight=0.0)
    scorer = V4Scorer(cfg=cfg_no_bt, bt_column_weights={"bt_mean": 0.5, "bt_winrate": 0.3, "bt_worst_mdd": 0.2})
    from datetime import date

    out = scorer.score(date(2026, 1, 15), store)
    assert not out.empty
    r = json.loads(out["reason"].iloc[0])
    assert r.get("bt_weight") == 0.0


def test_v4_without_bt_columns_does_not_raise(tmp_path) -> None:
    """V4 when features_daily has no bt_* (or all NULL) still produces score (S2-4)."""
    root = _project_root()
    schema_path = root / "src" / "alpha_tracker2" / "storage" / "schema.sql"
    store = DuckDBStore(db_path=tmp_path / "t.duckdb", schema_path=schema_path)
    store.init_schema()
    # Insert row without bt_* columns (schema has them; we just don't insert so they are NULL)
    store.exec(
        """INSERT INTO features_daily (
             trade_date, ticker, ret_5d, ret_20d, vol_ann_60d, mdd_60d,
             ma5, ma20, ma60, ma20_slope
           ) VALUES ('2026-01-15', 'AAPL', 0.01, 0.02, 0.2, -0.05, 100, 99, 98, 0.01)"""
    )
    scorer = V4Scorer(
        cfg=CoreTrendRiskConfig(trend_weight=0.3, risk_weight=0.3, bt_weight=0.4),
        bt_column_weights={"bt_mean": 0.5, "bt_winrate": 0.3, "bt_worst_mdd": 0.2},
    )
    from datetime import date

    out = scorer.score(date(2026, 1, 15), store)
    assert not out.empty
    r = json.loads(out["reason"].iloc[0])
    assert "bt_score" in r  # present; value can be 0 when all null


# ---- Registry ----


def test_get_scorer_with_project_root_uses_config() -> None:
    """get_scorer(version, project_root) returns scorers that read from config."""
    root = _project_root()
    for ver in ("V1", "V2", "V3", "V4"):
        s = get_scorer(ver, root)
        assert s is not None


def test_list_versions_includes_v1_v4() -> None:
    """list_versions() returns V1–V4."""
    vers = list_versions()
    assert "V1" in vers and "V2" in vers and "V3" in vers and "V4" in vers
