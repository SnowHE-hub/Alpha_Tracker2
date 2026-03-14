"""
Config structure tests (I-1): scoring.v1.weights, v2_v3_v4.bt_column_weights, versions.
"""

from pathlib import Path

import pytest

from alpha_tracker2.core.config import get_raw_config, load_settings


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_config_loads_and_has_scoring_v1_weights() -> None:
    """I1-2: scoring.v1.weights exists and is readable."""
    root = _project_root()
    config = get_raw_config(root)
    scoring = config.get("scoring") or {}
    v1 = scoring.get("v1") or {}
    weights = v1.get("weights")
    assert weights is not None, "scoring.v1.weights must exist"
    assert isinstance(weights, dict)
    assert "ret_5d" in weights or "ret_20d" in weights or "avg_amount_20" in weights


def test_config_has_v2_v3_v4_bt_column_weights() -> None:
    """I1-2: scoring.v2_v3_v4.bt_column_weights (or features.bt_weights) exists."""
    root = _project_root()
    config = get_raw_config(root)
    scoring = config.get("scoring") or {}
    v234 = scoring.get("v2_v3_v4") or {}
    bt = v234.get("bt_column_weights")
    assert bt is not None, "scoring.v2_v3_v4.bt_column_weights must exist"
    assert isinstance(bt, dict)
    assert "bt_mean" in bt
    assert "bt_winrate" in bt
    assert "bt_worst_mdd" in bt


def test_config_has_v2_v3_v4_versions() -> None:
    """I1-2: scoring.v2_v3_v4.versions (per-version overrides) can exist."""
    root = _project_root()
    config = get_raw_config(root)
    scoring = config.get("scoring") or {}
    v234 = scoring.get("v2_v3_v4") or {}
    versions = v234.get("versions")
    assert versions is not None
    assert isinstance(versions, dict)
    assert "V2" in versions or "V3" in versions or "V4" in versions


def test_load_settings_still_works() -> None:
    """Existing load_settings() still works and returns Settings."""
    root = _project_root()
    settings = load_settings(root)
    assert settings.project_name == "alpha_tracker2_us_hk"
    assert "store" in str(settings.store_db) and "alpha_tracker" in str(settings.store_db)
