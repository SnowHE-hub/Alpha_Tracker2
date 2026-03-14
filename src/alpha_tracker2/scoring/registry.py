from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from alpha_tracker2.scoring.base import Scorer
from alpha_tracker2.scoring.plugins.v1_baseline import V1BaselineScorer, load_v1_config
from alpha_tracker2.scoring.plugins.v2_v3_v4 import (
    V2Scorer,
    V3Scorer,
    V4Scorer,
    load_bt_column_weights,
    load_core_config,
)

_REGISTRY_KEYS = ("V1", "V2", "V3", "V4")


def get_scorer(version: str, project_root: Path | None = None) -> Scorer:
    """
    Return a scorer instance for the given version name.
    If project_root is provided, config (weights / trend-risk) is loaded from
    configs/default.yaml; otherwise code defaults are used.
    """
    key = version.upper()
    if key not in _REGISTRY_KEYS:
        available = ", ".join(sorted(_REGISTRY_KEYS))
        raise ValueError(f"Unknown scoring version {version!r}. Available: {available}")

    if project_root is not None:
        if key == "V1":
            return V1BaselineScorer(cfg=load_v1_config(project_root))
        if key == "V2":
            return V2Scorer(cfg=load_core_config(project_root, "V2"))
        if key == "V3":
            return V3Scorer(cfg=load_core_config(project_root, "V3"))
        if key == "V4":
            return V4Scorer(
                cfg=load_core_config(project_root, "V4"),
                bt_column_weights=load_bt_column_weights(project_root),
            )

    # Fallback: no project_root, use code defaults (e.g. tests)
    if key == "V1":
        return V1BaselineScorer()
    if key == "V2":
        return V2Scorer()
    if key == "V3":
        return V3Scorer()
    return V4Scorer()


def list_versions() -> List[str]:
    """
    List all registered scoring version names.
    """
    return list(_REGISTRY_KEYS)

