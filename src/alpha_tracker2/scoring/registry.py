from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict

from alpha_tracker2.scoring.base import Scorer
from alpha_tracker2.scoring.plugins.v1_baseline import V1BaselineScorer
from alpha_tracker2.scoring.plugins.v2_v3_v4 import V2TrendScorer, V3LowVolScorer, V4TrendMAScorer


def build_registry(root: Path) -> Dict[str, Callable[[], Scorer]]:
    hist_path = root / "data" / "cache" / "ab_threshold_history.json"
    return {
        "V1": lambda: V1BaselineScorer(),
        "V2": lambda: V2TrendScorer(hist_path),
        "V3": lambda: V3LowVolScorer(hist_path),
        "V4": lambda: V4TrendMAScorer(hist_path),
    }


def list_versions(root: Path) -> list[str]:
    return sorted(build_registry(root).keys())


def get_scorer(root: Path, version: str) -> Scorer:
    reg = build_registry(root)
    if version not in reg:
        raise ValueError(f"Unknown scorer version: {version}. Registered: {sorted(reg.keys())}")
    return reg[version]()
