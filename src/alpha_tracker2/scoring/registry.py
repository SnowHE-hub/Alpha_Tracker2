from __future__ import annotations

from typing import Dict, List

from alpha_tracker2.scoring.base import Scorer
from alpha_tracker2.scoring.plugins.v1_baseline import V1BaselineScorer
from alpha_tracker2.scoring.plugins.v2_v3_v4 import V2Scorer, V3Scorer, V4Scorer


_REGISTRY: Dict[str, Scorer] = {
    "V1": V1BaselineScorer(),
    "V2": V2Scorer(),
    "V3": V3Scorer(),
    "V4": V4Scorer(),
}


def get_scorer(version: str) -> Scorer:
    """
    Return a scorer instance for the given version name.
    """
    key = version.upper()
    try:
        return _REGISTRY[key]
    except KeyError as exc:  # pragma: no cover - simple error path
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise ValueError(f"Unknown scoring version {version!r}. Available: {available}") from exc


def list_versions() -> List[str]:
    """
    List all registered scoring version names.
    """
    return sorted(_REGISTRY.keys())

