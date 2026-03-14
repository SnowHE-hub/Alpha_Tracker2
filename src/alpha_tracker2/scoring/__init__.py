from __future__ import annotations

"""
Scoring package: model-layer score computation on top of features_daily.

Public surface:
  - scoring.base: core interfaces and shared utilities
  - scoring.registry: version → Scorer mapping
  - scoring.thresholds: AB-style rolling threshold helpers
  - scoring.plugins.*: concrete scorer implementations (V1–V4)
"""

__all__ = []

