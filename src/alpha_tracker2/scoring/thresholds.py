from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Any

import json
import math

import pandas as pd


@dataclass(frozen=True)
class ThresholdConfig:
    """
    Configuration for rolling quantile thresholds.

    q: target upper-tail quantile for score (e.g. 0.8 → top 20%).
    window: number of past trading days to keep in history (including today).
    """

    q: float = 0.8
    window: int = 60


def _load_history(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.is_file():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except json.JSONDecodeError:
        # Corrupted file – start fresh rather than failing pipeline.
        return {}
    return {}


def _save_history(path: Path, history: Dict[str, Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, sort_keys=True, indent=2)
    tmp.replace(path)


def update_history(
    path: Path,
    version: str,
    trade_date: date,
    scores: pd.Series,
    q: float,
    window: int,
) -> float:
    """
    Update rolling threshold history for a version and return today's threshold.

    History layout (JSON):
      {
        "V2": {
          "dates": ["2025-12-30", "2025-12-31", ...],
          "thresholds": [1.23, 1.10, ...]
        },
        ...
      }
    """
    clean_scores = pd.to_numeric(scores, errors="coerce")
    clean_scores = clean_scores.dropna()
    if clean_scores.empty:
        # Degenerate case: no valid scores; fall back to NaN threshold.
        thr_value = math.nan
    else:
        thr_value = float(clean_scores.quantile(q))

    history = _load_history(path)
    v_key = version.upper()
    version_hist = history.get(v_key, {"dates": [], "thresholds": []})
    dates: List[str] = list(version_hist.get("dates", []))
    thresholds: List[float] = list(version_hist.get("thresholds", []))

    date_str = trade_date.isoformat()
    if dates and dates[-1] == date_str:
        # Overwrite last entry for idempotency on same date.
        thresholds[-1] = thr_value
    else:
        dates.append(date_str)
        thresholds.append(thr_value)

    if window > 0 and len(dates) > window:
        dates = dates[-window:]
        thresholds = thresholds[-window:]

    history[v_key] = {"dates": dates, "thresholds": thresholds}
    _save_history(path, history)

    return thr_value


def get_threshold(path: Path, version: str, cfg: ThresholdConfig, scores: pd.Series, trade_date: date) -> float:
    """
    Convenience wrapper: update history then return today's threshold.
    """
    return update_history(
        path=path,
        version=version,
        trade_date=trade_date,
        scores=scores,
        q=cfg.q,
        window=cfg.window,
    )

