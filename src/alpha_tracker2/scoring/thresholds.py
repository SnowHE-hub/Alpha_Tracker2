from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np


@dataclass
class ThresholdConfig:
    q: float = 0.9          # 分位数，比如 0.9 / 0.95
    window: int = 60        # 交易日窗口长度（不是自然日）
    min_samples: int = 200  # 历史样本数不足时的降级阈值策略触发点


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_history(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"meta": {"schema": 1}, "versions": {}}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_history(path: Path, obj: Dict[str, Any]) -> None:
    _ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _to_float_list(scores) -> List[float]:
    arr = np.asarray(list(scores), dtype=float)
    arr = arr[np.isfinite(arr)]
    return arr.tolist()


def update_history(
    path: Path,
    trade_date: date,
    version: str,
    scores,
    keep_last: int = 180,
) -> Dict[str, Any]:
    """
    保存每次运行得到的一组 score 样本（通常是当日 universe 的 score 分布）。
    keep_last: 最多保留多少个 trade_date 的记录（避免文件无限膨胀）
    """
    obj = load_history(path)
    obj.setdefault("versions", {})
    obj["versions"].setdefault(version, [])
    scores_list = _to_float_list(scores)

    # 写入一条记录
    obj["versions"][version].append(
        {"trade_date": str(trade_date), "n": len(scores_list), "scores": scores_list}
    )

    # 只保留最近 keep_last 次
    if len(obj["versions"][version]) > keep_last:
        obj["versions"][version] = obj["versions"][version][-keep_last:]

    save_history(path, obj)
    return obj


def get_threshold(
    path: Path,
    version: str,
    cfg: Optional[ThresholdConfig] = None,
) -> float:
    """
    从历史记录里拿最近 window 次的 score 样本做分位数阈值。
    若历史不足，则降级：
      - 有历史但样本少：用所有历史样本
      - 完全无历史：返回 0.0（让策略更“宽松”，避免全空）
    """
    cfg = cfg or ThresholdConfig()
    obj = load_history(path)
    versions = obj.get("versions", {})
    records = versions.get(version, [])
    if not records:
        return 0.0

    # 取最近 window 条记录合并
    use_records = records[-cfg.window:] if len(records) >= cfg.window else records
    all_scores: List[float] = []
    for r in use_records:
        all_scores.extend(_to_float_list(r.get("scores", [])))

    if len(all_scores) == 0:
        return 0.0

    # 样本不足时：仍然算分位数，只是波动会更大
    thr = float(np.quantile(np.asarray(all_scores, dtype=float), cfg.q))
    return thr
