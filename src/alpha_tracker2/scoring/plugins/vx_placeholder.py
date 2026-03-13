from __future__ import annotations

import pandas as pd

from alpha_tracker2.scoring.base import Scorer


class PlaceholderScorer(Scorer):
    """
    占位 Scorer：用于第14步先跑通 registry + score_all 的并行机制。
    行为：按 features 的原顺序给一个简单 score（全为 0），并写 reason 标注 placeholder。
    第15步会替换为真实 V2/V3/V4 逻辑。
    """

    def __init__(self, version: str):
        self.version = version

    def score(self, features: pd.DataFrame, trade_date) -> pd.DataFrame:
        if features is None or features.empty:
            return pd.DataFrame(columns=["trade_date", "version", "ticker", "name", "rank", "score", "reason"])

        df = features.copy()
        if "name" not in df.columns:
            df["name"] = None

        # 简单给个稳定排序：ticker 升序
        df = df.sort_values("ticker").reset_index(drop=True)
        df["rank"] = range(1, len(df) + 1)
        df["score"] = 0.0
        df["reason"] = f"{self.version} placeholder (to be implemented in step 15)"
        df["trade_date"] = trade_date
        df["version"] = self.version

        return df[["trade_date", "version", "ticker", "name", "rank", "score", "reason"]]
