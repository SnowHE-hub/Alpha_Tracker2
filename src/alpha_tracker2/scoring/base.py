from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol
import pandas as pd


@dataclass(frozen=True)
class ScoreResult:
    trade_date: date
    ticker: str
    score: float


class Scorer(Protocol):
    name: str

    def score(self, trade_date: date, features: pd.DataFrame) -> pd.DataFrame:
        """
        输入：某个 trade_date 的 features（多只 ticker）
        输出：包含 ticker, score 的 DataFrame
        """
        ...
