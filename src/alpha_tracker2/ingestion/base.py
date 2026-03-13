from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol, List


@dataclass(frozen=True)
class UniverseRow:
    ticker: str
    name: str


class UniverseProvider(Protocol):
    """
    插件接口：给定 trade_date，返回股票池（ticker/name）
    """
    name: str

    def fetch_universe(self, trade_date: date) -> List[UniverseRow]:
        ...
