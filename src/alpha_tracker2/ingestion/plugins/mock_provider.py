from __future__ import annotations

from datetime import date
from typing import List

from alpha_tracker2.ingestion.base import UniverseProvider, UniverseRow


class MockUniverseProvider:
    name = "mock"

    def fetch_universe(self, trade_date: date) -> List[UniverseRow]:
        # 固定返回，保证可复现
        return [
            UniverseRow(ticker="000001.SZ", name="平安银行"),
            UniverseRow(ticker="600519.SH", name="贵州茅台"),
            UniverseRow(ticker="000858.SZ", name="五粮液"),
        ]
