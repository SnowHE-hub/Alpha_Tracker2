from __future__ import annotations

from datetime import date, timedelta
from typing import List
import random

from alpha_tracker2.ingestion.prices_base import PriceRow


class MockPriceProvider:
    name = "mock_prices"

    def fetch_prices(self, ticker: str, start: date, end: date) -> List[PriceRow]:
        # 生成可复现的伪价格序列（基于 ticker hash）
        rnd = random.Random(abs(hash(ticker)) % (10**8))

        rows: List[PriceRow] = []
        cur = start
        price = 100.0 + (abs(hash(ticker)) % 1000) / 10.0

        while cur <= end:
            # 简化：每天都生成一条（后面我们会用交易日历过滤）
            drift = rnd.uniform(-0.02, 0.02)
            close = max(1.0, price * (1.0 + drift))
            o = price
            h = max(o, close) * (1.0 + rnd.uniform(0.0, 0.01))
            l = min(o, close) * (1.0 - rnd.uniform(0.0, 0.01))
            vol = rnd.uniform(1e5, 5e5)
            amt = vol * close

            rows.append(
                PriceRow(
                    trade_date=cur,
                    ticker=ticker,
                    open=o,
                    high=h,
                    low=l,
                    close=close,
                    volume=vol,
                    amount=amt,
                )
            )
            price = close
            cur += timedelta(days=1)

        return rows
