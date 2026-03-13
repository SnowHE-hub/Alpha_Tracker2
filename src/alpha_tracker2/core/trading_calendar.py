from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import List

import akshare as ak
import pandas as pd


@dataclass(frozen=True)
class TradingCalendar:
    """
    使用 AkShare 的交易日历。
    后续如果你要换数据源/离线缓存，也只改这一层。
    """

    def trading_days(self, start: date, end: date) -> List[date]:
        df = ak.tool_trade_date_hist_sina()
        if df is None or df.empty:
            raise RuntimeError("Failed to load trading calendar from akshare")

        # 兼容列名
        col = "trade_date" if "trade_date" in df.columns else ("交易日期" if "交易日期" in df.columns else df.columns[0])
        days = pd.to_datetime(df[col]).dt.date.tolist()

        return [d for d in days if start <= d <= end]

    def latest_trading_day(self, today: date | None = None) -> date:
        if today is None:
            today = date.today()

        # 向前找，最多回退 30 天（足够覆盖长假）
        start = today - timedelta(days=30)
        days = self.trading_days(start, today)
        if not days:
            raise RuntimeError("No trading days found in last 30 days")
        return max(days)
