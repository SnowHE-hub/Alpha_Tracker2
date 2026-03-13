from __future__ import annotations

from datetime import date
from typing import List

import akshare as ak
import pandas as pd

from alpha_tracker2.ingestion.prices_base import PriceRow

def _to_ak_symbol(ticker: str) -> str:
    """
    stock_zh_a_hist 常用 symbol 传 6 位股票代码（不带市场前缀）
     """
    t = ticker.strip().upper()
    if t.endswith(".SH") or t.endswith(".SZ"):
        t = t.split(".")[0]
    return t

class AkSharePriceProvider:
    name = "akshare_prices"

    def fetch_prices(self, ticker: str, start: date, end: date) -> List[PriceRow]:
        symbol = _to_ak_symbol(ticker)

        # AkShare 接口：A 股历史行情（前复权/不复权等后面再加参数）
        # start_date/end_date 格式 YYYYMMDD
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            adjust="",
        )


        if df is None or df.empty:
            return []

        # 常见列名：日期/开盘/收盘/最高/最低/成交量/成交额
        # 做一层兼容
        colmap = {c: c for c in df.columns}
        date_col = "日期" if "日期" in colmap else ("date" if "date" in colmap else None)
        if date_col is None:
            raise ValueError(f"Unexpected columns from ak.stock_zh_a_hist: {list(df.columns)}")

        def pick(*names):
            for n in names:
                if n in colmap:
                    return n
            return None

        open_c = pick("开盘", "open")
        high_c = pick("最高", "high")
        low_c = pick("最低", "low")
        close_c = pick("收盘", "close")
        vol_c = pick("成交量", "volume")
        amt_c = pick("成交额", "amount")

        # 转换日期并过滤范围（双保险）
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col]).dt.date
        df = df[(df[date_col] >= start) & (df[date_col] <= end)]
        df = df.sort_values(date_col).drop_duplicates(subset=[date_col], keep="last")

        rows: List[PriceRow] = []
        for _, r in df.iterrows():
            rows.append(
                PriceRow(
                    trade_date=r[date_col],
                    ticker=ticker,
                    open=float(r[open_c]) if open_c and pd.notna(r[open_c]) else None,
                    high=float(r[high_c]) if high_c and pd.notna(r[high_c]) else None,
                    low=float(r[low_c]) if low_c and pd.notna(r[low_c]) else None,
                    close=float(r[close_c]) if close_c and pd.notna(r[close_c]) else None,
                    volume=float(r[vol_c]) if vol_c and pd.notna(r[vol_c]) else None,
                    amount=float(r[amt_c]) if amt_c and pd.notna(r[amt_c]) else None,
                )
            )
        return rows
