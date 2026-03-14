"""
Yahoo price provider: daily OHLCV via yfinance.

Uses yf.download with auto_adjust=False; Adj Close for adj_close, Close for close.
amount approximated as adj_close * volume. Timezone handling deferred; dates from index.
"""

from __future__ import annotations

from datetime import date
import pandas as pd

from alpha_tracker2.ingestion.base import PriceRow


def _as_date(d) -> date:
    if hasattr(d, "date"):
        return d.date()
    return date(d.year, d.month, d.day)


class YahooPriceProvider:
    """Fetches daily OHLCV from Yahoo via yfinance."""

    def fetch_prices(self, ticker: str, start: date, end: date) -> list[PriceRow]:
        import yfinance as yf

        start_s = start.isoformat()
        end_s = end.isoformat()
        # multi_level_index=False so we get flat columns for a single ticker
        df = yf.download(
            ticker,
            start=start_s,
            end=end_s,
            auto_adjust=False,
            progress=False,
            multi_level_index=False,
        )
        if df is None or df.empty:
            raise ValueError(f"No price data for {ticker} between {start_s} and {end_s}")

        # Normalize columns: sometimes multi-index is collapsed to single level
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)

        # Normalize column names (yfinance may use "Adj Close", etc.)
        col_map = {
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
        rename = {}
        for c in df.columns:
            cstr = str(c).strip()
            if cstr in col_map:
                rename[c] = col_map[cstr]
        df = df.rename(columns=rename)

        rows: list[PriceRow] = []
        for idx, r in df.iterrows():
            trade_d = _as_date(idx)
            def _f(key: str):
                if key not in r:
                    return None
                v = r[key]
                return None if pd.isna(v) else v
            open_v = _f("open")
            high_v = _f("high")
            low_v = _f("low")
            close_v = _f("close")
            adj_v = _f("adj_close")
            vol_raw = _f("volume")
            open_v = float(open_v) if open_v is not None else None
            high_v = float(high_v) if high_v is not None else None
            low_v = float(low_v) if low_v is not None else None
            close_v = float(close_v) if close_v is not None else None
            adj_v = float(adj_v) if adj_v is not None else None
            vol = int(vol_raw) if vol_raw is not None else None
            amount = None
            if adj_v is not None and vol is not None:
                amount = float(adj_v * vol)
            rows.append(
                PriceRow(
                    trade_date=trade_d,
                    open=open_v,
                    high=high_v,
                    low=low_v,
                    close=close_v,
                    adj_close=adj_v,
                    volume=vol,
                    amount=amount,
                    currency=None,
                )
            )
        return rows
