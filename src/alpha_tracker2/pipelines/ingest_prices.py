"""
Ingest prices into prices_daily from Yahoo (live then cache fallback).

CLI: --date, --start/--end, --last-n (default 60), --limit (default 3), --tickers.
Idempotent: DELETE (ticker, date range) then INSERT.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yaml

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.registry import get_price_provider
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.ingestion.base import PriceRow
from alpha_tracker2.ingestion.cache import PricesCache
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def _load_ingestion_config(project_root: Path) -> dict:
    path = project_root / "configs" / "default.yaml"
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg.get("ingestion") or {}


def _market_from_ticker(ticker: str) -> str:
    return "HK" if ticker.endswith(".HK") else "US"


def _rows_to_dataframe(rows: list[PriceRow]) -> pd.DataFrame:
    data = {
        "open": [r.open for r in rows],
        "high": [r.high for r in rows],
        "low": [r.low for r in rows],
        "close": [r.close for r in rows],
        "adj_close": [r.adj_close for r in rows],
        "volume": [r.volume for r in rows],
        "amount": [r.amount for r in rows],
        "currency": [r.currency for r in rows],
    }
    index = pd.DatetimeIndex([pd.Timestamp(r.trade_date) for r in rows])
    return pd.DataFrame(data, index=index)


def _dataframe_to_rows(df: pd.DataFrame) -> list[PriceRow]:
    rows: list[PriceRow] = []
    for idx, r in df.iterrows():
        d = idx.date() if hasattr(idx, "date") else date(idx.year, idx.month, idx.day)
        open_v = float(r["open"]) if "open" in r and pd.notna(r.get("open")) else None
        high_v = float(r["high"]) if "high" in r and pd.notna(r.get("high")) else None
        low_v = float(r["low"]) if "low" in r and pd.notna(r.get("low")) else None
        close_v = float(r["close"]) if "close" in r and pd.notna(r.get("close")) else None
        adj_v = float(r["adj_close"]) if "adj_close" in r and pd.notna(r.get("adj_close")) else None
        vol = int(r["volume"]) if "volume" in r and pd.notna(r.get("volume")) else None
        amount = float(r["amount"]) if "amount" in r and pd.notna(r.get("amount")) else None
        currency = str(r["currency"]) if "currency" in r and pd.notna(r.get("currency")) else None
        rows.append(
            PriceRow(
                trade_date=d,
                open=open_v,
                high=high_v,
                low=low_v,
                close=close_v,
                adj_close=adj_v,
                volume=vol,
                amount=amount,
                currency=currency,
            )
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest prices into prices_daily.")
    parser.add_argument("--date", type=str, default=None, help="Trade date YYYY-MM-DD")
    parser.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--last-n", type=int, default=60, help="Trading days lookback when start/end not set")
    parser.add_argument("--limit", type=int, default=3, help="Max tickers from UNIVERSE when --tickers not set")
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated tickers (overrides UNIVERSE)")
    args = parser.parse_args()

    project_root = _find_project_root(Path(__file__).resolve())
    settings = load_settings(project_root)
    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()
    cal = TradingCalendar()

    trade_date = (
        date.fromisoformat(args.date)
        if args.date
        else cal.latest_trading_day("US")
    )

    if args.start and args.end:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
    else:
        days = cal.trading_days(
            date(2000, 1, 1),
            trade_date + timedelta(days=1),
            "US",
        )
        # last-n trading days before (and including) trade_date
        window = [d for d in days if d <= trade_date][-args.last_n :]
        if not window:
            start = end = trade_date
        else:
            start = window[0]
            end = trade_date

    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    else:
        rows_db = store.fetchall(
            """SELECT ticker FROM picks_daily
               WHERE trade_date = ? AND version = ?
               ORDER BY rank
               LIMIT ?""",
            [trade_date.isoformat(), "UNIVERSE", args.limit],
        )
        tickers = [r[0] for r in rows_db]
        if not tickers:
            print("No tickers from picks_daily (version=UNIVERSE). Run ingest_universe first.")
            return

    ingestion_cfg = _load_ingestion_config(project_root)
    provider_name = ingestion_cfg.get("prices_provider", "yahoo_prices")
    provider_cls = get_price_provider(provider_name)
    provider = provider_cls()
    cache = PricesCache(settings.lake_dir)
    source_label = "yahoo"

    total_rows = 0
    for ticker in tickers:
        market = _market_from_ticker(ticker)
        with store.session() as conn:
            conn.execute(
                """DELETE FROM prices_daily
                   WHERE ticker = ? AND trade_date >= ? AND trade_date <= ?""",
                [ticker, start.isoformat(), end.isoformat()],
            )

        rows: list[PriceRow] | None = None
        source = "live"
        try:
            rows = provider.fetch_prices(ticker, start, end)
            if rows:
                cache.save(ticker, start, end, _rows_to_dataframe(rows))
        except Exception as e:
            df = cache.load(ticker, start, end)
            if df is not None:
                rows = _dataframe_to_rows(df)
                source = "cache"
            else:
                print(f"  {ticker}: live failed ({e}), no cache")
                continue

        if not rows:
            print(f"  {ticker}: no rows (skipped)")
            continue

        with store.session() as conn:
            for r in rows:
                conn.execute(
                    """INSERT INTO prices_daily (
                        trade_date, ticker, market, open, high, low, close,
                        adj_close, volume, amount, currency, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        r.trade_date.isoformat(),
                        ticker,
                        market,
                        r.open,
                        r.high,
                        r.low,
                        r.close,
                        r.adj_close,
                        r.volume,
                        r.amount,
                        r.currency,
                        source_label,
                    ],
                )
        total_rows += len(rows)
        print(f"  {ticker}: {len(rows)} rows, source={source}, [{start}, {end}]")

    print(f"ingest_prices: total {total_rows} rows")
    print(f"  db={settings.store_db}")


if __name__ == "__main__":
    main()
