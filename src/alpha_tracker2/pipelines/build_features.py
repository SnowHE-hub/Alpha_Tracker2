"""
Build daily price/volume features into features_daily from prices_daily.

CLI:
    --date YYYY-MM-DD (default latest US trading day)
    --tickers T1,T2,... (optional; if omitted, use picks_daily version='UNIVERSE')
    --limit N (when deriving tickers from UNIVERSE; default 50)

Idempotency:
    For the target trade_date and selected tickers, DELETE then INSERT
    into features_daily so repeated runs are safe.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.features.price_features import compute_price_features
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def _resolve_trade_date(arg_date: str | None, cal: TradingCalendar) -> date:
    if arg_date:
        return date.fromisoformat(arg_date)
    return cal.latest_trading_day("US")


def _load_tickers_from_universe(store: DuckDBStore, trade_date: date, limit: int) -> list[str]:
    rows = store.fetchall(
        """SELECT ticker FROM picks_daily
           WHERE trade_date = ? AND version = 'UNIVERSE'
           ORDER BY rank
           LIMIT ?""",
        [trade_date.isoformat(), limit],
    )
    return [r[0] for r in rows]


def _compute_window(cal: TradingCalendar, trade_date: date, lookback_days: int = 260) -> tuple[date, date, List[date]]:
    """
    Compute a trading-day window ending at trade_date with up to lookback_days history.
    """
    # Approximate calendar start then restrict to trading days
    approx_start = trade_date - timedelta(days=lookback_days * 2)
    days = cal.trading_days(approx_start, trade_date, "US")
    if not days:
        return trade_date, trade_date, [trade_date]
    window = days[-lookback_days:]
    return window[0], window[-1], window


def _fetch_prices_window(
    store: DuckDBStore,
    start: date,
    end: date,
    tickers: Iterable[str],
) -> pd.DataFrame:
    tickers_list = list({t for t in tickers})
    if not tickers_list:
        return pd.DataFrame()

    placeholders = ",".join(["?"] * len(tickers_list))
    params: list[object] = [start.isoformat(), end.isoformat(), *tickers_list]
    rows = store.fetchall(
        f"""SELECT trade_date, ticker, adj_close, amount
            FROM prices_daily
            WHERE trade_date >= ? AND trade_date <= ?
              AND ticker IN ({placeholders})
            ORDER BY trade_date, ticker""",
        params,
    )
    if not rows:
        return pd.DataFrame(columns=["trade_date", "ticker", "adj_close", "amount"])

    df = pd.DataFrame(rows, columns=["trade_date", "ticker", "adj_close", "amount"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def _write_features(
    store: DuckDBStore,
    features_df: pd.DataFrame,
    trade_date: date,
) -> int:
    if features_df.empty:
        return 0

    # Ensure index and columns
    if not isinstance(features_df.index, pd.MultiIndex):
        features_df = features_df.set_index(["trade_date", "ticker"])

    features_df = features_df.sort_index()
    trade_date_str = trade_date.isoformat()

    # Prepare rows for insertion
    cols = [
        "trade_date",
        "ticker",
        "ret_1d",
        "ret_5d",
        "ret_10d",
        "ret_20d",
        "vol_5d",
        "vol_ann_60d",
        "mdd_60d",
        "ma5",
        "ma10",
        "ma20",
        "ma60",
        "ma5_gt_ma10_gt_ma20",
        "ma20_above_ma60",
        "ma20_slope",
        "avg_amount_20",
    ]

    def _to_date(val) -> date:
        if isinstance(val, date) and not isinstance(val, pd.Timestamp):
            return val
        return pd.Timestamp(val).date()

    records: list[tuple] = []
    tickers: list[str] = []
    for (ts, ticker), row in features_df.iterrows():
        if _to_date(ts) != trade_date:
            continue
        tickers.append(str(ticker))
        records.append(
            (
                trade_date_str,
                str(ticker),
                float(row.get("ret_1d")) if pd.notna(row.get("ret_1d")) else None,
                float(row.get("ret_5d")) if pd.notna(row.get("ret_5d")) else None,
                float(row.get("ret_10d")) if pd.notna(row.get("ret_10d")) else None,
                float(row.get("ret_20d")) if pd.notna(row.get("ret_20d")) else None,
                float(row.get("vol_5d")) if pd.notna(row.get("vol_5d")) else None,
                float(row.get("vol_ann_60d")) if pd.notna(row.get("vol_ann_60d")) else None,
                float(row.get("mdd_60d")) if pd.notna(row.get("mdd_60d")) else None,
                float(row.get("ma5")) if pd.notna(row.get("ma5")) else None,
                float(row.get("ma10")) if pd.notna(row.get("ma10")) else None,
                float(row.get("ma20")) if pd.notna(row.get("ma20")) else None,
                float(row.get("ma60")) if pd.notna(row.get("ma60")) else None,
                bool(row.get("ma5_gt_ma10_gt_ma20"))
                if pd.notna(row.get("ma5_gt_ma10_gt_ma20"))
                else None,
                bool(row.get("ma20_above_ma60"))
                if pd.notna(row.get("ma20_above_ma60"))
                else None,
                float(row.get("ma20_slope")) if pd.notna(row.get("ma20_slope")) else None,
                float(row.get("avg_amount_20")) if pd.notna(row.get("avg_amount_20")) else None,
            )
        )

    if not records:
        return 0

    unique_tickers = sorted(set(tickers))

    placeholders_tickers = ",".join(["?"] * len(unique_tickers))
    delete_params: list[object] = [trade_date_str, *unique_tickers]

    with store.session() as conn:
        conn.execute(
            f"""DELETE FROM features_daily
                WHERE trade_date = ?
                  AND ticker IN ({placeholders_tickers})""",
            delete_params,
        )
        conn.executemany(
            """INSERT INTO features_daily (
                trade_date, ticker,
                ret_1d, ret_5d, ret_10d, ret_20d,
                vol_5d, vol_ann_60d, mdd_60d,
                ma5, ma10, ma20, ma60,
                ma5_gt_ma10_gt_ma20, ma20_above_ma60, ma20_slope,
                avg_amount_20
            ) VALUES (
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?
            )""",
            records,
        )

    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build daily price features into features_daily.")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target trade date YYYY-MM-DD; default latest US trading day",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated tickers; if omitted, use UNIVERSE from picks_daily",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max tickers from UNIVERSE when --tickers not set",
    )
    args = parser.parse_args()

    project_root = _find_project_root(Path(__file__).resolve())
    settings = load_settings(project_root)
    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()
    cal = TradingCalendar()

    trade_date = _resolve_trade_date(args.date, cal)

    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = _load_tickers_from_universe(store, trade_date, args.limit)
        if not tickers:
            print(
                "No tickers from picks_daily (version=UNIVERSE). "
                "Run ingest_universe first or pass --tickers.",
            )
            return

    start, end, window_days = _compute_window(cal, trade_date, lookback_days=260)
    prices_df = _fetch_prices_window(store, start, end, tickers)
    if prices_df.empty:
        print("No prices found in prices_daily for requested window/tickers.")
        return

    # Compute features for the target date only
    features_df = compute_price_features(
        prices_df,
        trading_days=window_days,
        target_trade_date=trade_date,
    )
    written = _write_features(store, features_df, trade_date)

    print(f"build_features: trade_date={trade_date}, tickers={len(set(tickers))}")
    print(f"  window=[{start}, {end}] trading_days={len(window_days)}")
    print(f"  wrote_rows={written}")
    print(f"  db={settings.store_db}")


if __name__ == "__main__":
    main()

