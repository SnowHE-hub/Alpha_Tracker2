from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def parse_args():
    ap = argparse.ArgumentParser(description="Diagnose price history coverage for a given universe and date.")
    ap.add_argument("--date", required=True, help="trade_date YYYY-MM-DD")
    ap.add_argument("--limit", type=int, default=300, help="top N tickers from UNIVERSE (default 300)")
    ap.add_argument(
        "--universe-source",
        choices=["universe_picks", "hot"],
        default="universe_picks",
        help="universe_picks: picks_daily(version=UNIVERSE); hot: universe_daily_hot",
    )
    return ap.parse_args()


def main():
    args = parse_args()
    trade_date = pd.to_datetime(args.date).date()

    root = Path(__file__).resolve().parents[1]
    cfg = load_settings(root)
    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    with store.session() as con:
        # load tickers
        if args.universe_source == "hot":
            df_u = con.execute(
                "SELECT DISTINCT ticker FROM universe_daily_hot WHERE trade_date = ?",
                (trade_date,),
            ).fetchdf()
            tickers = df_u["ticker"].tolist()
        else:
            df_u = con.execute(
                """
                SELECT ticker
                FROM picks_daily
                WHERE trade_date = ?
                  AND version = 'UNIVERSE'
                ORDER BY rank
                LIMIT ?;
                """,
                (trade_date, int(args.limit)),
            ).fetchdf()
            tickers = df_u["ticker"].tolist()

        if not tickers:
            raise RuntimeError("No tickers found for that date/universe-source.")

        # per ticker min/max date and count up to trade_date
        df = con.execute(
            f"""
            SELECT
              ticker,
              MIN(trade_date) AS first_date,
              MAX(trade_date) AS last_date,
              COUNT(*) AS n_rows,
              SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS n_amount_null
            FROM prices_daily
            WHERE ticker IN ({",".join(["?"] * len(tickers))})
              AND trade_date <= ?
            GROUP BY ticker
            ORDER BY n_rows ASC;
            """,
            (*tickers, trade_date),
        ).fetchdf()

    print("\n=== Diagnose prices_daily history ===")
    print("trade_date:", trade_date)
    print("universe_tickers:", len(tickers))
    print("tickers_with_any_prices:", df.shape[0])

    if df.empty:
        print("[FAIL] No prices found for these tickers.")
        return

    # quantify sufficiency for 20d/60d/150d (lookback+hold)
    def rate_ge(n: int) -> float:
        return float((df["n_rows"] >= n).mean())

    print("\nCoverage thresholds (fraction of tickers with at least N trading rows up to date):")
    for n in [21, 61, 151, 221]:
        print(f"  >= {n}: {rate_ge(n):.3f}")

    # amount availability
    df["amount_nonnull_rate"] = 1.0 - (df["n_amount_null"] / df["n_rows"]).clip(0, 1)
    print("\namount non-null rate summary:")
    print(df["amount_nonnull_rate"].describe(percentiles=[0.1, 0.5, 0.9]))

    print("\nWorst 20 tickers by history (fewest rows):")
    print(df.head(20)[["ticker", "first_date", "last_date", "n_rows", "amount_nonnull_rate"]])

    print("\nBest 20 tickers by history (most rows):")
    print(df.tail(20)[["ticker", "first_date", "last_date", "n_rows", "amount_nonnull_rate"]])


if __name__ == "__main__":
    main()
