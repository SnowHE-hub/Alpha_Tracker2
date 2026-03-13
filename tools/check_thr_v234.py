# check_thr_v234.py
# Purpose: Verify threshold fields (thr_value/pass_thr/picked_by) for V2/V3/V4 in picks_daily.

from __future__ import annotations

import duckdb
import pandas as pd


DB_PATH = r"D:\alpha_tracker2\data\store\alpha_tracker.duckdb"
TRADE_DATE = "2026-01-14"  # change if needed


def main() -> None:
    con = duckdb.connect(DB_PATH)

    sql = f"""
    SELECT
        version,
        rank,
        ticker,
        score,
        score_100,
        thr_value,
        pass_thr,
        picked_by,
        reason
    FROM picks_daily
    WHERE trade_date = '{TRADE_DATE}'
      AND version IN ('V2','V3','V4')
    ORDER BY version, rank
    """

    df = con.execute(sql).fetchdf()
    con.close()

    if df.empty:
        print(f"[WARN] No rows found for trade_date={TRADE_DATE} versions=V2/V3/V4")
        return

    # Make output easier to read in console
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_colwidth", 80)

    print(f"\n=== picks_daily threshold fields for V2/V3/V4 (trade_date={TRADE_DATE}) ===")
    print(df)

    # Quick sanity checks
    agg = (
        df.assign(
            thr_is_null=df["thr_value"].isna(),
            pass_is_null=df["pass_thr"].isna(),
            picked_by_is_null=df["picked_by"].isna(),
        )
        .groupby("version", as_index=False)
        .agg(
            n=("ticker", "count"),
            thr_null=("thr_is_null", "sum"),
            pass_null=("pass_is_null", "sum"),
            picked_by_null=("picked_by_is_null", "sum"),
            thr_min=("thr_value", "min"),
            thr_max=("thr_value", "max"),
        )
    )

    print("\n=== summary ===")
    print(agg)


if __name__ == "__main__":
    main()
