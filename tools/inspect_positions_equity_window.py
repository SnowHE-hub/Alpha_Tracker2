from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--initial_equity", type=float, default=100000.0)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    db_path = root / "data" / "store" / "alpha_tracker.duckdb"
    con = duckdb.connect(str(db_path))

    q = r"""
    WITH pos AS (
      SELECT
        asof_date,
        version,
        ticker,
        shares,
        cash
      FROM positions_daily
      WHERE version = ?
        AND asof_date BETWEEN ? AND ?
    ),
    px AS (
      SELECT
        p.asof_date,
        p.version,
        p.ticker,
        p.shares,
        p.cash,
        -- latest close on or before asof_date (ffill)
        (
          SELECT close
          FROM prices_daily d
          WHERE d.ticker = p.ticker
            AND d.trade_date <= p.asof_date
            AND d.close IS NOT NULL
          ORDER BY d.trade_date DESC
          LIMIT 1
        ) AS close_ffill,
        -- exact close on asof_date (may be NULL)
        (
          SELECT close
          FROM prices_daily d
          WHERE d.ticker = p.ticker
            AND d.trade_date = p.asof_date
        ) AS close_exact
      FROM pos p
    )
    SELECT
      asof_date,
      version,
      ticker,
      shares,
      close_exact,
      close_ffill,
      ROUND(shares * close_ffill, 6) AS mv_ffill,
      cash
    FROM px
    ORDER BY asof_date, ticker;
    """

    df = con.execute(q, [args.version, args.start, args.end]).df()
    con.close()

    if df.empty:
        print("[EMPTY] no positions in range")
        return

    # per-day equity breakdown
    df["asof_date"] = pd.to_datetime(df["asof_date"]).dt.date
    df["cash_num"] = pd.to_numeric(df["cash"], errors="coerce")
    df["mv_ffill"] = pd.to_numeric(df["mv_ffill"], errors="coerce")

    # cash可能每行重复/只有一行有值：用 MAX(cash) 当作当日现金
    g = (
        df.groupby(["asof_date", "version"], as_index=False)
        .agg(
            n_pos=("ticker", "count"),
            mv_sum=("mv_ffill", "sum"),
            cash_max=("cash_num", "max"),
            n_missing_exact=("close_exact", lambda s: int(pd.isna(s).sum())),
            n_missing_ffill=("close_ffill", lambda s: int(pd.isna(s).sum())),
        )
    )
    g["equity_ffill"] = g["mv_sum"] + g["cash_max"].fillna(0.0)
    g["nav_ffill"] = g["equity_ffill"] / float(args.initial_equity)

    print(f"[DB] {db_path}")
    print(f"[RANGE] {args.start} ~ {args.end}  version={args.version}  initial_equity={args.initial_equity:g}")
    print("\n=== Positions rows (ticker-level, using ffill close) ===")
    # 只打印窗口内全部行（ENS top3 * 天数，不会太多）
    print(df.to_string(index=False))

    print("\n=== Daily equity from positions (using ffill close) ===")
    print(g.to_string(index=False))


if __name__ == "__main__":
    main()
