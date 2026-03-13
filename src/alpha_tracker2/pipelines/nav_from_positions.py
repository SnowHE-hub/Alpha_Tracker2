# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import List

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore

ROOT = Path(__file__).resolve().parents[3]


def _parse_date(s: str) -> date:
    return pd.to_datetime(s).date()


def _read_nav_daily_gross(store: DuckDBStore, start: date, end: date, versions: List[str]) -> pd.DataFrame:
    """
    Read nav_daily with nav_gross for compare.
    Output columns:
      trade_date(str YYYY-MM-DD), version(str), nav_gross_db(float), nav_db(float)
    """
    if not versions:
        return pd.DataFrame()

    rows = store.fetchall(
        """
        SELECT trade_date, version, nav, nav_gross
        FROM nav_daily
        WHERE trade_date BETWEEN ? AND ?
          AND version = ANY(?)
        ORDER BY trade_date, version
        """,
        (start, end, versions),
    )
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["trade_date", "version", "nav", "nav_gross"])
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["version"] = df["version"].astype(str)

    df["nav_db"] = pd.to_numeric(df["nav"], errors="coerce")
    df["nav_gross_db"] = pd.to_numeric(df["nav_gross"], errors="coerce")

    return df[["trade_date", "version", "nav_db", "nav_gross_db"]]


def _exec_nav_gross_from_positions_filled(
    store: DuckDBStore, start: date, end: date, versions: List[str], initial_equity: float
) -> pd.DataFrame:
    """
    Compute gross NAV directly from positions_daily_filled:
      equity_gross = SUM(shares * close_ffill) + MAX(cash)
      nav_exec_gross = equity_gross / initial_equity

    Notes:
    - positions_daily_filled is expected to have one row per (asof_date, version, ticker).
    - cash is repeated per row; use MAX(cash).
    """
    rows = store.fetchall(
        """
        SELECT
            asof_date AS trade_date,
            version,
            SUM(COALESCE(shares, 0) * COALESCE(close_ffill, 0)) AS mv_sum,
            MAX(COALESCE(cash, 0)) AS cash_max,
            SUM(CASE WHEN close_ffill IS NULL THEN 1 ELSE 0 END) AS n_missing_price,
            COUNT(*) AS n_rows
        FROM positions_daily_filled
        WHERE asof_date BETWEEN ? AND ?
          AND version = ANY(?)
          AND ticker IS NOT NULL
          AND ticker <> '__CASH__'
        GROUP BY asof_date, version
        ORDER BY asof_date, version
        """,
        (start, end, versions),
    )
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        rows,
        columns=["trade_date", "version", "mv_sum", "cash", "n_missing_price", "n_rows"],
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["version"] = df["version"].astype(str)

    df["mv_sum"] = pd.to_numeric(df["mv_sum"], errors="coerce").fillna(0.0)
    df["cash"] = pd.to_numeric(df["cash"], errors="coerce").fillna(0.0)

    df["equity_exec_gross"] = df["mv_sum"] + df["cash"]
    df["nav_exec_gross"] = df["equity_exec_gross"] / float(initial_equity) if initial_equity > 0 else float("nan")
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--versions", required=True, help="comma-separated, e.g. ENS or ENS,V1")
    ap.add_argument("--initial_equity", type=float, default=100000.0)
    ap.add_argument("--out_dir", default=str(ROOT / "data" / "out"))
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    versions = [v.strip() for v in str(args.versions).split(",") if v.strip()]
    initial_equity = float(args.initial_equity)

    print("[OK] nav_from_positions started.")
    print("range:", start, "to", end)
    print("versions:", versions)
    print("initial_equity:", initial_equity)

    cfg = load_settings(ROOT)
    store = DuckDBStore(cfg.store_db, ROOT / "src" / "alpha_tracker2" / "storage" / "schema.sql")
    store.init_schema()

    # Ensure view/table exists (fail loudly if missing)
    with store.session() as con:
        con.execute("SELECT 1 FROM positions_daily_filled LIMIT 1;")

    nav_db = _read_nav_daily_gross(store, start, end, versions)
    df_exec = _exec_nav_gross_from_positions_filled(store, start, end, versions, initial_equity)

    if df_exec.empty:
        print("[SKIP] no rows produced.")
        return

    # Compare: exec gross vs nav_gross_db (should be exact if filled view is correct)
    if not nav_db.empty:
        df_cmp = df_exec.merge(nav_db, on=["trade_date", "version"], how="left")

        # primary compare column: nav_gross_db
        df_cmp["abs_diff_gross"] = (df_cmp["nav_exec_gross"] - df_cmp["nav_gross_db"]).abs()

        for v in versions:
            sub = df_cmp[df_cmp["version"] == v].dropna(subset=["nav_gross_db"]).copy()
            if sub.empty:
                print(f"[WARN] no nav_gross_db rows to compare for version={v}")
                continue

            worst = sub.sort_values("abs_diff_gross", ascending=False).iloc[0]
            max_abs = float(sub["abs_diff_gross"].max())

            print(f"\n=== Compare GROSS NAV (version={v}) ===")
            print("rows_exec:", int((df_exec["version"] == v).sum()), "rows_nav_daily:", int(len(sub)))
            print("max_abs_diff_gross:", f"{max_abs:.12f}")
            print(
                "worst_date:",
                worst["trade_date"],
                "nav_exec_gross:",
                round(float(worst["nav_exec_gross"]), 6),
                "nav_gross_db:",
                round(float(worst["nav_gross_db"]), 6),
                "n_rows:",
                int(worst["n_rows"]),
                "missing_px:",
                int(worst["n_missing_price"]),
            )
    else:
        print("[WARN] nav_daily empty for these versions; skipping compare.")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"nav_exec_from_positions_gross_{'_'.join(versions)}_{start}_{end}.csv"
    df_exec.to_csv(out_path, index=False, encoding="utf-8-sig")
    print("\n[OK] exec gross nav exported:", out_path)
    print("\n[OK] nav_from_positions passed.")


if __name__ == "__main__":
    main()
