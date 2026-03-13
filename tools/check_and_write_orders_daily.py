from __future__ import annotations

import argparse
from datetime import datetime, date
from pathlib import Path

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore

ROOT = Path(__file__).resolve().parents[1]


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders_csv", required=True, help=r"path to orders csv, e.g. data/out/orders/orders_ENS_2026-01-07_top3.csv")
    ap.add_argument("--nav_start", required=True, help="nav range start, YYYY-MM-DD")
    ap.add_argument("--nav_end", required=True, help="nav range end, YYYY-MM-DD")
    args = ap.parse_args()

    orders_path = Path(args.orders_csv)
    if not orders_path.is_file():
        raise FileNotFoundError(f"orders_csv not found: {orders_path}")

    nav_start = _parse_date(args.nav_start)
    nav_end = _parse_date(args.nav_end)

    cfg = load_settings(ROOT)
    store = DuckDBStore(cfg.store_db, ROOT / "src" / "alpha_tracker2" / "storage" / "schema.sql")
    store.init_schema()

    # ----------------------------
    # Load orders CSV
    # ----------------------------
    od = pd.read_csv(orders_path, dtype={"ticker": str, "version": str})
    if od.empty:
        raise RuntimeError("orders csv is empty")

    # required columns
    need_cols = {"signal_date", "version", "ticker", "action", "prev_weight", "target_weight", "delta_weight"}
    missing = need_cols - set(od.columns)
    if missing:
        raise ValueError(f"orders csv missing columns: {sorted(missing)}")

    signal_date = _parse_date(str(od["signal_date"].iloc[0]))
    version = str(od["version"].iloc[0])

    # turnover from orders weights
    od["prev_weight"] = pd.to_numeric(od["prev_weight"], errors="coerce").fillna(0.0)
    od["target_weight"] = pd.to_numeric(od["target_weight"], errors="coerce").fillna(0.0)
    od["abs_diff"] = (od["target_weight"] - od["prev_weight"]).abs()
    turnover_orders = 0.5 * float(od["abs_diff"].sum())

    # ----------------------------
    # Ensure orders_daily table exists (avoid schema.sql edit, do ALTER/CREATE here)
    # ----------------------------
    with store.session() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS orders_daily (
              signal_date DATE,
              version VARCHAR,
              ticker VARCHAR,
              name VARCHAR,
              action VARCHAR,
              prev_weight DOUBLE,
              target_weight DOUBLE,
              delta_weight DOUBLE,
              source_csv VARCHAR,
              created_at TIMESTAMP
            );
            """
        )

        # delete existing same (signal_date, version) then insert
        con.execute("BEGIN;")
        try:
            con.execute("DELETE FROM orders_daily WHERE signal_date = ? AND version = ?", (signal_date, version))

            od2 = od.copy()
            od2["signal_date"] = pd.to_datetime(od2["signal_date"], errors="coerce").dt.date
            od2["version"] = od2["version"].astype(str)
            od2["ticker"] = od2["ticker"].astype(str)
            if "name" not in od2.columns:
                od2["name"] = ""
            od2["name"] = od2["name"].fillna("").astype(str)
            od2["action"] = od2["action"].astype(str)
            od2["delta_weight"] = pd.to_numeric(od2["delta_weight"], errors="coerce").fillna(0.0)

            od2["source_csv"] = str(orders_path)
            od2["created_at"] = datetime.now()

            con.register("orders_df", od2[[
                "signal_date", "version", "ticker", "name", "action",
                "prev_weight", "target_weight", "delta_weight",
                "source_csv", "created_at"
            ]])

            con.execute(
                """
                INSERT INTO orders_daily
                SELECT * FROM orders_df
                """
            )
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise

    # ----------------------------
    # Compare with nav_daily turnover if available
    # nav_daily turnover stored on NAV rows (trade_date=t_next) based on rebalance at picks_trade_date=t
    # So we should look at nav_daily rows where picks_trade_date == signal_date
    # ----------------------------
    nav_rows = store.fetchall(
        """
        SELECT trade_date, turnover, cost_bps, cost
        FROM nav_daily
        WHERE version = ?
          AND trade_date BETWEEN ? AND ?
          AND picks_trade_date = ?
        ORDER BY trade_date
        """,
        (version, nav_start, nav_end, str(signal_date)),
    )

    print("[OK] orders_daily written.")
    print("signal_date:", signal_date, "version:", version)
    print("turnover_from_orders:", f"{turnover_orders:.6f}")

    if not nav_rows:
        print("[WARN] No matching nav_daily rows found for picks_trade_date == signal_date in given range.")
        print("You can still proceed; just ensure you have run portfolio_nav for this version & range.")
        return

    # nav turnover could vary per t_next if holdings missing etc; usually constant for that rebalance
    nav_turnovers = [r[1] for r in nav_rows if r[1] is not None]
    nav_turnover = float(nav_turnovers[0]) if nav_turnovers else None

    print("[OK] nav_daily matched rows:", len(nav_rows))
    if nav_turnover is not None:
        print("turnover_from_nav_daily:", f"{nav_turnover:.6f}")
        diff = abs(nav_turnover - turnover_orders)
        print("turnover_abs_diff:", f"{diff:.6f}")
        if diff <= 1e-9:
            print("[PASS] turnover matches exactly.")
        else:
            print("[WARN] turnover mismatch (check topk, version, or signal date).")

    # show a small preview
    preview = nav_rows[:5]
    print("\n=== nav_daily preview (first 5) ===")
    for td, to, bps, cost in preview:
        print("trade_date:", td, "turnover:", to, "cost_bps:", bps, "cost:", cost)


if __name__ == "__main__":
    main()
