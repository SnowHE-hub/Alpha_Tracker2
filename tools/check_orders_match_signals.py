# tools/check_orders_match_signals.py
import argparse
import duckdb
import pandas as pd
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=r"D:\alpha_tracker2\data\store\alpha_tracker.duckdb")
    ap.add_argument("--version", required=True)
    ap.add_argument("--date", required=True, help="signal_date / trade_date, e.g. 2026-01-07")
    args = ap.parse_args()

    db = args.db
    v = args.version
    d = args.date

    con = duckdb.connect(db)

    picks = con.execute(
        """
        SELECT trade_date, version, ticker, rank, score, picked_by
        FROM picks_daily
        WHERE version = ? AND trade_date = ?
        ORDER BY rank
        """,
        [v, d],
    ).df()

    orders = con.execute(
        """
        SELECT signal_date, version, ticker, target_weight, prev_weight, delta_weight, created_at
        FROM orders_daily
        WHERE version = ? AND signal_date = ?
        ORDER BY ticker
        """,
        [v, d],
    ).df()

    print(f"[DB] {db}")
    print(f"[CHECK] version={v} date={d}")
    print("\n=== picks_daily ===")
    print(picks.to_string(index=False) if len(picks) else "(none)")

    print("\n=== orders_daily ===")
    print(orders.to_string(index=False) if len(orders) else "(none)")

    if len(picks) == 0 or len(orders) == 0:
        print("\n[WARN] missing picks or orders; cannot compare.")
        return

    sp = set(picks["ticker"].tolist())
    so = set(orders["ticker"].tolist())

    only_in_picks = sorted(list(sp - so))
    only_in_orders = sorted(list(so - sp))

    print("\n=== diff ===")
    print("only_in_picks :", only_in_picks)
    print("only_in_orders:", only_in_orders)

    if not only_in_picks and not only_in_orders:
        print("\n[PASS] orders_daily tickers match picks_daily tickers.")
    else:
        print("\n[FAIL] orders_daily tickers DO NOT match picks_daily tickers.")
        print("=> Next step: patch generate_orders to write correct tickers (or purge old rows before insert).")

    con.close()

if __name__ == "__main__":
    main()
