from __future__ import annotations

import argparse
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore

ROOT = Path(__file__).resolve().parents[3]


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _load_orders_daily(store: DuckDBStore, signal_date: date, version: str) -> pd.DataFrame:
    rows = store.fetchall(
        """
        SELECT
          signal_date, version, ticker, name, action,
          prev_weight, target_weight, delta_weight
        FROM orders_daily
        WHERE signal_date = ? AND version = ?
        """,
        (signal_date, version),
    )
    cols = [
        "signal_date", "version", "ticker", "name", "action",
        "prev_weight", "target_weight", "delta_weight"
    ]
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows, columns=cols)
    df["ticker"] = df["ticker"].astype(str)
    df["version"] = df["version"].astype(str)
    df["action"] = df["action"].astype(str)
    for c in ["prev_weight", "target_weight", "delta_weight"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["name"] = df.get("name", "").fillna("").astype(str)
    return df


def _load_close_map(store: DuckDBStore, trade_date: date, tickers: List[str]) -> Dict[str, float]:
    if not tickers:
        return {}
    rows = store.fetchall(
        """
        SELECT ticker, close
        FROM prices_daily
        WHERE trade_date = ?
          AND ticker = ANY(?)
        """,
        (trade_date, tickers),
    )
    out = {}
    for tk, c in rows:
        if c is None:
            continue
        try:
            out[str(tk)] = float(c)
        except Exception:
            pass
    return out


def _round_lot(shares: float, lot_size: int) -> int:
    if shares <= 0:
        return 0
    lots = int(shares // lot_size)
    return lots * lot_size


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade_date", required=True, help="signal date (same as orders_daily.signal_date), YYYY-MM-DD")
    ap.add_argument("--version", required=True, help="e.g. ENS")
    ap.add_argument("--cash", type=float, default=100000.0, help="starting cash")
    ap.add_argument("--lot_size", type=int, default=100, help="A-share lot size (default 100)")
    ap.add_argument("--price_field", default="close", help="reserved (currently uses close)")
    ap.add_argument("--out_dir", default="", help="default: data/out/exec_orders")
    args = ap.parse_args()

    signal_date = _parse_date(args.trade_date)
    version = str(args.version).strip()
    cash = float(args.cash)
    lot_size = int(args.lot_size)

    print("[OK] generate_exec_orders started.")
    print("signal_date:", signal_date)
    print("version:", version)
    print("cash:", cash)
    print("lot_size:", lot_size)

    cfg = load_settings(ROOT)
    store = DuckDBStore(cfg.store_db, ROOT / "src" / "alpha_tracker2" / "storage" / "schema.sql")
    store.init_schema()

    orders = _load_orders_daily(store, signal_date, version)
    if orders.empty:
        print(f"[SKIP] no orders_daily rows for {version} on {signal_date}. Run generate_orders + check_and_write first.")
        return

    # We only need tickers that have target_weight > 0 to form the target portfolio
    tgt = orders[orders["target_weight"] > 0].copy()
    if tgt.empty:
        print("[SKIP] target portfolio is empty (all target_weight=0).")
        return

    tickers = tgt["ticker"].astype(str).tolist()
    px = _load_close_map(store, signal_date, tickers)

    tgt["price"] = tgt["ticker"].map(px)
    tgt["price"] = pd.to_numeric(tgt["price"], errors="coerce")

    # mark missing prices
    missing_price = tgt["price"].isna()
    n_missing = int(missing_price.sum())
    if n_missing > 0:
        print(f"[WARN] missing prices for {n_missing} tickers on {signal_date} (will set shares=0).")

    # target dollar allocation
    tgt["target_value"] = tgt["target_weight"] * cash

    # naive target shares
    tgt["target_shares_raw"] = (tgt["target_value"] / tgt["price"]).where(~tgt["price"].isna(), 0.0)

    # round to lot size
    tgt["target_shares"] = tgt["target_shares_raw"].apply(lambda x: _round_lot(float(x), lot_size))
    tgt["target_lots"] = (tgt["target_shares"] / lot_size).astype(int)

    # notional after rounding
    tgt["est_notional"] = tgt["target_shares"] * tgt["price"].fillna(0.0)

    # cash utilization
    used = float(tgt["est_notional"].sum())
    leftover = cash - used
    util = (used / cash) if cash > 0 else 0.0

    # Build output
    out = tgt[[
        "signal_date", "version", "ticker", "name",
        "price", "target_weight", "target_value",
        "target_shares_raw", "target_lots", "target_shares", "est_notional"
    ]].copy()

    # Sort by target_weight desc
    out = out.sort_values("target_weight", ascending=False)

    out_dir = Path(args.out_dir) if args.out_dir else (ROOT / "data" / "out" / "exec_orders")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"exec_orders_{version}_{signal_date}.csv"
    out.to_csv(out_path, index=False, encoding="utf-8-sig")

    print("[OK] exec orders exported:", out_path)
    print("tickers:", len(out))
    print("cash_used:", f"{used:.2f}", "cash_left:", f"{leftover:.2f}", "utilization:", f"{util:.2%}")
    if n_missing > 0:
        print("[WARN] Some tickers had missing price -> shares set to 0. Check prices_daily coverage.")
    print("[OK] generate_exec_orders passed.")


if __name__ == "__main__":
    main()
