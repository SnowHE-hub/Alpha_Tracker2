from __future__ import annotations

import argparse
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore

ROOT = Path(__file__).resolve().parents[3]


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _ensure_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS positions_daily (
          asof_date DATE,
          version VARCHAR,
          ticker VARCHAR,
          shares BIGINT,
          price DOUBLE,
          market_value DOUBLE,
          cash DOUBLE,
          created_at TIMESTAMP
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS trades_daily (
          trade_date DATE,
          version VARCHAR,
          ticker VARCHAR,
          side VARCHAR,
          shares BIGINT,
          price DOUBLE,
          notional DOUBLE,
          created_at TIMESTAMP
        );
        """
    )


def _load_target_from_exec_orders(exec_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(exec_csv, dtype={"ticker": str, "version": str})
    need = {"signal_date", "version", "ticker", "price", "target_shares", "est_notional"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"exec_orders csv missing columns: {sorted(missing)}")

    df["signal_date"] = pd.to_datetime(df["signal_date"], errors="coerce").dt.date
    df["target_shares"] = pd.to_numeric(df["target_shares"], errors="coerce").fillna(0).astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["est_notional"] = pd.to_numeric(df["est_notional"], errors="coerce").fillna(0.0)
    df["ticker"] = df["ticker"].astype(str)
    df["version"] = df["version"].astype(str)
    return df


def _load_prev_positions(con, version: str, prev_date: Optional[date]) -> pd.DataFrame:
    if prev_date is None:
        return pd.DataFrame(columns=["ticker", "shares", "price", "market_value", "cash"])

    rows = con.execute(
        """
        SELECT ticker, shares, price, market_value, cash
        FROM positions_daily
        WHERE asof_date = ? AND version = ?
        """,
        (prev_date, version),
    ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["ticker", "shares", "price", "market_value", "cash"])
    df = pd.DataFrame(rows, columns=["ticker", "shares", "price", "market_value", "cash"])
    df["ticker"] = df["ticker"].astype(str)
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0).astype(int)
    df["cash"] = pd.to_numeric(df["cash"], errors="coerce").fillna(0.0)
    return df


def _find_prev_position_date(con, version: str, signal_date: date) -> Optional[date]:
    row = con.execute(
        """
        SELECT MAX(asof_date)
        FROM positions_daily
        WHERE version = ? AND asof_date < ?
        """,
        (version, signal_date),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return row[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade_date", required=True, help="signal/execution date (YYYY-MM-DD)")
    ap.add_argument("--version", required=True)
    ap.add_argument("--exec_orders_csv", default="", help="default: data/out/exec_orders/exec_orders_<version>_<date>.csv")
    ap.add_argument("--cash", type=float, default=100000.0, help="starting cash if no prev positions")
    ap.add_argument("--out_dir", default="", help="default: data/out/trades")
    args = ap.parse_args()

    signal_date = _parse_date(args.trade_date)
    version = str(args.version).strip()

    root = ROOT
    cfg = load_settings(root)
    store = DuckDBStore(cfg.store_db, root / "src" / "alpha_tracker2" / "storage" / "schema.sql")
    store.init_schema()

    exec_csv = Path(args.exec_orders_csv) if args.exec_orders_csv else (
        root / "data" / "out" / "exec_orders" / f"exec_orders_{version}_{signal_date}.csv"
    )
    if not exec_csv.is_file():
        raise FileNotFoundError(f"exec_orders_csv not found: {exec_csv}")

    target = _load_target_from_exec_orders(exec_csv)
    if target.empty:
        print("[SKIP] empty target exec_orders.")
        return

    # target_map: ticker -> target_shares, price
    target_map = {r["ticker"]: int(r["target_shares"]) for _, r in target.iterrows()}
    price_map = {r["ticker"]: float(r["price"]) for _, r in target.iterrows() if pd.notna(r["price"])}

    out_dir = Path(args.out_dir) if args.out_dir else (root / "data" / "out" / "trades")
    out_dir.mkdir(parents=True, exist_ok=True)

    with store.session() as con:
        _ensure_tables(con)

        prev_date = _find_prev_position_date(con, version, signal_date)
        prev_pos = _load_prev_positions(con, version, prev_date)

        if prev_pos.empty:
            prev_cash = float(args.cash)
            prev_hold = {}
        else:
            # If multiple rows, cash will repeat; take first
            prev_cash = float(prev_pos["cash"].iloc[0]) if "cash" in prev_pos.columns and len(prev_pos) else float(args.cash)
            prev_hold = {r["ticker"]: int(r["shares"]) for _, r in prev_pos.iterrows()}

        # union of tickers
        keys = sorted(set(prev_hold.keys()) | set(target_map.keys()))

        trades = []
        cash = prev_cash

        # SELL first then BUY (simple simulation)
        for tk in keys:
            prev_sh = int(prev_hold.get(tk, 0))
            tgt_sh = int(target_map.get(tk, 0))
            delta = tgt_sh - prev_sh
            px = float(price_map.get(tk, 0.0))

            if delta < 0:
                sh = int(-delta)
                notional = sh * px
                cash += notional
                trades.append(
                    {
                        "trade_date": signal_date,
                        "version": version,
                        "ticker": tk,
                        "side": "SELL",
                        "shares": sh,
                        "price": px,
                        "notional": notional,
                    }
                )

        for tk in keys:
            prev_sh = int(prev_hold.get(tk, 0))
            tgt_sh = int(target_map.get(tk, 0))
            delta = tgt_sh - prev_sh
            px = float(price_map.get(tk, 0.0))

            if delta > 0:
                sh = int(delta)
                notional = sh * px
                if notional > cash + 1e-9:
                    # not enough cash; scale down shares
                    max_sh = int(cash // px) if px > 0 else 0
                    sh = max_sh
                    notional = sh * px
                cash -= notional
                if sh > 0:
                    trades.append(
                        {
                            "trade_date": signal_date,
                            "version": version,
                            "ticker": tk,
                            "side": "BUY",
                            "shares": sh,
                            "price": px,
                            "notional": notional,
                        }
                    )

        trades_df = pd.DataFrame(trades)
        trades_path = out_dir / f"trades_{version}_{signal_date}.csv"
        trades_df.to_csv(trades_path, index=False, encoding="utf-8-sig")

        # compute new positions
        new_hold = prev_hold.copy()
        for t in trades:
            tk = t["ticker"]
            sh = int(t["shares"])
            if t["side"] == "SELL":
                new_hold[tk] = int(new_hold.get(tk, 0) - sh)
            else:
                new_hold[tk] = int(new_hold.get(tk, 0) + sh)
        # remove zeros
        new_hold = {k: v for k, v in new_hold.items() if v > 0}

        # positions snapshot rows
        pos_rows = []
        total_mv = 0.0
        for tk, sh in sorted(new_hold.items()):
            px = float(price_map.get(tk, 0.0))
            mv = sh * px
            total_mv += mv
            pos_rows.append(
                {
                    "asof_date": signal_date,
                    "version": version,
                    "ticker": tk,
                    "shares": int(sh),
                    "price": px,
                    "market_value": mv,
                    "cash": cash,
                    "created_at": datetime.now(),
                }
            )

        pos_df = pd.DataFrame(pos_rows)

        # write trades_daily + positions_daily (replace same day+version)
        con.execute("BEGIN;")
        try:
            con.execute("DELETE FROM trades_daily WHERE trade_date = ? AND version = ?", (signal_date, version))
            con.execute("DELETE FROM positions_daily WHERE asof_date = ? AND version = ?", (signal_date, version))

            if not trades_df.empty:
                trades_df["created_at"] = datetime.now()
                con.register("trades_df", trades_df[[
                    "trade_date", "version", "ticker", "side", "shares", "price", "notional", "created_at"
                ]])
                con.execute("INSERT INTO trades_daily SELECT * FROM trades_df")

            if not pos_df.empty:
                con.register("pos_df", pos_df[[
                    "asof_date", "version", "ticker", "shares", "price", "market_value", "cash", "created_at"
                ]])
                con.execute("INSERT INTO positions_daily SELECT * FROM pos_df")
            else:
                # even if empty holdings, still write a cash-only row? (optional)
                pass

            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise

    print("[OK] execute_rebalance passed.")
    print("trades_csv:", trades_path)
    print("prev_cash:", f"{prev_cash:.2f}", "cash_after:", f"{cash:.2f}", "market_value:", f"{total_mv:.2f}", "total_equity:", f"{(cash+total_mv):.2f}")
    print("n_trades:", len(trades_df), "n_positions:", len(pos_df))


if __name__ == "__main__":
    main()
