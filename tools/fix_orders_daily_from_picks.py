from __future__ import annotations

import argparse
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd

from alpha_tracker2.core.config import load_settings

ROOT = Path(__file__).resolve().parents[1]  # D:\alpha_tracker2


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _load_picks(con: duckdb.DuckDBPyConnection, signal_date: date, version: str, topk: int) -> pd.DataFrame:
    rows = con.execute(
        """
        SELECT trade_date, version, ticker, COALESCE(name, '') AS name, rank, score
        FROM picks_daily
        WHERE trade_date = ? AND version = ?
        ORDER BY
          CASE WHEN rank IS NULL THEN 1 ELSE 0 END ASC,
          rank ASC,
          score DESC NULLS LAST
        LIMIT ?
        """,
        [signal_date, version, int(topk)],
    ).fetchall()

    cols = ["trade_date", "version", "ticker", "name", "rank", "score"]
    if not rows:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows, columns=cols)
    df["ticker"] = df["ticker"].astype(str)
    df["version"] = df["version"].astype(str)
    df["name"] = df["name"].fillna("").astype(str)
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def _find_prev_signal_date(con: duckdb.DuckDBPyConnection, signal_date: date, version: str) -> Optional[date]:
    row = con.execute(
        """
        SELECT MAX(trade_date) AS prev_date
        FROM picks_daily
        WHERE trade_date < ? AND version = ?
        """,
        [signal_date, version],
    ).fetchone()
    if not row or row[0] is None:
        return None
    # duckdb may return datetime/date; normalize
    return pd.to_datetime(row[0]).date()


def _equal_weight_map(tickers: List[str]) -> Dict[str, float]:
    n = len(tickers)
    if n <= 0:
        return {}
    w = 1.0 / n
    return {t: w for t in tickers}


def _build_orders_df(
    signal_date: date,
    version: str,
    prev_date: Optional[date],
    prev_targets: Dict[str, float],
    cur_targets: Dict[str, float],
    name_map: Dict[str, str],
) -> pd.DataFrame:
    keys = sorted(set(prev_targets.keys()) | set(cur_targets.keys()))
    rows = []

    for tk in keys:
        prev_w = float(prev_targets.get(tk, 0.0))
        cur_w = float(cur_targets.get(tk, 0.0))
        delta = cur_w - prev_w

        if prev_w == 0.0 and cur_w > 0.0:
            action = "BUY"
        elif prev_w > 0.0 and cur_w == 0.0:
            action = "SELL"
        else:
            action = "HOLD" if abs(delta) < 1e-12 else ("BUY" if delta > 0 else "SELL")

        rows.append(
            {
                "signal_date": pd.to_datetime(signal_date),
                "version": version,
                "prev_signal_date": pd.to_datetime(prev_date) if prev_date is not None else None,
                "ticker": tk,
                "name": name_map.get(tk, ""),
                "action": action,
                "prev_weight": prev_w,
                "target_weight": cur_w,
                "delta_weight": delta,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["abs_delta"] = out["delta_weight"].abs()
    out = out.sort_values(["action", "abs_delta"], ascending=[True, False]).drop(columns=["abs_delta"])
    return out


def _detect_orders_daily_columns(con: duckdb.DuckDBPyConnection) -> List[str]:
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    info = con.execute("PRAGMA table_info('orders_daily')").fetchall()
    return [r[1] for r in info]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="signal date, YYYY-MM-DD")
    ap.add_argument("--version", required=True, help="e.g. ENS")
    ap.add_argument("--topk", type=int, default=3)
    ap.add_argument("--purge", action="store_true", help="delete existing orders_daily rows for that date/version")
    ap.add_argument("--no_purge", action="store_true", help="do NOT delete existing rows")
    ap.add_argument("--dry_run", action="store_true", help="only print planned orders, do not write DB")
    args = ap.parse_args()

    signal_date = _parse_date(args.date)
    version = str(args.version).strip()
    topk = int(args.topk)
    do_purge = True if args.purge else False
    if args.no_purge:
        do_purge = False

    cfg = load_settings(ROOT)
    db_path = str(cfg.store_db)
    print("[DB]", db_path)
    con = duckdb.connect(db_path)

    cur_df = _load_picks(con, signal_date, version, topk)
    if cur_df.empty:
        raise RuntimeError(f"[ERROR] no picks_daily rows for version={version} date={signal_date}")

    name_map = {r["ticker"]: r["name"] for _, r in cur_df.iterrows()}

    prev_date = _find_prev_signal_date(con, signal_date, version)
    prev_targets: Dict[str, float] = {}
    if prev_date is not None:
        prev_df = _load_picks(con, prev_date, version, topk)
        if not prev_df.empty:
            for _, r in prev_df.iterrows():
                if r["ticker"] not in name_map and str(r["name"]):
                    name_map[str(r["ticker"])] = str(r["name"])
            prev_targets = _equal_weight_map(prev_df["ticker"].astype(str).tolist())

    cur_targets = _equal_weight_map(cur_df["ticker"].astype(str).tolist())
    orders = _build_orders_df(signal_date, version, prev_date, prev_targets, cur_targets, name_map)

    print("\n=== planned orders (from picks_daily) ===")
    print(orders.to_string(index=False))

    if args.dry_run:
        print("\n[DRY_RUN] not writing to DB.")
        return

    # Write to orders_daily with best-effort column matching
    cols = _detect_orders_daily_columns(con)
    print("\n[orders_daily columns]", cols)

    if do_purge:
        con.execute(
            "DELETE FROM orders_daily WHERE signal_date = ? AND version = ?",
            [signal_date, version],
        )
        print(f"[OK] purged orders_daily where signal_date={signal_date} version={version}")

    # Determine insert columns that exist
    base_cols = [
        "signal_date", "version", "prev_signal_date", "ticker", "name", "action",
        "prev_weight", "target_weight", "delta_weight"
    ]
    insert_cols = [c for c in base_cols if c in cols]

    if not insert_cols:
        raise RuntimeError("[ERROR] orders_daily has no compatible columns to insert.")

    # Build rows list in insert_cols order
    values = []
    for _, r in orders.iterrows():
        row = []
        for c in insert_cols:
            v = r.get(c, None)
            # DuckDB likes python date for DATE type
            if c in ("signal_date", "prev_signal_date") and pd.notna(v) and v is not None:
                v = pd.to_datetime(v).date()
            if pd.isna(v):
                v = None
            row.append(v)
        values.append(tuple(row))

    placeholders = ", ".join(["?"] * len(insert_cols))
    col_sql = ", ".join(insert_cols)

    con.executemany(
        f"INSERT INTO orders_daily ({col_sql}) VALUES ({placeholders})",
        values,
    )
    print(f"[OK] inserted {len(values)} rows into orders_daily.")

    # Show what is in DB now
    df_db = con.execute(
        """
        SELECT *
        FROM orders_daily
        WHERE signal_date = ? AND version = ?
        ORDER BY ticker
        """,
        [signal_date, version],
    ).df()

    print("\n=== orders_daily after fix ===")
    print(df_db.to_string(index=False))


if __name__ == "__main__":
    main()
