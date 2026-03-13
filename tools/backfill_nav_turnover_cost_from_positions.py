# -*- coding: utf-8 -*-
"""
Backfill nav_daily.turnover & nav_daily.cost from positions_daily snapshots.

Logic:
- Use positions_daily.market_value to compute daily turnover per strategy_id:
  w_t(i) = mv_t(i)/sum(mv_t)
  turnover_t = 0.5 * sum_i |w_t(i) - w_{t-1}(i)|
- Update nav_daily.turnover with computed turnover
- Update nav_daily.cost = turnover * cost_bps / 10000
  (uses nav_daily.cost_bps if present; else uses --default_cost_bps)

Usage:
  python tools/backfill_nav_turnover_cost_from_positions.py --db .\data\store\alpha_tracker.duckdb --start 2026-01-06 --end 2026-01-14 --where_model V4
"""

from __future__ import annotations

import argparse
from typing import Dict, Optional

import duckdb
import pandas as pd


def _ensure_columns(con: duckdb.DuckDBPyConnection) -> None:
    # Make sure columns exist (idempotent)
    con.execute("ALTER TABLE nav_daily ADD COLUMN IF NOT EXISTS turnover DOUBLE;")
    con.execute("ALTER TABLE nav_daily ADD COLUMN IF NOT EXISTS cost DOUBLE;")


def _load_positions(con: duckdb.DuckDBPyConnection, start: str, end: str, where_model: Optional[str]) -> pd.DataFrame:
    q = """
    SELECT
      asof_date AS trade_date,
      strategy_id,
      ticker,
      market_value
    FROM positions_daily
    WHERE asof_date BETWEEN ? AND ?
    """
    params = [start, end]
    ifq = q
    if where_model:
        q += " AND strategy_id LIKE ?"
        params.append(f"{where_model}__%")

    df = con.execute(q, params).df()
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["strategy_id"] = df["strategy_id"].astype(str)
    df["ticker"] = df["ticker"].astype(str)
    df["market_value"] = pd.to_numeric(df["market_value"], errors="coerce").fillna(0.0)
    return df


def _load_nav(con: duckdb.DuckDBPyConnection, start: str, end: str, where_model: Optional[str]) -> pd.DataFrame:
    cols = [r[1] for r in con.execute("PRAGMA table_info('nav_daily')").fetchall()]
    has_cost_bps = "cost_bps" in cols

    q = f"""
    SELECT
      trade_date,
      strategy_id
      {", cost_bps" if has_cost_bps else ""}
    FROM nav_daily
    WHERE trade_date BETWEEN ? AND ?
    """
    params = [start, end]
    if where_model:
        q += " AND strategy_id LIKE ?"
        params.append(f"{where_model}__%")

    df = con.execute(q, params).df()
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["strategy_id"] = df["strategy_id"].astype(str)
    if has_cost_bps:
        df["cost_bps"] = pd.to_numeric(df["cost_bps"], errors="coerce")
    else:
        df["cost_bps"] = pd.NA
    return df


def _reconstruct_turnover(pos: pd.DataFrame) -> pd.DataFrame:
    """
    turnover_t = 0.5 * sum_i |w_t(i)-w_{t-1}(i)| based on market_value.
    First day turnover set to 0.0 (baseline).
    """
    rows = []
    for sid, g in pos.groupby("strategy_id"):
        g = g.sort_values("trade_date")
        by_date: Dict[pd.Timestamp, pd.Series] = {}
        for dt, gg in g.groupby("trade_date"):
            s = gg.groupby("ticker")["market_value"].sum()
            by_date[pd.Timestamp(dt)] = s

        dts = sorted(by_date.keys())
        prev_w: Optional[pd.Series] = None
        for dt in dts:
            mv = by_date[dt]
            tot = float(mv.sum())
            w = mv / tot if tot > 0 else mv * 0.0

            if prev_w is None:
                to = 0.0
            else:
                all_idx = prev_w.index.union(w.index)
                w2 = w.reindex(all_idx).fillna(0.0)
                p2 = prev_w.reindex(all_idx).fillna(0.0)
                to = float((w2 - p2).abs().sum() * 0.5)

            rows.append((dt, sid, to))
            prev_w = w

    return pd.DataFrame(rows, columns=["trade_date", "strategy_id", "turnover_calc"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--where_model", default="")
    ap.add_argument("--default_cost_bps", type=float, default=10.0)
    args = ap.parse_args()

    where_model = args.where_model.strip() or None

    con = duckdb.connect(args.db)
    _ensure_columns(con)

    pos = _load_positions(con, args.start, args.end, where_model)
    if pos.empty:
        con.close()
        raise SystemExit("[FATAL] positions_daily empty in given range/filter. Nothing to backfill.")

    nav = _load_nav(con, args.start, args.end, where_model)
    if nav.empty:
        con.close()
        raise SystemExit("[FATAL] nav_daily empty in given range/filter. Run portfolio_nav.py first.")

    turn = _reconstruct_turnover(pos)

    # merge with nav rows to get cost_bps per row, then compute cost
    m = nav.merge(turn, on=["trade_date", "strategy_id"], how="left")
    m["turnover_calc"] = m["turnover_calc"].fillna(0.0)
    m["cost_bps_eff"] = pd.to_numeric(m["cost_bps"], errors="coerce")
    m["cost_bps_eff"] = m["cost_bps_eff"].fillna(args.default_cost_bps)
    m["cost_calc"] = m["turnover_calc"] * m["cost_bps_eff"] / 10000.0

    # register df and update nav_daily
    upd = m[["trade_date", "strategy_id", "turnover_calc", "cost_calc"]].copy()
    con.register("upd_turn", upd)

    con.execute("""
      UPDATE nav_daily AS n
      SET
        turnover = u.turnover_calc,
        cost = u.cost_calc
      FROM upd_turn AS u
      WHERE n.trade_date = u.trade_date
        AND n.strategy_id = u.strategy_id
    """)

    # sanity print
    nrows = con.execute("""
      SELECT COUNT(*) FROM nav_daily
      WHERE trade_date BETWEEN ? AND ?
    """, [args.start, args.end]).fetchone()[0]

    con.close()

    print(f"[OK] backfilled turnover/cost for range {args.start}..{args.end} (rows in nav_daily range={nrows})")
    print("[OK] Next: rerun diagnose_turnover_from_positions.py and export_strategy_leaderboard.py")


if __name__ == "__main__":
    main()
