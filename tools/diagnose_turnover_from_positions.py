# -*- coding: utf-8 -*-
"""
Diagnose whether turnover/cost are computed/recorded correctly by comparing:
- recorded nav_daily.turnover / nav_daily.cost
vs
- turnover reconstructed from positions snapshots (using market_value)

Works with your schema v2:
positions_daily(asof_date, strategy_id, ticker, market_value, ...)
nav_daily(trade_date, strategy_id, turnover, cost, nav, ...)

Usage:
  python tools/diagnose_turnover_from_positions.py --db .\data\store\alpha_tracker.duckdb --start 2026-01-06 --end 2026-01-14 --where_model V4
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict

import duckdb
import pandas as pd


CAND_POS_TABLES = ["positions_daily"]
CAND_NAV_TABLES = ["nav_daily"]


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        r = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
            [name],
        ).fetchone()
        return r is not None
    except Exception:
        return False


def _pick_first_existing(con: duckdb.DuckDBPyConnection, candidates: List[str]) -> Optional[str]:
    for t in candidates:
        if _table_exists(con, t):
            return t
    return None


def _cols(con: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    df = con.execute(f"PRAGMA table_info('{table}')").df()
    return df["name"].tolist()


def _find_col(cols: List[str], preferred: List[str]) -> Optional[str]:
    s = set(cols)
    for c in preferred:
        if c in s:
            return c
    return None


@dataclass
class PosSchema:
    table: str
    date_col: str
    strategy_id_col: str
    ticker_col: str
    mv_col: str


@dataclass
class NavSchema:
    table: str
    date_col: str
    strategy_id_col: str
    nav_col: str
    turnover_col: Optional[str]
    cost_col: Optional[str]


def _detect_pos_schema(con: duckdb.DuckDBPyConnection, table: str) -> PosSchema:
    cols = _cols(con, table)

    # IMPORTANT: your table uses asof_date
    date_col = _find_col(cols, ["asof_date", "trade_date", "date", "dt"])
    strategy_id_col = _find_col(cols, ["strategy_id", "sid"])
    ticker_col = _find_col(cols, ["ticker", "code", "symbol"])
    mv_col = _find_col(cols, ["market_value", "mv", "value", "position_value"])

    if date_col is None or strategy_id_col is None or ticker_col is None or mv_col is None:
        raise RuntimeError(
            f"[FATAL] Cannot detect required columns in {table}. cols={cols}\n"
            f"need: date(asof_date/trade_date), strategy_id, ticker, market_value"
        )

    return PosSchema(table=table, date_col=date_col, strategy_id_col=strategy_id_col, ticker_col=ticker_col, mv_col=mv_col)


def _detect_nav_schema(con: duckdb.DuckDBPyConnection, table: str) -> NavSchema:
    cols = _cols(con, table)
    date_col = _find_col(cols, ["trade_date", "asof_date", "date", "dt"])
    strategy_id_col = _find_col(cols, ["strategy_id", "sid"])
    nav_col = _find_col(cols, ["nav", "nav_gross", "cum_nav", "nav_value"])
    turnover_col = _find_col(cols, ["turnover", "turnover_daily"])
    cost_col = _find_col(cols, ["cost", "cost_daily", "fee", "fees"])

    if date_col is None or strategy_id_col is None or nav_col is None:
        raise RuntimeError(f"[FATAL] Cannot detect required columns in {table}. cols={cols}")

    return NavSchema(
        table=table,
        date_col=date_col,
        strategy_id_col=strategy_id_col,
        nav_col=nav_col,
        turnover_col=turnover_col,
        cost_col=cost_col,
    )


def _load_positions(con: duckdb.DuckDBPyConnection, sch: PosSchema, start: str, end: str, where_model: Optional[str]) -> pd.DataFrame:
    q = f"""
    SELECT
      {sch.date_col} AS trade_date,
      {sch.strategy_id_col} AS strategy_id,
      {sch.ticker_col} AS ticker,
      {sch.mv_col} AS market_value
    FROM {sch.table}
    WHERE {sch.date_col} BETWEEN ? AND ?
    """
    params = [start, end]
    if where_model:
        q += f" AND {sch.strategy_id_col} LIKE ?"
        params.append(f"{where_model}__%")

    df = con.execute(q, params).df()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["strategy_id"] = df["strategy_id"].astype(str)
    df["ticker"] = df["ticker"].astype(str)
    df["market_value"] = pd.to_numeric(df["market_value"], errors="coerce").fillna(0.0)
    return df


def _load_nav(con: duckdb.DuckDBPyConnection, sch: NavSchema, start: str, end: str, where_model: Optional[str]) -> pd.DataFrame:
    extra = ""
    if sch.turnover_col:
        extra += f", {sch.turnover_col} AS turnover"
    if sch.cost_col:
        extra += f", {sch.cost_col} AS cost"

    q = f"""
    SELECT
      {sch.date_col} AS trade_date,
      {sch.strategy_id_col} AS strategy_id,
      {sch.nav_col} AS nav
      {extra}
    FROM {sch.table}
    WHERE {sch.date_col} BETWEEN ? AND ?
    """
    params = [start, end]
    if where_model:
        q += f" AND {sch.strategy_id_col} LIKE ?"
        params.append(f"{where_model}__%")

    df = con.execute(q, params).df()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["strategy_id"] = df["strategy_id"].astype(str)
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    if "turnover" in df.columns:
        df["turnover"] = pd.to_numeric(df["turnover"], errors="coerce").fillna(0.0)
    if "cost" in df.columns:
        df["cost"] = pd.to_numeric(df["cost"], errors="coerce").fillna(0.0)
    return df


def _reconstruct_turnover_from_positions(pos: pd.DataFrame) -> pd.DataFrame:
    """
    Use weights from market_value:
      w_t(i) = mv_t(i) / sum_i mv_t(i)
      turnover_t = 0.5 * sum_i |w_t(i) - w_{t-1}(i)|
    """
    if pos.empty:
        return pd.DataFrame(columns=["trade_date", "strategy_id", "turnover_recon"])

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
                # first day "entry" turnover is not very meaningful; show 0 for baseline
                to = 0.0
            else:
                all_idx = prev_w.index.union(w.index)
                w2 = w.reindex(all_idx).fillna(0.0)
                p2 = prev_w.reindex(all_idx).fillna(0.0)
                to = float((w2 - p2).abs().sum() * 0.5)

            rows.append((dt, sid, to))
            prev_w = w

    return pd.DataFrame(rows, columns=["trade_date", "strategy_id", "turnover_recon"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--where_model", default="")
    args = ap.parse_args()

    con = duckdb.connect(args.db)

    pos_t = _pick_first_existing(con, CAND_POS_TABLES)
    nav_t = _pick_first_existing(con, CAND_NAV_TABLES)
    if not pos_t:
        raise RuntimeError(f"[FATAL] positions table not found. tried={CAND_POS_TABLES}")
    if not nav_t:
        raise RuntimeError(f"[FATAL] nav table not found. tried={CAND_NAV_TABLES}")

    pos_s = _detect_pos_schema(con, pos_t)
    nav_s = _detect_nav_schema(con, nav_t)

    where_model = args.where_model.strip() or None

    pos = _load_positions(con, pos_s, args.start, args.end, where_model)
    nav = _load_nav(con, nav_s, args.start, args.end, where_model)

    con.close()

    print(f"[OK] positions table={pos_s.table} date={pos_s.date_col} mv={pos_s.mv_col} rows={len(pos)}")
    print(f"[OK] nav table={nav_s.table} date={nav_s.date_col} turnover={nav_s.turnover_col} cost={nav_s.cost_col} rows={len(nav)}")

    recon = _reconstruct_turnover_from_positions(pos)
    merged = recon.merge(nav, on=["trade_date", "strategy_id"], how="left")

    rows = []
    for sid, g in merged.groupby("strategy_id"):
        g = g.sort_values("trade_date")
        to_recon_sum = float(g["turnover_recon"].sum())
        to_rec_sum = float(g["turnover"].sum()) if "turnover" in g.columns else float("nan")
        cost_sum = float(g["cost"].sum()) if "cost" in g.columns else float("nan")
        end_nav = float(g["nav"].iloc[-1]) if g["nav"].notna().any() else float("nan")
        rows.append((sid, to_recon_sum, to_rec_sum, cost_sum, end_nav))

    out = pd.DataFrame(
        rows,
        columns=["strategy_id", "turnover_recon_sum", "turnover_recorded_sum", "cost_recorded_sum", "end_nav"],
    ).sort_values("turnover_recon_sum", ascending=False)

    print("\n=== turnover diagnostics (per strategy) ===")
    print(out.to_string(index=False))

    # show one example daily detail for the first non-legacy if possible
    pick = None
    for sid in out["strategy_id"].tolist():
        if "__LEGACY__" not in sid:
            pick = sid
            break
    if pick:
        g = merged[merged["strategy_id"] == pick].sort_values("trade_date")
        cols = ["trade_date", "strategy_id", "turnover_recon", "nav"]
        if "turnover" in g.columns:
            cols.append("turnover")
        if "cost" in g.columns:
            cols.append("cost")
        print(f"\n=== daily detail sample: {pick} ===")
        print(g[cols].head(30).to_string(index=False))


if __name__ == "__main__":
    main()
