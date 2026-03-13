from __future__ import annotations

import argparse
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore

ROOT = Path(__file__).resolve().parents[3]


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_versions(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _load_picks(store: DuckDBStore, trade_date: date, version: str, topk: int) -> pd.DataFrame:
    rows = store.fetchall(
        """
        SELECT
          trade_date, version, ticker, name,
          score, rank, reason, score_100,
          thr_value, pass_thr, picked_by
        FROM picks_daily
        WHERE trade_date = ?
          AND version = ?
        ORDER BY
          CASE WHEN rank IS NULL THEN 1 ELSE 0 END ASC,
          rank ASC,
          score DESC NULLS LAST
        LIMIT ?
        """,
        (trade_date, version, int(topk)),
    )
    cols = [
        "trade_date", "version", "ticker", "name",
        "score", "rank", "reason", "score_100",
        "thr_value", "pass_thr", "picked_by",
    ]
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows, columns=cols)
    df["ticker"] = df["ticker"].astype(str)
    df["version"] = df["version"].astype(str)
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["score_100"] = pd.to_numeric(df["score_100"], errors="coerce")
    return df


def _load_latest_nav_meta(store: DuckDBStore, start: date, end: date, version: str) -> Dict[str, float]:
    """
    Pull last nav row in [start, end] for version to include as context.
    Not required; if missing return empty dict.
    """
    rows = store.fetchall(
        """
        SELECT trade_date, nav, nav_gross, turnover, cost_bps
        FROM nav_daily
        WHERE trade_date BETWEEN ? AND ?
          AND version = ?
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        (start, end, version),
    )
    if not rows:
        return {}
    td, nav, nav_gross, turnover, cost_bps = rows[0]
    out = {
        "nav_last": float(nav) if nav is not None else None,
        "nav_gross_last": float(nav_gross) if nav_gross is not None else None,
        "turnover_last": float(turnover) if turnover is not None else None,
        "cost_bps": float(cost_bps) if cost_bps is not None else None,
    }
    return {k: v for k, v in out.items() if v is not None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade_date", required=True, help="signal day, same as picks_daily.trade_date (YYYY-MM-DD)")
    ap.add_argument("--versions", required=True, help="comma separated versions, e.g. ENS or V1,V2")
    ap.add_argument("--topk", type=int, default=3)
    ap.add_argument("--out_dir", default="", help="default: data/out/signals")
    ap.add_argument("--with_nav_meta", action="store_true", help="append last nav meta columns if available")
    ap.add_argument("--nav_start", default="", help="when with_nav_meta: nav range start YYYY-MM-DD")
    ap.add_argument("--nav_end", default="", help="when with_nav_meta: nav range end YYYY-MM-DD")
    args = ap.parse_args()

    trade_date = _parse_date(args.trade_date)
    versions = _parse_versions(args.versions)
    topk = int(args.topk)

    root = ROOT
    cfg = load_settings(root)
    store = DuckDBStore(cfg.store_db, root / "src" / "alpha_tracker2" / "storage" / "schema.sql")
    store.init_schema()

    out_dir = Path(args.out_dir) if args.out_dir else (root / "data" / "out" / "signals")
    out_dir.mkdir(parents=True, exist_ok=True)

    for v in versions:
        df = _load_picks(store, trade_date, v, topk=topk)
        if df.empty:
            print(f"[SKIP] {v} trade_date={trade_date} picks_rows=0")
            continue

        n = len(df)
        df_out = pd.DataFrame(
            {
                "signal_date": trade_date,
                "version": v,
                "ticker": df["ticker"].astype(str),
                "name": df["name"],
                "weight": [1.0 / n] * n,  # equal weight
                "rank": df["rank"],
                "score": df["score"],
                "picked_by": df["picked_by"],
                "reason": df["reason"],
            }
        )

        if args.with_nav_meta:
            if not args.nav_start or not args.nav_end:
                raise ValueError("--with_nav_meta requires --nav_start and --nav_end")
            nav_meta = _load_latest_nav_meta(store, _parse_date(args.nav_start), _parse_date(args.nav_end), v)
            for k, val in nav_meta.items():
                df_out[k] = val

        out_path = out_dir / f"signal_{v}_{trade_date}_top{topk}.csv"
        df_out.to_csv(out_path, index=False, encoding="utf-8-sig")

        print(f"[OK] signal exported: {out_path} rows={len(df_out)}")

    print("[OK] export_signals passed.")


if __name__ == "__main__":
    main()
