# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


KEY_COLS = [
    "ret_10d",
    "ret_20d",
    "ma_20",
    "ma_60",
    "ma20_slope",
    "vol_ann_60d",
    "mdd_60d",
    "avg_amount_20",
    "limit_down_60",
    "bt_mean",
    "bt_winrate",
    "bt_worst_mdd",
]


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--limit", type=int, default=203)
    ap.add_argument("--universe-table", default="universe_daily_hot")
    ap.add_argument("--show", type=int, default=30, help="show worst N rows")
    return ap.parse_args()


def main():
    args = _parse_args()
    root = _project_root()
    cfg = load_settings(root)
    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    d = pd.to_datetime(args.date).date()

    # 1) hot universe tickers
    uni_rows = store.fetchall(
        f"""
        SELECT ticker
        FROM {args.universe_table}
        WHERE trade_date = ?
        ORDER BY ticker
        LIMIT ?;
        """,
        (d, int(args.limit)),
    )
    uni = [r[0] for r in uni_rows]

    # 2) features rows for those tickers ON THAT DATE ONLY
    placeholders = ",".join(["?"] * len(uni)) if uni else "?"
    rows = store.fetchall(
        f"""
        SELECT ticker, {",".join(KEY_COLS)}
        FROM features_daily
        WHERE trade_date = ?
          AND ticker IN ({placeholders});
        """,
        (d, *uni) if uni else (d, "DUMMY"),
    )

    if not rows:
        print("[FAIL] No features rows matched hot universe tickers on this date.")
        print("db:", cfg.store_db)
        return

    df = pd.DataFrame(rows, columns=["ticker"] + KEY_COLS)

    # 3) coverage
    features_tickers = sorted(df["ticker"].unique().tolist())
    inter = set(features_tickers)
    missing = [t for t in uni if t not in inter]

    print("trade_date:", d)
    print("hot universe tickers:", len(uni))
    print("features tickers in hot:", len(features_tickers))
    print("coverage:", round(len(features_tickers) / max(len(uni), 1), 4))
    if missing:
        print("missing hot tickers:", len(missing))
        print("missing examples:", missing[:20])

    # 4) missing rate per column (ONLY within the matched hot features rows)
    miss_rate = df[KEY_COLS].isna().mean().sort_values(ascending=False)
    print("\nMissing rate (within hot tickers only):")
    for k, v in miss_rate.items():
        print(f"  {k}: {v:.3f}")

    # 5) show worst rows by total missing keys
    df["_missing_keys"] = df[KEY_COLS].isna().sum(axis=1)
    worst = df.sort_values(["_missing_keys", "ticker"], ascending=[False, True]).head(int(args.show))
    print("\nWorst rows by missing keys:")
    print(worst[["ticker", "_missing_keys"] + KEY_COLS].to_string(index=False))

    print("\ndb:", cfg.store_db)


if __name__ == "__main__":
    main()
