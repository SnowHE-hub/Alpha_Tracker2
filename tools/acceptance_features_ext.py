# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path
from datetime import date

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


def _project_root_from_here() -> Path:
    # tools/acceptance_features_ext.py -> parents:
    # 0 tools, 1 project_root
    return Path(__file__).resolve().parents[1]


def _parse_args():
    ap = argparse.ArgumentParser(description="Acceptance test for extended features_daily.")
    ap.add_argument("--date", required=True, type=str, help="trade_date YYYY-MM-DD")
    ap.add_argument("--max-missing-rate", type=float, default=0.05, help="max missing rate allowed for key cols")
    ap.add_argument(
        "--min-coverage",
        type=float,
        default=0.98,
        help="min coverage required: features_tickers / universe_tickers (default=0.98 for legacy)",
    )
    ap.add_argument(
        "--universe-source",
        required=True,
        choices=["hot", "universe_picks"],
        help="hot => universe_daily_hot; universe_picks => picks_daily version=UNIVERSE",
    )
    ap.add_argument("--limit", type=int, default=300)
    return ap.parse_args()


def _load_universe_tickers(store: DuckDBStore, d: date, universe_source: str, limit: int) -> list[str]:
    if universe_source == "hot":
        # universe_daily_hot (your hot industry universe)
        rows = store.fetchall(
            """
            SELECT ticker
            FROM universe_daily_hot
            WHERE trade_date = ?
            ORDER BY hot_industry_rank, stock_rank_in_industry, ticker
            LIMIT ?;
            """,
            (d, int(limit)),
        )
        return [r[0] for r in rows]

    # universe_source == "universe_picks"
    rows = store.fetchall(
        """
        SELECT ticker
        FROM picks_daily
        WHERE trade_date = ?
          AND version = 'UNIVERSE'
        ORDER BY rank
        LIMIT ?;
        """,
        (d, int(limit)),
    )
    return [r[0] for r in rows]


def _load_features(store: DuckDBStore, d: date, tickers: list[str]) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame(columns=["ticker"] + KEY_COLS)

    placeholders = ",".join(["?"] * len(tickers))
    rows = store.fetchall(
        f"""
        SELECT
            ticker,
            {", ".join(KEY_COLS)}
        FROM features_daily
        WHERE trade_date = ?
          AND ticker IN ({placeholders})
        ORDER BY ticker;
        """,
        (d, *tickers),
    )
    cols = ["ticker"] + KEY_COLS
    return pd.DataFrame(rows, columns=cols)


def main():
    args = _parse_args()
    root = _project_root_from_here()
    cfg = load_settings(root)

    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    d = pd.to_datetime(args.date).date()

    uni_tickers = _load_universe_tickers(store, d, args.universe_source, args.limit)
    feat = _load_features(store, d, uni_tickers)

    universe_n = len(uni_tickers)
    features_n = int(feat["ticker"].nunique()) if not feat.empty else 0
    coverage = (features_n / universe_n) if universe_n > 0 else 0.0

    print("\n=== Acceptance: features_ext ===")
    print("trade_date:", d)
    print("universe_tickers:", universe_n)
    print("features_tickers:", features_n)
    print("coverage:", round(coverage, 4))

    # missing rates
    miss = {}
    if not feat.empty:
        for c in KEY_COLS:
            miss[c] = float(feat[c].isna().mean())
    else:
        for c in KEY_COLS:
            miss[c] = 1.0

    print("\nMissing rate (key cols):")
    for c in KEY_COLS:
        print(f"  {c}: {miss[c]:.3f}")

    fail_cols = [c for c, r in miss.items() if r > float(args.max_missing_rate)]

    ok = True
    if coverage < float(args.min_coverage):
        ok = False
        missing_tickers = sorted(set(uni_tickers) - set(feat["ticker"].tolist()))
        print("\n[FAIL]")
        print(
            f"Coverage < {args.min_coverage:.2f}; example missing tickers:",
            missing_tickers[:20],
        )
        # don't early return; still report miss cols

    if fail_cols:
        ok = False
        print("\n[FAIL]")
        print("Columns over max missing rate:", fail_cols)
        if not feat.empty:
            show = feat[["ticker"] + fail_cols].head(20)
            print("\nExample rows with missing key cols (head):")
            print(show.to_string(index=False))

    if ok:
        print("\n[OK] Step2.0 acceptance passed.")
        print("db:", cfg.store_db)
    else:
        print("\n[FAIL]")

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
