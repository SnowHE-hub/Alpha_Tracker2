"""
Evaluate picks by forward N-day return and write results to eval_5d_daily.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import List, Tuple

import yaml

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.evaluation.forward_returns import compute_forward_returns
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def _resolve_as_of_date(arg_date: str | None, cal: TradingCalendar) -> date:
    if arg_date:
        return date.fromisoformat(arg_date)
    return cal.latest_trading_day("US")


def _resolve_versions(arg_versions: str | None, project_root: Path) -> List[str]:
    if arg_versions:
        return [v.strip().upper() for v in arg_versions.split(",") if v.strip()]
    cfg_path = project_root / "configs" / "default.yaml"
    if cfg_path.is_file():
        with cfg_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        scoring_cfg = raw.get("scoring") or {}
        versions = scoring_cfg.get("score_versions")
        if isinstance(versions, list) and versions:
            return [str(v).upper() for v in versions]
    return ["V1", "V2", "V3", "V4"]


# Buckets: (bucket_name, rank_max inclusive; None = all)
BUCKETS: List[Tuple[str, int | None]] = [
    ("all", None),
    ("top3", 3),
    ("top5", 5),
]


def _load_picks_for_version(
    store: DuckDBStore,
    as_of_date: date,
    version: str,
) -> pd.DataFrame:
    """Load picks_daily for given date and version with ticker and rank. Sorted by rank."""
    rows = store.fetchall(
        """
        SELECT ticker, rank
        FROM picks_daily
        WHERE trade_date = ? AND version = ?
        ORDER BY rank ASC NULLS LAST
        """,
        [as_of_date.isoformat(), version],
    )
    if not rows:
        return pd.DataFrame(columns=["ticker", "rank"])
    df = pd.DataFrame(rows, columns=["ticker", "rank"])
    df["ticker"] = df["ticker"].astype(str)
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    return df


def _tickers_for_bucket(picks_df: pd.DataFrame, bucket: str, rank_max: int | None) -> List[str]:
    if picks_df.empty:
        return []
    if rank_max is None:
        return picks_df["ticker"].tolist()
    sub = picks_df[picks_df["rank"] <= rank_max]
    return sub["ticker"].tolist()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate picks by forward N-day return and write eval_5d_daily.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="As-of date YYYY-MM-DD; default latest US trading day",
    )
    parser.add_argument(
        "--versions",
        type=str,
        default=None,
        help="Comma-separated versions e.g. V1,V2,V3,V4; default from config",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=5,
        help="Forward return horizon in trading days (default 5)",
    )
    args = parser.parse_args()

    project_root = _find_project_root(Path(__file__).resolve())
    settings = load_settings(project_root)
    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()
    cal = TradingCalendar()

    as_of_date = _resolve_as_of_date(args.date, cal)
    versions = _resolve_versions(args.versions, project_root)

    # Idempotent: delete existing rows for this as_of_date
    store.exec(
        "DELETE FROM eval_5d_daily WHERE as_of_date = ?",
        [as_of_date.isoformat()],
    )

    as_of_str = as_of_date.isoformat()
    rows_to_insert: List[tuple] = []

    for version in versions:
        picks_df = _load_picks_for_version(store, as_of_date, version)
        if picks_df.empty:
            for bucket_name, _ in BUCKETS:
                rows_to_insert.append((as_of_str, version, bucket_name, None, 0, args.horizon))
            print(f"eval_5d: as_of_date={as_of_date} version={version} no picks, writing placeholder rows")
            continue

        for bucket_name, rank_max in BUCKETS:
            tickers = _tickers_for_bucket(picks_df, bucket_name, rank_max)
            if not tickers:
                rows_to_insert.append((as_of_str, version, bucket_name, None, 0, args.horizon))
                continue

            fr_df = compute_forward_returns(store, as_of_date, tickers, horizon=args.horizon)
            if fr_df.empty:
                fwd_ret_5d = None
                n_picks = len(tickers)
            else:
                valid = fr_df["fwd_ret"].notna()
                n_valid = valid.sum()
                if n_valid == 0:
                    fwd_ret_5d = None
                else:
                    fwd_ret_5d = float(fr_df.loc[valid, "fwd_ret"].mean())
                n_picks = len(tickers)

            rows_to_insert.append((as_of_str, version, bucket_name, fwd_ret_5d, n_picks, args.horizon))
            print(f"eval_5d: as_of_date={as_of_date} version={version} bucket={bucket_name} fwd_ret_5d={fwd_ret_5d} n_picks={n_picks}")

    if rows_to_insert:
        with store.session() as conn:
            conn.executemany(
                """
                INSERT INTO eval_5d_daily (as_of_date, version, bucket, fwd_ret_5d, n_picks, horizon)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows_to_insert,
            )
        print(f"eval_5d: as_of_date={as_of_date} wrote_rows={len(rows_to_insert)}")
    else:
        print("eval_5d: no rows to write")

    print(f"eval_5d: db={settings.store_db}")


if __name__ == "__main__":
    main()
