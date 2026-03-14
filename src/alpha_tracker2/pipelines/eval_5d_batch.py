"""
E-2: Batch 5-day evaluation over [start, end]. Writes eval_5d_daily, quintile_returns.csv, ic_series.csv.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import yaml

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.evaluation.forward_returns import compute_forward_returns
from alpha_tracker2.evaluation.metrics import ic
from alpha_tracker2.storage.duckdb_store import DuckDBStore

# Reuse same bucket definition as eval_5d
BUCKETS: List[Tuple[str, int | None]] = [
    ("all", None),
    ("top3", 3),
    ("top5", 5),
]

NUM_QUINTILES = 5


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


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


def _load_picks_with_score(
    store: DuckDBStore,
    as_of_date: date,
    version: str,
) -> pd.DataFrame:
    """Load picks_daily with ticker, rank, score for that date and version."""
    rows = store.fetchall(
        """
        SELECT ticker, rank, score
        FROM picks_daily
        WHERE trade_date = ? AND version = ?
        ORDER BY rank ASC NULLS LAST
        """,
        [as_of_date.isoformat(), version],
    )
    if not rows:
        return pd.DataFrame(columns=["ticker", "rank", "score"])
    df = pd.DataFrame(rows, columns=["ticker", "rank", "score"])
    df["ticker"] = df["ticker"].astype(str)
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    return df


def _load_picks_for_version(
    store: DuckDBStore,
    as_of_date: date,
    version: str,
) -> pd.DataFrame:
    """Load picks_daily with ticker, rank only (for bucket aggregation)."""
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


def _tickers_for_bucket(picks_df: pd.DataFrame, rank_max: int | None) -> List[str]:
    if picks_df.empty:
        return []
    if rank_max is None:
        return picks_df["ticker"].tolist()
    sub = picks_df[picks_df["rank"] <= rank_max]
    return sub["ticker"].tolist()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="E-2: Batch eval 5d over [start,end], write eval_5d_daily + quintile + IC CSVs.",
    )
    parser.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--versions",
        type=str,
        default=None,
        help="Comma-separated versions (default from config)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory for quintile_returns.csv and ic_series.csv (default: config out_dir)",
    )
    parser.add_argument("--horizon", type=int, default=5, help="Forward return horizon (default 5)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if end < start:
        raise ValueError("--end must be >= --start")

    project_root = _find_project_root(Path(__file__).resolve())
    settings = load_settings(project_root)
    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()
    cal = TradingCalendar()
    versions = _resolve_versions(args.versions, project_root)
    out_dir = Path(args.output_dir) if args.output_dir else settings.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    trading_days = cal.trading_days(start, end, market="US")
    if not trading_days:
        print("eval_5d_batch: no trading days in range")
        return

    # Idempotent: delete eval_5d_daily in range
    store.exec(
        "DELETE FROM eval_5d_daily WHERE as_of_date BETWEEN ? AND ?",
        [start.isoformat(), end.isoformat()],
    )

    rows_eval: List[tuple] = []
    quintile_rows: List[dict] = []
    ic_rows: List[dict] = []

    for as_of_date in trading_days:
        as_of_str = as_of_date.isoformat()
        for version in versions:
            picks_df = _load_picks_for_version(store, as_of_date, version)
            # Eval buckets -> eval_5d_daily
            for bucket_name, rank_max in BUCKETS:
                tickers = _tickers_for_bucket(picks_df, rank_max)
                if not tickers:
                    rows_eval.append((as_of_str, version, bucket_name, None, 0, args.horizon))
                    continue
                fr_df = compute_forward_returns(store, as_of_date, tickers, horizon=args.horizon)
                if fr_df.empty:
                    fwd_ret_5d = None
                    n_picks = len(tickers)
                else:
                    valid = fr_df["fwd_ret"].notna()
                    n_valid = valid.sum()
                    fwd_ret_5d = float(fr_df.loc[valid, "fwd_ret"].mean()) if n_valid else None
                    n_picks = len(tickers)
                rows_eval.append((as_of_str, version, bucket_name, fwd_ret_5d, n_picks, args.horizon))

            # Quintile returns and IC: need score + fwd_ret per ticker
            picks_with_score = _load_picks_with_score(store, as_of_date, version)
            if picks_with_score.empty or picks_with_score["score"].notna().sum() < 2:
                continue
            tickers_q = picks_with_score["ticker"].tolist()
            fr_q = compute_forward_returns(store, as_of_date, tickers_q, horizon=args.horizon)
            if fr_q.empty or fr_q["fwd_ret"].notna().sum() < 2:
                continue
            merged = picks_with_score.merge(
                fr_q[["ticker", "fwd_ret"]], on="ticker", how="inner"
            ).dropna(subset=["score", "fwd_ret"])
            if len(merged) < 2:
                continue
            # IC (E-1)
            ic_val = ic(merged["score"], merged["fwd_ret"], method="pearson")
            ic_rows.append({"as_of_date": as_of_str, "version": version, "ic": ic_val})
            # Quintiles
            try:
                merged["quintile"] = pd.qcut(
                    merged["score"].rank(method="first"),
                    NUM_QUINTILES,
                    labels=False,
                    duplicates="drop",
                )
            except Exception:
                continue
            for q, g in merged.groupby("quintile"):
                quintile_rows.append({
                    "as_of_date": as_of_str,
                    "version": version,
                    "quintile": int(q) + 1,
                    "mean_fwd_ret_5d": g["fwd_ret"].mean(),
                    "n_stocks": len(g),
                })

    if rows_eval:
        with store.session() as conn:
            conn.executemany(
                """
                INSERT INTO eval_5d_daily (as_of_date, version, bucket, fwd_ret_5d, n_picks, horizon)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows_eval,
            )
        print(f"eval_5d_batch: eval_5d_daily wrote_rows={len(rows_eval)}")

    quintile_path = out_dir / "quintile_returns.csv"
    if quintile_rows:
        pd.DataFrame(quintile_rows).to_csv(quintile_path, index=False)
        print(f"eval_5d_batch: wrote quintile_returns rows={len(quintile_rows)} -> {quintile_path}")
    else:
        pd.DataFrame(columns=["as_of_date", "version", "quintile", "mean_fwd_ret_5d", "n_stocks"]).to_csv(
            quintile_path, index=False
        )
        print(f"eval_5d_batch: wrote quintile_returns rows=0 -> {quintile_path}")

    ic_path = out_dir / "ic_series.csv"
    if ic_rows:
        pd.DataFrame(ic_rows).to_csv(ic_path, index=False)
        print(f"eval_5d_batch: wrote ic_series rows={len(ic_rows)} -> {ic_path}")
    else:
        pd.DataFrame(columns=["as_of_date", "version", "ic"]).to_csv(ic_path, index=False)
        print(f"eval_5d_batch: wrote ic_series rows=0 -> {ic_path}")

    print(f"eval_5d_batch: start={start} end={end} days={len(trading_days)} out_dir={out_dir} db={settings.store_db}")


if __name__ == "__main__":
    main()
