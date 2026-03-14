"""
Ensemble pipeline: aggregate V1–V4 (or config-specified versions) picks into picks_daily(version='ENS').

Reads picks_daily for the given date and input versions, computes an aggregate score (e.g. mean score_100),
ranks tickers, and writes picks_daily with version='ENS'. Idempotent: DELETE then INSERT for that date + ENS.

CLI:
  --date YYYY-MM-DD (default: latest US trading day)
  --versions V1,V2,V3,V4 (optional; default from configs/default.yaml scoring.ensemble.input_versions)
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import List

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def _resolve_trade_date(arg_date: str | None, cal: TradingCalendar) -> date:
    if arg_date:
        return date.fromisoformat(arg_date)
    return cal.latest_trading_day("US")


def _load_ensemble_versions(project_root: Path) -> List[str]:
    """Load input_versions from scoring.ensemble.input_versions; default ['V1','V2','V3','V4']."""
    import yaml

    cfg_path = project_root / "configs" / "default.yaml"
    default = ["V1", "V2", "V3", "V4"]
    if not cfg_path.is_file():
        return default
    with cfg_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    ens = (raw.get("scoring") or {}).get("ensemble") or {}
    if not isinstance(ens, dict):
        return default
    v = ens.get("input_versions")
    if isinstance(v, list) and v:
        return [str(x).upper() for x in v]
    return default


def _resolve_versions(arg_versions: str | None, project_root: Path) -> List[str]:
    if arg_versions:
        return [x.strip().upper() for x in arg_versions.split(",") if x.strip()]
    return _load_ensemble_versions(project_root)


def run(
    project_root: Path,
    trade_date: date,
    versions: List[str] | None = None,
    store: DuckDBStore | None = None,
) -> None:
    """
    Run score_ensemble for the given date and input versions. Optional store for testing.
    When store is None, create from settings(project_root).
    """
    if store is None:
        settings = load_settings(project_root)
        store = DuckDBStore(
            db_path=settings.store_db,
            schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
        )
        store.init_schema()
    versions_list = _resolve_versions(None, project_root) if versions is None else versions
    trade_date_str = trade_date.isoformat()

    # Load picks for target date and input versions
    placeholders = ",".join(["?"] * len(versions_list))
    params: list[object] = [trade_date_str, *versions_list]
    rows = store.fetchall(
        f"""
        SELECT trade_date, version, ticker, name, rank, score, score_100, reason
        FROM picks_daily
        WHERE trade_date = ? AND version IN ({placeholders})
        ORDER BY version, ticker
        """,
        params,
    )
    if not rows:
        print(f"score_ensemble: trade_date={trade_date} no picks for versions {versions_list}; nothing to aggregate")
        return

    df = pd.DataFrame(
        rows,
        columns=["trade_date", "version", "ticker", "name", "rank", "score", "score_100", "reason"],
    )
    df["score_100"] = pd.to_numeric(df["score_100"], errors="coerce")

    # Aggregate: mean of score_100 per ticker (only versions that have that ticker)
    agg = df.groupby("ticker").agg(
        score_100_mean=("score_100", "mean"),
        n_versions=("version", "nunique"),
        name=("name", "first"),
    ).reset_index()
    # Per-version contribution for reason (score_100 by version)
    contrib = df.pivot_table(index="ticker", columns="version", values="score_100", aggfunc="first")

    agg = agg.sort_values("score_100_mean", ascending=False).reset_index(drop=True)
    agg["rank"] = agg.index + 1
    agg["score"] = agg["score_100_mean"]
    agg["score_100"] = agg["score_100_mean"]
    if agg["score_100"].max() != agg["score_100"].min():
        # Normalize to 0–100 for display
        min_s, max_s = agg["score_100"].min(), agg["score_100"].max()
        agg["score_100"] = (agg["score_100"] - min_s) / (max_s - min_s) * 100.0
    else:
        agg["score_100"] = 50.0

    rows_out: list[tuple] = []
    for _, row in agg.iterrows():
        ticker = str(row["ticker"])
        reason_payload = {
            "method": "mean_score_100",
            "input_versions": versions_list,
            "score_100_mean": float(row["score_100_mean"]),
            "n_versions": int(row["n_versions"]),
        }
        if ticker in contrib.index:
            reason_payload["by_version"] = contrib.loc[ticker].dropna().to_dict()
        reason_str = json.dumps(reason_payload, ensure_ascii=False)
        rows_out.append(
            (
                trade_date_str,
                "ENS",
                ticker,
                row["name"] if pd.notna(row["name"]) else None,
                int(row["rank"]),
                float(row["score"]),
                float(row["score_100"]),
                reason_str,
                None,  # thr_value
                None,  # pass_thr
                "ENS_VOTE",  # picked_by
            )
        )

    # Idempotent write: DELETE then INSERT
    store.exec(
        "DELETE FROM picks_daily WHERE trade_date = ? AND version = 'ENS'",
        [trade_date_str],
    )
    if not rows_out:
        print("score_ensemble: no rows to insert")
        return

    with store.session() as conn:
        conn.executemany(
            """
            INSERT INTO picks_daily (
                trade_date, version, ticker, name, rank, score, score_100,
                reason, thr_value, pass_thr, picked_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_out,
        )

    print(f"score_ensemble: trade_date={trade_date} version=ENS wrote_rows={len(rows_out)} input_versions={versions_list}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate V1–V4 picks into picks_daily(version='ENS').",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target trade date YYYY-MM-DD; default latest US trading day",
    )
    parser.add_argument(
        "--versions",
        type=str,
        default=None,
        help="Comma-separated input versions (e.g. V1,V2,V3,V4); default from config scoring.ensemble.input_versions",
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

    trade_date = _resolve_trade_date(args.date, cal)
    versions = _resolve_versions(args.versions, project_root)

    run(project_root, trade_date, versions=versions, store=store)
    print(f"score_ensemble: db={settings.store_db}")


if __name__ == "__main__":
    main()
