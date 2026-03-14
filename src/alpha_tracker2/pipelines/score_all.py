from __future__ import annotations

"""
Compute scores for one or more model versions on top of features_daily
and write into picks_daily.
"""

import argparse
from datetime import date
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.scoring.registry import get_scorer, list_versions
from alpha_tracker2.scoring.thresholds import ThresholdConfig, get_threshold
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


def _resolve_versions(arg_versions: str | None, project_root: Path) -> List[str]:
    if arg_versions:
        return [v.strip().upper() for v in arg_versions.split(",") if v.strip()]
    # Fallback: read from config scoring.score_versions, else registry list.
    cfg_path = project_root / "configs" / "default.yaml"
    if cfg_path.is_file():
        import yaml

        with cfg_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        scoring_cfg = raw.get("scoring") or {}
        versions = scoring_cfg.get("score_versions")
        if isinstance(versions, list) and versions:
            return [str(v).upper() for v in versions]
    return list_versions()


def _load_per_version_threshold_config(project_root: Path) -> dict[str, tuple[float, int, int]]:
    """
    Load per-version q, window, topk_fallback from scoring.v2_v3_v4.versions.<V2|V3|V4>
    with fallback to common. Returns mapping version -> (q, window, topk_fallback).
    """
    import yaml

    cfg_path = project_root / "configs" / "default.yaml"
    default_q, default_window, default_topk = 0.8, 60, 50
    common_q, common_window, common_topk = default_q, default_window, default_topk
    versions_cfg: dict = {}

    if cfg_path.is_file():
        with cfg_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        v234 = (raw.get("scoring") or {}).get("v2_v3_v4") or {}
        if isinstance(v234, dict):
            common = v234.get("common") or {}
            if isinstance(common, dict):
                if isinstance(common.get("q"), (int, float)):
                    common_q = float(common["q"])
                if isinstance(common.get("window"), int):
                    common_window = common["window"]
                if isinstance(common.get("topk_fallback"), int):
                    common_topk = common["topk_fallback"]
            vers = v234.get("versions") or {}
            if isinstance(vers, dict):
                versions_cfg = vers

    out: dict[str, tuple[float, int, int]] = {}
    for ver in ("V2", "V3", "V4"):
        v = versions_cfg.get(ver) or {}
        if not isinstance(v, dict):
            v = {}
        q = float(v["q"]) if isinstance(v.get("q"), (int, float)) else common_q
        window = int(v["window"]) if isinstance(v.get("window"), int) else common_window
        topk = int(v["topk_fallback"]) if isinstance(v.get("topk_fallback"), int) else common_topk
        out[ver] = (q, window, topk)
    return out


def _load_universe_names(store: DuckDBStore, trade_date: date) -> pd.DataFrame:
    """
    Load universe tickers and optional names from picks_daily (version='UNIVERSE').
    """
    rows = store.fetchall(
        """
        SELECT ticker, name
        FROM picks_daily
        WHERE trade_date = ? AND version = 'UNIVERSE'
        """,
        [trade_date.isoformat()],
    )
    if not rows:
        return pd.DataFrame(columns=["ticker", "name"])
    df = pd.DataFrame(rows, columns=["ticker", "name"])
    df["ticker"] = df["ticker"].astype(str)
    return df


def _normalise_scores_to_100(scores: pd.Series) -> pd.Series:
    """
    Map scores to a 0–100 range based on cross-sectional rank.
    """
    if scores.empty:
        return scores.astype(float)
    # Higher score → higher rank_100.
    ranks = scores.rank(method="min", ascending=False)
    max_rank = ranks.max()
    if max_rank <= 1:
        return pd.Series(50.0, index=scores.index, name="score_100")
    score_100 = (1.0 - (ranks - 1) / (max_rank - 1)) * 100.0
    return score_100.rename("score_100")


def _prepare_rows_for_version(
    version: str,
    trade_date: date,
    scores_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    thr_cfg: ThresholdConfig,
    thr_history_path: Path,
    fallback_topk: int,
) -> list[tuple]:
    """
    Enrich scorer output with thresholds, ranks and other picks_daily columns,
    and convert to list of tuples ready for INSERT.
    """
    if scores_df.empty:
        return []

    df = scores_df.copy()
    df["ticker"] = df["ticker"].astype(str)

    # Attach name from UNIVERSE if missing
    if "name" not in df.columns:
        df = df.merge(universe_df, on="ticker", how="left")

    if "reason" not in df.columns:
        df["reason"] = f"{version} score_all normalized"

    # Ensure basic columns
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df = df.dropna(subset=["score"])

    # Rank by score (descending)
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1

    # score_100 for display
    df["score_100"] = _normalise_scores_to_100(df["score"])

    trade_date_str = trade_date.isoformat()
    df["trade_date"] = trade_date_str
    df["version"] = version

    if version == "V1":
        df["thr_value"] = None
        df["pass_thr"] = None
        df["picked_by"] = "BASELINE_RANK"
    else:
        thr_value = get_threshold(thr_history_path, version, thr_cfg, df["score"], trade_date)
        df["thr_value"] = float(thr_value) if pd.notna(thr_value) else None

        # pass_thr based on threshold
        if pd.isna(thr_value):
            df["pass_thr"] = False
        else:
            df["pass_thr"] = df["score"] >= thr_value

        picked_by: list[str] = []
        # Threshold-based picks
        if df["pass_thr"].any():
            picked_by = ["THRESHOLD" if flag else "" for flag in df["pass_thr"]]
        else:
            # Fallback: top-k picks by score (fallback_topk from config)
            k = min(fallback_topk, len(df))
            df.loc[: k - 1, "pass_thr"] = True
            picked_by = [
                "FALLBACK_TOPK" if i < k else ""
                for i in range(len(df))
            ]
        df["picked_by"] = picked_by

    # Convert to list[tuple] following picks_daily schema
    result: list[tuple] = []
    for _, row in df.iterrows():
        result.append(
            (
                trade_date_str,
                row["version"],
                row["ticker"],
                row.get("name"),
                int(row["rank"]),
                float(row["score"]),
                float(row["score_100"]),
                str(row["reason"]),
                row.get("thr_value"),
                None if pd.isna(row.get("pass_thr")) else bool(row.get("pass_thr")),
                row.get("picked_by") or None,
            )
        )
    return result


def _delete_existing_picks(
    store: DuckDBStore,
    trade_date: date,
    versions: Iterable[str],
) -> None:
    versions_list = list({v.upper() for v in versions})
    if not versions_list:
        return
    placeholders = ",".join(["?"] * len(versions_list))
    params: list[object] = [trade_date.isoformat(), *versions_list]
    store.exec(
        f"""
        DELETE FROM picks_daily
        WHERE trade_date = ?
          AND version IN ({placeholders})
        """,
        params,
    )


def run(
    project_root: Path,
    trade_date: date,
    versions: List[str] | None = None,
    store: DuckDBStore | None = None,
) -> None:
    """
    Run score_all for the given date and versions. Optional store for testing.
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

    universe_df = _load_universe_names(store, trade_date)

    # Per-version threshold config (S-3): V2/V3/V4 each get q, window, topk_fallback from config
    per_version_thr = _load_per_version_threshold_config(project_root)
    thr_history_path = project_root / "data" / "cache" / "ab_threshold_history.json"

    # Idempotent delete per (trade_date, version)
    _delete_existing_picks(store, trade_date, versions_list)

    for version in versions_list:
        scorer = get_scorer(version, project_root)
        scores_df = scorer.score(trade_date, store)
        # V1: no threshold; V2/V3/V4: per-version thr_cfg and fallback_topk
        if version == "V1":
            thr_cfg = ThresholdConfig(q=0.8, window=60)  # unused for V1
            fallback_topk = 50
        else:
            q, window, fallback_topk = per_version_thr.get(version, (0.8, 60, 50))
            thr_cfg = ThresholdConfig(q=q, window=window)
        rows = _prepare_rows_for_version(
            version=version,
            trade_date=trade_date,
            scores_df=scores_df,
            universe_df=universe_df,
            thr_cfg=thr_cfg,
            thr_history_path=thr_history_path,
            fallback_topk=fallback_topk,
        )
        if not rows:
            print(f"score_all: trade_date={trade_date} version={version}: no rows to write")
            continue

        with store.session() as conn:
            conn.executemany(
                """
                INSERT INTO picks_daily (
                    trade_date,
                    version,
                    ticker,
                    name,
                    rank,
                    score,
                    score_100,
                    reason,
                    thr_value,
                    pass_thr,
                    picked_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        print(
            f"score_all: trade_date={trade_date} version={version} "
            f"wrote_rows={len(rows)} thr_cfg(q={thr_cfg.q}, window={thr_cfg.window})"
        )
        if version != "V1" and rows:
            # Print threshold summary from first row (identical across rows).
            first = rows[0]
            thr_value = first[8]
            print(f"  threshold_value={thr_value}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Score all configured versions into picks_daily.")
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
        help="Comma-separated score versions, e.g. V1,V2,V3,V4; "
        "default from configs/default.yaml.scoring.score_versions",
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
    print(f"score_all: db={settings.store_db}")


if __name__ == "__main__":
    main()

