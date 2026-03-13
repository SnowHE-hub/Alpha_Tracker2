# tools/run_research_window.py
from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import duckdb


# -------------------------
# Helpers
# -------------------------
def _project_root() -> Path:
    # D:\alpha_tracker2\tools\run_research_window.py -> project root = parents[1]
    return Path(__file__).resolve().parents[1]


def _getattr_chain(obj, chain: str):
    """
    chain example: "paths.store_db"
    """
    cur = obj
    for part in chain.split("."):
        if not hasattr(cur, part):
            return None
        cur = getattr(cur, part)
    return cur


def _resolve_store_db(settings, project_root: Path) -> Path:
    """
    Robustly locate DuckDB path across different Settings shapes.
    Priority:
      1) settings.paths.store_db
      2) settings.store_db
      3) settings.db_path
      4) fallback: <root>/data/store/alpha_tracker.duckdb
    """
    for key in ("paths.store_db", "store_db", "db_path"):
        v = _getattr_chain(settings, key) if "." in key else getattr(settings, key, None)
        if v:
            return Path(v)
    return project_root / "data" / "store" / "alpha_tracker.duckdb"


def _detect_strategies_model_col(con: duckdb.DuckDBPyConnection, table: str = "strategies") -> str:
    cols = [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
    for cand in ("model_version", "model", "version"):
        if cand in cols:
            return cand
    raise RuntimeError(f"[FATAL] Cannot find model/version column in {table}. cols={cols}")


def _fetch_strategy_ids(db_path: Path, models: List[str]) -> List[str]:
    """
    Read strategy_id list for model versions (e.g., V2,V3,V4).
    This auto-detects the model/version column name.
    """
    con = duckdb.connect(str(db_path))
    try:
        model_col = _detect_strategies_model_col(con, "strategies")
        placeholders = ",".join(["?"] * len(models))
        q = f"""
            SELECT strategy_id
            FROM strategies
            WHERE {model_col} IN ({placeholders})
            ORDER BY strategy_id
        """
        rows = con.execute(q, models).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


def _run(cmd: List[str]) -> None:
    # Print exactly what will run (like your other batch tools)
    print("\n>> " + " ".join(cmd))
    subprocess.run(cmd, check=True)


def _py() -> str:
    return sys.executable


# -------------------------
# Main
# -------------------------
@dataclass
class Args:
    start: str
    end: str
    models: List[str]
    cost_bps: float
    initial_equity: float
    diag_model: str
    do_backfill_turnover: bool
    do_report_bundle: bool


def parse_args() -> Args:
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--models", required=True, help="Comma-separated, e.g. V2,V3,V4")
    p.add_argument("--cost_bps", type=float, default=10.0)
    p.add_argument("--initial_equity", type=float, default=100000.0)
    p.add_argument("--diag_model", default="V4")
    p.add_argument("--do_backfill_turnover", action="store_true")
    p.add_argument("--do_report_bundle", action="store_true")
    a = p.parse_args()

    models = [x.strip() for x in a.models.split(",") if x.strip()]
    if not models:
        raise SystemExit("[FATAL] --models is empty")

    return Args(
        start=a.start,
        end=a.end,
        models=models,
        cost_bps=a.cost_bps,
        initial_equity=a.initial_equity,
        diag_model=a.diag_model,
        do_backfill_turnover=a.do_backfill_turnover,
        do_report_bundle=a.do_report_bundle,
    )


def main() -> None:
    root = _project_root()

    # Load settings with project_root (your config requires it)
    from alpha_tracker2.core.config import load_settings  # local import to match your project

    settings = load_settings(root)
    db_path = _resolve_store_db(settings, root)
    if not db_path.exists():
        raise SystemExit(f"[FATAL] db not found: {db_path}")

    # 1) fetch strategy ids for models
    strategy_ids = _fetch_strategy_ids(db_path, args.models)
    if not strategy_ids:
        raise SystemExit(f"[FATAL] no strategies found for models={args.models} in db={db_path}")

    strategy_ids_arg = ",".join(strategy_ids)

    # 2) portfolio_nav for all strategies (same as your bundle flow)
    nav_py = str(root / "src" / "alpha_tracker2" / "pipelines" / "portfolio_nav.py")
    _run(
        [
            _py(),
            nav_py,
            "--start",
            args.start,
            "--end",
            args.end,
            "--strategy_ids",
            strategy_ids_arg,
            "--cost_bps",
            str(args.cost_bps),
            "--initial_equity",
            str(args.initial_equity),
        ]
    )

    # 3) eval_5d_from_nav per model
    eval_py = str(root / "tools" / "eval_5d_from_nav.py")
    for m in args.models:
        _run([_py(), eval_py, "--start", args.start, "--end", args.end, "--where_model", m, "--horizon", "5"])

    # 4) leaderboard
    lb_py = str(root / "tools" / "export_strategy_leaderboard.py")
    _run([_py(), lb_py, "--start", args.start, "--end", args.end])

    # 5) optional: turnover backfill + diagnostics
    if args.do_backfill_turnover:
        backfill_py = str(root / "tools" / "backfill_nav_turnover_cost_from_positions.py")
        diag_py = str(root / "tools" / "diagnose_turnover_from_positions.py")
        _run(
            [
                _py(),
                backfill_py,
                "--db",
                str(db_path),
                "--start",
                args.start,
                "--end",
                args.end,
                "--where_model",
                args.diag_model,
            ]
        )
        _run(
            [
                _py(),
                diag_py,
                "--db",
                str(db_path),
                "--start",
                args.start,
                "--end",
                args.end,
                "--where_model",
                args.diag_model,
            ]
        )

    # 6) optional: run_report_bundle (if you still want it as a final “one-shot”)
    if args.do_report_bundle:
        bundle_py = str(root / "tools" / "run_report_bundle.py")
        _run(
            [
                _py(),
                bundle_py,
                "--start",
                args.start,
                "--end",
                args.end,
                "--models",
                ",".join(args.models),
                "--cost_bps",
                str(args.cost_bps),
                "--initial_equity",
                str(args.initial_equity),
                "--diag_model",
                args.diag_model,
            ]
        )

    print("\n[ALL DONE] research window finished.")


if __name__ == "__main__":
    args = parse_args()
    main()
