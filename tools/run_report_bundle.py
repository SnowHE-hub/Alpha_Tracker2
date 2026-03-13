# -*- coding: utf-8 -*-
"""
Run report bundle for strategy matrix:
1) run portfolio_nav for selected strategies / model
2) run eval_5d_from_nav for models
3) export leaderboard
4) run sanity checks (turnover/cost not all-zero for executed strategies)

Usage:
  python tools/run_report_bundle.py --start 2026-01-06 --end 2026-01-14 --models V2,V3,V4 --cost_bps 10 --initial_equity 100000
  python tools/run_report_bundle.py --start 2026-01-06 --end 2026-01-14 --strategy_ids "V4__...,V4__..." --cost_bps 10 --initial_equity 100000

Notes:
- Uses Settings.paths.store_db, so always hits the same DB as pipelines.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / ".venv" / "Scripts" / "python.exe"
PIPE_NAV = ROOT / "src" / "alpha_tracker2" / "pipelines" / "portfolio_nav.py"
TOOL_EVAL = ROOT / "tools" / "eval_5d_from_nav.py"
TOOL_LB = ROOT / "tools" / "export_strategy_leaderboard.py"
TOOL_DIAG = ROOT / "tools" / "diagnose_turnover_from_positions.py"


def run(cmd: list[str]) -> None:
    print("\n>> " + " ".join(cmd))
    p = subprocess.run(cmd, cwd=str(ROOT))
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def _strategy_ids_from_models(models: list[str]) -> str:
    # keep consistent with register_strategies.py
    rebs = ["REB_DAILY", "REB_ON_SIGNAL_CHANGE"]
    holds = [1, 5, 20]
    topk = 6
    cost_bps = 10  # strategy_id has C10 fixed in your registry (if you change registry, update here)
    ids = []
    for m in models:
        for r in rebs:
            for h in holds:
                ids.append(f"{m}__{r}__H{h}__TOP{topk}__C{cost_bps}")
    return ",".join(ids)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--models", default="V2,V3,V4")
    ap.add_argument("--strategy_ids", default="")
    ap.add_argument("--cost_bps", type=float, default=10.0)
    ap.add_argument("--initial_equity", type=float, default=100000.0)
    ap.add_argument("--horizon", type=int, default=5)
    ap.add_argument("--diag_model", default="")  # optionally run diagnose for one model (e.g. V4)
    args = ap.parse_args()

    models = [x.strip() for x in args.models.split(",") if x.strip()]
    strategy_ids = args.strategy_ids.strip() or _strategy_ids_from_models(models)

    # 1) portfolio_nav
    run([
        str(PY), str(PIPE_NAV),
        "--start", args.start,
        "--end", args.end,
        "--strategy_ids", strategy_ids,
        "--cost_bps", str(args.cost_bps),
        "--initial_equity", str(args.initial_equity),
    ])

    # 2) eval for each model
    for m in models:
        run([
            str(PY), str(TOOL_EVAL),
            "--start", args.start,
            "--end", args.end,
            "--where_model", m,
            "--horizon", str(args.horizon),
        ])

    # 3) leaderboard
    run([
        str(PY), str(TOOL_LB),
        "--start", args.start,
        "--end", args.end,
    ])

    # 4) optional diagnose
    if args.diag_model.strip():
        run([
            str(PY), str(TOOL_DIAG),
            "--db", str(ROOT / "data" / "store" / "alpha_tracker.duckdb"),
            "--start", args.start,
            "--end", args.end,
            "--where_model", args.diag_model.strip(),
        ])

    print("\n[ALL DONE] report bundle finished.")


if __name__ == "__main__":
    main()
