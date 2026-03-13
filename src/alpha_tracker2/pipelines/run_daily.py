from __future__ import annotations

"""alpha_tracker2: one-click daily runner (new pipelines mainline).

This module ONLY orchestrates *existing* pipeline modules in a stable order.
It intentionally avoids adding new business logic.

Default order (research closed-loop MVP):
  1) ingest_universe
  2) ingest_prices
  3) build_features
  4) score_all
  5) (optional) score_ensemble (ENS)
  6) (optional) export_signals / generate_orders / execute_rebalance_range / nav_from_positions
  7) eval_batch (optional auto-run when dashboard needs it)
  8) portfolio_nav  (writes nav_daily)
  9) make_dashboard (reads nav_daily / eval_* from DuckDB)

Run from repo root:
  python -m alpha_tracker2.pipelines.run_daily --date 2026-01-14

Or with range:
  python -m alpha_tracker2.pipelines.run_daily --start 2025-12-20 --end 2026-01-14

Or with ENS closed-loop:
  python -m alpha_tracker2.pipelines.run_daily --start 2025-12-20 --end 2026-01-14 --run-ensemble --run-exec --cash 100000
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import pandas as pd
import yaml

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.pipelines.eval_5d_batch import main as eval_5d_batch_main


def _repo_root() -> Path:
    # pipelines/*.py -> src/alpha_tracker2/pipelines -> repo root
    return Path(__file__).resolve().parents[3]


def _run_step(step: str, fn: Callable[[], None], argv: List[str]) -> None:
    """Run a pipeline main() with controlled argv (in-process).

    We do NOT use subprocess to keep Windows quoting/powershell issues away.
    """
    old_argv = sys.argv[:]
    try:
        sys.argv = [f"{step}"] + argv
        cmd_preview = " ".join(argv)
        if len(cmd_preview) > 300:
            cmd_preview = cmd_preview[:300] + " ... (truncated)"
        print(f"\n=== [STEP] {step} {cmd_preview} ===")
        fn()
        print(f"=== [DONE] {step} ===")
    finally:
        sys.argv = old_argv


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="alpha_tracker2 one-click daily runner (new pipelines).")

    # single-day mode (legacy)
    ap.add_argument("--date", type=str, default=None, help="trade_date YYYY-MM-DD (default: latest trading day)")

    # range mode (recommended for NAV/dashboard)
    ap.add_argument("--start", type=str, default=None, help="range start YYYY-MM-DD (for nav/dashboard/eval)")
    ap.add_argument("--end", type=str, default=None, help="range end YYYY-MM-DD (for nav/dashboard/eval)")

    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit tickers for DEV (0 = all tickers in UNIVERSE).",
    )
    ap.add_argument(
        "--last-n",
        type=int,
        default=10,
        help="Price ingestion lookback trading days (only used when ingest_prices derives start).",
    )

    # optional eval (legacy single-horizon)
    ap.add_argument(
        "--run-eval",
        action="store_true",
        help="Run eval_5d after scoring (optional; uses real forward returns).",
    )
    ap.add_argument("--eval-horizon", type=int, default=5, help="Eval horizon days (default=5). Used only when --run-eval.")
    ap.add_argument("--eval-asof", type=str, default=None, help="Optional asof YYYY-MM-DD for walk-forward eval. Used only when --run-eval.")

    # ENS ensemble
    ap.add_argument("--run-ensemble", action="store_true", help="Run score_ensemble to produce version=ENS picks.")
    ap.add_argument("--ensemble-topk", type=int, default=3, help="TopK for ensemble signal (default=3).")
    ap.add_argument("--ensemble-min-agree", type=int, default=2, help="Min agree across base versions (default=2).")
    ap.add_argument("--ensemble-versions", type=str, default="V1,V2,V3,V4", help="Base versions for ensemble (default=V1,V2,V3,V4).")
    ap.add_argument("--ensemble-signal-mode", type=str, default="raw", choices=["raw", "streak"], help="Ensemble signal_mode (raw/streak).")
    ap.add_argument("--streak-k", type=int, default=2, help="If signal_mode=streak, require streak_k (default=2).")
    ap.add_argument("--lookback-days", type=int, default=120, help="If signal_mode=streak, lookback window in calendar days (default=120).")

    # execution closed-loop (optional)
    ap.add_argument("--run-exec", action="store_true", help="Run export_signals + generate_orders + execute_rebalance_range + nav_from_positions for ENS.")
    ap.add_argument("--cash", type=float, default=100000.0, help="Initial cash for execution simulator (default=100000).")
    ap.add_argument("--lot-size", type=int, default=100, help="Lot size for exec orders (default=100).")

    # portfolio_nav params
    ap.add_argument("--topk", type=int, default=3, help="TopK used for portfolio_nav/dashboard outputs (default=3).")
    ap.add_argument("--cost-bps", type=float, default=10.0, help="Transaction cost bps for portfolio_nav (default=10).")

    # skips
    ap.add_argument("--skip-universe", action="store_true", help="Skip ingest_universe.")
    ap.add_argument("--skip-prices", action="store_true", help="Skip ingest_prices.")
    ap.add_argument("--skip-features", action="store_true", help="Skip build_features.")
    ap.add_argument("--skip-score", action="store_true", help="Skip score_all.")
    ap.add_argument("--skip-portfolio", action="store_true", help="Skip portfolio_nav.")
    ap.add_argument("--skip-dashboard", action="store_true", help="Skip make_dashboard.")
    return ap.parse_args()


def _write_run_meta(repo_root: Path, payload: dict) -> Optional[Path]:
    """Write a small run metadata json into data/runs (best-effort)."""
    try:
        s = load_settings(repo_root)
        runs_dir = Path(s.runs_dir)
        if not runs_dir.is_absolute():
            runs_dir = repo_root / runs_dir
        runs_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = runs_dir / f"run_daily_{ts}.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return out
    except Exception:
        return None


def _infer_prev_and_end_trading_days(repo_root: Path, end_str: str) -> Tuple[str, str]:
    """Infer (prev_day, end_day) from prices_daily up to end_str."""
    from alpha_tracker2.storage.duckdb_store import DuckDBStore

    s = load_settings(repo_root)
    store = DuckDBStore(
        db_path=s.store_db,
        schema_path=repo_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    rows = store.fetchall(
        """
        SELECT DISTINCT trade_date
        FROM prices_daily
        WHERE trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT 2
        """,
        (end_str,),
    )
    if len(rows) < 2:
        raise RuntimeError(
            f"Need >=2 trading days in prices_daily up to {end_str}. "
            f"Got only {len(rows)} day(s). Try re-run ingest_prices or choose an earlier date."
        )

    end_day = str(rows[0][0])
    prev_day = str(rows[1][0])
    return prev_day, end_day


def _get_cfg_versions(repo_root: Path) -> List[str]:
    cfg_path = repo_root / "configs" / "default.yaml"
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    versions = cfg.get("scoring", {}).get("score_versions", ["V1", "V2", "V3", "V4"])
    return [str(v) for v in versions]


def main() -> None:
    args = _parse_args()
    repo_root = _repo_root()

    # Determine date/range strategy
    # Priority:
    #  1) if start+end provided -> range mode
    #  2) else if date provided -> single-day mode (prev_day inferred)
    #  3) else -> single-day mode with "latest trading day" is handled inside pipelines (ingest_*),
    #             but NAV/dashboard need explicit end; we will infer using prices_daily after ingest_prices.
    range_start = args.start
    range_end = args.end
    single_date = args.date

    # Step 0: run meta (best-effort)
    meta = {
        "runner": "alpha_tracker2.pipelines.run_daily",
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "date": single_date,
        "start": range_start,
        "end": range_end,
        "limit": int(args.limit),
        "last_n": int(args.last_n),
        "run_eval": bool(args.run_eval),
        "eval_horizon": int(args.eval_horizon),
        "eval_asof": args.eval_asof,
        "run_ensemble": bool(args.run_ensemble),
        "run_exec": bool(args.run_exec),
        "topk": int(args.topk),
        "cost_bps": float(args.cost_bps),
        "skips": {
            "universe": bool(args.skip_universe),
            "prices": bool(args.skip_prices),
            "features": bool(args.skip_features),
            "score": bool(args.skip_score),
            "portfolio": bool(args.skip_portfolio),
            "dashboard": bool(args.skip_dashboard),
        },
    }
    meta_path = _write_run_meta(repo_root, meta)
    if meta_path:
        print(f"[OK] run meta saved: {meta_path}")

    # import pipeline mains lazily
    from alpha_tracker2.pipelines.ingest_universe import main as ingest_universe_main
    from alpha_tracker2.pipelines.ingest_prices import main as ingest_prices_main
    from alpha_tracker2.pipelines.build_features import main as build_features_main
    from alpha_tracker2.pipelines.score_all import main as score_all_main
    from alpha_tracker2.pipelines.portfolio_nav import main as portfolio_nav_main
    from alpha_tracker2.pipelines.make_dashboard import main as make_dashboard_main

    eval_main = None
    if args.run_eval:
        from alpha_tracker2.pipelines.eval_5d import main as _eval_main
        eval_main = _eval_main

    # ---- 1) universe
    if not args.skip_universe:
        argv = []
        if single_date:
            argv += ["--date", single_date]
        _run_step("ingest_universe", ingest_universe_main, argv)

    # ---- 2) prices
    if not args.skip_prices:
        argv = []
        if single_date:
            argv += ["--date", single_date]
        argv += ["--last-n", str(args.last_n)]

        if args.limit and args.limit > 0:
            argv += ["--limit", str(args.limit)]
        else:
            # full run: avoid --tickers huge argv
            argv += ["--limit", "999999"]

        _run_step("ingest_prices", ingest_prices_main, argv)

    # ---- 3) features
    if not args.skip_features:
        argv = []
        if single_date:
            argv += ["--date", single_date]
        if args.limit and args.limit > 0:
            argv += ["--limit", str(args.limit)]
        else:
            argv += ["--limit", "999999"]
        _run_step("build_features", build_features_main, argv)

    # ---- 4) score_all
    if not args.skip_score:
        argv = []
        if single_date:
            argv += ["--date", single_date]
        _run_step("score_all", score_all_main, argv)

    # ---- 5) eval_5d (optional)
    if eval_main is not None:
        argv = []
        if args.eval_asof:
            argv += ["--asof", args.eval_asof]
        argv += ["--horizon", str(args.eval_horizon)]
        _run_step("eval_5d", eval_main, argv)

    # ---- 6) score_ensemble (ENS) optional
    if args.run_ensemble:
        if not single_date:
            raise RuntimeError("score_ensemble needs --date YYYY-MM-DD (use the signal generation day).")

        from alpha_tracker2.pipelines.score_ensemble import main as score_ensemble_main

        argv = [
            "--trade_date", single_date,
            "--versions", args.ensemble_versions,
            "--topk", str(args.ensemble_topk),
            "--min_agree", str(args.ensemble_min_agree),
            "--signal_mode", args.ensemble_signal_mode,
        ]
        if args.ensemble_signal_mode == "streak":
            argv += ["--streak_k", str(args.streak_k), "--lookback_days", str(args.lookback_days)]
        _run_step("score_ensemble", score_ensemble_main, argv)

    # ---- 7) optional execution closed-loop for ENS
    if args.run_exec:
        if not single_date:
            raise RuntimeError("Execution loop needs --date YYYY-MM-DD (signal date).")

        # (a) export_signals
        from alpha_tracker2.pipelines.export_signals import main as export_signals_main

        argv = ["--trade_date", single_date, "--versions", "ENS", "--topk", str(args.topk)]
        _run_step("export_signals", export_signals_main, argv)

        # (b) generate_orders (high-level target orders)
        from alpha_tracker2.pipelines.generate_orders import main as generate_orders_main

        argv = ["--trade_date", single_date, "--version", "ENS", "--topk", str(args.topk)]
        _run_step("generate_orders", generate_orders_main, argv)

        # (c) execute_rebalance_range (write positions_daily/trades_daily)
        from alpha_tracker2.pipelines.execute_rebalance_range import main as execute_rebalance_range_main

        # choose exec range:
        # - if --start/--end provided: use them
        # - else: use [--date, --date] would be too short for NAV; in practice you want a window, so fallback to [--date, --end] not possible
        #   -> here we run only [--date, --end] if --end provided, else [--date, --date] (still ok for positions snapshot).
        exec_start = range_start or single_date
        exec_end = range_end or single_date

        argv = [
            "--start", exec_start,
            "--end", exec_end,
            "--version", "ENS",
            "--topk", str(args.topk),
            "--cash", str(args.cash),
            "--lot_size", str(args.lot_size),
        ]
        _run_step("execute_rebalance_range", execute_rebalance_range_main, argv)

        # (d) nav_from_positions compare (non-blocking; still useful)
        from alpha_tracker2.pipelines.nav_from_positions import main as nav_from_positions_main

        argv = ["--start", exec_start, "--end", exec_end, "--versions", "ENS"]
        _run_step("nav_from_positions", nav_from_positions_main, argv)

    # ---- 8) portfolio_nav (writes nav_daily) + ---- 9) dashboard
    # Determine nav/dashboard range
    if range_start and range_end:
        nav_start = range_start
        nav_end = range_end
    else:
        # single-day mode: infer prev+end from prices_daily
        if not single_date:
            raise RuntimeError("NAV/dashboard needs either --start/--end or --date.")
        prev_day, end_day = _infer_prev_and_end_trading_days(repo_root, single_date)
        nav_start, nav_end = prev_day, end_day

    # versions for nav: config versions (+ ENS if ran)
    versions = _get_cfg_versions(repo_root)
    if args.run_ensemble and "ENS" not in versions:
        versions.append("ENS")

    versions_str = ",".join(versions)

    # ---- 8) portfolio_nav
    if not args.skip_portfolio:
        argv = [
            "--start", nav_start,
            "--end", nav_end,
            "--versions", versions_str,
            "--topk", str(args.topk),
            "--cost_bps", str(args.cost_bps),
        ]
        _run_step("portfolio_nav", portfolio_nav_main, argv)

    # ---- 9) make_dashboard (ensure eval summary exists; prefer DuckDB eval tables, but keep legacy safety)
    if not args.skip_dashboard:
        # If your make_dashboard already reads eval_batch_daily from DB, it will work even if CSV is missing.
        # We keep a small safety: if eval_5d_batch summary is needed by your local code path, auto-run it once.
        # (Your newer make_dashboard has DB-first logic; this is harmless.)
        try:
            # auto-run eval_5d_batch in the same range (legacy) - minimal args
            argv_eval_batch = ["--start", nav_start, "--end", nav_end]
            _run_step("eval_5d_batch", eval_5d_batch_main, argv_eval_batch)
        except Exception as e:
            print(f"[WARN] eval_5d_batch auto-run failed (may be ok if dashboard reads eval from DB): {e}")

        argv = [
            "--start", nav_start,
            "--end", nav_end,
            "--topk", str(args.topk),
        ]
        _run_step("make_dashboard", make_dashboard_main, argv)


if __name__ == "__main__":
    main()


# $env:PYTHONPATH="src"
# .\.venv\Scripts\python.exe .\src\alpha_tracker2\pipelines\run_daily.py --start 2025-12-20 --end 2026-01-14 --date 2026-01-07 --run-ensemble --run-exec --topk 3 --cost-bps 10 --cash 100000 --lot-size 100
