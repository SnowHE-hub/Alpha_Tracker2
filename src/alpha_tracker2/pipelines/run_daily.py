"""
One-shot orchestration: run ingest → features → score → eval → nav → make_dashboard.

Pure orchestration only: parses CLI, calls each pipeline's main() in order.
No business logic (no feature computation, scoring, or NAV calculation) lives here.
All DuckDB and data work is done inside the invoked pipelines.

CLI:
  Single-day: --date YYYY-MM-DD
  Range:      --start YYYY-MM-DD --end YYYY-MM-DD

  Optional skip flags: --skip-ingest-universe, --skip-prices, --skip-features,
  --skip-score, --skip-ensemble, --skip-eval, --skip-nav, --skip-dashboard

  Optional pass-through: --limit N, --topk N (passed to build_features / portfolio_nav etc.
  when applicable).

Range mode behaviour:
  - ingest_universe, build_features, score_all, eval_5d are single-day steps; they run
    once for the end date of the range (so the pipeline has universe/scores/eval for that day).
  - ingest_prices runs over [start, end] so prices exist for the full range.
  - portfolio_nav and make_dashboard run over [start, end].
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def _invoke_main(module_main, argv: list[str]) -> None:
    """Replace sys.argv with argv, call module_main(), restore sys.argv."""
    old_argv = list(sys.argv)
    try:
        sys.argv = [old_argv[0]] + argv
        module_main()
    finally:
        sys.argv = old_argv


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run full daily pipeline: ingest → features → score → eval → nav → dashboard.",
    )
    parser.add_argument("--date", type=str, default=None, help="Trade date YYYY-MM-DD (single-day mode)")
    parser.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD (range mode)")
    parser.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD (range mode)")
    parser.add_argument("--skip-ingest-universe", action="store_true", help="Skip ingest_universe")
    parser.add_argument("--skip-prices", action="store_true", help="Skip ingest_prices")
    parser.add_argument("--skip-features", action="store_true", help="Skip build_features")
    parser.add_argument("--skip-score", action="store_true", help="Skip score_all")
    parser.add_argument("--skip-ensemble", action="store_true", help="Skip score_ensemble (after score_all)")
    parser.add_argument("--skip-eval", action="store_true", help="Skip eval_5d")
    parser.add_argument("--skip-nav", action="store_true", help="Skip portfolio_nav")
    parser.add_argument("--skip-dashboard", action="store_true", help="Skip make_dashboard")
    parser.add_argument("--limit", type=int, default=None, help="Pass to build_features (ticker limit from UNIVERSE)")
    parser.add_argument("--topk", type=int, default=None, help="Pass to portfolio_nav (top-K picks per version)")
    parser.add_argument("--last-n", type=int, default=None, help="Pass to ingest_prices (trading days lookback when no range)")
    args = parser.parse_args()

    if args.date:
        single_date = date.fromisoformat(args.date)
        start = end = single_date
    elif args.start and args.end:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
        if end < start:
            raise ValueError("--end must be >= --start")
        single_date = end  # single-day steps use end date
    else:
        raise SystemExit("Provide either --date YYYY-MM-DD or both --start and --end.")

    project_root = _find_project_root(Path(__file__).resolve())

    # Optional: write run metadata
    try:
        from alpha_tracker2.core.config import load_settings

        settings = load_settings(project_root)
        runs_dir = settings.runs_dir
        runs_dir.mkdir(parents=True, exist_ok=True)
        run_id = f"run_daily_{start.isoformat().replace('-', '')}_{end.isoformat().replace('-', '')}_{datetime.now(timezone.utc).strftime('%H%M%S')}"
        meta = {
            "run_id": run_id,
            "date": args.date,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "single_date": single_date.isoformat(),
            "skips": {
                "ingest_universe": args.skip_ingest_universe,
                "prices": args.skip_prices,
                "features": args.skip_features,
                "score": args.skip_score,
                "ensemble": args.skip_ensemble,
                "eval": args.skip_eval,
                "nav": args.skip_nav,
                "dashboard": args.skip_dashboard,
            },
            "limit": args.limit,
            "topk": args.topk,
            "last_n": args.last_n,
        }
        meta_path = runs_dir / f"{run_id}.json"
        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
        print(f"run_daily: run_meta -> {meta_path}")
    except Exception as e:
        print(f"run_daily: could not write run meta: {e}")

    # Import pipeline modules (only their main entrypoints—no business imports for computation)
    from alpha_tracker2.pipelines import build_features
    from alpha_tracker2.pipelines import eval_5d
    from alpha_tracker2.pipelines import ingest_prices
    from alpha_tracker2.pipelines import ingest_universe
    from alpha_tracker2.pipelines import make_dashboard
    from alpha_tracker2.pipelines import portfolio_nav
    from alpha_tracker2.pipelines import score_all
    from alpha_tracker2.pipelines import score_ensemble

    sd = single_date.isoformat()
    s_str = start.isoformat()
    e_str = end.isoformat()

    # 1. ingest_universe (single date)
    if not args.skip_ingest_universe:
        _invoke_main(ingest_universe.main, ["--date", sd])

    # 2. ingest_prices (range or single-day; --date ensures universe lookup uses same day as ingest_universe)
    if not args.skip_prices:
        if args.start and args.end:
            argv = ["--date", sd, "--start", s_str, "--end", e_str]
            if args.last_n is not None:
                argv.extend(["--last-n", str(args.last_n)])
            _invoke_main(ingest_prices.main, argv)
        else:
            argv = ["--date", sd]
            if args.last_n is not None:
                argv.extend(["--last-n", str(args.last_n)])
            _invoke_main(ingest_prices.main, argv)

    # 3. build_features (single date)
    if not args.skip_features:
        argv = ["--date", sd]
        if args.limit is not None:
            argv.extend(["--limit", str(args.limit)])
        _invoke_main(build_features.main, argv)

    # 4. score_all (single date)
    if not args.skip_score:
        _invoke_main(score_all.main, ["--date", sd])

    # 4b. score_ensemble (single date; writes version='ENS')
    if not args.skip_ensemble:
        _invoke_main(score_ensemble.main, ["--date", sd])

    # 5. eval_5d (single date, as_of = single_date)
    if not args.skip_eval:
        _invoke_main(eval_5d.main, ["--date", sd])

    # 6. portfolio_nav (start, end)
    if not args.skip_nav:
        argv = ["--start", s_str, "--end", e_str]
        if args.topk is not None:
            argv.extend(["--topk", str(args.topk)])
        _invoke_main(portfolio_nav.main, argv)

    # 7. make_dashboard (export [start, end] to data/out/*.csv)
    if not args.skip_dashboard:
        _invoke_main(make_dashboard.main, ["--start", s_str, "--end", e_str])

    print("run_daily: done.")


if __name__ == "__main__":
    main()
