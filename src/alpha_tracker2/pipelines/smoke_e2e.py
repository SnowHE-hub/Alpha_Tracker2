"""
End-to-end smoke test: run the full pipeline over a date range and verify
all core tables are populated with reasonable row counts and non-null key columns.

Usage:
  PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --start 2026-01-06 --end 2026-01-15 [--limit 5] [--topk 3]
  PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --date 2026-01-15 [--limit 5] [--topk 3]

Steps (in order): ingest_universe → ingest_prices → build_features → score_all → eval_5d → portfolio_nav.
Then checks: prices_daily, features_daily, picks_daily, eval_5d_daily, nav_daily exist with data and key columns non-null.
Exits with code 0 on success, 1 on failure (with clear SMOKE FAIL message).

Note: build_features needs enough price history (e.g. 260 trading days) for the target date. Use a --start/--end
range that includes sufficient history (e.g. --start 2024-01-01 --end 2026-01-14) when running from empty DB.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def _run_step(project_root: Path, module: str, args: list[str]) -> None:
    cmd = [sys.executable, "-m", module] + args
    env = {**__import__("os").environ, "PYTHONPATH": str(project_root / "src")}
    result = subprocess.run(cmd, cwd=project_root, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"Step {module} failed with exit code {result.returncode}")


def _check_table_exists(store: DuckDBStore, table: str) -> bool:
    row = store.fetchone(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    )
    return row is not None and int(row[0]) > 0


def _run_checks(
    store: DuckDBStore,
    start: date,
    end: date,
    target_date: date,
) -> list[str]:
    """Run all smoke checks. Returns list of failure messages (empty if all pass)."""
    failures: list[str] = []
    start_str = start.isoformat()
    end_str = end.isoformat()
    target_str = target_date.isoformat()

    tables = ["prices_daily", "features_daily", "picks_daily", "eval_5d_daily", "nav_daily"]
    for t in tables:
        if not _check_table_exists(store, t):
            failures.append(f"Table {t} does not exist")
            continue

    if "prices_daily does not exist" not in str(failures):
        r = store.fetchone(
            """
            SELECT COUNT(*), COUNT(DISTINCT ticker)
            FROM prices_daily
            WHERE trade_date >= ? AND trade_date <= ?
            """,
            [start_str, end_str],
        )
        if not r or int(r[0]) < 1:
            failures.append("SMOKE FAIL: prices_daily has 0 rows in range [start, end]")
        elif int(r[1]) < 1:
            failures.append("SMOKE FAIL: prices_daily has 0 distinct tickers in range")
        else:
            nulls = store.fetchone(
                """
                SELECT COUNT(*) FROM prices_daily
                WHERE trade_date >= ? AND trade_date <= ?
                  AND (trade_date IS NULL OR ticker IS NULL OR adj_close IS NULL)
                """,
                [start_str, end_str],
            )
            if nulls and int(nulls[0]) > 0:
                failures.append("SMOKE FAIL: prices_daily has NULL in trade_date/ticker/adj_close")

    if "features_daily does not exist" not in str(failures):
        r = store.fetchone(
            "SELECT COUNT(*) FROM features_daily WHERE trade_date = ?",
            [target_str],
        )
        if not r or int(r[0]) < 1:
            failures.append("SMOKE FAIL: features_daily has 0 rows for target trade_date")
        else:
            nulls = store.fetchone(
                """
                SELECT COUNT(*) FROM features_daily
                WHERE trade_date = ? AND (trade_date IS NULL OR ticker IS NULL)
                """,
                [target_str],
            )
            if nulls and int(nulls[0]) > 0:
                failures.append("SMOKE FAIL: features_daily has NULL in trade_date/ticker")
            # at least one feature column non-null in some row
            rn = store.fetchone(
                """
                SELECT COUNT(*) FROM features_daily
                WHERE trade_date = ? AND ret_1d IS NOT NULL
                """,
                [target_str],
            )
            if not rn or int(rn[0]) < 1:
                failures.append("SMOKE FAIL: features_daily has no row with ret_1d non-null")

    if "picks_daily does not exist" not in str(failures):
        r_univ = store.fetchone(
            "SELECT COUNT(*) FROM picks_daily WHERE trade_date = ? AND version = 'UNIVERSE'",
            [target_str],
        )
        if not r_univ or int(r_univ[0]) < 1:
            failures.append("SMOKE FAIL: picks_daily has 0 rows for version='UNIVERSE' on target date")
        r_ver = store.fetchone(
            """
            SELECT COUNT(*) FROM picks_daily
            WHERE trade_date = ? AND version IN ('V1','V2','V3','V4')
            """,
            [target_str],
        )
        if not r_ver or int(r_ver[0]) < 1:
            failures.append(
                "SMOKE FAIL: picks_daily has 0 rows for V1/V2/V3/V4 on target date"
            )
        nulls = store.fetchone(
            """
            SELECT COUNT(*) FROM picks_daily
            WHERE trade_date = ? AND (trade_date IS NULL OR version IS NULL OR ticker IS NULL OR score IS NULL)
            """,
            [target_str],
        )
        if nulls and int(nulls[0]) > 0:
            failures.append("SMOKE FAIL: picks_daily has NULL in trade_date/version/ticker/score")

    if "eval_5d_daily does not exist" not in str(failures):
        r = store.fetchone(
            "SELECT COUNT(*) FROM eval_5d_daily WHERE as_of_date = ?",
            [target_str],
        )
        if not r or int(r[0]) < 1:
            failures.append("SMOKE FAIL: eval_5d_daily has 0 rows for as_of_date=target")
        else:
            nulls = store.fetchone(
                """
                SELECT COUNT(*) FROM eval_5d_daily
                WHERE as_of_date = ? AND (as_of_date IS NULL OR version IS NULL OR bucket IS NULL)
                """,
                [target_str],
            )
            if nulls and int(nulls[0]) > 0:
                failures.append("SMOKE FAIL: eval_5d_daily has NULL in as_of_date/version/bucket")

    if "nav_daily does not exist" not in str(failures):
        r = store.fetchone(
            """
            SELECT COUNT(*), COUNT(DISTINCT portfolio)
            FROM nav_daily
            WHERE trade_date >= ? AND trade_date <= ?
            """,
            [start_str, end_str],
        )
        if not r or int(r[0]) < 1:
            failures.append("SMOKE FAIL: nav_daily has 0 rows in range [start, end]")
        elif int(r[1]) < 1:
            failures.append("SMOKE FAIL: nav_daily has 0 distinct portfolios in range")
        else:
            nulls = store.fetchone(
                """
                SELECT COUNT(*) FROM nav_daily
                WHERE trade_date >= ? AND trade_date <= ?
                  AND (trade_date IS NULL OR portfolio IS NULL OR nav IS NULL)
                """,
                [start_str, end_str],
            )
            if nulls and int(nulls[0]) > 0:
                failures.append("SMOKE FAIL: nav_daily has NULL in trade_date/portfolio/nav")

    return failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="E2E smoke: run pipeline steps then verify five core tables.",
    )
    parser.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Single date YYYY-MM-DD (equivalent to --start X --end X)",
    )
    parser.add_argument("--limit", type=int, default=5, help="Max tickers from UNIVERSE (ingest_prices, build_features)")
    parser.add_argument("--topk", type=int, default=3, help="Top-K per version (portfolio_nav)")
    args = parser.parse_args()

    if args.date:
        start = end = target_date = date.fromisoformat(args.date)
    elif args.start and args.end:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
        target_date = end
    else:
        print("SMOKE FAIL: provide --start and --end, or --date")
        return 1

    if end < start:
        print("SMOKE FAIL: --end must be >= --start")
        return 1

    project_root = _find_project_root(Path(__file__).resolve())
    settings = load_settings(project_root)
    target_str = target_date.isoformat()
    start_str = start.isoformat()
    end_str = end.isoformat()

    print("smoke_e2e: running 6 steps...")
    print(f"  project_root={project_root} store_db={settings.store_db}")
    print(f"  start={start_str} end={end_str} target_date={target_str} limit={args.limit} topk={args.topk}")

    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    try:
        _run_step(project_root, "alpha_tracker2.pipelines.ingest_universe", ["--date", target_str])
        _run_step(
            project_root,
            "alpha_tracker2.pipelines.ingest_prices",
            ["--start", start_str, "--end", end_str, "--date", target_str, "--limit", str(args.limit)],
        )
        # Resolve target to actual max trade_date in prices_daily so build_features has data (Yahoo may not return end date)
        row = store.fetchone(
            "SELECT MAX(trade_date) FROM prices_daily WHERE trade_date >= ? AND trade_date <= ?",
            [start_str, end_str],
        )
        resolved_date = row[0] if row and row[0] is not None else None
        if resolved_date is None:
            resolved_date = end
        else:
            resolved_date = resolved_date if isinstance(resolved_date, date) else date.fromisoformat(str(resolved_date)[:10])
        if resolved_date < start:
            print("SMOKE FAIL: no prices_daily in range [start, end]; cannot run build_features.")
            return 1
        resolved_str = resolved_date.isoformat()
        if resolved_str != target_str:
            print(f"smoke_e2e: using resolved target_date={resolved_str} (prices_daily max in range; requested end={end_str})")
            target_date = resolved_date
            target_str = resolved_str
        # Ensure UNIVERSE exists for resolved date (may equal end already)
        _run_step(project_root, "alpha_tracker2.pipelines.ingest_universe", ["--date", target_str])
        _run_step(
            project_root,
            "alpha_tracker2.pipelines.build_features",
            ["--date", target_str, "--limit", str(args.limit)],
        )
        _run_step(project_root, "alpha_tracker2.pipelines.score_all", ["--date", target_str])
        _run_step(project_root, "alpha_tracker2.pipelines.eval_5d", ["--date", target_str])
        _run_step(
            project_root,
            "alpha_tracker2.pipelines.portfolio_nav",
            ["--start", start_str, "--end", end_str, "--topk", str(args.topk)],
        )
    except RuntimeError as e:
        print(str(e))
        return 1

    print("smoke_e2e: running table checks...")
    failures = _run_checks(store, start, end, target_date)
    if failures:
        for f in failures:
            print(f)
        return 1

    print("smoke_e2e: all steps and checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
