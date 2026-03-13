from __future__ import annotations

import argparse
import subprocess
from datetime import datetime, timedelta, date
from pathlib import Path

import duckdb

from alpha_tracker2.core.trading_calendar import TradingCalendar


def _to_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _run(cmd: list[str]) -> None:
    print("\n$ " + " ".join(cmd))
    subprocess.check_call(cmd)


def _has_universe(db_path: str, trade_date: str) -> bool:
    con = duckdb.connect(db_path)
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM picks_daily WHERE trade_date=? AND version='UNIVERSE';",
            [trade_date],
        ).fetchone()[0]
        return int(n) > 0
    finally:
        con.close()


def _parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--prices_lookback_days", type=int, default=90, help="calendar days lookback for prices")
    ap.add_argument("--prices_forward_days", type=int, default=30, help="calendar days forward for prices")
    ap.add_argument(
        "--tickers",
        default=None,
        help="optional: comma separated tickers to override universe selection (e.g. 000001.SZ,000002.SZ,000004.SZ)",
    )
    return ap.parse_args()


def main():
    args = _parse_args()
    cal = TradingCalendar()

    start = _to_date(args.start)
    end = _to_date(args.end)

    days = cal.trading_days(start, end)
    if not days:
        raise RuntimeError(f"No trading days between {start} and {end}")

    root = Path(__file__).resolve().parents[3]
    py = str(root / ".venv" / "Scripts" / "python.exe")

    # DB 路径（用于检查 universe 是否存在）
    db_path = str(root / "data" / "store" / "alpha_tracker.duckdb")

    print("[OK] backfill_runs started.")
    print("range:", start, "to", end)
    print("trading_days_n:", len(days))
    if args.tickers:
        print("tickers_override:", args.tickers)

    for d in days:
        dstr = d.isoformat()
        prices_start = (d - timedelta(days=int(args.prices_lookback_days))).isoformat()
        prices_end = (d + timedelta(days=int(args.prices_forward_days))).isoformat()

        print("\n" + "=" * 80)
        print("trade_date:", dstr)
        print("=" * 80)

        # 0) ensure universe exists for that trade_date
        if not _has_universe(db_path, dstr):
            _run(
                [
                    py,
                    str(root / "src" / "alpha_tracker2" / "pipelines" / "ingest_universe.py"),
                    "--date",
                    dstr,
                ]
            )
        else:
            print("[OK] universe already exists for", dstr)

        # 1) prices
        cmd_prices = [
            py,
            str(root / "src" / "alpha_tracker2" / "pipelines" / "ingest_prices.py"),
            "--date",
            dstr,
            "--start",
            prices_start,
            "--end",
            prices_end,
        ]
        if args.tickers:
            cmd_prices += ["--tickers", args.tickers]
        _run(cmd_prices)

        # 2) features
        _run([py, str(root / "src" / "alpha_tracker2" / "pipelines" / "build_features.py"), "--date", dstr])

        # 3) score_all
        _run([py, str(root / "src" / "alpha_tracker2" / "pipelines" / "score_all.py"), "--date", dstr])

    print("\n[OK] backfill_runs passed.")


if __name__ == "__main__":
    main()
