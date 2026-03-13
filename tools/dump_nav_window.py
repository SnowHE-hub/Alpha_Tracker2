# tools/dump_nav_window.py
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def find_latest_nav_csv(out_dir: Path) -> Path:
    files = sorted(out_dir.glob("portfolio_nav_*_top*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No portfolio_nav csv found in: {out_dir}")
    return files[0]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", type=str, default="", help="Path to portfolio_nav_*.csv. If empty, use latest in data/out.")
    ap.add_argument("--start", type=str, required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", type=str, required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--version", type=str, default="ENS", help="Version filter (default ENS)")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_dir = root / "data" / "out"

    csv_path = Path(args.file) if args.file else find_latest_nav_csv(out_dir)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    # normalize dates
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    start = pd.to_datetime(args.start)
    end = pd.to_datetime(args.end)

    # filter
    dff = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)]
    if "version" in dff.columns:
        dff = dff[dff["version"].astype(str) == str(args.version)]

    # sort for readability
    dff = dff.sort_values(["trade_date"])

    print(f"[FILE] {csv_path}")
    print(f"[ROWS_IN_WINDOW] {len(dff)}")
    if len(dff) == 0:
        print("[WARN] No rows matched. Check dates/version.")
        return

    # print all columns, no truncation
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_colwidth", 200)
    print(dff.to_string(index=False))


if __name__ == "__main__":
    main()
