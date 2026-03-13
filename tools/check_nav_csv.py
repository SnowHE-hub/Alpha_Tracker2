# check_nav_csv.py
# Usage:
#   python check_nav_csv.py --latest
#   python check_nav_csv.py --file data/out/portfolio_nav_2026-01-13_2026-01-14_top3.csv

from __future__ import annotations

import argparse
from pathlib import Path
import re
import pandas as pd


def _root() -> Path:
    return Path(__file__).resolve().parents[1] 


def _out_dir() -> Path:
    return _root() / "data" / "out"


def _pick_latest_nav_csv(out_dir: Path) -> Path | None:
    files = sorted(out_dir.glob("portfolio_nav_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _infer_dates_from_filename(name: str) -> tuple[str | None, str | None]:
    # portfolio_nav_2026-01-13_2026-01-14_top3.csv
    m = re.search(r"portfolio_nav_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_", name)
    if not m:
        return None, None
    return m.group(1), m.group(2)


def _detect_col(cols_lower: list[str], candidates: tuple[str, ...]) -> str | None:
    for c in candidates:
        if c in cols_lower:
            return c
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=None, help="Explicit CSV file path under data/out")
    ap.add_argument("--latest", action="store_true", help="Use latest portfolio_nav_*.csv from data/out")
    ap.add_argument("--top", type=int, default=10, help="Show top N rows")
    args = ap.parse_args()

    out_dir = _out_dir()
    if not out_dir.exists():
        raise FileNotFoundError(f"[ERROR] data/out not found: {out_dir}")

    if args.latest:
        fp = _pick_latest_nav_csv(out_dir)
        if fp is None:
            raise FileNotFoundError("[ERROR] No portfolio_nav_*.csv found in data/out")
    elif args.file:
        fp = Path(args.file)
        if not fp.is_absolute():
            fp = _root() / fp
    else:
        raise SystemExit("Provide --latest or --file")

    if not fp.exists():
        raise FileNotFoundError(f"[ERROR] CSV not found: {fp}")

    start_dt, end_dt = _infer_dates_from_filename(fp.name)

    print(f"[FILE] {fp}")
    print(f"[NAME] {fp.name}")
    if start_dt and end_dt:
        print(f"[FILENAME_DATES] start={start_dt}, end={end_dt}")

    df = pd.read_csv(fp)
    print("\n" + "=" * 90)
    print("[COLUMNS]")
    print(list(df.columns))
    print(f"[ROWS] {len(df)}")

    cols_lower = [c.lower() for c in df.columns]

    ver_col = _detect_col(cols_lower, ("version", "strategy", "model"))
    date_col = _detect_col(cols_lower, ("date", "trade_date", "dt"))
    nav_col = _detect_col(cols_lower, ("nav", "equity", "value", "nav_value"))
    ret_col = _detect_col(cols_lower, ("ret", "return", "day_ret", "daily_ret"))

    # map back to original col names
    def orig(c_lower: str | None) -> str | None:
        if c_lower is None:
            return None
        return df.columns[cols_lower.index(c_lower)]

    ver_col_o = orig(ver_col)
    date_col_o = orig(date_col)
    nav_col_o = orig(nav_col)
    ret_col_o = orig(ret_col)

    print("\n" + "=" * 90)
    print("[DETECTED]")
    print(f"version_col = {ver_col_o}")
    print(f"date_col    = {date_col_o}")
    print(f"nav_col     = {nav_col_o}")
    print(f"ret_col     = {ret_col_o}")

    print("\n" + "=" * 90)
    print("[HEAD]")
    print(df.head(args.top))

    # Summaries
    if date_col_o:
        try:
            dd = pd.to_datetime(df[date_col_o], errors="coerce")
            print("\n" + "=" * 90)
            print("[DATE_RANGE]")
            print(f"min={dd.min()} max={dd.max()} null_dates={dd.isna().sum()}")
        except Exception as e:
            print(f"[WARN] Failed to parse date column {date_col_o}: {e}")

    if ver_col_o and nav_col_o:
        try:
            tmp = df.copy()
            if date_col_o:
                tmp["_dt"] = pd.to_datetime(tmp[date_col_o], errors="coerce")
                tmp = tmp.sort_values(["_dt"])
            last = tmp.groupby(ver_col_o, as_index=False).tail(1)
            print("\n" + "=" * 90)
            print("[LAST_NAV_PER_VERSION]")
            cols_show = [ver_col_o]
            if date_col_o:
                cols_show.append(date_col_o)
            cols_show.append(nav_col_o)
            if ret_col_o:
                cols_show.append(ret_col_o)
            print(last[cols_show].reset_index(drop=True))
        except Exception as e:
            print(f"[WARN] Failed last nav summary: {e}")

    # Basic sanity: if filename has start/end and date_col exists, check containment
    if start_dt and end_dt and date_col_o:
        try:
            dd = pd.to_datetime(df[date_col_o], errors="coerce")
            s = pd.to_datetime(start_dt)
            e = pd.to_datetime(end_dt)
            out_of_range = ((dd < s) | (dd > e)).sum()
            print("\n" + "=" * 90)
            print("[FILENAME_RANGE_CHECK]")
            print(f"out_of_range_rows={int(out_of_range)} (expect 0)")
        except Exception as e:
            print(f"[WARN] filename range check failed: {e}")

    print("\n[DONE] check_nav_csv")


if __name__ == "__main__":
    main()
