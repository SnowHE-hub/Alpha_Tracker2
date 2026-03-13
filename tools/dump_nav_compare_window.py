# tools/dump_nav_compare_window.py
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"None of columns {candidates} found. columns={list(df.columns)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--version", required=True)
    ap.add_argument("--exec_csv", required=True)
    ap.add_argument("--nav_csv", default=None)  # default: latest portfolio_nav_*.csv in data/out
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_dir = root / "data" / "out"

    exec_path = Path(args.exec_csv)
    if not exec_path.is_absolute():
        exec_path = (root / args.exec_csv).resolve()
    if not exec_path.exists():
        raise FileNotFoundError(f"exec_csv not found: {exec_path}")

    if args.nav_csv:
        nav_path = Path(args.nav_csv)
        if not nav_path.is_absolute():
            nav_path = (root / args.nav_csv).resolve()
    else:
        cands = sorted(out_dir.glob("portfolio_nav_*_top*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not cands:
            raise FileNotFoundError(f"No portfolio_nav_*_top*.csv under: {out_dir}")
        nav_path = cands[0]
    if not nav_path.exists():
        raise FileNotFoundError(f"nav_csv not found: {nav_path}")

    df_exec = pd.read_csv(exec_path)
    df_nav = pd.read_csv(nav_path)

    # normalize types
    for df in (df_exec, df_nav):
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
        df["version"] = df["version"].astype(str)

    # pick columns safely
    exec_nav_col = _pick_col(df_exec, ["nav", "nav_exec", "nav_net"])
    nav_nav_col = _pick_col(df_nav, ["nav", "nav_daily", "nav_net"])
    exec_ret_col = "day_ret" if "day_ret" in df_exec.columns else None
    nav_ret_col = "day_ret" if "day_ret" in df_nav.columns else None

    # rename BEFORE merge to avoid suffix confusion
    keep_exec = ["trade_date", "version", exec_nav_col]
    if exec_ret_col:
        keep_exec.append(exec_ret_col)
    df_exec = df_exec[keep_exec].rename(
        columns={exec_nav_col: "nav_exec", exec_ret_col: "day_ret_exec"} if exec_ret_col else {exec_nav_col: "nav_exec"}
    )

    keep_nav = ["trade_date", "version", nav_nav_col]
    extra_cols = [c for c in ["picks_trade_date", "n_valid", "n_picks", "turnover", "cost", "day_ret"] if c in df_nav.columns]
    keep_nav += extra_cols
    df_nav = df_nav[keep_nav].rename(columns={nav_nav_col: "nav_nav", "day_ret": "day_ret_nav"})

    s = pd.to_datetime(args.start).date()
    e = pd.to_datetime(args.end).date()
    v = str(args.version)

    df_exec = df_exec[(df_exec["version"] == v) & (df_exec["trade_date"] >= s) & (df_exec["trade_date"] <= e)].copy()
    df_nav = df_nav[(df_nav["version"] == v) & (df_nav["trade_date"] >= s) & (df_nav["trade_date"] <= e)].copy()

    df = df_exec.merge(df_nav, on=["trade_date", "version"], how="outer")
    df["abs_diff"] = (df["nav_exec"] - df["nav_nav"]).abs()
    df = df.sort_values("trade_date")

    # output
    cols = ["trade_date", "version", "nav_exec", "nav_nav", "abs_diff"]
    for c in ["day_ret_exec", "day_ret_nav", "picks_trade_date", "n_picks", "n_valid", "turnover", "cost"]:
        if c in df.columns:
            cols.append(c)

    print(f"[EXEC] {exec_path}")
    print(f"[NAV ] {nav_path}")
    print(df[cols].to_string(index=False))


if __name__ == "__main__":
    main()
