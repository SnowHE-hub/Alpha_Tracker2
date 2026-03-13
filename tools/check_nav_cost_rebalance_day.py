from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(Path("data/out").resolve() / "portfolio_nav_2025-12-20_2026-01-14_top3.csv"))
    ap.add_argument("--version", default="ENS")
    ap.add_argument("--date", default="2026-01-08")  # 预期 rebalance 扣成本日
    args = ap.parse_args()

    p = Path(args.csv)
    if not p.exists():
        raise FileNotFoundError(f"csv not found: {p}")

    df = pd.read_csv(p)
    df = df[df["version"].astype(str) == str(args.version)].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date.astype(str)

    row = df[df["trade_date"] == args.date]
    if row.empty:
        raise RuntimeError(f"no row for version={args.version} date={args.date}")

    cols = ["trade_date", "version", "turnover", "cost_bps", "cost", "day_ret_gross", "day_ret", "nav_gross", "nav", "picks_trade_date", "n_picks", "n_valid"]
    print(row[cols].to_string(index=False))

    # 额外给出期望值
    turnover = float(row["turnover"].iloc[0])
    cost_bps = float(row["cost_bps"].iloc[0])
    expected_cost = turnover * (cost_bps / 10000.0)
    print("\n[EXPECT]")
    print(f"expected_cost = turnover * cost_bps/10000 = {turnover:.6f} * {cost_bps:.2f}/10000 = {expected_cost:.6f}")


if __name__ == "__main__":
    main()
