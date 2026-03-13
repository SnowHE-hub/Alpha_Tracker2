from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data/store/alpha_tracker.duckdb"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--where_model", default="V4")  # V2/V3/V4
    args = ap.parse_args()

    con = duckdb.connect(str(DB))
    try:
        # 1) nav summary
        nav = con.execute(
            """
            SELECT trade_date, strategy_id, nav, turnover, cost
            FROM nav_daily
            WHERE trade_date BETWEEN ? AND ?
              AND strategy_id LIKE ?
            ORDER BY strategy_id, trade_date
            """,
            [args.start, args.end, f"{args.where_model}%"],
        ).df()
        nav["trade_date"] = pd.to_datetime(nav["trade_date"])

        # 2) positions summary
        pos = con.execute(
            """
            SELECT asof_date, strategy_id, ticker, shares
            FROM positions_daily
            WHERE asof_date BETWEEN ? AND ?
              AND strategy_id LIKE ?
            ORDER BY strategy_id, asof_date
            """,
            [args.start, args.end, f"{args.where_model}%"],
        ).df()
        pos["asof_date"] = pd.to_datetime(pos["asof_date"])

        if nav.empty:
            raise RuntimeError("nav_daily empty for this model in range")

        rows = []
        for sid, g in nav.groupby("strategy_id"):
            g = g.sort_values("trade_date")
            end_nav = float(g["nav"].iloc[-1])
            sum_turnover = float(pd.to_numeric(g["turnover"], errors="coerce").fillna(0).sum())
            n_turn_days = int((pd.to_numeric(g["turnover"], errors="coerce").fillna(0) > 0).sum())
            sum_cost = float(pd.to_numeric(g["cost"], errors="coerce").fillna(0).sum())

            p = pos[pos["strategy_id"] == sid]
            distinct_tickers = int(p["ticker"].nunique()) if not p.empty else 0
            n_pos_days = int(p["asof_date"].nunique()) if not p.empty else 0

            rows.append(
                {
                    "strategy_id": sid,
                    "end_nav": end_nav,
                    "sum_turnover": sum_turnover,
                    "n_turn_days": n_turn_days,
                    "sum_cost": sum_cost,
                    "distinct_tickers_in_positions": distinct_tickers,
                    "position_days": n_pos_days,
                }
            )

        out = pd.DataFrame(rows).sort_values("end_nav", ascending=False)
        print(out.to_string(index=False))

    finally:
        con.close()


if __name__ == "__main__":
    main()
