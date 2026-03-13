from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data/store/alpha_tracker.duckdb"


def max_drawdown(nav: pd.Series) -> float:
    if nav is None or nav.empty:
        return 0.0
    nav = nav.astype(float)
    peak = nav.cummax()
    dd = nav / peak - 1.0
    return float(dd.min())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--exclude_legacy", action="store_true", default=True)
    ap.add_argument(
        "--out",
        default=str(ROOT / "data/out/leaderboard/leaderboard.csv"),
    )
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    try:
        nav = con.execute(
            """
            SELECT trade_date, strategy_id, version, nav, day_ret
            FROM nav_daily
            WHERE trade_date BETWEEN ? AND ?
            ORDER BY strategy_id, trade_date
            """,
            [args.start, args.end],
        ).df()

        ev = con.execute(
            """
            SELECT trade_date, strategy_id,
                   coverage, hit_rate, avg_ret_5d, median_ret_5d
            FROM eval_5d_batch_daily
            WHERE trade_date BETWEEN ? AND ?
            ORDER BY strategy_id, trade_date
            """,
            [args.start, args.end],
        ).df()

        if nav.empty:
            raise RuntimeError("nav_daily empty in this range")

        if args.exclude_legacy:
            nav = nav[~nav["strategy_id"].str.contains("__LEGACY__", na=False)]
            if not ev.empty:
                ev = ev[~ev["strategy_id"].str.contains("__LEGACY__", na=False)]

        nav["trade_date"] = pd.to_datetime(nav["trade_date"])
        if not ev.empty:
            ev["trade_date"] = pd.to_datetime(ev["trade_date"])

        rows = []
        for sid, g in nav.groupby("strategy_id"):
            g = g.sort_values("trade_date")
            nav_series = g["nav"].astype(float)
            start_nav = float(nav_series.iloc[0])
            end_nav = float(nav_series.iloc[-1])

            total_ret = end_nav / start_nav - 1.0
            mdd = max_drawdown(nav_series)
            vol = float(g["day_ret"].astype(float).std(ddof=0)) if len(g) > 1 else 0.0

            cov = hr = a5 = m5 = float("nan")
            if not ev.empty:
                e = ev[ev["strategy_id"] == sid]
                if not e.empty:
                    last = e.sort_values("trade_date").iloc[-1]
                    cov = float(last["coverage"])
                    hr = float(last["hit_rate"])
                    a5 = float(last["avg_ret_5d"])
                    m5 = float(last["median_ret_5d"])

            rows.append(
                {
                    "strategy_id": sid,
                    "total_return": total_ret,
                    "max_drawdown": mdd,
                    "vol_daily": vol,
                    "start_nav": start_nav,
                    "end_nav": end_nav,
                    "eval_coverage_last": cov,
                    "eval_hit_rate_last": hr,
                    "eval_avg_ret_5d_last": a5,
                    "eval_median_ret_5d_last": m5,
                    "days": int(len(g)),
                }
            )

        df = pd.DataFrame(rows).sort_values("total_return", ascending=False)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")

        print(f"[OK] leaderboard exported: {out_path}")
        print(df.head(20).to_string(index=False))
    finally:
        con.close()


if __name__ == "__main__":
    main()
