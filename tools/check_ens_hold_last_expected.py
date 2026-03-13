# tools/check_ens_hold_last_expected.py
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def _parse_date(s: str) -> date:
    return pd.to_datetime(s).date()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "data" / "store" / "alpha_tracker.duckdb"))
    ap.add_argument("--version", default="ENS")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--topk", type=int, default=3)
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)

    con = duckdb.connect(args.db)

    # trading calendar
    cal = con.execute(
        """
        SELECT DISTINCT trade_date
        FROM prices_daily
        WHERE trade_date BETWEEN ? AND ?
        ORDER BY trade_date
        """,
        (start, end),
    ).fetchdf()
    cal["trade_date"] = pd.to_datetime(cal["trade_date"]).dt.date
    days = cal["trade_date"].tolist()

    # signal days (picks exist)
    sig = con.execute(
        """
        SELECT trade_date, COUNT(*) AS n
        FROM picks_daily
        WHERE version = ?
          AND trade_date BETWEEN ? AND ?
          AND rank <= ?
        GROUP BY 1
        ORDER BY 1
        """,
        (args.version, start, end, args.topk),
    ).fetchdf()
    if sig.empty:
        print("[INFO] no signals in range.")
        return
    sig["trade_date"] = pd.to_datetime(sig["trade_date"]).dt.date

    # map signal_date -> exec_date (next trading day)
    sig_dates = sig["trade_date"].tolist()
    day_to_idx = {d: i for i, d in enumerate(days)}

    print("=== Signals (signal_date=t, should exec at t+1) ===")
    for t in sig_dates:
        i = day_to_idx.get(t, None)
        exec_d = days[i + 1] if i is not None and i + 1 < len(days) else None
        print(f"signal_date={t} picks={int(sig[sig['trade_date']==t]['n'].iloc[0])} -> exec_date={exec_d}")

    # expected holding segments
    print("\n=== Expected holding days (mark-to-market) ===")
    for t in sig_dates:
        i = day_to_idx.get(t, None)
        if i is None or i + 1 >= len(days):
            continue
        exec_d = days[i + 1]
        # hold until next signal's exec_date - 1, else to end
        next_exec = None
        later = [x for x in sig_dates if x > t]
        if later:
            t2 = later[0]
            j = day_to_idx.get(t2, None)
            if j is not None and j + 1 < len(days):
                next_exec = days[j + 1]

        hold_start = exec_d
        hold_end = (days[day_to_idx[next_exec] - 1] if next_exec else end)
        print(f"from {hold_start} to {hold_end}  (held tickers from signal {t})")

    con.close()


if __name__ == "__main__":
    main()
