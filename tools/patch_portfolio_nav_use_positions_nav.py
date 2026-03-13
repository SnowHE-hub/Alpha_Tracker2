from __future__ import annotations

from pathlib import Path


NEW_CODE = r'''from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Dict, Tuple

import duckdb
import pandas as pd


def _parse_date(s: str) -> date:
    return pd.to_datetime(s).date()


def _parse_versions(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _project_root() -> Path:
    # .../src/alpha_tracker2/pipelines/portfolio_nav.py -> project root
    return Path(__file__).resolve().parents[3]


def _db_path(root: Path) -> Path:
    return root / "data" / "store" / "alpha_tracker.duckdb"


def _out_path(root: Path, start: date, end: date, topk: int) -> Path:
    out_dir = root / "data" / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"portfolio_nav_{start}_{end}_top{topk}.csv"


def _latest_signal_on_or_before(con: duckdb.DuckDBPyConnection, version: str, d: date) -> str:
    # picks_daily.trade_date is signal date
    r = con.execute(
        """
        SELECT MAX(trade_date) AS sig
        FROM picks_daily
        WHERE version = ?
          AND trade_date <= ?
        """,
        [version, d],
    ).fetchone()
    sig = r[0]
    return str(sig) if sig is not None else "INIT"


def _nav_from_positions_ffill(
    con: duckdb.DuckDBPyConnection,
    version: str,
    start: date,
    end: date,
    initial_equity: float,
) -> pd.DataFrame:
    # Build daily equity from positions_daily, using close_ffill from prices_daily when exact close missing.
    # positions_daily has: asof_date, ticker, shares, cash
    # prices_daily has: trade_date, ticker, close
    q = """
    WITH pos AS (
      SELECT
        asof_date AS trade_date,
        version,
        ticker,
        shares,
        cash
      FROM positions_daily
      WHERE version = ?
        AND asof_date BETWEEN ? AND ?
        AND ticker <> '__CASH__'
    ),
    px_exact AS (
      SELECT trade_date, ticker, close
      FROM prices_daily
      WHERE trade_date BETWEEN ? AND ?
    ),
    pos_px AS (
      SELECT
        p.trade_date,
        p.version,
        p.ticker,
        p.shares,
        p.cash,
        e.close AS close_exact,
        (
          SELECT close FROM prices_daily x
          WHERE x.ticker = p.ticker
            AND x.trade_date <= p.trade_date
            AND x.close IS NOT NULL
          ORDER BY x.trade_date DESC
          LIMIT 1
        ) AS close_ffill
      FROM pos p
      LEFT JOIN px_exact e
        ON e.trade_date = p.trade_date AND e.ticker = p.ticker
    ),
    agg AS (
      SELECT
        trade_date,
        version,
        COUNT(*) AS n_pos,
        SUM(COALESCE(close_exact, close_ffill) * shares) AS mv_sum,
        MAX(cash) AS cash_max,
        SUM(CASE WHEN close_exact IS NULL THEN 1 ELSE 0 END) AS n_missing_exact,
        SUM(CASE WHEN close_ffill IS NULL THEN 1 ELSE 0 END) AS n_missing_ffill
      FROM pos_px
      GROUP BY 1,2
    )
    SELECT
      trade_date,
      version,
      n_pos,
      mv_sum,
      cash_max,
      n_missing_exact,
      n_missing_ffill,
      (COALESCE(mv_sum, 0) + COALESCE(cash_max, 0)) AS equity,
      (COALESCE(mv_sum, 0) + COALESCE(cash_max, 0)) / ? AS nav
    FROM agg
    ORDER BY trade_date;
    """
    df = con.execute(
        q,
        [version, start, end, start, end, float(initial_equity)],
    ).df()

    # Ensure full trading day index (some days may have no positions; treat as cash-only nav=1.0 for early INIT zone)
    # We'll expand with distinct trade dates from prices_daily within range
    days = con.execute(
        """
        SELECT DISTINCT trade_date
        FROM prices_daily
        WHERE trade_date BETWEEN ? AND ?
        ORDER BY trade_date
        """,
        [start, end],
    ).df()["trade_date"].tolist()

    base = pd.DataFrame({"trade_date": pd.to_datetime(days).dt.date})
    base["version"] = version
    out = base.merge(df, on=["trade_date", "version"], how="left")

    # If no positions rows, interpret as 100% cash (nav stays last, start at 1.0)
    out["nav"] = pd.to_numeric(out["nav"], errors="coerce")
    out["nav"] = out["nav"].fillna(method="ffill").fillna(1.0)

    out["equity"] = pd.to_numeric(out["equity"], errors="coerce")
    out["equity"] = out["equity"].fillna(out["nav"] * float(initial_equity))

    out["n_pos"] = pd.to_numeric(out["n_pos"], errors="coerce").fillna(0).astype(int)

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--versions", required=True)
    ap.add_argument("--topk", type=int, default=3)
    ap.add_argument("--cost_bps", type=float, default=0.0)
    ap.add_argument("--hold_last_signal", action="store_true")
    ap.add_argument("--initial_equity", type=float, default=100000.0)
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    versions = _parse_versions(args.versions)
    topk = int(args.topk)

    root = _project_root()
    db_path = _db_path(root)
    out_path = _out_path(root, start, end, topk)

    print("[OK] portfolio_nav (positions-based) started.")
    print(f"range: {start} to {end}")
    print(f"versions: {versions}")
    print(f"topk: {topk}")
    print(f"initial_equity: {float(args.initial_equity)}")
    print(f"db: {db_path}")

    con = duckdb.connect(str(db_path))

    records: List[pd.DataFrame] = []
    for v in versions:
        navdf = _nav_from_positions_ffill(con, v, start, end, float(args.initial_equity))
        # day_ret based on nav pct change
        navdf["day_ret"] = navdf["nav"].pct_change().fillna(0.0)
        navdf["day_ret_gross"] = navdf["day_ret"]

        # picks_trade_date: latest signal on or before trade_date (for display/contract)
        navdf["picks_trade_date"] = [
            _latest_signal_on_or_before(con, v, d) for d in navdf["trade_date"].tolist()
        ]
        # INIT rows: before first signal keep INIT
        first_sig = con.execute(
            "SELECT MIN(trade_date) FROM picks_daily WHERE version = ? AND trade_date BETWEEN ? AND ?",
            [v, start, end],
        ).fetchone()[0]
        if first_sig is not None:
            navdf.loc[navdf["trade_date"] < first_sig, "picks_trade_date"] = "INIT"
        else:
            navdf["picks_trade_date"] = "INIT"

        navdf["asof_date"] = navdf["trade_date"]
        navdf["turnover"] = 0.0
        navdf["cost_bps"] = float(args.cost_bps)
        navdf["cost"] = 0.0
        navdf["n_picks"] = navdf["n_pos"]
        navdf["n_valid"] = navdf["n_pos"]

        keep = [
            "trade_date", "picks_trade_date", "asof_date", "version",
            "day_ret_gross", "day_ret", "nav_gross", "nav",
            "turnover", "cost_bps", "cost", "n_picks", "n_valid"
        ]
        # align to prior CSV contract: nav_gross == nav when we don't model cost here
        navdf["nav_gross"] = navdf["nav"]
        out = navdf[[
            "trade_date", "picks_trade_date", "asof_date", "version",
            "day_ret_gross", "day_ret", "nav_gross", "nav",
            "turnover", "cost_bps", "cost", "n_picks", "n_valid"
        ]].copy()

        records.append(out)

    df = pd.concat(records, ignore_index=True)

    # write CSV
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date.astype(str)
    df["asof_date"] = pd.to_datetime(df["asof_date"]).dt.date.astype(str)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    # write nav_daily (overwrite range)
    df_db = df.copy()
    df_db["trade_date"] = pd.to_datetime(df_db["trade_date"]).dt.date
    df_db["asof_date"] = pd.to_datetime(df_db["asof_date"]).dt.date

    with con:
        con.execute("BEGIN;")
        try:
            for v in versions:
                con.execute(
                    "DELETE FROM nav_daily WHERE trade_date BETWEEN ? AND ? AND version = ?",
                    [start, end, v],
                )
            con.register("nav_df", df_db)
            con.execute(
                """
                INSERT INTO nav_daily (
                  trade_date, picks_trade_date, asof_date, version,
                  day_ret, nav, n_picks, n_valid,
                  day_ret_gross, nav_gross,
                  turnover, cost_bps, cost
                )
                SELECT
                  trade_date, picks_trade_date, asof_date, version,
                  day_ret, nav, n_picks, n_valid,
                  day_ret_gross, nav_gross,
                  turnover, cost_bps, cost
                FROM nav_df
                """
            )
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise

    # print last nav per version
    tail = df.groupby("version").tail(1)[["version", "trade_date", "nav"]]
    print("[OK] portfolio_nav (positions-based) passed.")
    print("nav_last:")
    print(tail.to_string(index=False))
    print(f"out: {out_path}")
    print(f"db:  {db_path}")

    con.close()


if __name__ == "__main__":
    main()
'''
# NOTE: This file intentionally overwrites portfolio_nav.py with a positions-based implementation.


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    target = root / "src" / "alpha_tracker2" / "pipelines" / "portfolio_nav.py"
    if not target.exists():
        raise FileNotFoundError(f"target not found: {target}")

    target.write_text(NEW_CODE, encoding="utf-8")
    print(f"[OK] patched: {target}")


if __name__ == "__main__":
    main()
