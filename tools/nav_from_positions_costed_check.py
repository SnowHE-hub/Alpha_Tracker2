from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def _get_trading_days(con: duckdb.DuckDBPyConnection, start: str, end: str) -> pd.DataFrame:
    df = con.execute(
        """
        SELECT DISTINCT trade_date
        FROM prices_daily
        WHERE trade_date BETWEEN ? AND ?
        ORDER BY trade_date
        """,
        [start, end],
    ).df()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def _load_positions(con: duckdb.DuckDBPyConnection, version: str, start: str, end: str) -> pd.DataFrame:
    df = con.execute(
        """
        SELECT asof_date, version, ticker, shares, cash
        FROM positions_daily
        WHERE version = ?
          AND asof_date BETWEEN ? AND ?
          AND ticker IS NOT NULL
          AND ticker <> '__CASH__'
        ORDER BY asof_date, ticker
        """,
        [version, start, end],
    ).df()
    if df.empty:
        return df
    df["asof_date"] = pd.to_datetime(df["asof_date"])
    df["ticker"] = df["ticker"].astype(str)
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0).astype(int)
    df["cash"] = pd.to_numeric(df["cash"], errors="coerce")
    return df


def _load_prices(con: duckdb.DuckDBPyConnection, tickers: list[str], start: str, end: str) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame(columns=["trade_date", "ticker", "close"])
    df = con.execute(
        """
        SELECT trade_date, ticker, close
        FROM prices_daily
        WHERE trade_date BETWEEN ? AND ?
          AND ticker = ANY(?)
        ORDER BY trade_date, ticker
        """,
        [start, end, tickers],
    ).df()
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["ticker"] = df["ticker"].astype(str)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df


def _equal_weight_turnover(prev: list[str], cur: list[str]) -> float:
    prev = list(dict.fromkeys(prev or []))
    cur = list(dict.fromkeys(cur or []))
    if not prev and not cur:
        return 0.0
    w0 = {t: (1.0 / len(prev)) for t in prev} if prev else {}
    w1 = {t: (1.0 / len(cur)) for t in cur} if cur else {}
    s = 0.0
    for tk in set(w0) | set(w1):
        s += abs(w1.get(tk, 0.0) - w0.get(tk, 0.0))
    return 0.5 * s


def build_positions_nav_costed(
    con: duckdb.DuckDBPyConnection,
    version: str,
    start: str,
    end: str,
    initial_equity: float,
    cost_bps: float,
) -> pd.DataFrame:
    cal = _get_trading_days(con, start, end)
    if cal.empty:
        raise RuntimeError(f"no trading days in prices_daily for {start}~{end}")

    pos = _load_positions(con, version, start, end)
    base = cal.copy()
    base["version"] = version

    if pos.empty:
        out = base.copy()
        out["nav_gross"] = 1.0
        out["day_ret_gross"] = 0.0
        out["turnover"] = 0.0
        out["cost_bps"] = float(cost_bps)
        out["cost"] = 0.0
        out["day_ret"] = 0.0
        out["nav"] = 1.0
        return out

    tickers = sorted(pos["ticker"].unique().tolist())
    px = _load_prices(con, tickers, start, end)

    # build full close table and ffill per ticker
    px_full = (
        base[["trade_date"]]
        .assign(_k=1)
        .merge(pd.DataFrame({"ticker": tickers, "_k": 1}), on="_k", how="outer")
        .drop(columns="_k")
    )
    px_full = px_full.merge(px, on=["trade_date", "ticker"], how="left")
    px_full["close_ffill"] = px_full.groupby("ticker")["close"].ffill()

    p = pos.rename(columns={"asof_date": "trade_date"}).copy()
    p = p.merge(px_full[["trade_date", "ticker", "close_ffill"]], on=["trade_date", "ticker"], how="left")
    p["mv_ffill"] = p["shares"] * p["close_ffill"]

    daily = (
        p.groupby(["trade_date", "version"], as_index=False)
        .agg(
            n_picks=("ticker", "count"),
            mv_sum=("mv_ffill", "sum"),
            cash_max=("cash", "max"),
        )
    )
    daily["cash_max"] = pd.to_numeric(daily["cash_max"], errors="coerce").fillna(0.0)
    daily["equity_gross"] = daily["mv_sum"] + daily["cash_max"]
    daily["nav_gross"] = daily["equity_gross"] / float(initial_equity)

    out = base.merge(daily[["trade_date", "version", "nav_gross", "n_picks"]], on=["trade_date", "version"], how="left")
    out["nav_gross"] = out["nav_gross"].ffill().fillna(1.0)
    out["n_picks"] = pd.to_numeric(out["n_picks"], errors="coerce").fillna(0).astype(int)
    out["day_ret_gross"] = out["nav_gross"].pct_change().fillna(0.0)

    holdings_map = (
        p.groupby("trade_date")["ticker"]
        .apply(lambda s: sorted(s.astype(str).tolist()))
        .to_dict()
    )
    prev_hold = []
    turnovers = []
    for d in out["trade_date"].tolist():
        cur_hold = holdings_map.get(d, [])
        tv = _equal_weight_turnover(prev_hold, cur_hold)
        turnovers.append(float(tv))
        prev_hold = cur_hold

    out["turnover"] = turnovers
    out["cost_bps"] = float(cost_bps)
    out["cost"] = out["turnover"] * (out["cost_bps"] / 10000.0)
    out["day_ret"] = out["day_ret_gross"] - out["cost"]

    nav_net = []
    cur = 1.0
    for r in out["day_ret"].tolist():
        cur *= (1.0 + float(r))
        nav_net.append(cur)
    out["nav"] = nav_net

    return out


def load_nav_daily(con: duckdb.DuckDBPyConnection, version: str, start: str, end: str) -> pd.DataFrame:
    df = con.execute(
        """
        SELECT trade_date, version, nav, nav_gross, day_ret, day_ret_gross, turnover, cost_bps, cost
        FROM nav_daily
        WHERE version = ?
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date
        """,
        [version, start, end],
    ).df()
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(Path("data/store/alpha_tracker.duckdb")))
    ap.add_argument("--version", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--initial_equity", type=float, default=100000.0)
    ap.add_argument("--cost_bps", type=float, default=10.0)
    ap.add_argument("--topn", type=int, default=20)
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"db not found: {db_path}")

    con = duckdb.connect(str(db_path))

    df_exec = build_positions_nav_costed(
        con=con,
        version=args.version,
        start=args.start,
        end=args.end,
        initial_equity=float(args.initial_equity),
        cost_bps=float(args.cost_bps),
    )
    df_nav = load_nav_daily(con, args.version, args.start, args.end)

    if df_nav.empty:
        raise RuntimeError("nav_daily has no rows in the range; run portfolio_nav_positions_costed first")

    m = df_nav.merge(
        df_exec[["trade_date", "version", "nav", "nav_gross", "day_ret", "day_ret_gross", "turnover", "cost"]],
        on=["trade_date", "version"],
        how="inner",
        suffixes=("_nav", "_exec"),
    )

    for c in ["nav_nav", "nav_exec", "nav_gross_nav", "nav_gross_exec"]:
        m[c] = pd.to_numeric(m[c], errors="coerce")

    m["abs_diff_nav"] = (m["nav_exec"] - m["nav_nav"]).abs()
    m["abs_diff_gross"] = (m["nav_gross_exec"] - m["nav_gross_nav"]).abs()

    max_abs = float(m["abs_diff_nav"].max())
    worst = m.sort_values("abs_diff_nav", ascending=False).head(1)

    print(f"[DB] {db_path.resolve()}")
    print(f"[RANGE] {args.start} ~ {args.end}  version={args.version}  initial={args.initial_equity}  cost_bps={args.cost_bps}")
    print("\n=== NAV compare (nav_daily vs exec-from-positions, costed) ===")
    print(f"rows_merged: {len(m)}")
    print(f"max_abs_diff(nav): {max_abs:.12f}")
    if not worst.empty:
        w = worst.iloc[0]
        print(f"worst_date: {w['trade_date'].date()}  nav_exec: {w['nav_exec']:.6f}  nav_nav: {w['nav_nav']:.6f}  abs: {w['abs_diff_nav']:.6f}")

    show = m.sort_values("trade_date")[
        [
            "trade_date",
            "nav_nav",
            "nav_exec",
            "abs_diff_nav",
            "nav_gross_nav",
            "nav_gross_exec",
            "abs_diff_gross",
            "turnover_nav",
            "cost_nav",
            "day_ret_nav",
            "day_ret_exec",
        ]
    ]
    print("\n=== Window detail ===")
    show["trade_date"] = show["trade_date"].dt.date.astype(str)
    print(show.to_string(index=False))

    con.close()


if __name__ == "__main__":
    main()
