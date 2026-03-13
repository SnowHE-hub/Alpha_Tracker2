from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def _get_trading_days(con: duckdb.DuckDBPyConnection, start: str, end: str) -> list[pd.Timestamp]:
    df = con.execute(
        """
        SELECT DISTINCT trade_date
        FROM prices_daily
        WHERE trade_date BETWEEN ? AND ?
        ORDER BY trade_date
        """,
        [start, end],
    ).df()
    if df.empty:
        return []
    return pd.to_datetime(df["trade_date"]).sort_values().tolist()


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


def build_nav_positions_costed(
    con: duckdb.DuckDBPyConnection,
    version: str,
    start: str,
    end: str,
    initial_equity: float,
    cost_bps: float,
) -> pd.DataFrame:
    days = _get_trading_days(con, start, end)
    if len(days) == 0:
        raise RuntimeError(f"no trading days in prices_daily for {start}~{end}")

    pos = _load_positions(con, version, start, end)

    # base frame (trading days)
    base = pd.DataFrame({"trade_date": pd.to_datetime(days)})
    base["asof_date"] = base["trade_date"]
    base["version"] = version

    # If no positions at all, output flat NAV=1
    if pos.empty:
        out = base.copy()
        out["nav_gross"] = 1.0
        out["day_ret_gross"] = 0.0
        out["turnover"] = 0.0
        out["cost_bps"] = float(cost_bps)
        out["cost"] = 0.0
        out["day_ret"] = 0.0
        out["nav"] = 1.0
        out["picks_trade_date"] = "INIT"
        out["n_picks"] = 0
        out["n_valid"] = 0
        return out

    tickers = sorted(pos["ticker"].unique().tolist())
    px = _load_prices(con, tickers, start, end)

    # build close table aligned to trading days with ffill per ticker
    px_full = (
        base[["trade_date"]]
        .assign(_k=1)
        .merge(pd.DataFrame({"ticker": tickers, "_k": 1}), on="_k", how="outer")
        .drop(columns="_k")
    )
    px_full = px_full.merge(px, on=["trade_date", "ticker"], how="left")
    px_full["close_ffill"] = px_full.groupby("ticker")["close"].ffill()

    # join positions with closes on asof_date=trade_date
    p = pos.rename(columns={"asof_date": "trade_date"}).copy()
    p = p.merge(px_full[["trade_date", "ticker", "close", "close_ffill"]], on=["trade_date", "ticker"], how="left")

    # market value by ffill close
    p["mv_ffill"] = p["shares"] * p["close_ffill"]

    # daily equity
    daily = (
        p.groupby(["trade_date", "version"], as_index=False)
        .agg(
            n_picks=("ticker", "count"),
            n_missing_exact=("close", lambda s: int(pd.isna(s).sum())),
            n_missing_ffill=("close_ffill", lambda s: int(pd.isna(s).sum())),
            mv_sum=("mv_ffill", "sum"),
            cash_max=("cash", "max"),
        )
    )
    daily["cash_max"] = pd.to_numeric(daily["cash_max"], errors="coerce").fillna(0.0)
    daily["equity_gross"] = daily["mv_sum"] + daily["cash_max"]
    daily["nav_gross"] = daily["equity_gross"] / float(initial_equity)

    # merge into base (ensures every trading day exists)
    out = base.merge(daily[["trade_date", "version", "nav_gross", "n_picks", "n_missing_ffill"]], on=["trade_date", "version"], how="left")
    out["nav_gross"] = out["nav_gross"].ffill().fillna(1.0)
    out["n_picks"] = pd.to_numeric(out["n_picks"], errors="coerce").fillna(0).astype(int)
    out["n_valid"] = out["n_picks"]  # ffill后我们当作可用；若你想更严格，可改成 n_picks - n_missing_ffill

    # day_ret_gross from nav_gross
    out["day_ret_gross"] = out["nav_gross"].pct_change().fillna(0.0)

    # turnover from holdings change (equal-weight, based on tickers list each day)
    holdings_map = (
        p.groupby("trade_date")["ticker"]
        .apply(lambda s: sorted(s.astype(str).tolist()))
        .to_dict()
    )

    prev_hold = []
    turnovers = []
    picks_td = []
    last_pick = "INIT"

    for d in out["trade_date"].tolist():
        cur_hold = holdings_map.get(d, [])
        tv = _equal_weight_turnover(prev_hold, cur_hold)
        turnovers.append(float(tv))

        # picks_trade_date：发生换仓(建仓)的那天记为当日，否则沿用上一次
        if tv > 0:
            last_pick = str(pd.to_datetime(d).date())
        picks_td.append(last_pick)

        prev_hold = cur_hold

    out["turnover"] = turnovers
    out["cost_bps"] = float(cost_bps)
    out["cost"] = out["turnover"] * (out["cost_bps"] / 10000.0)

    # net nav (iterative): nav_t = nav_{t-1} * (1 + day_ret_gross - cost_on_rebalance_day)
    out["day_ret"] = out["day_ret_gross"] - out["cost"]
    nav_net = []
    cur = 1.0
    for r in out["day_ret"].tolist():
        cur *= (1.0 + float(r))
        nav_net.append(cur)
    out["nav"] = nav_net

    out["picks_trade_date"] = picks_td

    # final typing
    out["trade_date"] = out["trade_date"].dt.date.astype(str)
    out["asof_date"] = out["asof_date"].dt.date.astype(str)
    out["version"] = out["version"].astype(str)

    for c in ["day_ret_gross", "day_ret", "nav_gross", "nav", "turnover", "cost_bps", "cost"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    for c in ["n_picks", "n_valid"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)

    return out[
        [
            "trade_date",
            "picks_trade_date",
            "asof_date",
            "version",
            "day_ret_gross",
            "day_ret",
            "nav_gross",
            "nav",
            "turnover",
            "cost_bps",
            "cost",
            "n_picks",
            "n_valid",
        ]
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(Path("data/store/alpha_tracker.duckdb")))
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--versions", required=True, help="comma separated, e.g. ENS")
    ap.add_argument("--topk", type=int, default=3, help="only used in output filename")
    ap.add_argument("--initial_equity", type=float, default=100000.0)
    ap.add_argument("--cost_bps", type=float, default=0.0)
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"db not found: {db_path}")

    versions = [x.strip() for x in args.versions.split(",") if x.strip()]
    if not versions:
        raise ValueError("no versions parsed")

    con = duckdb.connect(str(db_path))
    out_all = []
    for v in versions:
        navdf = build_nav_positions_costed(
            con=con,
            version=v,
            start=args.start,
            end=args.end,
            initial_equity=float(args.initial_equity),
            cost_bps=float(args.cost_bps),
        )
        out_all.append(navdf)

    df = pd.concat(out_all, ignore_index=True)

    # write CSV
    out_dir = Path("data/out")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"portfolio_nav_{args.start}_{args.end}_top{args.topk}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    # write back to nav_daily
    con.execute("ALTER TABLE nav_daily ADD COLUMN IF NOT EXISTS day_ret_gross DOUBLE;")
    con.execute("ALTER TABLE nav_daily ADD COLUMN IF NOT EXISTS nav_gross DOUBLE;")
    con.execute("ALTER TABLE nav_daily ADD COLUMN IF NOT EXISTS turnover DOUBLE;")
    con.execute("ALTER TABLE nav_daily ADD COLUMN IF NOT EXISTS cost_bps DOUBLE;")
    con.execute("ALTER TABLE nav_daily ADD COLUMN IF NOT EXISTS cost DOUBLE;")

    con.execute("BEGIN;")
    try:
        for v in versions:
            con.execute(
                "DELETE FROM nav_daily WHERE trade_date BETWEEN ? AND ? AND version = ?",
                [args.start, args.end, v],
            )

        df_db = df.copy()
        df_db["trade_date"] = pd.to_datetime(df_db["trade_date"]).dt.date
        df_db["asof_date"] = pd.to_datetime(df_db["asof_date"]).dt.date
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
    finally:
        con.close()

    # print last nav
    last = (
        df.sort_values(["version", "trade_date"])
        .groupby("version", as_index=False)
        .tail(1)[["version", "trade_date", "nav", "nav_gross"]]
    )
    print("[OK] portfolio_nav_positions_costed finished.")
    print(last.to_string(index=False))
    print(f"out: {out_path.resolve()}")
    print(f"db:  {db_path.resolve()}")


if __name__ == "__main__":
    main()
