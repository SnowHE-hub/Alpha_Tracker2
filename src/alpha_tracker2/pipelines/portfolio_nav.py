from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import List, Tuple

import duckdb
import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.strategy_id import parse_strategy_id


ROOT = Path(__file__).resolve().parents[3]


def _parse_date(s: str) -> date:
    return pd.to_datetime(s).date()


def _parse_versions(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _parse_strategy_ids(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _get_store_db_path(project_root: Path, settings) -> Path:
    """
    统一从 Settings.paths.store_db 取 DuckDB 路径。
    兼容：
      - paths 是 dict
      - paths 是对象
    """
    paths = getattr(settings, "paths", None)
    store_db = None

    if isinstance(paths, dict):
        store_db = paths.get("store_db") or paths.get("store_db_path") or paths.get("db")
    else:
        store_db = getattr(paths, "store_db", None) or getattr(paths, "store_db_path", None) or getattr(paths, "db", None)

    if not store_db:
        # 兜底（与你 default.yaml 一致）
        store_db = "data/store/alpha_tracker.duckdb"

    return (project_root / str(store_db)).resolve()


def _latest_signal_on_or_before(con: duckdb.DuckDBPyConnection, version: str, d: date) -> str:
    row = con.execute(
        """
        SELECT trade_date
        FROM picks_daily
        WHERE version = ? AND trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        [version, d],
    ).fetchone()
    if not row or row[0] is None:
        return ""
    return str(row[0])

def _turnover_from_positions(
    con: duckdb.DuckDBPyConnection,
    strategy_id: str,
    start: date,
    end: date,
) -> pd.DataFrame:
    """
    Reconstruct daily turnover from positions_daily using market_value + cash.
    turnover_t = 0.5 * sum_i |mv_i(t) - mv_i(t-1)| / equity(t-1)
    (include cash as a pseudo-ticker '__CASH__')
    """
    q = """
    WITH pos AS (
      SELECT
        asof_date AS trade_date,
        strategy_id,
        ticker,
        CAST(market_value AS DOUBLE) AS mv
      FROM positions_daily
      WHERE strategy_id = ?
        AND asof_date BETWEEN ? AND ?
        AND ticker <> '__CASH__'
    ),
    cash AS (
      SELECT
        asof_date AS trade_date,
        strategy_id,
        '__CASH__' AS ticker,
        CAST(MAX(cash) AS DOUBLE) AS mv
      FROM positions_daily
      WHERE strategy_id = ?
        AND asof_date BETWEEN ? AND ?
      GROUP BY 1,2
    ),
    all_pos AS (
      SELECT * FROM pos
      UNION ALL
      SELECT * FROM cash
    ),
    pos_lag AS (
      SELECT
        trade_date,
        strategy_id,
        ticker,
        mv,
        LAG(mv) OVER (PARTITION BY strategy_id, ticker ORDER BY trade_date) AS mv_prev
      FROM all_pos
    ),
    equity AS (
      SELECT
        trade_date,
        strategy_id,
        SUM(mv) AS equity
      FROM all_pos
      GROUP BY 1,2
    ),
    equity_lag AS (
      SELECT
        trade_date,
        strategy_id,
        equity,
        LAG(equity) OVER (PARTITION BY strategy_id ORDER BY trade_date) AS equity_prev
      FROM equity
    ),
    diff_sum AS (
      SELECT
        p.trade_date,
        p.strategy_id,
        SUM(ABS(p.mv - COALESCE(p.mv_prev, p.mv))) AS abs_diff_sum
      FROM pos_lag p
      GROUP BY 1,2
    )
    SELECT
      e.trade_date,
      e.strategy_id,
      CASE
        WHEN e.equity_prev IS NULL OR e.equity_prev = 0 THEN 0.0
        ELSE 0.5 * d.abs_diff_sum / e.equity_prev
      END AS turnover_recon
    FROM equity_lag e
    LEFT JOIN diff_sum d
      ON e.trade_date = d.trade_date AND e.strategy_id = d.strategy_id
    ORDER BY e.trade_date;
    """
    df = con.execute(
        q,
        [strategy_id, start, end, strategy_id, start, end],
    ).df()
    if not df.empty:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df["turnover_recon"] = df["turnover_recon"].astype(float)
    return df


def _nav_from_positions_ffill_by_strategy(
    con: duckdb.DuckDBPyConnection,
    strategy_id: str,
    version: str,
    start: date,
    end: date,
    initial_equity: float,
) -> pd.DataFrame:
    q = """
    WITH pos AS (
      SELECT
        asof_date AS trade_date,
        strategy_id,
        version,
        ticker,
        shares,
        cash
      FROM positions_daily
      WHERE strategy_id = ? AND asof_date BETWEEN ? AND ?
        AND ticker <> '__CASH__'
    ),
    px_exact AS (
      SELECT trade_date, ticker, close AS close_exact
      FROM prices_daily
      WHERE trade_date BETWEEN ? AND ?
    ),

    px_ffill AS (
        SELECT
            trade_date,
            ticker,
            max_by(close, trade_date) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS close_ffill
        FROM prices_daily
        WHERE trade_date BETWEEN ? AND ?
    ),

    pos_px AS (
      SELECT
        p.trade_date,
        p.strategy_id,
        p.version,
        p.ticker,
        p.shares,
        p.cash,
        e.close_exact,
        f.close_ffill,
        COALESCE(e.close_exact, f.close_ffill) AS close_use
      FROM pos p
      LEFT JOIN px_exact e
        ON p.trade_date = e.trade_date AND p.ticker = e.ticker
      LEFT JOIN px_ffill f
        ON p.trade_date = f.trade_date AND p.ticker = f.ticker
    ),
    agg AS (
      SELECT
        trade_date,
        strategy_id,
        version,
        COUNT(*) AS n_pos,
        SUM(COALESCE(close_use, 0) * shares) AS mv_sum,
        MAX(cash) AS cash_max,
        SUM(CASE WHEN close_exact IS NULL THEN 1 ELSE 0 END) AS n_missing_exact,
        SUM(CASE WHEN close_ffill IS NULL THEN 1 ELSE 0 END) AS n_missing_ffill
      FROM pos_px
      GROUP BY 1,2,3
    )
    SELECT
      trade_date,
      strategy_id,
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
        [strategy_id, start, end, start, end, start, end, float(initial_equity)],
    ).df()

    # expand to all trading dates in range
    all_days = con.execute(
        """
        SELECT DISTINCT trade_date
        FROM prices_daily
        WHERE trade_date BETWEEN ? AND ?
        ORDER BY trade_date
        """,
        [start, end],
    ).df()

    if not all_days.empty:
        all_days["trade_date"] = pd.to_datetime(all_days["trade_date"]).dt.date
        if not df.empty:
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df = all_days.merge(df, on="trade_date", how="left")
        df["strategy_id"] = strategy_id
        df["version"] = version
        df["nav"] = df["nav"].fillna(1.0)
        df["equity"] = df["equity"].fillna(float(initial_equity))
        df["n_pos"] = df["n_pos"].fillna(0).astype(int)

    return df


def _nav_from_positions_ffill_by_version(
    con: duckdb.DuckDBPyConnection,
    version: str,
    start: date,
    end: date,
    initial_equity: float,
) -> pd.DataFrame:
    q = """
    WITH pos AS (
      SELECT
        asof_date AS trade_date,
        version,
        ticker,
        shares,
        cash
      FROM positions_daily
      WHERE version = ? AND asof_date BETWEEN ? AND ?
        AND ticker <> '__CASH__'
    ),
    px_exact AS (
      SELECT trade_date, ticker, close AS close_exact
      FROM prices_daily
      WHERE trade_date BETWEEN ? AND ?
    ),

    px_ffill AS (
    SELECT
        trade_date,
        ticker,
        max_by(close, trade_date) OVER (
        PARTITION BY ticker ORDER BY trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS close_ffill
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
        e.close_exact,
        f.close_ffill,
        COALESCE(e.close_exact, f.close_ffill) AS close_use
      FROM pos p
      LEFT JOIN px_exact e
        ON p.trade_date = e.trade_date AND p.ticker = e.ticker
      LEFT JOIN px_ffill f
        ON p.trade_date = f.trade_date AND p.ticker = f.ticker
    ),
    agg AS (
      SELECT
        trade_date,
        version,
        COUNT(*) AS n_pos,
        SUM(COALESCE(close_use, 0) * shares) AS mv_sum,
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
        [version, start, end, start, end, start, end, float(initial_equity)],
    ).df()

    # expand to all trading dates in range
    all_days = con.execute(
        """
        SELECT DISTINCT trade_date
        FROM prices_daily
        WHERE trade_date BETWEEN ? AND ?
        ORDER BY trade_date
        """,
        [start, end],
    ).df()

    if not all_days.empty:
        all_days["trade_date"] = pd.to_datetime(all_days["trade_date"]).dt.date
        if not df.empty:
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df = all_days.merge(df, on="trade_date", how="left")
        df["version"] = version
        df["nav"] = df["nav"].fillna(1.0)
        df["equity"] = df["equity"].fillna(float(initial_equity))
        df["n_pos"] = df["n_pos"].fillna(0).astype(int)

    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)

    # legacy
    ap.add_argument("--versions", required=False, default="")
    ap.add_argument("--topk", type=int, default=3)

    # NEW: strategy_ids preferred
    ap.add_argument("--strategy_ids", required=False, default="", help="comma-separated strategy_id list")

    ap.add_argument("--cost_bps", type=float, default=0.0)
    ap.add_argument("--initial_equity", type=float, default=100000.0)

    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)

    strategy_ids = _parse_strategy_ids(args.strategy_ids)
    versions = _parse_versions(args.versions)

    if strategy_ids:
        specs = [parse_strategy_id(sid) for sid in strategy_ids]
        plan: List[Tuple[str | None, str]] = [
            (sid, sp.model_version) for sid, sp in zip(strategy_ids, specs)
        ]
    else:
        if not versions:
            raise ValueError("Either --strategy_ids or --versions must be provided.")
        plan = [(None, v) for v in versions]

    # ✅ 统一从 Settings.paths.store_db 取 DB
    settings = load_settings(ROOT)
    db_path = _get_store_db_path(ROOT, settings)

    con = duckdb.connect(str(db_path))

    records: List[pd.DataFrame] = []

    for sid, v in plan:
        if sid:
            navdf = _nav_from_positions_ffill_by_strategy(
                con, sid, v, start, end, float(args.initial_equity)
            )
        else:
            navdf = _nav_from_positions_ffill_by_version(
                con, v, start, end, float(args.initial_equity)
            )
            navdf["strategy_id"] = f"{v}__LEGACY__H0__TOP{int(args.topk)}__C0"

        navdf["trade_date"] = pd.to_datetime(navdf["trade_date"]).dt.date

        # returns
        navdf["day_ret"] = navdf["nav"].pct_change().fillna(0.0)
        navdf["day_ret_gross"] = navdf["day_ret"]
        navdf["nav_gross"] = navdf["nav"]

        # picks_trade_date
        navdf["picks_trade_date"] = [
            _latest_signal_on_or_before(con, v, d) for d in navdf["trade_date"].tolist()
        ]

        # before first signal: INIT
        first_sig = con.execute(
            "SELECT MIN(trade_date) FROM picks_daily WHERE version = ? AND trade_date BETWEEN ? AND ?",
            [v, start, end],
        ).fetchone()[0]

        if first_sig is not None:
            first_sig = pd.to_datetime(first_sig).date()
            navdf.loc[navdf["trade_date"] < first_sig, "picks_trade_date"] = "INIT"
        else:
            navdf["picks_trade_date"] = "INIT"

        navdf["asof_date"] = navdf["trade_date"]

        # --- turnover/cost from positions (source of truth) ---
        tdf = _turnover_from_positions(con, navdf["strategy_id"].iloc[0], start, end)
        navdf = navdf.merge(tdf[["trade_date", "turnover_recon"]], on="trade_date", how="left")
        navdf["turnover"] = navdf["turnover_recon"].fillna(0.0).astype(float)
        navdf.drop(columns=["turnover_recon"], inplace=True, errors="ignore")

        navdf["cost_bps"] = float(args.cost_bps)
        navdf["cost"] = navdf["turnover"] * (navdf["cost_bps"] / 10000.0)

        navdf["n_picks"] = navdf["n_pos"]
        navdf["n_valid"] = navdf["n_pos"]

        keep = [
            "trade_date",
            "picks_trade_date",
            "asof_date",
            "version",
            "strategy_id",
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
        records.append(navdf[keep].copy())

    df_db = pd.concat(records, ignore_index=True) if records else pd.DataFrame()

    # write into nav_daily (idempotent per trade_date+strategy_id)
    con.execute("BEGIN;")
    try:
        if not df_db.empty:
            if strategy_ids:
                for sid in strategy_ids:
                    con.execute(
                        "DELETE FROM nav_daily WHERE trade_date BETWEEN ? AND ? AND strategy_id = ?",
                        [start, end, sid],
                    )
            else:
                for v in versions:
                    con.execute(
                        "DELETE FROM nav_daily WHERE trade_date BETWEEN ? AND ? AND version = ?",
                        [start, end, v],
                    )

            con.register("nav_df", df_db)
            con.execute(
                """
                INSERT INTO nav_daily (
                  trade_date, picks_trade_date, asof_date, version, strategy_id,
                  day_ret, nav, n_picks, n_valid,
                  day_ret_gross, nav_gross,
                  turnover, cost_bps, cost
                )
                SELECT
                  trade_date, picks_trade_date, asof_date, version, strategy_id,
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

    out_dir = ROOT / "data" / "out" / "nav"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"nav_daily_{start}_{end}.csv"
    df_db.to_csv(out_path, index=False, encoding="utf-8-sig")
    print("[OK] nav exported:", out_path)


if __name__ == "__main__":
    main()
