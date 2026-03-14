"""
Build minimal NAV from picks_daily (equal-weight top-K per version) and prices_daily.adj_close;
write nav_daily. Idempotent over the written date range.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import List

import pandas as pd
import yaml

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _find_project_root(start: Path) -> Path:
    current = start
    for parent in [current, *current.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    raise RuntimeError("Could not locate project root containing configs/default.yaml")


def _resolve_versions(arg_versions: str | None, project_root: Path) -> List[str]:
    if arg_versions:
        return [v.strip().upper() for v in arg_versions.split(",") if v.strip()]
    cfg_path = project_root / "configs" / "default.yaml"
    if cfg_path.is_file():
        with cfg_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        scoring_cfg = raw.get("scoring") or {}
        versions = scoring_cfg.get("score_versions")
        if isinstance(versions, list) and versions:
            return [str(v).upper() for v in versions]
    return ["V1", "V2", "V3", "V4"]


def _load_topk_picks(
    store: DuckDBStore,
    trade_date: date,
    version: str,
    topk: int,
) -> List[str]:
    """Return up to topk tickers for (trade_date, version) ordered by rank."""
    rows = store.fetchall(
        """
        SELECT ticker
        FROM picks_daily
        WHERE trade_date = ? AND version = ?
        ORDER BY rank ASC NULLS LAST
        LIMIT ?
        """,
        [trade_date.isoformat(), version, topk],
    )
    return [str(r[0]) for r in rows]


def _load_prices(
    store: DuckDBStore,
    dates: List[date],
    tickers: List[str],
) -> pd.DataFrame:
    """Load adj_close for (date, ticker) for the given dates and tickers."""
    if not dates or not tickers:
        return pd.DataFrame(columns=["trade_date", "ticker", "adj_close"])
    date_strs = [d.isoformat() for d in dates]
    placeholders_d = ",".join(["?"] * len(date_strs))
    placeholders_t = ",".join(["?"] * len(tickers))
    params: list[object] = [*date_strs, *tickers]
    rows = store.fetchall(
        f"""
        SELECT trade_date, ticker, adj_close
        FROM prices_daily
        WHERE trade_date IN ({placeholders_d})
          AND ticker IN ({placeholders_t})
        """,
        params,
    )
    if not rows:
        return pd.DataFrame(columns=["trade_date", "ticker", "adj_close"])
    df = pd.DataFrame(rows, columns=["trade_date", "ticker", "adj_close"])
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["ticker"] = df["ticker"].astype(str)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute equal-weight NAV from picks and write nav_daily.",
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="Start date YYYY-MM-DD",
    )
    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="End date YYYY-MM-DD",
    )
    parser.add_argument(
        "--versions",
        type=str,
        default=None,
        help="Comma-separated versions e.g. V1,V2; default from config",
    )
    parser.add_argument(
        "--topk",
        type=int,
        default=3,
        help="Number of top picks per version (default 3)",
    )
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if end < start:
        raise ValueError("--end must be >= --start")

    project_root = _find_project_root(Path(__file__).resolve())
    settings = load_settings(project_root)
    store = DuckDBStore(
        db_path=settings.store_db,
        schema_path=project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()
    cal = TradingCalendar()

    trading_days = cal.trading_days(start, end, market="US")
    if not trading_days:
        print("portfolio_nav: no trading days in range")
        return

    versions = _resolve_versions(args.versions, project_root)
    portfolios = [f"{v}_top{args.topk}" for v in versions]

    # Idempotent: delete nav_daily in [start, end] for these portfolios
    placeholders = ",".join(["?"] * len(portfolios))
    params_del: list[object] = [start.isoformat(), end.isoformat(), *portfolios]
    store.exec(
        f"""
        DELETE FROM nav_daily
        WHERE trade_date BETWEEN ? AND ?
          AND portfolio IN ({placeholders})
        """,
        params_del,
    )

    # Extend calendar backwards to get previous trading day for first day in range
    start_ext = start - timedelta(days=60)
    all_days = cal.trading_days(start_ext, end, market="US")
    day_to_prev: dict[date, date | None] = {}
    for i, d in enumerate(all_days):
        if d < start:
            continue
        if d > end:
            break
        prev = all_days[i - 1] if i > 0 else None
        day_to_prev[d] = prev

    rows_to_insert: List[tuple] = []

    for version in versions:
        portfolio_id = f"{version}_top{args.topk}"
        prev_nav = 1.0

        for d in trading_days:
            tickers = _load_topk_picks(store, d, version, args.topk)
            prev_d = day_to_prev.get(d)

            if not tickers:
                # No picks: keep nav unchanged, ret = 0
                ret_d = 0.0
                nav_d = prev_nav
            elif prev_d is None:
                # First trading day in data: use same-day return 0 or skip
                ret_d = 0.0
                nav_d = prev_nav
            else:
                prices_df = _load_prices(store, [prev_d, d], tickers)
                if prices_df.empty:
                    ret_d = 0.0
                    nav_d = prev_nav
                else:
                    p_prev = prices_df[prices_df["trade_date"] == prev_d].set_index("ticker")["adj_close"]
                    p_curr = prices_df[prices_df["trade_date"] == d].set_index("ticker")["adj_close"]
                    rets = []
                    for t in tickers:
                        if t in p_prev.index and t in p_curr.index:
                            p0 = float(p_prev[t])
                            p1 = float(p_curr[t])
                            if p0 and p0 > 0:
                                rets.append((p1 / p0) - 1.0)
                    if not rets:
                        ret_d = 0.0
                    else:
                        ret_d = sum(rets) / len(rets)
                    nav_d = prev_nav * (1.0 + ret_d)

            rows_to_insert.append((d.isoformat(), portfolio_id, nav_d, ret_d))
            prev_nav = nav_d

    if rows_to_insert:
        with store.session() as conn:
            conn.executemany(
                """
                INSERT INTO nav_daily (trade_date, portfolio, nav, ret)
                VALUES (?, ?, ?, ?)
                """,
                rows_to_insert,
            )
        print(f"portfolio_nav: start={start} end={end} versions={versions} topk={args.topk} wrote_rows={len(rows_to_insert)}")
    else:
        print("portfolio_nav: no rows to write")

    print(f"portfolio_nav: db={settings.store_db}")


if __name__ == "__main__":
    main()
