from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import numpy as np
import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.storage.duckdb_store import DuckDBStore


# ====== constants aligned to old script (v23/v4 needs these) ======
PRED_HORIZON_DAYS = 5
HOLD_DAYS = 10
ROLLING_LOOKBACK_DAYS = 140


def _project_root_from_here() -> Path:
    # src/alpha_tracker2/pipelines/build_features.py -> project root
    return Path(__file__).resolve().parents[3]


def _parse_args():
    ap = argparse.ArgumentParser(description="Build features_daily for a given trade_date.")

    ap.add_argument("--date", type=str, default=None, help="trade_date YYYY-MM-DD (default: latest trading day)")
    ap.add_argument(
        "--tickers",
        default=None,
        help="optional: comma separated tickers (override universe), e.g. 000001.SZ,000002.SZ",
    )

    # universe-source: allow passing actual table name like universe_daily_hot
    ap.add_argument(
        "--universe-source",
        dest="universe_source",
        default=None,
        help="universe table name in DuckDB, e.g. universe_daily_hot. If omitted, fallback to picks_daily version=UNIVERSE.",
    )

    ap.add_argument("--limit", type=int, default=300, help="if using universe: take top N (default=300)")

    # IMPORTANT: overwrite-by-date to avoid mixed universes
    ap.add_argument(
        "--purge-date",
        dest="purge_date",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="purge ALL rows of this trade_date before insert (default: true).",
    )

    return ap.parse_args()


def max_drawdown_from_nav(nav: np.ndarray) -> float:
    """Return min drawdown (negative), computed on nav series."""
    if nav.size == 0:
        return float("nan")
    peak = np.maximum.accumulate(nav)
    dd = nav / peak - 1.0
    return float(np.min(dd))


def rolling_backtest(prices: np.ndarray, horizon: int, dca_n: Optional[int], lookback: int) -> Dict[str, float]:
    """Exact port from old four_version_compare.py"""
    p = prices.astype(float)
    n = p.size
    if n < horizon + 40:
        return {}
    start_i = max(0, n - lookback - horizon - 1)
    end_i = n - horizon - 1
    if end_i <= start_i:
        return {}

    rets = []
    mdds = []
    for i in range(start_i, end_i + 1):
        end = i + horizon
        window = p[i : end + 1]
        if window.size < horizon + 1:
            continue

        if dca_n is None:
            cost = window[0]
        else:
            k = min(dca_n, window.size)
            cost = float(np.mean(window[:k]))

        final = window[-1]
        r = final / cost - 1.0
        rets.append(r)

        nav = window / cost
        mdd = max_drawdown_from_nav(nav)
        mdds.append(mdd)

    if not rets:
        return {}

    rets = np.array(rets, dtype=float)
    mdds = np.array(mdds, dtype=float)

    return {
        "bt_mean": float(np.mean(rets)),
        "bt_median": float(np.median(rets)),
        "bt_winrate": float(np.mean(rets > 0)),
        "bt_p10": float(np.percentile(rets, 10)),
        "bt_worst": float(np.min(rets)),
        "bt_avg_mdd": float(np.mean(mdds)),
        "bt_worst_mdd": float(np.min(mdds)),
    }


def choose_best_style(prices: np.ndarray) -> Tuple[str, Dict[str, float]]:
    styles = [
        ("LUMP", None),
        ("DCA5", 5),
        ("DCA10", 10),
    ]
    best_name = "LUMP"
    best = {}
    best_mean = -1e18
    best_win = -1e18

    for name, dca_n in styles:
        m = rolling_backtest(prices, horizon=HOLD_DAYS, dca_n=dca_n, lookback=ROLLING_LOOKBACK_DAYS)
        if not m:
            continue
        mean = m.get("bt_mean", -1e18)
        win = m.get("bt_winrate", -1e18)
        if (mean > best_mean) or (mean == best_mean and win > best_win):
            best_name = name
            best = m
            best_mean = mean
            best_win = win
    return best_name, best


def rolling_mdd_60(close: pd.Series) -> float:
    """60d max drawdown on price-based nav. Return negative."""
    if close.shape[0] < 60:
        return np.nan
    w = close.tail(60).to_numpy(dtype=float)
    nav = w / w[0]
    return max_drawdown_from_nav(nav)


def _load_tickers_from_universe_table(
    store: DuckDBStore,
    trade_date,
    universe_table: str,
    limit: int,
) -> List[str]:
    # universe_daily_hot is NOT guaranteed to have rank; use stable ordering
    rows = store.fetchall(
        f"""
        SELECT ticker
        FROM {universe_table}
        WHERE trade_date = ?
        ORDER BY ticker
        LIMIT ?;
        """,
        (trade_date, int(limit)),
    )
    return [r[0] for r in rows]


def _load_tickers_default_universe(store: DuckDBStore, trade_date, limit: int) -> List[str]:
    # your legacy default: picks_daily version=UNIVERSE
    rows = store.fetchall(
        """
        SELECT ticker
        FROM picks_daily
        WHERE trade_date = ?
          AND version = 'UNIVERSE'
        ORDER BY rank
        LIMIT ?;
        """,
        (trade_date, int(limit)),
    )
    return [r[0] for r in rows]


def main() -> None:
    args = _parse_args()

    root = _project_root_from_here()
    cfg = load_settings(root)

    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    cal = TradingCalendar()
    if args.date is None:
        trade_date = cal.latest_trading_day()
    else:
        trade_date = pd.to_datetime(args.date).date()

    # ====== window: we need MA60 + risk60 + avg_amount20 + rolling_backtest lookback/horizon ======
    buffer_days = 420  # natural days buffer
    end = trade_date
    days = cal.trading_days(end - timedelta(days=buffer_days), end)
    if not days:
        raise RuntimeError(f"No trading days found up to trade_date={trade_date}")

    target_n = 260  # >= 220
    window_days = days[-target_n:] if len(days) >= target_n else days
    start = window_days[0]

    # tickers: --tickers overrides; else universe-source table; else default picks_daily UNIVERSE; else fallback to prices_daily
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
        universe_source = "manual"
        print(f"[OK] tickers loaded from --tickers n={len(tickers)}")
    else:
        tickers = []
        universe_source = args.universe_source

        if universe_source:
            tickers = _load_tickers_from_universe_table(store, trade_date, universe_source, int(args.limit))
            print(f"[OK] tickers loaded from universe table={universe_source} n={len(tickers)}")

        if not tickers:
            tickers = _load_tickers_default_universe(store, trade_date, int(args.limit))
            if tickers:
                universe_source = "picks_daily:UNIVERSE"
                print(f"[OK] tickers loaded from picks_daily version=UNIVERSE n={len(tickers)}")

        if not tickers:
            rows2 = store.fetchall(
                """
                SELECT DISTINCT ticker
                FROM prices_daily
                WHERE trade_date <= ?
                ORDER BY ticker
                LIMIT ?;
                """,
                (trade_date, int(args.limit)),
            )
            tickers = [r[0] for r in rows2]
            if tickers:
                universe_source = "prices_daily:fallback"
                print(f"[WARN] No universe for {trade_date}; fallback to prices_daily tickers (n={len(tickers)}).")

    if not tickers:
        raise RuntimeError(
            f"No tickers available for trade_date={trade_date}. "
            f"Fix: run build_universe / ingest_prices earlier, or pass --tickers."
        )

    # fetch prices + amount
    placeholders = ",".join(["?"] * len(tickers))
    rows = store.fetchall(
        f"""
        SELECT trade_date, ticker, close, amount
        FROM prices_daily
        WHERE ticker IN ({placeholders})
          AND trade_date BETWEEN ? AND ?
        ORDER BY ticker, trade_date;
        """,
        tuple(tickers) + (start, end),
    )
    if not rows:
        raise RuntimeError(f"No prices found in window [{start},{end}]. Run ingest_prices first.")

    df = pd.DataFrame(rows, columns=["trade_date", "ticker", "close", "amount"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    out_rows = []
    for ticker, g in df.groupby("ticker", sort=False):
        g = g.sort_values("trade_date").copy()

        g["ret_1d"] = g["close"].pct_change(1)
        g["ret_5d"] = g["close"].pct_change(5)
        g["ret_10d"] = g["close"].pct_change(10)
        g["ret_20d"] = g["close"].pct_change(20)

        g["ma_5"] = g["close"].rolling(5).mean()
        g["ma_10"] = g["close"].rolling(10).mean()
        g["ma_20"] = g["close"].rolling(20).mean()
        g["ma_60"] = g["close"].rolling(60).mean()

        g["ma5_gt_ma10_gt_ma20"] = ((g["ma_5"] > g["ma_10"]) & (g["ma_10"] > g["ma_20"])).astype(int)
        g["ma20_above_ma60"] = (g["ma_20"] > g["ma_60"]).astype(int)

        g["ma20_slope"] = g["ma_20"] / g["ma_20"].shift(5) - 1.0

        g["mom_5d"] = g["ret_5d"]
        g["vol_5d"] = g["ret_1d"].rolling(5).std()

        g["vol_ann_60d"] = g["ret_1d"].rolling(60).std(ddof=1) * np.sqrt(252.0)
        g["worst_day_60d"] = g["ret_1d"].rolling(60).min()

        g["avg_amount_20"] = g["amount"].rolling(20).mean()

        g["limit_down_60"] = (g["ret_1d"] <= -0.097).rolling(60).sum()
        g["limit_up_60"] = (g["ret_1d"] >= 0.097).rolling(60).sum()

        tgt = g[g["trade_date"].dt.date <= trade_date]
        if tgt.empty:
            continue
        r = tgt.iloc[-1]

        mdd60 = rolling_mdd_60(tgt["close"])

        prices_np = tgt["close"].to_numpy(dtype=float)
        best_style, bt = choose_best_style(prices_np)

        out_rows.append(
            {
                "trade_date": trade_date,
                "ticker": ticker,
                "ret_1d": float(r["ret_1d"]) if pd.notna(r["ret_1d"]) else None,
                "mom_5d": float(r["mom_5d"]) if pd.notna(r["mom_5d"]) else None,
                "vol_5d": float(r["vol_5d"]) if pd.notna(r["vol_5d"]) else None,
                "ma_5": float(r["ma_5"]) if pd.notna(r["ma_5"]) else None,
                "ret_5d": float(r["ret_5d"]) if pd.notna(r["ret_5d"]) else None,
                "ret_10d": float(r["ret_10d"]) if pd.notna(r["ret_10d"]) else None,
                "ret_20d": float(r["ret_20d"]) if pd.notna(r["ret_20d"]) else None,
                "ma_10": float(r["ma_10"]) if pd.notna(r["ma_10"]) else None,
                "ma_20": float(r["ma_20"]) if pd.notna(r["ma_20"]) else None,
                "ma_60": float(r["ma_60"]) if pd.notna(r["ma_60"]) else None,
                "ma5_gt_ma10_gt_ma20": int(r["ma5_gt_ma10_gt_ma20"]) if pd.notna(r["ma5_gt_ma10_gt_ma20"]) else None,
                "ma20_above_ma60": int(r["ma20_above_ma60"]) if pd.notna(r["ma20_above_ma60"]) else None,
                "ma20_slope": float(r["ma20_slope"]) if pd.notna(r["ma20_slope"]) else None,
                "vol_ann_60d": float(r["vol_ann_60d"]) if pd.notna(r["vol_ann_60d"]) else None,
                "mdd_60d": float(mdd60) if pd.notna(mdd60) else None,
                "worst_day_60d": float(r["worst_day_60d"]) if pd.notna(r["worst_day_60d"]) else None,
                "avg_amount_20": float(r["avg_amount_20"]) if pd.notna(r["avg_amount_20"]) else None,
                "limit_up_60": int(r["limit_up_60"]) if pd.notna(r["limit_up_60"]) else None,
                "limit_down_60": int(r["limit_down_60"]) if pd.notna(r["limit_down_60"]) else None,
                "bt_best_style": best_style if bt else None,
                "bt_mean": float(bt.get("bt_mean")) if bt else None,
                "bt_median": float(bt.get("bt_median")) if bt else None,
                "bt_winrate": float(bt.get("bt_winrate")) if bt else None,
                "bt_p10": float(bt.get("bt_p10")) if bt else None,
                "bt_worst": float(bt.get("bt_worst")) if bt else None,
                "bt_avg_mdd": float(bt.get("bt_avg_mdd")) if bt else None,
                "bt_worst_mdd": float(bt.get("bt_worst_mdd")) if bt else None,
                "source": "calc",
            }
        )

    feat = pd.DataFrame(out_rows)
    if feat.empty:
        raise RuntimeError("Features dataframe is empty (likely not enough history / missing prices).")

    insert_cols = [
        "trade_date", "ticker",
        "ret_1d", "mom_5d", "vol_5d", "ma_5",
        "ret_5d", "ret_10d", "ret_20d",
        "ma_10", "ma_20", "ma_60",
        "ma5_gt_ma10_gt_ma20", "ma20_above_ma60", "ma20_slope",
        "vol_ann_60d", "mdd_60d", "worst_day_60d",
        "avg_amount_20", "limit_up_60", "limit_down_60",
        "bt_best_style", "bt_mean", "bt_median", "bt_winrate", "bt_p10", "bt_worst", "bt_avg_mdd", "bt_worst_mdd",
        "source",
    ]

    def _to_py(v):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, (np.generic,)):
            return v.item()
        return v

    records = []
    for row in feat[insert_cols].itertuples(index=False, name=None):
        records.append(tuple(_to_py(x) for x in row))

    with store.session() as con:
        con.execute("BEGIN;")
        try:
            if args.purge_date:
                # CRITICAL FIX: overwrite by date to avoid mixed universes
                con.execute("DELETE FROM features_daily WHERE trade_date = ?;", (trade_date,))
            else:
                # legacy behavior (not recommended)
                con.execute(
                    f"DELETE FROM features_daily WHERE trade_date = ? AND ticker IN ({placeholders});",
                    (trade_date, *tickers),
                )

            con.executemany(
                f"""
                INSERT INTO features_daily({",".join(insert_cols)})
                VALUES ({",".join(["?"] * len(insert_cols))});
                """,
                records,
            )
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise

    n = store.fetchone("SELECT COUNT(*) FROM features_daily WHERE trade_date=?;", (trade_date,))[0]
    print("[OK] build_features passed.")
    print("trade_date:", trade_date)
    print("window:", start, "to", end)
    print("universe_source:", universe_source)
    print("tickers:", tickers[:10], ("..." if len(tickers) > 10 else ""))
    print("rows_written:", n)
    print("db:", cfg.store_db)


if __name__ == "__main__":
    main()
