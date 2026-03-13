from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore
from alpha_tracker2.scoring.thresholds import ThresholdConfig, update_history, get_threshold


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=str, default=None)
    ap.add_argument("--version", type=str, default="V1")
    ap.add_argument("--q", type=float, default=0.9)
    ap.add_argument("--window", type=int, default=60)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[3]
    cfg = load_settings(root)

    cal = TradingCalendar()
    if args.date is None:
        trade_date = cal.latest_trading_day()
    else:
        trade_date = pd.to_datetime(args.date).date()

    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    # 取当日该 version 的 score 分布，作为历史样本写入
    rows = store.fetchall(
        """
        SELECT score
        FROM picks_daily
        WHERE trade_date = ? AND version = ?
        ORDER BY rank;
        """,
        (trade_date, args.version),
    )
    if not rows:
        raise RuntimeError(f"No picks_daily rows for trade_date={trade_date}, version={args.version}. Run score_all first.")

    scores = [r[0] for r in rows if r[0] is not None]

    hist_path = root / "data" / "cache" / "ab_threshold_history.json"
    update_history(hist_path, trade_date, args.version, scores)

    cfg_thr = ThresholdConfig(q=args.q, window=args.window)
    thr = get_threshold(hist_path, args.version, cfg=cfg_thr)

    print("[OK] threshold cache updated.")
    print("trade_date:", trade_date)
    print("version:", args.version)
    print("scores_n:", len(scores))
    print("q:", args.q, "window:", args.window)
    print("threshold:", thr)
    print("file:", hist_path)


if __name__ == "__main__":
    main()
