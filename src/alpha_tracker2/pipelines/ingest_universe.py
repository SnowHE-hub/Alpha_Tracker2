from __future__ import annotations

import argparse
from pathlib import Path
from datetime import date

import pandas as pd
import yaml

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.registry import REGISTRY
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.ingestion.cache import UniverseCache
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _parse_args():
    ap = argparse.ArgumentParser(description="Ingest universe into picks_daily(version=UNIVERSE).")
    ap.add_argument("--date", type=str, default=None, help="trade_date YYYY-MM-DD (default: latest trading day)")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()

    root = Path(__file__).resolve().parents[3]
    s = load_settings(root)

    store = DuckDBStore(
        db_path=s.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    cal = TradingCalendar()
    if args.date is None:
        trade_date = cal.latest_trading_day()
    else:
        trade_date = pd.to_datetime(args.date).date()

    cache = UniverseCache(lake_dir=s.lake_dir)

    # 从 configs/default.yaml 读取 provider 名称
    cfg_path = root / "configs" / "default.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg_obj = yaml.safe_load(f) or {}
    provider_name = (cfg_obj.get("ingestion", {}) or {}).get("universe_provider", "mock")

    Provider = REGISTRY.get_universe_provider(provider_name)
    provider = Provider()

    try:
        rows = provider.fetch_universe(trade_date)
        df = pd.DataFrame([{"ticker": r.ticker, "name": r.name} for r in rows])

        # 成功就保存缓存
        cache_path = cache.save(trade_date, df) if not df.empty else None
        source = "live"
    except Exception as e:
        # 失败就用缓存（当天 -> 最近一次）
        df = cache.load(trade_date)
        if df is None:
            df = cache.load_latest()

        if df is None or df.empty:
            raise RuntimeError(f"Universe fetch failed and no cache available: {e}") from e

        cache_path = None
        source = "cache"

    df = df.copy()
    df["trade_date"] = pd.to_datetime(trade_date).date()
    df["version"] = "UNIVERSE"
    df["score"] = 0.0
    df["rank"] = range(1, len(df) + 1)

    with store.session() as con:
        try:
            con.execute("BEGIN;")
            con.execute(
                "DELETE FROM picks_daily WHERE trade_date=? AND version='UNIVERSE';",
                [trade_date],
            )

            for _, r in df.iterrows():
                con.execute(
                    """
                    INSERT INTO picks_daily(trade_date, version, ticker, name, score, rank)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        trade_date,
                        r["version"],
                        r["ticker"],
                        r["name"],
                        float(r["score"]),
                        int(r["rank"]),
                    ],
                )

            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise

    n = store.fetchone(
        "SELECT COUNT(*) FROM picks_daily WHERE trade_date=? AND version='UNIVERSE';",
        (trade_date,),
    )[0]

    print("[OK] ingest_universe passed.")
    print("trade_date:", trade_date)
    print("rows_written:", n)
    print("db:", s.store_db)
    print("provider:", provider_name)
    print("source:", source)
    if cache_path:
        print("cache_saved:", cache_path)


if __name__ == "__main__":
    main()
