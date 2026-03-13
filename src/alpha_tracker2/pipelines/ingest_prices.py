from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import yaml
import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.registry import REGISTRY
from alpha_tracker2.ingestion.prices_cache import PricesCache
from alpha_tracker2.storage.duckdb_store import DuckDBStore
from alpha_tracker2.core.trading_calendar import TradingCalendar


def _to_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _read_tickers_file(p: str | Path) -> list[str]:
    path = Path(p)
    if not path.exists():
        raise FileNotFoundError(f"tickers-file not found: {path}")
    txt = path.read_text(encoding="utf-8", errors="ignore")
    out: list[str] = []
    for line in txt.splitlines():
        t = line.strip()
        if not t:
            continue
        # allow comments
        if t.startswith("#"):
            continue
        out.append(t)
    # de-dup keep order
    seen = set()
    uniq = []
    for t in out:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq


def _disable_proxies_in_process() -> None:
    """
    Hard-disable proxies for this Python process.

    Why: AkShare calls requests.get(...) directly. requests can read proxies from:
      - env vars (HTTP_PROXY/HTTPS_PROXY/ALL_PROXY/NO_PROXY, lower-case too)
      - requests Session trust_env
      - urllib.getproxies / requests.utils.getproxies()

    We disable all of them, so even AkShare's internal requests.get won't use proxy.
    """
    # 1) remove proxy env vars (delete, not empty)
    proxy_keys = [
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
        "http_proxy", "https_proxy", "all_proxy", "no_proxy",
    ]
    for k in proxy_keys:
        if k in os.environ:
            try:
                del os.environ[k]
            except Exception:
                os.environ[k] = ""

    # 2) set NO_PROXY wildcard as extra guard (some libs still consult it)
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"

    # 3) force requests to ignore env/system proxy
    try:
        import requests  # noqa

        # disable reading environment variables for all new Sessions
        requests.sessions.Session.trust_env = False

        # and make getproxies return empty (double insurance)
        try:
            requests.utils.getproxies = lambda: {}  # type: ignore
        except Exception:
            pass
    except Exception:
        # if requests import fails (shouldn't), we at least removed env vars
        pass


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--date",
        default=None,
        help="trade_date YYYY-MM-DD (default: latest trading day)",
    )
    ap.add_argument(
        "--start",
        default=None,
        help="start date YYYY-MM-DD (optional). If omitted, derived from trade_date and --last-n.",
    )
    ap.add_argument(
        "--end",
        default=None,
        help="end date YYYY-MM-DD (optional). If omitted, defaults to trade_date + 30 calendar days.",
    )
    ap.add_argument(
        "--last-n",
        type=int,
        default=10,
        help="number of trading days to include ending at trade_date (only used when --start is not set). default=10",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=3,
        help="how many tickers to ingest from UNIVERSE (small-step). default=3",
    )
    ap.add_argument(
        "--tickers",
        default=None,
        help="comma separated explicit tickers, e.g. 000001.SZ,000002.SZ. If provided, overrides --limit universe selection.",
    )
    ap.add_argument(
        "--tickers-file",
        default=None,
        help="path to a txt file, one ticker per line. If provided, overrides --tickers and universe selection.",
    )
    ap.add_argument(
        "--no-proxy",
        action="store_true",
        help="disable proxy env vars for this run (fix ProxyError with AkShare/Eastmoney).",
    )
    return ap.parse_args()


def main() -> None:
    args = _parse_args()

    if args.no_proxy:
        _disable_proxies_in_process()

    root = Path(__file__).resolve().parents[3]
    s = load_settings(root)

    store = DuckDBStore(
        db_path=s.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    cfg_obj = yaml.safe_load((root / "configs" / "default.yaml").read_text(encoding="utf-8")) or {}
    ingestion = cfg_obj.get("ingestion", {}) or {}
    prices_provider_name = ingestion.get("prices_provider", "mock_prices")

    cal = TradingCalendar()

    # trade_date：默认取最近一个交易日；也支持 --date
    if args.date:
        trade_date = _to_date(args.date)
    else:
        trade_date = cal.latest_trading_day()

    # end：默认 trade_date + 30 天（保证 eval_5d 有未来窗口）；也支持 --end
    if args.end:
        end = _to_date(args.end)
    else:
        end = trade_date + timedelta(days=30)

    # start：如果显式给了 --start，就用它；否则用“trade_date 往前 last_n 个交易日窗口”
    if args.start:
        start = _to_date(args.start)
    else:
        days = cal.trading_days(trade_date - timedelta(days=90), trade_date)
        last_n = max(1, int(args.last_n))
        window_days = days[-last_n:] if len(days) >= last_n else days
        if not window_days:
            raise RuntimeError("No trading days returned by TradingCalendar; cannot determine start date.")
        start = window_days[0]

    if start > end:
        raise ValueError(f"Invalid window: start={start} > end={end}")

    # === tickers：优先使用 --tickers-file；其次 --tickers；否则用 UNIVERSE 前 limit 个（小步验证用）===
    if args.tickers_file:
        tickers = _read_tickers_file(args.tickers_file)
    elif args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    else:
        limit = max(1, int(args.limit))
        rows = store.fetchall(
            """
            SELECT ticker
            FROM picks_daily
            WHERE trade_date = ?
              AND version = 'UNIVERSE'
            ORDER BY rank
            LIMIT ?;
            """,
            (trade_date, limit),
        )
        tickers = [r[0] for r in rows]

    if not tickers:
        raise RuntimeError(
            f"No tickers found. trade_date={trade_date}. "
            f"Run ingest_universe first or pass --tickers/--tickers-file."
        )

    Provider = REGISTRY.get_prices_provider(prices_provider_name)
    provider = Provider()

    cache = PricesCache(lake_dir=s.lake_dir)

    rows_written_total = 0

    with store.session() as con:
        for ticker in tickers:
            # 幂等：先删窗口内的旧数据
            con.execute("BEGIN;")
            try:
                con.execute(
                    "DELETE FROM prices_daily WHERE ticker=? AND trade_date BETWEEN ? AND ?;",
                    [ticker, start, end],
                )
                con.execute("COMMIT;")
            except Exception:
                con.execute("ROLLBACK;")
                raise

            # 再拉取：live-first + fallback cache
            source = "live"
            try:
                rows = provider.fetch_prices(ticker, start, end)
                df = pd.DataFrame([r.__dict__ for r in rows])
                if not df.empty:
                    cache.save(ticker, start, end, df)
            except Exception:
                df = cache.load(ticker, start, end)
                if df is None or df.empty:
                    raise
                source = "cache"

            if df is None or df.empty:
                print(f"[WARN] no prices for {ticker} in {start}..{end} (provider={prices_provider_name})")
                continue

            df["ticker"] = df["ticker"].astype(str)
            df["source"] = source

            con.execute("BEGIN;")
            try:
                for _, r in df.iterrows():
                    con.execute(
                        """
                        INSERT OR REPLACE INTO prices_daily
                        (trade_date, ticker, open, high, low, close, volume, amount, source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            r["trade_date"],
                            r["ticker"],
                            r.get("open"),
                            r.get("high"),
                            r.get("low"),
                            r.get("close"),
                            r.get("volume"),
                            r.get("amount"),
                            r["source"],
                        ],
                    )
                con.execute("COMMIT;")
            except Exception:
                con.execute("ROLLBACK;")
                raise

            rows_written_total += len(df)

    n = store.fetchone(
        "SELECT COUNT(*) FROM prices_daily WHERE trade_date BETWEEN ? AND ?;",
        (start, end),
    )[0]
    max_dt = store.fetchone("SELECT MAX(trade_date) FROM prices_daily;", ())[0]

    print("[OK] ingest_prices passed.")
    print("provider:", prices_provider_name)
    print("trade_date:", trade_date)
    print("tickers:", tickers[:20], ("..." if len(tickers) > 20 else ""))
    print("window:", start, "to", end)
    print("rows_written_total:", rows_written_total)
    print("rows_in_db_window:", n)
    print("prices_daily_max_trade_date:", max_dt)
    print("db:", s.store_db)


if __name__ == "__main__":
    main()
