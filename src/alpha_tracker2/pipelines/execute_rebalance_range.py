from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Dict, List

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.strategy_id import parse_strategy_id
from alpha_tracker2.storage.duckdb_store import DuckDBStore  # 注意：你的 DuckDBStore 需要 (db_path, schema_path)


ROOT = Path(__file__).resolve().parents[3]


def _parse_date(s: str) -> date:
    return pd.to_datetime(s).date()


def _get_store_db_path(project_root: Path, settings) -> Path:
    """
    兼容 Settings 结构：
    - settings.paths 可能是 dict，也可能是对象
    - 目标字段：paths.store_db（来自 default.yaml）
    """
    paths = getattr(settings, "paths", None)
    store_db = None

    if isinstance(paths, dict):
        store_db = paths.get("store_db") or paths.get("store_db_path") or paths.get("db")
    else:
        # object-like
        store_db = getattr(paths, "store_db", None) or getattr(paths, "store_db_path", None) or getattr(paths, "db", None)

    # 最后兜底：与 default.yaml 一致
    if not store_db:
        store_db = "data/store/alpha_tracker.duckdb"

    return (project_root / str(store_db)).resolve()


def _get_schema_path(project_root: Path) -> Path:
    """
    你的 DuckDBStore 需要 schema_path。
    这里优先用项目根目录的 schema.sql（你上传过 schema.sql）
    """
    p = project_root / "schema.sql"
    if p.exists():
        return p.resolve()
    # 兜底：常见路径
    p2 = project_root / "src" / "alpha_tracker2" / "storage" / "schema.sql"
    return p2.resolve()


def _trading_days_between(store: DuckDBStore, start: date, end: date) -> List[date]:
    rows = store.fetchall(
        """
        SELECT DISTINCT trade_date
        FROM prices_daily
        WHERE trade_date BETWEEN ? AND ?
        ORDER BY trade_date
        """,
        (start, end),
    )
    return [pd.to_datetime(r[0]).date() for r in rows]


def _load_close(store: DuckDBStore, d: date, tickers: List[str]) -> Dict[str, float]:
    if not tickers:
        return {}
    rows = store.fetchall(
        """
        SELECT ticker, close
        FROM prices_daily
        WHERE trade_date = ? AND ticker IN (SELECT UNNEST(?))
        """,
        (d, tickers),
    )
    out: Dict[str, float] = {}
    for tk, px in rows:
        if px is None:
            continue
        out[str(tk)] = float(px)
    return out


def _load_signal(store: DuckDBStore, signal_date: date, version: str, topk: int) -> List[str]:
    rows = store.fetchall(
        """
        SELECT ticker
        FROM picks_daily
        WHERE trade_date = ? AND version = ?
        ORDER BY
          CASE WHEN rank IS NULL THEN 1 ELSE 0 END ASC,
          rank ASC,
          score DESC NULLS LAST
        LIMIT ?
        """,
        (signal_date, version, topk),
    )
    return [str(r[0]) for r in rows]


def _equal_weight_target_alloc(
    equity: float,
    tickers: List[str],
    px_map: Dict[str, float],
    lot_size: int,
) -> Dict[str, int]:
    tickers = [t for t in tickers if t in px_map and float(px_map.get(t, 0.0)) > 0]
    if not tickers:
        return {}
    per = float(equity) / float(len(tickers))

    target: Dict[str, int] = {}
    for tk in tickers:
        px = float(px_map[tk])
        raw_shares = int(per // px)
        sh = (raw_shares // lot_size) * lot_size
        if sh > 0:
            target[tk] = int(sh)
    return target


def _diff_to_trades(prev: Dict[str, int], target: Dict[str, int]) -> List[Dict]:
    keys = set(prev.keys()) | set(target.keys())
    out: List[Dict] = []
    for tk in sorted(keys):
        a = int(prev.get(tk, 0))
        b = int(target.get(tk, 0))
        d = b - a
        if d == 0:
            continue
        out.append({"ticker": tk, "side": "BUY" if d > 0 else "SELL", "shares": abs(d)})
    return out


def _ensure_positions_schema(con) -> None:
    # v2 schema 已经迁移过；这里仅做幂等兜底（不建索引，不做危险修改）
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS positions_daily (
          asof_date DATE,
          version VARCHAR,
          ticker VARCHAR,
          shares BIGINT,
          price DOUBLE,
          market_value DOUBLE,
          cash DOUBLE,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          strategy_id VARCHAR,
          avg_cost DOUBLE,
          pnl_pct DOUBLE,
          hold_days INTEGER,
          tp_half_done BOOLEAN,
          score DOUBLE,
          meta_json VARCHAR
        );
        """
    )
    con.execute("ALTER TABLE positions_daily ADD COLUMN IF NOT EXISTS strategy_id VARCHAR;")


def _ensure_trades_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS trades_daily (
          trade_date DATE,
          version VARCHAR,
          strategy_id VARCHAR,
          ticker VARCHAR,
          side VARCHAR,
          shares BIGINT,
          price DOUBLE,
          notional DOUBLE,
          cost DOUBLE,
          reason VARCHAR,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    con.execute("ALTER TABLE trades_daily ADD COLUMN IF NOT EXISTS strategy_id VARCHAR;")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)

    ap.add_argument("--strategy_id", required=False, default="", help="e.g. V1__REB_DAILY__H5__TOP6__C10")
    ap.add_argument("--version", required=False, default="", help="legacy: e.g. ENS")
    ap.add_argument("--topk", type=int, default=3)

    ap.add_argument("--cash", type=float, default=100000.0)
    ap.add_argument("--lot_size", type=int, default=100)
    ap.add_argument("--hold_last_signal", action="store_true")

    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)

    # ---- resolve strategy spec ----
    strategy_id = (args.strategy_id or "").strip()
    if strategy_id:
        spec = parse_strategy_id(strategy_id)
        version = spec.model_version
        topk = int(spec.topk)
        trade_rule = spec.trade_rule  # REB_DAILY / REB_ON_SIGNAL_CHANGE
        hold_n = int(spec.hold_n)     # reserved
        cost_bps = int(spec.cost_bps) # reserved
    else:
        version = (args.version or "").strip()
        if not version:
            raise ValueError("Either --strategy_id or --version must be provided.")
        topk = int(args.topk)
        trade_rule = "REB_ON_SIGNAL_CHANGE"
        hold_n = 0
        cost_bps = 0
        strategy_id = f"{version}__LEGACY__H0__TOP{topk}__C0"

    lot_size = int(args.lot_size)
    hold_last_signal = bool(args.hold_last_signal)

    # ✅ 正确调用：load_settings(ROOT)
    settings = load_settings(ROOT)

    # ✅ 正确取 db_path：settings.paths.store_db（兼容 dict/object）
    db_path = _get_store_db_path(ROOT, settings)
    schema_path = _get_schema_path(ROOT)

    # ✅ 你的 DuckDBStore 需要两个参数
    store = DuckDBStore(db_path=db_path, schema_path=schema_path)

    # schema兜底（你已经 migrate v2 了，这里只是保险）
    with store.session() as con:
        _ensure_positions_schema(con)
        _ensure_trades_schema(con)

    days = _trading_days_between(store, start, end)
    if not days:
        print("[WARN] no trading days in range.")
        return

    cash = float(args.cash)
    hold: Dict[str, int] = {}

    last_signal: List[str] = []
    last_effective: List[str] = []
    n_rebalances = 0

    for d in days:
        sig = _load_signal(store, d, version, topk)
        if sig:
            last_signal = sig
            eff_signal = sig
        else:
            eff_signal = last_signal if hold_last_signal else []

        if trade_rule == "REB_DAILY":
            need_rebalance = True
        else:
            need_rebalance = (eff_signal != last_effective)

        px_needed = list(set(list(hold.keys()) + eff_signal))
        px = _load_close(store, d, px_needed)

        trades: List[Dict] = []
        if need_rebalance and eff_signal:
            mv = sum(float(px.get(tk, 0.0)) * sh for tk, sh in hold.items() if tk in px)
            equity = float(cash) + float(mv)

            target = _equal_weight_target_alloc(equity, eff_signal, px, lot_size)
            trades = _diff_to_trades(hold, target)

            # execute at close
            for tr in trades:
                tk = tr["ticker"]
                side = tr["side"]
                sh = int(tr["shares"])
                price = float(px.get(tk, 0.0) or 0.0)
                if price <= 0:
                    continue
                notional = price * sh

                if side == "BUY":
                    if notional > cash:
                        continue
                    cash -= notional
                    hold[tk] = int(hold.get(tk, 0) + sh)
                else:
                    cur = int(hold.get(tk, 0))
                    sell_sh = min(cur, sh)
                    if sell_sh <= 0:
                        continue
                    cash += price * sell_sh
                    new_sh = cur - sell_sh
                    if new_sh > 0:
                        hold[tk] = int(new_sh)
                    else:
                        hold.pop(tk, None)

            n_rebalances += 1
            last_effective = eff_signal

        # write trades + daily positions snapshot
        with store.session() as con:
            con.execute("BEGIN;")
            try:
                # trades
                trade_rows = []
                for tr in trades:
                    tk = tr["ticker"]
                    side = tr["side"]
                    sh = int(tr["shares"])
                    price = float(px.get(tk, 0.0) or 0.0)
                    if price <= 0:
                        continue
                    notional = float(price * sh)
                    cost = 0.0
                    trade_rows.append([d, version, strategy_id, tk, side, sh, price, notional, cost, "rebalance"])

                if trade_rows:
                    con.executemany(
                        """
                        INSERT INTO trades_daily
                        (trade_date, version, strategy_id, ticker, side, shares, price, notional, cost, reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        trade_rows,
                    )

                # positions: idempotent per day+strategy
                con.execute(
                    "DELETE FROM positions_daily WHERE asof_date = ? AND strategy_id = ?",
                    [d, strategy_id],
                )

                pos_rows = []
                for tk, sh in hold.items():
                    price = float(px.get(tk, 0.0) or 0.0)
                    mv = float(price * sh)
                    pos_rows.append([d, version, tk, int(sh), price, mv, float(cash), strategy_id])

                # cash row
                pos_rows.append([d, version, "__CASH__", 0, 1.0, float(cash), float(cash), strategy_id])

                con.executemany(
                    """
                    INSERT INTO positions_daily
                    (asof_date, version, ticker, shares, price, market_value, cash, strategy_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    pos_rows,
                )

                con.execute("COMMIT;")
            except Exception:
                con.execute("ROLLBACK;")
                raise

    out_dir = ROOT / "data" / "out" / "exec" / "summary"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"exec_summary_{strategy_id}_{start}_{end}.csv"
    pd.DataFrame(
        [
            {
                "strategy_id": strategy_id,
                "version": version,
                "trade_rule": trade_rule,
                "hold_n": hold_n,
                "cost_bps": cost_bps,
                "start": str(start),
                "end": str(end),
                "days": len(days),
                "rebalances": n_rebalances,
                "cash_left": float(cash),
                "db_path": str(db_path),
            }
        ]
    ).to_csv(out_path, index=False, encoding="utf-8-sig")
    print("[OK] summary exported:", out_path)


if __name__ == "__main__":
    main()
