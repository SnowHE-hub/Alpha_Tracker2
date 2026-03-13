# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore

ROOT = Path(__file__).resolve().parents[3]


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _load_signal_picks(store: DuckDBStore, signal_date: date, version: str, topk: int) -> pd.DataFrame:
    rows = store.fetchall(
        """
        SELECT trade_date, version, ticker, name, rank, score
        FROM picks_daily
        WHERE trade_date = ? AND version = ?
        ORDER BY
          CASE WHEN rank IS NULL THEN 1 ELSE 0 END ASC,
          rank ASC,
          score DESC NULLS LAST
        LIMIT ?
        """,
        (signal_date, version, int(topk)),
    )
    if not rows:
        return pd.DataFrame(columns=["trade_date", "version", "ticker", "name", "rank", "score"])

    df = pd.DataFrame(rows, columns=["trade_date", "version", "ticker", "name", "rank", "score"])
    df["ticker"] = df["ticker"].astype(str)
    df["version"] = df["version"].astype(str)
    df["name"] = df["name"].fillna("").astype(str)
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    return df


def _find_prev_signal_date(store: DuckDBStore, signal_date: date, version: str) -> Optional[date]:
    rows = store.fetchall(
        """
        SELECT MAX(trade_date) AS prev_date
        FROM picks_daily
        WHERE trade_date < ? AND version = ?
        """,
        (signal_date, version),
    )
    if not rows or rows[0][0] is None:
        return None
    return rows[0][0]


def _equal_weight_targets(df: pd.DataFrame) -> Dict[str, float]:
    tickers = df["ticker"].astype(str).tolist() if not df.empty else []
    n = len(tickers)
    if n == 0:
        return {}
    w = 1.0 / n
    return {t: w for t in tickers}


def _turnover_from_weights(prev_w: Dict[str, float], cur_w: Dict[str, float]) -> float:
    keys = set(prev_w.keys()) | set(cur_w.keys())
    s = 0.0
    for k in keys:
        s += abs(prev_w.get(k, 0.0) - cur_w.get(k, 0.0))
    return 0.5 * float(s)


def _build_orders(
    signal_date: date,
    version: str,
    prev_date: Optional[date],
    prev_targets: Dict[str, float],
    cur_targets: Dict[str, float],
    name_map: Dict[str, str],
) -> pd.DataFrame:
    keys = sorted(set(prev_targets.keys()) | set(cur_targets.keys()))

    rows = []
    for tk in keys:
        prev_w = float(prev_targets.get(tk, 0.0))
        cur_w = float(cur_targets.get(tk, 0.0))
        delta = cur_w - prev_w

        if prev_w == 0.0 and cur_w > 0.0:
            action = "BUY"
        elif prev_w > 0.0 and cur_w == 0.0:
            action = "SELL"
        else:
            action = "HOLD" if abs(delta) < 1e-12 else ("BUY" if delta > 0 else "SELL")

        rows.append(
            {
                "signal_date": signal_date,
                "version": version,
                "prev_signal_date": prev_date if prev_date is not None else "",
                "ticker": tk,
                "name": name_map.get(tk, ""),
                "action": action,
                "prev_weight": prev_w,
                "target_weight": cur_w,
                "delta_weight": delta,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["abs_delta"] = out["delta_weight"].abs()
    out = out.sort_values(["action", "abs_delta"], ascending=[True, False]).drop(columns=["abs_delta"])
    return out


def _write_orders_daily(store: DuckDBStore, orders: pd.DataFrame, purge: bool, source_csv: str) -> None:
    if orders.empty:
        return

    signal_date = orders["signal_date"].iloc[0]
    version = str(orders["version"].iloc[0])

    with store.session() as con:
        if purge:
            con.execute(
                "DELETE FROM orders_daily WHERE signal_date = ? AND version = ?",
                (signal_date, version),
            )

        # 用 SQL 的 CURRENT_TIMESTAMP 保证 created_at 不会 NaT
        con.execute(
            """
            INSERT INTO orders_daily (
              signal_date, version, ticker, name, action,
              prev_weight, target_weight, delta_weight,
              source_csv, created_at
            )
            SELECT
              ?, ?, ?, ?, ?,
              ?, ?, ?,
              ?, CURRENT_TIMESTAMP
            """,
            (
                signal_date,
                version,
                None, None, None,
                None, None, None,
                source_csv,
            ),
        )
        # 上面 insert 是占位写法，不适合逐行；下面用 executemany 才是正确写法
        con.execute("DELETE FROM orders_daily WHERE signal_date = ? AND version = ? AND ticker IS NULL", (signal_date, version))

        rows = []
        for _, r in orders.iterrows():
            rows.append(
                (
                    r["signal_date"],
                    r["version"],
                    str(r["ticker"]),
                    str(r.get("name", "") or ""),
                    str(r.get("action", "") or ""),
                    float(r.get("prev_weight", 0.0) or 0.0),
                    float(r.get("target_weight", 0.0) or 0.0),
                    float(r.get("delta_weight", 0.0) or 0.0),
                    source_csv,
                )
            )

        con.executemany(
            """
            INSERT INTO orders_daily (
              signal_date, version, ticker, name, action,
              prev_weight, target_weight, delta_weight,
              source_csv, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            rows,
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade_date", required=True, help="signal date (picks_daily.trade_date), YYYY-MM-DD")
    ap.add_argument("--version", required=True, help="one version, e.g. ENS")
    ap.add_argument("--topk", type=int, default=3)
    ap.add_argument("--out_dir", default="", help="default: data/out/orders")

    ap.add_argument("--write_db", action="store_true", help="write orders into orders_daily")
    ap.add_argument("--purge", action="store_true", help="purge existing orders_daily rows for that date+version before insert")
    args = ap.parse_args()

    signal_date = _parse_date(args.trade_date)
    version = str(args.version).strip()
    topk = int(args.topk)

    print("[OK] generate_orders started.")
    print("signal_date:", signal_date)
    print("version:", version)
    print("topk:", topk)

    cfg = load_settings(ROOT)
    store = DuckDBStore(cfg.store_db, ROOT / "src" / "alpha_tracker2" / "storage" / "schema.sql")
    store.init_schema()

    cur_df = _load_signal_picks(store, signal_date, version, topk=topk)
    if cur_df.empty:
        print(f"[SKIP] no picks for {version} on {signal_date}")
        return

    name_map = {str(r["ticker"]): (r["name"] if pd.notna(r["name"]) else "") for _, r in cur_df.iterrows()}

    prev_date = _find_prev_signal_date(store, signal_date, version)
    prev_df = pd.DataFrame(columns=cur_df.columns)
    if prev_date is not None:
        prev_df = _load_signal_picks(store, prev_date, version, topk=topk)
        for _, r in prev_df.iterrows():
            tk = str(r["ticker"])
            if tk not in name_map:
                name_map[tk] = r["name"] if pd.notna(r["name"]) else ""

    prev_targets = _equal_weight_targets(prev_df)
    cur_targets = _equal_weight_targets(cur_df)
    turnover = _turnover_from_weights(prev_targets, cur_targets)

    orders = _build_orders(
        signal_date=signal_date,
        version=version,
        prev_date=prev_date,
        prev_targets=prev_targets,
        cur_targets=cur_targets,
        name_map=name_map,
    )

    out_dir = Path(args.out_dir) if args.out_dir else (ROOT / "data" / "out" / "orders")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"orders_{version}_{signal_date}_top{topk}.csv"
    orders.to_csv(out_path, index=False, encoding="utf-8-sig")

    print("[OK] orders exported:", out_path)
    print("prev_signal_date:", prev_date if prev_date is not None else "(none)")
    print("turnover_est:", f"{turnover:.6f}")
    print("rows:", len(orders))

    if args.write_db:
        _write_orders_daily(store, orders, purge=bool(args.purge), source_csv=str(out_path))
        print("[OK] orders_daily written. purge:", bool(args.purge))

    print("[OK] generate_orders passed.")


if __name__ == "__main__":
    main()
