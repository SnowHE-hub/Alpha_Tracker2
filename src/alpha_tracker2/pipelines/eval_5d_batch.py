from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


ROOT = Path(__file__).resolve().parents[3]


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_horizons(s: str) -> List[int]:
    """
    Parse '5,10,20' -> [5,10,20]
    """
    hs = []
    for x in (s or "").split(","):
        x = x.strip()
        if not x:
            continue
        hs.append(int(x))
    hs = [h for h in hs if h > 0]
    if not hs:
        return [5]
    return sorted(list(dict.fromkeys(hs)))


def _ensure_out_dir() -> Path:
    out_dir = ROOT / "data" / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _get_trading_days(store: DuckDBStore, start: date, end: date, buffer_days: int = 240) -> List[date]:
    """
    Use prices_daily distinct trade_date as the trading calendar.
    Fetch with lookback buffer to support multiple horizons.
    """
    lookback_start = start - timedelta(days=buffer_days)
    sql = """
    SELECT DISTINCT trade_date
    FROM prices_daily
    WHERE trade_date BETWEEN ? AND ?
    ORDER BY trade_date
    """
    rows = store.fetchall(sql, (lookback_start, end))
    return [r[0] for r in rows]


def _offset_trade_date(trading_days: List[date], asof: date, horizon: int) -> date | None:
    """
    picks_trade_date = asof_date shifted backward by `horizon` trading days.
    """
    if asof not in trading_days:
        return None
    i = trading_days.index(asof)
    j = i - horizon
    if j < 0:
        return None
    return trading_days[j]


def _load_picks(store: DuckDBStore, picks_trade_date: date, versions: List[str], topk: int | None) -> pd.DataFrame:
    placeholders = ",".join(["?"] * len(versions))
    sql = f"""
    SELECT trade_date, version, ticker, name, rank, score, reason
    FROM picks_daily
    WHERE trade_date = ?
      AND version IN ({placeholders})
    ORDER BY version,
             CASE WHEN rank IS NULL THEN 1 ELSE 0 END ASC,
             rank ASC
    """
    params = (picks_trade_date, *versions)
    rows = store.fetchall(sql, params)
    if not rows:
        return pd.DataFrame(columns=["trade_date", "version", "ticker", "name", "rank", "score", "reason"])

    df = pd.DataFrame(rows, columns=["trade_date", "version", "ticker", "name", "rank", "score", "reason"])
    df["ticker"] = df["ticker"].astype(str)
    df["version"] = df["version"].astype(str)

    # TopK filter (rank-based)
    if topk is not None:
        df = df[df["rank"].notna()].copy()
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
        df = df.dropna(subset=["rank"]).copy()
        df["rank"] = df["rank"].astype(int)
        df = df[df["rank"] <= int(topk)].copy()

    return df.reset_index(drop=True)


def _load_close_map(store: DuckDBStore, d: date, tickers: List[str]) -> Dict[str, float]:
    if not tickers:
        return {}
    placeholders = ",".join(["?"] * len(tickers))
    sql = f"""
    SELECT ticker, close
    FROM prices_daily
    WHERE trade_date = ?
      AND ticker IN ({placeholders})
    """
    params = (d, *tickers)
    rows = store.fetchall(sql, params)
    return {str(t): float(c) for t, c in rows if c is not None}


def _calc_detail(
    picks: pd.DataFrame,
    close0: Dict[str, float],
    close1: Dict[str, float],
    picks_trade_date: date,
    asof_date: date,
    horizon: int,
) -> pd.DataFrame:
    """
    detail rows per ticker, for one (asof_date, horizon).
    ret = close(asof) / close(picks_trade_date) - 1
    """
    if picks.empty:
        return pd.DataFrame(
            columns=[
                "asof_date",
                "horizon",
                "picks_trade_date",
                "version",
                "ticker",
                "name",
                "rank",
                "score",
                "reason",
                "close_0",
                "close_1",
                "ret_h",
                "n_picks",
                "n_valid",
            ]
        )

    df = picks.copy()
    df["asof_date"] = asof_date
    df["horizon"] = int(horizon)
    df["picks_trade_date"] = picks_trade_date

    df["close_0"] = df["ticker"].map(close0)
    df["close_1"] = df["ticker"].map(close1)
    df["ret_h"] = (df["close_1"] / df["close_0"]) - 1.0

    # n_picks per version based on picks (even if later invalid)
    n_picks_map = df.groupby("version", dropna=False).size().to_dict()

    # keep only valid rows for returns
    df_valid = df.dropna(subset=["close_0", "close_1", "ret_h"]).copy().reset_index(drop=True)
    n_valid_map = df_valid.groupby("version", dropna=False).size().to_dict()

    df_valid["n_picks"] = df_valid["version"].map(lambda v: int(n_picks_map.get(v, 0)))
    df_valid["n_valid"] = df_valid["version"].map(lambda v: int(n_valid_map.get(v, 0)))
    return df_valid


def _summarize_one_day(
    picks_all: pd.DataFrame,
    detail_valid: pd.DataFrame,
    asof_date: date,
    picks_trade_date: date,
    horizon: int,
) -> pd.DataFrame:
    """
    One row per version for a single day/horizon.
    """
    cols = [
        "asof_date",
        "horizon",
        "picks_trade_date",
        "version",
        "n_picks",
        "n_valid",
        "ret_h_mean",
        "ret_h_median",
        "win_rate_h",
        "top1_ret_h",
    ]

    if picks_all is None or picks_all.empty:
        return pd.DataFrame(columns=cols)

    n_picks_map = picks_all.groupby("version", dropna=False).size().to_dict()
    versions = sorted(picks_all["version"].dropna().unique().tolist())

    if detail_valid is None or detail_valid.empty:
        return pd.DataFrame(
            [
                {
                    "asof_date": asof_date,
                    "horizon": int(horizon),
                    "picks_trade_date": picks_trade_date,
                    "version": v,
                    "n_picks": int(n_picks_map.get(v, 0)),
                    "n_valid": 0,
                    "ret_h_mean": float("nan"),
                    "ret_h_median": float("nan"),
                    "win_rate_h": float("nan"),
                    "top1_ret_h": float("nan"),
                }
                for v in versions
            ]
        )[cols]

    g = detail_valid.groupby("version", dropna=False)

    def _win_rate(x: pd.Series) -> float:
        x = x.dropna()
        if x.empty:
            return float("nan")
        return float((x > 0).mean())

    summary = pd.DataFrame(
        {
            "n_valid": g["ret_h"].count(),
            "ret_h_mean": g["ret_h"].mean(),
            "ret_h_median": g["ret_h"].median(),
            "win_rate_h": g["ret_h"].apply(_win_rate),
        }
    ).reset_index()

    # top1_ret_h: smallest rank row (from picks_all, but only if valid return exists)
    # We'll compute from detail_valid sorted by rank.
    top1 = (
        detail_valid.sort_values(["version", "rank"], na_position="last")
        .groupby("version", as_index=False)
        .head(1)[["version", "ret_h"]]
        .rename(columns={"ret_h": "top1_ret_h"})
    )
    summary = summary.merge(top1, on="version", how="left")

    summary["asof_date"] = asof_date
    summary["horizon"] = int(horizon)
    summary["picks_trade_date"] = picks_trade_date
    summary["n_picks"] = summary["version"].map(lambda v: int(n_picks_map.get(v, 0)))

    # fill missing versions (picked but no valid rows)
    seen = set(summary["version"].dropna().unique().tolist())
    for v in versions:
        if v not in seen:
            summary = pd.concat(
                [
                    summary,
                    pd.DataFrame(
                        [
                            {
                                "version": v,
                                "n_valid": 0,
                                "ret_h_mean": float("nan"),
                                "ret_h_median": float("nan"),
                                "win_rate_h": float("nan"),
                                "top1_ret_h": float("nan"),
                                "asof_date": asof_date,
                                "horizon": int(horizon),
                                "picks_trade_date": picks_trade_date,
                                "n_picks": int(n_picks_map.get(v, 0)),
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

    return summary[cols].sort_values(["ret_h_mean", "n_valid"], ascending=[False, False]).reset_index(drop=True)


def _write_eval_to_duckdb_unified(
    store: DuckDBStore,
    start: date,
    end: date,
    horizons: List[int],
    batch: pd.DataFrame,
) -> None:
    """
    Write to unified table eval_batch_daily with primary key (trade_date, version, horizon).

    Maps CSV-style summary -> unified:
      trade_date := asof_date
      avg_ret    := ret_h_mean
      median_ret := ret_h_median
      hit_rate   := win_rate_h
      coverage   := n_valid / n_picks
      eval_n_*   := n_picks/n_valid
      extra      := json: picks_trade_date, top1_ret_h
    """
    if batch is None or batch.empty:
        print("[WARN] batch is empty; nothing to write into eval_batch_daily.")
        return

    df = batch.copy()
    df["trade_date"] = pd.to_datetime(df["asof_date"], errors="coerce").dt.date
    df["version"] = df["version"].astype(str)
    df["horizon"] = pd.to_numeric(df["horizon"], errors="coerce").astype(int)

    df["eval_n_picks"] = pd.to_numeric(df.get("n_picks"), errors="coerce").fillna(0).astype(int)
    df["eval_n_valid"] = pd.to_numeric(df.get("n_valid"), errors="coerce").fillna(0).astype(int)

    df["avg_ret"] = pd.to_numeric(df.get("ret_h_mean"), errors="coerce")
    df["median_ret"] = pd.to_numeric(df.get("ret_h_median"), errors="coerce")
    df["hit_rate"] = pd.to_numeric(df.get("win_rate_h"), errors="coerce")

    df["coverage"] = df.apply(
        lambda r: float(r["eval_n_valid"] / r["eval_n_picks"]) if int(r["eval_n_picks"]) > 0 else 0.0,
        axis=1,
    )

    def _extra(r) -> str:
        payload = {
            "picks_trade_date": str(r.get("picks_trade_date", "")),
            "top1_ret_h": (None if pd.isna(r.get("top1_ret_h")) else float(r.get("top1_ret_h"))),
        }
        return json.dumps(payload, ensure_ascii=False)

    df["extra"] = df.apply(_extra, axis=1)

    df_db = df[
        [
            "trade_date",
            "version",
            "horizon",
            "coverage",
            "hit_rate",
            "avg_ret",
            "median_ret",
            "eval_n_picks",
            "eval_n_valid",
            "extra",
        ]
    ].copy()

    # Idempotent: delete then insert (range + horizons)
    with store.session() as con:
        con.execute("BEGIN;")
        try:
            # delete only targeted horizons within range
            # (if you run with horizons 5,10,20 it won't wipe other horizons)
            placeholders = ",".join(["?"] * len(horizons))
            con.execute(
                f"""
                DELETE FROM eval_batch_daily
                WHERE trade_date BETWEEN ? AND ?
                  AND horizon IN ({placeholders})
                """,
                (start, end, *horizons),
            )

            con.register("eval_df", df_db)
            con.execute(
                """
                INSERT INTO eval_batch_daily (
                  trade_date, version, horizon,
                  coverage, hit_rate, avg_ret, median_ret,
                  eval_n_picks, eval_n_valid,
                  extra
                )
                SELECT
                  trade_date, version, horizon,
                  coverage, hit_rate, avg_ret, median_ret,
                  eval_n_picks, eval_n_valid,
                  extra
                FROM eval_df
                """
            )
            con.unregister("eval_df")
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise

    print("[OK] multi-horizon eval written to DuckDB table eval_batch_daily.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="asof start date, e.g. 2025-12-20")
    ap.add_argument("--end", required=True, help="asof end date, e.g. 2026-01-14")
    ap.add_argument(
        "--horizons",
        default="5",
        help="comma list of trading-day horizons, e.g. 5,10,20 (default 5)",
    )
    ap.add_argument("--versions", default="V1,V2,V3,V4", help="comma list, default V1,V2,V3,V4")
    ap.add_argument("--topk", type=int, default=None, help="optional: evaluate only top-k picks per version")
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    horizons = _parse_horizons(args.horizons)
    versions = [v.strip() for v in args.versions.split(",") if v.strip()]
    topk = args.topk

    print("[OK] eval_batch started.")
    print(f"asof_range: {start} to {end}")
    print(f"horizons: {horizons}")
    print(f"versions: {versions}")
    print(f"topk: {topk}")

    cfg = load_settings(ROOT)
    store = DuckDBStore(cfg.store_db, ROOT / "src" / "alpha_tracker2" / "storage" / "schema.sql")
    store.init_schema()
    out_dir = _ensure_out_dir()

    # calendar covers max horizon lookback
    all_days = _get_trading_days(store, start, end, buffer_days=240)
    asof_days = [d for d in all_days if start <= d <= end]

    print(f"calendar_days_total_n: {len(all_days)} (includes lookback)")
    print(f"asof_trading_days_n: {len(asof_days)}")

    batch_rows = []

    for asof_date in asof_days:
        for h in horizons:
            picks_trade_date = _offset_trade_date(all_days, asof_date, h)
            if picks_trade_date is None:
                print(f"[SKIP] asof_date={asof_date} horizon={h} (not enough prior trading days)")
                continue

            picks = _load_picks(store, picks_trade_date, versions, topk=topk)
            if picks.empty:
                print(f"[SKIP] asof_date={asof_date} horizon={h} picks_trade_date={picks_trade_date} picks_rows=0")
                continue

            tickers = sorted(picks["ticker"].astype(str).unique().tolist())
            close0 = _load_close_map(store, picks_trade_date, tickers)
            close1 = _load_close_map(store, asof_date, tickers)

            detail_valid = _calc_detail(
                picks=picks,
                close0=close0,
                close1=close1,
                picks_trade_date=picks_trade_date,
                asof_date=asof_date,
                horizon=h,
            )

            summary = _summarize_one_day(
                picks_all=picks,
                detail_valid=detail_valid,
                asof_date=asof_date,
                picks_trade_date=picks_trade_date,
                horizon=h,
            )

            # export per-day files (optional but useful)
            detail_path = out_dir / f"model_eval_{h}d_asof_{asof_date}.csv"
            summary_path = out_dir / f"model_eval_{h}d_asof_summary_{asof_date}.csv"
            detail_valid.to_csv(detail_path, index=False, encoding="utf-8-sig")
            summary.to_csv(summary_path, index=False, encoding="utf-8-sig")

            if not summary.empty:
                batch_rows.append(summary)

            print(
                f"[OK] asof_date={asof_date} horizon={h} picks_trade_date={picks_trade_date} "
                f"picks_rows={len(picks)} valid_rows={len(detail_valid)} summary_rows={len(summary)}"
            )

    # concat batch
    if batch_rows:
        batch = pd.concat(batch_rows, ignore_index=True)
    else:
        batch = pd.DataFrame(
            columns=[
                "asof_date",
                "horizon",
                "picks_trade_date",
                "version",
                "n_picks",
                "n_valid",
                "ret_h_mean",
                "ret_h_median",
                "win_rate_h",
                "top1_ret_h",
            ]
        )

    # --- CSV exports (compat + multi-horizon) ---
    # 1) Multi-horizon export (new)
    mh_path = out_dir / f"model_eval_batch_summary_{start}_{end}.csv"
    batch.to_csv(mh_path, index=False, encoding="utf-8-sig")
    print(f"[OK] multi_horizon_batch_summary exported: {mh_path}")

    # 2) Backward-compatible 5d export (old name) if 5 in horizons
    if 5 in horizons:
        batch_5 = batch[batch["horizon"].astype(int) == 5].copy()
        legacy_path = out_dir / f"model_eval_5d_batch_summary_{start}_{end}.csv"
        batch_5.to_csv(legacy_path, index=False, encoding="utf-8-sig")
        print(f"[OK] legacy_5d_batch_summary exported: {legacy_path}")

        # Also keep old DB table eval_5d_batch_daily up-to-date for now (optional)
        # We won't write it here to avoid double-maintenance; dashboard now reads unified table in next step.

    # --- write to unified DuckDB table ---
    _write_eval_to_duckdb_unified(store=store, start=start, end=end, horizons=horizons, batch=batch)

    print("[OK] eval_batch passed.")


if __name__ == "__main__":
    main()
