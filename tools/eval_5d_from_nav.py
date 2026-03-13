from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Tuple

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _parse_strategy_ids(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _get_all_strategy_ids(con: duckdb.DuckDBPyConnection, where_model: str) -> List[Tuple[str, str]]:
    # returns [(strategy_id, model_version)]
    if where_model:
        rows = con.execute(
            "SELECT strategy_id, model_version FROM strategies WHERE model_version=? ORDER BY strategy_id",
            [where_model],
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT strategy_id, model_version FROM strategies ORDER BY strategy_id",
        ).fetchall()
    return [(str(a), str(b)) for a, b in rows]


def _load_nav(con: duckdb.DuckDBPyConnection, strategy_id: str, start: str, end: str) -> pd.DataFrame:
    df = con.execute(
        """
        SELECT trade_date, nav, version
        FROM nav_daily
        WHERE strategy_id = ?
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date
        """,
        [strategy_id, start, end],
    ).df()
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def _calc_fwd_ret(df: pd.DataFrame, horizon: int = 5) -> pd.DataFrame:
    # expects columns: trade_date, nav
    out = df.copy()
    out["nav_fwd"] = out["nav"].shift(-horizon)
    out["ret_fwd"] = out["nav_fwd"] / out["nav"] - 1.0
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "data/store/alpha_tracker.duckdb"))
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--strategy_ids", default="", help="comma-separated; empty => load from strategies table")
    ap.add_argument("--where_model", default="", help="e.g. V1")
    ap.add_argument("--horizon", type=int, default=5)
    args = ap.parse_args()

    con = duckdb.connect(args.db)

    try:
        if args.strategy_ids.strip():
            sids = _parse_strategy_ids(args.strategy_ids)
            # version lookup from strategies table (best-effort)
            rows = con.execute(
                "SELECT strategy_id, model_version FROM strategies WHERE strategy_id IN (SELECT UNNEST(?))",
                [sids],
            ).fetchall()
            mv_map = {str(a): str(b) for a, b in rows}
            plan = [(sid, mv_map.get(sid, "")) for sid in sids]
        else:
            plan = _get_all_strategy_ids(con, args.where_model)

        if not plan:
            raise RuntimeError("No strategies matched for evaluation.")

        records = []

        for sid, mv in plan:
            navdf = _load_nav(con, sid, args.start, args.end)
            if navdf.empty:
                # no nav for this strategy in range -> skip
                continue

            # mv may be empty; fallback to navdf version if present
            version = mv or (str(navdf["version"].iloc[0]) if "version" in navdf.columns and len(navdf) > 0 else "")

            tmp = _calc_fwd_ret(navdf[["trade_date", "nav"]], horizon=int(args.horizon))
            # valid = those having forward data
            valid = tmp.dropna(subset=["ret_fwd"]).copy()

            if valid.empty:
                continue

            # daily aggregate (here each date has 1 ret; keep as 1-row per date)
            # coverage: valid / total
            coverage = float(len(valid)) / float(len(tmp)) if len(tmp) > 0 else 0.0
            hit_rate = float((valid["ret_fwd"] > 0).mean())

            avg_ret = float(valid["ret_fwd"].mean())
            median_ret = float(valid["ret_fwd"].median())
            eval_n = int(len(tmp))
            eval_valid = int(len(valid))

            # We write the SAME summary for each trade_date? No: table uses trade_date as PK key.
            # Design: store per-date fwd-ret stats would require more columns.
            # So here: store rolling-evaluable snapshot per trade_date as:
            # - avg_ret_5d / median_ret_5d computed over "future window available up to that date"
            # But simplest consistent design: write 1 row at start date (or end date) only.
            # However your schema implies DAILY rows; to keep it daily, we write per trade_date:
            # - coverage/hit_rate/avg/median over single observation ret_fwd at that date (degenerate but valid)
            # This is still useful for plotting distribution over time.

            # Per-date row:
            per = valid[["trade_date", "ret_fwd"]].copy()
            per["coverage"] = 1.0
            per["hit_rate"] = (per["ret_fwd"] > 0).astype(float)
            per["avg_ret_5d"] = per["ret_fwd"].astype(float)
            per["median_ret_5d"] = per["ret_fwd"].astype(float)
            per["eval_n_picks"] = 1
            per["eval_n_valid"] = 1
            per["version"] = version
            per["strategy_id"] = sid
            per["extra"] = json.dumps(
                {
                    "horizon": int(args.horizon),
                    "global_coverage": coverage,
                    "global_hit_rate": hit_rate,
                    "global_avg_ret": avg_ret,
                    "global_median_ret": median_ret,
                    "global_eval_n": eval_n,
                    "global_eval_valid": eval_valid,
                },
                ensure_ascii=False,
            )

            records.append(
                per[[
                    "trade_date",
                    "version",
                    "coverage",
                    "hit_rate",
                    "avg_ret_5d",
                    "median_ret_5d",
                    "eval_n_picks",
                    "eval_n_valid",
                    "extra",
                    "strategy_id",
                ]]
            )

        if not records:
            raise RuntimeError("No eval rows produced (check nav_daily data).")

        out = pd.concat(records, ignore_index=True)

        # write idempotent (trade_date + strategy_id)
        con.execute("BEGIN;")
        try:
            con.register("eval_df", out)
            con.execute(
                """
                DELETE FROM eval_5d_batch_daily
                WHERE (trade_date, strategy_id) IN (
                  SELECT trade_date, strategy_id FROM eval_df
                );
                """
            )
            con.execute(
                """
                INSERT INTO eval_5d_batch_daily (
                  trade_date, version, coverage, hit_rate, avg_ret_5d, median_ret_5d,
                  eval_n_picks, eval_n_valid, extra, strategy_id
                )
                SELECT
                  trade_date, version, coverage, hit_rate, avg_ret_5d, median_ret_5d,
                  eval_n_picks, eval_n_valid, extra, strategy_id
                FROM eval_df
                """
            )
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise

        out_dir = ROOT / "data/out/eval"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"eval_5d_batch_daily_{args.start}_{args.end}.csv"
        out.to_csv(out_path, index=False, encoding="utf-8-sig")
        print("[OK] eval exported:", out_path)
        print("[OK] rows_written:", len(out))

    finally:
        con.close()


if __name__ == "__main__":
    main()
