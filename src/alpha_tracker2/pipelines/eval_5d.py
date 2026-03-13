from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


ROOT = Path(__file__).resolve().parents[3]


def _parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--asof",
        default=None,
        help="as-of date YYYY-MM-DD (default: latest trading day from prices_daily). "
             "Evaluation uses picks at (asof - horizon trading days).",
    )
    ap.add_argument("--horizon", type=int, default=5, help="forward trading days horizon, default=5")
    ap.add_argument("--versions", default="V1,V2,V3,V4", help="comma separated versions")
    ap.add_argument("--topk", type=int, default=None, help="optional: only evaluate top-k picks per version")
    return ap.parse_args()


def _to_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _fetch_trading_days(store: DuckDBStore, start: date, end: date) -> list[date]:
    sql = """
    SELECT DISTINCT trade_date
    FROM prices_daily
    WHERE trade_date BETWEEN ? AND ?
    ORDER BY trade_date
    """
    rows = store.fetchall(sql, (start, end))
    return [r[0] for r in rows]


def _latest_trading_day(store: DuckDBStore) -> date:
    sql = "SELECT MAX(trade_date) FROM prices_daily"
    rows = store.fetchall(sql, ())
    if not rows or rows[0][0] is None:
        raise RuntimeError("prices_daily is empty; cannot determine latest trading day.")
    return rows[0][0]


def _offset_trading_day(days: list[date], d: date, offset: int) -> date | None:
    """
    offset < 0 : previous trading day(s)
    offset > 0 : next trading day(s)
    """
    if d not in days:
        return None
    i = days.index(d)
    j = i + offset
    if j < 0 or j >= len(days):
        return None
    return days[j]


def _fetch_picks(store: DuckDBStore, picks_date: date, versions: list[str], topk: int | None) -> pd.DataFrame:
    placeholders = ",".join(["?"] * len(versions))
    sql = f"""
        SELECT trade_date, version, ticker, name, rank, score, reason
        FROM picks_daily
        WHERE trade_date = ?
          AND version IN ({placeholders})
        ORDER BY version, rank
    """
    rows = store.fetchall(sql, tuple([picks_date] + versions))
    df = pd.DataFrame(rows, columns=["trade_date", "version", "ticker", "name", "rank", "score", "reason"])

    if topk is not None and not df.empty:
        df = df[df["rank"].notna()].copy()
        df["rank"] = df["rank"].astype(int)
        df = df[df["rank"] <= int(topk)].copy()

    return df


def _fetch_close(store: DuckDBStore, d: date, tickers: list[str]) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame(columns=["trade_date", "ticker", "close"])
    placeholders = ",".join(["?"] * len(tickers))
    sql = f"""
        SELECT trade_date, ticker, close
        FROM prices_daily
        WHERE trade_date = ?
          AND ticker IN ({placeholders})
    """
    params = (d, *tickers)
    rows = store.fetchall(sql, params)
    if not rows:
        return pd.DataFrame(columns=["trade_date", "ticker", "close"])
    return pd.DataFrame(rows, columns=["trade_date", "ticker", "close"])


def _calc_detail(
    picks: pd.DataFrame,
    px0: pd.DataFrame,
    px1: pd.DataFrame,
    asof_date: date,
    picks_trade_date: date,
    horizon: int,
) -> pd.DataFrame:
    if picks.empty:
        return pd.DataFrame(
            columns=[
                "trade_date", "version", "ticker", "name", "rank", "score", "reason",
                "asof_date", "horizon", "picks_trade_date", "n_picks", "n_valid",
                "close_0", "close_1", "ret_h", "ret_5d",
            ]
        )

    d0 = px0.rename(columns={"close": "close_0"})
    d1 = px1.rename(columns={"close": "close_1"})

    out = picks.copy()
    out["ticker"] = out["ticker"].astype(str)

    out = out.merge(d0[["ticker", "close_0"]], on="ticker", how="left")
    out = out.merge(d1[["ticker", "close_1"]], on="ticker", how="left")

    out["asof_date"] = asof_date
    out["horizon"] = int(horizon)
    out["picks_trade_date"] = picks_trade_date

    out["ret_h"] = (out["close_1"] / out["close_0"]) - 1.0
    # 兼容字段：上游/旧逻辑常用 ret_5d
    out["ret_5d"] = out["ret_h"] if int(horizon) == 5 else out["ret_h"]

    # 只算有效行
    out = out.dropna(subset=["close_0", "close_1", "ret_h"]).reset_index(drop=True)

    # 每行都带 n_picks/n_valid（按 version）
    n_picks_map = picks.groupby("version", dropna=False).size().to_dict()
    n_valid_map = out.groupby("version", dropna=False).size().to_dict()
    out["n_picks"] = out["version"].map(lambda v: int(n_picks_map.get(v, 0)))
    out["n_valid"] = out["version"].map(lambda v: int(n_valid_map.get(v, 0)))

    return out


def _summary(detail: pd.DataFrame, asof_date: date, picks_trade_date: date, horizon: int, picks_all: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "asof_date",
        "horizon",
        "picks_trade_date",
        "version",
        "n_picks",
        "n_valid",
        "ret_h_mean",
        "ret_h_median",
        "hit_rate_h",
        "top1_ret_h",
    ]

    if picks_all is None or picks_all.empty:
        return pd.DataFrame(columns=cols)

    n_picks_map = picks_all.groupby("version", dropna=False).size().to_dict()

    if detail is None or detail.empty:
        # 仍然输出每个 version 一行，n_valid=0
        rows = []
        for v in sorted(picks_all["version"].dropna().unique().tolist()):
            rows.append(
                {
                    "asof_date": asof_date,
                    "horizon": int(horizon),
                    "picks_trade_date": picks_trade_date,
                    "version": v,
                    "n_picks": int(n_picks_map.get(v, 0)),
                    "n_valid": 0,
                    "ret_h_mean": float("nan"),
                    "ret_h_median": float("nan"),
                    "hit_rate_h": float("nan"),
                    "top1_ret_h": float("nan"),
                }
            )
        return pd.DataFrame(rows)[cols]

    def hit_rate(s: pd.Series) -> float:
        s = s.dropna()
        if len(s) == 0:
            return float("nan")
        return float((s > 0).mean())

    rows = []
    for v, d in detail.groupby("version", dropna=False):
        r = d["ret_h"].dropna()
        # top1：按 rank 最小的那只
        top1 = float("nan")
        if "rank" in d.columns and not d.empty:
            dd = d.sort_values("rank", na_position="last").head(1)
            if not dd.empty and pd.notna(dd["ret_h"].iloc[0]):
                top1 = float(dd["ret_h"].iloc[0])

        rows.append(
            {
                "asof_date": asof_date,
                "horizon": int(horizon),
                "picks_trade_date": picks_trade_date,
                "version": v,
                "n_picks": int(n_picks_map.get(v, 0)),
                "n_valid": int(len(r)),
                "ret_h_mean": float(r.mean()) if len(r) else float("nan"),
                "ret_h_median": float(r.median()) if len(r) else float("nan"),
                "hit_rate_h": hit_rate(d["ret_h"]),
                "top1_ret_h": top1,
            }
        )

    # 把“有 picks 但全无效”的版本也补齐
    picked_versions = set(picks_all["version"].dropna().unique().tolist())
    seen = set([x["version"] for x in rows])
    for v in sorted(picked_versions - seen):
        rows.append(
            {
                "asof_date": asof_date,
                "horizon": int(horizon),
                "picks_trade_date": picks_trade_date,
                "version": v,
                "n_picks": int(n_picks_map.get(v, 0)),
                "n_valid": 0,
                "ret_h_mean": float("nan"),
                "ret_h_median": float("nan"),
                "hit_rate_h": float("nan"),
                "top1_ret_h": float("nan"),
            }
        )

    return pd.DataFrame(rows)[cols].sort_values("version").reset_index(drop=True)


def main():
    args = _parse_args()

    cfg = load_settings(ROOT)
    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=ROOT / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )

    horizon = max(1, int(args.horizon))
    versions = [v.strip() for v in args.versions.split(",") if v.strip()]

    # asof 默认取 prices_daily 最新交易日
    asof_date = _to_date(args.asof) if args.asof else _latest_trading_day(store)

    # trading days：向前取足够 buffer（避免 horizon 不够）
    buf_start = asof_date - timedelta(days=180)
    trading_days = _fetch_trading_days(store, buf_start, asof_date)

    if asof_date not in trading_days:
        raise RuntimeError(f"asof_date {asof_date} not found in prices_daily trading calendar.")

    picks_trade_date = _offset_trading_day(trading_days, asof_date, offset=-horizon)
    if picks_trade_date is None:
        raise RuntimeError(f"Not enough trading-day history before {asof_date} for horizon={horizon}.")

    print("[OK] eval_5d started.")
    print("asof_date:", asof_date.isoformat())
    print("horizon:", horizon)
    print("picks_trade_date:", picks_trade_date.isoformat(), f"(asof - {horizon} trading days)")
    print("versions:", versions)
    print("topk:", args.topk)

    picks = _fetch_picks(store, picks_trade_date, versions, topk=args.topk)
    print("picks_rows:", len(picks))

    tickers = sorted(picks["ticker"].astype(str).unique().tolist()) if not picks.empty else []
    px0 = _fetch_close(store, picks_trade_date, tickers)
    px1 = _fetch_close(store, asof_date, tickers)

    detail = _calc_detail(
        picks=picks,
        px0=px0,
        px1=px1,
        asof_date=asof_date,
        picks_trade_date=picks_trade_date,
        horizon=horizon,
    )
    summary = _summary(
        detail=detail,
        asof_date=asof_date,
        picks_trade_date=picks_trade_date,
        horizon=horizon,
        picks_all=picks,
    )

    out_dir = ROOT / "data" / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 注意：这里是 “asof 单次评估”，文件名用 asof 防止歧义
    out_detail = out_dir / f"model_eval_{horizon}d_asof_{asof_date.isoformat()}.csv"
    out_sum = out_dir / f"model_eval_{horizon}d_asof_summary_{asof_date.isoformat()}.csv"

    detail.to_csv(out_detail, index=False, encoding="utf-8-sig")
    summary.to_csv(out_sum, index=False, encoding="utf-8-sig")

    print("[OK] eval_5d passed.")
    print("detail:", str(out_detail))
    print("summary:", str(out_sum))


if __name__ == "__main__":
    main()
