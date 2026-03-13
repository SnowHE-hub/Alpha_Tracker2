from __future__ import annotations

import argparse
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore

ROOT = Path(__file__).resolve().parents[3]


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_versions(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _parse_weights(s: str, versions: List[str]) -> Dict[str, float]:
    """
    weights string: "V1=0.2,V2=0.2,V3=0.4,V4=0.2"
    if empty -> equal weights among given versions
    """
    if not s:
        w = 1.0 / max(len(versions), 1)
        return {v: w for v in versions}

    out: Dict[str, float] = {}
    for part in s.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        v = v.strip()
        try:
            out[k] = float(v)
        except Exception:
            continue

    for v in versions:
        out.setdefault(v, 0.0)

    ssum = sum(out.get(v, 0.0) for v in versions)
    if ssum > 0:
        out = {v: out.get(v, 0.0) / ssum for v in versions}
    else:
        w = 1.0 / max(len(versions), 1)
        out = {v: w for v in versions}
    return out


def _zscore(x: pd.Series) -> pd.Series:
    x = pd.to_numeric(x, errors="coerce")
    mu = x.mean()
    sd = x.std(ddof=0)
    if sd is None or sd == 0 or pd.isna(sd):
        return x * 0.0
    return (x - mu) / sd


def _get_trading_days(store: DuckDBStore, start: date, end: date) -> List[date]:
    sql = """
    SELECT DISTINCT trade_date
    FROM prices_daily
    WHERE trade_date BETWEEN ? AND ?
    ORDER BY trade_date
    """
    rows = store.fetchall(sql, (start, end))
    return [r[0] for r in rows]


def _load_base_picks_one_day(
    store: DuckDBStore,
    trade_date: date,
    versions: List[str],
    topk: int,
) -> pd.DataFrame:
    """
    Load V1-V4 (or given versions) topk rows from picks_daily for a signal day.
    """
    placeholders = ",".join(["?"] * len(versions))
    sql = f"""
    SELECT trade_date, version, ticker, name, rank, score, reason
    FROM picks_daily
    WHERE trade_date = ?
      AND version IN ({placeholders})
      AND rank IS NOT NULL
      AND rank <= ?
    """
    rows = store.fetchall(sql, (trade_date, *versions, int(topk)))
    if not rows:
        return pd.DataFrame(columns=["trade_date", "version", "ticker", "name", "rank", "score", "reason"])

    df = pd.DataFrame(rows, columns=["trade_date", "version", "ticker", "name", "rank", "score", "reason"])
    df["ticker"] = df["ticker"].astype(str)
    df["version"] = df["version"].astype(str)
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    return df


def _build_ensemble_for_day(
    base: pd.DataFrame,
    signal_date: date,
    versions: List[str],
    weights: Dict[str, float],
    topk: int,
    min_agree: int,
) -> pd.DataFrame:
    """
    Build ENS picks for one signal_date based on agreement + weighted zscore aggregation.

    Returns DataFrame columns:
      trade_date, version(ENS), ticker, name, score, rank, reason, score_100, thr_value, pass_thr, picked_by
    """
    if base.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "version",
                "ticker",
                "name",
                "score",
                "rank",
                "reason",
                "score_100",
                "thr_value",
                "pass_thr",
                "picked_by",
            ]
        )

    df = base.copy()
    df["score_norm"] = 0.0
    for v in versions:
        m = df["version"] == v
        if m.any():
            df.loc[m, "score_norm"] = _zscore(df.loc[m, "score"]).astype(float)

    # agreement per ticker
    agree = df.groupby("ticker", as_index=False).agg(
        name=("name", "max"),
        agree=("version", "nunique"),
    )
    agree = agree[agree["agree"] >= int(min_agree)].copy()
    if agree.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "version",
                "ticker",
                "name",
                "score",
                "rank",
                "reason",
                "score_100",
                "thr_value",
                "pass_thr",
                "picked_by",
            ]
        )

    # weighted sum of normalized scores
    ticker_scores: Dict[str, float] = {}
    ticker_components: Dict[str, Dict[str, float]] = {}

    for tkr, sub in df.groupby("ticker"):
        tkr = str(tkr)
        s = 0.0
        comps: Dict[str, float] = {}
        for _, r in sub.iterrows():
            v = str(r["version"])
            w = float(weights.get(v, 0.0))
            val = float(r["score_norm"]) if pd.notna(r["score_norm"]) else 0.0
            s += w * val
            comps[v] = val
        ticker_scores[tkr] = float(s)
        ticker_components[tkr] = comps

    agree["ens_score"] = agree["ticker"].astype(str).map(ticker_scores).astype(float)
    agree["trade_date"] = signal_date

    agree = agree.sort_values(["ens_score", "agree"], ascending=[False, False]).reset_index(drop=True)
    agree["rank"] = agree.index + 1
    agree = agree[agree["rank"] <= int(topk)].copy()

    # score_100 for display
    smin = agree["ens_score"].min()
    smax = agree["ens_score"].max()
    if pd.isna(smin) or pd.isna(smax) or smax == smin:
        agree["score_100"] = 50.0
    else:
        agree["score_100"] = (agree["ens_score"] - smin) / (smax - smin) * 100.0

    def _reason_row(r):
        tkr = str(r["ticker"])
        comps = ticker_components.get(tkr, {})
        parts = []
        for v in versions:
            if v in comps:
                parts.append(f"{v}:{comps[v]:.3f}*w{weights.get(v,0.0):.2f}")
        meta = {"agree": int(r["agree"]), "weights": weights, "components": comps}
        return f"ENS(raw) agree={int(r['agree'])} | " + " ".join(parts) + " | " + json.dumps(meta, ensure_ascii=False)

    agree["reason"] = agree.apply(_reason_row, axis=1)

    out = pd.DataFrame(
        {
            "trade_date": agree["trade_date"],
            "version": "ENS",
            "ticker": agree["ticker"].astype(str),
            "name": agree["name"],
            "score": agree["ens_score"].astype(float),
            "rank": agree["rank"].astype(int),
            "reason": agree["reason"].astype(str),
            "score_100": pd.to_numeric(agree["score_100"], errors="coerce"),
            "thr_value": None,
            "pass_thr": None,
            "picked_by": "ensemble",
        }
    )
    return out


def _streak_filter(
    daily_candidates: Dict[date, pd.DataFrame],
    signal_date: date,
    streak_k: int,
    topk: int,
) -> pd.DataFrame:
    """
    Keep tickers that appear in candidates for the last `streak_k` consecutive trading days ending at signal_date.
    daily_candidates[d] must contain columns: ticker, score, name, reason, agree
    We output at most topk tickers ranked by latest day's score (signal_date).
    """
    if streak_k <= 1:
        # no filter
        cur = daily_candidates.get(signal_date)
        if cur is None or cur.empty:
            return pd.DataFrame()
        return cur.copy()

    # ensure we have consecutive days available in dict
    days_sorted = sorted(daily_candidates.keys())
    if signal_date not in daily_candidates:
        return pd.DataFrame()

    # find index of signal_date and take last streak_k days
    i = days_sorted.index(signal_date)
    if i - (streak_k - 1) < 0:
        return pd.DataFrame()
    window = days_sorted[i - (streak_k - 1) : i + 1]

    # intersection of tickers across window
    sets = []
    for d in window:
        df = daily_candidates.get(d)
        if df is None or df.empty:
            return pd.DataFrame()
        sets.append(set(df["ticker"].astype(str).tolist()))
    common = set.intersection(*sets) if sets else set()
    if not common:
        return pd.DataFrame()

    cur = daily_candidates[signal_date].copy()
    cur["ticker"] = cur["ticker"].astype(str)
    cur = cur[cur["ticker"].isin(common)].copy()
    if cur.empty:
        return pd.DataFrame()

    # rank by latest score desc
    cur = cur.sort_values(["score"], ascending=[False]).reset_index(drop=True)
    cur = cur.head(int(topk)).copy()
    return cur


def _write_picks_daily(store: DuckDBStore, df: pd.DataFrame) -> int:
    """
    Insert ENS rows into picks_daily (idempotent: delete same trade_date+version first).
    """
    if df is None or df.empty:
        return 0

    trade_date = df["trade_date"].iloc[0]
    version = "ENS"

    with store.session() as con:
        con.execute("BEGIN;")
        try:
            con.execute("DELETE FROM picks_daily WHERE trade_date = ? AND version = ?", (trade_date, version))
            con.register("ens_df", df)
            con.execute(
                """
                INSERT INTO picks_daily (
                  trade_date, version, ticker, name,
                  score, rank, reason, score_100,
                  thr_value, pass_thr, picked_by
                )
                SELECT
                  trade_date, version, ticker, name,
                  score, rank, reason, score_100,
                  thr_value, pass_thr, picked_by
                FROM ens_df
                """
            )
            con.unregister("ens_df")
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise

    return int(len(df))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade_date", required=True, help="signal date (picks_daily.trade_date)")
    ap.add_argument("--versions", default="V1,V2,V3,V4")
    ap.add_argument("--topk", type=int, default=3)
    ap.add_argument("--min_agree", type=int, default=2)
    ap.add_argument("--weights", default="", help='optional weights: "V1=0.2,V2=0.2,V3=0.4,V4=0.2"')

    # NEW: streak mode
    ap.add_argument("--signal_mode", default="raw", choices=["raw", "streak"], help="raw: today only; streak: require consecutive days")
    ap.add_argument("--streak_k", type=int, default=2, help="consecutive days required in streak mode")
    ap.add_argument("--lookback_days", type=int, default=90, help="calendar days to look back for streak mode trading days")

    args = ap.parse_args()

    trade_date = _parse_date(args.trade_date)
    versions = _parse_versions(args.versions)
    topk = int(args.topk)
    min_agree = int(args.min_agree)
    weights = _parse_weights(args.weights, versions)

    signal_mode = str(args.signal_mode)
    streak_k = int(args.streak_k)
    lookback_days = int(args.lookback_days)

    print("[OK] score_ensemble started.")
    print("trade_date:", trade_date)
    print("versions:", versions)
    print("topk:", topk)
    print("min_agree:", min_agree)
    print("weights:", weights)
    print("signal_mode:", signal_mode)
    if signal_mode == "streak":
        print("streak_k:", streak_k)
        print("lookback_days:", lookback_days)

    cfg = load_settings(ROOT)
    store = DuckDBStore(cfg.store_db, ROOT / "src" / "alpha_tracker2" / "storage" / "schema.sql")
    store.init_schema()

    if signal_mode == "raw":
        base = _load_base_picks_one_day(store, trade_date, versions, topk=topk)
        if base.empty:
            print("[WARN] No base picks found. ENS not written.")
            return
        ens = _build_ensemble_for_day(base, trade_date, versions, weights, topk=topk, min_agree=min_agree)
        if ens.empty:
            print("[WARN] ENS empty after min_agree filter. Nothing written.")
            return
        n = _write_picks_daily(store, ens)
        print("[OK] score_ensemble passed.")
        print("rows_written:", n)
        print(ens[["trade_date", "version", "rank", "ticker", "score"]].to_string(index=False))
        return

    # -------- streak mode --------
    # Build daily raw candidates for a window ending at trade_date,
    # then apply intersection across last streak_k trading days.
    start_cal = trade_date - timedelta(days=lookback_days)
    trading_days = _get_trading_days(store, start_cal, trade_date)
    if trade_date not in trading_days:
        print("[WARN] trade_date not in trading calendar (prices_daily). ENS not written.")
        return

    # only need last (streak_k * 3) days to tolerate gaps where picks missing
    # but we compute for all trading days in window to keep logic simple.
    daily_candidates: Dict[date, pd.DataFrame] = {}

    for d in trading_days:
        base = _load_base_picks_one_day(store, d, versions, topk=topk)
        if base.empty:
            continue
        ens_day = _build_ensemble_for_day(base, d, versions, weights, topk=topk, min_agree=min_agree)
        if ens_day.empty:
            continue
        # store minimal columns for streak filter
        cand = ens_day[["ticker", "name", "score", "reason"]].copy()
        daily_candidates[d] = cand

    if trade_date not in daily_candidates:
        print("[WARN] No raw ENS candidates for trade_date. ENS not written.")
        return

    streak_df = _streak_filter(daily_candidates, trade_date, streak_k=streak_k, topk=topk)
    if streak_df is None or streak_df.empty:
        print("[WARN] ENS empty after streak filter. Nothing written.")
        return

    streak_df = streak_df.copy().reset_index(drop=True)
    streak_df["trade_date"] = trade_date
    streak_df["version"] = "ENS"
    streak_df["rank"] = streak_df.index + 1

    # score_100 for display
    smin = streak_df["score"].min()
    smax = streak_df["score"].max()
    if pd.isna(smin) or pd.isna(smax) or smax == smin:
        streak_df["score_100"] = 50.0
    else:
        streak_df["score_100"] = (streak_df["score"] - smin) / (smax - smin) * 100.0


    streak_df["reason"] = streak_df.apply(
        lambda r: f"ENS(streak={streak_k}) | {r['reason']}", axis=1
    )

    out = pd.DataFrame(
        {
            "trade_date": streak_df["trade_date"],
            "version": "ENS",
            "ticker": streak_df["ticker"].astype(str),
            "name": streak_df["name"],
            "score": pd.to_numeric(streak_df["score"], errors="coerce"),
            "rank": streak_df["rank"].astype(int),
            "reason": streak_df["reason"].astype(str),
            "score_100": pd.to_numeric(streak_df["score_100"], errors="coerce"),
            "thr_value": None,
            "pass_thr": None,
            "picked_by": "ensemble_streak",
        }
    )

    n = _write_picks_daily(store, out)
    print("[OK] score_ensemble passed.")
    print("rows_written:", n)
    print(out[["trade_date", "version", "rank", "ticker", "score"]].to_string(index=False))


if __name__ == "__main__":
    main()
