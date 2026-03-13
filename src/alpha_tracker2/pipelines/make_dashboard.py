from __future__ import annotations

import argparse
from pathlib import Path
import re

import duckdb
import pandas as pd

from alpha_tracker2.core.config import load_settings

ROOT = Path(__file__).resolve().parents[3]


def _parse_date(s: str) -> str:
    return str(pd.to_datetime(s).date())


def _pick_latest_nav_csv(out_dir: Path) -> Path | None:
    files = list(out_dir.glob("portfolio_nav_*_top*.csv"))
    if not files:
        return None

    pat = re.compile(r"portfolio_nav_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_top\d+\.csv$")
    scored = []
    for p in files:
        m = pat.search(p.name)
        if m:
            start_s, end_s = m.group(1), m.group(2)
            scored.append((end_s, start_s, p.stat().st_mtime, p))
        else:
            scored.append(("", "", p.stat().st_mtime, p))

    scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    return scored[0][3]


def _pick_latest_eval_csv(out_dir: Path) -> Path | None:
    # legacy 5d file
    files = list(out_dir.glob("model_eval_5d_batch_summary_*_*.csv"))
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


def _load_nav_from_duckdb(start: str, end: str) -> pd.DataFrame | None:
    cfg = load_settings(ROOT)
    db_path = cfg.store_db
    try:
        con = duckdb.connect(str(db_path))
    except Exception as e:
        print(f"[WARN] Cannot connect DuckDB: {db_path} ({e})")
        return None

    try:
        df = con.execute(
            """
            SELECT
              trade_date,
              picks_trade_date,
              asof_date,
              version,
              day_ret,
              nav,
              n_picks,
              n_valid
            FROM nav_daily
            WHERE trade_date BETWEEN ? AND ?
            ORDER BY version, trade_date
            """,
            (start, end),
        ).fetchdf()
        if df is None or df.empty:
            return None
        return df
    except Exception as e:
        print(f"[WARN] NAV from DuckDB nav_daily unavailable ({e}); will fallback to CSV.")
        return None
    finally:
        con.close()


def _load_eval_from_duckdb_unified(start: str, end: str, horizons: list[int]) -> pd.DataFrame | None:
    """
    Prefer eval from unified table eval_batch_daily.
    Return wide-format df keyed by (trade_date, version):
      - horizon=5 -> avg_ret_5d, hit_rate_5d, coverage_5d, eval_n_picks_5d, eval_n_valid_5d
      - horizon=10 -> avg_ret_10d, ...
      - horizon=20 -> avg_ret_20d, ...
    """
    cfg = load_settings(ROOT)
    db_path = cfg.store_db
    try:
        con = duckdb.connect(str(db_path))
    except Exception as e:
        print(f"[WARN] Cannot connect DuckDB: {db_path} ({e})")
        return None

    try:
        placeholders = ",".join(["?"] * len(horizons))
        df = con.execute(
            f"""
            SELECT
              trade_date,
              version,
              horizon,
              coverage,
              hit_rate,
              avg_ret,
              median_ret,
              eval_n_picks,
              eval_n_valid,
              extra
            FROM eval_batch_daily
            WHERE trade_date BETWEEN ? AND ?
              AND horizon IN ({placeholders})
            ORDER BY version, trade_date, horizon
            """,
            (start, end, *horizons),
        ).fetchdf()

        if df is None or df.empty:
            return None

        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date.astype(str)
        df["version"] = df["version"].astype(str)
        df["horizon"] = pd.to_numeric(df["horizon"], errors="coerce").astype(int)

        # build wide columns
        out = df[["trade_date", "version"]].drop_duplicates().reset_index(drop=True)

        for h in horizons:
            d = df[df["horizon"] == h].copy()
            if d.empty:
                continue

            d = d.rename(
                columns={
                    "avg_ret": f"avg_ret_{h}d",
                    "median_ret": f"median_ret_{h}d",
                    "hit_rate": f"hit_rate_{h}d",
                    "coverage": f"coverage_{h}d",
                    "eval_n_picks": f"eval_n_picks_{h}d",
                    "eval_n_valid": f"eval_n_valid_{h}d",
                }
            )
            keep = [
                "trade_date",
                "version",
                f"avg_ret_{h}d",
                f"median_ret_{h}d",
                f"hit_rate_{h}d",
                f"coverage_{h}d",
                f"eval_n_picks_{h}d",
                f"eval_n_valid_{h}d",
            ]
            out = out.merge(d[keep], on=["trade_date", "version"], how="left")

        return out
    except Exception as e:
        print(f"[WARN] Eval from DuckDB eval_batch_daily unavailable ({e}); will fallback.")
        return None
    finally:
        con.close()


def _load_eval_from_duckdb_legacy_5d(start: str, end: str) -> pd.DataFrame | None:
    """
    Fallback: old table eval_5d_batch_daily (only 5d).
    Return columns aligned to unified output for horizon=5.
    """
    cfg = load_settings(ROOT)
    db_path = cfg.store_db
    try:
        con = duckdb.connect(str(db_path))
    except Exception as e:
        print(f"[WARN] Cannot connect DuckDB: {db_path} ({e})")
        return None

    try:
        df = con.execute(
            """
            SELECT
              trade_date,
              version,
              coverage,
              hit_rate,
              avg_ret_5d,
              median_ret_5d,
              eval_n_picks,
              eval_n_valid,
              extra
            FROM eval_5d_batch_daily
            WHERE trade_date BETWEEN ? AND ?
            ORDER BY version, trade_date
            """,
            (start, end),
        ).fetchdf()

        if df is None or df.empty:
            return None

        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date.astype(str)
        df["version"] = df["version"].astype(str)

        df = df.rename(
            columns={
                "avg_ret_5d": "avg_ret_5d",
                "median_ret_5d": "median_ret_5d",
                "hit_rate": "hit_rate_5d",
                "coverage": "coverage_5d",
                "eval_n_picks": "eval_n_picks_5d",
                "eval_n_valid": "eval_n_valid_5d",
            }
        )
        keep = [
            "trade_date",
            "version",
            "avg_ret_5d",
            "median_ret_5d",
            "hit_rate_5d",
            "coverage_5d",
            "eval_n_picks_5d",
            "eval_n_valid_5d",
        ]
        return df[keep].copy()
    except Exception as e:
        print(f"[WARN] Eval from DuckDB eval_5d_batch_daily unavailable ({e}); will fallback to CSV.")
        return None
    finally:
        con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--topk", type=int, default=3)
    ap.add_argument("--eval_horizons", default="5,10,20", help="comma list, used only when reading unified eval table")
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    topk = int(args.topk)
    horizons = []
    for x in args.eval_horizons.split(","):
        x = x.strip()
        if x:
            horizons.append(int(x))
    horizons = [h for h in horizons if h > 0]
    if not horizons:
        horizons = [5]

    out_dir = ROOT / "data" / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    nav_path = out_dir / f"portfolio_nav_{start}_{end}_top{topk}.csv"
    batch_path = out_dir / f"model_eval_5d_batch_summary_{start}_{end}.csv"
    out_path = out_dir / f"alpha_dashboard_{start}_{end}_top{topk}.csv"

    # -----------------------------
    # NAV: DuckDB -> CSV fallback
    # -----------------------------
    nav_db = _load_nav_from_duckdb(start, end)
    if nav_db is not None:
        print("[OK] NAV loaded from DuckDB table nav_daily.")
        nav = nav_db.copy()
        nav_source = "duckdb:nav_daily"
    else:
        if not nav_path.exists():
            latest = _pick_latest_nav_csv(out_dir)
            if latest is None:
                raise FileNotFoundError(
                    f"Missing: {nav_path}\n"
                    f"And no portfolio_nav_*.csv found in {out_dir}\n"
                    f"Run portfolio_nav first for this range."
                )
            print(f"[WARN] Missing exact NAV: {nav_path}")
            print(f"[WARN] Fallback to latest NAV: {latest}")
            nav_path = latest
        nav = pd.read_csv(nav_path)
        nav_source = f"csv:{nav_path}"

    # -----------------------------
    # EVAL: unified DuckDB -> legacy DuckDB -> CSV fallback
    # -----------------------------
    eval_wide = _load_eval_from_duckdb_unified(start, end, horizons=horizons)
    if eval_wide is not None:
        print("[OK] EVAL loaded from DuckDB table eval_batch_daily (wide).")
        batch_wide = eval_wide
        eval_source = "duckdb:eval_batch_daily"
    else:
        eval_legacy = _load_eval_from_duckdb_legacy_5d(start, end)
        if eval_legacy is not None:
            print("[OK] EVAL loaded from DuckDB table eval_5d_batch_daily (legacy).")
            batch_wide = eval_legacy
            eval_source = "duckdb:eval_5d_batch_daily"
        else:
            # final fallback: legacy CSV
            if not batch_path.exists():
                latest_eval = _pick_latest_eval_csv(out_dir)
                if latest_eval is None:
                    raise FileNotFoundError(
                        f"Missing: {batch_path}\n"
                        f"And no model_eval_5d_batch_summary_*.csv found in {out_dir}\n"
                        f"Run eval_5d_batch first for this range."
                    )
                print(f"[WARN] Missing exact EVAL summary: {batch_path}")
                print(f"[WARN] Fallback to latest EVAL summary: {latest_eval}")
                batch_path = latest_eval

            batch = pd.read_csv(batch_path)
            # normalize to wide horizon=5 columns
            if "asof_date" in batch.columns:
                batch["trade_date"] = pd.to_datetime(batch["asof_date"], errors="coerce").dt.date.astype(str)
            elif "trade_date" in batch.columns:
                batch["trade_date"] = pd.to_datetime(batch["trade_date"], errors="coerce").dt.date.astype(str)
            else:
                raise RuntimeError("CSV eval summary missing asof_date/trade_date")

            batch = batch.dropna(subset=["trade_date", "version"]).copy()
            batch["version"] = batch["version"].astype(str)

            # If CSV contains horizon column, keep only 5 for dashboard default
            if "horizon" in batch.columns:
                batch = batch[pd.to_numeric(batch["horizon"], errors="coerce").fillna(5).astype(int) == 5].copy()

            # rename legacy metrics into wide 5d columns
            rename_map = {}
            if "n_picks" in batch.columns:
                rename_map["n_picks"] = "eval_n_picks_5d"
            if "n_valid" in batch.columns:
                rename_map["n_valid"] = "eval_n_valid_5d"
            if "ret_h_mean" in batch.columns:
                rename_map["ret_h_mean"] = "avg_ret_5d"
            if "ret_h_median" in batch.columns:
                rename_map["ret_h_median"] = "median_ret_5d"
            if "win_rate_h" in batch.columns:
                rename_map["win_rate_h"] = "hit_rate_5d"
            batch = batch.rename(columns=rename_map)

            # coverage_5d
            if "coverage_5d" not in batch.columns:
                if "eval_n_picks_5d" in batch.columns and "eval_n_valid_5d" in batch.columns:
                    batch["coverage_5d"] = batch.apply(
                        lambda r: float(r["eval_n_valid_5d"] / r["eval_n_picks_5d"]) if r["eval_n_picks_5d"] else 0.0,
                        axis=1,
                    )
                else:
                    batch["coverage_5d"] = pd.NA

            keep = [
                "trade_date",
                "version",
                "avg_ret_5d",
                "median_ret_5d",
                "hit_rate_5d",
                "coverage_5d",
                "eval_n_picks_5d",
                "eval_n_valid_5d",
            ]
            batch_wide = batch[[c for c in keep if c in batch.columns]].copy()
            eval_source = f"csv:{batch_path}"

    # -----------------------------
    # Validate + normalize NAV
    # -----------------------------
    required_nav = {"trade_date", "version", "day_ret", "nav"}
    missing_nav = [c for c in required_nav if c not in nav.columns]
    if missing_nav:
        raise RuntimeError(f"NAV missing columns {missing_nav} (source={nav_source})")

    nav["trade_date"] = pd.to_datetime(nav["trade_date"], errors="coerce").dt.date.astype(str)
    nav = nav.dropna(subset=["trade_date", "version"])
    nav["version"] = nav["version"].astype(str)

    if "n_picks" not in nav.columns:
        nav["n_picks"] = 0
    if "n_valid" not in nav.columns:
        nav["n_valid"] = 0

    nav["n_picks"] = pd.to_numeric(nav["n_picks"], errors="coerce").fillna(0).astype(int)
    nav["n_valid"] = pd.to_numeric(nav["n_valid"], errors="coerce").fillna(0).astype(int)
    nav["day_ret"] = pd.to_numeric(nav["day_ret"], errors="coerce").fillna(0.0)
    nav["nav"] = pd.to_numeric(nav["nav"], errors="coerce").fillna(1.0)

    # Optional columns (warning only; OK)
    for c in ["picks_trade_date", "asof_date"]:
        if c in nav.columns:
            nav[c] = pd.to_datetime(nav[c], errors="coerce").dt.date.astype("string")

    nav = nav[(nav["trade_date"] >= start) & (nav["trade_date"] <= end)].copy()
    if nav.empty:
        raise RuntimeError(f"NAV has no rows inside requested range {start}~{end} (source={nav_source})")

    # -----------------------------
    # Normalize eval wide keys
    # -----------------------------
    batch_wide = batch_wide.copy()
    if "trade_date" not in batch_wide.columns or "version" not in batch_wide.columns:
        raise RuntimeError(f"Eval wide is missing merge keys trade_date/version (source={eval_source})")
    batch_wide["trade_date"] = pd.to_datetime(batch_wide["trade_date"], errors="coerce").dt.date.astype(str)
    batch_wide = batch_wide.dropna(subset=["trade_date", "version"]).copy()
    batch_wide["version"] = batch_wide["version"].astype(str)

    # -----------------------------
    # Merge dashboard
    # -----------------------------
    dash = nav.merge(batch_wide, on=["trade_date", "version"], how="left")
    dash.to_csv(out_path, index=False, encoding="utf-8-sig")

    # -----------------------------
    # Console summary (keep old behavior)
    # -----------------------------
    dash_sorted = dash.sort_values(["version", "trade_date"]).reset_index(drop=True)

    last_nav = (
        dash_sorted.groupby("version", as_index=False)
        .tail(1)[["version", "trade_date", "nav"]]
        .rename(columns={"trade_date": "last_date", "nav": "nav_last"})
    )

    eff_mask = dash_sorted.groupby("version").cumcount() >= 1
    dash_eff = dash_sorted.loc[eff_mask].copy()

    stats = (
        dash_eff.groupby("version")
        .agg(
            days=("trade_date", "count"),
            valid_days=("n_valid", lambda s: int((pd.to_numeric(s, errors="coerce").fillna(0) > 0).sum())),
            day_ret_mean=("day_ret", "mean"),
            day_ret_std=("day_ret", "std"),
        )
        .reset_index()
    )

    all_versions = sorted(dash["version"].dropna().unique().tolist())
    if all_versions:
        stats = stats.set_index("version")
        for v in all_versions:
            if v not in stats.index:
                stats.loc[v] = {
                    "days": 0,
                    "valid_days": 0,
                    "day_ret_mean": float("nan"),
                    "day_ret_std": float("nan"),
                }
        stats = stats.reset_index()

    dd = []
    for v, dfv in dash_sorted.groupby("version"):
        x = pd.to_numeric(dfv["nav"], errors="coerce").fillna(1.0).values
        peak = 1.0
        mdd = 0.0
        for val in x:
            if val > peak:
                peak = val
            mdd = min(mdd, val / peak - 1.0)
        dd.append((v, float(mdd)))
    dd = pd.DataFrame(dd, columns=["version", "max_drawdown"])

    summary = (
        stats.merge(dd, on="version", how="left")
        .merge(last_nav[["version", "nav_last"]], on="version", how="left")
    )

    summary["day_ret_std"] = pd.to_numeric(summary["day_ret_std"], errors="coerce").fillna(0.0)
    summary["nav_last"] = pd.to_numeric(summary["nav_last"], errors="coerce")
    summary["max_drawdown"] = pd.to_numeric(summary["max_drawdown"], errors="coerce").fillna(0.0)

    cov = summary.apply(lambda r: (r["valid_days"] / r["days"]) if r["days"] else 0.0, axis=1)

    summary["score_rank"] = (
        summary["nav_last"].rank(ascending=False, method="min")
        + summary["max_drawdown"].rank(ascending=False, method="min")
        + summary["day_ret_std"].rank(ascending=True, method="min")
        + cov.rank(ascending=False, method="min")
    )
    summary = summary.sort_values(["score_rank", "nav_last"], ascending=[True, False])

    print("[OK] make_dashboard passed.")
    print("dashboard:", out_path)
    print("nav_source:", nav_source)
    print("eval_source:", eval_source)
    print("\n=== Version summary (ranked) ===")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
