from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def _get_picks_daily_columns(store: DuckDBStore) -> List[str]:
    """
    DuckDB: PRAGMA table_info('table') returns:
      cid, name, type, notnull, dflt_value, pk
    """
    rows = store.fetchall("PRAGMA table_info('picks_daily');")
    return [r[1] for r in rows] if rows else []



def _fetch_version_df(
    store: DuckDBStore,
    trade_date,
    version: str,
    has_reason: bool,
) -> pd.DataFrame:
    # UNIVERSE: 展示层只取最必要字段，避免误导
    if version == "UNIVERSE":
        cols = ["ticker", "name", "rank"]
        select_cols = ", ".join(cols)
        sql = f"""
            SELECT {select_cols}
            FROM picks_daily
            WHERE trade_date = ? AND version = ?
            ORDER BY rank ASC;
        """
        rows = store.fetchall(sql, (trade_date, version))
        df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
        if not df.empty:
            df["rank"] = pd.to_numeric(df["rank"], errors="coerce").astype("Int64")
        return df

    # Scoring versions
    cols = ["ticker", "name", "rank", "score"]
    if has_reason:
        cols.append("reason")

    select_cols = ", ".join(cols)
    sql = f"""
        SELECT {select_cols}
        FROM picks_daily
        WHERE trade_date = ? AND version = ?
        ORDER BY rank ASC;
    """
    rows = store.fetchall(sql, (trade_date, version))
    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

    if not df.empty:
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce").astype("Int64")
        df["score"] = pd.to_numeric(df["score"], errors="coerce").astype(float)

    return df


def _print_table(title: str, df: pd.DataFrame, max_rows: int) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)
    if df is None or df.empty:
        print("(empty)")
        return
    show = df.head(max_rows).copy()
    print(show.to_string(index=False))


def main() -> None:
    ap = argparse.ArgumentParser(description="Show picks (UNIVERSE + V1..V4) for a trade_date.")
    ap.add_argument("--date", type=str, default=None, help="trade_date, e.g. 2026-01-14 (default: latest trading day)")
    ap.add_argument(
        "--versions",
        type=str,
        default="UNIVERSE,V1",
        help="comma-separated versions to show, default: UNIVERSE,V1",
    )
    ap.add_argument("--top", type=int, default=20, help="top N rows to print per version (default: 20)")
    ap.add_argument("--out", type=str, default=None, help="optional: output csv path for comparison table")
    ap.add_argument(
        "--overlap-only",
        action="store_true",
        help="only show tickers that appear in ALL scoring versions (exclude UNIVERSE)",
    )
    args = ap.parse_args()

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

    versions = [v.strip() for v in args.versions.split(",") if v.strip()]
    if not versions:
        raise ValueError("No versions specified. Example: --versions UNIVERSE,V1,V2")

    cols = _get_picks_daily_columns(store)
    has_reason = "reason" in cols

    # 1) 计数汇总
    cnt_rows = store.fetchall(
        """
        SELECT version, COUNT(*) AS n
        FROM picks_daily
        WHERE trade_date = ?
        GROUP BY version
        ORDER BY version;
        """,
        (trade_date,),
    )
    cnt_df = pd.DataFrame(cnt_rows, columns=["version", "n"]) if cnt_rows else pd.DataFrame(columns=["version", "n"])
    print("[OK] show_picks started.")
    print("trade_date:", trade_date)
    if cnt_df.empty:
        print("[WARN] picks_daily has no rows for this trade_date.")
    else:
        print("counts:\n" + cnt_df.to_string(index=False))

    # 2) 先抓取所有版本（先不打印，后面要做 name 回填）
    version_dfs: Dict[str, pd.DataFrame] = {}
    for v in versions:
        dfv = _fetch_version_df(store, trade_date, v, has_reason=has_reason)
        version_dfs[v] = dfv

    # 3) 对比表前：拿 UNIVERSE，并用其回填各 scoring 版本的 name（如果为空）
    universe_df = version_dfs.get("UNIVERSE")

    if universe_df is not None and not universe_df.empty:
        uni_map = universe_df[["ticker", "name"]].copy()
        uni_map["ticker"] = uni_map["ticker"].astype(str)

        for v, dfv in list(version_dfs.items()):
            if v == "UNIVERSE" or dfv is None or dfv.empty:
                continue
            if "ticker" not in dfv.columns:
                continue

            tmp = dfv.copy()
            tmp["ticker"] = tmp["ticker"].astype(str)

            if "name" not in tmp.columns:
                tmp["name"] = None

            tmp = tmp.merge(uni_map, on="ticker", how="left", suffixes=("", "_uni"))
            tmp["name"] = tmp["name"].fillna(tmp["name_uni"])
            tmp = tmp.drop(columns=["name_uni"])
            version_dfs[v] = tmp

    # 2.5) 回填完成后，再逐版本打印 TopN
    for v in versions:
        _print_table(f"{v} (top {args.top})", version_dfs.get(v), max_rows=args.top)

    # 4) 构建 base（优先用 UNIVERSE；否则用并集）
    if universe_df is None or universe_df.empty:
        all_tickers = set()
        for v, dfv in version_dfs.items():
            if dfv is not None and not dfv.empty and "ticker" in dfv.columns:
                all_tickers.update(dfv["ticker"].astype(str).tolist())

        base = pd.DataFrame({"ticker": sorted(all_tickers)})
        base["name"] = None
    else:
        base = universe_df[["ticker", "name"]].copy()

    scoring_versions = [v for v in versions if v != "UNIVERSE"]


    # overlap-only：只保留所有 scoring versions 都出现的 ticker
    if args.overlap_only and scoring_versions:
        sets = []
        for v in scoring_versions:
            dfv = version_dfs.get(v)
            if dfv is None or dfv.empty:
                sets.append(set())
            else:
                sets.append(set(dfv["ticker"].astype(str).tolist()))
        common = set.intersection(*sets) if sets else set()
        base = base[base["ticker"].astype(str).isin(common)].copy()

    # merge each version
    comp = base.copy()
    comp["ticker"] = comp["ticker"].astype(str)

    for v in scoring_versions:
        dfv = version_dfs.get(v)
        if dfv is None or dfv.empty:
            comp[f"{v}_rank"] = pd.NA
            comp[f"{v}_score"] = pd.NA
            continue
        tmp = dfv.copy()
        tmp["ticker"] = tmp["ticker"].astype(str)
        tmp = tmp[["ticker", "rank", "score"]].rename(columns={"rank": f"{v}_rank", "score": f"{v}_score"})
        comp = comp.merge(tmp, on="ticker", how="left")

    # 排序规则：优先按第一个 scoring version 的 rank，其次 ticker
    if scoring_versions and f"{scoring_versions[0]}_rank" in comp.columns:
        comp = comp.sort_values(by=[f"{scoring_versions[0]}_rank", "ticker"], na_position="last")
    else:
        comp = comp.sort_values(by=["ticker"])

    _print_table("COMPARE (wide)", comp, max_rows=max(args.top, 50))

    # 4) 可选输出 CSV
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        comp.to_csv(out_path, index=False, encoding="utf-8-sig")
        print("\n[OK] compare csv saved:", str(out_path))

    print("\n[OK] show_picks passed.")
    print("db:", s.store_db)


if __name__ == "__main__":
    main()
