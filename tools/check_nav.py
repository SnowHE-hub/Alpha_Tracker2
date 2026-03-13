# check_nav.py
# Usage:
#   python check_nav.py
#   python check_nav.py --date 2026-01-14
#   python check_nav.py --top 10

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, List, Tuple

import duckdb
import pandas as pd


def _project_root() -> Path:
    # check_nav.py is expected at repo root
    return Path(__file__).resolve().parent


def _default_db_path() -> Path:
    return _project_root() / "data" / "store" / "alpha_tracker.duckdb"


def _connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    if not db_path.exists():
        raise FileNotFoundError(f"[ERROR] DuckDB not found: {db_path}")
    return duckdb.connect(str(db_path))


def _list_tables(con: duckdb.DuckDBPyConnection) -> List[str]:
    df = con.execute("SHOW TABLES;").fetchdf()
    if df.empty:
        return []
    col = df.columns[0]
    return [str(x) for x in df[col].tolist()]


def _find_nav_tables(table_names: List[str]) -> List[str]:
    keywords = ("nav", "portfolio", "equity", "perf", "performance")
    out = [t for t in table_names if any(k in t.lower() for k in keywords)]
    return sorted(set(out))


def _table_info(con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    return con.execute(f"PRAGMA table_info('{table}');").fetchdf()


def _detect_col(cols: List[str], candidates: Tuple[str, ...]) -> Optional[str]:
    lower_map = {c.lower(): c for c in cols}
    for c in candidates:
        if c in lower_map:
            return lower_map[c]
    return None


def _summarize_nav_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    only_date: Optional[str],
    top: int,
) -> None:
    info = _table_info(con, table)
    cols = info["name"].tolist() if "name" in info.columns else []
    if not cols:
        print(f"\n[TABLE] {table}\n[WARN] no columns found, skip.")
        return

    # Heuristic column detection
    date_col = _detect_col(cols, ("trade_date", "date", "dt", "asof", "day"))
    ver_col = _detect_col(cols, ("version", "strategy", "model", "portfolio", "name"))
    nav_col = _detect_col(cols, ("nav", "nav_value", "equity", "value", "pv", "nav_last"))

    print("\n" + "=" * 90)
    print(f"[TABLE] {table}")
    print("- schema:")
    print(info)

    if date_col is None:
        print(f"[WARN] Could not detect a date column in {table}. Showing top rows only.")
        try:
            df_top = con.execute(f"SELECT * FROM {table} LIMIT {int(top)};").fetchdf()
            print(df_top)
        except Exception as e:
            print(f"[WARN] Failed to fetch rows from {table}: {e}")
        return

    where = ""
    if only_date:
        where = f"WHERE {date_col} = '{only_date}'"

    # Basic range
    try:
        df_basic = con.execute(
            f"""
            SELECT MIN({date_col}) AS min_date,
                   MAX({date_col}) AS max_date,
                   COUNT(*) AS n_rows
            FROM {table}
            {where};
            """
        ).fetchdf()
        print("- basic:")
        print(df_basic)
    except Exception as e:
        print(f"[WARN] Failed basic stats for {table}: {e}")
        return

    # Version breakdown
    if ver_col is not None:
        try:
            df_byv = con.execute(
                f"""
                SELECT {ver_col} AS version,
                       COUNT(*) AS n_rows,
                       MIN({date_col}) AS min_date,
                       MAX({date_col}) AS max_date
                FROM {table}
                {where}
                GROUP BY {ver_col}
                ORDER BY n_rows DESC, version;
                """
            ).fetchdf()
            print("- by version:")
            print(df_byv)
        except Exception as e:
            print(f"[WARN] Failed version breakdown for {table}: {e}")

    # Last nav per version
    if (ver_col is not None) and (nav_col is not None):
        try:
            last_nav = con.execute(
                f"""
                SELECT t.{ver_col} AS version,
                       t.{date_col} AS date,
                       t.{nav_col} AS nav
                FROM {table} t
                JOIN (
                    SELECT {ver_col} AS v, MAX({date_col}) AS max_dt
                    FROM {table}
                    {where}
                    GROUP BY {ver_col}
                ) m
                ON t.{ver_col} = m.v AND t.{date_col} = m.max_dt
                ORDER BY version;
                """
            ).fetchdf()
            print("- last nav per version:")
            print(last_nav)   # ✅ 修复：不再打印不存在的 Rosa_nav
        except Exception as e:
            print(f"[WARN] last-nav join failed ({e}); falling back to top rows.")
            try:
                df_top = con.execute(
                    f"""
                    SELECT *
                    FROM {table}
                    {where}
                    ORDER BY {date_col} DESC
                    LIMIT {int(top)};
                    """
                ).fetchdf()
                print("- top rows:")
                print(df_top)
            except Exception as e2:
                print(f"[WARN] Failed to fetch top rows for {table}: {e2}")
    else:
        # Fallback: just show recent rows
        try:
            df_top = con.execute(
                f"""
                SELECT *
                FROM {table}
                {where}
                ORDER BY {date_col} DESC
                LIMIT {int(top)};
                """
            ).fetchdf()
            print("- top rows:")
            print(df_top)
        except Exception as e:
            print(f"[WARN] Failed to fetch top rows for {table}: {e}")


def _scan_out_dir(root: Path) -> None:
    out_dir = root / "data" / "out"
    if not out_dir.exists():
        print(f"\n[INFO] data/out not found: {out_dir}")
        return

    files = sorted(out_dir.glob("*.csv"))
    nav_like = [p for p in files if ("nav" in p.name.lower()) or ("portfolio" in p.name.lower())]

    print("\n" + "=" * 90)
    print("[data/out] CSV inventory (nav-like first)")
    if nav_like:
        for p in nav_like[:20]:
            print(f"  - {p.name}")
    else:
        print("  (no nav-like CSV found)")

    recent = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[:10]
    if recent:
        print("\n[data/out] most recent CSV:")
        for p in recent:
            print(f"  - {p.name}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--db",
        default=str(_default_db_path()),
        help="Path to alpha_tracker.duckdb (default: data/store/alpha_tracker.duckdb)",
    )
    ap.add_argument("--date", default=None, help="Optional date filter (YYYY-MM-DD)")
    ap.add_argument("--top", default=10, type=int, help="Top N rows to show for each nav-like table")
    ap.add_argument("--no-out-scan", action="store_true", help="Skip scanning data/out CSV files")
    args = ap.parse_args()

    root = _project_root()
    db_path = Path(args.db)

    print(f"[ROOT] {root}")
    print(f"[DB]   {db_path}")

    con = _connect(db_path)
    try:
        table_names = _list_tables(con)
        print("\n" + "=" * 90)
        print("[DuckDB] tables")
        if table_names:
            for t in table_names:
                print(f"  - {t}")
        else:
            print("  (no tables)")

        nav_tables = _find_nav_tables(table_names)
        if not nav_tables:
            print("\n[WARN] No nav-like tables detected. This may be OK if NAV is only exported to CSV.")
        else:
            print("\n[DuckDB] nav-like tables detected:")
            for t in nav_tables:
                print(f"  - {t}")

            for t in nav_tables:
                _summarize_nav_table(con, t, only_date=args.date, top=args.top)

    finally:
        con.close()

    if not args.no_out_scan:
        _scan_out_dir(root)

    print("\n[DONE] check_nav")


if __name__ == "__main__":
    main()
