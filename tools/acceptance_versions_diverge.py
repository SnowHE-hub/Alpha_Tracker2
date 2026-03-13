from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Set, Tuple

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Acceptance: versions diverge (picks not identical)")
    p.add_argument("--date", required=True, help="trade_date YYYY-MM-DD")
    p.add_argument("--versions", required=True, help="comma-separated, e.g. V1,V2,V3,V4")
    p.add_argument("--topk", type=int, default=3, help="compare only topk picks per version (default 3)")
    p.add_argument("--max-overlap-ratio", type=float, default=0.99,
                   help="FAIL if overlap_ratio >= this (default 0.99). Use 1.0 if you only want 'not exactly equal'.")
    p.add_argument("--show-tickers", action="store_true", help="print tickers list for each version")
    return p.parse_args()


def _overlap_stats(sets: Dict[str, Set[str]]) -> Tuple[Set[str], Set[str], float]:
    all_sets = list(sets.values())
    if not all_sets:
        return set(), set(), 0.0
    inter = set.intersection(*all_sets) if all_sets else set()
    uni = set.union(*all_sets) if all_sets else set()
    ratio = (len(inter) / len(uni)) if len(uni) > 0 else 0.0
    return inter, uni, ratio


def main() -> None:
    args = parse_args()
    trade_date = args.date
    versions = [v.strip() for v in args.versions.split(",") if v.strip()]
    if len(versions) < 2:
        raise SystemExit("[FAIL] need at least 2 versions")

    root = Path(__file__).resolve().parents[1]
    cfg = load_settings(root)
    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    print("\n=== Acceptance: versions_diverge ===")
    print("trade_date:", trade_date)
    print("versions:", versions)
    print("topk:", args.topk)
    print("db:", cfg.store_db)

    version_sets: Dict[str, Set[str]] = {}
    version_lists: Dict[str, List[str]] = {}

    for v in versions:
        sql = """
        SELECT ticker
        FROM picks_daily
        WHERE trade_date = ? AND version = ?
        ORDER BY rank ASC
        LIMIT ?
        """
        rows = store.fetchall(sql, [trade_date, v, args.topk])
        tickers = [r[0] for r in rows]
        version_lists[v] = tickers
        version_sets[v] = set(tickers)

        if len(tickers) < args.topk:
            print(f"[WARN] {v}: only {len(tickers)} rows found (expected topk={args.topk}).")

    # If any version has no picks, fail
    missing = [v for v in versions if len(version_lists.get(v, [])) == 0]
    if missing:
        raise SystemExit(f"[FAIL] missing picks for versions: {missing}")

    if args.show_tickers:
        for v in versions:
            print(f"\n{v} tickers:", version_lists[v])

    inter, uni, ratio = _overlap_stats(version_sets)
    print("\nintersection size:", len(inter), "union size:", len(uni), "overlap_ratio:", f"{ratio:.3f}")

    # Additionally check if all lists are exactly identical (stronger condition)
    first = version_lists[versions[0]]
    all_equal = all(version_lists[v] == first for v in versions[1:])
    print("exactly_equal_lists:", all_equal)

    if ratio >= args.max_overlap_ratio:
        raise SystemExit(f"[FAIL] overlap_ratio {ratio:.3f} >= max {args.max_overlap_ratio}")
    if all_equal:
        raise SystemExit("[FAIL] versions picks are exactly identical (same tickers order)")

    print("[OK] acceptance_versions_diverge passed.")


if __name__ == "__main__":
    main()


'''
& .\.venv\Scripts\python.exe .\tools\acceptance_versions_diverge.py --date 2026-01-14 --versions V1,V2,V3,V4 --topk 3 --max-overlap-ratio 0.99 --show-tickers

'''