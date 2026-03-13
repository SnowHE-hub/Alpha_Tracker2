from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Any, List

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.storage.duckdb_store import DuckDBStore


REQUIRED_TOP_KEYS = ["universe", "features", "signals", "filters", "rank_detail"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Acceptance: picks_daily.reason JSON validity + required keys")
    p.add_argument("--date", required=True, help="trade_date YYYY-MM-DD")
    p.add_argument("--versions", required=True, help="comma-separated, e.g. V1,V2,V3,V4,ENS")
    p.add_argument("--topk", type=int, default=None, help="optional: only check topk rows per version by rank")
    p.add_argument("--strict-v3", action="store_true", help="require V3 threshold keys in reason")
    p.add_argument("--strict-v4", action="store_true", help="require V4 backtest keys in reason")
    p.add_argument("--max-bad", type=int, default=0, help="allowed bad rows per version (default 0)")
    return p.parse_args()


def _safe_json_load(s: str) -> Dict[str, Any] | None:
    try:
        return json.loads(s)
    except Exception:
        return None


def _has_keys(obj: Dict[str, Any], keys: List[str]) -> bool:
    return all(k in obj for k in keys)


def main() -> None:
    args = parse_args()
    trade_date = args.date
    versions = [v.strip() for v in args.versions.split(",") if v.strip()]
    if not versions:
        raise SystemExit("[FAIL] --versions empty")

    root = Path(__file__).resolve().parents[1]
    cfg = load_settings(root)
    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    print("\n=== Acceptance: picks_reason ===")
    print("trade_date:", trade_date)
    print("versions:", versions)
    print("db:", cfg.store_db)

    overall_fail = False

    for v in versions:
        sql = """
        SELECT trade_date, version, ticker, rank, score, reason, picked_by
        FROM picks_daily
        WHERE trade_date = ? AND version = ?
        ORDER BY rank ASC
        """
        rows = store.fetchall(sql, [trade_date, v])

        if args.topk is not None:
            rows = rows[: args.topk]

        if not rows:
            print(f"\n[FAIL] {v}: no rows found in picks_daily for {trade_date}")
            overall_fail = True
            continue

        bad = 0
        bad_examples = []

        for (td, ver, ticker, rank, score, reason, picked_by) in rows:
            if reason is None or str(reason).strip() == "":
                bad += 1
                if len(bad_examples) < 5:
                    bad_examples.append((ticker, rank, "reason empty"))
                continue

            obj = _safe_json_load(str(reason))
            if obj is None or not isinstance(obj, dict):
                bad += 1
                if len(bad_examples) < 5:
                    bad_examples.append((ticker, rank, "reason not valid JSON"))
                continue

            if not _has_keys(obj, REQUIRED_TOP_KEYS):
                bad += 1
                missing = [k for k in REQUIRED_TOP_KEYS if k not in obj]
                if len(bad_examples) < 5:
                    bad_examples.append((ticker, rank, f"missing top keys: {missing}"))
                continue

            # Strict checks by version (optional toggles)
            if args.strict_v3 or ver.upper() == "V3":
                # Only enforce if strict-v3 OR it's V3 (you can change this policy if desired)
                # Common threshold keys:
                # - thr_value / pass_thr may exist as columns; we want them in reason too for interpretability
                filt = obj.get("filters", {})
                if ver.upper() == "V3":
                    need = ["thr_value", "pass_thr"]
                    miss = [k for k in need if k not in filt]
                    if miss and args.strict_v3:
                        bad += 1
                        if len(bad_examples) < 5:
                            bad_examples.append((ticker, rank, f"V3 missing filters keys: {miss}"))
                        continue

            if args.strict_v4 or ver.upper() == "V4":
                if ver.upper() == "V4" and args.strict_v4:
                    sig = obj.get("signals", {})
                    need = ["bt_mean", "bt_winrate", "bt_worst_mdd"]
                    miss = [k for k in need if k not in sig]
                    if miss:
                        bad += 1
                        if len(bad_examples) < 5:
                            bad_examples.append((ticker, rank, f"V4 missing signals keys: {miss}"))
                        continue

        print(f"\n{v}: rows_checked={len(rows)} bad={bad} allowed_max_bad={args.max_bad}")
        if bad_examples:
            print("bad examples (ticker, rank, why):")
            for ex in bad_examples:
                print(" ", ex)

        if bad > args.max_bad:
            print(f"[FAIL] {v}: bad rows {bad} > allowed {args.max_bad}")
            overall_fail = True
        else:
            print(f"[OK] {v}")

    if overall_fail:
        raise SystemExit("\n[FAIL] acceptance_picks_reason")
    print("\n[OK] acceptance_picks_reason passed.")


if __name__ == "__main__":
    main()

'''
cd D:\alpha_tracker2
& .\.venv\Scripts\python.exe .\tools\acceptance_picks_reason.py --date 2026-01-14 --versions V1 --topk 3 --max-bad 0

'''