from __future__ import annotations
import argparse
import subprocess
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]

def run(cmd: list[str]) -> int:
    print("\n>>", " ".join(cmd))
    p = subprocess.run(cmd, cwd=str(ROOT))
    return p.returncode

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--cash", type=float, default=100000)
    ap.add_argument("--lot_size", type=int, default=100)
    ap.add_argument("--cost_bps", type=float, default=10)
    ap.add_argument("--initial_equity", type=float, default=100000)
    ap.add_argument("--limit", type=int, default=2)
    ap.add_argument("--where_model", default="")  # e.g. V1
    args = ap.parse_args()

    db = ROOT / "data/store/alpha_tracker.duckdb"
    con = duckdb.connect(str(db))
    try:
        q = "SELECT strategy_id, model_version FROM strategies ORDER BY strategy_id"
        rows = con.execute(q).fetchall()
    finally:
        con.close()

    # filter
    items = []
    for sid, mv in rows:
        sid = str(sid)
        mv = str(mv)
        if args.where_model and mv != args.where_model:
            continue
        items.append(sid)

    items = items[: args.limit]
    if not items:
        raise RuntimeError("No strategies matched.")

    ok = 0
    for sid in items:
        # 1) execute
        rc1 = run([
            str(ROOT / ".venv/Scripts/python.exe"),
            str(ROOT / "src/alpha_tracker2/pipelines/execute_rebalance_range.py"),
            "--start", args.start,
            "--end", args.end,
            "--strategy_id", sid,
            "--cash", str(args.cash),
            "--lot_size", str(args.lot_size),
            "--hold_last_signal",
        ])
        if rc1 != 0:
            print("[FAIL] execute:", sid)
            continue

        # 2) nav
        rc2 = run([
            str(ROOT / ".venv/Scripts/python.exe"),
            str(ROOT / "src/alpha_tracker2/pipelines/portfolio_nav.py"),
            "--start", args.start,
            "--end", args.end,
            "--strategy_ids", sid,
            "--cost_bps", str(args.cost_bps),
            "--initial_equity", str(args.initial_equity),
        ])
        if rc2 != 0:
            print("[FAIL] nav:", sid)
            continue

        print("[OK] done:", sid)
        ok += 1

    print(f"\n[SUMMARY] ok={ok} / total={len(items)}")

if __name__ == "__main__":
    main()
