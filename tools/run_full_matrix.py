from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> int:
    print("\n>>", " ".join(cmd))
    p = subprocess.run(cmd, cwd=str(ROOT))
    return p.returncode


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--models", default="V1,V2,V3,V4")
    ap.add_argument("--limit", type=int, default=24)
    ap.add_argument("--cash", type=float, default=100000)
    ap.add_argument("--lot_size", type=int, default=100)
    ap.add_argument("--cost_bps", type=float, default=10)
    ap.add_argument("--initial_equity", type=float, default=100000)
    ap.add_argument("--horizon", type=int, default=5)
    args = ap.parse_args()

    py = str(ROOT / ".venv/Scripts/python.exe")
    models = [x.strip() for x in args.models.split(",") if x.strip()]

    for mv in models:
        print(f"\n========== RUN MODEL {mv} ==========")

        rc1 = run([
            py, str(ROOT / "tools/run_strategy_batch.py"),
            "--start", args.start, "--end", args.end,
            "--limit", str(args.limit),
            "--where_model", mv,
            "--cash", str(args.cash),
            "--lot_size", str(args.lot_size),
            "--cost_bps", str(args.cost_bps),
            "--initial_equity", str(args.initial_equity),
        ])
        if rc1 != 0:
            print(f"[FAIL] run_strategy_batch for {mv}")
            continue

        rc2 = run([
            py, str(ROOT / "tools/eval_5d_from_nav.py"),
            "--start", args.start, "--end", args.end,
            "--where_model", mv,
            "--horizon", str(args.horizon),
        ])
        if rc2 != 0:
            print(f"[FAIL] eval_5d_from_nav for {mv}")
            continue

        print(f"[OK] model finished: {mv}")

    print("\n[ALL DONE]")


if __name__ == "__main__":
    main()
