# tools/snippet_portfolio_nav_price_join.py
from __future__ import annotations

import argparse
from pathlib import Path
import re


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", type=str, default="", help="Path to portfolio_nav.py (default src/alpha_tracker2/pipelines/portfolio_nav.py)")
    ap.add_argument("--context", type=int, default=12, help="Lines of context around matches")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    path = Path(args.file) if args.file else (root / "src" / "alpha_tracker2" / "pipelines" / "portfolio_nav.py")
    if not path.exists():
        raise FileNotFoundError(f"Not found: {path}")

    text = path.read_text(encoding="utf-8", errors="ignore").splitlines()

    patterns = [
        r"prices_daily",
        r"\bclose\b",
        r"join",
        r"merge",
        r"valid",
        r"n_valid",
        r"ffill",
        r"fillna",
    ]
    rx = re.compile("|".join(f"(?:{p})" for p in patterns), re.IGNORECASE)

    hits = [i for i, line in enumerate(text) if rx.search(line)]
    if not hits:
        print(f"[FILE] {path}")
        print("[NO_HITS] No obvious price/join/valid keywords found.")
        return

    print(f"[FILE] {path}")
    printed = set()
    for i in hits:
        # avoid printing overlapping blocks repeatedly
        start = max(0, i - args.context)
        end = min(len(text), i + args.context + 1)
        key = (start, end)
        if key in printed:
            continue
        printed.add(key)

        print("\n" + "=" * 100)
        print(f"[LINES] {start+1} - {end}")
        for j in range(start, end):
            prefix = ">>" if j == i else "  "
            print(f"{prefix} {j+1:4d} | {text[j]}")


if __name__ == "__main__":
    main()
