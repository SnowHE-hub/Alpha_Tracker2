from __future__ import annotations
import argparse
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="project root, default=.")
    ap.add_argument("--pattern", default="positions_daily", help="search token")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    hits = []
    for p in root.rglob("*.py"):
        try:
            txt = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if args.pattern in txt:
            hits.append(str(p))

    print(f"[ROOT] {root}")
    print(f"[PATTERN] {args.pattern}")
    if not hits:
        print("[OK] no hits")
        return
    print("[HITS]")
    for h in hits:
        print(" -", h)

if __name__ == "__main__":
    main()
