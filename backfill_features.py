# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import subprocess
from pathlib import Path


def _date_range(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    out = []
    cur = s
    while cur <= e:
        # 只跑工作日的话你可以后面改成用 prices_daily 的交易日集合
        if cur.weekday() < 5:
            out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--limit", type=int, default=300)

    ap.add_argument("--universe-source", "--universe_source", dest="universe_source", default=None)
    ap.add_argument("--universe-version", "--universe_version", dest="universe_version", default="UNIVERSE")

    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    py = root / ".venv" / "Scripts" / "python.exe"
    script = root / "src" / "alpha_tracker2" / "pipelines" / "build_features.py"

    dates = _date_range(args.start, args.end)

    for d in dates:
        cmd = [str(py), str(script), "--date", d, "--limit", str(args.limit)]
        if args.universe_source:
            cmd += ["--universe-source", args.universe_source]
        else:
            cmd += ["--universe-version", args.universe_version]

        subprocess.check_call(cmd)


if __name__ == "__main__":
    main()
