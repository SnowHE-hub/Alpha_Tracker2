from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> None:
    print("\n>>>", " ".join(cmd))
    p = subprocess.run(cmd, cwd=r"D:\alpha_tracker2")
    if p.returncode != 0:
        raise SystemExit(f"[FAIL] command failed: {' '.join(cmd)}")


def main() -> None:
    py = str(Path(r"D:\alpha_tracker2\.venv\Scripts\python.exe"))

    # 1) 生成 nav_daily（positions-based + cost）
    run(
        [
            py,
            r".\tools\portfolio_nav_positions_costed.py",
            "--start",
            "2025-12-20",
            "--end",
            "2026-01-14",
            "--versions",
            "ENS",
            "--topk",
            "3",
            "--initial_equity",
            "100000",
            "--cost_bps",
            "10",
        ]
    )

    # 2) 回归对齐（nav_daily vs positions+cost 复算）
    run(
        [
            py,
            r".\tools\nav_from_positions_costed_check.py",
            "--version",
            "ENS",
            "--start",
            "2025-12-20",
            "--end",
            "2026-01-14",
            "--initial_equity",
            "100000",
            "--cost_bps",
            "10",
        ]
    )

    # 3) 点检：换仓日成本是否正确（我们已验证 2026-01-07）
    run([py, r".\tools\check_nav_cost_rebalance_day.py", "--version", "ENS", "--date", "2026-01-07"])

    print("\n[PASS] Step4 acceptance passed: nav_daily matches positions-based costed NAV.")


if __name__ == "__main__":
    main()
