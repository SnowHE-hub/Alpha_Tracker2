from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(r"D:\alpha_tracker2")
TARGET = ROOT / "src" / "alpha_tracker2" / "pipelines" / "nav_from_positions.py"


def main():
    if not TARGET.exists():
        raise FileNotFoundError(f"[ERROR] file not found: {TARGET}")

    text = TARGET.read_text(encoding="utf-8")

    n_filled = len(re.findall(r"\bpositions_daily_filled\b", text))
    n_raw = len(re.findall(r"\bpositions_daily\b", text))

    print(f"[FILE] {TARGET}")
    print(f"[COUNT] positions_daily_filled={n_filled}, positions_daily={n_raw}")

    # 如果已经没有 raw，只剩 filled，就不需要 patch
    if n_raw == 0:
        print("[SKIP] no 'positions_daily' token found; nothing to patch.")
        return

    # 注意：先替换 raw -> filled；避免把 positions_daily_filled 再次处理
    # 使用负向前瞻，确保不会匹配 positions_daily_filled 的前半段
    patched, n = re.subn(r"\bpositions_daily\b(?!_filled)", "positions_daily_filled", text)

    TARGET.write_text(patched, encoding="utf-8")
    print(f"[OK] patched nav_from_positions.py: replaced {n} occurrence(s) of positions_daily -> positions_daily_filled")


if __name__ == "__main__":
    main()
