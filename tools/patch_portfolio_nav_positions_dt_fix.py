from __future__ import annotations

from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    target = root / "src" / "alpha_tracker2" / "pipelines" / "portfolio_nav.py"
    if not target.exists():
        raise FileNotFoundError(f"target not found: {target}")

    txt = target.read_text(encoding="utf-8")

    old = "pd.to_datetime(days).dt.date"
    new = "pd.to_datetime(days).date"

    if old not in txt:
        # 兜底：如果你未来又改成了别的写法，就把相关片段打印出来方便定位
        print("[WARN] pattern not found, no changes made.")
        print("       expected to find:", old)
        return

    txt2 = txt.replace(old, new, 1)
    target.write_text(txt2, encoding="utf-8")
    print(f"[OK] patched: {target}")
    print(f"[OK] replaced: {old} -> {new}")


if __name__ == "__main__":
    main()
