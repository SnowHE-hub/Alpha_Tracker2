from __future__ import annotations

from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    target = root / "src" / "alpha_tracker2" / "pipelines" / "portfolio_nav.py"
    if not target.exists():
        raise FileNotFoundError(f"target not found: {target}")

    txt = target.read_text(encoding="utf-8")

    old = 'out["nav"] = out["nav"].fillna(method="ffill").fillna(1.0)'
    new = 'out["nav"] = out["nav"].ffill().fillna(1.0)'

    if old not in txt:
        print("[WARN] pattern not found; no changes made.")
        return

    target.write_text(txt.replace(old, new), encoding="utf-8")
    print(f"[OK] patched FutureWarning ffill: {target}")


if __name__ == "__main__":
    main()
