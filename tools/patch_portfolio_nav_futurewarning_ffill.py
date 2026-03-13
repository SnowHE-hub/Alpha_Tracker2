from __future__ import annotations

from pathlib import Path


def main() -> None:
    path = Path(r"D:\alpha_tracker2\src\alpha_tracker2\pipelines\portfolio_nav.py")
    s = path.read_text(encoding="utf-8")

    old = 'out["nav"] = out["nav"].fillna(method="ffill").fillna(1.0)'
    new = 'out["nav"] = out["nav"].ffill().fillna(1.0)'

    if old not in s:
        raise RuntimeError("[ERROR] target line not found; portfolio_nav.py may have changed.")

    s2 = s.replace(old, new)
    path.write_text(s2, encoding="utf-8")
    print(f"[OK] patched ffill FutureWarning: {path}")


if __name__ == "__main__":
    main()
