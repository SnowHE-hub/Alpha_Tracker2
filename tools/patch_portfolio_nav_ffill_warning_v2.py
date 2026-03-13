from __future__ import annotations

import re
from pathlib import Path


def main() -> None:
    path = Path(r"D:\alpha_tracker2\src\alpha_tracker2\pipelines\portfolio_nav.py")
    s = path.read_text(encoding="utf-8")

    # 目标：把任何类似 fillna(method="ffill") 的写法替换为 .ffill()
    # 覆盖常见形态：
    #   out["nav"] = out["nav"].fillna(method="ffill").fillna(1.0)
    #   out["nav"] = out["nav"].fillna(method="ffill")
    #   out["nav"] = out["nav"].fillna(method='ffill').fillna(1.0)
    patterns = [
        (r'out\["nav"\]\s*=\s*out\["nav"\]\.fillna\(\s*method\s*=\s*["\']ffill["\']\s*\)\.fillna\(\s*1\.0\s*\)',
         'out["nav"] = out["nav"].ffill().fillna(1.0)'),
        (r'out\["nav"\]\s*=\s*out\["nav"\]\.fillna\(\s*method\s*=\s*["\']ffill["\']\s*\)',
         'out["nav"] = out["nav"].ffill()'),
    ]

    changed = False
    for pat, rep in patterns:
        if re.search(pat, s):
            s = re.sub(pat, rep, s)
            changed = True

    if not changed:
        print("[SKIP] no fillna(method='ffill') pattern found; nothing to patch.")
        return

    path.write_text(s, encoding="utf-8")
    print(f"[OK] patched ffill FutureWarning (v2): {path}")


if __name__ == "__main__":
    main()
