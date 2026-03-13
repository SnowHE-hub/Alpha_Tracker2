from __future__ import annotations

from pathlib import Path
import re


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    target = root / "src" / "alpha_tracker2" / "pipelines" / "portfolio_nav.py"
    if not target.exists():
        raise FileNotFoundError(f"target not found: {target}")

    txt = target.read_text(encoding="utf-8")

    # 我们找：out = base.merge(df, on=["trade_date", "version"], how="left")
    # 然后在它前面插入两行，把 base/df 的 trade_date 都转成 date
    pat = r"(\n\s*)out\s*=\s*base\.merge\(df,\s*on=\[\"trade_date\",\s*\"version\"\],\s*how=\"left\"\)\s*\n"
    m = re.search(pat, txt)
    if not m:
        print("[WARN] merge line pattern not found; no changes made.")
        print("       expected merge like: out = base.merge(df, on=[\"trade_date\", \"version\"], how=\"left\")")
        return

    indent = m.group(1)

    inject = (
        f"{indent}# --- dtype align for merge keys (avoid object vs datetime64 mismatch)\n"
        f"{indent}base[\"trade_date\"] = pd.to_datetime(base[\"trade_date\"], errors=\"coerce\").dt.date\n"
        f"{indent}df[\"trade_date\"] = pd.to_datetime(df[\"trade_date\"], errors=\"coerce\").dt.date\n"
    )

    txt2 = re.sub(pat, inject + m.group(0), txt, count=1)

    target.write_text(txt2, encoding="utf-8")
    print(f"[OK] patched: {target}")
    print("[OK] inserted dtype align lines before merge")


if __name__ == "__main__":
    main()
