from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "src" / "alpha_tracker2" / "pipelines" / "nav_from_positions.py"

def main():
    text = TARGET.read_text(encoding="utf-8")

    # --- 1) After building nav_db DataFrame, rename nav columns to avoid merge suffix confusion ---
    # Find: df = pd.DataFrame(rows, columns=[...])
    m = re.search(r"df\s*=\s*pd\.DataFrame\(rows,\s*columns\s*=\s*\[[^\]]+\]\)\s*\n", text)
    if not m:
        raise RuntimeError("[ERROR] cannot find DataFrame(rows, columns=[...]) in _read_nav_daily; file changed?")

    df_line = m.group(0)
    if "nav_gross_db" not in text:
        insert = df_line + (
            '    # normalize nav_daily columns to avoid merge suffix confusion\n'
            '    if "nav" in df.columns:\n'
            '        df = df.rename(columns={"nav": "nav_db"})\n'
            '    if "nav_gross" in df.columns:\n'
            '        df = df.rename(columns={"nav_gross": "nav_gross_db"})\n'
        )
        text = text.replace(df_line, insert, 1)

    # --- 2) Ensure abs_diff uses nav_ref (prefer gross db) ---
    # Add/replace nav_ref creation after merge result exists (look for "df = df.merge" first occurrence)
    if "nav_ref" not in text:
        text = re.sub(
            r"(df\s*=\s*df\.merge\([^\n]+\)\s*\n)",
            r"\1"
            r'    # choose reference NAV from nav_daily: prefer gross if available\n'
            r'    if "nav_gross_db" in df.columns:\n'
            r'        df["nav_ref"] = df["nav_gross_db"]\n'
            r'    elif "nav_db" in df.columns:\n'
            r'        df["nav_ref"] = df["nav_db"]\n'
            r'    else:\n'
            r'        # fallback: try common merge suffix names\n'
            r'        if "nav_gross" in df.columns:\n'
            r'            df["nav_ref"] = df["nav_gross"]\n'
            r'        elif "nav" in df.columns:\n'
            r'            df["nav_ref"] = df["nav"]\n'
            r'        else:\n'
            r'            raise RuntimeError("No nav columns found after merge; columns=" + ",".join(df.columns))\n',
            text,
            count=1,
        )

    # Replace any abs_diff computation that subtracts from a nav_* column, force to nav_ref
    text, n1 = re.subn(
        r'df\["abs_diff"\]\s*=\s*\(df\["nav_exec"\]\s*-\s*df\["[^"]*"\]\)\.abs\(\)\s*\n',
        'df["abs_diff"] = (df["nav_exec"] - df["nav_ref"]).abs()\n',
        text,
    )
    # If no match above, also try the exact old pattern with nav
    text = text.replace(
        'df["abs_diff"] = (df["nav_exec"] - df["nav"]).abs()',
        'df["abs_diff"] = (df["nav_exec"] - df["nav_ref"]).abs()'
    )

    # --- 3) Fix worst_date print to show nav_ref ---
    text = text.replace("nav_daily: {worst[\"nav\"]}", "nav_daily(ref): {worst[\"nav_ref\"]}")
    text = text.replace("nav_daily: {worst['nav']}", "nav_daily(ref): {worst['nav_ref']}")

    TARGET.write_text(text, encoding="utf-8")
    print(f"[OK] patched: force gross reference via nav_gross_db when available: {TARGET}")

if __name__ == "__main__":
    main()
