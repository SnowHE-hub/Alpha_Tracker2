from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "src" / "alpha_tracker2" / "pipelines" / "nav_from_positions.py"

def main():
    text = TARGET.read_text(encoding="utf-8")

    # Replace the strict numeric conversion lines with safe logic
    pattern_nav = r'df\["nav"\]\s*=\s*pd\.to_numeric\(df\["nav"\],\s*errors="coerce"\)\s*\n'
    repl_nav = (
        '    # numeric cast (compatible with patched column names)\n'
        '    if "nav" in df.columns:\n'
        '        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")\n'
        '    if "nav_db" in df.columns:\n'
        '        df["nav_db"] = pd.to_numeric(df["nav_db"], errors="coerce")\n'
    )

    pattern_gross = r'df\["nav_gross"\]\s*=\s*pd\.to_numeric\(df\["nav_gross"\],\s*errors="coerce"\)\s*\n'
    repl_gross = (
        '    if "nav_gross" in df.columns:\n'
        '        df["nav_gross"] = pd.to_numeric(df["nav_gross"], errors="coerce")\n'
        '    if "nav_gross_db" in df.columns:\n'
        '        df["nav_gross_db"] = pd.to_numeric(df["nav_gross_db"], errors="coerce")\n'
    )

    new_text, n1 = re.subn(pattern_nav, repl_nav, text, count=1)
    new_text, n2 = re.subn(pattern_gross, repl_gross, new_text, count=1)

    if n1 == 0:
        print("[WARN] did not find df['nav']=pd.to_numeric(...) line; maybe already edited.")
    if n2 == 0:
        print("[WARN] did not find df['nav_gross']=pd.to_numeric(...) line; maybe not present in file.")

    TARGET.write_text(new_text, encoding="utf-8")
    print(f"[OK] patched: safe nav/nav_gross numeric casts: {TARGET}")

if __name__ == "__main__":
    main()
