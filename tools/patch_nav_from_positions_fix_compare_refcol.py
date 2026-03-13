from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "src" / "alpha_tracker2" / "pipelines" / "nav_from_positions.py"

def main():
    text = TARGET.read_text(encoding="utf-8")

    # Replace the hard-coded abs_diff line that references df_cmp["nav_daily"]
    pattern = r'df_cmp\["abs_diff"\]\s*=\s*\(df_cmp\["nav_exec"\]\s*-\s*df_cmp\["nav_daily"\]\)\.abs\(\)\s*'
    replacement = (
        '# choose reference column from nav_daily side (patched names)\n'
        '    ref_col = None\n'
        '    for _c in ["nav_gross_db", "nav_db", "nav_gross", "nav", "nav_daily"]:\n'
        '        if _c in df_cmp.columns:\n'
        '            ref_col = _c\n'
        '            break\n'
        '    if ref_col is None:\n'
        '        raise RuntimeError(f"[ERROR] no reference NAV column found in df_cmp. columns={list(df_cmp.columns)}")\n'
        '    df_cmp["abs_diff"] = (df_cmp["nav_exec"] - df_cmp[ref_col]).abs()\n'
    )

    new_text, n = re.subn(pattern, replacement, text, count=1)
    if n == 0:
        raise RuntimeError("[ERROR] target abs_diff line not found (df_cmp['nav_exec'] - df_cmp['nav_daily']). File changed?")

    TARGET.write_text(new_text, encoding="utf-8")
    print(f"[OK] patched compare ref column (auto ref_col): {TARGET}")

if __name__ == "__main__":
    main()
