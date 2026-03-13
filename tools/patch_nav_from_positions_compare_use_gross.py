from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "src" / "alpha_tracker2" / "pipelines" / "nav_from_positions.py"

def main():
    text = TARGET.read_text(encoding="utf-8")

    # 找到 merge 后的对比列使用 nav / nav_exec 的位置，替换为优先使用 nav_gross
    # 思路：如果 nav_db 里有 nav_gross，就比较 exec 的 gross（通常叫 nav 或 nav_exec） vs nav_gross
    # 这里做一个比较稳的替换：把 "nav_daily" 参与 diff 的列名从 nav 改为 nav_gross（若存在）
    if "nav_gross" not in text:
        raise RuntimeError("[ERROR] nav_from_positions.py does not contain nav_gross; run previous patch first?")

    # 1) 在合并后的 df 上新增一个“nav_ref”列：优先 nav_gross，否则 nav
    if "nav_ref" not in text:
        text = re.sub(
            r'(df\s*=\s*df\.merge\([^\n]+\)\n)',
            r'\1    # choose reference NAV: prefer gross if available\n'
            r'    if "nav_gross" in df.columns:\n'
            r'        df["nav_ref"] = df["nav_gross"]\n'
            r'    else:\n'
            r'        df["nav_ref"] = df["nav"]\n',
            text,
            count=1,
        )

    # 2) 把 abs_diff 的计算从 nav 改成 nav_ref
    text = text.replace('df["abs_diff"] = (df["nav_exec"] - df["nav"]).abs()',
                        'df["abs_diff"] = (df["nav_exec"] - df["nav_ref"]).abs()')

    # 3) 输出 worst_date 的 nav_daily 字段也改成 nav_ref（避免打印误导）
    text = text.replace('worst_date: {worst["trade_date"]} nav_exec: {worst["nav_exec"]} nav_daily: {worst["nav"]}',
                        'worst_date: {worst["trade_date"]} nav_exec: {worst["nav_exec"]} nav_daily(ref): {worst["nav_ref"]}')

    TARGET.write_text(text, encoding="utf-8")
    print(f"[OK] patched: compare exec NAV against nav_gross when available: {TARGET}")

if __name__ == "__main__":
    main()
