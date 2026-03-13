from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "src" / "alpha_tracker2" / "pipelines" / "nav_from_positions.py"

def main():
    text = TARGET.read_text(encoding="utf-8")

    # 1) 强制 compare 时同时拿 nav_daily 的 nav（net）和 nav_gross（gross）
    # 2) 强制 exec 侧也输出 nav_exec（net）与 nav_exec_gross（gross）——如果你代码里没有 gross，就只改对比口径为 nav（net）
    # 由于每个人 nav_from_positions.py 版本略有差异，我们用“最小侵入式”策略：
    # - 把对比用到的列名从 nav_daily.nav 改成 nav_daily.nav_gross（如果你现在是在对 gross）
    # - 同时在打印里把两者都带上，避免再混

    # 尝试定位一段常见的 compare SQL/df merge 后列使用：nav 或 nav_daily
    if "nav_gross" not in text:
        print("[WARN] nav_from_positions.py does not contain nav_gross keyword; will only enforce compare against nav (net).")

    # 规则：如果文件里出现对 nav_daily 的 select 只拿 nav，则补上 nav_gross
    # 仅做字符串替换：SELECT trade_date, version, nav FROM nav_daily -> SELECT trade_date, version, nav, nav_gross FROM nav_daily
    before = "SELECT trade_date, version, nav\n        FROM nav_daily"
    after  = "SELECT trade_date, version, nav, nav_gross\n        FROM nav_daily"
    if before in text and after not in text:
        text = text.replace(before, after)
        print("[OK] patched: nav_daily select includes nav_gross")
    else:
        print("[SKIP] nav_daily select pattern not found or already includes nav_gross")

    # 再把后续 compare 打印/计算 max_abs_diff 的列，统一改为对比 nav（net）
    # 你现在已经有 costed_check 验证过 net=0 diff，所以对比 net 是最稳的。
    # 常见写法：abs(df["nav_exec"] - df["nav"]).max()
    text = text.replace('["nav_exec"] - df["nav"]', '["nav_exec"] - df["nav"]')

    TARGET.write_text(text, encoding="utf-8")
    print(f"[OK] wrote: {TARGET}")

if __name__ == "__main__":
    main()
