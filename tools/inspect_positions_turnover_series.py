from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(Path("data/store/alpha_tracker.duckdb")))
    ap.add_argument("--version", required=True)
    ap.add_argument("--start", required=True)  # YYYY-MM-DD
    ap.add_argument("--end", required=True)    # YYYY-MM-DD
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"db not found: {db_path}")

    con = duckdb.connect(str(db_path))

    # 取 positions（含 cash 列；注意你表里没有 __CASH__ 行也没关系，我们用 cash 列）
    sql = """
    SELECT
      asof_date,
      version,
      ticker,
      shares,
      market_value,
      cash
    FROM positions_daily
    WHERE version = ?
      AND asof_date BETWEEN ? AND ?
      AND ticker IS NOT NULL
      AND ticker <> '__CASH__'
    ORDER BY asof_date, ticker
    """
    df = con.execute(sql, [args.version, args.start, args.end]).df()
    con.close()

    if df.empty:
        raise RuntimeError("no positions rows in range")

    # 你表里 market_value 现在是 NaN，所以这里用 shares * close_ffill 也不行（close不在表里）
    # 但我们可以用“等权假设”来计算 turnover：只看持仓名单变化（tickers set changes）
    # turnover_equal_weight(prev, cur) = 0.5 * sum |w_i,t - w_i,t-1|
    # 其中 w 等权：w=1/N
    days = sorted(df["asof_date"].dropna().unique().tolist())

    def equal_weight_map(tickers: list[str]) -> dict[str, float]:
        tickers = list(dict.fromkeys(tickers))
        if not tickers:
            return {}
        w = 1.0 / len(tickers)
        return {t: w for t in tickers}

    rows = []
    prev_holdings: list[str] = []
    prev_w = {}

    for d in days:
        cur = df[df["asof_date"] == d]["ticker"].astype(str).tolist()
        cur_w = equal_weight_map(cur)

        union = set(prev_w) | set(cur_w)
        s = 0.0
        for tk in union:
            s += abs(cur_w.get(tk, 0.0) - prev_w.get(tk, 0.0))
        turnover = 0.5 * s

        rows.append({
            "asof_date": str(pd.to_datetime(d).date()),
            "n_holdings": len(cur_w),
            "holdings": ",".join(sorted(cur_w.keys())),
            "turnover": turnover
        })

        prev_w = cur_w

    out = pd.DataFrame(rows)
    print(out.to_string(index=False))

    changed = out[out["turnover"] > 0].copy()
    print("\n=== REBALANCE DAYS (turnover>0) ===")
    if changed.empty:
        print("None (no holdings changes detected)")
    else:
        print(changed[["asof_date", "turnover", "n_holdings"]].to_string(index=False))


if __name__ == "__main__":
    main()
