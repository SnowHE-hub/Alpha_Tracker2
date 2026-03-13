from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "store" / "alpha_tracker.duckdb"


def _parse_date(s: str):
    # 返回 python date
    return pd.to_datetime(s).date()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--initial_equity", type=float, required=True)
    ap.add_argument("--cost_bps", type=float, default=0.0)  # 这里保留参数，便于一致性，不直接用
    args = ap.parse_args()

    v = args.version
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    initial = float(args.initial_equity)

    con = duckdb.connect(str(DB_PATH))

    # 1) nav_daily（系统口径）
    nav = con.execute(
        """
        SELECT trade_date, version, nav, nav_gross, turnover, cost
        FROM nav_daily
        WHERE version = ?
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date
        """,
        [v, start, end],
    ).df()

    if nav.empty:
        raise RuntimeError(
            "[ERROR] nav_daily has no rows in this window. "
            "Run portfolio_nav_positions_costed.py first."
        )

    # 统一 key 类型：trade_date -> datetime64[ns]
    nav["trade_date"] = pd.to_datetime(nav["trade_date"])

    # 2) positions_daily_filled（filled view 估值口径）
    pos = con.execute(
        """
        SELECT
          asof_date AS trade_date,
          version,
          SUM(market_value_filled) AS mv_sum,
          MAX(cash) AS cash_max
        FROM positions_daily_filled
        WHERE version = ?
          AND asof_date BETWEEN ? AND ?
          AND ticker <> '__CASH__'
        GROUP BY 1,2
        ORDER BY 1
        """,
        [v, start, end],
    ).df()

    if pos.empty:
        raise RuntimeError(
            "[ERROR] positions_daily_filled has no rows in this window. "
            "Run create_positions_filled_view.py and make sure positions exist."
        )

    # 统一 key 类型：trade_date -> datetime64[ns]
    pos["trade_date"] = pd.to_datetime(pos["trade_date"])

    # base：用完整日历做对齐（datetime64[ns]）
    base = pd.DataFrame({"trade_date": pd.date_range(start, end, freq="D")})
    base["version"] = v

    out = base.merge(pos, on=["trade_date", "version"], how="left")

    out["mv_sum"] = pd.to_numeric(out["mv_sum"], errors="coerce")
    out["cash_max"] = pd.to_numeric(out["cash_max"], errors="coerce")

    # equity / gross nav
    out["equity"] = out["mv_sum"].fillna(0.0) + out["cash_max"].fillna(0.0)
    # 如果某天完全没数据，equity=0，这种我们不拿来做严格对齐（但你这里窗口应该都有）
    out.loc[out["equity"] <= 0, "equity"] = pd.NA

    out["nav_calc_gross"] = out["equity"] / initial

    # merge 对齐到 nav_daily
    merged = nav.merge(
        out[["trade_date", "version", "nav_calc_gross"]],
        on=["trade_date", "version"],
        how="left",
    )

    merged["abs_diff_gross"] = (merged["nav_calc_gross"] - merged["nav_gross"]).abs()

    max_abs = float(merged["abs_diff_gross"].max())
    worst = merged.sort_values("abs_diff_gross", ascending=False).head(1)

    print(f"[DB] {DB_PATH}")
    print(f"[RANGE] {start} ~ {end}  version={v}  initial={initial}")

    print("\n=== dtypes debug ===")
    print("nav.trade_date dtype:", nav["trade_date"].dtype)
    print("pos.trade_date dtype:", pos["trade_date"].dtype)
    print("base.trade_date dtype:", base["trade_date"].dtype)

    print("\n=== Gross NAV check (positions_daily_filled vs nav_daily.nav_gross) ===")
    print(f"max_abs_diff(gross): {max_abs:.12f}")
    print("worst_row:")
    print(worst.to_string(index=False))

    # 阈值：理论上应接近 0
    if max_abs > 1e-8:
        raise RuntimeError("[FAIL] gross nav mismatch too large. Need to inspect positions_daily_filled or nav_daily.")
    print("\n[PASS] Step5 filled-view gross NAV matches nav_daily.")


if __name__ == "__main__":
    main()
