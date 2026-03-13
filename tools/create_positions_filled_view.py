from __future__ import annotations

import duckdb

DB_PATH = r"D:\alpha_tracker2\data\store\alpha_tracker.duckdb"


def main() -> None:
    con = duckdb.connect(DB_PATH)

    # 清理旧对象（如果存在）
    con.execute("DROP VIEW IF EXISTS positions_daily_filled;")

    # DuckDB 兼容版 ffill：
    # 对每个 (asof_date, ticker)，取 prices_daily 中 <= asof_date 的最后一个非空 close
    # 用 correlated subquery 实现（DuckDB 支持）
    con.execute(
        """
        CREATE VIEW positions_daily_filled AS
        SELECT
          p.asof_date,
          p.version,
          p.ticker,
          p.shares,
          p.price AS price_raw,
          p.market_value AS market_value_raw,

          -- 当日 exact close
          (
            SELECT pd.close
            FROM prices_daily pd
            WHERE pd.trade_date = p.asof_date
              AND pd.ticker = p.ticker
            LIMIT 1
          ) AS close_exact,

          -- ffill close: <= asof_date 最近一个非空 close
          (
            SELECT pd2.close
            FROM prices_daily pd2
            WHERE pd2.ticker = p.ticker
              AND pd2.trade_date <= p.asof_date
              AND pd2.close IS NOT NULL
            ORDER BY pd2.trade_date DESC
            LIMIT 1
          ) AS close_ffill,

          -- 最终用于估值的价格：优先用 positions_daily.price，否则用 close_ffill
          COALESCE(
            p.price,
            (
              SELECT pd2.close
              FROM prices_daily pd2
              WHERE pd2.ticker = p.ticker
                AND pd2.trade_date <= p.asof_date
                AND pd2.close IS NOT NULL
              ORDER BY pd2.trade_date DESC
              LIMIT 1
            )
          ) AS price_filled,

          -- 最终估值：shares * price_filled（现金行不算）
          CASE
            WHEN p.ticker = '__CASH__' THEN NULL
            ELSE CAST(p.shares AS DOUBLE) * COALESCE(
              p.price,
              (
                SELECT pd2.close
                FROM prices_daily pd2
                WHERE pd2.ticker = p.ticker
                  AND pd2.trade_date <= p.asof_date
                  AND pd2.close IS NOT NULL
                ORDER BY pd2.trade_date DESC
                LIMIT 1
              )
            )
          END AS market_value_filled,

          p.cash,
          p.created_at
        FROM positions_daily p
        ;
        """
    )

    con.close()
    print("[OK] created view: positions_daily_filled (v2, no IGNORE NULLS)")
    print(f"[DB] {DB_PATH}")


if __name__ == "__main__":
    main()
