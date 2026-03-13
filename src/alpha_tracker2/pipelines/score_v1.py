from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.scoring.plugins.v1_baseline import V1BaselineScorer
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def main() -> None:
    root = Path(__file__).resolve().parents[3]
    s = load_settings(root)

    store = DuckDBStore(
        db_path=s.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    cal = TradingCalendar()
    trade_date = cal.latest_trading_day()

    # 读取 features
    rows = store.fetchall(
        """
        SELECT trade_date, ticker, ret_1d, mom_5d, vol_5d, ma_5
        FROM features_daily
        WHERE trade_date = ?
        ORDER BY ticker;
        """,
        (trade_date,),
    )
    if not rows:
        raise RuntimeError("No features found for today. Run build_features first.")

    feat = pd.DataFrame(
        rows,
        columns=["trade_date", "ticker", "ret_1d", "mom_5d", "vol_5d", "ma_5"],
    )

    scorer = V1BaselineScorer()
    scored = scorer.score(trade_date, feat)

    # 排名（score 越大越好）
    scored = scored.sort_values("score", ascending=False).reset_index(drop=True)
    scored["rank"] = scored.index + 1
    scored["version"] = "V1"
    scored["reason"] = "V1 baseline: 0.6*mom_rank + 0.3*low_vol_rank + 0.1*ret_rank"

    # 写入 picks_daily：先删再插（只影响 V1）
    with store.session() as con:
        con.execute("BEGIN;")
        try:
            con.execute(
                "DELETE FROM picks_daily WHERE trade_date=? AND version='V1';",
                [trade_date],
            )
            for _, r in scored.iterrows():
                con.execute(
                    """
                    INSERT OR REPLACE INTO picks_daily
                    (trade_date, version, rank, ticker, score, reason)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        trade_date,
                        r["version"],
                        int(r["rank"]),
                        r["ticker"],
                        float(r["score"]),
                        r["reason"],
                    ],
                )
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise

    n = store.fetchone("SELECT COUNT(*) FROM picks_daily WHERE trade_date=? AND version='V1';", (trade_date,))[0]

    print("[OK] score_v1 passed.")
    print("trade_date:", trade_date)
    print("rows_written:", n)
    print(scored[["rank", "ticker", "score"]].to_string(index=False))
    print("db:", s.store_db)


if __name__ == "__main__":
    main()
