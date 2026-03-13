import duckdb
import pandas as pd

DB = r"D:\alpha_tracker2\data\store\alpha_tracker.duckdb"
TRADE_DATE = "2026-01-14"
VERSIONS = ("V1", "V2", "V3", "V4")


def _pretty_nulls_for_print(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    仅用于打印展示：把 pandas 的 NA/NaN 统一成 Python None，
    避免输出 NaN/<NA> 误导（DuckDB 里其实是 SQL NULL）。
    """
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].astype(object)
            out[c] = out[c].where(~pd.isna(out[c]), None)
    return out


def main():
    con = duckdb.connect(DB)

    # 1) 表结构
    print(con.execute("PRAGMA table_info('picks_daily');").fetchdf())

    # 2) 统计（以 DuckDB SQL 为准，保证 null_thr 真实反映 SQL NULL）
    stats_df = con.execute(
        f"""
        SELECT version,
               COUNT(*) n,
               MIN(score_100) min_s100,
               MAX(score_100) max_s100,
               SUM(CASE WHEN thr_value IS NULL THEN 1 ELSE 0 END) null_thr
        FROM picks_daily
        WHERE trade_date='{TRADE_DATE}' AND version IN {VERSIONS}
        GROUP BY version
        ORDER BY version;
        """
    ).fetchdf()
    print(stats_df)

    # 3) 样例行：fetchdf() 后 pandas 会把 NULL 显示成 NaN/<NA>，这里做“展示层清洗”
    sample_df = con.execute(
        f"""
        SELECT version, rank, ticker, score, score_100, thr_value, pass_thr, picked_by, reason
        FROM picks_daily
        WHERE trade_date='{TRADE_DATE}' AND version IN {VERSIONS}
        ORDER BY version, rank
        LIMIT 20;
        """
    ).fetchdf()

    # 关键：只改显示，不改数据
    sample_df = _pretty_nulls_for_print(sample_df, cols=["thr_value", "pass_thr"])

    print(sample_df)

    con.close()


if __name__ == "__main__":
    main()
