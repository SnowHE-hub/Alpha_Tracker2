import duckdb

db = r"D:\alpha_tracker2\data\store\alpha_tracker.duckdb"
con = duckdb.connect(db)

print("\n=== features_daily: rows per trade_date ===")
print(
    con.execute("""
        SELECT trade_date, COUNT(*) AS n
        FROM features_daily
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT 5
    """).fetchdf()
)

print("\n=== features_daily: distinct tickers per trade_date ===")
print(
    con.execute("""
        SELECT trade_date, COUNT(DISTINCT ticker) AS nt
        FROM features_daily
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT 5
    """).fetchdf()
)

con.close()
