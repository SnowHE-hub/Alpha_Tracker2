import duckdb
db = r"D:\alpha_tracker2\data\store\alpha_tracker.duckdb"
con = duckdb.connect(db)

print(con.execute("""
SELECT version, COUNT(*) n, COUNT(DISTINCT ticker) nt
FROM picks_daily
WHERE trade_date='2026-01-14'
GROUP BY version
ORDER BY version;
""").fetchdf())

print("\nTop picks per version:")
print(con.execute("""
SELECT version, rank, ticker, score
FROM picks_daily
WHERE trade_date='2026-01-14' AND version IN ('V1','V2','V3','V4')
ORDER BY version, rank
LIMIT 50;
""").fetchdf())

con.close()
