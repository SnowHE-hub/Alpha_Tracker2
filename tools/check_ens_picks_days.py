# tools/check_ens_picks_days.py
from pathlib import Path
import duckdb

db = r"D:\alpha_tracker2\data\store\alpha_tracker.duckdb"
con = duckdb.connect(db)

rows = con.execute("""
SELECT trade_date, COUNT(*) AS n
FROM picks_daily
WHERE version='ENS'
GROUP BY trade_date
ORDER BY trade_date
""").fetchall()

print("ENS picks days:", len(rows))
for d, n in rows[:30]:
    print(d, n)

con.close()
