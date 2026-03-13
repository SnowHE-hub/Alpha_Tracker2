import duckdb

DB = r"D:\alpha_tracker2\data\store\alpha_tracker.duckdb"

sqls = [
    "ALTER TABLE picks_daily ADD COLUMN IF NOT EXISTS reason VARCHAR;",
    "ALTER TABLE picks_daily ADD COLUMN IF NOT EXISTS score_100 DOUBLE;",
    "ALTER TABLE picks_daily ADD COLUMN IF NOT EXISTS thr_value DOUBLE;",
    "ALTER TABLE picks_daily ADD COLUMN IF NOT EXISTS pass_thr BOOLEAN;",
    "ALTER TABLE picks_daily ADD COLUMN IF NOT EXISTS picked_by VARCHAR;",
]

con = duckdb.connect(DB)
for s in sqls:
    con.execute(s)
print("[OK] picks_daily schema patched")

print(con.execute("PRAGMA table_info('picks_daily');").fetchdf())
con.close()
