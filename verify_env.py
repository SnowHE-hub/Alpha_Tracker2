# verify_env.py
import sys
import yaml
import pandas as pd
import duckdb

print("Python:", sys.version)
print("Executable:", sys.executable)

# YAML 读写测试（模拟后续 default.yaml）
obj = {"ok": True, "paths": {"runs_dir": "data/runs"}}
s = yaml.safe_dump(obj, allow_unicode=True)
obj2 = yaml.safe_load(s)
assert obj2["ok"] is True

# DuckDB 最小写入读取测试（模拟后续 nav 表写入）
con = duckdb.connect("verify.duckdb")
con.execute("CREATE TABLE IF NOT EXISTS t(a INT, b VARCHAR);")
con.execute("INSERT INTO t VALUES (1, 'hello');")
df = con.execute("SELECT * FROM t;").df()
con.close()

print(df)
print("[OK] env verified.")
