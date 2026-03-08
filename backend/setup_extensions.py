import duckdb

print("Installing DuckDB extensions...")
con = duckdb.connect()
con.execute("INSTALL lance FROM community")
con.close()
print("✅ DuckDB extensions installed")