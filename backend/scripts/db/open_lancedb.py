import duckdb
from pathlib import Path
import time

lance_table_path = Path(__file__).parent / "../../dbs/files/catalog.lance/column_catalog.lance"

con = duckdb.connect()
con.execute("LOAD lance")
con.execute(f"CREATE VIEW column_catalog AS SELECT * FROM '{lance_table_path.resolve()}'")

print(f"✅ Connected to: {lance_table_path.resolve()}")
print("📊 Opening DuckDB UI at http://localhost:4213")

con.execute("CALL start_ui()")

while True:
    time.sleep(1)