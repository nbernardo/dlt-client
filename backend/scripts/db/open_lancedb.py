import duckdb
from pathlib import Path
import time

datacatalog_path = Path(__file__).parent / "../../dbs/files/catalog.lance/column_catalog.lance"
metadata_path = Path(__file__).parent / "../../dbs/files/catalog.lance/pipeline_metadata.lance"
charts_path = Path(__file__).parent / "../../dbs/files/catalog.lance/dashboard_config.lance"

con = duckdb.connect()
con.execute("LOAD lance")
con.execute(f"CREATE VIEW column_catalog AS SELECT * FROM '{datacatalog_path.resolve()}'")
con.execute(f"CREATE VIEW pipeline_metadata AS SELECT * FROM '{metadata_path.resolve()}'")
con.execute(f"CREATE VIEW chart_config AS SELECT * FROM '{charts_path.resolve()}'")
con.execute(f"CREATE VIEW dashboard_config AS SELECT * FROM '{charts_path.resolve()}'")

print(f"✅ Connected to LanceDB tables: column_catalog and pipeline_metadata")
print("📊 Opening DuckDB UI at http://localhost:4213")

con.execute("CALL start_ui()")

while True:
    time.sleep(1)