"""Check available catalogs in Databricks"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

os.environ.setdefault('HASHICORP_HOST', 'http://127.0.0.1:8200')
os.environ.setdefault('HASHICORP_TOKEN', 'root')

from services.workspace.SecretManager import SecretManager
from databricks import sql

SecretManager.ppline_connect_to_vault()
creds = SecretManager.get_db_secret('anonymous-dlt@none.dlt', 'databricks_test', from_pipeline=True)

connection = sql.connect(
    server_hostname=creds['server_hostname'],
    http_path=creds['http_path'],
    access_token=creds['access_token']
)

cursor = connection.cursor()
cursor.execute("SHOW CATALOGS")
catalogs = [row[0] for row in cursor.fetchall()]

print("Available catalogs:")
for cat in catalogs:
    print(f"  - {cat}")

cursor.close()
connection.close()
