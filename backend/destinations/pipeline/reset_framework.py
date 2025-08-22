import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database
from os import getenv as env
from pathlib import Path
import sys

#Adding root folder to allow import  from src
src_path = str(Path(__file__).parent).replace('/destinations/pipeline','')
sys.path.insert(0, src_path)

from src.services.workspace.Workspace import Workspace

mysql_srv = env('MYSQLDBSRV')
mysql_usr = env('MYSQLDBUSR')
mysql_pwd = env('MYSQLDBPWD')
mysql_prt = env('MYSQLDBPRT')

# Bellow mapping: schema = SqlDBComponent.source_database
database_to_connect = "testdb"

dialects = {
    "mysql": f"mysql+pymysql://{mysql_usr}:\
            {mysql_pwd}@{mysql_srv}:\
            {mysql_prt}/{database_to_connect}" 
}

# Bellow mapping: schema = SqlDBComponent.source_dbengine
credentials = ConnectionStringCredentials(dialects["mysql"])

def load_select_tables_from_db():
    dest_folder = Workspace.get_duckdb_path_on_ppline()
    ppline_name = "reset_framework"
    dest_db = dlt.destinations.duckdb(f'{dest_folder}/{ppline_name}.duckdb')
    pipeline = dlt.pipeline(
        pipeline_name=ppline_name,
        destination=dest_db,
        # Bellow mapping: schema = DuckDBOutput.duckdb_dest
        dataset_name="saida"
    )

    # Bellow mapping: schema = SqlDBComponent.source_tables
    source = sql_database(table_names=['orders'], credentials=credentials)

    info = pipeline.run(source)
    print(info)

load_select_tables_from_db()