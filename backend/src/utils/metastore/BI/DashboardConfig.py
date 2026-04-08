import pyarrow as pa
import duckdb
from utils.db.lancedb import LanceConnectionFactory
from lancedb import Table
from datetime import datetime
from utils.duckdb_util import DuckdbUtil

CHART_CONFIG_SCHEMA = pa.schema([
    pa.field('config_details', pa.string()),
    pa.field('chart_name', pa.string()),
    pa.field('chart_id', pa.string()),
    pa.field('namespace', pa.string()),
    pa.field('data_source', pa.string()),
    #This is the pipeline name from which the chart got generated
    pa.field('context', pa.string()),
    pa.field('created_at', pa.string()),
    pa.field('updated_at', pa.string())
])


DASH_CONFIG_SCHEMA = pa.schema([
    pa.field('charts_list', pa.string()),
    pa.field('dashboard_name', pa.string()),
    pa.field('dashboard_id', pa.string()),
    pa.field('namespace', pa.string()),
    pa.field('created_at', pa.string()),
    pa.field('updated_at', pa.string())
])


class DashboardConfig:
    """LanceDB-backed catalog store. Writes via LanceDB (MVCC), reads via DuckDB SQL."""

    @staticmethod
    def _get_lance_conn() -> str:
        return LanceConnectionFactory.get()


    @staticmethod
    def _get_dashboard_table() -> Table:
        """Opens or creates the LanceDB chart_config table"""
        try:
            return DashboardConfig._get_lance_conn().open_table('dashboard_config')
        except Exception:
            try:
                return DashboardConfig._get_lance_conn().create_table('dashboard_config', schema=DASH_CONFIG_SCHEMA)
            except Exception:
                return DashboardConfig._get_lance_conn().open_table('dashboard_config')


    @staticmethod
    def _get_chart_table() -> Table:
        """Opens or creates the LanceDB chart_config table"""
        try:
            return DashboardConfig._get_lance_conn().open_table('chart_config')
        except Exception:
            try:
                return DashboardConfig._get_lance_conn().create_table('chart_config', schema=CHART_CONFIG_SCHEMA)
            except Exception:
                return DashboardConfig._get_lance_conn().open_table('chart_config')


    @staticmethod
    def _get_duckdb_conn() -> duckdb.DuckDBPyConnection:
        """Returns a DuckDB connection with a catalog view over the LanceDB files."""
        lance_path = DuckdbUtil.workspacedb_path
        con = duckdb.connect()
        con.execute("LOAD lance")
        con.execute(f"CREATE VIEW chart_config AS SELECT * FROM '{lance_path}/catalog.lance/chart_config.lance'")
        return con


    @staticmethod
    def persist_dashboard_config(namespace = None, charts_list=None, dashboard_name = None, dashboard_id = None):
        """Persists the pipeline metadata — This is called from the pipeline run itself"""
        now = datetime.now().isoformat()

        try:
            tbl = DashboardConfig._get_dashboard_table()
            rows_to_insert = [
                { 
                    'charts_list': charts_list, 'dashboard_name': dashboard_name, 'dashboard_id': dashboard_id,
                    'namespace': namespace, 'created_at': now
                }
            ]
            tbl.add(rows_to_insert)
            if tbl.version % 100 == 0: DashboardConfig.compact_metadata()

        except Exception as e:
            print(f"Dashboard config save Failed: {str(e)}")


    @staticmethod
    def persist_chart_config(
        namespace = None, config_details=None, context = None, 
        chart_name = None, data_source = None, chart_id = None
    ):
        """Persists the chart metadata"""
        now = datetime.now().isoformat()

        try:
            tbl = DashboardConfig._get_chart_table()
            DashboardConfig._migrate(tbl)

            rows_to_insert = [
                { 
                    'config_details': config_details, 'namespace': namespace, 'created_at': now, 
                    'context': context, 'chart_name': chart_name, 'data_source': data_source, 'chart_id': chart_id 
                }
            ]
            tbl.add(rows_to_insert)
            if tbl.version % 100 == 0: DashboardConfig.compact_metadata()

        except Exception as e:
            print(f"Chart config save Failed: {str(e)}")


    @staticmethod
    def get_chart_configs(namespace: str = None, chart_name: str = None):
        try:
            con = DashboardConfig._get_duckdb_conn()
            more_filter = f"and chart_name = '{chart_name}'" if chart_name != None else ''
            return con.execute(f'SELECT config_details FROM chart_config WHERE namespace = ? {more_filter}', [namespace]).fetchall()

        except (Exception, IOError) as err:
            if(str(err).lower().__contains__('not found')): return []
            if str(err).__contains__('Extension'):
                print(f'ERROR: Duckdb missing Extension - The lancedb extension for Duckdb is not installed: {err}')
                print(f'Please install this extentions according to the documentation on https://duckdb.org/docs/current/core_extensions/lance')
                return []
            else:
                print(f'Error while loading catalog: {err}')
            return err


    @staticmethod
    def compact_metadata(older_than_days=30):
        from datetime import timedelta
        tbl = DashboardConfig._get_chart_table()
        tbl.cleanup_old_versions(older_than=timedelta(days=older_than_days))
        tbl.compact_files()
        print("✅ ConfigChart compacted")

    
    @staticmethod
    def _migrate(tbl):
        """This is used to add a new metadata field in case it didn't exist"""
        try:
            existing_cols = tbl.schema.names
            new_fields = {}

            for col, expr in new_fields.items():
                if col not in existing_cols:
                    tbl.add_columns({col: expr})
                    print(f'chart_config.{col} added')
                else:
                    print(f'chart_config.{col} already exists — skipped')

        except Exception as e:
            print(f'chart_config migration failed: {e}')    