from utils.metastore.meta_storage import MetaStore
from integrations.database.OdooDBIntegration import OdooDBIntegration
from utils.DestinationQueryUtil import DestinationQueryUtil
from utils.duckdb_util import DuckdbUtil
from integrations.database.DuckdbStage import DuckdbStage


def handle_duckdb_path(connection_name, namespace):
    import platform
    from controller.pipeline import BasePipeline
    [tbl_catalog, dataset] = str(connection_name).split('.')
    sep = '/' if platform.system() != 'Windows' else '\\\\'
    db_path = f'{BasePipeline.folder}{sep}duckdb{sep}{namespace}{sep}{tbl_catalog}.duckdb'

    return db_path, dataset, tbl_catalog



class BIService:

    @staticmethod
    def save_dashboard(namespace = None, charts_list=None, dashboard_name = None, dashboard_id = None):
        return MetaStore.save_dashboard(namespace, charts_list, dashboard_name, dashboard_id)    
    
    
    @staticmethod
    def save_chart(namespace, config_details, context, chart_name, data_source, chart_id):
        return MetaStore.save_analytics_chart(namespace, config_details, context, chart_name, data_source, chart_id)    
    
    
    @staticmethod
    def get_chart_configs(namespace, chart_name = None):
        return MetaStore.chart_config_store().get_chart_configs(namespace, chart_name)  
    
      
    @staticmethod
    def get_dashboard_configs(namespace):
        return MetaStore.chart_config_store().get_dashboard_configs(namespace)


    @staticmethod
    def get_db_tables(namespace, connection_name, pipeline):
        if(str(connection_name).__contains__('.')):
            db_path, dataset, _ = handle_duckdb_path(connection_name, namespace)
            result = DuckdbStage.get_duckdb_tables(db_path, dataset)
            return result
            
        else:
            return OdooDBIntegration.get_db_tables(namespace, connection_name, pipeline)
    

    @staticmethod
    def get_odoo_tables_hierarchy(anchor_table, namespace, connection_name, pipeline):
        if(str(connection_name).__contains__('.')):
            db_path, dataset, tbl_catalog = handle_duckdb_path(connection_name, namespace)
            result = BIService.get_duckdb_tables_hererarchy(db_path, tbl_catalog, anchor_table, dataset)
            return result
            ...
        else:
            return OdooDBIntegration.get_tables_hierarchy(anchor_table, namespace, connection_name, pipeline)    


    @staticmethod
    def query_sql_rdbms(query, namespace, connection_name):
        result = None
        if not connection_name:
            return { 'result': 'Invalid data source', 'error': True }
        
        line_count, invalid_query = 0, False
        for query_line in str(query).split('\n'):
            if line_count > 0: break
            if str(query_line).strip().startswith('--') or str(query_line).strip() == '':
                continue
            if not str(query_line).strip().lower().startswith('select'):
                invalid_query = True
            line_count += 1

        if(invalid_query):
            return { 'result': 'Invalid type of query', 'error': True }
        
        if(str(connection_name).__contains__('.')):
            db_path, dataset, _ = handle_duckdb_path(connection_name, namespace)
            result = DuckdbStage.run_sql_query_on_stage_tables(db_path, query)
        else:
            result = DestinationQueryUtil.query_sql_database(query, namespace, connection_name)

        return { **result, 'error': result.get('error', False) }
    

    @staticmethod
    def get_duckdb_tables_hererarchy(db_file, tbl_catalog, table, dataset):

        try:
            return DuckdbStage.get_tables_hierarchy(db_file, table, tbl_catalog, dataset)
            
        except Exception as error:
            result = f'Erro while running analytics query. {str(error)}'
            return result