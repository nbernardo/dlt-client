from utils.db.lancedb import LanceConnectionFactory
from utils.metastore.DataCatalog import DataCatalog
from utils.metastore.PipelineMedatata import PipelineMedatata
from utils.metastore.BI.DashboardConfig import DashboardConfig

class MetaStore:
    """ Centralize the access to all metastore capabilities (e.g. DataCatalog) """
    @staticmethod
    def _get_lance_conn(dbs_path=None) -> str:
        return LanceConnectionFactory.get(dbs_path)


    @staticmethod
    def persist_catalog(table_source: str, dbs_path=None, pipeline=None, load_info=None, additionals={}, catalog_exist = False):
        """Persists column catalog to LanceDB. Concurrent writes via MVCC — This is called from the pipeline run itself"""
        
        dest_storage = getattr(load_info,'destination_displayable_credentials').split('/')[-1]
        print(f'DEST_STRG=__dest_storage__:{dest_storage}', flush=True) # Notify the main process about pipeline run completion
        print(f'DEST_TABLES=__dest_tables__:{','.join(additionals.get('tbls', []))}', flush=True) # Notify the main process about pipeline run completion
        print(f'TABLESPK=__tables_pks__:{','.join(additionals.get('pks', []))}', flush=True) # Notify the main process about pipeline run completion
        print(f'DATA=__dlt__destination__datasetname__:{pipeline.config.dataset_name}', flush=True)
        print(f'__dlt__transaction_id:{getattr(pipeline._last_trace, 'transaction_id')}', flush=True)
        print(load_info, flush=True) # Print pipeline completion details for main process and UI
        print('RUN_SUCCESSFULLY', flush=True) # Notify the main process about pipeline run completion
        print(f'Analyzing/Generating the data catalog for pipeline with transaction_id {getattr(pipeline._last_trace, 'transaction_id')}')
        
        # TODO: Implement UI based feature flag to trigger this call
        #if not catalog_exist:
        #    DataCatalog.persist_catalog(table_source, dbs_path, pipeline, load_info, table_source, additionals)


    @staticmethod
    def get_pipeline_catalog(pipeline_name, metadata, src_path = None):
        return DataCatalog.get_pipeline_catalog(pipeline_name, metadata, src_path)


    @staticmethod
    def get_pipeline_metadata(pipeline: str, dbs_path=None, display_fields = False):
        return DataCatalog.get_pipeline_datacatalog(pipeline, dbs_path, display_fields)


    @staticmethod
    def get_pipeline_source_destination_type(namespace: str):
        return PipelineMedatata.get_pipeline_source_destination_type(namespace)


    @staticmethod
    def persist_pipeline_metadata(
        namespace: str, pipeline: str, details: dict, dataset_name: str, 
        short_query: str, pipline_plan_id, params: dict = None
    ):
        return PipelineMedatata.update_metadata_and_pipeline_plan(
            namespace, pipeline, details, dataset_name, short_query, pipline_plan_id, params
        )


    @staticmethod
    def update_metadata(namespace: str, pipeline: str, dataset_name: str, short_query: str):
        return PipelineMedatata.update_metadata(namespace, pipeline, dataset_name, short_query)


    @staticmethod
    def save_analytics_chart(namespace, config_details, context, chart_name, data_source, chart_id):
        try:
            DashboardConfig.persist_chart_config(namespace, config_details, context, chart_name, data_source, chart_id)
            return { 'error': False, 'result': {} }
        except Exception as e:
            print(f'Error on saving chart configs {str(e)}')
            return { 'error': True, 'result': f'Error on saving chart configs {str(e)}' }


    @staticmethod
    def save_dashboard(namespace = None, charts_list=None, dashboard_name = None, dashboard_id = None):
        try:
            DashboardConfig.persist_dashboard_config(namespace, charts_list, dashboard_name, dashboard_id)
            return { 'error': False, 'result': {} }
        except Exception as e:
            print(f'Error on saving dashboard configs {str(e)}')
            return { 'error': True, 'result': f'Error on saving dashboard configs {str(e)}' }
        
    
    @staticmethod
    def chart_config_store() -> DashboardConfig: return DashboardConfig