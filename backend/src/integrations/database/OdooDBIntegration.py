from utils.DestinationQueryUtil import DestinationQueryUtil
from utils.metastore.PipelineMedatata import PipelineMedatata

class OdooDBIntegration:

    @staticmethod
    def get_modules(namespace, pipeline):
        query = '''
            SELECT m.model AS table_name
            FROM ir_model m
            WHERE 
                m.transient = false  -- Exclude temporary wizard tables
                AND m.state = 'base' -- Focus on installed module core tables
                AND m.model NOT LIKE 'ir.%' -- Exclude internal Odoo metadata
                AND m.model NOT LIKE 'base.%' -- Exclude low-level base config
            ORDER BY  m.model
        '''
        pipeline_meta = PipelineMedatata.get_pipeline_metadata(pipeline, namespace)
        connection_name = pipeline_meta[2]
        return DestinationQueryUtil._query_sql_database(query, namespace, connection_name)
        