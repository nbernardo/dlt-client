import duckdb
from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
import os
import uuid

class DuckDBOutput(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext, component_id = None):
        """
        Initialize the instance
        """
        self.template = None

        # When instance is created only to get the template 
        # Nothing more takes place except for the template itself
        if data is None: return None

        self.context = context
        self.component_id = data['componentId']

        self.context.emit_start(self, '')
        # database is mapped in /pipeline_templates/simple.txt and simple_transform_field.txt
        self.output_dest_name = data['database']
        # table_name is mapped in /pipeline_templates/simple.txt and simple_transform_field.txt
        self.ppline_dest_table = data.get('tableName', f'random_tbl_{str(uuid.uuid4()).replace('-','_')}')


    def run(self) -> None:
        """
        Run the initial steps
        """
        super().run()
        print(f'Inited DuckDB with : {self.output_dest_name} and {self.ppline_dest_table}')
        self.check_table()

    def check_table(self) -> None:
        """
        Check if the table already exists
        """
        path = self.context.ppline_path
        cnx, error = None, None

        if os.path.exists(f'{path}/{self.context.ppline_name}.duckdb'):
            cnx = duckdb.connect(f'{path}/{self.context.ppline_name}.duckdb')
        try:
            if cnx:
                table = self.table_name
                dbname = self.database
                query = f"SELECT * FROM duckdb_tables WHERE table_name = '{table}'"
                result = cnx.sql(query)
                if (result.fetchone() is not None):
                    error = f'Table with name {table} already exists for {dbname}'
                    self.notify_failure_to_ui('DuckDBOutput', error)
            
            if error is not None:
                self.notify_completion_to_ui()

        except Exception as err:
            print(f'Error on querying DB {err}')
