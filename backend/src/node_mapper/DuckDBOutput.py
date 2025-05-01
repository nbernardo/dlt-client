import duckdb
from .TemplateNodeType import TemplateNodeType
from node_mapper.RequestContext import RequestContext
import os

class DuckDBOutput(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext):
        """
        Initialize the instance
        """
        self.context = context
        self.component_id = data['componentId']

        self.context.emit_start(self, '')

        self.database = data['database']
        self.table_name = data['tableName']

    def run(self) -> None:
        """
        Run the initial steps
        """
        super().run()
        print(f'Inited DuckDB with : {self.database} and {self.table_name}')
        self.check_table()

    def check_table(self) -> None:
        """
        Check if the table already exists
        """
        path = self.context.ppline_files_path
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
                    error = {'message': error, 'componentId': self.component_id}
                    self.context.add_exception('DuckDBOutput', error)
                    self.context.emit_error(self, error)
            
            if error is not None:
                success = {'componentId': self.component_id}
                self.context.emit_success(self, success)

        except Exception as err:
            print(f'Error on querying DB {err}')
