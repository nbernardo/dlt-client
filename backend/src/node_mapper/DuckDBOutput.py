import duckdb
from .TemplateNodeType import TemplateNodeType
from node_mapper.RequestContext import RequestContext
from flask_socketio import emit


class DuckDBOutput(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext):
        """
        Initialize the instance
        """
        self.database = data['database']
        self.table_name = data['tableName']
        self.context = context

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
        cnx = duckdb.connect(f'{path}/{self.context.ppline_name}.duckdb')
        try:
            sid = self.context.socket_sid
            table = self.table_name
            query = f"SELECT * FROM duckdb_tables WHERE table_name = '{table}'"
            result = cnx.sql(query)
            if (result.fetchone() is not None):
                self.context.add_exception(
                    'DuckDBOutput', 'Table already exusts')
                emit('pplineError', 'Table already',
                     namespace='/pipeline', to=sid)

        except Exception as err:
            print(f'Error on querying DB {err}')
