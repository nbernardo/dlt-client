from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
from connectors.db.mysql import get_mysql_connection


class SqlDBComponent(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext):
        """
        Initialize the instance
        """
        self.context = context
        self.tables = data['tables']
        self.database = data['database']
        self.component_id = data['componentId']
        self.dbengine = data['dbengine']
        self.table_list = list(self.tables.values())

        self.context.emit_start(self, '')

    def run(self) -> None:
        """
        Run the initial steps
        """
        super().run()
        print(f'Inited Source SqlDb with : \
              {self.database} and {self.tables}\
              and DBEngine is {self.dbengine}')
        self.check_db_and_tables(self.table_list)

    def check_db_and_tables(self, tables: list[str]) -> None:
        """
        Check if the table already exists
        """
        query_template = 'SELECT 1 FROM @tblName'
        final_query = query_template.replace('@tblName', tables[0])
        try:

            for tbl in tables[1:]:
                final_query += ' UNION '
                final_query += query_template.replace('@tblName', tbl)

            #If specified DB does not exists it'll throw an exception
            db_connection = get_mysql_connection(self.database)
            #If any of tables in the query does not exists it'll throw an exception
            db_connection.cursor().execute(final_query)

        except Exception as err:
            error = {'message': f'{err}', 'componentId': self.component_id}
            self.context.emit_error(self, error)
    


