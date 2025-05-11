from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
from connectors.db.mysql import get_mysql_connection
from services.pipeline.DltPipeline import DltPipeline

class SqlDBComponent(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext):
        """
        Initialize the instance
        """
        self.template = DltPipeline.get_sql_db_template()

        # When instance is created only to get the template 
        # Nothing more takes place except for the template itself
        if data is None: return None

        self.context = context
        self.component_id = data['componentId']
        self.context.emit_start(self, '')

        # source_tables fields is mapped in /pipeline_templates/sql_db.txt
        self.source_tables = list(data['tables'].values())
        # source_database fields is mapped in /pipeline_templates/sql_db.txt
        self.source_database = data['database']
        # source_dbengine fields is mapped in /pipeline_templates/sql_db.txt
        self.source_dbengine = data['dbengine']


    def run(self) -> None:
        """
        Run the initial steps
        """
        super().run()
        print(f'Inited Source SqlDb with : \
              {self.source_database} and {self.source_tables}\
              and DBEngine is {self.source_dbengine}')
        self.check_db_and_tables(self.source_tables)


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
            db_connection = get_mysql_connection(self.source_database)
            #If any of tables in the query does not exists it'll throw an exception
            db_connection.cursor().execute(final_query)

            # Notify the UI that this step completed successfully
            self.notify_completion_to_ui()

        except Exception as err:
            self.notify_failure_to_ui('SqlDBComponent', err)    


