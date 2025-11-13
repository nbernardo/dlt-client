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

        schema = None
        if len(self.source_tables) > 0:
            if(self.source_tables[0].__contains__('.')):
                schema = self.source_tables[0].split('.')[0]

                # When the UI sends the tables that is under a schema (e.g. Postgres, SQLServer)
                # the tables names will be prefixed with the schema name, bellow logic is to clean it up
                self.source_tables = [table.replace(f'{schema}.','') for table in self.source_tables]

        # primary_keys fields is mapped in /pipeline_templates/sql_db.txt
        self.primary_keys = list(data['primaryKeys'].values())

        # source_database fields is mapped in /pipeline_templates/sql_db.txt
        self.source_database = data['database']
        
        # source_dbengine fields is mapped in /pipeline_templates/sql_db.txt
        self.source_dbengine = data['dbengine']

        # source_dbengine fields is mapped in /pipeline_templates/sql_db.txt
        self.namespace = data['namespace']

        # source_dbengine fields is mapped in /pipeline_templates/sql_db.txt
        self.connection_name = data['connectionName']

        # source_dbengine fields is mapped in /pipeline_templates/sql_db.txt
        self.schema = schema


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
        Check if the table exists
        TODO: Implemente validation to check table existance if needed otherwise
              this should be deprecated since the connection not comes from secrets 
              connection which provides auto-complete for the existing table thereby
              preventing non-existing tables to be send from the UI
        """
        
        """        
        query_template = 'SELECT 1 FROM @tblName'
        final_query = query_template.replace('@tblName', tables[0])
        try:

            for tbl in tables[1:]:
                final_query += ' UNION '
                final_query += query_template.replace('@tblName', tbl)

            print('FILAN QUERY IS: '+final_query)

            #If specified DB does not exists it'll throw an exception
            db_connection = get_mysql_connection(self.source_database)
            #If any of tables in the query does not exists it'll throw an exception
            db_connection.cursor().execute(final_query)

            # Notify the UI that this step completed successfully
            self.notify_completion_to_ui()

        except Exception as err:
            self.notify_failure_to_ui('SqlDBComponent', err)    
        """


