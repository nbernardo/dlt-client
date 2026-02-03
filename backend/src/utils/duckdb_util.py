import duckdb
import time
from duckdb import DuckDBPyConnection

class DuckdbUtil:

    dltdbinstance = None
    dltdbinstance_count = 0
    logdbinstance = None
    workspacedb_path = None

    @staticmethod
    def get_tables(database = None, where = None):
        cnx = duckdb.connect(f'{database}', read_only=True)
        cursor = cnx.cursor()
        where_clause = f'WHERE {where}' if where is not None else ''
        query = f"SELECT \
                    database_name, schema_name, table_name, \
                    estimated_size, column_count FROM \
                    duckdb_tables {where_clause}"
        print(f'THE QUERY WILL BE: {query}')
        return cursor.execute(query)
    

    @staticmethod
    def get_workspace_db_instance():
        DuckdbUtil.dltdbinstance_count += 1
        if DuckdbUtil.dltdbinstance_count == 1:
            workspacedb = f'{DuckdbUtil.workspacedb_path}/dltworkspace.duckdb'
            DuckdbUtil.dltdbinstance = duckdb.connect(workspacedb)
        return DuckdbUtil.dltdbinstance    


    @staticmethod
    def get_log_db_instance():
        if DuckdbUtil.logdbinstance == None:
            workspacedb = f'{DuckdbUtil.workspacedb_path}/dltlogs.duckdb'
            DuckdbUtil.logdbinstance = duckdb.connect(workspacedb)
        return DuckdbUtil.logdbinstance


    @staticmethod
    def check_pipline_db(dbfile_path):
        """
        This is only for trying to check if duckdb is not locked in case it's locked an exception will
        be thrown, because this is calle in the pipeline job, it'll prevent it to move forward
        """
        cnx = DuckdbUtil.get_connection_for(dbfile_path)
        cnx.close()
        time.sleep(1)


    @staticmethod
    def create_socket_conection_table():
        cnx = DuckdbUtil.get_workspace_db_instance()
        query = "CREATE TABLE IF NOT EXISTS socket_connection (\
            namespace VARCHAR,\
            socket_id VARCHAR,\
            PRIMARY KEY (namespace))"
        cnx.execute(query)


    @staticmethod
    def create_namespace_alias_table():
        cnx = DuckdbUtil.get_workspace_db_instance()
        query = "CREATE TABLE IF NOT EXISTS namespace (\
            namespace_id VARCHAR,\
            namespaces_alias JSON,\
            PRIMARY KEY (namespace_id))"
        cnx.execute(query)


    @staticmethod
    def create_cache_table():
        cnx = DuckdbUtil.get_workspace_db_instance()
        query = "CREATE TABLE IF NOT EXISTS cache (\
                key VARCHAR PRIMARY KEY,\
                value VARCHAR,\
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP\
                )"
        cnx.execute(query)


    @staticmethod
    def create_namespace_user_table():
        cnx = DuckdbUtil.get_workspace_db_instance()
        query = "CREATE TABLE IF NOT EXISTS users (\
            user_email VARCHAR,\
            namespaces JSON,\
            PRIMARY KEY (user_email))"
        cnx.execute(query)


    @staticmethod
    def create_ppline_schedule_table():
        cnx = DuckdbUtil.get_workspace_db_instance()
        cnx.execute('CREATE SEQUENCE ppline_schedul_sequence;')

        query = "CREATE TABLE IF NOT EXISTS ppline_schedule (\
            id INTEGER PRIMARY KEY DEFAULT nextval('ppline_schedul_sequence'),\
            ppline_name VARCHAR,\
            type VARCHAR,\
            periodicity VARCHAR,\
            time VARCHAR,\
            namespace VARCHAR,\
            last_run TIMESTAMP,\
            schedule_settings JSON,  \
            is_paused VARCHAR\
            )"
        cnx.execute(query)

    @staticmethod
    def create_pipeline_logs_table():
        """
        Create the pipeline_logs table for persistent log storage.
        This table stores all logs from dltHub pipeline runs and application-level Python logs.
        """
        cnx = DuckdbUtil.get_log_db_instance()
        
        # Create sequence for log IDs
        cnx.execute('CREATE SEQUENCE IF NOT EXISTS pipeline_logs_sequence;')
        
        # Create the main logs table
        query = """CREATE TABLE IF NOT EXISTS pipeline_logs (
            id INTEGER PRIMARY KEY DEFAULT nextval('pipeline_logs_sequence'),
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            namespace VARCHAR(255) NOT NULL,
            pipeline_id VARCHAR(255) NOT NULL,
            execution_id VARCHAR(255) NOT NULL,
            log_level VARCHAR(20) NOT NULL,
            logger_name VARCHAR(255) NOT NULL,
            message TEXT NOT NULL,
            module VARCHAR(255),
            function_name VARCHAR(255),
            line_number INTEGER,
            thread_id BIGINT,
            process_id INTEGER,
            correlation_id VARCHAR(255),
            extra_data JSON,
            stack_trace TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
        cnx.execute(query)
        
        # Create indexes for efficient querying
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_pipeline_logs_pipeline_id ON pipeline_logs(pipeline_id)",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_logs_execution_id ON pipeline_logs(execution_id)",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_logs_timestamp ON pipeline_logs(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_logs_level ON pipeline_logs(log_level)",
            "CREATE INDEX IF NOT EXISTS idx_pipeline_logs_correlation_id ON pipeline_logs(correlation_id)"
        ]
        
        for index_query in indexes:
            cnx.execute(index_query)

    @staticmethod
    def workspace_table_exists(tbl = 'namespace'):
        cnx = DuckdbUtil.get_workspace_db_instance()
        cursor = cnx.cursor()
        query = f"SELECT EXISTS (SELECT 1 FROM duckdb_tables WHERE table_name = '{tbl}') as tbl_exists"
        result = cursor.execute(query).fetchall()[0][0]   
        return result


    @staticmethod
    def get_socket_id(namespace):
        cnx = DuckdbUtil.get_workspace_db_instance()
        cursor = cnx.cursor()
        query = f"SELECT socket_id FROM socket_connection WHERE namespace = '{namespace}'"
        result = cursor.execute(query).fetchall()[0][0]   
        return result


    @staticmethod
    def create_namespace_alias(namespace):
        cnx = DuckdbUtil.get_workspace_db_instance()
        cursor = cnx.cursor()
        query = f"SELECT namespaces_alias FROM namespace WHERE namespace_id = '{namespace}'"
        result = cursor.execute(query).fetchall()[0][0]   
        return result


    db_connections: list[DuckDBPyConnection] = {}
    @staticmethod
    def get_connection_for(_db_filename) -> DuckDBPyConnection:
        db_filename = _db_filename.replace('//','/')
        if(not(db_filename in DuckdbUtil.db_connections)):
            DuckdbUtil.db_connections[db_filename] = duckdb.connect(f'{db_filename}')
        
        try:
            DuckdbUtil.db_connections[db_filename].query('SELECT 1')
            return DuckdbUtil.db_connections[db_filename]
        except Exception as err:
            print(f'Reconnecting to DB {db_filename}')
            DuckdbUtil.db_connections[db_filename] = duckdb.connect(f'{db_filename}')
            return DuckdbUtil.db_connections[db_filename]
            
    
    
    @staticmethod
    def del_connection_for(db_filename) -> DuckDBPyConnection:
        del DuckdbUtil.db_connections[db_filename]

    @staticmethod
    def initialize_logging_tables():
        """
        Initialize all logging-related tables.
        This method should be called during application startup.
        """
        try:
            DuckdbUtil.create_pipeline_logs_table()
            print("Pipeline logs table initialized successfully")
        except Exception as e:
            print(f"Error initializing logging tables: {e}")
            # Don't raise the exception to prevent application startup failure
            # Logging should be optional and not break the main application


