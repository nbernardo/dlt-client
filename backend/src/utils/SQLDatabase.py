import json
from sqlalchemy import create_engine, inspect, MetaData, Engine, Table, text
from sqlalchemy.engine import reflection
from sqlalchemy.exc import SQLAlchemyError
from services.workspace.supper.SecretManagerType import SecretManagerType
import traceback
import platform

class SQLDatabase:

    """
    This util classe has the sole purpose to connect to SQL databases using
    secret thereby providing the workspace with Dynamic connection capabilities
    """

    secret_manager: SecretManagerType
    connections = {
        'mysql': {},
        'postgresql': {},
        'mssql': {},
        'oracle': {},
    }


    def get_mysql_tables(namespace, connection_name, secret):

        mysql_conection, database = SQLConnection\
                                        .mysql_connect(namespace, connection_name, secret)
        
        query_string = f'''
            SELECT table_name, column_name, CONCAT('"', COLUMN_TYPE, '"') AS quoted_type
            FROM information_schema.columns
            WHERE table_schema = :schema
            ORDER BY table_name, ORDINAL_POSITION
        '''

        tables = {}
        result = mysql_conection.connect().execute(text(query_string), { 'schema': database })
        for table_name, column_name, col_type in result.fetchall():
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append({ 'column': column_name, 'type':  col_type  })

        return tables


    def get_pgsql_tables(namespace, connection_name, secret):
        
        pgsql_conection, database = SQLConnection\
                                        .pgsql_connect(namespace, connection_name, secret)

        query_string = """
            SELECT
                table_catalog AS database_name,
                table_schema AS schema_name,
                table_name AS table_name,
                column_name AS column_name,
                '"' || data_type || '"' AS quoted_type
            FROM information_schema.columns
            WHERE table_catalog = :database_name
            ORDER BY table_schema, table_name, ordinal_position;
        """

        tables = {}
        result = pgsql_conection.connect().execute(text(query_string), { 'database_name': database })
        for dbo, table_schema, table_name, column_name, col_type in result.fetchall():
   
            if table_schema not in tables:
                tables[table_schema] = {}

            if table_name not in tables[table_schema]:
                tables[table_schema][table_name] = []

            tables[table_schema][table_name].append({ 'column': column_name, 'type':  col_type  })

        tables['schema_based'] = True
        return tables


    def get_mssql_tables(namespace, connection_name, secret):
        
        mssql_conection, database = SQLConnection\
                    .mssql_connect(namespace, connection_name, secret)

        query_string = """
            SELECT 
                TABLE_CATALOG AS database_name,
                TABLE_SCHEMA AS schema_name,
                TABLE_NAME AS table_name,
                COLUMN_NAME AS column_name,
                '"' + DATA_TYPE + '"' AS quoted_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_CATALOG = :database_name
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
        """

        tables = {}
        result = mssql_conection.connect().execute(text(query_string), { 'database_name': database })
        for dbo, table_schema, table_name, column_name, col_type in result.fetchall():
   
            if table_schema not in tables:
                tables[table_schema] = {}

            if table_name not in tables[table_schema]:
                tables[table_schema][table_name] = []

            tables[table_schema][table_name].append({ 'column': column_name, 'type':  col_type  })

        tables['schema_based'] = True
        return tables


    def get_oracle_tables(namespace, connection_name, secret):
        
        oracle_conection, owner = SQLConnection\
                                        .oracle_connect(namespace, connection_name, secret)
        
        query_string = """
            SELECT 
                TABLE_NAME AS table_name,
                COLUMN_NAME AS column_name,
                '"' || DATA_TYPE || '"' AS quoted_type
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :schema
            ORDER BY TABLE_NAME, COLUMN_ID
        """

        tables = {}
        result = oracle_conection.connect().execute(text(query_string), { 'schema': owner })
        for table_name, column_name, col_type in result.fetchall():
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append({ 'column': column_name, 'type':  col_type  })

        return tables


    def get_tables_list(namespace, connection_name):

        try:
            
            path = f'main/db/{connection_name}'
            secret = SQLDatabase.secret_manager.get_secret(namespace,path=path)

            table_list = None
            dbengine = secret['dbengine']

            if(dbengine == 'mysql'):
                table_list = SQLDatabase.get_mysql_tables(namespace, connection_name, secret)

            if(dbengine == 'postgresql'):
                table_list = SQLDatabase.get_pgsql_tables(namespace, connection_name, secret)

            if(dbengine == 'mssql'):
                table_list = SQLDatabase.get_mssql_tables(namespace, connection_name, secret)

            if(dbengine == 'oracle'):
                table_list = SQLDatabase.get_oracle_tables(namespace, connection_name, secret)
            
            return { 'tables': table_list, 'details': secret }
        
        except Exception as err:
            print(f'Error while loading table from {connection_name}')
            print(err)
            traceback.print_exc()
            return { 'error': True, 'message': str(err) }


    def get_fields_from_table(namespace, dbengine, connection_name, table_name: str, metadata = None):

        try:
            fields = []
            db_conection = None
            if(dbengine == 'mysql'):
                db_conection, _ = SQLConnection.mysql_connect(namespace, connection_name, None)

            if(dbengine == 'postgresql'):
                db_conection, _ = SQLConnection.pgsql_connect(namespace, connection_name, None)

            if(dbengine == 'mssql'):
                db_conection, _ = SQLConnection.mssql_connect(namespace, connection_name, None)

            if(dbengine == 'oracle'):
                db_conection, _ = SQLConnection.oracle_connect(namespace, connection_name, None)
            
            if not metadata:

                metadata = MetaData()
                table = {}

                if table_name.__contains__('.'):
                    schema, table = table_name.split('.',1)
                    table = Table(table, metadata, autoload_with=db_conection, schema=schema) 
                else:
                    table = Table(table_name, metadata, autoload_with=db_conection)
                fields = list(table.columns.keys())
            else:
                # If all fields metadata, bellow is the approach to go for
                inspector = reflection.Inspector.from_engine(db_conection)
                fields = inspector.get_columns(table_name)
            
            print(fields)
            return { 'fields': fields }
        
        except Exception as err:
            print(f'Error while fetching fields from {table_name}')
            print(err)
            traceback.print_exc()
            return { 'error': True, 'message': str(err) }


    def db_schemas():
        ...


    @staticmethod
    def test_sql_connection(dbengine, config):
        from utils.database_secret import parse_connection_string
        query_string = parse_connection_string(dbengine, config)

        if dbengine == 'mssql':
            query_string = query_string+f'{SQLConnection.get_mssql_driver()}'

        message, error = '', False
        try:
            with create_engine(query_string).connect():
                error = False
        except SQLAlchemyError as e:
            error = True
            message = f'Error while trying to connect to Database {str(e)}'
            print(message)
        finally:
            return { 'result': f'{message}', 'error': error }


class SQLConnection:

    def mysql_connect(namespace, connection_name, secret = None) -> Engine:

        connection_key = f'{namespace}-{connection_name}'

        if connection_key in SQLDatabase.connections['mysql']:
            return SQLDatabase.connections['mysql'][connection_key]
        
        if secret == None:
            secret = SQLDatabase.secret_manager.get_db_secret(namespace,connection_name)

        connection_string = secret['connection_url']
        connection = create_engine(connection_string)

        database = secret['database']
        SQLDatabase.connections['mysql'][connection_key] = connection, database

        return connection, database


    def pgsql_connect(namespace, connection_name, secret = None) -> Engine:

        connection_key = f'{namespace}-{connection_name}'
        if connection_key in SQLDatabase.connections['postgresql']:
            return SQLDatabase.connections['postgresql'][connection_key]
        
        if secret == None:
            secret = SQLDatabase.secret_manager.get_db_secret(namespace,connection_name)

        connection_string = secret['connection_url']
        database = secret['database']

        postgress_prefix = 'postgresql://'
        psycopa2_driver_prefix = 'postgresql+psycopg2://'

        connection_string = str(connection_string).replace(postgress_prefix,psycopa2_driver_prefix)

        connection = create_engine(connection_string)
        SQLDatabase.connections['postgresql'][connection_key] = connection, database

        return connection, database


    def oracle_connect(namespace, connection_name, secret = None) -> Engine:

        connection_key = f'{namespace}-{connection_name}'
        if connection_key in SQLDatabase.connections['oracle']:
            return SQLDatabase.connections['oracle'][connection_key]
        
        if secret == None:
            secret = SQLDatabase.secret_manager.get_db_secret(namespace,connection_name)

        connection_string = secret['connection_url']

        owner = secret['username']
        connection = create_engine(connection_string)
        SQLDatabase.connections['oracle'][connection_key] = connection, owner

        return connection, owner
    


    def mssql_connect(namespace, connection_name, secret = None) -> Engine:

        connection_key = f'{namespace}-{connection_name}'
        if connection_key in SQLDatabase.connections['mssql']:
            return SQLDatabase.connections['mssql'][connection_key]
        
        if secret == None:
            secret = SQLDatabase.secret_manager.get_db_secret(namespace,connection_name)

        driver = SQLConnection.get_mssql_driver()

        connection_string = secret['connection_url']+driver

        database = secret['database']
        connection = create_engine(connection_string)
        SQLDatabase.connections['mssql'][connection_key] = connection, database

        return connection, database
    

    def get_mssql_driver():
        driver = '?driver=ODBC+Driver+18+for+SQL+Server;Encrypt=yes;TrustServerCertificate=yes'
        if platform.system() != 'Windows':
            driver = '?driver=ODBC%20Driver%2018%20for%20SQL%20Server&Encrypt=yes&TrustServerCertificate=yes'
        return driver


    def get_oracle_dn(hostname, port):
        """
        TODO: Analyse the suitable scenario (e.g. OCI Autonomous DB) that it would bee needed to use DN
        Connects to a TLS/SSL server, retrieves its certificate,
        and returns the full DN string (CN, O, L, ST, C).
        """
        import ssl
        import socket
        context = ssl.create_default_context()

        cert, dn_string = {}, None
        with socket.create_connection((hostname, port)) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                cert = ssock.getpeercert()

        if 'subject' in cert:
            dn_string = 'CN={CN},O={O},L={L},ST={ST},C={C}'
            for item in cert['subject']:
                key, value = item[0]
                if key == 'commonName':
                    dn_string = dn_string.replace('{CN}',value)
                elif key == 'organizationName':
                    dn_string = dn_string.replace('{O}',value)
                elif key == 'localityName':
                    dn_string = dn_string.replace('{L}',value)
                elif key == 'stateOrProvinceName':
                    dn_string = dn_string.replace('{ST}',value)
                elif key == 'countryName':
                    dn_string = dn_string.replace('{C}',value)

        return dn_string


    def parse_oracle_dsn(hostname, port, database):
        dsn_template = """
        (DESCRIPTION=
            (RETRY_COUNT=20)
            (RETRY_DELAY=3)
            (ADDRESS=(PROTOCOL=tcps)(HOST={host})(PORT={port}))
            (CONNECT_DATA=(SERVICE_NAME={service_name}))
            (SECURITY=(SSL_SERVER_CERT_DN_MATCH=YES))
        )
        """

        return dsn_template.format(
            host=hostname,
            port=port,
            service_name=f'{database}.{hostname}'
        )
