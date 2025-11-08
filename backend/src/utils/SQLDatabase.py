import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, Engine, Table
from sqlalchemy.engine import reflection
from services.workspace.supper.SecretManagerType import SecretManagerType
import traceback

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
        mysql_conection = SQLConnection\
                    .mysql_connect(namespace, connection_name, secret)
        return inspect(mysql_conection).get_table_names()


    def get_pgsql_tables(namespace, connection_name, secret):
        
        mysql_conection = SQLConnection\
                    .pgsql_connect(namespace, connection_name, secret)
        schemas = inspect(mysql_conection).get_schema_names()

        inspector = inspect(mysql_conection)

        tables_per_schema = { schma_name: inspector.get_table_names(schema=schma_name) for schma_name in schemas if schma_name != 'information_schema' }
        tables_per_schema['schema_based'] = True

        return tables_per_schema


    def get_tables_list(namespace, connection_name):

        try:
            
            path = f'main/db/{connection_name}'
            secret = SQLDatabase.secret_manager.get_secret(namespace,key=None,path=path)

            table_list = None
            dbengine = secret['dbengine']

            if(dbengine == 'mysql'):
                table_list = SQLDatabase.get_mysql_tables(namespace, connection_name, secret)

            if(dbengine == 'postgresql'):
                table_list = SQLDatabase.get_pgsql_tables(namespace, connection_name, secret)
            
            return { 'tables': table_list, 'details': secret }
        
        except Exception as err:
            print(f'Error while loading table from {connection_name}')
            print(err)
            traceback.print_exc()
            return { 'error': True, 'message': str(err) }


    def get_fields_from_table(namespace, connection_name, table_name: str, metadata = None):

        try:
            fields = []
            mysql_conection = SQLConnection\
                                .mysql_connect(namespace, connection_name, None)
            
            if not metadata:

                metadata = MetaData()
                table = {}

                if table_name.__contains__('.'):
                    schema, table = table_name.split('.',1)
                    table = Table(table, metadata, autoload_with=mysql_conection, schema=schema) 
                else:
                    table = Table(table_name, metadata, autoload_with=mysql_conection)
                fields = list(table.columns.keys())
            else:
                # If all fields metadata, bellow is the approach to go for
                inspector = reflection.Inspector.from_engine(mysql_conection)
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


class SQLConnection:

    def mysql_connect(namespace, connection_name, secret = None) -> Engine:

        connection_key = f'{namespace}-{connection_name}'

        if connection_key in SQLDatabase.connections['mysql']:
            return SQLDatabase.connections['mysql'][connection_key]
        
        if secret == None:
            secret = SQLDatabase.secret_manager.get_db_secret(namespace,connection_name)

        connection_string = secret['connection_url']
        connection = create_engine(connection_string)

        SQLDatabase.connections['mysql'][connection_key] = connection

        return connection


    def pgsql_connect(namespace, connection_name, secret = None) -> Engine:

        connection_key = f'{namespace}-{connection_name}'
        if connection_key in SQLDatabase.connections['postgresql']:
            return SQLDatabase.connections['postgresql'][connection_key]
        
        if secret == None:
            secret = SQLDatabase.secret_manager.get_db_secret(namespace,connection_name)

        connection_string = secret['connection_url']

        postgress_prefix = 'postgresql://'
        psycopa2_driver_prefix = 'postgresql+psycopg2://'

        connection_string = str(connection_string).replace(postgress_prefix,psycopa2_driver_prefix)

        connection = create_engine(connection_string)
        SQLDatabase.connections['postgresql'][connection_key] = connection

        return connection
