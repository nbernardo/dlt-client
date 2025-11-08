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

    def get_tables_list(namespace, connection_name):

        try:
            mysql_conection = SQLConnection\
                                .mysql_connect(namespace, connection_name)
            table_list = inspect(mysql_conection).get_table_names()
            
            return { 'tables': table_list }
        
        except Exception as err:
            print(f'Error while loading table from {connection_name}')
            print(err)
            traceback.print_exc()
            return { 'error': True, 'message': str(err) }


    def get_fields_from_table(namespace, connection_name, table_name, metadata = None):

        try:
            fields = []
            mysql_conection = SQLConnection\
                                .mysql_connect(namespace, connection_name)
            
            if not metadata:
                metadata = MetaData()
                table = Table(table_name, metadata, autoload_with=mysql_conection)
                fields = list(table.columns.keys())
            else:
                # If all fields metadata, bellow is the approach to go for
                inspector = reflection.Inspector.from_engine(mysql_conection)
                fields = inspector.get_columns(table_name)
            
            return { 'fields': fields }
        
        except Exception as err:
            print(f'Error while fetching fields from {table_name}')
            print(err)
            traceback.print_exc()
            return { 'error': True, 'message': str(err) }



class SQLConnection:

    def mysql_connect(namespace, connection_name) -> Engine:

        connection_key = f'{namespace}-{connection_name}'

        if connection_key in SQLDatabase.connections['mysql']:
            return SQLDatabase.connections['mysql'][connection_key]

        path = f'main/db/{connection_name}'
        secret = SQLDatabase.secret_manager.get_secret(namespace,key=None,path=path)
        connection_string = secret['connection_url']
        connection = create_engine(connection_string)

        SQLDatabase.connections['mysql'][connection_key] = connection

        return connection
