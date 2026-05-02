""" SQL Query Utility Handles SQL query execution across multiple destination """
import polars as pl
from sqlalchemy import create_engine, text
from services.workspace.SecretManager import SecretManager
import traceback
import re
from utils.pipeline.Enums import DestinationType, ProviderURL
from flask import jsonify
import base64


def serialize_value(val):
    if isinstance(val, memoryview):
        return base64.b64encode(val.tobytes()).decode('utf-8')
    return val


class DestinationQueryUtil:
    """ Utility class for executing SQL queries using Polars and connectorx across different database types """

    @staticmethod
    def execute_query(
        query: str, namespace: str, 
        connection_name: str, destination_details = {}):
        """
        Execute a SQL query using Polars and connectorx
        
        Args:
            query: SQL query string
            namespace: User namespace for secret retrieval
            connection_name: Name of the connection/secret
            
        Returns:
            dict: {'result': list of tuples, 'fields': comma-separated field names}
            or {'error': True, 'result': error message, 'code': error code}
        """
        dest_type = destination_details.get('dest_type')
        try:
            if dest_type == 'duckdb':
                return DestinationQueryUtil._query_duckdb(query, connection_name)
            elif dest_type == 'sql':
                return DestinationQueryUtil._query_sql_database(query, namespace, connection_name)
            elif dest_type == 's3':
                return DestinationQueryUtil._query_s3(query, namespace, connection_name)
            elif dest_type in DestinationType._value2member_map_:
                return DestinationQueryUtil._query_cloud_warehouse(query, namespace, destination_details)
            else:
                return { 'error': True, 'result': f'Unsupported destination type: {dest_type}', 'code': 'err' }
                
        except Exception as err:
            print(f'Error executing query: {query}')
            print(traceback.format_exc())
            return { 'error': True, 'result': str(err), 'code': 'err' }


    @staticmethod
    def _query_duckdb(query: str, database_path: str):
        """Query DuckDB database using SQLAlchemy"""
        try:
            # For DuckDB, use SQLAlchemy with duckdb-engine
            connection_uri = f'duckdb:///{database_path}'
            engine = create_engine(connection_uri)
            
            with engine.connect() as conn:
                result_proxy = conn.execute(text(query))
                result = [tuple(row) for row in result_proxy.fetchall()]
                fields = ','.join(result_proxy.keys())
            
            return {'result': result, 'fields': fields}
            
        except Exception as err:
            print(f'Error querying DuckDB: {str(err)}')
            raise


    @staticmethod
    def _query_sql_database(query: str, namespace: str, connection_name: str):
        """Query SQL databases (MySQL, PostgreSQL, Oracle, SQL Server, MariaDB) using Polars"""
        try:
            # Get connection credentials from secret manager
            secret = SecretManager.get_db_secret(namespace, connection_name, from_pipeline=True)
            connection_url = secret['connection_url']
                        
            db_engine = DestinationQueryUtil._detect_database_engine(connection_url)
            engine = create_engine(connection_url).connect()
            
            df = pl.read_database(query, connection=engine)
            
            result = [tuple(row) for row in df.iter_rows()]
            fields = ','.join(df.columns)
                
            print(f'Query successful using Polars, returned {len(result)} rows')
            return {'result': result, 'fields': fields, 'db_engine': db_engine}
            
        except Exception as err:
            print(f'Error querying SQL database: {str(err)}')
            print(f'Connection: {connection_name}, Namespace: {namespace}')
            raise


    @staticmethod
    def query_sql_database(query: str, namespace: str, connection_name: str):
        """Query SQL databases (MySQL, PostgreSQL, Oracle, SQL Server, MariaDB) using Polars"""
        try:
            # Get connection credentials from secret manager
            secret = SecretManager.get_db_secret(namespace, connection_name, from_pipeline=True)
            connection_url = secret['connection_url']
                        
            db_engine = DestinationQueryUtil._detect_database_engine(connection_url)
            engine = create_engine(connection_url)
            
            rows, fields = {}, {}
            with engine.connect() as conn:
                result = conn.execute(text(query))
                fields = list(result.keys())
                rows = [ [serialize_value(v) for v in row] for row in result.fetchall()]

            return { 'result': rows, 'fields': fields, 'db_engine': db_engine }
            
        except Exception as err:
            print(f'Error querying SQL database: {str(err)}')
            print(f'Connection: {connection_name}, Namespace: {namespace}')
            raise


    @staticmethod
    def _query_s3(query: str, namespace: str, connection_name: str):
        """
        Query S3 files using DuckDB with S3 extension
        
        Args:
            query: SQL query (can reference S3 paths or table names)
            namespace: User namespace
            connection_name: S3 connection name
            
        Returns:
            dict: {'result': list of tuples, 'fields': comma-separated field names}
        """
        try:
            import duckdb
            
            # Get S3 credentials from Vault
            secret = SecretManager.get_secret(namespace, connection_name, secret_group=True, from_pipeline=True)
            
            access_key_id = secret['access_key_id']
            secret_access_key = secret['secret_access_key']
            bucket_name = secret['bucket_name']
            region = secret.get('region', 'us-east-1')
            
            con = duckdb.connect()
            
            # Install and load httpfs extension for S3 support
            con.execute("INSTALL httpfs;") # TODO: This is an extension, needs to be installed when make install takes place
            con.execute("LOAD httpfs;")
            
            # Configure S3 credentials
            con.execute(f"SET s3_region='{region}';")
            con.execute(f"SET s3_access_key_id='{access_key_id}';")
            con.execute(f"SET s3_secret_access_key='{secret_access_key}';")
            
            # Check if query already contains s3:// path
            if 's3://' not in query.lower():
                # Extract table/file name from query
                # Query format: SELECT * FROM filename.csv
                from_match = re.search(r'from\s+([^\s,;]+)', query, re.IGNORECASE)
                if from_match:
                    table_name = from_match.group(1).strip('`"\'')
                    s3_path = f"'s3://{bucket_name}/{table_name}'"
                    query = re.sub(r'from\s+([^\s,;]+)', f'FROM {s3_path}', query, flags=re.IGNORECASE)
            
            result = con.execute(query).fetchall()            
            fields_list = [desc[0] for desc in con.description]
            con.close()
            
            print(f'S3 query successful, returned {len(result)} rows')
            return {'result': result, 'fields': ','.join(fields_list)}
            
        except Exception as err:
            error_msg = str(err)
            print(f'Error querying S3: {error_msg}')
            import traceback
            traceback.print_exc()
            return { 'error': True, 'result': f'S3 query error: {error_msg}', 'code': 'err' }
        

    @staticmethod
    def _query_cloud_warehouse(query: str, namespace: str, destination_details: dict = {}):
        """Query cloud data warehouses (BigQuery, Databricks)"""

        dest_type = destination_details.get('dest_type')
        try:
            if dest_type == DestinationType.BIG_QUERY:
                return DestinationQueryUtil._query_bigquery(query, namespace, destination_details)
            elif dest_type == DestinationType.DATABRICKS:
                return DestinationQueryUtil._query_databricks(query, namespace, destination_details)
            else:
                return { 'error': True, 'result': f'Unsupported cloud warehouse type: {dest_type}', 'code': 'err' }
        except Exception as err:
            print(f'Error querying {dest_type}: {str(err)}')
            raise
    

    @staticmethod
    def _query_bigquery(query: str, namespace: str, destination_details: dict):
        """Query Google BigQuery using the BigQuery client library"""
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
            
            __secrets = SecretManager.referencedSecrets(namespace, destination_details.get('referenced_secrets'))
            config = DestinationQueryUtil._parse_config(destination_details.get('destination_config'), __secrets)

            [credentials, project_id] = [None, config.get('project_id')]
            
            credentials = service_account.Credentials.from_service_account_info(
                {
                    'type': 'service_account',
                    'project_id': config['project_id'],
                    'private_key': config['private_key'],
                    'client_email': config['client_email'],
                    'token_uri': ProviderURL.GOOGLE_API_AUTH0_ENDPOINT,
                },
                scopes=[ProviderURL.GOOGLE_BIG_QUERY]
            )
                
            if credentials:
                client = bigquery.Client(credentials=credentials, project=project_id)
            
            # Execute the query
            query_job = client.query(query)
            results = query_job.result()  # Wait for query to complete
            
            # Convert results to list of tuples
            result_list = []
            fields_list = [field.name for field in results.schema]
            
            for row in results:
                result_list.append(tuple(row.values()))
            
            print(f'BigQuery query successful, returned {len(result_list)} rows')
            return {'result': result_list, 'fields': ','.join(fields_list)}
            
        except Exception as err:
            error_msg = str(err)
            print(f'Error querying BigQuery: {error_msg}')
            return { 'error': True, 'result': f'BigQuery query error: {error_msg}', 'code': 'err' }
    

    @staticmethod
    def _parse_config(config_str, __secrets):
        import ast

        secret_mappings = re.findall(r'"(\w+)":\s*__secrets\.(\w+)', config_str)

        # Replace all __secrets.* with placeholder strings, then parse
        cleaned = re.sub(r'__secrets\.(\w+)', r'"__SECRET_\1__"', config_str)
        dict_str = cleaned.split("=", 1)[1].strip()
        config = ast.literal_eval(dict_str)

        for dict_key, secret_key in secret_mappings:
            config[dict_key] = getattr(__secrets, secret_key)
            
        return config


    @staticmethod
    def _query_databricks(query: str, namespace: str, destination_details: dict = {}):
        """Query Databricks using the Databricks SQL connector"""
        try:
            from databricks import sql
            __secrets = SecretManager.referencedSecrets(namespace, destination_details.get('referenced_secrets'))
            config = DestinationQueryUtil._parse_config(destination_details.get('destination_config'), __secrets)
                    
            # Required fields for Databricks connection
            server_hostname = config.get('server_hostname')
            http_path = config.get('http_path')
            access_token = config.get('access_token')
            
            if not all([server_hostname, http_path, access_token]):
                return {
                    'error': True,
                    'result': 'Missing required Databricks credentials (server_hostname, http_path, access_token)',
                    'code': 'err'
                }
                        
            # Connect to Databricks
            connection = sql.connect(server_hostname=server_hostname, http_path=http_path, access_token=access_token)            
            cursor = connection.cursor()
            
            [catalog, schema] = [config.get('catalog', 'main'), destination_details.get('destinationDB')]

            # Set catalog and schema if provided
            if catalog: cursor.execute(f'USE CATALOG {catalog}')
            if schema: cursor.execute(f'USE SCHEMA {schema}')
            
            cursor.execute(query)
            
            results = cursor.fetchall()
            fields_list = [desc[0] for desc in cursor.description] if cursor.description else []
            result_list = [tuple(row) for row in results]
            
            cursor.close()
            connection.close()
            
            return {'result': result_list, 'fields': ','.join(fields_list)}
            
        except Exception as err:
            error_msg = str(err)
            print(f'Error querying Databricks: {error_msg}')
            return { 'error': True, 'result': f'Databricks query error: {error_msg}', 'code': 'err' }
        

    @staticmethod
    def _detect_database_engine(connection_url: str) -> str:
        """
        Detect database engine from connection URL
        
        Returns: 'mysql', 'postgresql', 'mssql', 'oracle', 'mariadb', or 'unknown'
        """
        url_lower = connection_url.lower()
        
        if 'mysql' in url_lower or 'mariadb' in url_lower:
            return 'mysql'
        elif 'postgresql' in url_lower or 'postgres' in url_lower:
            return 'postgresql'
        elif 'mssql' in url_lower or 'sqlserver' in url_lower:
            return 'mssql'
        elif 'oracle' in url_lower:
            return 'oracle'
        else:
            return 'unknown'