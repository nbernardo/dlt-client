"""
SQL Query Utility
Handles SQL query execution across multiple destination types using Polars and connectorx
"""
import polars as pl
import connectorx as cx
from sqlalchemy import create_engine, text
from services.workspace.SecretManager import SecretManager
import traceback
import asyncio
from concurrent.futures import ThreadPoolExecutor
import re
from utils.pipeline.Enums import DestinationType

class PolarsQueryUtil:
    """
    Utility class for executing SQL queries using Polars and connectorx across different database types
    """

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
            dest_type: Type of destination ('sql', 'duckdb', 's3', 'bigquery', 'databricks')
            
        Returns:
            dict: {'result': list of tuples, 'fields': comma-separated field names}
            or {'error': True, 'result': error message, 'code': error code}
        """
        dest_type = destination_details.get('dest_type')
        try:
            if dest_type == 'duckdb':
                return PolarsQueryUtil._query_duckdb(query, connection_name)
            elif dest_type == 'sql':
                return PolarsQueryUtil._query_sql_database(query, namespace, connection_name)
            elif dest_type == 's3':
                return PolarsQueryUtil._query_s3(query, namespace, connection_name)
            elif dest_type in DestinationType._value2member_map_:
                return PolarsQueryUtil._query_cloud_warehouse(query, namespace, connection_name, destination_details)
            else:
                return {
                    'error': True,
                    'result': f'Unsupported destination type: {dest_type}',
                    'code': 'err'
                }
                
        except Exception as err:
            print(f'Error executing query: {query}')
            print(traceback.format_exc())
            return {
                'error': True,
                'result': str(err),
                'code': 'err'
            }

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
        """Query SQL databases (MySQL, PostgreSQL, Oracle, SQL Server, MariaDB) using Polars and connectorx"""
        try:
            # Get connection credentials from secret manager
            secret = SecretManager.get_db_secret(namespace, connection_name, from_pipeline=True)
            connection_url = secret['connection_url']
                        
            # Detect database engine from connection URL
            db_engine = PolarsQueryUtil._detect_database_engine(connection_url)
            
            # Convert SQLAlchemy URL to connectorx format
            # SQLAlchemy: mysql+pymysql://user:pass@host:port/db
            # Connectorx: mysql://user:pass@host:port/db
            cx_url = PolarsQueryUtil._convert_to_connectorx_url(connection_url)
            engine = create_engine(cx_url).connect()
            
            # Use Polars with connectorx to execute query
            df = pl.read_database(query, connection=engine)
            
            # Convert DataFrame to list of tuples for JSON serialization
            result = [tuple(row) for row in df.iter_rows()]
            fields = ','.join(df.columns)
                
            print(f'Query successful using Polars, returned {len(result)} rows')
            return {'result': result, 'fields': fields, 'db_engine': db_engine}
            
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
            
            print(f'Querying S3 with connection: {connection_name}')
            
            # Get S3 credentials from Vault
            secret = SecretManager.get_secret(namespace, connection_name, secret_group=True, from_pipeline=True)
            
            access_key_id = secret['access_key_id']
            secret_access_key = secret['secret_access_key']
            bucket_name = secret['bucket_name']
            region = secret.get('region', 'us-east-1')
            
            # Create DuckDB connection
            con = duckdb.connect()
            
            # Install and load httpfs extension for S3 support
            con.execute("INSTALL httpfs;")
            con.execute("LOAD httpfs;")
            
            # Configure S3 credentials
            con.execute(f"SET s3_region='{region}';")
            con.execute(f"SET s3_access_key_id='{access_key_id}';")
            con.execute(f"SET s3_secret_access_key='{secret_access_key}';")
            
            print(f'Executing S3 query: {query}')
            
            # Check if query already contains s3:// path
            if 's3://' not in query.lower():
                # Extract table/file name from query
                # Query format: SELECT * FROM filename.csv
                from_match = re.search(r'from\s+([^\s,;]+)', query, re.IGNORECASE)
                if from_match:
                    table_name = from_match.group(1).strip('`"\'')
                    # Construct S3 path
                    s3_path = f"'s3://{bucket_name}/{table_name}'"
                    # Replace table name with S3 path in query
                    query = re.sub(
                        r'from\s+([^\s,;]+)',
                        f'FROM {s3_path}',
                        query,
                        flags=re.IGNORECASE
                    )
                    print(f'Converted query to: {query}')
            
            # Execute the query
            result = con.execute(query).fetchall()
            
            # Get column names
            fields_list = [desc[0] for desc in con.description]
            fields = ','.join(fields_list)
            
            print(f'S3 query successful, returned {len(result)} rows')
            
            # Close connection
            con.close()
            
            return {'result': result, 'fields': fields}
            
        except Exception as err:
            error_msg = str(err)
            print(f'Error querying S3: {error_msg}')
            import traceback
            traceback.print_exc()
            return {
                'error': True,
                'result': f'S3 query error: {error_msg}',
                'code': 'err'
            }
    @staticmethod
    def _query_cloud_warehouse(query: str, namespace: str, connection_name: str, destination_details: dict = {}):
        """Query cloud data warehouses (BigQuery, Databricks)"""

        dest_type = destination_details.get('dest_type')
        try:
            if dest_type == DestinationType.BIG_QUERY:
                return PolarsQueryUtil._query_bigquery(query, namespace, connection_name)
            elif dest_type == DestinationType.DATABRICKS:
                return PolarsQueryUtil._query_databricks(query, namespace, connection_name, destination_details)
            else:
                return {
                    'error': True,
                    'result': f'Unsupported cloud warehouse type: {dest_type}',
                    'code': 'err'
                }
        except Exception as err:
            print(f'Error querying {dest_type}: {str(err)}')
            raise
    
    @staticmethod
    def _query_bigquery(query: str, namespace: str, connection_name: str):
        """Query Google BigQuery using the BigQuery client library"""
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
            import json
            import tempfile
            import os
            
            print(f'Querying BigQuery with connection: {connection_name}')
            
            # Get BigQuery credentials from Vault
            secret = SecretManager.get_db_secret(namespace, connection_name, from_pipeline=True)
            
            # BigQuery credentials can be stored in different formats
            # Format 1: credentials_json (service account JSON as dict)
            # Format 2: credentials_path (path to service account JSON file)
            # Format 3: credentials_base64 (base64 encoded service account JSON)
            
            credentials = None
            project_id = secret.get('project_id')
            
            if 'credentials_json' in secret:
                # Service account JSON as dict
                creds_info = secret['credentials_json']
                if isinstance(creds_info, str):
                    creds_info = json.loads(creds_info)
                credentials = service_account.Credentials.from_service_account_info(creds_info)
                if not project_id:
                    project_id = creds_info.get('project_id')
                    
            elif 'credentials_path' in secret:
                # Path to service account JSON file
                credentials = service_account.Credentials.from_service_account_file(secret['credentials_path'])
                
            elif 'credentials_base64' in secret:
                # Base64 encoded service account JSON
                import base64
                creds_json = base64.b64decode(secret['credentials_base64']).decode('utf-8')
                creds_info = json.loads(creds_json)
                credentials = service_account.Credentials.from_service_account_info(creds_info)
                if not project_id:
                    project_id = creds_info.get('project_id')
            else:
                # Fallback: try to use application default credentials
                print('No explicit credentials found, using application default credentials')
                client = bigquery.Client(project=project_id)
                
            if credentials:
                client = bigquery.Client(credentials=credentials, project=project_id)
            
            print(f'Executing BigQuery query on project: {project_id}')
            
            # Execute the query
            query_job = client.query(query)
            results = query_job.result()  # Wait for query to complete
            
            # Convert results to list of tuples
            result_list = []
            fields_list = [field.name for field in results.schema]
            
            for row in results:
                result_list.append(tuple(row.values()))
            
            fields = ','.join(fields_list)
            
            print(f'BigQuery query successful, returned {len(result_list)} rows')
            return {'result': result_list, 'fields': fields}
            
        except Exception as err:
            error_msg = str(err)
            print(f'Error querying BigQuery: {error_msg}')
            return {
                'error': True,
                'result': f'BigQuery query error: {error_msg}',
                'code': 'err'
            }
    

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
    def _query_databricks(query: str, namespace: str, connection_name: str, destination_details: dict = {}):
        """Query Databricks using the Databricks SQL connector"""
        try:
            from databricks import sql
            __secrets = SecretManager.referencedSecrets(namespace, destination_details.get('referenced_secrets'))
            config = PolarsQueryUtil._parse_config(destination_details.get('destination_config'), __secrets)
            
            print(f'Querying Databricks with connection: {connection_name}')
            
        
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
            
            # Optional: catalog and schema
            catalog = config.get('catalog', 'main')
            schema = destination_details.get('destinationDB')
            
            print(f'Connecting to Databricks: {server_hostname}')
            
            # Connect to Databricks
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token
            )
            
            cursor = connection.cursor()
            
            # Set catalog and schema if provided
            if catalog:
                cursor.execute(f'USE CATALOG {catalog}')
            if schema:
                cursor.execute(f'USE SCHEMA {schema}')
            
            # Execute the query
            cursor.execute(query)
            
            # Fetch results
            results = cursor.fetchall()
            
            # Get column names
            fields_list = [desc[0] for desc in cursor.description] if cursor.description else []
            fields = ','.join(fields_list)
            
            # Convert to list of tuples
            result_list = [tuple(row) for row in results]
            
            cursor.close()
            connection.close()
            
            print(f'Databricks query successful, returned {len(result_list)} rows')
            return {'result': result_list, 'fields': fields}
            
        except Exception as err:
            error_msg = str(err)
            print(f'Error querying Databricks: {error_msg}')
            return {
                'error': True,
                'result': f'Databricks query error: {error_msg}',
                'code': 'err'
            }

    @staticmethod
    def _convert_to_connectorx_url(sqlalchemy_url: str) -> str:
        """
        Convert SQLAlchemy URL format to connectorx format
        
        SQLAlchemy formats:
        - mysql+pymysql://user:pass@host:port/db
        - postgresql+psycopg2://user:pass@host:port/db
        - mssql+pymssql://user:pass@host:port/db
        - oracle+cx_oracle://user:pass@host:port/db
        
        Connectorx formats:
        - mysql://user:pass@host:port/db
        - postgresql://user:pass@host:port/db
        - mssql://user:pass@host:port/db
        - oracle://user:pass@host:port/db
        """
        # Remove the driver part (e.g., +pymysql, +psycopg2, etc.)
        if '+' in sqlalchemy_url:
            # Split on '://' to separate scheme from rest
            parts = sqlalchemy_url.split('://', 1)
            if len(parts) == 2:
                scheme, rest = parts
                # Remove driver from scheme (e.g., mysql+pymysql -> mysql)
                base_scheme = scheme.split('+')[0]
                return f'{base_scheme}://{rest}'
        
        # If no driver specified, return as-is
        return sqlalchemy_url

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

    # ============================================================================
    # ASYNC METADATA METHODS (Non-blocking)
    # ============================================================================
    
    @staticmethod
    async def get_sql_database_metadata_async(namespace: str, connection_name: str, table_names: list):
        """
        Async wrapper for get_sql_database_metadata to prevent blocking
        Runs the blocking I/O operation in a thread pool
        """
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(
                executor,
                PolarsQueryUtil.get_sql_database_metadata,
                namespace,
                connection_name,
                table_names
            )
    
    @staticmethod
    async def get_bigquery_metadata_async(namespace: str, connection_name: str, table_names: list):
        """
        Async wrapper for get_bigquery_metadata to prevent blocking
        Runs the blocking I/O operation in a thread pool
        """
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(
                executor,
                PolarsQueryUtil.get_bigquery_metadata,
                namespace,
                connection_name,
                table_names
            )
    
    @staticmethod
    async def get_databricks_metadata_async(namespace: str, connection_name: str, table_names: list):
        """
        Async wrapper for get_databricks_metadata to prevent blocking
        Runs the blocking I/O operation in a thread pool
        """
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(
                executor,
                PolarsQueryUtil.get_databricks_metadata,
                namespace,
                connection_name,
                table_names
            )

    # ============================================================================
    # SYNC METADATA METHODS (Blocking - wrapped by async methods above)
    # ============================================================================

    @staticmethod
    def get_sql_database_metadata(namespace: str, connection_name: str, table_names: list):
        """
        Query SQL database to get schema, table, and column information
        Returns dict with schema/database names, table names, and column metadata
        
        Args:
            namespace: User namespace
            connection_name: Connection name
            table_names: List of table names from pipeline metadata
            
        Returns:
            dict: {
                'tables': [
                    {
                        'schema': 'schema_name', 
                        'table': 'table_name',
                        'columns': [{'name': 'col1', 'type': 'varchar'}, ...]
                    }, 
                    ...
                ]
            }
        """
        try:
            # Get connection credentials
            secret = SecretManager.get_db_secret(namespace, connection_name, from_pipeline=True)
            connection_url = secret['connection_url']
            
            # Convert to connectorx format
            cx_url = PolarsQueryUtil._convert_to_connectorx_url(connection_url)
            
            # Build table name list for query - also try with dots replaced by underscores
            expanded_table_names = []
            for tname in table_names:
                expanded_table_names.append(tname)
                # If table name has dots, also try with underscores (e.g., doctors_data.csv -> doctors_data_csv)
                if '.' in tname:
                    expanded_table_names.append(tname.replace('.', '_'))
            
            table_list = "', '".join(expanded_table_names)
            
            # Query to get schema, table, and column information
            # This works for MySQL, PostgreSQL, SQL Server, Oracle
            query = f"""
                SELECT 
                    c.table_schema, 
                    c.table_name,
                    c.column_name,
                    c.data_type
                FROM information_schema.columns c
                WHERE c.table_name IN ('{table_list}')
                AND c.table_schema NOT IN ('information_schema', 'pg_catalog', 'mysql', 'sys', 'performance_schema')
                ORDER BY c.table_schema, c.table_name, c.ordinal_position
            """
            
            print(f'Querying database metadata with columns for connection: {connection_name}')
            print(f'Looking for tables: {table_names}')
            print(f'Expanded table list: {expanded_table_names}')
            
            # Use Polars to execute query
            df = pl.read_database_uri(query, cx_url)
            
            # Group results by schema and table
            result_map = {}
            for row in df.iter_rows(named=True):
                # Polars returns lowercase column names
                schema = row.get('table_schema') or row.get('TABLE_SCHEMA')
                table = row.get('table_name') or row.get('TABLE_NAME')
                col_name = row.get('column_name') or row.get('COLUMN_NAME')
                col_type = row.get('data_type') or row.get('DATA_TYPE')
                
                key = f'{schema}.{table}'
                if key not in result_map:
                    result_map[key] = {
                        'schema': schema,
                        'table': table,
                        'columns': []
                    }
                
                result_map[key]['columns'].append({
                    'name': col_name,
                    'type': col_type
                })
            
            result = list(result_map.values())
            
            print(f'Found {len(result)} tables with column metadata in database')
            for item in result:
                print(f"  - {item['schema']}.{item['table']}: {len(item['columns'])} columns")
            
            return {'tables': result, 'error': False}
            
        except Exception as err:
            print(f'Error querying database metadata: {str(err)}')
            import traceback
            traceback.print_exc()
            # Return empty result on error, don't fail the whole pipeline listing
            return {'tables': [], 'error': True, 'message': str(err)}

    @staticmethod
    def extract_fields_from_query(query: str) -> str:
        """
        Extract field names from SELECT query
        Fallback method if SQLAlchemy doesn't provide column names
        """
        try:
            fields = query.lower().split('from')[0].split('select', 1)[1]
            return fields.strip()
        except:
            return ''

    @staticmethod
    def get_bigquery_metadata(namespace: str, connection_name: str, table_names: list):
        """
        Query BigQuery to get dataset and table information
        
        Args:
            namespace: User namespace for secret retrieval
            connection_name: Name of the connection/secret
            table_names: List of table names to query metadata for
            
        Returns:
            dict: {'tables': [{'schema': str, 'table': str, 'columns': [{'name': str, 'type': str}]}]}
        """
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
            import json
            
            print(f'Querying BigQuery metadata for connection: {connection_name}')
            print(f'Looking for tables: {table_names}')
            
            # Get BigQuery credentials
            secret = SecretManager.get_db_secret(namespace, connection_name, from_pipeline=True)
            
            credentials = None
            project_id = secret.get('project_id')
            
            if 'credentials_json' in secret:
                creds_info = secret['credentials_json']
                if isinstance(creds_info, str):
                    creds_info = json.loads(creds_info)
                credentials = service_account.Credentials.from_service_account_info(creds_info)
                if not project_id:
                    project_id = creds_info.get('project_id')
            
            client = bigquery.Client(credentials=credentials, project=project_id) if credentials else bigquery.Client(project=project_id)
            
            # Get default dataset from secret or use first available
            dataset_id = secret.get('dataset')
            
            tables_metadata = []
            
            # Query each table for metadata
            for table_name in table_names:
                try:
                    # Construct full table reference
                    if dataset_id:
                        table_ref = f"{project_id}.{dataset_id}.{table_name}"
                    else:
                        # Try to find the table in any dataset
                        datasets = list(client.list_datasets())
                        table_ref = None
                        for dataset in datasets:
                            try:
                                test_ref = f"{project_id}.{dataset.dataset_id}.{table_name}"
                                client.get_table(test_ref)
                                table_ref = test_ref
                                dataset_id = dataset.dataset_id
                                break
                            except:
                                continue
                        
                        if not table_ref:
                            print(f'Table {table_name} not found in any dataset')
                            continue
                    
                    # Get table schema
                    table = client.get_table(table_ref)
                    
                    columns = []
                    for field in table.schema:
                        columns.append({'name': field.name, 'type': field.field_type})
                    
                    tables_metadata.append({
                        'schema': dataset_id,
                        'table': table_name,
                        'columns': columns
                    })
                    
                    print(f'Found BigQuery table: {dataset_id}.{table_name} with {len(columns)} columns')
                    
                except Exception as table_err:
                    print(f'Error getting metadata for table {table_name}: {str(table_err)}')
                    continue
            
            print(f'Found {len(tables_metadata)} BigQuery tables')
            return {'tables': tables_metadata, 'error': False}
            
        except Exception as err:
            print(f'Error querying BigQuery metadata: {str(err)}')
            import traceback
            traceback.print_exc()
            return {'tables': [], 'error': True, 'message': str(err)}
    
    @staticmethod
    def get_databricks_metadata(namespace: str, connection_name: str, table_names: list):
        """
        Query Databricks to get catalog/schema and table information
        
        Args:
            namespace: User namespace for secret retrieval
            connection_name: Name of the connection/secret
            table_names: List of table names to query metadata for
            
        Returns:
            dict: {'tables': [{'schema': str, 'table': str, 'columns': [{'name': str, 'type': str}]}]}
        """
        try:
            from databricks import sql
            
            print(f'Querying Databricks metadata for connection: {connection_name}')
            print(f'Looking for tables: {table_names}')
            
            # Get Databricks credentials
            secret = SecretManager.get_db_secret(namespace, connection_name, from_pipeline=True)
            
            server_hostname = secret.get('server_hostname')
            http_path = secret.get('http_path')
            access_token = secret.get('access_token')
            catalog = secret.get('catalog', 'main')
            schema = secret.get('schema', 'default')
            
            # Connect to Databricks
            connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token
            )
            
            cursor = connection.cursor()
            
            # Set catalog and schema
            if catalog:
                cursor.execute(f'USE CATALOG {catalog}')
            if schema:
                cursor.execute(f'USE SCHEMA {schema}')
            
            tables_metadata = []
            
            # Query each table for metadata
            for table_name in table_names:
                try:
                    # Get table columns
                    cursor.execute(f'DESCRIBE TABLE {table_name}')
                    results = cursor.fetchall()
                    
                    columns = []
                    for row in results:
                        col_name = row[0]
                        col_type = row[1]
                        # Skip partition info and other metadata rows
                        if col_name and not col_name.startswith('#'):
                            columns.append({'name': col_name, 'type': col_type})
                    
                    tables_metadata.append({
                        'schema': schema,
                        'table': table_name,
                        'columns': columns
                    })
                    
                    print(f'Found Databricks table: {schema}.{table_name} with {len(columns)} columns')
                    
                except Exception as table_err:
                    print(f'Error getting metadata for table {table_name}: {str(table_err)}')
                    continue
            
            cursor.close()
            connection.close()
            
            print(f'Found {len(tables_metadata)} Databricks tables')
            return {'tables': tables_metadata, 'error': False}
            
        except Exception as err:
            print(f'Error querying Databricks metadata: {str(err)}')
            import traceback
            traceback.print_exc()
            return {'tables': [], 'error': True, 'message': str(err)}
