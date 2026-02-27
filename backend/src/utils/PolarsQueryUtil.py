"""
SQL Query Utility
Handles SQL query execution across multiple destination types using Polars and connectorx
"""
import polars as pl
import connectorx as cx
from sqlalchemy import create_engine, text
from services.workspace.SecretManager import SecretManager
import traceback


class PolarsQueryUtil:
    """
    Utility class for executing SQL queries using Polars and connectorx across different database types
    """

    @staticmethod
    def execute_query(query: str, namespace: str, connection_name: str, dest_type: str = 'sql'):
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
        try:
            if dest_type == 'duckdb':
                return PolarsQueryUtil._query_duckdb(query, connection_name)
            elif dest_type == 'sql':
                return PolarsQueryUtil._query_sql_database(query, namespace, connection_name)
            elif dest_type == 's3':
                return PolarsQueryUtil._query_s3(query, namespace, connection_name)
            elif dest_type in ['bigquery', 'databricks']:
                return PolarsQueryUtil._query_cloud_warehouse(query, namespace, connection_name, dest_type)
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
            
            print(f'Querying SQL database with connection: {connection_name}')
            print(f'Connection URL (SQLAlchemy format): {connection_url}')
            
            # Convert SQLAlchemy URL to connectorx format
            # SQLAlchemy: mysql+pymysql://user:pass@host:port/db
            # Connectorx: mysql://user:pass@host:port/db
            cx_url = PolarsQueryUtil._convert_to_connectorx_url(connection_url)
            print(f'Connection URL (connectorx format): {cx_url}')
            
            # Use Polars with connectorx to execute query
            df = pl.read_database_uri(query, cx_url)
            
            # Convert DataFrame to list of tuples for JSON serialization
            result = [tuple(row) for row in df.iter_rows()]
            fields = ','.join(df.columns)
                
            print(f'Query successful using Polars, returned {len(result)} rows')
            return {'result': result, 'fields': fields}
            
        except Exception as err:
            print(f'Error querying SQL database: {str(err)}')
            print(f'Connection: {connection_name}, Namespace: {namespace}')
            raise

    @staticmethod
    def _query_s3(query: str, namespace: str, connection_name: str):
        """Query S3 files (Parquet/CSV) - Not yet implemented"""
        try:
            # For S3, we need to parse the query to get the file path
            # This is a simplified implementation - may need enhancement
            
            # Get S3 credentials
            secret = SecretManager.get_db_secret(namespace, connection_name, from_pipeline=True)
            
            # Extract table name from query (simplified)
            # Assuming query like: SELECT * FROM table_name
            table_name = query.lower().split('from')[1].split()[0].strip()
            
            # TODO: Map table name to S3 file path
            # For now, return error indicating S3 queries need special handling
            return {
                'error': True,
                'result': 'S3 queries require file path mapping - not yet implemented',
                'code': 'err'
            }
            
        except Exception as err:
            print(f'Error querying S3: {str(err)}')
            raise

    @staticmethod
    def _query_cloud_warehouse(query: str, namespace: str, connection_name: str, dest_type: str):
        """Query cloud data warehouses (BigQuery, Databricks) using Polars and connectorx"""
        try:
            # Get connection credentials
            secret = SecretManager.get_db_secret(namespace, connection_name, from_pipeline=True)
            connection_url = secret['connection_url']
            
            print(f'Querying {dest_type} with connection: {connection_name}')
            
            # Convert SQLAlchemy URL to connectorx format
            cx_url = PolarsQueryUtil._convert_to_connectorx_url(connection_url)
            
            # Use Polars with connectorx to execute query
            df = pl.read_database_uri(query, cx_url)
            
            # Convert DataFrame to list of tuples
            result = [tuple(row) for row in df.iter_rows()]
            fields = ','.join(df.columns)
            
            print(f'Query successful using Polars, returned {len(result)} rows')
            return {'result': result, 'fields': fields}
            
        except Exception as err:
            print(f'Error querying {dest_type}: {str(err)}')
            raise

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
