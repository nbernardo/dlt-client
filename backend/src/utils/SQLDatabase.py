from sqlalchemy import create_engine, inspect, MetaData, Table, text
from sqlalchemy.engine import reflection
from sqlalchemy.exc import NoInspectionAvailable
from services.workspace.supper.SecretManagerType import SecretManagerType
import traceback
import platform
from utils.SQLServerUtil import column_type_conversion

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
        
        try:

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
        
        except Exception as err:
            return None


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
            dbengine = secret['dbengine'] if 'dbengine' in secret else None

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


    def get_connnection(namespace, dbengine, connection_name):
        db_conection = None
        if(dbengine == 'mysql'):
            db_conection, _ = SQLConnection.mysql_connect(namespace, connection_name, None)

        if(dbengine == 'postgresql'):
            db_conection, _ = SQLConnection.pgsql_connect(namespace, connection_name, None)

        if(dbengine == 'mssql'):
            db_conection, _ = SQLConnection.mssql_connect(namespace, connection_name, None)

        if(dbengine == 'oracle'):
            db_conection, _ = SQLConnection.oracle_connect(namespace, connection_name, None)
        
        return db_conection


    def get_fields_from_table(namespace, dbengine, connection_name, table_name: str, metadata = None):

        try:
            fields = []
            db_conection = SQLDatabase.get_connnection(namespace,dbengine,connection_name)
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
            with create_engine(query_string).connect() as conn:
                if dbengine == 'oracle':
                    conn.execute(text("SELECT 1 FROM DUAL"))
                else:
                    conn.execute(text("SELECT 1"))
                error = False
        except Exception as e:
            error = True
            message = f'Error while trying to connect to Database {str(e)}'
            print(message)
        finally:
            return { 'result': f'{message}', 'error': error }


class SQLConnection:

    def mysql_connect(namespace, connection_name, secret = None):

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


    def pgsql_connect(namespace, connection_name, secret = None):

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


    def oracle_connect(namespace, connection_name, secret = None):

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
    


    def mssql_connect(namespace, connection_name, secret = None):

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


def generate_join_query(target_tables: dict = {}, relationships = {}, schema_metadata = {}, db_name = ''):
    if not target_tables: return ""

    select_parts = []
    for table in target_tables.keys():
        for col in schema_metadata.get(table, []):
            select_parts.append(f"{table}.{col} AS {table}_{col}")
    
    select_clause = "SELECT\n  " + ",\n  ".join(select_parts)
    
    primary_table = list(target_tables.keys())[0]
    query = f"{select_clause}\nFROM {primary_table}"
    joined_tables, seen_rels = {primary_table}, set()
    tables_list = target_tables.keys()

    added = True
    while added:
        added = False
        for table in tables_list:
            if table not in relationships: continue
            for rel in relationships[table]:
                ref_table = rel['referred_table']
                # Only join if it's in our target list and not a self-join
                if ref_table == table or ref_table not in tables_list: continue
                
                rel_key = tuple(sorted([table, ref_table]) + sorted(rel['columns'] + rel['referred_columns']))
                if rel_key in seen_rels: continue

                on_clause = " AND ".join([f"{table}.{l} = {ref_table}.{r}" for l, r in zip(rel['columns'], rel['referred_columns'])])

                if (table in joined_tables and ref_table not in joined_tables) or \
                   (ref_table in joined_tables and table not in joined_tables):
                    join_target = ref_table if table in joined_tables else table
                    query += f"\nFULL OUTER JOIN {join_target} ON {on_clause}"
                    joined_tables.add(join_target)
                    seen_rels.add(rel_key)
                    added = True
    return query


def _normalize_table_names_backward(secrets, tables, primary_keys=None):
    dbengine  = secrets['dbengine']
    actual_tables = tables
    actual_pks    = primary_keys

    if dbengine == 'oracle':
        engine    = create_engine(secrets['connection_url'])
        inspector = inspect(engine)
        available = inspector.get_table_names(schema=secrets['username'])

        if available[0].islower():
            actual_tables = [t.lower() for t in tables]

        if primary_keys:
            actual_pks = [
                [pk.lower() for pk in pks] if isinstance(pks, list) else pks.lower()
                for pks in primary_keys
            ]

    return actual_tables, actual_pks


def normalize_table_names(secrets, tables, primary_keys=None, db_name = {}):
    dbengine = secrets.get('dbengine', 'postgres')
    connection_url = secrets.get('connection_url')
    schema = secrets.get('schema', 'public')
    
    # To keep backword compatibility
    # TODO: Migrate all scenario to the new implementation 
    #       to support big_query generation
    if dbengine != 'postgresql':
        return _normalize_table_names_backward(secrets, tables, primary_keys)

    engine = create_engine(connection_url)
    inspector = inspect(engine)
    available = inspector.get_table_names(schema=schema)

    if dbengine == 'oracle':
        actual_tables = [t.lower() if any(a.islower() for a in available) else t.upper() for t in tables]
    else:
        actual_tables = {
            t.lower().split('.')[1] if t.lower().__contains__('.') else t.lower(): t.lower()   for t in tables
        }

    actual_pks = primary_keys if primary_keys else []
    [relationships, schema_metadata, ddls] = [{}, {}, {}]

    for table in actual_tables.keys():
        if table in available:
            [cols, column_defs] = [inspector.get_columns(table, schema=schema), []]
            schema_metadata[table] = { c['name']: c['type']  for c in cols }

            for c in cols:
                null_str = 'NOT NULL' if not c.get('nullable') else ''
                column_defs.append(f"  {c['name']} {str(c['type'])} {null_str}".strip())

            fks = inspector.get_foreign_keys(table, schema=schema)
            relationships[table] = [
                { 'columns': fk['constrained_columns'], 'referred_table': fk['referred_table'], 'referred_columns': fk['referred_columns']  }
                for fk in fks
            ]
            
            for fk in fks:
                l_cols = ', '.join(fk['constrained_columns'])
                r_table = fk['referred_table']
                r_cols = ', '.join(fk['referred_columns'])
                column_defs.append(f'  FOREIGN KEY ({l_cols}) REFERENCES {r_table} ({r_cols})')

            ddls[table] = f"CREATE TABLE {table} (\n" + ",\n".join(column_defs) + "\n);"

    final_big_query = generate_join_query(actual_tables, relationships, schema_metadata, db_name)

    return tables, actual_pks, relationships, schema_metadata, ddls, final_big_query


def converts_field_type(table, pk):
    columns_config = {}
    
    for col_name, col_info in table.compute_table_schema().get("columns", {}).items():
        if(col_name.lower() == pk.lower()):
            if col_info.get("data_type") == "double":
                columns_config[col_name] = {"data_type": "text"}
                print(f"Converting {table.name}.{col_name}: double → text")
    
    if columns_config:
        table.apply_hints(columns=columns_config)
    
    table.apply_hints(schema_contract={"tables": "evolve", "columns": "evolve"})
    table = table.apply_hints(additional_table_hints={"x-dlt-materialize-schema": True})

    return table
