from services.workspace.supper.SecretManagerType import SecretManagerType


def create_sql_db_secret(namespace, config, sec_managet: SecretManagerType, dbengine, path):

    connection_url = parse_connection_string(dbengine, config)

    sec_managet.vault_instance.secrets.kv.v2.create_or_update_secret(
        path=f'main/db/{path}',
        secret={
            'connection_url': connection_url,
            'host': config['host'],
            'port': int(config['port']),
            'username': config['username'],
            'password': config['password'],
            'database': config['dbname'],
            'dbengine': dbengine,
            'dbConnectionParams': config['dbConnectionParams']
        },
        mount_point=namespace
    )


def parse_connection_string(dbengine, config):

    query_string, oracle_dsn_descrptr = '', False
    connection_url = None
    if(len(config['dbConnectionParams']) > 0):
        if dbengine == 'oracle':
            query_string = f'?{config['dbConnectionParams']}'
        
        # Edge case for OCI when using DSN Descriptor
        if dbengine == 'oracle':
            dsn_descrptr_str = 'DESCRIPTION=(RETRY_COUNT=20)(RETRY_DELAY=3)(ADDRESS=(PROTOCOL=tcps)'
            oracle_dsn_descrptr = True if config['dbConnectionParams'].__contains__(dsn_descrptr_str) else False

    if dbengine == 'postgresql':
        connection_url = f'postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}{query_string}'
    
    elif dbengine == 'oracle':
        if oracle_dsn_descrptr:
            connection_url = f'oracle+oracledb://{config['username']}:{config['password']}@?dsn={config['dbConnectionParams']}'
        else:
            connection_url = f'oracle+oracledb://{config['username']}:{config['password']}@{config['host']}:{config['port']}/?service_name={config.get('dbname', 'ORCL')}'

    elif dbengine == 'mssql':
        connection_url = f'mssql+pyodbc://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}{query_string}'
    
    elif dbengine == 'mysql':
        connection_url = f'mysql+pymysql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}{query_string}'
    
    return connection_url








