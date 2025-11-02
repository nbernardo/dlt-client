from services.workspace.supper.SecretManagerType import SecretManagerType


def create_sql_db_secret(namespace, config, sec_managet: SecretManagerType, db_engind, path):

    if db_engind == 'postgresql':
        connection_url = f'postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}'
    elif db_engind == 'oracle':
        connection_url = f'oracle+cx_oracle://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['sid']}'
    elif db_engind == 'mssql':
        connection_url = f'mssql+pyodbc://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}?driver=ODBC+Driver+17+for+SQL+Server'
    elif db_engind == 'mysql':
        connection_url = f'mysql+pymysql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}'

    sec_managet.vault_instance.secrets.kv.v2.create_or_update_secret(
        path=f'main/db/{path}',
        secret={
            'connection_url': connection_url,
            'host': config['host'],
            'port': int(config['port']),
            'username': config['username'],
            'password': config['password'],
            'database': config['dbname']
        },
        mount_point=namespace
    )







