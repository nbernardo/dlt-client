
import oracledb
from oracledb import ConnectionPool
from services.workspace.supper.SecretManagerType import SecretManagerType

class OracleDBUtil:

    connection_pool: ConnectionPool = None
    secret_manager: SecretManagerType

    def create_pool(
        user,
        password,
        dsn,
        min_conn=1,
        max_conn=5,
        increment=1
    ) -> ConnectionPool:
        pool = oracledb.create_pool(
            user=user,
            password=password,
            dsn=dsn,
            min=min_conn,
            max=max_conn,
            increment=increment,
            homogeneous=True,
            timeout=60,
            ping_interval=60,
        )

        return pool


    def get_pool(namespace,connection_name) -> ConnectionPool:

        if OracleDBUtil.connection_pool == None:

            secret = OracleDBUtil.secret_manager.get_db_secret(namespace,connection_name)
            dsn = secret['dbConnectionParams']
            user = secret['username']
            password = secret['password']

            OracleDBUtil.connection_pool = OracleDBUtil.create_pool(user=user,password=password,dsn=dsn)

        return OracleDBUtil.connection_pool


    def create_table_if_not_exists(table_name: str, columns: dict):
        """
        columns example:
        {
            "id": "NUMBER PRIMARY KEY",
            "name": "VARCHAR2(200)",
            "value": "NUMBER"
        }
        """

        conn = OracleDBUtil.connection_pool.acquire()
        cur = conn.cursor()

        tname = table_name.upper()

        cur.execute("""SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = :t""", [tname])
        table_exists = cur.fetchone()[0]

        if table_exists == 0:
            col_def = ", ".join(
                f'"{col.upper()}" {dtype}'
                for col, dtype in columns.items()
            )

            sql = f'CREATE TABLE "{tname}" ({col_def})'
            cur.execute(sql)
            conn.commit()

        cur.close()
        OracleDBUtil.connection_pool.release(conn)