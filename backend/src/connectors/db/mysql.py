from os import getenv as env
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection

db_connection = None

db_config = {
    "host": env('MYSQLDBSRV'),
    "port": 3306,
    "user": env('MYSQLDBUSR'),
    "password": env('MYSQLDBPWD'),
    "database": 'e2eadmin'
}

pool = MySQLConnectionPool(
    pool_name="AdminPool",
    pool_size=30,
    pool_reset_session=True,
    **db_config
)

def get_mysql_connection() -> PooledMySQLConnection:
    #This connection in principle is only for DB and tables validations
    global pool    
    return pool.get_connection()
