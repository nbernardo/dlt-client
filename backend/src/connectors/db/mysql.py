import mysql.connector
from os import getenv as env
from mysql.connector import MySQLConnection

db_connection = None

def get_mysql_connection(dbname) -> MySQLConnection:
    #This connection in principle is only for DB and tables validatios
    return mysql.connector.connect(
        host=env('MYSQLDBSRV'),
        user=env('MYSQLDBUSR'),
        passwd=env('MYSQLDBPWD'),
        database=dbname
    )