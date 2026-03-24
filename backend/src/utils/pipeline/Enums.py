from enum import StrEnum

class SQL_DB(StrEnum):
    MSSQL = 'mssql'
    ORCLE = 'orcale'
    PGSQL = 'postgresql'
    MySQL = 'mysql'


class DestinationType(StrEnum):
    DATABRICKS = 'databricks'
    BIG_QUERY = 'bigquery'


class ProviderURL(StrEnum):
    GOOGLE_BIG_QUERY = 'https://www.googleapis.com/auth/bigquery'
    GOOGLE_API_AUTH0_ENDPOINT = 'https://oauth2.googleapis.com/token'