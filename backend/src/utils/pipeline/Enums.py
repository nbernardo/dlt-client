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


class Checkpoint(StrEnum):
    INIT = 'INIT'
    DONE = 'DONE'
    DELAY = 'DELAYED'
    FAILED = 'FAILED'
    TABLE_COMMIT = 'COMMIT'
    TAKING_CONTROL = 'BASTION_PASSED'
    # Mean the data have been put in stage storage
    STAGED = 'STAGED'

    TIME_UNSET = 'UNSET'
    # Means that the pipeline run status was update from bastion, but no real time was set yeat
    TIME_FROM_BASTION = 'FROM_BASTION'


class FileOperation(StrEnum):
    REPLACE = 'R'


class Trigger(StrEnum):
    STATUS_ACTIVE = 'ACTIVE'