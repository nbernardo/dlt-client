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
    # DWH -> Same as DONE Indicate the was stored into the Datawarehouse sourced from Stage area
    DONE_DWH = 'COMPLETED_DWH'
    DELAY = 'DELAYED'
    FAILED = 'FAILED'
    FAILED_INGEST_DW = 'FAILED'
    TABLE_COMMIT = 'COMMIT'
    TAKING_CONTROL = 'BASTION_PASSED'
    # STAGED -> Mean the data have been put in stage storage
    STAGED = 'STAGED'
    MANUAL = 'MANUAL_RUN'
    ARCHIVE = 'ARCHIVED'

    TIME_UNSET = 'UNSET'
    # TIME_FROM_BASTION -> Means that the pipeline run status was update from bastion, but no real time was set yeat
    TIME_FROM_BASTION = 'FROM_BASTION'
    
    PROCESS = 'PROCESSED'
    RESOLVE = 'RESOLVED'


class FileOperation(StrEnum):
    REPLACE = 'R'


class Trigger(StrEnum):
    STATUS_ACTIVE = 'ACTIVE'
    STATUS_PAUSE = 'PAUSED'
    STATUS_STOP = 'STOPED'