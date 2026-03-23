from enum import StrEnum

class DestinationType(StrEnum):
    DATABRICKS = 'databricks'
    BIG_QUERY = 'bigquery'