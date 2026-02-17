from utils.duckdb_util import DuckdbUtil
from utils.logging.log_storage import DuckDBLogStore

class LoggingService:
    """
    Centralized service for managing all logging operations.
    Handles log storage, retrieval, and pipeline-specific logging.
    """
    
    def __init__(self):
        ...

    @staticmethod
    async def get_execution_ids(namespace):
        return await DuckDBLogStore.get_execution_ids(namespace)
    
    @staticmethod
    async def get_logs_by_namespace(namespace, filters = {}):
        return await DuckDBLogStore.get_logs_by_namespace(namespace, filters)
