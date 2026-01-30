import json
from datetime import datetime
from utils.duckdb_util import DuckdbUtil

class DuckDBLogStore:
    """Simplified DuckDB store focusing on batch performance with auto-parsing."""
    
    def __init__(self):
        DuckdbUtil.initialize_logging_tables()

    def store_logs_batch(self, log_records: list):
        """High-performance batch insert into DuckDB."""
        if not log_records:
            return

        # Skip those fields in the extra params from the logger.info
        STANDARD_ATTRS = {
            'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename',
            'module', 'exc_info', 'exc_text', 'stack_info', 'lineno', 'funcName',
            'created', 'msecs', 'relativeCreated', 'thread', 'threadName',
            'processName', 'process', 'message', 'pipeline_id', 'execution_id'
        }
        
        try:
            conn = DuckdbUtil.get_workspace_db_instance()
            query = """
                INSERT INTO pipeline_logs (
                    timestamp, namespace, pipeline_id, execution_id, log_level, logger_name,
                    message, module, extra_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            batch_data = []
            for r in log_records:
                name_parts = r.name.split('.')
                
                nspace = "global"
                p_id = "system"
                e_id = "na"

                # Hierarchy parsing
                if len(name_parts) >= 4 and name_parts[0] == "pipeline":
                    nspace = name_parts[1]
                    p_id = name_parts[2]
                    e_id = name_parts[3]

                # Manual attribute overrides
                p_id = getattr(r, 'pipeline_id', p_id)
                e_id = getattr(r, 'execution_id', e_id)
                nspace = getattr(r, 'namespace', nspace)

                # This pulls everything you added that isn't a standard logging field
                custom_extra = {
                    k: v for k, v in r.__dict__.items() 
                    if k not in STANDARD_ATTRS
                }

                batch_data.append((
                    datetime.fromtimestamp(r.created),
                    nspace,
                    p_id,
                    e_id,
                    r.levelname,
                    r.name,
                    r.getMessage(),
                    r.module,
                    json.dumps(custom_extra, default=str)
                ))
            
            conn.executemany(query, batch_data)
        except Exception as e:
            print(f"DuckDB Batch Write Failed: {e}")