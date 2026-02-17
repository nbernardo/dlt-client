import json
from datetime import datetime
from utils.duckdb_util import DuckdbUtil
import asyncio

class DuckDBLogStore:
    """Simplified DuckDB store focusing on batch performance with auto-parsing."""
    
    def __init__(self):
        DuckdbUtil.initialize_logging_tables()


    def _get_conn(): return DuckdbUtil.get_log_db_instance()


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
            conn = DuckdbUtil.get_log_db_instance()
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


    def fetch_perspective(self, pipeline_id=None, execution_id=None, level=None, days_back=None):
            """
            The 'Universal Perspective' Query. 
            Allows you to pivot your view by simply passing different arguments.
            """
            conn = DuckdbUtil.get_log_db_instance()
            
            query = "SELECT * FROM pipeline_logs WHERE 1=1"
            params = []

            if pipeline_id:
                query += " AND pipeline_id = ?"
                params.append(pipeline_id)

            if execution_id:
                query += " AND execution_id = ?"
                params.append(execution_id)

            if level:
                query += " AND log_level = ?"
                params.append(level)

            if days_back:
                query += " AND timestamp >= current_date - ?"
                params.append(days_back)

            query += " ORDER BY timestamp DESC"
            
            return conn.execute(query, params).df()
    

    def get_stuck_pipelines(self, timeout_hours=1):
            """
            Perspective: State Change.
            Finds executions that 'started' but never logged 'finished' or 'failed'.
            """
            query = f"""
                SELECT 
                    pipeline_id, 
                    execution_id, 
                    min(timestamp) as started_at,
                    max(timestamp) as last_activity
                FROM pipeline_logs
                GROUP BY 1, 2
                HAVING 
                    COUNT(CASE WHEN message LIKE '%finished%' OR extra_data->>'$.status' = 'finished' THEN 1 END) = 0
                    AND COUNT(CASE WHEN log_level = 'ERROR' THEN 1 END) = 0
                    AND min(timestamp) < now() - INTERVAL '{timeout_hours} hours'
                ORDER BY started_at DESC
            """
            return self._get_conn().execute(query).df()
    

    def get_performance_metrics(self):
        """
        Perspective: Resource Consumption.
        Calculates throughput (rows per second) based on extra_data.
        """
        query = """
            SELECT 
                pipeline_id,
                avg(CAST(extra_data->>'$.duration_sec' AS FLOAT)) as avg_duration,
                sum(CAST(extra_data->>'$.rows' AS INTEGER)) as total_rows,
                (sum(CAST(extra_data->>'$.rows' AS INTEGER)) / 
                 NULLIF(sum(CAST(extra_data->>'$.duration_sec' AS FLOAT)), 0)) as rows_per_sec
            FROM pipeline_logs
            WHERE extra_data->>'$.rows' IS NOT NULL
            GROUP BY 1
            ORDER BY rows_per_sec DESC
        """
        return self._get_conn().execute(query).df()


    def get_execution_timeline(self, execution_id):
        """
        Perspective: Flow & Sequence.
        Shows the 'lag' between every log step in a specific run to find bottlenecks.
        """
        query = """
            SELECT 
                timestamp,
                message,
                module,
                timestamp - LAG(timestamp) OVER (ORDER BY timestamp) as step_duration
            FROM pipeline_logs
            WHERE execution_id = ?
            ORDER BY timestamp ASC
        """
        return self._get_conn().execute(query, [execution_id]).df()


    def get_log_volume_histogram(self, interval='5 minutes', limit_hours=6):
        """
        Perspective: Time-Series / Histogram.
        Identifies 'Log Storms' by binning log counts into time buckets.
        """
        query = f"""
            SELECT 
                time_bucket(INTERVAL '{interval}', timestamp) as time_window,
                log_level,
                count(*) as log_count
            FROM pipeline_logs
            WHERE timestamp > now() - INTERVAL '{limit_hours} hours'
            GROUP BY 1, 2
            ORDER BY 1 DESC
        """
        return self._get_conn().execute(query).df()


    def get_health_pivot(self):
        """
        Perspective: Pivot.
        Returns a side-by-side comparison of log levels per pipeline.
        """
        # PIVOT is a native DuckDB keyword that is extremely fast
        query = """
            PIVOT pipeline_logs 
            ON log_level 
            USING COUNT(*) 
            GROUP BY pipeline_id
        """
        return self._get_conn().execute(query).df()


    def get_error_hotspots(self):
        """
        Perspective: Blast Radius.
        Finds which modules are responsible for the most errors.
        """
        query = """
            SELECT 
                module,
                count(*) as error_count,
                arg_max(message, timestamp) as latest_error_msg
            FROM pipeline_logs
            WHERE log_level IN ('ERROR', 'CRITICAL')
            GROUP BY 1
            ORDER BY error_count DESC
        """
        return self._get_conn().execute(query).df()


    async def get_execution_ids(namespace):

        con = DuckdbUtil.get_log_db_instance()
        cursor = con.cursor()
        query = f"""
            SELECT to_json(list(json_object('name', execution_id))) 
            FROM (
                SELECT DISTINCT execution_id 
                FROM pipeline_logs 
                WHERE namespace = ? 
                AND timestamp >= now() - INTERVAL '30 days'
            )
        """
        
        result = await asyncio.to_thread(
            lambda: cursor.execute(query, [namespace]).fetchone()
        )

        return result[0] if result and result[0] else "[]"


    async def get_logs_by_namespace(namespace, filters = {}):

        where = ''
        params = [namespace]

        if 'pipeline_id' in filters:
            if filters['pipeline_id'] != 'All Pipelines':
                where += " AND pipeline_id = ?"
                params.append(filters['pipeline_id'])

        if 'execution_id' in filters:
            if filters['execution_id'] != 'All Runs':
                where += " AND execution_id = ?"
                params.append(filters['execution_id'])

        if 'level' in filters:
            if filters['level'] != 'All Levels':
                where += " AND log_level = ?"
                params.append(filters['level'])

        if 'days_back' in filters:
            where += " AND timestamp >= current_date - ?"
            params.append(filters['days_back'])

        con = DuckdbUtil.get_log_db_instance()
        cursor = con.cursor()
        query = f"""
            SELECT 
                timestamp,
                id,
                log_level,
                module,
                execution_id,
                line_number,
                message,
                namespace,
                extra_data 
            FROM pipeline_logs
            WHERE 
                namespace = ? AND pipeline_id not in ('system', 'flask_server')
                {where if where != '' else ''}
            AND timestamp >= now() - INTERVAL '30 days'
            ORDER BY timestamp DESC
        """
        
        result = await asyncio.to_thread(
            lambda: cursor.execute(query, params).fetchall()
        )

        return result if result and result[0] else "[]"