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
                to_json(list(row_arr))
            FROM (
                SELECT [
                    timestamp::VARCHAR,
                    id::VARCHAR,
                    log_level,
                    module,
                    execution_id,
                    line_number::VARCHAR,
                    message,
                    namespace,
                    extra_data::VARCHAR, 
                    (bool_or(extra_data->>'$.stage' = 'pipeline_completion') OVER (PARTITION BY execution_id))::VARCHAR
                ] as row_arr
                FROM pipeline_logs
                WHERE 
                    namespace = ? AND pipeline_id not in ('system', 'flask_server')
                    {where if where != '' else ''}
                    AND timestamp >= now() - INTERVAL '30 days'
                ORDER BY timestamp
            )
        """
        
        result = await asyncio.to_thread(
            lambda: cursor.execute(query, params).fetchone()
        )
        
        return json.loads(result[0]) if result and result[0] else []


    async def get_logs_summary(namespace, filters = {}):

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
            SELECT to_json(list(row_arr))
            FROM (
                SELECT [
                    execution_id,
                    -- Use any_value because these are identical within one execution_id
                    any_value(pipeline_id),
                    any_value(namespace),
                    min(timestamp)::VARCHAR,
                    max(timestamp)::VARCHAR,
                    (max(timestamp) - min(timestamp))::VARCHAR,
                    count(*)::VARCHAR,
                    arg_max(CAST(extra_data->>'$.stage' AS VARCHAR), timestamp),
                    count_if(log_level = 'ERROR')::VARCHAR,
                    bool_or(extra_data->>'$.stage' = 'pipeline_completion')::VARCHAR
                ] as row_arr
                FROM pipeline_logs
                WHERE 
                    namespace = ?
                    AND pipeline_id NOT IN ('system', 'flask_server')
                    AND timestamp >= now() - INTERVAL '30 days'
                    {where if where != '' else ''}
                GROUP BY execution_id
                ORDER BY min(timestamp) DESC
            )
        """
        
        result = await asyncio.to_thread(
            lambda: cursor.execute(query, params).fetchone()
        )

        return json.loads(result[0]) if result and result[0] else []


    async def get_run_status_summary(namespace: str):
        def _fetch():

            con = DuckdbUtil.get_log_db_instance()
            query = """
WITH record_counts AS (
    SELECT
        execution_id,
        regexp_extract(message, '^([a-z_]+):[ ]+[0-9]+', 1) as resource_name,
        max(TRY_CAST(
            regexp_extract(message, '^[a-z_]+:[ ]+([0-9]+)', 1)
        AS INTEGER)) as max_records
    FROM pipeline_logs
    WHERE 
        namespace = ?
        AND regexp_matches(message, '^[a-z_]+:[ ]+[0-9]+[ ]+\|')
        AND timestamp >= now() - INTERVAL '7 days'
    GROUP BY execution_id, resource_name
),
record_totals AS (
    SELECT execution_id, sum(max_records)::INT as total_records
    FROM record_counts
    GROUP BY execution_id
),
base_stats AS (
    SELECT 
        lower(regexp_replace(trim(pl.pipeline_id), '[_\s]+', '_', 'g')) as clean_id,
        pl.execution_id,
        CASE 
            WHEN count_if(UPPER(log_level) = 'ERROR') > 0 THEN 'Failed'
            WHEN count_if(UPPER(log_level) = 'WARNING') > 0 THEN 'Warning'
            ELSE 'Success'
        END as final_status,
        max(timestamp) as last_run_time,
        (max(timestamp) - min(timestamp)) as execution_duration,
        COALESCE(rt.total_records, 0) as total_records
    FROM pipeline_logs pl
    LEFT JOIN record_totals rt ON pl.execution_id = rt.execution_id
    WHERE 
        pl.namespace = ?
        AND pl.pipeline_id NOT IN ('system', 'flask_server')
        AND pl.pipeline_id IS NOT NULL
        AND pl.timestamp >= now() - INTERVAL '7 days'
    GROUP BY pl.execution_id, clean_id, rt.total_records
),
trend_stats AS (
    SELECT
        clean_id,
        CAST(last_run_time AS DATE) as run_date,
        count(*)::INT as daily_runs
    FROM base_stats
    GROUP BY clean_id, run_date
),
date_spine AS (
    SELECT CAST(range AS DATE) as run_date
    FROM range(
        CAST(current_date - INTERVAL '6 days' AS TIMESTAMP),
        CAST(current_date + INTERVAL '1 day' AS TIMESTAMP),
        INTERVAL '1 day'
    )
),
trend_arrays AS (
    SELECT
        p.clean_id,
        list(COALESCE(t.daily_runs, 0) ORDER BY d.run_date ASC) as trend_data
    FROM (SELECT DISTINCT clean_id FROM base_stats) p
    CROSS JOIN date_spine d
    LEFT JOIN trend_stats t 
        ON t.clean_id = p.clean_id 
        AND t.run_date = d.run_date
    GROUP BY p.clean_id
),
global_totals AS (
    SELECT 
        'SUMMARY' as report_type,
        'ALL' as pipeline_id,
        count(*)::INT as total_runs,
        count(*) filter (where final_status = 'Success')::INT as success_count,
        count(*) filter (where final_status = 'Failed')::INT as failed_count,
        count(*) filter (where final_status = 'Warning')::INT as warning_count,
        'N/A' as status_indicator,
        []::INT[] as trend_data,
        0 as avg_duration,
        sum(total_records)::INT as total_records
    FROM base_stats
),
pipeline_list AS (
    SELECT 
        'LIST' as report_type,
        b.clean_id as pipeline_id,
        count(*)::INT as total_runs,
        count(*) filter (where final_status = 'Success')::INT as success_count,
        count(*) filter (where final_status = 'Failed')::INT as failed_count,
        count(*) filter (where final_status = 'Warning')::INT as warning_count,
        CASE 
            WHEN count_if(final_status = 'Failed') > 0 THEN 'Failed'
            WHEN count_if(final_status = 'Warning') > 0 THEN 'Warning'
            ELSE 'Success'
        END as status_indicator,
        COALESCE(t.trend_data, []::INT[]) as trend_data,
        round(avg(extract(epoch from b.execution_duration)))::INT as avg_duration,
        sum(b.total_records)::INT as total_records
    FROM base_stats b
    LEFT JOIN trend_arrays t ON b.clean_id = t.clean_id
    GROUP BY b.clean_id, t.trend_data
)
SELECT to_json(list(d)) FROM (
    SELECT * FROM global_totals
    UNION ALL
    SELECT * FROM (SELECT * FROM pipeline_list ORDER BY failed_count DESC, total_runs DESC)
) d;
            """
            
            cursor = con.cursor()
            result = cursor.execute(query, [namespace, namespace]).fetchone()
            return json.loads(result[0]) if result and result[0] else []

        data = await asyncio.to_thread(_fetch)
        
        return {
            "summary": next((item for item in data if item['report_type'] == 'SUMMARY'), {}),
            "pipelines": [item for item in data if item['report_type'] == 'LIST']
        }