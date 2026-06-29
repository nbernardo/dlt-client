import io
import os
import uuid
import threading
import duckdb
import pyarrow.parquet as pq
from flask import Blueprint, request, jsonify, send_file, after_this_request
from concurrent.futures import ThreadPoolExecutor
from utils.duckdb_util import DuckdbUtil
import platform
from controller.pipeline import BasePipeline
from controller.user_management import require_permission
from services.user_management.UserService import UserService
import asyncio

duckdb_bridge = Blueprint('duckdb_bridge', __name__)

EXPORT_DIR = "/tmp/parquet_exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

_db_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=5)


def _get_connection(namespace, dwname) -> duckdb.DuckDBPyConnection:
    sep = '/' if platform.system() != 'Windows' else '\\\\'
    db_path = f'{BasePipeline.folder}{sep}duckdb{sep}{namespace}{sep}{dwname}.duckdb'
    return DuckdbUtil.get_connection_for(db_path)
    

def _validate_select(sql: str) -> str:
    sql_stripped = sql.strip().rstrip(";")
    if not sql_stripped.lower().startswith("select"):
        raise ValueError("Only SELECT queries are allowed")
    return sql_stripped


def _run_in_memory(con: duckdb.DuckDBPyConnection, sql: str) -> io.BytesIO:

    with _db_lock:
        arrow_table = con.execute(sql).fetch_arrow_table()
    # Parquet serialization happens outside the lock, other queries can run
    buffer = io.BytesIO()
    pq.write_table(arrow_table, buffer, compression="ZSTD", compression_level=3)
    buffer.seek(0)
    return buffer


def _run_export(con: duckdb.DuckDBPyConnection, sql: str) -> str:

    filename = f"{uuid.uuid4()}.parquet"
    filepath = os.path.join(EXPORT_DIR, filename)
    with _db_lock:
        con.execute(f"COPY ({sql}) TO '{filepath}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    # File is on disk, lock released, other queries can run while we serve the file
    return filepath


def _check_user_permission(permissions, sql_query, dw_name):
    
    async def fetch_data_async():
        field_by_table = UserService.access_level.extract_columns_by_table(sql_query, dw_name)
        result = await UserService.access_level.get_access_tables_constraint(permissions, dw_name, field_by_table)
        return result.get('result'), result.get('total_not_allowed')

    def run_in_isolated_loop():
        return asyncio.run(fetch_data_async())

    future = _executor.submit(run_in_isolated_loop)
    return future.result()


@duckdb_bridge.route("/query-parquet/<namespace>/<dw>", methods=["POST"])
@require_permission('query:dw')
def query_parquet(namespace = None, dw = None):
    """In-memory parquet response — best for small/aggregated result sets."""
    body = request.get_json() or {}
    sql = body.get("sql", "").strip()
    if not sql:
        return jsonify({"error": "Missing 'sql' in request body"}), 400

    try:
        sql, con = _validate_select(sql), _get_connection(namespace, dw)

        future = _executor.submit(_run_in_memory, con, sql)
        buffer = future.result()

        return send_file(buffer, mimetype="application/octet-stream", as_attachment=True, download_name="result.parquet")

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        print(f"DATABASE ERROR CRASH: {str(e)}")
        return jsonify({"error": str(e)}), 500


@duckdb_bridge.route("/query-parquet-export/<namespace>/<dw>", methods=["POST"])
@require_permission('query:dw')
def query_parquet_export(namespace = None, dw = None):
    """Disk-based parquet export — handles larger result sets with lower RAM usage."""
    
    body = request.get_json() or {}
    sql = body.get("sql", "").strip()
    if not sql:
        return jsonify({"error": "Missing 'sql' in request body"}), 400
    if str(sql).__contains__('*'):
        return jsonify({"error": "Invalid query. '*' is not allowed, please specify the column names"}), 400

    result, not_allowed_access = _check_user_permission(request.permissions, sql, dw)

    if(not_allowed_access > 0):
        return jsonify({"error": result}), 200

    flpath, mmtype = None, "application/octet-stream"
    try:
        sql, con = _validate_select(sql), _get_connection(namespace, dw)

        future = _executor.submit(_run_export, con, sql)
        flpath = future.result()

        @after_this_request
        def cleanup(response):
            if flpath and os.path.exists(flpath):
                os.remove(flpath)
            return response
        
        return send_file(flpath, mimetype=mmtype, as_attachment=True, download_name="result.parquet")

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        print(f"DATABASE ERROR CRASH: {str(e)}")
        if flpath and os.path.exists(flpath):
            os.remove(flpath)
        return jsonify({"error": str(e)}), 500


def cleanup_export_dir():
    for f in os.listdir(EXPORT_DIR):
        try:
            os.remove(os.path.join(EXPORT_DIR, f))
        except Exception:
            pass