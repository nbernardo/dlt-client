import threading
import lancedb
from utils.duckdb_util import DuckdbUtil


class LanceConnectionFactory:

    _conn = None
    lance_path = None

    @staticmethod
    def get(dbs_path = None, storage_options=None):

        lance_path = \
            f'{dbs_path}catalog.lance' if dbs_path else f'{DuckdbUtil.workspacedb_path}/catalog.lance'

        return lancedb.connect(lance_path, storage_options=storage_options or {})
    

    @staticmethod
    def generate_id():
        import time
        import secrets
        timestamp = int(time.time() * 1000)   
        rand_part = secrets.randbelow(10**6)
        return timestamp * 10**6 + rand_part