import threading
import lancedb
from utils.duckdb_util import DuckdbUtil


class LanceConnectionFactory:

    _conn = None
    lance_path = None

    @staticmethod
    def get(dbs_path, storage_options=None):

        lance_path = \
            f'{dbs_path}catalog.lance' if dbs_path else f'{DuckdbUtil.workspacedb_path}/catalog.lance'

        return lancedb.connect(lance_path, storage_options=storage_options or {})