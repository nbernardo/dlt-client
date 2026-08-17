from datetime import datetime
from utils.duckdb_util import DuckdbUtil

class DuckDBCache:
    
    def connect():
        DuckdbUtil.create_cache_table()
    

    def get(key):
        """Get value from cache"""
        cnx = DuckdbUtil.get_workspace_db_instance()
        result = cnx.execute("SELECT value FROM cache WHERE key = ?", [key]).fetchone()
        return result[0] if result else None
    

    def set(key, value):
        """Set value in cache"""
        cnx = DuckdbUtil.get_workspace_db_instance()
        cnx.execute("""
            INSERT OR REPLACE INTO cache (key, value) VALUES (?, ?)
        """, [key, value])
    

    def remove(key):
        """Remove a specific key from cache"""
        cnx = DuckdbUtil.get_workspace_db_instance()
        cnx.execute("DELETE FROM cache WHERE key = ?", [key])


    def clear(self):
        """Clear all cache entries"""
        DuckdbUtil.get_workspace_db_instance().execute("DELETE FROM cache")

