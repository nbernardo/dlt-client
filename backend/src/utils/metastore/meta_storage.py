from utils.db.lancedb import LanceConnectionFactory
from utils.metastore.DataCatalog import DataCatalog

class MetaStore:
    """LanceDB-backed catalog store. Writes via LanceDB (MVCC), reads via DuckDB SQL."""

    @staticmethod
    def _get_lance_conn(dbs_path=None) -> str:
        return LanceConnectionFactory.get(dbs_path)


    @staticmethod
    def persist_catalog(table_source: str, dbs_path=None, pipeline=None, load_info=None, table_to_schema_map={}):
        """Persists column catalog to LanceDB. Concurrent writes via MVCC — This is called from the pipeline run itself"""
        return DataCatalog.persist_catalog(table_source, dbs_path, pipeline, load_info, table_source)


    @staticmethod
    def get_pipeline_metadata(pipeline: str, dbs_path=None, display_fields = False):
        return DataCatalog.get_pipeline_datacatalog(pipeline, dbs_path, display_fields)