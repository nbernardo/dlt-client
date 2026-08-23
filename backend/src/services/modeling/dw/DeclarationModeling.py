from utils.duckdb_util import DuckdbUtil

class DeclarationModeling:
    ...

    def persist_model(self, namespace: str, dw: str, declaration: str, model: str):
        try:
            cnx = DuckdbUtil.get_workspace_db_instance()
            with cnx.cursor() as cursor:
                query = f"INSERT INTO dw_declarations (dw_name, type, namespace, declaration, model) VALUES (?,?,?,?,?)"
                cursor.execute(query, [dw, 'model', namespace, declaration, model])
            return { 'error': False, 'result': True }
        except Exception as err:
            return { 'error': True, 'result': str(err) }


    def persist_quality_rules():
        ...