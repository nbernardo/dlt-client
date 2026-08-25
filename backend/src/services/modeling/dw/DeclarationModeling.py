from utils.duckdb_util import DuckdbUtil

class DeclarationModeling:
    ...

    def persist_model(self, namespace: str, dw: str, declaration: str, model: str, model_name: str):
        try:
            cnx = DuckdbUtil.get_workspace_db_instance()
            with cnx.cursor() as cursor:
                query = 'SELECT * FROM dw_declarations WHERE namespace = ? AND dw_name = ? AND model_name = ?'
                result = cursor.execute(query, [namespace, dw, model_name]).fetchall()
                if(len(result) > 0): 
                    return { 'error': False, 'result': False, 'existing': True }

            with cnx.cursor() as cursor:
                query = f"""
                    INSERT INTO dw_declarations (dw_name, type, namespace, declaration, model, model_name) VALUES (?,?,?,?,?,?)
                    ON CONFLICT (id) DO UPDATE SET declaration = EXCLUDED.declaration, model = EXCLUDED.model
                """
                cursor.execute(query, [dw, 'model', namespace, declaration, model, model_name])
                cursor.execute('CHECKPOINT')
            return { 'error': False, 'result': True }
        except Exception as err:
            return { 'error': True, 'result': str(err) }


    def persist_quality_rules():
        ...