import duckdb


class DuckdbUtil:

    @staticmethod
    def get_tables(database = None, where = None):
        cnx = duckdb.connect(f'{database}', read_only=True)
        cursor = cnx.cursor()
        where_clause = f'WHERE {where}' if where is not None else ''
        query = f"SELECT \
                    database_name, schema_name, table_name, \
                    estimated_size, column_count FROM \
                    duckdb_tables {where_clause}"
        print(f'THE QUERY WILL BE: {query}')
        return cursor.execute(query)


