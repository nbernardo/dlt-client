import dlt
from sqlalchemy import create_engine, text, inspect

def column_type_conversion(columns, connection, table, schema):

    column_query = text(f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema
        AND TABLE_NAME = :table
        ORDER BY ORDINAL_POSITION
    """)
            
    result = connection.execute(column_query, {"schema": schema, "table": table})
    columns = []

    for row in result:
        col_name = row[0]
        data_type = row[1]
        
        if data_type.lower() in ['hierarchyid', 'geometry', 'geography', 'xml']:
            columns.append(f"CAST([{col_name}] AS NVARCHAR(MAX)) as [{col_name}]")
        else:
            columns.append(f"[{col_name}]")
            
    return ", ".join(columns)


@dlt.source
def dynamic_mssql_source(
    tables: list[str],
    primary_keys: list[str],
    connection_string: str
):
    """Source that generates resources dynamically with injected engine"""    
    def create_table_resource(table: str, key):
        @dlt.resource(name=table, primary_key=key)
        def table_data():

            engine = create_engine(connection_string)
            inspector = inspect(engine)

            schema_name, table_name = table.split('.')
            columns = inspector.get_columns(table_name, schema=schema_name)
            parsed_columns = column_type_conversion(columns, engine.connect(), table_name, schema_name)

            query = f"SELECT {parsed_columns} FROM {schema_name}.{table_name}"
            result = engine.connect().execute(text(query))
            
            for row in result:
                yield dict(row._mapping)
        
        return table_data
    
    return [create_table_resource(tables[index], primary_keys[index]) for index in range(len(tables))]