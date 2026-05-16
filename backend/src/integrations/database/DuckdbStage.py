class DuckdbStage:
    ''''
    This class handle different scenario concerning to data which was put in stage 
    in Duckdb database so to address integration from different data sources 
    '''

    def get_tables_hierarchy(stg_table, stg_db):
        query = f''''
            WITH RECURSIVE fk_tree AS (

                SELECT
                    t.table_schema AS schema,
                    t.table_name AS "table",
                    NULL::VARCHAR AS parent_schema,
                    NULL::VARCHAR AS parent_table,
                    NULL::VARCHAR AS fk_column,
                    NULL::VARCHAR AS ref_column,
                    0 AS depth,
                    ARRAY[t.table_schema || '.' || t.table_name] AS path_names

                FROM information_schema.tables t
                WHERE t.table_name = '{stg_table}' AND t.table_schema = 'novastage'

                UNION ALL

                SELECT
                    fk.schema AS schema,
                    m.table_name AS "table",
                    fk.schema AS parent_schema,
                    fk."table" AS parent_table,
                    m.fk_col AS fk_column,
                    m.ref_col AS ref_column,
                    fk.depth + 1 AS depth,
                    fk.path_names || ARRAY[fk.schema || '.' || m.table_name]

                FROM fk_tree fk
                JOIN dwhperformance_meta.fk_map m ON m.ref_table = fk."table"
                WHERE fk.depth < 5
                    AND NOT list_contains(fk.path_names, fk.schema || '.' || m.table_name)
                    AND EXISTS (
                    SELECT 1 FROM information_schema.tables t
                    WHERE t.table_name = m.table_name
                        AND t.table_schema  = fk.schema
                        AND t.table_catalog = '{stg_db}'
                        AND t.table_schema NOT LIKE '%_staging'
                    )
            ),
            table_columns AS (

                SELECT
                    c.table_schema, c.table_name, string_agg(c.column_name, ', ' ORDER BY c.ordinal_position) AS columns
                FROM information_schema.columns c
                WHERE 
                    (c.table_schema, c.table_name) IN (
                        SELECT schema, "table" FROM fk_tree
                        UNION
                        SELECT parent_schema, parent_table FROM fk_tree WHERE parent_table IS NOT NULL
                    )
                    AND c.table_catalog = '{stg_db}' AND c.table_schema NOT LIKE '%_staging'
                GROUP BY c.table_schema, c.table_name
            )

            SELECT
                fk.depth,
                COALESCE(fk.parent_schema || '.' || fk.parent_table, '(root)') AS parent,
                fk."table",
                array_to_string(fk.path_names, ' -> ') AS path,
                fk.ref_column,
                fk.fk_column,
                fk.ref_column || ' ➔ ' || fk.fk_column AS relation_label,
                tc.columns
            FROM fk_tree fk
            LEFT JOIN table_columns tc ON tc.table_schema = fk.schema AND tc.table_name = fk."table"
            ORDER BY fk.path_names;
        '''
    


    