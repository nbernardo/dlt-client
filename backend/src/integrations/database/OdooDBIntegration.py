from utils.DestinationQueryUtil import DestinationQueryUtil
from utils.metastore.PipelineMedatata import PipelineMedatata

class OdooDBIntegration:
        
    @staticmethod
    def get_db_tables(namespace, connection_name = None, pipeline = None):
        query = '''
            SELECT  n.nspname AS schema_name, c.relname AS table_name, split_part(c.relname, '_', 1) AS module_prefix
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relkind = 'r'
            AND c.relname NOT SIMILAR TO '(ir_|base_|mail_|bus_|report_|sms_|snailmail_|calendar_|spreadsheet_|web_|wizard_|portal_|l10n_|digest_|res_).*'            
            ORDER BY c.relname;
        '''
        try:
            if connection_name == None:
                pipeline_meta = PipelineMedatata.get_pipeline_metadata(pipeline, namespace)
                connection_name = pipeline_meta[2]

            result = DestinationQueryUtil._query_sql_database(query, namespace, connection_name)
            return result
        except Exception as err:
            print(f'Error while fetching Odoo modules: {str(err)}')
            return {}
    

    @staticmethod
    def _tables_rel_query():
        return '''
            SELECT 
                conname AS constraint_name, att2.attname AS source_column,  cl.relname AS source_table, att.attname AS target_column, cl2.relname AS target_table
            FROM pg_constraint con
            JOIN pg_class cl ON cl.oid = con.conrelid
            JOIN pg_class cl2 ON cl2.oid = con.confrelid
            JOIN pg_attribute att ON att.attrelid = con.confrelid AND att.attnum = con.confkey[1]
            JOIN pg_attribute att2 ON att2.attrelid = con.conrelid AND att2.attnum = con.conkey[1]
            WHERE con.contype = 'f'
            -- Filter out non-business tables directly at the DB level
            AND cl.relname NOT SIMILAR TO '(ir_|base_|mail_|bus_|sms_|web_|spreadsheet_|digest_|report_|wizard_|res_users|res_groups|auth_).*'
            AND cl2.relname NOT SIMILAR TO '(ir_|base_|mail_|bus_|sms_|web_|spreadsheet_|digest_|report_|wizard_|res_users|res_groups|auth_).*';
        '''
    

    @staticmethod
    def get_tables_by_module(module_name: str, namespace, connection_name, pipeline):
        query = f'''
                WITH RECURSIVE fk_tree AS (

                SELECT
                    c.oid AS table_oid,
                    n.nspname AS schema,
                    c.relname AS "table",
                    NULL::oid AS parent_oid,
                    NULL::text COLLATE "C" AS parent_schema,
                    NULL::text COLLATE "C" AS parent_table,
                    NULL::text COLLATE "C" AS fk_column,
                    NULL::text COLLATE "C" AS ref_column,
                    0 AS depth,
                    ARRAY[c.oid] AS path_oids,
                    ARRAY[c.relname] AS path_names
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = '{module_name.lower()}'
                    AND n.nspname = 'public'

                UNION ALL

                SELECT
                    child_cl.oid AS table_oid,
                    child_ns.nspname AS schema,
                    child_cl.relname AS "table",
                    fk.table_oid AS parent_oid,
                    fk.schema AS parent_schema,
                    fk."table" AS parent_table,
                    child_att.attname AS fk_column,
                    par_att.attname AS ref_column,
                    fk.depth + 1 AS depth,
                    fk.path_oids || child_cl.oid,
                    fk.path_names || (child_cl.relname)
                FROM fk_tree fk
                JOIN pg_constraint con ON con.confrelid = fk.table_oid
                                        AND con.contype = 'f'
                JOIN pg_class child_cl ON child_cl.oid = con.conrelid
                JOIN pg_namespace child_ns ON child_ns.oid = child_cl.relnamespace
                JOIN pg_attribute child_att ON child_att.attrelid = con.conrelid
                                            AND child_att.attnum = con.conkey[1]
                JOIN pg_attribute par_att ON par_att.attrelid = con.confrelid
                                            AND par_att.attnum = con.confkey[1]
                WHERE fk.depth < 5
                    AND NOT child_cl.oid = ANY(fk.path_oids)

                ),
                table_columns AS (

                    -- Collect all columns with PK flag for every table touched by the tree
                    SELECT
                        a.attrelid AS table_oid,
                        string_agg(
                            a.attname || CASE WHEN pk.attnum IS NOT NULL THEN ' (PK)' ELSE '' END,
                            ','
                            ORDER BY a.attnum
                        ) AS columns
                    FROM pg_attribute a
                    LEFT JOIN (
                        SELECT conrelid, unnest(conkey) AS attnum
                        FROM pg_constraint
                        WHERE contype = 'p'
                    ) pk ON pk.conrelid = a.attrelid AND pk.attnum = a.attnum
                    WHERE a.attnum > 0
                        AND NOT a.attisdropped
                        AND a.attrelid IN (SELECT table_oid FROM fk_tree)
                    GROUP BY a.attrelid

                )
                SELECT
                    fk.depth,
                    COALESCE(fk.parent_table, '(root)') AS parent,
                    fk."table",
                    array_to_string(fk.path_names, ' -> ') AS path,
                    fk.ref_column,
                    fk.fk_column,
                    fk.ref_column || ' ➔ ' || fk.fk_column as relation_label,
                    CASE
                        WHEN fk.depth = 0 THEN tc.columns
                        WHEN fk.depth = 1 THEN pc.columns
                        ELSE tc.columns
                    END AS columns,
                    '{module_name.lower()}' as anchor
                FROM fk_tree fk
                LEFT JOIN table_columns tc ON tc.table_oid = fk.table_oid
                LEFT JOIN table_columns pc ON pc.table_oid = fk.parent_oid
                ORDER BY fk.path_oids;
        '''

        try:
            if connection_name == None:
                pipeline_meta = PipelineMedatata.get_pipeline_metadata(pipeline, namespace)
                connection_name = pipeline_meta[2]

            tables = DestinationQueryUtil._query_sql_database(query, namespace, connection_name)
            relations = DestinationQueryUtil._query_sql_database(OdooDBIntegration._tables_rel_query(), namespace, connection_name)

            return { 'tables': tables.get('result', {}), 'relations': relations.get('result', {}) }
        
        except Exception as err:
            print(f'Error while fetching Odoo modules: {str(err)}')
            return {}