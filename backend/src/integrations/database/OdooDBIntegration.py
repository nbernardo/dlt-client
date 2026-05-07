from utils.DestinationQueryUtil import DestinationQueryUtil
from utils.metastore.PipelineMedatata import PipelineMedatata

class OdooDBIntegration:

    @staticmethod
    def get_modules(namespace, connection_name = None, pipeline = None):
        query = '''
            -- FETCH ALL UNIQUE MODULES AS ENTRY POINTS (ULTIMATE FILTER)
            WITH RawModules AS (
                SELECT DISTINCT UPPER(split_part(model, '.', 1)) AS module_name
                FROM ir_model WHERE transient = false AND state = 'base'
            )
            SELECT 
                1 AS level, NULL::text AS parent_table, module_name AS table_name, module_name AS path
            FROM RawModules
            WHERE module_name NOT SIMILAR TO '(IR|BASE|MAIL|BUS|REPORT|SMS|SNAILMAIL|CALENDAR|SPREADSHEET|WEB|WIZARD|PORTAL|L10N|DIGEST|AUTH|AVATAR|BARCODE|BARCODES|DECIMAL|FETCHMAIL|GOOGLE|IAP|IMAGE|PHONE|PRIVACY|PUBLISHER|RESOURCE|SEQUENCE|TEMPLATE|UOM|UTM|RES|WEB_EDITOR|WEB_TOUR|FORMAT|UNKNOWN|UTM|VENDOR|BANK|ANALYTIC|ACCOUNT_FOLLOWUP|PAYMENT|RATING|UTM|CRM_IAP|BASE_IMPORT|ONBOARDING).*'
            AND module_name NOT LIKE 'IR_ACTIONS%' AND module_name NOT LIKE 'L10N_%'
            AND module_name ~ '^[A-Z]' AND length(module_name) > 2
            ORDER BY table_name;
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
            WITH RECURSIVE TableHierarchy AS (
                -- Anchor: Fetch core tables for the module
                SELECT 
                    1 AS level,
                    NULL::text COLLATE "default" AS parent_table,
                    relname::text COLLATE "default" AS physical_table,
                    UPPER('{module_name}')::text COLLATE "default" AS path, 
                    NULL::text COLLATE "default" AS source_column, 
                    NULL::text COLLATE "default" AS target_column  
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public' 
                AND c.relkind = 'r'
                AND relname LIKE '{module_name.lower()}_%' 
                AND relname NOT SIMILAR TO '(ir_|base_|mail_|bus_|report_|sms_|snailmail_|calendar_|spreadsheet_|web_|wizard_|portal_|l10n_|digest_|res_).*'

                UNION ALL

                -- Recursive: Trace relations
                SELECT 
                    th.level + 1,
                    th.physical_table,
                    ref_cl.relname::text COLLATE "default",
                    (th.path || ' -> ' || ref_cl.relname)::text COLLATE "default",
                    (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = con.conrelid AND a.attnum = con.conkey[1])::text COLLATE "default" AS source_column,
                    (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = con.confrelid AND a.attnum = con.confkey[1])::text COLLATE "default" AS target_column
                FROM TableHierarchy th
                JOIN pg_class child_cl ON child_cl.relname = th.physical_table
                JOIN pg_constraint con ON con.conrelid = child_cl.oid  
                JOIN pg_class ref_cl ON ref_cl.oid = con.confrelid    
                WHERE con.contype = 'f' 
                AND th.level < 5
                AND ref_cl.relname NOT SIMILAR TO '(ir_|base_|mail_|bus_|sms_|snailmail_|calendar_|web_|spreadsheet_|portal_|l10n_|digest_|report_|wizard_|res_).*'
            )
            SELECT DISTINCT ON (path)
                level,
                parent_table,
                physical_table AS table_name,
                path,
                source_column,
                target_column,
                CASE 
                    WHEN source_column IS NOT NULL THEN source_column || ' ➔ ' || target_column 
                    ELSE '' 
                END AS relation_label,
                (
                    SELECT string_agg(
                        a.attname || CASE WHEN pk.contype IS NOT NULL THEN ' (PK)' ELSE '' END, 
                        ', '
                    )
                    FROM pg_attribute a
                    JOIN pg_class c ON c.oid = a.attrelid
                    -- Standard PK check for the table identified in this specific row
                    LEFT JOIN pg_constraint pk ON pk.conrelid = c.oid 
                        AND pk.contype = 'p' 
                        AND a.attnum = ANY(pk.conkey)
                    WHERE c.relname = physical_table -- Use the current row's table name directly
                    AND a.attnum > 0 
                    AND NOT a.attisdropped
                ) AS all_fields
            FROM TableHierarchy
            WHERE 
                physical_table NOT LIKE 'ir_%' AND
                physical_table NOT LIKE 'mail_%' AND
                physical_table NOT LIKE 'sms_%' AND
                physical_table NOT LIKE 'report_%' AND
                physical_table NOT LIKE 'l10n_%' AND
                
                (parent_table IS NULL OR (
                    parent_table NOT LIKE 'ir_%' AND
                    parent_table NOT LIKE 'mail_%' AND
                    parent_table NOT LIKE 'sms_%' AND
                    parent_table NOT LIKE 'report_%' AND
                    parent_table NOT LIKE 'l10n_%'
                ))
            ORDER BY path, level;
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