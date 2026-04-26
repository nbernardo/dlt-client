from utils.DestinationQueryUtil import DestinationQueryUtil
from utils.metastore.PipelineMedatata import PipelineMedatata

class OdooDBIntegration:

    @staticmethod
    def get_modules(namespace, pipeline):
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
        pipeline_meta = PipelineMedatata.get_pipeline_metadata(pipeline, namespace)
        connection_name = pipeline_meta[2]
        return DestinationQueryUtil._query_sql_database(query, namespace, connection_name)
    

    @staticmethod
    def get_tables_by_module(module_name: str, namespace, pipeline):
        query = f'''
            WITH RECURSIVE TableHierarchy AS (
                -- Anchor: Fetch core business models for the specific module
                SELECT 
                    1 AS level, NULL::text COLLATE "default" AS parent_table,
                    REPLACE(m.model, '.', '_')::text COLLATE "default" AS physical_table,
                    UPPER(split_part(m.model, '.', 1))::text COLLATE "default" AS path
                FROM ir_model m
                WHERE m.transient = false 
                AND m.state = 'base'
                -- Block technical namespaces
                AND m.model NOT SIMILAR TO '(ir\.|base\.|mail\.|bus\.|report\.|sms\.|snailmail\.|calendar\.|spreadsheet\.|web\.|wizard\.|portal\.|l10n\.|digest\.).*'
                AND m.model LIKE '{module_name.lower()}.%' 

                UNION ALL

                -- Recursive: Trace relationships while blocking utility/noise tables
                SELECT 
                    th.level + 1,
                    th.physical_table,
                    child_cl.relname::text COLLATE "default",
                    (th.path || ' -> ' || child_cl.relname)::text COLLATE "default"
                FROM TableHierarchy th
                JOIN pg_class parent_cl ON parent_cl.relname = th.physical_table
                JOIN pg_constraint con ON con.confrelid = parent_cl.oid
                JOIN pg_class child_cl ON child_cl.oid = con.conrelid
                WHERE con.contype = 'f' 
                AND th.level < 5
                -- Block physical tables that are technical or cross-module noise
                AND child_cl.relname NOT SIMILAR TO '(ir_|mail_|bus_|sms_|snailmail_|calendar_|web_|spreadsheet_|portal_|l10n_|digest_|report_|wizard_|res_users|res_groups|res_partner_bank|res_currency|payment_token).*'
                AND child_cl.relname NOT LIKE '%_wizard'
                AND child_cl.relname NOT LIKE '%_rel' -- Optional: removes many-to-many join tables
            )
            SELECT DISTINCT ON (path)
                level, parent_table, physical_table AS table_name, path
            FROM TableHierarchy
            ORDER BY path, level;
        '''
        pipeline_meta = PipelineMedatata.get_pipeline_metadata(pipeline, namespace)
        connection_name = pipeline_meta[2]
        return DestinationQueryUtil._query_sql_database(query, namespace, connection_name)