import json
import time
import aiosqlite
import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from collections import defaultdict
import hashlib
import re
import logging

class UserAccessLevel:

    db_connection: aiosqlite.Connection 
    
    async def save_access_tables_constraint(conn: aiosqlite.Connection, role_name: str, tables_config_json: str):
        tables = tables_config_json.get('tables')
        if not tables: return

        path_value_pairs, sql_placeholders = [], []
        
        for table_name, table_constraint in tables.items():
            table_path = f"$.tables.{table_name}"
            serialized_config = json.dumps(table_constraint)
            
            path_value_pairs.append(table_path)
            path_value_pairs.append(serialized_config)
            sql_placeholders.append("?, json(?)")

        arguments_string = ", ".join(sql_placeholders)
        
        query = f"UPDATE roles SET data_access_level = json_set(data_access_level, {arguments_string}) WHERE role_name = ?;"
        query_params = path_value_pairs + [role_name]
        
        async with conn as dbconn:
            await dbconn.execute(query, query_params)
            await dbconn.commit()    


    async def get_access_tables_constraint(roles_list: list[str], dw_name, field_by_table: dict):
        from services.user_management.UserService import get_db_connection
        
        target_tables = list(field_by_table.keys())
        role_placeholders = ",".join(["?"] * len(roles_list))
        table_placeholders = ",".join(["?"] * len(target_tables))
        
        query = f"""
            SELECT jt.key as table_name, jt.value as governance_rules
            FROM 
                roles r, json_tree(r.data_access_level, '$.tables') jt
            WHERE 
                r.role_name IN ({role_placeholders}) AND jt.path = '$.tables'  AND LOWER(jt.key) IN ({table_placeholders})
        """
        
        query_args = roles_list + target_tables

        async with get_db_connection() as conn:
            try:
                result, total_not_allowed = {}, 0
                cursor = await conn.execute(query, query_args)
                rows = await cursor.fetchall()
                await cursor.close()
                
                for _table_name, governance_rules in rows:
                    table_name = str(_table_name).lower()
                    if not governance_rules: 
                        continue
                    
                    table_path = f'{dw_name}.{table_name.replace(dw_name,'')}'
                    rule_dict = json.loads(governance_rules)
                    if not rule_dict.get("has_access", True):
                        total_not_allowed = total_not_allowed + 1
                        result[table_name] = f"You don't have permission to query {table_path}"
                        continue

                    hidden_cols, blocked_fields = rule_dict.get("hidden_columns", []), None
                    fields_from_query = set(field_by_table[table_name]) if (table_name in field_by_table) else set()

                    if(table_name in field_by_table):
                        blocked_fields = (fields_from_query & set([col.lower() for col in hidden_cols]))
                    
                    if len(blocked_fields) > 0:
                        total_not_allowed = total_not_allowed + 1
                        result[table_name] = f"You don't have permission in {table_path} to see {str(blocked_fields)} fields"

                return { 'result': result, 'total_not_allowed': total_not_allowed }
                
            except Exception as err:
                logging.error(f'Error while fetching constraint: {str(err)}')
                return {}


    async def get_access_tables_constraints(role_string: str, dw_name):
        from services.user_management.UserService import get_db_connection
        async with get_db_connection() as conn:
            try:
                result = {}
                cursor = await conn.execute(f"SELECT data_access_level FROM roles WHERE role_name = '{role_string}'")
                rows = await cursor.fetchall()
                await cursor.close()

                contraints = json.loads(rows[0][0])
                
                for table in list(contraints.get('tables',{}).keys()):
                    result[table.replace(f'{dw_name}_','')] = contraints.get('tables').get(table)

                return result
            except Exception as err:
                logging.error(f'Error while fetching constraint: {str(err)}')


    def extract_and_translate_query(con, sql_query, dw=None, dialect="tsql"):
        """
        1. Parses SQL query and returns the original dictionary mapping physical tables to columns.
        2. Builds a translated outer wrapper query for direct DuckDB Parquet exports without modifying the original
           also considering the data dictionary.
        """
        parsed_ast = sqlglot.parse_one(sql_query, read=dialect)
        
        try:
            qualified_ast = qualify(parsed_ast, dialect=dialect)
        except Exception:
            qualified_ast = parsed_ast

        cte_names = {cte.alias_or_name.lower() for cte in qualified_ast.find_all(exp.CTE)}

        # A dictionary that defaults to an empty set to ensure columns are unique
        tbl_col_map, select_items = defaultdict(set), []
        
        # Pre-map table aliases to true names to catch edge cases
        alias_to_table = { tbl_exp.alias_or_name.lower(): tbl_exp.name for tbl_exp in qualified_ast.find_all(exp.Table) }

        for col_exp in qualified_ast.find_all(exp.Column):
            column_name, table_prefix = col_exp.name, col_exp.text("table")           
            resolved_table = table_prefix if table_prefix else ''
                
            for table_exp in qualified_ast.find_all(exp.Table):
                if table_exp.alias_or_name.lower() == table_prefix.lower():
                    resolved_table = table_exp.name
                    break
                
            if resolved_table.lower() not in cte_names:
                tbl_col_map[resolved_table].add(column_name)

        # Reconstruct original expected dictionary format
        field_by_tbl = { f"{dw + '_' if dw else ''}{tbl}": sorted(list(cols)) for tbl, cols in tbl_col_map.items() }

        # Clean the keys to pass only physical table names (without dw_ prefix) to the metadata query
        detected_tables = [tbl.replace(f'{dw}_','') for tbl in field_by_tbl.keys()]

        # Retrieve the exact-case translation map from your metadata method
        translation_map = UserAccessLevel.get_translation_map_by_tables(con, detected_tables)
        
        for expression in qualified_ast.expressions:
            if isinstance(expression, exp.Alias):
                output_name, base_node = expression.alias, expression.this
            else:
                output_name, base_node = expression.name, expression

            # Get the true table name tied to this outer column
            table_prefix = base_node.text("table")
            resolved_table = alias_to_table.get(table_prefix.lower(), table_prefix)

            table_map = translation_map.get(resolved_table, {})
            translated_name = table_map.get(output_name, output_name)

            select_items.append(f'"{output_name}" AS "{translated_name}"')

        # Wrap your original untouched sql_query string
        translated_wrapper_sql = f"SELECT {', '.join(select_items)} FROM ({sql_query}) AS final_output_stream"

        return field_by_tbl, translated_wrapper_sql


    def get_translation_map_by_tables(con, table_names):
        """
        Queries dwhperformance_meta.field_dictionary for a specific list of tables 
        and returns a nested dictionary of case-sensitive translations.
        """
        if not table_names: return {}
            
        # Build the structured subquery to aggregate matching tables only
        dictionary_query = """
        SELECT 
            json_group_object(table_name, fields_json::JSON)
        FROM (
            SELECT 
                LOWER(table_name) as table_name, json_group_object(LOWER(field_name), translation) AS fields_json
            FROM 
                dwhperformance_meta.field_dictionary
            WHERE status = true AND LOWER(table_name) IN ({}) GROUP BY table_name
        );
        """.format(", ".join(f"'{t}'" for t in table_names))

        # Execute and parse directly to a native dictionary
        json_result = con.execute(dictionary_query).fetchone()[0]
        return json.loads(json_result) if json_result else {}


    def generate_query_hash(sql_query: str, size_bytes: int = 8) -> str:
        """Normalizes a SQL query string and hashes it into a compact hex string to generate cache key."""
        
        normalized = re.sub(r'(--.*)|(/\*[\s\S]*?\*/)', '', sql_query.lower())        
        normalized = " ".join(normalized.split())
        
        hasher = hashlib.blake2b(digest_size=size_bytes)
        hasher.update(normalized.encode('utf-8'))
        
        return hasher.hexdigest()