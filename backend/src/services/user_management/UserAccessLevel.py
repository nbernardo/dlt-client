import json
import time
import aiosqlite
import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from collections import defaultdict
import hashlib
import re

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
                r.role_name IN ({role_placeholders}) AND jt.path = '$.tables'  AND jt.key IN ({table_placeholders})
        """
        
        query_args = roles_list + target_tables

        async with get_db_connection() as conn:
            try:
                result, total_not_allowed = {}, 0
                cursor = await conn.execute(query, query_args)
                rows = await cursor.fetchall()
                await cursor.close()
                
                for table_name, governance_rules in rows:
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
                        blocked_fields = (fields_from_query & set(hidden_cols))
                    
                    if len(blocked_fields) > 0:
                        total_not_allowed = total_not_allowed + 1
                        result[table_name] = f"You don't have permission in {table_path} to see {str(blocked_fields)} fields"

                return { 'result': result, 'total_not_allowed': total_not_allowed }
                
            except Exception as err:
                print(f'Error while fetching constraint: {str(err)}')
                return {}


    async def get_access_tables_constraints(role_string: str, dw_name):
        from services.user_management.UserService import get_db_connection
        async with get_db_connection() as conn:
            try:
                result = {}
                cursor = await conn.execute(f"SELECT data_access_level FROM roles WHERE role_name = '{role_string}'")
                rows = await cursor.fetchall()
                await cursor.close()
                
                for row in rows:
                    if row[0] == '{}': continue

                    constraint = json.loads(row[0]).get('tables').items()
                    table = next(iter(constraint.mapping))

                    result[table.replace(f'{dw_name}_','')] = constraint.mapping[table]

                return result
            except Exception as err:
                print(f'Error while fetching constraint: {str(err)}')


    def extract_columns_by_table(sql_query, dw_name = None, dialect="tsql"):
        """Parses SQL query and returns a dictionary mapping each physical table to its respective columns."""
        parsed_ast = sqlglot.parse_one(sql_query, read=dialect)
        
        # Qualify the query. This automatically maps aliases (like 'c.name') 
        # back to their true table names (like 'customers.name') based on the context.
        try:
            qualified_ast = qualify(parsed_ast, dialect=dialect)
        except Exception:
            # Fallback to standard parse if qualify encounters strict schema mismatches
            qualified_ast = parsed_ast

        cte_names = {cte.alias_or_name.lower() for cte in qualified_ast.find_all(exp.CTE)}

        # A dictionary that defaults to an empty set to ensure columns are unique
        table_column_map = defaultdict(set)

        for column_exp in qualified_ast.find_all(exp.Column):
            column_name, table_prefix = column_exp.name, column_exp.text("table")           
            
            resolved_table = table_prefix if table_prefix else ''
                
            for table_exp in qualified_ast.find_all(exp.Table):
                if table_exp.alias_or_name.lower() == table_prefix.lower():
                    resolved_table = table_exp.name
                    break
                
            if resolved_table.lower() not in cte_names:
                table_column_map[resolved_table].add(column_name)

        return {f'{dw_name+'_' if dw_name else ''}{table}': sorted(list(cols)) for table, cols in table_column_map.items()}


    def generate_query_hash(sql_query: str, size_bytes: int = 8) -> str:
        """Normalizes a SQL query string and hashes it into a compact hex string to generate cache key."""
        
        normalized = re.sub(r'(--.*)|(/\*[\s\S]*?\*/)', '', sql_query.lower())        
        normalized = " ".join(normalized.split())
        
        hasher = hashlib.blake2b(digest_size=size_bytes)
        hasher.update(normalized.encode('utf-8'))
        
        return hasher.hexdigest()