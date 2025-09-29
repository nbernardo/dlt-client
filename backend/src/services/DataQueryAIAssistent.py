import duckdb
import json
import re
from mistralai import Mistral
from os import getenv as env
from utils.duckdb_util import DuckdbUtil

class DataQueryAIAssistent:

    def __init__(self, base_db_path):
        self.DB_SCHEMA = '''The database contains %total_table% tables:'''
        self.db_path = base_db_path
        self.db = f'{self.db_path}/servicos_agendandos_pipeline.duckdb'
        self.messages = [{"role": "system", "content": self.get_system_instructions()}]

        api_key = env('MISTRAL_API_KEY')
        self.model = "mistral-large-latest"
        self.client = Mistral(api_key=api_key)
        self.chat_turns = []
        

    SYSTEM_INSTRUCTION = (
        "You are a SQL query generator. When a user asks about data, you MUST first call the 'generate_sql_query_signal' tool. "
        "Always call this tool before doing anything else." \
        f" You'll always run the function calling even if it's a previous answered questions."
        f"\n\n--- DATABASE SCHEMA ---\n%db_schema%\n-----------------------."
    )


    def parse_sql(content):

        sql_query = None
        #sql_pattern = r'\b(SELECT\s+.*?;|INSERT\s+.*?;|UPDATE\s+.*?;|DELETE\s+.*?;)'
        sql_pattern = r'\b(SELECT\s+.*?;)'
        sql_match = re.search(sql_pattern, content, re.IGNORECASE | re.DOTALL)
        if sql_match:
            sql_query = sql_match.group(1).strip()
        
        return sql_query


    def get_system_instructions(self):

        DB_SCHEMA, db = self.DB_SCHEMA, self.db
        tables = {}
        table_count = 0
        skip_tables = ['_dlt_loads','_dlt_pipeline_state','_dlt_pipeline_state','_dlt_version']
        query = f"SELECT DISTINCT \
                    t.table_name, \
                    t.schema_name, \
                    column_name, \
                    data_type \
                FROM duckdb_tables t \
                JOIN duckdb_columns c ON t.table_name = c.table_name \
                ORDER BY t.table_name, column_name"

        conn = duckdb.connect(db)
        columns = conn.execute(query).fetchall()
        
        for table_name, schema_name, column_name, data_type in columns:

            if table_name not in skip_tables:
                if not(table_name in tables):
                    table_count = table_count + 1
                    tables[table_name] = True
                    DB_SCHEMA += f"\n\n{table_count}. Table: '{schema_name}.{table_name}'"
                else:
                    DB_SCHEMA += f'\n- {column_name}'
            else:
                pass

        DB_SCHEMA = DB_SCHEMA.replace('%total_table%',str(table_count))
        conn.close()

        return DataQueryAIAssistent.SYSTEM_INSTRUCTION.replace('%db_schema%',DB_SCHEMA)


    def run_query(self, query):
        conn = DuckdbUtil.get_connection_for(self.db)
        db_result = conn.execute(query).fetchall()
        print(db_result)
        return db_result
        

    def generate_sql_query_signal(self, natural_language_question: str) -> str:
        """
        Signals the start of the SQL generation process. 
        Returns the user's natural language question back to the LLM to guide SQL generation.
        """
        return f"Request acknowledged: {natural_language_question}"


    def cloud_mistral_call(self, user_prompt):
        client, model = self.client, self.model
        
        try:
            self.messages.append({"role": "user", "content": user_prompt})
            tool_calling = client.chat.complete(
                model=model,
                messages=self.messages,
                tools=ToolsDefinition.SQL_TOOL,
            )

            tool_calls = tool_calling.choices[0].message.tool_calls

            if tool_calls is not None:

                found_tool = tool_calls[0]
                function_name = found_tool.function.name
                function_args = found_tool.function.arguments
                function_args = json.loads(function_args)
                            
                print(f"1. LLM requested Tool Call: {function_name}")
                print(f"   -> Arguments: {function_args.get('natural_language_question')}")

                if function_name == "generate_sql_query_signal":
                    function_output = self.generate_sql_query_signal(function_args.get('natural_language_question', ''))

                    self.messages.append(tool_calling.choices[0].message)
                    for tool_cal in tool_calls:
                        tool_cal.function.name
                        self.messages.append(                         {
                             "role": "tool",
                             "content": json.dumps({"result": function_output}),
                             "tool_call_id": found_tool.id
                         })
                    
                    print("\n2. Sending Tool Output back to LLM for SQL Generation...")                
                    return { 'answer': 'final', 'result': self.handle_response(client, model) }

                else:
                    print(f"Error: Unknown function name requested: {function_name}")
                    
            else:
                print("1. LLM decided not to call a function. Raw response:")
                print(tool_calling)
                return { 'answer': 'intermediate', 'result': tool_calling }

        except Exception as e:
            print(f"\nAn error occurred: {e}")
            print("Please ensure the 'ollama' Python package is installed (`pip install ollama`), Ollama is running, and you have pulled the required model (`ollama pull phi3`).")


    def handle_response(self, client: Mistral, model):
        nl_to_sql_call = client.chat.complete(model=model, messages=self.messages,)
        self.messages.append({ 'role': 'assistant', 'content': nl_to_sql_call.choices[0].message.content })
        sql_query = nl_to_sql_call.choices[0].message.content.strip()
        sql_query = DataQueryAIAssistent.parse_sql(sql_query)
        self.run_query(sql_query)



class ToolsDefinition:
    SQL_TOOL = [
        {
            "type": "function",
            "function": {
                "name": "generate_sql_query_signal",
                "description": "Call this function whenever the user asks a question that requires accessing structured data, "
                            "like counting employees or filtering records. The LLM will then use the schema provided in "
                            "the system prompt to generate the corresponding SQL query.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "natural_language_question": {
                            "type": "string",
                            "description": "The full natural language question from the user. IMPORTANT: The argument passed to this function MUST be the original user question, verbatim, without alteration."
                        }
                    },
                    "required": ["natural_language_question"]
                }
            }
        },
    ]







