import json
import re
from mistralai import Mistral
from os import getenv as env
from utils.duckdb_util import DuckdbUtil
from services.workspace.Workspace import Workspace
from groq import Groq, RateLimitError, BadRequestError
import traceback

class DataQueryAIAssistent:

    prev_answered = 'PREV_ANSWER:'
    generate_sql_query = "'generate_sql_query_signal'"

    def __init__(self, base_db_path, dbfile = ''):

        self.DB_SCHEMA = '''The database contains %total_table% tables:'''
        self.db_path = base_db_path
        self.db = f'{self.db_path}/{dbfile}'

        self.ini_tables = Workspace.list_duck_dbs_with_fields(self.db_path, None)
        self.messages = [{"role": "system", "content": self.get_system_instructions_from_ini_meta()}]

        api_key = env('MISTRAL_API_KEY')
        #self.model = "mistral-large-latest"
        self.model = "mistral-medium-2508"
        self.client = Mistral(api_key=api_key)
        self.chat_turns = []
        

    SYSTEM_INSTRUCTION = (
        f"You are a SQL query generator which will get initial data from DATABASE SCHEMA, and also you might be able to update the schema when asked for it."
        f" At the end of each table columns there is two more metadata, the DB-File and the Schema for table seen above. "
        f" When generating the query, you MUST only output the query, nothing else. Query MUST use all needes fields from table separated by comma, do use *. "
        f" You'll STRICTLY act according to the following points:"
        f" 1. You'll function call 'get_database_update' only when asked about update or to update yoursel." \
        f" 2. If asked to query a specific table, you'll do {generate_sql_query} call, and you MUST generate the query enclosed in sql``` and you'll also add the DB-File enclosed in %% of that same table."
        f" 3. If your answer involves function calling you'll always run the function calling according to point 1 and 2 even if it's a previous answered questions."
        f" 4. If you're asked about tables or metadata or DB/DATABASE SCHEMA, just use the loaded DATABASE SCHEMA, don't query the Database. "
        f" 5. If for some reason, you're unable to call the function and answering with previous answer, just prefix it with {prev_answered}."
        f"\n\n--- DATABASE SCHEMA ---\n%db_schema%\n-----------------------."
    )


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

        try:
            conn = DuckdbUtil.get_connection_for(db)
            columns = conn.execute(query).fetchall()
        except Exception as err:
            print('Error while trying to connect AI Agent: '+str(err))
        
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

        return DataQueryAIAssistent.SYSTEM_INSTRUCTION.replace('%db_schema%',DB_SCHEMA)


    def get_system_instructions_from_ini_meta(self):

        DB_SCHEMA = self.DB_SCHEMA.replace('%total_table%',self.ini_tables)
        complete_instructions = DataQueryAIAssistent.SYSTEM_INSTRUCTION\
            .replace('%db_schema%',DB_SCHEMA)

        return complete_instructions


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
                
                is_prev_response = tool_calling.choices[0]\
                    .message.content.strip().startswith(DataQueryAIAssistent.prev_answered)

                if function_name == 'get_database_update':

                    self.ini_tables = Workspace.list_duck_dbs_with_fields(self.db_path, None)
                    self.messages = [{"role": "system", "content": self.get_system_instructions_from_ini_meta()}]
                    return { 'answer': 'final', 'result': "I'm now updated. Do you want to know something specific" }
                
                if function_name == "generate_sql_query_signal" or is_prev_response == True:

                    if(is_prev_response != True):
                        # Only handle function call if the actual function was called, in case of prev answer skip this
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
                    return { 'answer': 'final', **self.handle_response(client, model) }

                else:
                    print(f"Error: Unknown function name requested: {function_name}")
                    
            else:
                print("1. LLM decided not to call a function. Raw response:")
                print(tool_calling.choices[0].message.content)
                self.messages.append({ 'role': 'assistant', 'content': tool_calling.choices[0].message.content })
                #self.messages.append({ 'role': 'user', 'content': 'Thanks for the answer' })
                return { 'answer': 'intermediate', 'result': tool_calling.choices[0].message.content }

        except Exception as e:
            error = str(e) 
            print(f"\nInternal error occurred: {e}")
            traceback.print_exc()
            if error.lower().index('error code:') >= 0 and error.lower().index('400') >= 0:
                error = "Could not process your request, let's try again, what's your ask?"
                return { 'answer': 'intermediate', 'result': error }
            return { 'answer': 'intermediate', 'result': f"\nInternal error occurred: {e}" }
            

    def cloud_groq_call(self, user_prompt):

        api_key = env('GROQ_API_KEY')
        self.model = "llama-3.3-70b-versatile"
        self.client = Groq(api_key=api_key)

        client, model = self.client, self.model
        
        try:
            self.messages.append({"role": "user", "content": user_prompt})
            tool_calling = client.chat.completions.create(
                model=model,
                messages=self.messages,
                tools=ToolsDefinition.SQL_TOOL,
                stream=False
            )

            tool_calls = tool_calling.choices[0].message.tool_calls

            if tool_calls is not None:

                is_prev_response = False
                found_tool = tool_calls[0]
                function_name = found_tool.function.name
                function_args = found_tool.function.arguments
                function_args = json.loads(function_args)
                            
                print(f"1. LLM requested Tool Call: {function_name}")
                if function_args != None:
                    print(f"   -> Arguments1: {function_args.get('natural_language_question')}")
                    print(f"   -> Arguments2: {function_args.get('database_file')}")
                
                if tool_calling.choices[0].message.content:
                    is_prev_response = tool_calling.choices[0]\
                        .message.content.strip().startswith(DataQueryAIAssistent.prev_answered)
                
                if function_name == 'get_database_update':

                    self.ini_tables = Workspace.list_duck_dbs_with_fields(self.db_path, None)
                    self.messages = [{"role": "system", "content": self.get_system_instructions_from_ini_meta()}]
                    return { 'answer': 'final', 'result': "I'm now updated. Do you want to know something specific?" }
                
                if function_name == "generate_sql_query_signal" or is_prev_response == True:

                    if(is_prev_response != True):
                        # Only handle function call if the actual function was called, in case of prev answer skip this
                        function_output = self.generate_sql_query_signal(function_args.get('natural_language_question', ''))

                        db_file = function_args.get('database_file')
                        db_file = AIDataContentParser.extract_dbfile_pat(db_file)

                        self.db = db_file
                        self.messages.append(tool_calling.choices[0].message)
                        for tool_cal in tool_calls:
                            tool_cal.function.name
                            self.messages.append({
                                "role": "tool",
                                "content": json.dumps({"result": function_output}),
                                "tool_call_id": found_tool.id
                            })
                    
                    print("\n2. Sending Tool Output back to LLM for SQL Generation...")                
                    return { 'answer': 'final', 'db_file': db_file, **self.handle_response(client, model, 'Groq') }

                else:
                    print(f"Error: Unknown function name requested: {function_name}")
                    
            else:
                print("1. LLM decided not to call a function. Raw response:")
                print(tool_calling.choices[0].message.content)
                self.messages.append({ 'role': 'assistant', 'content': tool_calling.choices[0].message.content })
                #self.messages.append({ 'role': 'user', 'content': 'Thanks for the answer' })
                return { 'answer': 'intermediate', 'result': tool_calling.choices[0].message.content }

        except RateLimitError as e:
            print(f"\nInternal error occurred: {e}")
            return { 'answer': 'final', 'result': 'AI agent today\'s API call limit reached' }
        
        except BadRequestError as e:
                print(f"\nInternal error occurred: {e}")
                error = "Could not process your request, let's try again, what's your ask?"
                return { 'answer': 'final', 'result': error }
        
        except Exception as e:
            error = str(e) 
            print(f"\nInternal error occurred: {e}")
            return { 'answer': 'intermediate', 'result': f"\nInternal error occurred: {e}" }
            

    def handle_response(self, client: Mistral | Groq, model, strategy = 'mistral'):

        nl_to_sql_call = None
        if strategy == 'Groq':
            nl_to_sql_call = client.chat.completions.create(model=model, messages=self.messages,stream=False)
        if strategy == 'mistral':
            nl_to_sql_call = client.chat.complete(model=model, messages=self.messages,)

        actual_query = nl_to_sql_call.choices[0].message.content

        print('THE QUERY WILL BE:')
        print(actual_query)

        if actual_query.index('%%') >= 0:
            actual_query = actual_query.split('%%')[0]

        self.messages.append({ 'role': 'assistant', 'content': nl_to_sql_call.choices[0].message.content })
        sql_query = actual_query.strip()
        sql_query = AIDataContentParser.parse_sql(sql_query)
        if sql_query == None:
            return { 'result': 'Could not run the query. Can you refine it?' }
        print('Going to run the query')
        return { 
            'result': self.run_query(sql_query), 
            'fields': actual_query.lower().split('from')[0].split('select',1)[1],
            'actual_query': sql_query
        }



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
                        },
                        "database_file": {
                            "type": "string",
                            "description": "LLM Will extract from the Database Schema from DB-File. IMPORTANT: This MUST be extracted from the last line of each table corresponding to the one being queried."
                        },
                        "fields_in_query": {
                            "type": "string"
                        }
                    },
                    "required": ["natural_language_question"]
                }
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_database_update",
                "description": "This is especially for providing Metadata update capabilities in the implementation. "
                               "This will be called everytime the agent is asked to update itself. "
                               "The update ask can include terms like Medatata, Database or both",
            },
        },
    ]



class AIDataContentParser:

    @staticmethod
    def parse_sql(content):

        sql_query = None
        sql_pattern = r'(SELECT\s+.*?;)'
        sql_match = re.search(sql_pattern, content, re.IGNORECASE | re.DOTALL)

        if sql_match:
            sql_query = sql_match.group(1).strip()

        if sql_match == None and str(content).lower().strip().index('```sql') >= 0:
            sql_query = content.replace('```sql','').replace('```','').replace('\n','').strip()

        return sql_query


    @staticmethod
    def extract_dbfile_pat(content):
        
        pattern = r'%%(.+?)%%'
        matches = re.findall(pattern, content, re.DOTALL)
        
        return matches[0] if len(matches) > 0 else content





