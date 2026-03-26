import json
import re
from os import getenv as env
from services.workspace.Workspace import Workspace
from groq import Groq, RateLimitError, BadRequestError
import traceback
from services.agents.AbstractAgent import AbstractAgent
from services.agents.data_query.prompts.data_query_with_catalog import CATALOG_AGENT_SYSTEM_PROMPT

class DataQueryFromCatalogAIAssistent(AbstractAgent):
    
    agent_factory = None

    def __init__(self, base_db_path, dbfile = ''):
        
        api_key = env('GROQ_API_KEY')
        self.model = "llama-3.3-70b-versatile"
        self.client = Groq(api_key=api_key)

        if (DataQueryFromCatalogAIAssistent.agent_factory == None):
            from services.agents import AgentFactory
            DataQueryFromCatalogAIAssistent.agent_factory = AgentFactory

        self.DB_SCHEMA = '''The database contains %total_table% tables:'''
        self.db_path = base_db_path if base_db_path.__contains__('/') else self.db_path
        self.db = f'{self.db_path}/{dbfile}'

        self.ini_tables = Workspace.list_duck_dbs_with_fields(self.db_path, None)
        self.messages = [{"role": "system", "content": CATALOG_AGENT_SYSTEM_PROMPT}]

        self.model = None
        self.client = None
        self.chat_turns = []
        

    def check_catalog(self, user_prompt):

        client, model = self.client, self.model

        try:
            self.messages.append({"role": "user", "content": user_prompt})
            response = client.chat.completions.create(
                model=model,
                messages=self.messages,
                stream=False,
                tools=[ToolsDefinition.QUERY_TOOLS[1]]
            )

            content = response.choices[0].message.content
            from utils.metastore.DataCatalog import DataCatalog
            results = DataCatalog.query_similarity_catalog([user_prompt])

            if not results:
                return {'answer': 'final', 'result': 'No matching columns found in the catalog.'}

            content = response.choices[0].message.content
            self.messages.append({'role': 'assistant', 'content': results})
            return {'answer': 'final', 'result': content}

        except (RateLimitError, BadRequestError, Exception) as e:
            print(f"\nUnable to process request: {e}")
            return {'answer': 'intermediate', 'result': "Could not process your request, let's try again, what's your ask?"}
        

    def execute_query(self, user_prompt):

        try:

            from utils.metastore.DataCatalog import DataCatalog
            results = DataCatalog.query_similarity_catalog([user_prompt])

            if not results:
                return {'answer': 'final', 'result': 'No matching columns found in the catalog.'}

            context = "\n".join([
                f"pipeline={r['pipeline']} table={r['table_name']} "
                f"column={r['original_column_name']} type={r['data_type']} "
                f"concept={r['semantic_concept']} description={r['description']}"
                for r in results
            ])

            self.messages.append({"role": "user", "content": f"""
                The user asked: {user_prompt}
                Here are the matching catalog columns found, please generate the respecive SQL query according to the Database engine:
                {context}"""})

            response = self.client.chat.completions.create(
                model=self.model,
                messages=self.messages,
                stream=False
            )

            query = response.choices[0].message.content
            # TODO: Implement query running against the destination by using the configuration (e.g. secrets)

            return {'answer': 'final', 'result': ''}

        except (RateLimitError, BadRequestError, Exception) as e:
            print(f"\nUnable to process request: {e}")
            return {'answer': 'intermediate', 'result': "Could not process your request, let's try again, what's your ask?"}
        


    def user_request_routing(self, user_prompt):

        self.messages.append({"role": "user", "content": user_prompt})

        tool_calling = self.client.chat.completions.create(
            model=self.model,
            messages=self.messages,
            stream=False,
            tools=ToolsDefinition.QUERY_TOOLS
        )

        tool_calls = tool_calling.choices[0].message.tool_calls

        if tool_calls is not None:

            found_tool = tool_calls[0]
            function_name = found_tool.function.name
            function_args = found_tool.function.arguments
            function_args = json.loads(function_args)

            if function_name == 'execute_query':
                return self.execute_query(user_prompt)
            
            if function_name == 'check_catalog':
                return self.check_catalog(user_prompt)
        
        else:
            content = tool_calling.choices[0].message.content
            print("1. LLM decided not to call a function. Raw response:")
            print(content)
            return { 'answer': 'intermediate', 'result': content }




class ToolsDefinition:

    QUERY_TOOLS = [{
        'type': 'function',
        'function': {
            'name': 'execute_query',
            'description': 'Executes a SQL query against the source database and returns the results',
            'parameters': {
                'type': 'object',
                'properties': {
                    "sql": {
                        'type': 'string',
                        'description': 'The SQL query to execute'
                    },
                    'pipeline': {
                        'type': 'string',
                        'description': 'The pipeline name identifying which source database to query'
                    }
                },
                "required": ['sql','pipeline']
            }
        }
    },
    {
        'type': 'function',
        'function': {
            'name': 'check_catalog',
            'description': 'Query the catalog and returns the details according to the user query'
        }
    }]