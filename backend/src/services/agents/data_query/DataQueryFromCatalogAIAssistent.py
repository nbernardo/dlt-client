import json
from os import getenv as env
import requests
import json
import time
import duckdb
from groq import Groq, RateLimitError, BadRequestError
from services.agents.AbstractAgent import AbstractAgent
from services.agents.data_query.prompts.data_query_with_catalog import CATALOG_AGENT_SYSTEM_PROMPT
from utils.metastore.DataCatalog import DataCatalog
from utils.metastore.PipelineMedatata import PipelineMedatata
from services.agents.data_query.query_mode.DataQueryOfflineMode import DataQueryOfflineMode

class DataQueryFromCatalogAIAssistent(AbstractAgent):
    
    agent_factory = None

    def __init__(self):
        self.db_path = ''
        api_key = env('GROQ_API_KEY')
        self.model = "llama-3.3-70b-versatile"
        self.client = Groq(api_key=api_key)

        if (DataQueryFromCatalogAIAssistent.agent_factory == None):
            from services.agents import AgentFactory
            DataQueryFromCatalogAIAssistent.agent_factory = AgentFactory

        self.messages = [{"role": "system", "content": CATALOG_AGENT_SYSTEM_PROMPT}]
        self.chat_turns = []

        self.offlineRequest = DataQueryOfflineMode()
        

    @staticmethod
    def format_schema_for_llm(records, database):
        grouped = {}
        for r in records:
            table = r.get('table_name', 'unknown_table')
            column = f"- {r.get('column_name')} ({r.get('data_type', 'text')})"
            grouped.setdefault(table, []).append(column)

        output = []
        for table, columns in grouped.items():
            output.append(f"Table: {database}.{table}\nColumns:\n" + "\n".join(columns))
        
        return "\n\n".join(output)



    def call_offline_model(self, user_prompt, namespace, language="PT"):
        #  ------------------------------------------------------------------------------------
        #  Orchestrate the AI agent request according user's request considering the defined tools 
        #  it uses either cloud LLM service (e.g. Groq, Mistral) provider  
        #  or a local offline infered LLM approach using any the available means (e.g. Ollama)
        #  ------------------------------------------------------------------------------------
        #   If using local infered model:
        #     - This requires having a local mode runnig (e.g. qwen2.5-coder:3b)
        #       - As recommendation on dev environment the model can run through Ollama -> (https://docs.ollama.com/quickstart)
        #       - For production it's recommender to use vLLM -> (https://docs.vllm.ai/en/latest/getting_started/quickstart/)
        
        [schema_context, db_file, start_time] = ['', '', time.time()]

        schema_context = DataCatalog.query_similarity_catalog(user_prompt)
        db_file = json.loads(schema_context[0]['dest_store'])['credentials']

        _, pipeline = schema_context[0]['pipeline'].split('_at_',1)
        pipeline_metadata = PipelineMedatata.get_pipeline_metadata(pipeline, namespace)
        schema_context = DataQueryFromCatalogAIAssistent.format_schema_for_llm(schema_context, pipeline_metadata[7])

        print(f"Took {(time.time() - start_time):.2f} sec to fetch semantic model + pipeline metadata")

        payload = {
            'model': 'qwen2.5-coder:3b', 'stream': False, "tools": ToolsDefinition.QUERY_TOOLS,
            'messages': [
                { 'role': 'system',  'content': "You are a helpful assistant. Use the provided tools to determine what function to call." },
                { 'role': 'user',  'content': f'User Request: {user_prompt}' }
            ]
        }

        response = requests.post("http://localhost:11434/api/chat", json=payload)
        message = response.json().get("message", {})

        function_name, arguments = '', ''
        if 'tool_calls' in message:
            for tool in message["tool_calls"]:
                [function_name, arguments] = [tool["function"]["name"], tool["function"]["arguments"]]

        elif 'content' in message:
            
            content = message.get('content')
            content = DataQueryFromCatalogAIAssistent._clean_function(message)

            tool_data = json.loads(content)
            [function_name, arguments] = [tool_data.get("name"), tool_data.get("arguments")]

        if function_name == 'execute_query':
            return self.offlineRequest.execute_query(user_prompt, schema_context, db_file, language)

        if function_name == 'check_catalog':
            return self.offlineRequest.check_catalog(user_prompt, schema_context, language)
    
        return { 'answer': 'final', 'result': 'Could not process your request, can you be more specific', 'analytics_query': True, 'success': True }
        



class ToolsDefinition:

    QUERY_TOOLS = [{
        'type': 'function',
        'function': {
            'name': 'execute_query',
            'description': 'Generate the final query only and executes a SQL query against the database and returns the results',
        }
    },
    {
        'type': 'function',
        'function': {
            'name': 'check_catalog',
            'description': 'Used when the users request is about the Data/Database/Tables schema, summarizes according to the found schema and respond to the user by explaining according to the schema'
        }
    }]