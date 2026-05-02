import json
from os import getenv as env
import requests
import json
import time
import duckdb
from groq import Groq, RateLimitError, BadRequestError
from services.agents.AbstractAgent import AbstractAgent
from utils.metastore.DataCatalog import DataCatalog
from utils.metastore.PipelineMedatata import PipelineMedatata
from services.agents.data_query.query_mode.DataQueryOfflineMode import DataQueryOfflineMode
from services.agents.data_query.query_mode.DataQueryCloudMode import DataQueryCloudMode

class DataQueryFromCatalogAIAssistent(AbstractAgent):
    
    agent_factory = None

    def __init__(self):
        self.db_path = ''
        self.model = "llama-3.1-8b-instant"
        self.client = Groq(api_key=env('GROQ_API_KEY'))
        # TODO: Use a environment variables
        self.llm_cloud_mode = True

        if (DataQueryFromCatalogAIAssistent.agent_factory == None):
            from services.agents import AgentFactory
            DataQueryFromCatalogAIAssistent.agent_factory = AgentFactory

        self.offlineRequest = DataQueryOfflineMode(language='PT')
        self.cloudRequest = DataQueryCloudMode(language='PT')
        

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

        tool_calling = {}
        if self.llm_cloud_mode:
            tool_calling = self.get_cloud_mode_tool(user_prompt)
        else:
            payload = {
                'model': 'qwen2.5-coder:3b', 'stream': False, "tools": ToolsDefinition.QUERY_TOOLS,
                'messages': [
                    { 'role': 'system',  'content': "You are a helpful assistant. Use the provided tools to determine what function to call." },
                    { 'role': 'user',  'content': f'User Request: {user_prompt}' }
                ]
            }
            tool_calling = self.get_offline_mode_tool(payload)

        function_name, arguments = tool_calling.get('function_name'), tool_calling.get('arguments')
        request_mode = self.offlineRequest if self.llm_cloud_mode == False else self.cloudRequest

        if function_name == 'execute_query':
            return request_mode.execute_query(user_prompt, schema_context, db_file)
        if function_name == 'check_catalog':
            return request_mode.check_catalog(user_prompt, schema_context)
    
        return { 'answer': 'final', 'result': 'Could not process your request, can you be more specific', 'analytics_query': True, 'success': True }
        

    def get_cloud_mode_tool(self, user_prompt):
        
        try:
            completion = self.client.chat.completions.create(
                model=self.model, tools=ToolsDefinition.QUERY_TOOLS, tool_choice="auto", 
                stream=False, extra_body={"disable_tool_validation": True},
                messages=[
                    {
                        "role": "system", 
                        "content": (
                            "You are a helpful assistant. Use the provided tools to determine what function to call."
                            "IMPORTANT: Both 'check_catalog' and 'execute_query' tool takes NO parameters. Call it with empty arguments: {}"
                        )
                     },
                    {"role": "user", "content": user_prompt}
                ],
            )

            response_message = completion.choices[0].message
            tool_calls = response_message.tool_calls

            if tool_calls:
                call = tool_calls[0]
                return { 'function_name': call.function.name, 'arguments': json.loads(call.function.arguments) }
            
            return {"content": response_message.content}

        except Exception as e:
            print(f"Error in tool calling: {e}")
            return {}


    def get_offline_mode_tool(self, payload):

        [function_name, arguments] = ['','']
        response = requests.post("http://localhost:11434/api/chat", json=payload)
        message = response.json().get("message", {})

        if 'tool_calls' in message:
            for tool in message["tool_calls"]:
                [function_name, arguments] = [tool["function"]["name"], tool["function"]["arguments"]]

        elif 'content' in message:
            content = DataQueryFromCatalogAIAssistent._clean_function(message)

            tool_data = json.loads(content)
            [function_name, arguments] = [tool_data.get("name"), tool_data.get("arguments")]

        return { 'function_name': function_name, 'arguments': arguments }




class ToolsDefinition:

    QUERY_TOOLS = [{
        'type': 'function',
        'function': {
            'name': 'execute_query',
            'description': 'Generate the final query only and executes a SQL query against the database and returns the results',
        },
    },
    {
        'type': 'function',
        'function': {
            'name': 'check_catalog',
            'description': 'Used when the users request is about the Data/Database/Tables schema, summarizes according to the found schema and respond to the user by explaining according to the schema'
        },
    }]