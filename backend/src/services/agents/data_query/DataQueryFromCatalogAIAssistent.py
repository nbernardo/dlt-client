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

        self.DB_SCHEMA = '''The database contains %total_table% tables:'''
        self.messages = [{"role": "system", "content": CATALOG_AGENT_SYSTEM_PROMPT}]

        self.model = None
        self.client = None
        self.chat_turns = []
        

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
        """ 
            Generates and execute the database query using local infered model
                - This requires having a local mode runnig (e.g. qwen2.5-coder:3b)
                    - As recommendation on dev environment the model can run through Ollama -> (https://docs.ollama.com/quickstart)
                    - For production it's recommender to use vLLM -> (https://docs.vllm.ai/en/latest/getting_started/quickstart/)
        """

        start_time = time.time()

        adjust_query = f"{user_prompt}. If not needed, don't fetch additional columns extept for table_name and column_name"
        schema_context = DataCatalog.query_similarity_catalog(adjust_query)
        db_file = json.loads(schema_context[0]['dest_store'])['credentials']

        _, pipeline = schema_context[0]['pipeline'].split('_at_',1)
        pipeline_metadata = PipelineMedatata.get_pipeline_metadata(pipeline, namespace)
        schema_context = DataQueryFromCatalogAIAssistent.format_schema_for_llm(schema_context, pipeline_metadata[7])

        duration = time.time() - start_time
        print(f"Total time taken to get semantic model + pipeline metadata: {duration:.2f} seconds")

        system_message = 'Você é um Analista de Dados.' if language == 'PT' else 'You are a Data Analyst.'        
        complement = 'Rules:\n- Only output final SQL\n- No explanation\n- Use provided schema only'
        complement += '\n- Match the columns name appropriately as per the table it relates\n- Be precise and minimal\n\n'

        payload = {
            #"model": "qwen2.5-coder:1.5b",
            'model': 'qwen2.5-coder:3b', 'stream': True,
            'system': f'{system_message} Output only the code.',
            'prompt': f'Context: {schema_context}\n\nUser Request: {user_prompt}\n\n{complement}',
        }

        [full_response, start_time] = ["", time.time()]

        with requests.post("http://localhost:11434/api/generate", json=payload, stream=True) as response:

            duration = time.time() - start_time
            print(f"Total time taken to get model SQL query: {duration:.2f} seconds")

            for line in response.iter_lines():
                if line:
                    chunk = json.loads(line.decode('utf-8'))
                    token = chunk.get("response", "")
                    print(token, end="", flush=True)
                    
                    full_response += token
                    if chunk.get("done"): break

        db = duckdb.connect(db_file)

        result = db.sql(DataQueryFromCatalogAIAssistent._clean_query(full_response))
        records = result.fetchall()
        json_output = json.dumps(
            list(
                DataQueryFromCatalogAIAssistent._stream_as_json(getattr(result, 'columns'), records)
            )
        )

        return { 'answer': 'final', 'result': json_output, 'analytics_query': True }

        

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
        

    @staticmethod
    def _clean_query(query): return query.replace('```sql','').replace('`','').strip()


    @staticmethod
    def _stream_as_json(cols, recs):
        for row in recs: yield dict(zip(cols, row))



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