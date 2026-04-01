import json
import requests
import json
import time
import duckdb
from groq import Groq, RateLimitError, BadRequestError
from services.agents.AbstractAgent import AbstractAgent
from .CommonContent import CommonContent

class DataQueryOfflineMode(AbstractAgent):
    
    agent_factory = None

    def __init__(self, language = 'PT'):
        self.system_message = CommonContent.system_prompt(language)
        

    def execute_query(self, user_prompt, schema_context, db_file, exception_content = None, retry = 0):
    
        ask_correction = ''
        if exception_content != None:
            ask_correction = f"You've generated the bellow wrong query, also follow the exception. "
            ask_correction = f"Please correct things according to table mapping\n\n{exception_content}"

        payload = {
            'model': 'qwen2.5-coder:3b', 'stream': True,
            'system': f'Retry: {retry}\n{self.system_message} Output only the code. {CommonContent.no_query_generation_rule}',
            'prompt': f'{ask_correction}Context: {schema_context}\n\nUser Request: {user_prompt}\n\n{CommonContent.complement}',
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

        if full_response.__contains__('CLARIFY:') or full_response.__contains__('CLARIFY:'):
            return { 'answer': 'final', 'result': full_response, 'analytics_query': True, 'success': False, 'clarify': True }

        try:
            query = DataQueryOfflineMode._clean_query(full_response)
            result = db.sql(query)
            records = result.fetchall()
            json_output = json.dumps(
                list(DataQueryOfflineMode._stream_as_json(getattr(result, 'columns'), records)),
                default=str
            )
        
        except Exception as err:
            print('Error on running Analytics query: ', str(err))
            if retry == 4:
                return { 'answer': 'final', 'result': 'Was not able to process the request, let\'s try again.', 'analytics_query': True, 'success': False }

            print(f'----------------------- Retrying request execution ({retry + 1}x) -----------------------')
            retry_payload = f'The failed query: {query}\n\nException from the failed query: {str(err)}'
            self.execute_query(user_prompt, schema_context, db_file, retry_payload, retry + 1)
    
        return { 'answer': 'final', 'result': json_output, 'analytics_query': True, 'success': True }
        


    def check_catalog(self, user_prompt, schema_context):

        payload = {
            'model': 'qwen2.5-coder:1.5b', 'stream': False,
            'messages': [
                {'role': 'system', 'content': f'{self.system_message} Explain the following data schema clearly using markdown output'},
                {'role': 'user', 'content': f'Schema: {schema_context}\n\nUser Request: {user_prompt}\n\n{CommonContent.explain_complement}'}
            ],
        }

        start_time = time.time()

        with requests.post("http://localhost:11434/api/chat", json=payload) as response:

            print(f"Total time taken to geenrate the explamation: {(time.time() - start_time):.2f} seconds")
            return { 'answer': 'schema-clarification', 'result': response.json()['message']['content'], 'analytics_query': True, 'success': True }
        
        response = "Couldn't process your request, can you please refine of make it more clear"
        return { 'answer': 'final', 'result': response, 'analytics_query': True, 'success': True }
