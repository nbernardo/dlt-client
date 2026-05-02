import json
import json
import time
import duckdb
from groq import Groq, RateLimitError, BadRequestError
from services.agents.AbstractAgent import AbstractAgent
from .CommonContent import CommonContent
from os import getenv as env

class DataQueryCloudMode(AbstractAgent):
    
    agent_factory = None

    def __init__(self, language = 'PT'):
        self.client = Groq(api_key=env('GROQ_API_KEY'))
        self.model_sql = "llama-3.3-70b-versatile"
        self.model_explain = "llama-3.1-8b-instant"
        system_prompt = CommonContent.system_prompt(language)

        self.query_messages = [
            {"role": "system", "content": f'{system_prompt} Output only the code according to Duckdb SQL Syntax. {CommonContent.no_query_generation_rule}'}
        ]
        
        self.explain_messages = [
            {"role": "system", "content": f'{system_prompt} Just clearly answer the question by providing the explanation according Schema. Use markdown'}
        ]
        

    def execute_query(self, user_prompt, schema_context, db_file, exception_content = None, retry = 0):
        complement = CommonContent.complement

        ask_correction = ''
        if exception_content != None:
            ask_correction = f"You've generated the bellow wrong query, also follow the exception. "
            ask_correction = f"Please correct things according to table mapping\n\n{exception_content}"

        [full_response, start_time] = ["", time.time()]
        user_message = {"role": "user", "content": f'Retry: {retry}\n{ask_correction}Context: {schema_context}\n\nUser Request: {user_prompt}\n\n{complement}'}
        
        self.query_messages.append(user_message)

        completion = self.client.chat.completions.create(
            model=self.model_sql, messages=self.query_messages, temperature=0
        )

        full_response = completion.choices[0].message.content

        duration = time.time() - start_time
        print(f"\nTotal time taken to get model SQL query: {duration:.2f} seconds")

        db = duckdb.connect(db_file)

        if full_response.__contains__('CLARIFY:') or full_response.__contains__('CLARIFY:'):
            return { 'answer': 'final', 'result': full_response, 'analytics_query': True, 'success': False, 'clarify': True }

        try:
            query = DataQueryCloudMode._clean_query(full_response)
            result = db.sql(query)
            records = result.fetchall()
            json_output = json.dumps(
                list(DataQueryCloudMode._stream_as_json(getattr(result, 'columns'), records)),
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

        complement = CommonContent.explain_complement
        user_message = {'role': 'user', 'content': f'Schema: {schema_context}\n\nUser Request: {user_prompt}\n\n{complement}'}

        self.explain_messages.append(user_message)

        completion = self.client.chat.completions.create(
            model=self.model_explain, messages=self.explain_messages, temperature=0
        )

        response = completion.choices[0].message.content
        start_time = time.time()

        print(f"Total time taken to geenrate the explamation: {(time.time() - start_time):.2f} seconds")
        return { 'answer': 'schema-clarification', 'result': response, 'analytics_query': True, 'success': True }