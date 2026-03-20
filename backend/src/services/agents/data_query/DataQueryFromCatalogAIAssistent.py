import json
import re
from mistralai import Mistral
from os import getenv as env
from utils.duckdb_util import DuckdbUtil
from services.workspace.Workspace import Workspace
from groq import Groq, RateLimitError, BadRequestError
import traceback
from services.agents.AbstractAgent import AbstractAgent


class DataQueryFromCatalogAIAssistent(AbstractAgent):
    
    agent_factory = None

    def __init__(self, base_db_path, dbfile = ''):
        
        if (DataQueryFromCatalogAIAssistent.agent_factory == None):
            from services.agents import AgentFactory
            DataQueryFromCatalogAIAssistent.agent_factory = AgentFactory

        self.DB_SCHEMA = '''The database contains %total_table% tables:'''
        self.db_path = base_db_path if base_db_path.__contains__('/') else self.db_path
        self.db = f'{self.db_path}/{dbfile}'

        self.ini_tables = Workspace.list_duck_dbs_with_fields(self.db_path, None)
        self.messages = [{"role": "system", "content": self.get_system_instructions_from_ini_meta()}]

        self.model = None
        self.client = None
        self.chat_turns = []
        

    def cloud_groq_call(self, user_prompt):

        if self.client is None:
            api_key = env('GROQ_API_KEY')
            self.model = "llama-3.3-70b-versatile"
            self.client = Groq(api_key=api_key)

        client, model = self.client, self.model

        try:
            self.messages.append({"role": "user", "content": user_prompt})
            response = client.chat.completions.create(
                model=model,
                messages=self.messages,
                stream=False
            )

            content = response.choices[0].message.content
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
                Here are the matching catalog columns found:
                {context}"""})

            response = self.client.chat.completions.create(
                model=self.model,
                messages=self.messages,
                stream=False
            )

            content = response.choices[0].message.content
            self.messages.append({'role': 'assistant', 'content': content})
            return {'answer': 'final', 'result': content}

        except (RateLimitError, BadRequestError, Exception) as e:
            print(f"\nUnable to process request: {e}")
            return {'answer': 'intermediate', 'result': "Could not process your request, let's try again, what's your ask?"}