from mistralai import Mistral
from os import getenv as env
from groq import Groq, RateLimitError, BadRequestError
from services.agents.prompts.pipeline import SYSTEM_PROMPT
from services.agents.AbstractAgent import AbstractAgent

class PipelineAIAssistent(AbstractAgent):

    agent_factory = None
    prev_answered = 'PREV_ANSWER:'
    generate_sql_query = "'generate_sql_query_signal'"

    def __init__(self):

        if (PipelineAIAssistent.agent_factory == None):
            from services.agents import AgentFactory
            PipelineAIAssistent.agent_factory = AgentFactory

        self.messages = [{"role": "system", "content": SYSTEM_PROMPT}]

        #api_key = env('MISTRAL_API_KEY')
        self.model = "mistral-medium-2508"
        self.client = None #Mistral(api_key=api_key)
        self.chat_turns = []


    def cloud_mistral_call(self, user_prompt):
        """
            TODO: Implement accordingly and if needed to call Mistral
        """
            

    def cloud_groq_call(self, user_prompt):

        if self.client == None:
            api_key = env('GROQ_API_KEY')
            self.model = "openai/gpt-oss-120b"
            self.client = Groq(api_key=api_key)
            self.client

        client, model = self.client, self.model
        
        try:
            self.messages.append({"role": "user", "content": user_prompt})
            pipeline_create_request = client.chat.completions.create(
                model=model,
                messages=self.messages,
                stream=False,
                temperature=0.3,
                max_tokens=1000
            )

            pipeline_content = pipeline_create_request.choices[0].message.content
           
            if str(pipeline_content).__contains__(PipelineAIAssistent.agent_factory.agents_type_list['data']):
                last_user_prompt = self.messages[-1]
                data_query_agent_reply = self.call_data_query_agent(last_user_prompt['content'])
                return data_query_agent_reply
            else:
                return { 'answer': 'final', 'result': pipeline_content }

        except RateLimitError as e:
            print(f"\nI'm unable to unrestand your request: {str(e)}")
            return { 'answer': 'final', 'result': 'AI agent today\'s API call limit reached' }
        
        except BadRequestError as e:
                print(f"\nI'm unable to unrestand your request: {str(e)}")
                error = "Could not process your request, let's try again, what's your ask?"
                return { 'answer': 'final', 'result': error }
        
        except Exception as e:
            error = str(e) 
            print(f"\nI'm unable to unrestand your request: {error}")
            return { 'answer': 'intermediate', 'result': f"\nCould not process your request, let's try again, what's your ask? {error}" }
            

    def call_data_query_agent(self, message):
        return PipelineAIAssistent.\
            agent_factory.\
            get_data_agent(self.namespace).cloud_groq_call(message)