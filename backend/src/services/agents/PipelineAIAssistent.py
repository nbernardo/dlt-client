from mistralai import Mistral
from os import getenv as env
from groq import Groq, RateLimitError, BadRequestError
from services.agents.prompts.pipeline import SYSTEM_PROMPT

class PipelineAIAssistent:

    prev_answered = 'PREV_ANSWER:'
    generate_sql_query = "'generate_sql_query_signal'"

    def __init__(self):

        self.messages = [{"role": "system", "content": SYSTEM_PROMPT}]

        api_key = env('MISTRAL_API_KEY')
        self.model = "mistral-medium-2508"
        self.client = Mistral(api_key=api_key)
        self.chat_turns = []


    def cloud_mistral_call(self, user_prompt):
        """
            TODO: Implement accordingly and if needed to call Mistral
        """
            

    def cloud_groq_call(self, user_prompt):

        if self.client == None:
            api_key = env('GROQ_API_KEY')
            self.model = "llama-3.3-70b-versatile"
            self.client = Groq(api_key=api_key)

        client, model = self.client, self.model
        
        try:
            self.messages.append({"role": "user", "content": user_prompt})
            pipeline_create_request = client.chat.completions.create(
                model=model,
                messages=self.messages,
                stream=False
            )

            pipeline_content = pipeline_create_request.choices[0].message.content
            return { 'answer': 'final', 'result': pipeline_content }

        except RateLimitError as e:
            print(f"\nInternal error occurred: {str(e)}")
            return { 'answer': 'final', 'result': 'AI agent today\'s API call limit reached' }
        
        except BadRequestError as e:
                print(f"\nInternal error occurred: {str(e)}")
                error = "Could not process your request, let's try again, what's your ask?"
                return { 'answer': 'final', 'result': error }
        
        except Exception as e:
            error = str(e) 
            print(f"\nInternal error occurred: {error}")
            return { 'answer': 'intermediate', 'result': f"\nInternal error occurred: {error}" }
            
