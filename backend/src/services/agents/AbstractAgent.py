class AbstractAgent:
    def __init__(self):
        self.username = None
        self.namespace = None

    
    @staticmethod
    def _clean_function(msg): 
        if str(msg).__contains__('```'):
            return msg.get('content').replace('```json','').replace('```sql','').replace('`','').replace('\n','')
        return msg


    @staticmethod
    def _clean_query(query): return query.replace('```sql','').replace('`','').strip()
                

    @staticmethod
    def _stream_as_json(cols, recs):
        for row in recs: yield dict(zip(cols, row))



def test_groq_connection():
    from os import getenv as env
    from groq import Groq

    api_key = env('GROQ_API_KEY')
    return Groq(api_key=api_key)