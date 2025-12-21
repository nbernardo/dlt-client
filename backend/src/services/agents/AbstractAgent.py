class AbstractAgent:
    def __init__(self):
        self.username = None
        self.namespace = None


def test_groq_connection():
    from os import getenv as env
    from groq import Groq

    api_key = env('GROQ_API_KEY')
    return Groq(api_key=api_key)