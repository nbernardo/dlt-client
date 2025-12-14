from utils.cache_util import DuckDBCache
from datetime import datetime
from os import getenv as env

def get_request_ip(request):

    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ',' in ip:
        ip = ip.split(',')[0].strip()
    return ip


def handle_conversasion_turn_limit(request, namespace):

    today_date = datetime.now().strftime('%d/%m/%Y')
    ip = get_request_ip(request)
    k_plus_ip = f'{today_date}/{ip}/{namespace}'
    k = f'{today_date}/{namespace}'

    total_conversation_turns = DuckDBCache.get(k_plus_ip)
    total_conversation_turns1 = DuckDBCache.get(k)

    if DuckDBCache.get(k_plus_ip) == None:
        DuckDBCache.set(k_plus_ip,1)
        DuckDBCache.set(k,1)

    else:
        total_conversation_turns = int(total_conversation_turns)
        total_conversation_turns1 = int(total_conversation_turns1)
        daily_limit = int(env('CONVERSATION_TURN_LIMIT'))
            
        if(((total_conversation_turns >= daily_limit) or (total_conversation_turns1 >= daily_limit))\
            and not(daily_limit == -1)):
            return { 'error': True, 'result': { 'result': 'Exceeded the free Daily limit' }, 'exceed_limit': True }
            
        DuckDBCache.set(k_plus_ip,total_conversation_turns + 1)
        DuckDBCache.set(k,total_conversation_turns1 + 1)

    return {}