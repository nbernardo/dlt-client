class PaginateParam:
    start_param = None
    end_param = None
    batch_size: None
    start_record = 0

    def __init__(self, start_param, end_param, batch_size, start_record = 0):
        self.start_param = start_param
        self.end_param = end_param
        self.batch_size = batch_size
        self.start_record = start_record


def get_api_auth_type(connection_type, secret):
    
    if connection_type == 'bearer-token':
        auth_config = "\nauth=BearerTokenAuth(token=secret['apiSettings']['apiTknValue'])"
        auth_strategy = 'from dlt.sources.helpers.rest_client.auth import BearerTokenAuth'
    
    if connection_type == 'api-key':
        auth_config = f"\nauth=APIKeyAuth(name='{secret['apiSettings']['apiKeyName']}', api_key=secret['apiSettings']['apiKeyValue'], location='header')"
        auth_strategy = 'from dlt.sources.helpers.rest_client.auth import APIKeyAuth'

    return auth_config, auth_strategy
    

def parse_endpoint_details(endpoints_id, endpoints):
    resource_names = [] 
    primary_keys = []
    data_selectors = [] 
    endpoints_params = []
    paginate_params = []

    for endpoint_id in endpoints_id:
        resource_names.append(endpoints.get(f'apiEndpointPath{endpoint_id}',None))
        primary_keys.append(endpoints.get(f'apiEndpointPathPK{endpoint_id}',None))
        data_selectors.append(endpoints.get(f'apiEndpointDS{endpoint_id}',None))
        endpoints_params.append(endpoints.get(f'apiEndpointDS{endpoint_id}',None))
        paginate_params.append(endpoints.get(f'apiEndpointDS{endpoint_id}',None))
    
    return resource_names, primary_keys, data_selectors, endpoints_params, paginate_params


def test_api(namespace, base_url, connection_type = None, connection_name = None, endpoints: dict = {}):

    from dlt.sources.helpers.rest_client import RESTClient
    api_client = RESTClient(base_url=base_url)
    auth = None
    endpoints_id = [
        path.replace('apiEndpointPath','') 
        for path in endpoints.keys() if (path.startswith('apiEndpointPath') and not(path.__contains__('PK')))
    ]


    if connection_name != None:
        
        from services.workspace.SecretManager import SecretManager
        SecretManager.ppline_connect_to_vault()
        secret = SecretManager.get_secret(namespace, connection_name)

        if connection_type == 'bearer-token':
            from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
            auth = BearerTokenAuth(token=secret['apiSettings']['apiTknValue'])

        if connection_type == 'api-key':
            from dlt.sources.helpers.rest_client.auth import APIKeyAuth
            auth = APIKeyAuth(name=secret['apiSettings']['apiKeyName'], api_key=secret['apiSettings']['apiKeyValue'], location='header')


    if(auth != None):
        api_client = RESTClient(base_url=base_url, auth=auth)

    resource_names, \
    primary_keys, \
    data_selectors, \
    endpoints_params, \
    paginate_params = parse_endpoint_details(endpoints_id, endpoints)

    def calls_api_endpoint(
            path: str, 
            data_selector: str = None, 
            primary_key: str = None,
            page_param: PaginateParam = None,
            path_params: dict = {}
        ):

        start, end = None, None
        page = path_params

        if page != None and page != '':
            start = page.start_record
            end = page.start_record + page.batch_size

        full_path = path if (page == None or page == '') else f'{path}?{page.start_param}={start}&{page.end_param}={end}'
        data = api_client.get(full_path, {} if path_params == None else path_params)

        if data_selector and data_selector != '':
            return data.json().get(data_selector,[])
        else:
            return data.json()

    api_responses = []
    params_tuple = zip(resource_names, endpoints_params, primary_keys, data_selectors, paginate_params)
    for path, path_params, pk , selector, page_params in params_tuple:
        api_responses.append(calls_api_endpoint(path, selector, pk, page_params, path_params))
    
    return api_responses

