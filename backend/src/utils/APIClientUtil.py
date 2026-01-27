class PaginateParam:
    start_param = None
    end_param = None
    batch_size: None
    start_record = 0

    def __init__(self, start_param, end_param, batch_size, start_record = 0):
        self.start_param = start_param
        self.end_param = end_param
        self.batch_size = batch_size if not(batch_size == '' or batch_size == None) else 100
        self.start_record = start_record


def get_api_auth_type(connection_type, secret):
    
    if connection_type == 'bearer-token':
        auth_config = "\nauth=BearerTokenAuth(token=secret['apiSettings']['apiTknValue'])"
        auth_strategy = 'from dlt.sources.helpers.rest_client.auth import BearerTokenAuth'
    
    if connection_type == 'api-key':
        apiKeyName = secret['apiSettings']['apiKeyName']
        apiKeyName = apiKeyName if not(apiKeyName == None or apiKeyName == '') else 'X-API-Key'
        auth_config = f"\nauth=APIKeyAuth(name='{apiKeyName}', api_key=secret['apiSettings']['apiKeyValue'], location='header')"
        auth_strategy = 'from dlt.sources.helpers.rest_client.auth import APIKeyAuth'

    return auth_config, auth_strategy
    

def parse_endpoint_details(endpoints):
    resource_names = [] 
    primary_keys = []
    data_selectors = [] 
    endpoints_params = []
    paginate_params = []

    for idx in range(len(endpoints['endPointsGroup']['apiEndpointPath'])):
        cur_endpoint = endpoints['endPointsGroup']
        endpoints_source = cur_endpoint['apiEndpointPath']
        endpoints_pk = cur_endpoint['apiEndpointPathPK']
        endpoints_ds = cur_endpoint['apiEndpointDS']
        endpoints_param = cur_endpoint['apiEndpointParams']

        resource_names.append(endpoints_source[idx])
        primary_keys.append(endpoints_pk[idx])
        data_selectors.append(endpoints_ds[idx])
        endpoints_params.append(endpoints_param[idx])

        paginate_params.append(
            None 
            if 
                cur_endpoint['paginationStartField'][idx] == '' 
            else
                PaginateParam(
                    cur_endpoint['paginationStartField'][idx],
                    cur_endpoint['paginationLimitField'][idx],
                    int(cur_endpoint['paginationRecPerPage'][idx]) if cur_endpoint['paginationRecPerPage'][idx] != '' else None
                )
        )
    
    return resource_names, primary_keys, data_selectors, endpoints_params, paginate_params


def test_api(
        namespace, base_url, connection_type = None, connection_name = None, 
        endpoints: dict = {}, authTknOrKey = None, apiKeyName = None
    ):

    from dlt.sources.helpers.rest_client import RESTClient
    api_client = RESTClient(base_url=base_url)
    auth = None

    if connection_name != None or authTknOrKey != None:

        secret = {}
        if connection_name != None:        
            from services.workspace.SecretManager import SecretManager
            SecretManager.ppline_connect_to_vault()
            secret = SecretManager.get_secret(namespace, connection_name)

        if connection_type == 'bearer-token':
            from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
            if connection_name:
                auth = BearerTokenAuth(token=secret['apiSettings']['apiTknValue'])
            else:
                auth = BearerTokenAuth(token=authTknOrKey)

        if connection_type == 'api-key':
            from dlt.sources.helpers.rest_client.auth import APIKeyAuth
            if connection_name:
                auth = APIKeyAuth(name=secret['apiSettings']['apiKeyName'], api_key=secret['apiSettings']['apiKeyValue'], location='header')
            else:
                auth = APIKeyAuth(name=f'{apiKeyName if not(apiKeyName == None or apiKeyName == '') else 'X-API-Key'}', api_key=authTknOrKey, location='header')


    if(auth != None):
        api_client = RESTClient(base_url=base_url, auth=auth)

    resource_names, \
    primary_keys, \
    data_selectors, \
    endpoints_params, \
    paginate_params = parse_endpoint_details(endpoints)

    def calls_api_endpoint(
            path: str, 
            data_selector: str = None, 
            primary_key: str = None,
            page_param: PaginateParam = None,
            path_params: dict = {}
        ):

        start, end = None, None
        page = page_param

        if page != None and page != '':
            start = page.start_record
            if str(page.start_param).__contains__('='):
                start_params = str(page.start_param).split('=')
                page.start_param = start_params[0].strip()
                start = start_params[1].strip()

            end = page.start_record + page.batch_size
        full_path = path if (page == None or page == '') else f'{path}?{page.start_param}={start}&{page.end_param}={end}'
        data = api_client.get(full_path, {} if path_params == None else path_params)

        if  data.status_code < 200 or data.status_code >= 300:
            return { '1-Endpoint': full_path, '2-StatusCode': data.status_code, 'data': [], '3-Success': False }

        if data_selector and data_selector != '':
            return { '1-Endpoint': full_path, '2-StatusCode': data.status_code, 'data': data.json().get(data_selector,[]), '3-Success': True }
        else:
            return { '1-Endpoint': full_path, '2-StatusCode': data.status_code, 'data': data.json(), '3-Success': True }

    api_responses = []
    params_tuple = zip(resource_names, endpoints_params, primary_keys, data_selectors, paginate_params)
    for path, path_params, pk , selector, page_params in params_tuple:
        if path == None: continue
        api_responses.append(calls_api_endpoint(path, selector, pk, page_params, path_params))
    
    return api_responses

