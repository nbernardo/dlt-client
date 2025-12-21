from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
from services.pipeline.DltPipeline import DltPipeline
from services.workspace.SecretManager import SecretManager
import requests
from requests.exceptions import ConnectionError, Timeout, RequestException

class InputAPI(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext = None, component_id = None):
        """
        Initialize the instance
        """
        
        try:
            self.context = context
            self.template_type = None
            template = DltPipeline.get_api_templete()
            self.template = self.parse_destination_string(template)
            self.component_id = component_id
            
            # When instance is created only to get the template 
            # Nothing more takes place except for the template itself
            if data is None: return None
            if len(data.keys()) == 0: return None

            # Every field in this list will be parse and considerd as pure python code
            self.parse_to_literal = ['paginate_params','auth_config','auth_strategy','endpoints_params']
            
            # Bellow fields (connection_name, base_url, component_id, namespace)
            # are mapped in /pipeline_templates/api.txt
            self.connection_name = data['connectionName']
            self.base_url = data['baseUrl']
            self.component_id = data['componentId']
            self.namespace = data['namespace']

            SecretManager.ppline_connect_to_vault()
            secret = SecretManager.get_secret(self.namespace, self.connection_name)

            # Bellow fields (resource_names, primary_keys, data_selectors, endpoints_params)
            # are mapped in /pipeline_templates/api.txt
            self.resource_names = secret['apiSettings']['endPointsGroup']['apiEndpointPath']
            self.source_tables = str(secret['apiSettings']['endPointsGroup']['apiEndpointPath']).replace('/','')
            self.primary_keys = secret['apiSettings']['endPointsGroup']['apiEndpointPathPK']
            self.data_selectors = secret['apiSettings']['endPointsGroup']['apiEndpointDS']
            self.endpoints_params = str(secret['apiSettings']['endPointsGroup']['apiEndpointParams'])\
                                        .replace('\\t','')\
                                        .replace('\t','')\
                                        .replace("'",'')
            
            url_status, url_call_error = InputAPI.check_base_url_exists(secret['apiSettings']['apiBaseUrl'])
            if url_status == False:
                return self.notify_failure_to_ui('InputAPI',url_call_error)
            
            # Bellow field (paginate_params) is mapped in /pipeline_templates/api.txt
            self.paginate_params = self.get_paginate_params(secret).replace('"','')
            
            # Bellow field (auth_config) is mapped in /pipeline_templates/api.txt
            self.auth_config, self.auth_strategy = self.parse_connection_strategy(secret)
            
            self.notify_completion_to_ui()

        except Exception as error:
            self.notify_failure_to_ui('InputAPI',error)


    def run(self):
        """
        Run the initial steps
        """
        super().run()
        return True
    

    def get_paginate_params(self, secret):
        paginate_start_field = secret['apiSettings']['endPointsGroup']['paginationStartField']
        paginate_limit_field_name = secret['apiSettings']['endPointsGroup']['paginationLimitField']
        paginate_rec_per_page = secret['apiSettings']['endPointsGroup']['paginationRecPerPage']

        paginate_params = zip(paginate_start_field, paginate_limit_field_name, paginate_rec_per_page)
        pagination_params = []

        for start, end, batch in paginate_params:
            if len(start) > 0:
                pagination_params.append(f"PaginateParam('{start}','{end}',{batch})")
            else:
                pagination_params.append(None)
        
        return str(pagination_params)
    

    def parse_connection_strategy(self, secret):

        connection_type = None
        auth_config = ''
        auth_strategy = '\n'

        if 'apiAuthType' in secret['apiSettings']:

            connection_type = secret['apiSettings']['apiAuthType']

            if connection_type == 'bearer-token':
                auth_config = "\nauth=BearerTokenAuth(token=secret['apiSettings']['apiTknValue'])"
                auth_strategy = 'from dlt.sources.helpers.rest_client.auth import BearerTokenAuth'
            
            if connection_type == 'api-key':
                auth_config = f"\nauth=APIKeyAuth(name='{secret['apiSettings']['apiKeyName']}', api_key=secret['apiSettings']['apiKeyValue'], location='header')"
                auth_strategy = 'from dlt.sources.helpers.rest_client.auth import APIKeyAuth'

        return auth_config, auth_strategy


    def check_base_url_exists(url):
        """ Checks if a base URL exists and is reachable. """
        try:
            response = requests.head(url, timeout=5)  # Set a timeout to prevent indefinite waiting
            if response.status_code == 200:
                return True, None
            else:
                return False, f'URL {url} returned status code: {response.status_code}'
        except ConnectionError:
            return False, f'Connection error: Could not connect to {url}'
        except Timeout:
            return False, f'Timeout error: Request to {url} timed out.'
        except RequestException as e:
            return False, f'An unexpected error occurred for {url}: {e}'