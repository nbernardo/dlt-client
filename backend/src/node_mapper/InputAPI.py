from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext, OperationalPipelineMetadata
from services.pipeline.DltPipeline import DltPipeline
from services.workspace.SecretManager import SecretManager
import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
from utils.pipeline import NodeType
import json
from services.admin.DataFlowService import DataFlowService
import os
from services.pipeline.DltPipeline import DltPipeline
from utils.logging.pipeline_logger_config import handle_pipeline_log
from cryptography.fernet import Fernet
import base64
import hashlib
from node_mapper.FileOutput import FileOutput

ENCRYPTION_KEY = os.environ.get("SQLITE_DB_ENCRYPTION_KEY").encode('utf-8')
ENCRYPTION_KEY = hashlib.sha256(ENCRYPTION_KEY).digest()

cipher = Fernet(base64.urlsafe_b64encode(ENCRYPTION_KEY))

_ODOO_COLUMN_TYPES = {
    "char", "text", "html", "integer", "float", "monetary", "boolean",
    "date", "datetime", "selection", "many2one", "binary", "reference",
}

class InputAPI(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict = None, context: RequestContext = None, component_id = None):
        """
        Initialize the instance
        """
        if data == None: return
        try:
            self.context = context
            self.template_type = None
            self.component_id = component_id
            self.component_id = data['componentId']

            if data['componentId'] in context.transaction_trace:
                return

            # In case it's a hook kind of API then it does not go to regular flow            
            if data['apiType'] == OperationalPipelineMetadata.HOOK_TYPE:
                if context.operational_metadata.transaction == True:
                    #context.operational_metadata.input_file
                    if context.operational_metadata.input_file:
                        source_file = context.operational_metadata.input_file
                        folder = 'received'
                        # TODO: 
                        #   1 - Offload this resposibility to the UI FileOutput component node
                        #   2 - Make sure that at this level only happens if the Hook is of type that accepts upload auto-managed
                        result, error_msg = FileOutput({}).s3_upload(None, context.transaction_namespace, source_file, source_file.rsplit('/')[-1], folder)

                        if result == False:
                            handle_pipeline_log(error_msg, context.operational_metadata.logger, True)
                        else:
                            handle_pipeline_log(f'Archiving the received file: {source_file}', context.operational_metadata.logger, False)
                    pass
                else:
                    self.handle_hook_request(data, context)
                    # context.operational_metadata.transaction = True
                    context.operational_metadata.source_type = data['apiType'].lower()
                    self.notify_completion_to_ui()
                return


            # When instance is created only to get the template 
            # Nothing more takes place except for the template itself
            if data is None: return None
            if len(data.keys()) == 0: return None

            self.context.pipeline_metadata.source_type = NodeType.API_SOURCE

            # Every field in this list will be parse and considerd as pure python code
            self.parse_to_literal = ['paginate_params','auth_config','auth_strategy','endpoints_params']
            
            # Bellow fields (connection_name, base_url, component_id, namespace)
            # are mapped in /pipeline_templates/api.txt
            self.connection_name = data['connectionName']
            self.base_url = data['baseUrl']
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
            
            template = DltPipeline.get_api_templete()

            if secret.get('apiSettings', {}).get('isOdoo'):

                template = DltPipeline.get_api_templete('api_odoo.txt')
                db = secret.get('apiSettings').get('odooDB')
                pwd = secret.get('apiSettings').get('odooPassword')
                usr = secret.get('apiSettings').get('odooUser')
                tables = self.source_tables.strip('[]').replace("'",'').replace(' ','').split(',')
                test = self.test_odoo_connection(self.base_url, tables, self.primary_keys, db, usr, pwd)
                
                if len(test.get('failed_tables', [])) > 0 or test.get('error'):
                    url_call_error = test.get('result') if test.get('error') else\
                        f'Error found about {str(test.get('failed_tables'))} tables'
                    
                    return self.notify_failure_to_ui('InputAPI',url_call_error)
            else:
                url_status, url_call_error = InputAPI.check_base_url_exists(secret['apiSettings'])
                if url_status == False:
                    return self.notify_failure_to_ui('InputAPI',url_call_error)
            
            self.template = self.parse_destination_string(template,'\n')
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

        from utils.APIClientUtil import get_api_auth_type

        connection_type = None
        auth_config = ''
        auth_strategy = '\n'

        if 'apiAuthType' in secret['apiSettings']:

            connection_type = secret['apiSettings']['apiAuthType']
            if connection_type == 'bearer-token' or connection_type == 'api-key':
                auth_config , auth_strategy = get_api_auth_type(connection_type, secret)

        return auth_config, auth_strategy


    def check_base_url_exists(apiSettings):
        """ Checks if a base URL exists and is reachable. """
        url = apiSettings['apiBaseUrl']
        authType = apiSettings['apiAuthType'] if ('apiAuthType' in apiSettings) else ''
        firstEndpoint = apiSettings['endPointsGroup']['apiEndpointPath'][0]
        clean_url = f'{url}{firstEndpoint}'.replace('http://','').replace('https://','').replace('//','/')
        url = str(url).split('://')[0] + '://' + clean_url

        try:
            test_call = False
            if authType == 'api-key':
                response = requests.head(url, timeout=5, headers={ 'X-API-Key': apiSettings['apiKeyValue'] })
                test_call = True 

            if authType == 'bearer-token':
                response = requests.head(url, timeout=5, headers={ 'Authorization': f'Bearer {apiSettings['apiTknValue']}'})    
                test_call = True

            if test_call == False:
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
        

    def odoo_call(self, url: str, service: str, method: str, args: list):
        try:
            resp = requests.post(
                f"{url}/jsonrpc",
                json={ "jsonrpc": "2.0", "method": "call", "params": {"service": service, "method": method, "args": args}, "id": 1, },
            )
        except Exception as err:
            return { 'error': True, 'result': { 'data': { 'message': str(err) } } }
        
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            return { 'error': True, 'result': data["error"] }
        return { 'error': False, 'result': data["result"] }


    def get_column_fields(self, url: str, db: str, uid: int, password: str, table: str):
        fields_result = self.odoo_call(
            url, "object", "execute_kw", [db, uid, password, table, "fields_get", [], {"attributes": ["type", "store"]}],
        )
        if fields_result.get('error'):
            return fields_result

        fields_meta = fields_result['result']
        columns = {
            name: meta["type"]
            for name, meta in fields_meta.items()
            if meta["type"] in _ODOO_COLUMN_TYPES and meta.get("store")
        }
        return { 'error': False, 'result': columns }


    def test_odoo_connection(self, url: str, tables: list, pks: list, db: str, user: str, password: str):
        db_name, result, failed_tables = db, {}, []
        auth_result = self.odoo_call(url, "common", "login", [db_name, user, password])
        if auth_result.get('error') or not auth_result.get('result'):
            return auth_result

        for table in tables:

            fields_result = self.get_column_fields(url, db_name, auth_result['result'], password, table)
            if fields_result.get('error'):
                result[table] = fields_result
                continue
            field_names = list(fields_result['result'].keys())

            rows = self.odoo_call(
                url, "object", "execute_kw",
                [db_name, auth_result['result'], password, table, "search_read", [[]], { "fields": field_names, "limit": 1 }],
            )
            if rows.get('error'):
                result[table] = rows
                failed_tables.append(table)
                continue

            result[table] = rows['result'][0] if rows['result'] else None

        return { 'error': False, 'result': result, 'failed_tables': failed_tables }

    
    def handle_hook_request(self, data: dict = None, context: RequestContext = None):
            self.template = '' # Template set is important as the flow still considers some regular (no operationl) pipeline checking 
            # In case .transaction is not True then it's pipeline creation flow

            context.pipeline_metadata.dest_dw_name = OperationalPipelineMetadata.OP
            logger = DltPipeline.get_pipeline_logger(context)
            context.operational_metadata.logger = logger

            creds_obj: dict = data.get('creds',{})
            creds = { "user": creds_obj.get('user'), "pass": creds_obj.get('pass') }
            creds = cipher.encrypt(str(creds).encode("utf-8"))

            record = { 
                'pipeline': context.pipeline_name,
                'destination': json.dumps(str(data.get('destinationSetting','')).replace("'",'"')),
                'node_connections': json.dumps(str(list(context.connections)).replace("'",'"')),
                'node_list': json.dumps(str(context.node_params).replace("'",'"')),
                'creds': creds,
                'hook': data.get('hookEndpoint', ''),
            }
            DataFlowService().create_hooked_flow([record], context.transaction_namespace)
            handle_pipeline_log(f'Created the pipeline instance record: ', logger, False)
            context.transaction_trace[data['componentId']] = True