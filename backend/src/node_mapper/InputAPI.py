from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
from services.pipeline.DltPipeline import DltPipeline
from controller.file_upload import BaseUpload
from services.workspace.SecretManager import SecretManager

class InputAPI(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext = None):
        """
        Initialize the instance
        """
        
        try:

            self.template = DltPipeline.get_api_templete()
            
            # When instance is created only to get the template 
            # Nothing more takes place except for the template itself
            if data is None: return None
            if len(data.keys()) == 0: return None

            self.parse_to_literal = ['paginate_params']

            self.context = context
            self.connection_name = data['connectionName']
            self.base_url = data['baseUrl']
            self.component_id = data['componentId']
            self.namespace = data['namespace']

            SecretManager.ppline_connect_to_vault()
            secret = SecretManager.get_secret(self.namespace, self.connection_name)

            self.resource_names = secret['apiSettings']['endPointsGroup']['apiEndpointPath']
            self.primary_keys = secret['apiSettings']['endPointsGroup']['apiEndpointPathPK']
            self.data_selectors = secret['apiSettings']['endPointsGroup']['apiEndpointDS']
            self.paginate_params = self.get_paginate_params(secret).replace('"','')
            
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
