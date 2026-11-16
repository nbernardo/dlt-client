from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
from utils.pipeline import NodeType
import logging
from services.admin.MappingTemplateService import MappingTemplateService
from services.workspace.SecretManager import SecretManager
from utils.DestinationQueryUtil import DestinationQueryUtil as SQLDBSource

class SourceMapper(TemplateNodeType):
    """
    Bucket type mapping class
    """

    operational = '%e2e_operational%'

    def __init__(self, data: dict, context: RequestContext = None, component_id = None):
        """
        Initialize the instance
        """
        self.template = ''
        if len(data.keys()) == 0: return

        try:
            #selectedTemplate
            #templateType
            self.context = context
            self.template = ''
            self.context.pipeline_metadata.dest_dw_name = SourceMapper.operational
            result = MappingTemplateService.get_template_records(data['namespace'], data['selectedTemplate'])
            secret = SecretManager.get_db_secret(data['namespace'], result['result'][0][2])
            query_result = SQLDBSource.query_sql_database(result['result'][0][3], data['namespace'], result['result'][0][2])
            return result
            
        except Exception as error:
            self.notify_failure_to_ui('SourceMapper',error)


    def run(self):
        """
        Run the initial steps
        """
        super().run()
        
        # No need to handle secrets here - template will handle secret retrieval
        # Following SQLDatabase pattern where secrets are handled in the pipeline template
        try:
            logging.info(f'Worked with value: {self.bucket_url} and {self.file_pattern}')
        except:
            ...


