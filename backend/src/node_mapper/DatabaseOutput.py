from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext, OperationalPipelineMetadata
from utils.pipeline import NodeType
from os import getenv as env
from utils.logging.pipeline_logger_config import handle_pipeline_log

class DatabaseOutput(TemplateNodeType):
    """ Bucket type mapping class """

    def __init__(self, data: dict, context: RequestContext, component_id = None):
        """ Initialize the instance """

        self.parse_to_literal = ['outdb_secret_name']

        # When instance is created only to get the template 
        # Nothing more takes place except for the template itself
        if data is None: return None

        self.context = context
        self.component_id = data['componentId']
        self.context.emit_start(self, '')

        if context.operational_metadata.source_type == \
            OperationalPipelineMetadata.HOOK_TYPE:
            if context.operational_metadata.transaction == True:
                
                logger = context.operational_metadata.logger
                context.success_emitted = True
                handle_pipeline_log(f'pipeline.success.conclusion', logger)

                ...
            else:
                ...
            context.emit_ppsuccess()
            return
            ...
        
        self.output_dest_name = data['databaseName'].replace('-','_')
        # outdb_secret_name is mapped to any pipeline template (in the /pipeline_templates folder) 
        # and this takes place according to the logic implemented on the 
        # TemplateNodeType.parse_destination_string which is inherited by ant nodeType (e.g. DatabaseOutput)
        self.outdb_secret_name = data['outDBconnectionName']
        self.context.pipeline_metadata.destination_secret = self.outdb_secret_name
        self.context.pipeline_metadata.destination_type = NodeType.SQL_DEST + f'({data['dbengine']})'


    def run(self) -> None:
        """ Run the initial steps """
        return True
    

    def archive_file(self):
        url = env('S3_URL')
        user = env('ACCESS_KEY')
        _pass = env('SECRET_KEY')
        ...