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

        if data['componentId'] in context.transaction_trace:
            return

        if context.operational_metadata.source_type == \
            OperationalPipelineMetadata.HOOK_TYPE.lower():
            if context.operational_metadata.transaction == True:

                logger = context.operational_metadata.logger
                context.success_emitted = True
                handle_pipeline_log(f'pipeline.success.conclusion', logger)

                pipeline_name = context.pipeline_name
                namespace = context.transaction_namespace
                connection_name = data['outDBconnectionName']
                source_file = context.operational_metadata.output_file

                self.json_to_database(pipeline_name, namespace, connection_name, source_file, data['destTable'])

                context.transaction_trace[data['componentId']] = True
                ...
            else:
                if len(context.operational_metadata.timer.keys()) > 0:
                    timer_settings = self.context.operational_metadata.timer
                    settings = { 'periodicity': None, 'ppline_label': context.pipeline_name, 'operational': True }

                    from services.workspace.Workspace import Workspace
                    settings = { 'settings': { **settings, **timer_settings } }
                    Workspace.handle_pipeline_schedule(settings, context.user, context.pipeline_name)
                ...
            context.emit_ppsuccess()
            return
            
        
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

    
    def json_to_database(self, pipeline_name, namespace, connection_name, source_file, target_table):
        import json
        import dlt
        from services.workspace.SecretManager import SecretManager
        from utils.DestinationQueryUtil import DestinationQueryUtil
        from sqlalchemy.engine import make_url

        secret = SecretManager.get_db_secret(namespace, connection_name, from_pipeline=True)
        connection_url = secret['connection_url']
                        
        db_engine = DestinationQueryUtil._detect_database_engine(connection_url)
        destination = None

        if db_engine == 'postgresql':
            destination = dlt.destinations.postgres(connection_url)
        else:
            destination = dlt.destinations.sqlalchemy(f'{connection_url}')

        @dlt.resource(
            table_name=target_table,
            write_disposition="append"
        )
        def read_json(file_path: str):
            with open(file_path, "r", encoding="utf-8") as f:
                yield json.load(f)

        pipeline = dlt.pipeline(
            pipeline_name=f'operational_{namespace}_{pipeline_name}', 
            destination=destination, 
            dataset_name=make_url(connection_url).database
        )

        load_info = pipeline.run(read_json(source_file))
        print(load_info)