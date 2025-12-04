from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext

class DatabaseOutput(TemplateNodeType):
    """ Bucket type mapping class """

    def __init__(self, data: dict, context: RequestContext):
        """ Initialize the instance """

        self.parse_to_literal = ['outdb_secret_name']

        # When instance is created only to get the template 
        # Nothing more takes place except for the template itself
        if data is None: return None

        self.context = context
        self.component_id = data['componentId']
        self.output_dest_name = data['databaseName']

        self.context.emit_start(self, '')
        # outdb_secret_name is mapped to any pipeline template (in the /pipeline_templates folder) 
        # and this takes place according to the logic implemented on the 
        # TemplateNodeType.parse_destination_string which is inherited by ant nodeType (e.g. DatabaseOutput)
        self.outdb_secret_name = data['outDBconnectionName']


    def run(self) -> None:
        """ Run the initial steps """
        return True