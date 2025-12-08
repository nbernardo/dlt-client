from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext


class Transformation(TemplateNodeType):
    """ Transformation type mapping class """

    def __init__(self, data: dict = None, context: RequestContext = None, component_id = None):
        """
        When called on the parse_node, this node type is only for transition to the next step, 
        (e.g. Load data to Duckdb) nothing gets exdcuted here, hence run returns Ture without any logic

        
        IMPORTANT: If transformation is used in an SQL Database data extraction, 
                   the Transformation step in UI is notified by SqlDBComponent
        """
        self.component_id = None
        self.context = None
        
        # This is to mark the transformation step as started only
        if(data is not None):
            if('componentId' in data):
                self.context = context
                self.component_id = data['componentId']
                self.context.emit_start(self, '')
        
         # When completed, a new temp instance is created, and the bellow 
         # setup is for it to notify the UI that the step was completed
        if(component_id != None):
            self.component_id = component_id
            self.context = context
        

    def run(self):
        return True
