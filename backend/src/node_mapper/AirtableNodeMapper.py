from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
from services.pipeline.DltPipeline import DltPipeline

class AirtableNodeMapper(TemplateNodeType):
    """
    Node mapper for Airtable data source nodes.
    Maps UI node configuration to Airtable template placeholders.
    """
    
    def __init__(self, data: dict, context: RequestContext, component_id=None):
        """
        Initialize the Airtable node mapper instance.
        
        Args:
            data: Node configuration from UI containing base_id, table_names, connection_name
            context: Request context with user namespace and pipeline configuration
            component_id: UI component identifier for progress notifications
        """
        try:
            self.context = context
            self.template_type = 'airtable'
            self.component_id = component_id
            
            # Load Airtable template
            template = DltPipeline.get_airtable_template()
            self.template = self.parse_destination_string(template, '\n')
            
            # When instance is created only to get the template 
            # Nothing more takes place except for the template itself
            if data is None: return None
            if len(data.keys()) == 0: return None
            
            # Fields that should be converted to Python literals (not quoted strings)
            self.parse_to_literal = ['table_names']
            
            # Extract node configuration from UI data
            # Bellow fields (base_id, table_names, connection_name, namespace)
            # are mapped in /pipeline_templates/airtable.txt
            self.base_id = data.get('baseId', data.get('base_id', ''))
            self.table_names = data.get('tableNames', data.get('table_names', []))
            self.connection_name = data.get('connectionName', data.get('connection_name', ''))
            self.component_id = data.get('componentId', component_id)
            self.namespace = data.get('namespace', context.user)
            
            # Set source_tables for metadata section (used by SQL destinations)
            # Convert table names to string format for metadata
            if isinstance(self.table_names, list):
                self.source_tables = str(self.table_names)
            else:
                self.source_tables = f"['{self.table_names}']"
            
            self.context.emit_start(self, '')
            
            # Notify UI that node initialization completed successfully
            self.notify_completion_to_ui()
            
        except Exception as error:
            self.notify_failure_to_ui('AirtableNodeMapper', error)
    
    def run(self):
        """
        Run the initial steps and validate configuration.
        """
        super().run()
        
        # Validate required fields
        if not self.base_id:
            error_msg = 'Airtable base_id is required'
            self.notify_failure_to_ui('AirtableNodeMapper', error_msg)
            return self.context.FAILED
        
        if not self.table_names or (isinstance(self.table_names, list) and len(self.table_names) == 0):
            error_msg = 'At least one table name is required'
            self.notify_failure_to_ui('AirtableNodeMapper', error_msg)
            return self.context.FAILED
        
        if not self.connection_name:
            error_msg = 'Airtable connection_name is required for secret retrieval'
            self.notify_failure_to_ui('AirtableNodeMapper', error_msg)
            return self.context.FAILED
        
        print(f'Initialized Airtable source with base: {self.base_id}, tables: {self.table_names}')
        
        return True
