from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
from services.pipeline.DltPipeline import DltPipeline

class DLTCode(TemplateNodeType):
    """ DLTCode type mapping class """

    def __init__(self, data: dict, context: RequestContext):
        """ Initialize the instance """
        self.template = DltPipeline.get_dlt_code_template()

        # When instance is created only to get the template 
        # Nothing more takes place except for the template itself
        if data is None: return None
        if len(data.keys()) == 0: return None

        self.parse_to_literal = ['template_code']

        self.context = context
        self.component_id = data['componentId']

        self.context.emit_start(self, '')
        # template_code is mapped in /pipeline_templates/dlt_code.txt
        self.template_code = data['dltCode']

        self.notify_completion_to_ui()


    def run(self) -> None:
        """
        Run the initial steps
        """
        super().run()
        return True
