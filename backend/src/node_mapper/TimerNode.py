import os
from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
from utils.logging.pipeline_logger_config import handle_pipeline_log

class TimerNode(TemplateNodeType):
    """ TimerNode type mapping class """

    def __init__(self, data: dict, context: RequestContext = None, component_id = None):
        """ Initialize the instance """

        self.template = ''
        if len(data.keys()) == 0: return

        logger = context.operational_metadata.logger

        try:
            self.context = context

            errors = self.context.operational_metadata.step_error.keys()
            if len(errors) > 0: return

            self.component_id = data['componentId']
            self.context.operational_metadata.timer['timeUnit'] = data['timeUnit']
            self.context.operational_metadata.timer['timeValue'] = data['timeValue']

            handle_pipeline_log(f'Setting the trigger times: {str(self.context.operational_metadata.timer)}', logger, False)
            self.notify_completion_to_ui()
            
        except Exception as error:
            handle_pipeline_log(f'Error while handling Timer: {str(errors)}', logger, False)
            self.context.operational_metadata.step_error['TimerNode'] = str(error)
            print(f'Error while creating XML file: {str(error)}')
            self.notify_failure_to_ui('TimerNode',error)


    def run(self):
        """ Run the initial steps """
        super().run()
        


