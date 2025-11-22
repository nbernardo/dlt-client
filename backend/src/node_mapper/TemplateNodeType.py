from controller.RequestContext import RequestContext

class TemplateNodeType:
    """
    Class to serve as supper for node types
    """
    def __init__(self, data: dict, context: RequestContext):
        self.template = None
        self.context = context
        self.component_id = None
        self.parse_to_literal = []


    def run(self):
        """ Method to be implemented by each node type """


    def notify_completion_to_ui(self):
            success = {'componentId': self.component_id}
            self.context.emit_success(self, success)

    def notify_failure_to_ui(self, type, err, add_exception = True):
            error = {'message': f'{err}', 'componentId': self.component_id}
            if(add_exception):
                self.context.add_exception(type, error)
            self.context.emit_error(self, error)
            
            return self.context.FAILED