from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
import os


class Bucket(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext = None):
        """
        Initialize the instance
        """
        self.context = context
        self.component_id = data['componentId']

        self.context.emit_start(self, '')

        self.bucket_url = data['bucketUrl']
        self.file_pattern = data['filePattern']

    def run(self):
        """
        Run the initial steps
        """
        super().run()
        print(f'Worked with value: {self.bucket_url} and {self.file_pattern}')
        return self.check_bucket_url()

    def check_bucket_url(self):
        path_exists = os.path.exists(self.bucket_url)
        if not path_exists:
            error = 'Specified bucket url does not exists'
            error = {'componentId': self.component_id, 'message': error}
            self.context.add_exception('Bucket', error)
            self.context.emit_error(self, error)
            return self.context.FAILED
        else:
            success = {'componentId': self.component_id}
            self.context.emit_success(self, success)
