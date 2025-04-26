from .TemplateNodeType import TemplateNodeType
from node_mapper.RequestContext import RequestContext
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
        self.bucket_url = data['bucketUrl']
        self.file_pattern = data['filePattern']

    def run(self):
        """
        Run the initial steps
        """
        super().run()
        self.check_bucket_url()
        print(f'Worked with value: {self.bucket_url} and {self.file_pattern}')

    def check_bucket_url(self):
        path_exists = os.path.exists(self.bucket_url)
        if not path_exists:
            error = 'Specified bucket url does not exists'
            error = {'componentId': self.component_id, 'error': error}
            self.context.add_exception('Bucket', error)
            self.context.emit_error(self, error)
        else:
            success = {'componentId': self.component_id}
            self.context.emit_success(self, success)
