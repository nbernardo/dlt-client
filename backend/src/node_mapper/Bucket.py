from .TemplateNodeType import TemplateNodeType
from node_mapper.RequestContext import RequestContext


class Bucket(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext = None):
        """
        Initialize the instance
        """
        self.bucket_url = data['bucketUrl']
        self.file_pattern = data['filePattern']
        self.context = context

    def run(self):
        """
        Run the initial steps
        """
        super().run()
        print(f'Worked with value: {self.bucket_url} and {self.file_pattern}')
