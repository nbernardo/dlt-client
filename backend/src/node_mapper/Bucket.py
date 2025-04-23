from .TemplateNodeType import TemplateNodeType

class Bucket(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict):
        """
        Initialize the instance
        """
        self.bucket_url = data['bucketUrl']
        self.file_pattern = data['filePattern']

    def run(self):
        """
        Run the initial steps
        """
        super().run()
        print(f'Worked with value: {self.bucket_url} and {self.file_pattern}')
        