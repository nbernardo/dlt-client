from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
import os
from services.pipeline.DltPipeline import DltPipeline
from controller.file_upload import BaseUpload


class Bucket(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext = None):
        """
        Initialize the instance
        """
        self.template = DltPipeline.get_template()
        
        # When instance is created only to get the template 
        # Nothing more takes place except for the template itself
        if data is None: return None

        self.context = context
        self.component_id = data['componentId']
        user_folder = BaseUpload.upload_folder+'/'+context.user

        self.context.emit_start(self, '')
        # bucket_url is mapped in /pipeline_templates/simple.txt
        self.bucket_url = data['bucketUrl'] if int(data['bucketFileSource']) == 2 else user_folder
        # file_pattern is mapped in /pipeline_templates/simple.txt
        self.file_pattern = data['filePattern']
        # To point to the 
        self.bucket_file_source = data['bucketFileSource']

    def run(self):
        """
        Run the initial steps
        """
        super().run()
        print(f'Worked with value: {self.bucket_url} and {self.file_pattern}')
        return self.check_bucket_url()

    def check_bucket_url(self):
        path_exists = os.path.exists(self.bucket_url)
        file_exists = os.path.exists(self.bucket_url+'/'+str(self.file_pattern).replace('*',''))
        error = None
        if not path_exists:
            error = 'Specified bucket url does not exists'
        if not file_exists:
            error = f'Files with specified patterns "{self.file_pattern}" does not exists'
        else:
            # Notify the UI that this step completed successfully
            return self.notify_completion_to_ui()
            
        return self.notify_failure_to_ui('Bucket',error)
