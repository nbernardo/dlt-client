from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
import os
from services.pipeline.DltPipeline import DltPipeline
from controller.file_upload import BaseUpload


class Bucket(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext = None, component_id = None):
        """
        Initialize the instance
        """
        
        try:
            self.context = context
            self.bucket_path_prefix = ""
            self.template_type = 'non_database_source'
            self.template = DltPipeline.get_s3_no_auth_template()

            if(context.is_cloud_url != True):
                self.template = DltPipeline.get_template()\
                                    if context.transformation == None else DltPipeline.get_transform_template()
            
            self.template = self.parse_destination_string(self.template)

            # When instance is created only to get the template 
            # Nothing more takes place except for the template itself
            if data is None: return None
            if len(data.keys()) == 0: return None

            self.namespace = data['namespace']

            self.component_id = data['componentId']
            user_folder = BaseUpload.upload_folder+'/'+context.user

            self.context.emit_start(self, '')
            # bucket_url is mapped in /pipeline_templates/simple.txt
            self.bucket_url = data['bucketUrl'] if int(data['bucketFileSource']) == 2 else user_folder

            # file_pattern is mapped in /pipeline_templates/simple.txt
            file_path = data['filePattern'].split('.')
            file_pattern_name = ''.join(file_path[0:-1])+'*.'+file_path[-1]
            self.file_pattern = file_pattern_name

            # To point to the 
            self.bucket_file_source = data['bucketFileSource']
            if(str(data['bucketFileSource']).endswith('.csv') and not str(data['bucketFileSource']).endswith('*.csv')):
                self.bucket_file_source = data['bucketFileSource'].replace('.csv','*.csv')
            # primary_key is mapped in /pipeline_templates/simple.txt and simple_transform_field.txt
            self.primary_key = data.get('primaryKey', 'UNDEFINED')
            
        except Exception as error:
            self.notify_failure_to_ui('Bucket',error)


    def run(self):
        """
        Run the initial steps
        """
        super().run()
        print(f'Worked with value: {self.bucket_url} and {self.file_pattern}')
        return self.check_bucket_url()


    def check_bucket_url(self):
        is_cloud_url = str(self.bucket_url).replace(' ','').__contains__('://')
        path_exists = os.path.exists(self.bucket_url)
        file_exists = os.path.exists(self.bucket_url+'/'+str(self.file_pattern).replace('*',''))
        error = None
        if not path_exists and is_cloud_url == False:
            error = 'Specified bucket url does not exists'
        if not file_exists and is_cloud_url == False:
            error = f'Files with specified patterns "{self.file_pattern}" does not exists'
        else:
            # Notify the UI that this step completed successfully
            return self.notify_completion_to_ui()

        if is_cloud_url:
            backet_path_pieces = str(self.bucket_url).split('/',3)

            if len(backet_path_pieces) > 3:
                self.bucket_path_prefix = backet_path_pieces[3]

        return self.notify_failure_to_ui('Bucket',error)
