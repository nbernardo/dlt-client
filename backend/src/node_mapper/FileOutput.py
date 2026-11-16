from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
import os
from services.pipeline.DltPipeline import DltPipeline
from controller.file_upload import BaseUpload
from utils.BucketConnector import BucketConnector
from utils.pipeline import NodeType
from services.workspace.Workspace import Workspace
import logging

class FileOutput(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext = None, component_id = None):
        """
        Initialize the instance
        """
        if len(data.keys()) == 0: return
        
        try:
            self.context = context

            
        except Exception as error:
            self.notify_failure_to_ui('FileOutput',error)


    def run(self):
        """
        Run the initial steps
        """
        super().run()
        
        # No need to handle secrets here - template will handle secret retrieval
        # Following SQLDatabase pattern where secrets are handled in the pipeline template
        try:
            logging.info(f'Worked with value: {self.bucket_url} and {self.file_pattern}')
        except:
            ...


