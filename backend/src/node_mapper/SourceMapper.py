from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
import logging
from services.admin.MappingTemplateService import MappingTemplateService
from utils.DestinationQueryUtil import DestinationQueryUtil as SQLDBSource
from utils.mappers.data_to_xml import render_xml
import os
from pathlib import Path
from services.admin.DataFlowService import DataFlowService
import json

class SourceMapper(TemplateNodeType):
    """
    Bucket type mapping class
    """

    operational = '%e2e_operational%'

    def __init__(self, data: dict, context: RequestContext = None, component_id = None):
        """ Initialize the instance """

        self.template = ''
        if len(data.keys()) == 0: return

        from controller.pipeline import BasePipeline
        file_dir = BasePipeline.folder+'/operations/mappedxml/'+data['namespace']
        os.makedirs(file_dir, exist_ok=True)

        try:

            self.context = context

            errors = self.context.operational_metadata.step_error.keys()
            if len(errors) > 0: return

            self.context.pipeline_metadata.dest_dw_name = SourceMapper.operational
            result = MappingTemplateService.get_template_records(data['namespace'], data['selectedTemplate'])
            query_result = SQLDBSource.query_sql_database(result['result'][0][3], data['namespace'], result['result'][0][2])
            
            template = result['result'][0][5]
            fields = query_result['normalized_fields']
            rows = query_result['result']

            xml_content = render_xml(template, fields, rows)
            (Path(file_dir) / 'output_example.xml').write_text(xml_content, encoding='utf-8')
            self.context.operational_metadata.output_file = f'{file_dir}/output_example.xml'

            record = { 
                'id_source': result['result'][0][4], 
                'source_type': 'XML', 
                'destination': json.dumps(str(data['destinationSetting']).replace("'",'"')) 
            }

            DataFlowService().create_flow([record], data['namespace'])
            
        except Exception as error:
            self.context.operational_metadata.step_error['SourceMapper'] = str(error)
            print(f'Error while creating XML file: {str(error)}')
            self.notify_failure_to_ui('SourceMapper',error)


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


