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
from utils.logging.pipeline_logger_config import handle_pipeline_log

class SourceMapper(TemplateNodeType):
    """
    SourceMapper type mapping class
    """

    operational = '%e2e_operational%'

    def __init__(self, data: dict, context: RequestContext = None, component_id = None):
        """ Initialize the instance """

        self.template = ''
        if len(data.keys()) == 0: return

        from controller.pipeline import BasePipeline
        file_dir = BasePipeline.folder+'/operations/mappedxml/'+data['namespace']
        os.makedirs(file_dir, exist_ok=True)

        from services.pipeline.DltPipeline import DltPipeline
        logger = DltPipeline.get_pipeline_logger(context)
        context.operational_metadata.logger = logger

        try:
            self.context = context

            errors = self.context.operational_metadata.step_error.keys()
            if len(errors) > 0: return

            self.component_id = data['componentId']

            self.context.pipeline_metadata.dest_dw_name = SourceMapper.operational
            result = MappingTemplateService.get_template_records(data['namespace'], data['selectedTemplate'])
            handle_pipeline_log(f'Fetching pipeline details', logger, False)
            query_result = SQLDBSource.query_sql_database(result['result'][0][3], data['namespace'], result['result'][0][2])
            handle_pipeline_log(f'Data source records fetch ran', logger, False)

            template = result['result'][0][5]
            fields = query_result['normalized_fields']
            rows = query_result['result']

            handle_pipeline_log(f'Total of records to be written to the output: {len(rows)}', logger, False)
            xml_content = render_xml(template, fields, rows)
            (Path(file_dir) / 'output_example_2.xml').write_text(xml_content, encoding='utf-8')
            self.context.operational_metadata.output_file = f'{file_dir}/output_example_2.xml'

            if context.operational_metadata.transaction == True:
                handle_pipeline_log(f'Data Mapping run successfull for {len(rows)} records', logger, False)
            else:
                # In case .transaction is not True then it's pipeline creation flow
                record = { 
                    'pipeline': context.pipeline_name,
                    'id_source': result['result'][0][4], 
                    'source_type': 'XML', 
                    'destination': json.dumps(str(data['destinationSetting']).replace("'",'"')),
                    'node_connections': json.dumps(str(list(context.connections)).replace("'",'"')),
                    'node_list': json.dumps(str(context.node_params).replace("'",'"'))
                }
                DataFlowService().create_flow([record], data['namespace'])
                handle_pipeline_log(f'Created the pipeline instance record: {len(rows)}', logger, False)

            self.notify_completion_to_ui()
            
        except Exception as error:
            handle_pipeline_log(f'Error while generating the output data source mapper output: {str(errors)}', logger, True)
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


