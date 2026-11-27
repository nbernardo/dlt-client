from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext, OperationalPipelineMetadata
from os import getenv as env
from utils.logging.pipeline_logger_config import handle_pipeline_log

class FormatConvertion(TemplateNodeType):
    """ FormatConvertion type mapping class """

    XML = 'XML'
    JSON = 'JSON'

    def __init__(self, data: dict, context: RequestContext, component_id = None):
        """ Initialize the instance """

        # When instance is created only to get the template 
        # Nothing more takes place except for the template itself
        if data is None: return None

        self.context = context
        self.component_id = data['componentId']
        self.context.emit_start(self, '')

        if data['componentId'] in context.transaction_trace:
            return

        if context.operational_metadata.source_type == \
            OperationalPipelineMetadata.HOOK_TYPE.lower():
            if context.operational_metadata.transaction == True:
                
                source_type = data.get('sourceType')
                target_type = data.get('targetType')
                file_path, file_name = context.operational_metadata.input_file.rsplit('/', 1)
                source_file = context.operational_metadata.input_file
                dest_file = f'{file_path}/{file_name.rsplit('.',1)[0]}.json'

                context.operational_metadata.output_file = dest_file

                logger = context.operational_metadata.logger
                if source_type == FormatConvertion.XML and target_type == \
                    FormatConvertion.JSON:

                    result, error = xml_to_json_file(xml_path=source_file, json_path=dest_file)

                    if result == False:
                        handle_pipeline_log(error, logger, True)
                    else:
                        handle_pipeline_log(f"Converted XML ({source_file}) to JSON ({dest_file})", logger, False)

                context.transaction_trace[data['componentId']] = True
                ...
            else:
                ...
            self.notify_completion_to_ui()
            return
            ...


    def run(self) -> None:
        """ Run the initial steps """
        return True


import orjson

import xmltodict
def xml_to_json_file(xml_path: str, json_path: str, list_tags: tuple = ()):
    
    try:
        with open(xml_path, "rb") as xml_file:
            parsed_data = xmltodict.parse(xml_file, force_list=list_tags)

        with open(json_path, "wb") as json_file:
            json_file.write(orjson.dumps(parsed_data, option=orjson.OPT_INDENT_2))

        return True, ''
    except Exception as error:
        return False, str(error)
