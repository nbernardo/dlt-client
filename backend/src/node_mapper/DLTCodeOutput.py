from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
from services.pipeline.DltPipeline import DltPipeline
from services.workspace.SecretManager import SecretManager
import re

class DLTCodeOutput(TemplateNodeType):
    """ DLTCodeOutput type mapping class """

    def __init__(self, data: dict, context: RequestContext, component_id = None):
        """ Initialize the instance """
        
        self.context = context
        self.template_type = None
        template = DltPipeline.get_dlt_code_template()
        self.template = self.parse_destination_string(template)

        # When instance is created only to get the template 
        # Nothing more takes place except for the template itself
        if data is None: return None
        if len(data.keys()) == 0: return None

        self.parse_to_literal = ['destination_settings','secret_manager_import']
        self.secret_manager_import = ''

        self.context = context
        self.component_id = data['componentId']

        self.context.emit_start(self, '')

        import_stmnt = r'\n*\s*import dlt\s*\n*'

        # destination_settings is mapped in /pipeline_templates/dlt_code.txt
        self.destination_settings = re.sub(import_stmnt,'',data['dltCode'])
        no_transformation = True if self.context.transformation == None else False

        if len(context.sql_destinations) > 0:
            if(self.destination_settings.__contains__('destination=dlt.destinations.bigquery')):
                self.parse_to_literal.append('table_format')
                self.table_format = 'table_format="native"'

            self.secret_manager_import = ',SecretManager'
            if no_transformation and not(self.context.code_source):
                self.destination_settings = self.destination_settings.replace('\n','\n    ')

        self.ppline_dest_table = 'no_table_name'

        referenced_secrets = self.parse__secrets(data['namespace'])
        if len(referenced_secrets) > 0:
            if self.context.code_source:
                self.context.additional_secrets.append(str(referenced_secrets).replace('[','').replace(']',''))
            else:
                n = '\n    ' if no_transformation else '\n'
                self.destination_settings = f"namespace = '{data['namespace']}'{n}secret_names = {referenced_secrets}{n}__secrets = SecretManager.referencedSecrets(namespace, secret_names){n}{self.destination_settings}"

        self.notify_completion_to_ui()


    def run(self) -> None:
        """ Run the initial steps """
        super().run()
        return True
    

    def parse__secrets(self, namespace, path = 'main/db'):
        import re
        pattern = r"__secrets\.(\w+)"
        referenced_secrets = re.findall(pattern, self.destination_settings)
        secrets_list: list = SecretManager.list_secrets_by_path(namespace, path)
        return [
            secret for secret in referenced_secrets\
                  if secrets_list.__contains__(secret)
        ]
