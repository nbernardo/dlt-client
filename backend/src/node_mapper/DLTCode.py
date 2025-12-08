from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
from services.pipeline.DltPipeline import DltPipeline
from services.workspace.SecretManager import SecretManager

class DLTCode(TemplateNodeType):
    """ DLTCode type mapping class """

    def __init__(self, data: dict, context: RequestContext):
        """ Initialize the instance """
        
        self.context = context
        self.template_type = None
        template = DltPipeline.get_dlt_code_template()
        self.template = self.parse_destination_string(template)

        # When instance is created only to get the template 
        # Nothing more takes place except for the template itself
        if data is None: return None
        if len(data.keys()) == 0: return None

        self.parse_to_literal = ['template_code','secret_manager_import']
        self.secret_manager_import = ''
        
        if len(self.context.sql_destinations) > 0:
            self.secret_manager_import = ',SecretManager'


        self.context = context
        self.component_id = data['componentId']

        self.context.emit_start(self, '')
        # template_code is mapped in /pipeline_templates/dlt_code.txt
        self.template_code = data['dltCode']

        referenced_secrets = self.parse__secrets(data['namespace'])
        if len(referenced_secrets) > 0:
            self.template_code = f"""\nnamespace = '{data['namespace']}'\nsecret_names = {referenced_secrets}\n__secrets = referencedSecrets(namespace, secret_names)\n{self.template_code}\n\n"""

        self.notify_completion_to_ui()


    def run(self) -> None:
        """ Run the initial steps """
        super().run()
        return True
    

    def parse__secrets(self, namespace, path = 'main/db'):
        import re
        pattern = r"__secrets\.(\w+)"
        referenced_secrets = re.findall(pattern, self.template_code)
        secrets_list: list = SecretManager.list_secrets_by_path(namespace, path)
        return [
            secret for secret in referenced_secrets\
                  if secrets_list.__contains__(secret)
        ]
