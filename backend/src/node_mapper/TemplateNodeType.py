from controller.RequestContext import RequestContext

class TemplateNodeType:
    """
    Class to serve as supper for node types
    """
    def __init__(self, data: dict, context: RequestContext):
        self.template = None
        self.context = context
        self.component_id = None
        self.parse_to_literal = []
        self.template_type = None


    def run(self):
        """ Method to be implemented by each node type """


    def notify_completion_to_ui(self):
            success = {'componentId': self.component_id}
            self.context.emit_success(self, success)


    def notify_failure_to_ui(self, type, err, add_exception = True):
            error = {'message': f'{err}', 'componentId': self.component_id}
            if(add_exception):
                self.context.add_exception(type, error)
            self.context.emit_error(self, error)
            
            return self.context.FAILED
    

    def parse_destination_string(self, template: str):
        
        destinations = self.context.sql_destinations
        template_type = None

        if self.__dict__.__contains__('template_type'):
             template_type = self.template_type

        if len(destinations) > 0:
            if template_type == 'has_duckdb_path':
                # TODO: Implement this scenario
                ...
            else:
                # This'll add a section in the top of the template file with the %source_tables% placeholder
                # which is then filled by any input node type (Backet, InputAPI, SQLDBComponent, etc.)
                metadata_section = f'# METADATA: dest_tables=%source_tables%\n'
                template = template.replace('%metadata_section%',metadata_section)

                # n variable is to add a new line and alikely space * 4 (corresponding to tab)
                n = '\n    ' if template_type == 'sql_database' else '\n'

                connaction_name_var = f"{n}dbconnection_name = ['%outdb_secret_name%']"
                dbcredentials_var = f"{n}dbcredentials = SecretManager.get_db_secret(namespace, dbconnection_name[0])['connection_url']"
                dbconnecting_log = f"{n}print('Connecting to destination Database', flush=True){n}"

                secret_code = f"{connaction_name_var}{dbcredentials_var}{dbconnecting_log}"
                template = template.replace('%dest_secret_code%',secret_code)
                destination_string = "dlt.destinations.sqlalchemy(credentials=dbcredentials)"

        else:
            template = template.replace('%metadata_section%','')
            if self.template_type == 'has_duckdb_path':
                destination_string = 'dlt.destinations.duckdb("%Usr_folder%/%Dbfile_name%.duckdb")'
            else:
                destination_string = "dlt.destinations.duckdb(f'{dest_folder}/%User_folder%/{ppline_name}.duckdb')"
        
        return template.replace('%destination_string%',destination_string)