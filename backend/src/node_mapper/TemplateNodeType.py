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
            destination_string = "dlt.destinations.sqlalchemy(credentials=dbcredentials)"
            if template_type == 'non_database_source':
                # TODO: Implement this scenario
                template = self.parse_nondb_source_pipeline_template(template, template_type, destinations)
            else:
                template = self.parse_pipeline_template(template, template_type)

        else:
            # Remove placeholder for in-file pipeline metadata (e.g. destination tables when SQL DB)
            template = template.replace('%metadata_section%','')
            # Remove placeholder for destination database secrets
            template = template.replace('%dest_secret_code%','')

            if self.template_type == 'non_database_source':
                # Remove SecretManager import placeholder
                template = template.replace('%import_from_src%', '')

                destination_string = 'dlt.destinations.duckdb("%Usr_folder%/%Dbfile_name%.duckdb")'
            else:
                destination_string = "dlt.destinations.duckdb(f'{dest_folder}/%User_folder%/{ppline_name}.duckdb')"
        
        return template.replace('%destination_string%',destination_string)
    

    def parse_pipeline_template(self, template, template_type):
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
        return template.replace('%dest_secret_code%',secret_code)
            

    def parse_nondb_source_pipeline_template(self, template, template_type, destinations):
        
        dest_table_name = str(destinations[0]).lower().strip().replace(' ','_')

        # This'll add a section in the top of the template file with the %source_tables% placeholder
        # which is then filled by any input node type (Backet, InputAPI, SQLDBComponent, etc.)
        metadata_section = f"# METADATA: dest_tables=['{dest_table_name}']\n"
        template = template.replace('%metadata_section%',metadata_section)

        # n variable is to add a new line and alikely space * 4 (corresponding to tab)
        n = '\n    ' if template_type == 'sql_database' else '\n'

        src_path_add = '#Adding root folder to allow import  from src'
        src_path_add += f"{n}from pathlib import Path{n}import sys"
        src_path_add += f"{n}src_path = str(Path(__file__).parent).replace('/destinations/pipeline/%User_folder%','')"
        src_path_add += f"{n}sys.path.insert(0, src_path){n}sys.path.insert(0, src_path+'/src')"

        import_secret_manager = f'{n}{n}from src.services.workspace.SecretManager import SecretManager'
        add_path_and_import_secret_manager = f'{src_path_add}{import_secret_manager}'

        template = template.replace('%import_from_src%', add_path_and_import_secret_manager)

        # This replacement only happen if pipeline destination is Database otherwise
        # the replacement is done by the specialized source (e.g. InputAPI, Bucket)
        template = template.replace('%ppline_dest_table%', f"'{dest_table_name}'")
        
        namespace_var = f"{n}namespace = %namespace%"
        connect_secret_vault = f"{n}SecretManager.ppline_connect_to_vault()"
        connaction_name_var = f"{n}dbconnection_name = ['%outdb_secret_name%']"
        dbcredentials_var = f"{n}dbcredentials = SecretManager.get_db_secret(namespace, dbconnection_name[0])['connection_url']"
        dbconnecting_log = f"{n}print('Connecting to destination Database', flush=True){n}"

        secret_code = f"{namespace_var}{connect_secret_vault}{connaction_name_var}{dbcredentials_var}{dbconnecting_log}"
        return template.replace('%dest_secret_code%',secret_code)
        