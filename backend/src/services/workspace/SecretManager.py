import hvac
from os import getenv as env
from hvac import Client
from hvac.exceptions import InvalidPath, InvalidRequest
import traceback
from services.workspace.supper.SecretManagerType import SecretManagerType
import utils.database_secret as DBSecret

class SecretManager(SecretManagerType):
    """
    This is a singleton classe which the responsibility is to handle 
    secret managements, initially focusing on Hashicorp vault
    """

    vault_instance: Client = None
    vault_url = env('HASHICORP_HOST')
    vault_token = env('HASHICORP_TOKEN')
    vault_crt_path = False \
        if str(env('HASHICORP_CERTIF_PATH','false')).lower() == 'false'\
        else env('HASHICORP_CERTIF_PATH')
    
    db_secrete_obj: DBSecret = None

    def __init__():
        pass

    
    def connect_to_vault(params = {}):

        if('vault_url' in params):
            SecretManager.vault_url = params['vault_url']
            SecretManager.vault_token = params['vault_token']
            SecretManager.vault_crt_path = params['vault_crt_path']

        SecretManager.vault_instance = hvac.Client(
            url=SecretManager.vault_url,
            token=SecretManager.vault_token,
            verify=SecretManager.vault_crt_path
        )

    
    def create_namespace(namespace, type = None):

        type_prefix = '' if type == None else f'_{type}'
        SecretManager.vault_instance.sys.enable_secrets_engine(
            backend_type='kv',
            path=type_prefix+namespace,
            options={'version': '2'}
        )


    def set_namespace(namespace, type = None) -> SecretManagerType | None:

        exception = False

        try:
            SecretManager.vault_instance.sys.read_mount_configuration(namespace)
        except (InvalidPath, InvalidRequest):
            SecretManager.create_namespace(namespace, type)
        except Exception as error:
            print(f'Error while reading vault namespace for {namespace} - {str(error)}')
            print(traceback.print_exc())
            exception = True
        finally:
            return SecretManager if exception == False else None
        
            
    def create_secret(namespace, params: dict):
        SecretManager.vault_instance.secrets.kv.v2.create_or_update_secret(
            mount_point=namespace,
            path='main',
            secret=params
        ) 
        
            
    def create_db_secret(namespace, params: dict):

        config = params['dbConfig']
        config['password'] = params['env']['val1-db']
        dbengine = str(config['plugin_name']).split('-')[0]

        print(dbengine)
        SecretManager.db_secrete_obj.create_sql_db_secret(namespace, config, SecretManager, dbengine)


    def get_secret(namespace, key):
        secrets = SecretManager.vault_instance.secrets.kv.v2.read_secret_version(
            path='main',
            mount_point=namespace,
            raise_on_deleted_version=True
        )

        return secrets['data']['data'][key]


if __name__ == '__main__':
    print('Standalone running')

    vault_url = ''
    vault_token = ''
    vault_crt_path = False

    params = { 
        'vault_url' : vault_url, 
        'vault_token': vault_token,
        'vault_crt_path': vault_crt_path 
    }

    SecretManager.connect_to_vault(params)

    namespace = 'standtest'
    sec_manager = SecretManager.set_namespace(namespace)
    
    key = 'mysecret'
    value = 'myvalue'
    secret = { key: value }

    sec_manager.create_secret(namespace, secret)

