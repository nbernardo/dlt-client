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

        SecretManager.vault_instance.sys.enable_secrets_engine(
            backend_type='kv',
            path=namespace,
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
        
            
    def create_secret(namespace, params: dict, path = 'main'):
        SecretManager.vault_instance.secrets.kv.v2.create_or_update_secret(
            mount_point=namespace,
            path=path,
            secret=params
        ) 
        
            
    def create_db_secret(namespace, params: dict, path):

        config = params['dbConfig']

        if('val1-db' in params['env']):
            config['password'] = params['env']['val1-db']

        if('key1-secret' in params['env']):
            config['password'] = params['env']['key1-secret']

        dbengine = str(config['plugin_name']).split('-')[0]

        SecretManager.db_secrete_obj.create_sql_db_secret(namespace, config, SecretManager, dbengine, path)
        SecretManager.save_secrets_metadata(namespace, { config['connectionName'] : config['host'] })


    def get_secret(namespace, key, path = 'main/api/'):
        secrets = SecretManager.vault_instance.secrets.kv.v2.read_secret_version(
            path=path,
            mount_point=namespace,
            raise_on_deleted_version=True
        )

        data = secrets['data']['data']
        if key != None:
            return secrets['data']['data'][key]
        return data


    def list_secret_names(namespace):

        if SecretManager.vault_instance == None:
            SecretManager.connect_to_vault()

        secret_paths = []
        try:
            secret_paths = SecretManager.vault_instance.secrets.kv.v2.list_secrets(path='main', mount_point=namespace)
        except InvalidPath as err:
            print('Error while reading secrets list: ')
            print(err)
            return None
        
        secret_paths = secret_paths['data']['keys']

        db_secrets = []
        if secret_paths.__contains__('db/'):
            db_secrets = SecretManager.vault_instance\
                .secrets.kv.v2.list_secrets( path='main/db/', mount_point=namespace)['data']['keys']
        
        api_secrets = []
        if secret_paths.__contains__('api/'):
            api_secrets = SecretManager.vault_instance\
                .secrets.kv.v2.list_secrets( path='main/api/', mount_point=namespace)['data']['keys']

        metadata = {}
        db_secrets
        if(len(db_secrets) or len(api_secrets)):
            metadata = SecretManager.get_secret(namespace, key=None, path='metadata')

        return {
            'db_secrets': db_secrets,
            'api_secrets': api_secrets,
            'metadata': metadata
        }


    def save_secrets_metadata(namespace, new_data):
        try:
            metadata = SecretManager.get_secret(namespace, key=None, path='metadata')
            metadata = { **metadata, **new_data }
            SecretManager.create_secret(namespace, metadata, path='metadata')
        except InvalidPath:
            SecretManager.create_secret(namespace, { **new_data }, path='metadata')
            


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

