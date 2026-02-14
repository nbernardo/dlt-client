import hvac
from os import getenv as env
from hvac import Client
from hvac.exceptions import InvalidPath, InvalidRequest
import traceback
from services.workspace.supper.SecretManagerType import SecretManagerType
import utils.database_secret as DBSecret
from utils.SQLDatabase import SQLConnection


def referencedSecrets(namespace, secret_names):
    SecretManager.ppline_connect_to_vault()
    return SecretManager.get_from_references(namespace, secret_names)



class SecretManager(SecretManagerType):
    """
    This is a singleton classe which the responsibility is to handle 
    secret managements, initially focusing on Hashicorp vault
    """
    vault_host, vault_pass = env('VAULT_ADDR'), env('VAULT_TOKEN')

    vault_instance: Client = None
    vault_url = vault_host if vault_host != None else env('HASHICORP_HOST')
    vault_token = vault_pass if vault_pass != None else env('HASHICORP_TOKEN')
    vault_crt_path = False \
        if str(env('HASHICORP_CERTIF_PATH','false')).lower() == 'false'\
        else env('HASHICORP_CERTIF_PATH')
    
    db_secrete_obj: DBSecret = None

    def __init__():
        pass

    def referencedSecrets(namespace, secret_names):
        return referencedSecrets(namespace, secret_names)

    
    def ppline_connect_to_vault() -> hvac.Client:

        vault_host, vault_pass = env('VAULT_ADDR'), env('VAULT_TOKEN')
        vault_url = vault_host if vault_host != None else env('HASHICORP_HOST')
        vault_token = vault_pass if vault_pass != None else env('HASHICORP_TOKEN')

        vault_crt_path = False \
            if str(env('HASHICORP_CERTIF_PATH','false')).lower() == 'false'\
            else env('HASHICORP_CERTIF_PATH')

        params = { 
            'vault_url': vault_url, 
            'vault_token': vault_token,
            'vault_crt_path': vault_crt_path
        }

        return SecretManager.connect_to_vault(params)


    def connect_to_vault(params = {}) -> hvac.Client :

        if('vault_url' in params):
            SecretManager.vault_url = params['vault_url']
            SecretManager.vault_token = params['vault_token']
            SecretManager.vault_crt_path = params['vault_crt_path']

        SecretManager.vault_instance = hvac.Client(
            url=SecretManager.vault_url,
            token=SecretManager.vault_token,
            #verify=SecretManager.vault_crt_path
        )

        return SecretManager.vault_instance

    
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
        if path.startswith('main/db'):

            secrets = params['dbConfig']['secrets'][0]
            if secrets['isKvSecret']:
                if secrets['secretType'] == 's3-access-and-secret-keys':
                    secret_values = { 
                        'access_key_id': secrets['firstKey'], 
                        'secret_access_key': secrets['secondKey'],
                        'bucket_name': (secrets['bucketUrl'].split('//')[1] or '').replace('/','')
                    }

                    SecretManager.save_secrets_metadata(namespace, { secrets['connectionName'] : secrets['bucketUrl'] })
                    return SecretManager.vault_instance.secrets.kv.v2.create_or_update_secret(
                        mount_point=namespace,
                        path=path,
                        secret=secret_values
                    )
 

            for item in params['dbConfig']['secrets']:
                key_value = list(item.items())[0]
                k = key_value[0]
                v = key_value[1]

                SecretManager.vault_instance.secrets.kv.v2.create_or_update_secret(
                    mount_point=namespace,
                    path=path+'/'+k,
                    secret={ k: v }
                )
        else:
            if 'dbConfig' in params:
                params = { **params, 'dbConfig': {} }

            SecretManager.vault_instance.secrets.kv.v2.create_or_update_secret(
                mount_point=namespace,
                path=path,
                secret=params
            )

        SecretManager.save_api_secret_metadata(namespace, path, params)

    
    def save_api_secret_metadata(namespace, path, params):
        path_pieces = str(path).split('/')
        if len(path_pieces) >= 2:
            if path_pieces[-2] == 'api':
                api_metadata = { 
                    path_pieces[-1] : {
                        'host': params['apiSettings']['apiBaseUrl'],
                        'totalEndpoints': len(params['apiSettings']['endPointsGroup']['apiEndpointPath'])
                    }
                }
                SecretManager.save_secrets_metadata(namespace, api_metadata)
        
            
    def create_db_secret(namespace, params: dict, path):

        config = params['dbConfig']

        if('val1-db' in params['env']):
            config['password'] = params['env']['val1-db']

        if('key1-secret' in params['env']):
            config['password'] = params['env']['key1-secret']

        dbengine = str(config['plugin_name']).split('-')[0]

        SecretManager.db_secrete_obj.create_sql_db_secret(namespace, config, SecretManager, dbengine, path)
        SecretManager.save_secrets_metadata(namespace, { config['connectionName'] : config['host'] })


    def get_secret(namespace, path, edit=False, secret_group=False, from_pipeline = False):
        data = {}
        if from_pipeline:
            SecretManager.ppline_connect_to_vault()
        if secret_group: path = f'main/db/{path}'
        elif not edit and not str(path).startswith('main/db/'):
            path = 'metadata' if path == 'metadata' else 'main/api/'+path
        try:
            secrets = SecretManager.vault_instance.secrets.kv.v2.read_secret_version(
                path=path,
                mount_point=namespace,
                raise_on_deleted_version=True
            )
            data = secrets['data']['data']
        except Exception as err:
            print('Error on getting the secrets: ', str(err))
            ...
        return data
    
    
    def get_pipeline_secret(namespace, path):
        return SecretManager.get_secret(namespace, path, False, False, True)


    def list_secrets_by_path(namespace, path):
        secrets = SecretManager.vault_instance.secrets.kv.v2.list_secrets(
            path=path,
            mount_point=namespace
        )
        print(secrets)
        return secrets['data']['keys']
        

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
            metadata = SecretManager.get_secret(namespace, path='metadata')

        return {
            'db_secrets': db_secrets,
            'api_secrets': api_secrets,
            'metadata': metadata
        }


    def save_secrets_metadata(namespace, new_data):
        try:
            metadata = SecretManager.get_secret(namespace, path='metadata')
            metadata = { **metadata, **new_data, 'dbConfig': {} }
            SecretManager.create_secret(namespace, metadata, path='metadata')
        except InvalidPath:
            SecretManager.create_secret(namespace, { **new_data, 'dbConfig': {} }, path='metadata')


    def get_db_secret(namespace, connection_name, from_pipeline = False):
        if from_pipeline:
            SecretManager.ppline_connect_to_vault()
            
        path = f'main/db/{connection_name}'
        secret = SecretManager.get_secret(namespace, path=path)

        if 'dbengine' in secret:
            if secret['dbengine'] == 'mssql':
                secret['connection_url'] = secret['connection_url']+f'{SQLConnection.get_mssql_driver()}'

        return secret
    

    def get_db_secret_from_ppline(namespace, connection_name):
        return SecretManager.get_db_secret(namespace, connection_name, from_pipeline = True)
    

    def get_from_references(namespace,references: list = []):

        def clean_private_key(secret):
            if str(secret).__contains__('BEGIN')\
               and str(secret).__contains__('END')\
               and str(secret).__contains__('PRIVATE KEY'):
                   secret = secret.replace('\\n','\n')
            
            return secret

        secrets = {
            secret: clean_private_key(SecretManager.get_db_secret(namespace,secret)[secret]) 
            for secret in references
        }
        
        if(len(secrets) > 0):
            from collections import namedtuple
            Secrets = namedtuple('Secrets', secrets.keys())
            return Secrets(**secrets)
        else:
            from types import SimpleNamespace
            return SimpleNamespace()



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

