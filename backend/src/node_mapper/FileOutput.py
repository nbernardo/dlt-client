from .TemplateNodeType import TemplateNodeType
from controller.RequestContext import RequestContext
import logging
from minio import Minio
from services.workspace.SecretManager import SecretManager
import requests
import os

class FileOutput(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict, context: RequestContext = None, component_id = None):
        """
        Initialize the instance
        """
        if len(data.keys()) == 0: return

        try:
            
            self.context = context
            errors = self.context.operational_metadata.step_error.keys()
            if len(errors) > 0: return
            
            secrets = SecretManager.get_secret(context.transaction_namespace, data['selectedSecret'])

            source_file = context.operational_metadata.output_file
            file_name = source_file.split('/')[-1]

            if data['selectedOutputType'] == 'S3':
                self.s3_upload(secrets, context.transaction_namespace, source_file, file_name)
            if data['selectedOutputType'] == 'MFT':
                self.mft_sftpgo(secrets, source_file, f'/{context.transaction_namespace}/')
                
        except Exception as error:
            self.notify_failure_to_ui('FileOutput',error)


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


    def get_params(self, secrets):

        user = secrets['apiSettings']['userCredName']
        pwd = secrets['apiSettings']['userCredPass']
        url = secrets['apiSettings']['apiBaseUrl'] or ''

        return user, pwd, url


    def get_path_param(self, url):
        is_secure = url.startswith('https://')
        clean_path = url.replace('http://','').replace('https://','')
        return is_secure, clean_path.split('/')

    
    def s3_upload(self, secrets, namespace, source_file, file):

        user, pwd, url = self.get_params(secrets)
        is_secure, upload_path = self.get_path_param(url)

        host = upload_path[0]
        folder = 'operations'
        bucket = namespace

        if len(upload_path) > 1:
            bucket = upload_path[1]
            if len(upload_path) > 2:
                folder = upload_path[2]

        client = Minio(host, access_key=user, secret_key=pwd, secure=is_secure)

        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)

            client.fput_object(bucket, f'{folder}/{file}', source_file)
        
        except Exception as err:
            self.context.operational_metadata.step_error['FileOutput'] = str(err)
            print(f'Error while uploading the file into S3: {str(err)}')


    def mft_sftpgo(self, secrets, local_file: str, remote_path: str):
        user, pwd, url = self.get_params(secrets)
        auth_res = requests.get(f"{url}/user/token", auth=(user, pwd))
        auth_res.raise_for_status()
        token = auth_res.json()["access_token"]

        headers = {"Authorization": f"Bearer {token}"}
        secret_path = secrets['apiSettings']['endPointsGroup']['apiEndpointPath'][0]

        if secret_path and secret_path != '/':
            remote_path = secret_path

        dest_file = os.path.basename(local_file)
        clean_dir = remote_path.strip('/')
        target_path = f"{clean_dir}/{dest_file}" if clean_dir else dest_file

        try:
            with open(local_file, "rb") as f:
                params = { "path": target_path, "mkdir_parents": "true" }
                res = requests.post(f"{url}/user/files/upload", params=params, headers=headers, data=f)
                res.raise_for_status()
                return res.json()

        except Exception as err:
            print(f"Error while uploading the file into MFT - SFTPgo: {str(err)}")
            self.context.operational_metadata.step_error['FileOutput'] = str(err)




