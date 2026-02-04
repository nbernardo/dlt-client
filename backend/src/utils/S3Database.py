import boto3
import polars as pl
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from botocore.client import Config
from botocore import UNSIGNED
import traceback
from io import BytesIO
import json
from services.workspace.supper.SecretManagerType import SecretManagerType


class S3Database:
    """
    S3 utility class following SQLDatabase pattern for connection management
    Handles S3 connections using Secret Manager for credential storage
    """

    secret_manager: SecretManagerType
    connections = {}

    @staticmethod
    def get_s3_connection(namespace, connection_name, secret=None):
        """
        Get S3 connection following SQLDatabase pattern
        
        Args:
            namespace (str): User namespace
            connection_name (str): S3 connection name
            secret (dict, optional): Secret data (if already retrieved)
            
        Returns:
            boto3.client: Authenticated S3 client
        """
        connection_key = f'{namespace}-{connection_name}'
        
        if connection_key in S3Database.connections:
            return S3Database.connections[connection_key]
        
        if secret is None:
            secret = S3Database.secret_manager.get_secret(namespace, f'main/s3/{connection_name}')
        
        # Create S3 client with credentials from secret
        s3_client = boto3.client(
            's3',
            aws_access_key_id=secret['access_key_id'],
            aws_secret_access_key=secret['secret_access_key'],
            region_name=secret.get('region', 'us-east-1')
        )
        
        S3Database.connections[connection_key] = s3_client
        return s3_client

    @staticmethod
    def test_s3_connection(namespace, connection_name):
        """
        Test S3 connection using Secret Manager credentials
        Following SQLDatabase.test_sql_connection pattern
        
        Args:
            namespace (str): User namespace
            connection_name (str): S3 connection name
            
        Returns:
            dict: {'result': str, 'error': bool}
        """
        message, error = '', False
        
        try:
            # Get secret from Secret Manager
            secret = S3Database.secret_manager.get_secret(namespace, f'main/s3/{connection_name}')
            
            # Extract configuration
            access_key_id = secret['access_key_id']
            secret_access_key = secret['secret_access_key']
            bucket_name = secret['bucket_name']
            region = secret.get('region', 'us-east-1')
            
            # Create S3 client
            s3_client = boto3.client(
                's3',
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                region_name=region
            )
            
            # Test connection by listing objects (limit to 1 for efficiency)
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                MaxKeys=1
            )
            
            # If we get here, connection is successful
            object_count = response.get('KeyCount', 0)
            message = f'Connection successful. Bucket contains {object_count} objects (showing first 1).'
            error = False
            
        except NoCredentialsError:
            error = True
            message = 'AWS credentials not found or invalid'
        except PartialCredentialsError:
            error = True
            message = 'Incomplete AWS credentials provided'
        except ClientError as e:
            error = True
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                message = f'Bucket "{bucket_name}" does not exist or you do not have access to it'
            elif error_code == 'AccessDenied':
                message = f'Access denied to bucket "{bucket_name}". Check your credentials and permissions.'
            elif error_code == 'InvalidAccessKeyId':
                message = 'Invalid AWS Access Key ID'
            elif error_code == 'SignatureDoesNotMatch':
                message = 'Invalid AWS Secret Access Key'
            else:
                message = f'AWS S3 Error ({error_code}): {e.response["Error"]["Message"]}'
        except Exception as e:
            error = True
            message = f'Error while trying to connect to S3: {str(e)}'
            print(message)
            traceback.print_exc()
        
        return {'result': message, 'error': error}

    @staticmethod
    def list_s3_objects(namespace, connection_name, prefix='', max_keys=100):
        """
        List objects in S3 bucket using Secret Manager credentials
        
        Args:
            namespace (str): User namespace
            connection_name (str): S3 connection name
            prefix (str): Object key prefix filter
            max_keys (int): Maximum number of objects to return
            
        Returns:
            dict: {'objects': list, 'error': bool, 'message': str}
        """
        try:
            # Get secret and S3 client
            secret = S3Database.secret_manager.get_secret(namespace, f'main/s3/{connection_name}')
            s3_client = S3Database.get_s3_connection(namespace, connection_name, secret)
            bucket_name = secret['bucket_name']
            
            # List objects
            kwargs = {
                'Bucket': bucket_name,
                'MaxKeys': max_keys
            }
            
            if prefix:
                kwargs['Prefix'] = prefix
            
            response = s3_client.list_objects_v2(**kwargs)
            
            objects = []
            for obj in response.get('Contents', []):
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'storage_class': obj.get('StorageClass', 'STANDARD')
                })
            
            return {
                'objects': objects,
                'error': False,
                'message': f'Found {len(objects)} objects'
            }
            
        except Exception as e:
            return {
                'objects': [],
                'error': True,
                'message': f'Error listing S3 objects: {str(e)}'
            }

    @staticmethod
    def preview_s3_file_with_polars(namespace, connection_name, file_key, rows=10):
        """
        Preview data from S3 file using Polars and Secret Manager credentials
        Following DltPipeline.run_transform_preview pattern
        
        Args:
            namespace (str): User namespace
            connection_name (str): S3 connection name
            file_key (str): S3 object key (file path)
            rows (int): Number of rows to preview (default: 10)
            
        Returns:
            dict: {'data': list, 'error': bool, 'message': str}
        """
        try:
            # Get secret and S3 client
            secret = S3Database.secret_manager.get_secret(namespace, f'main/s3/{connection_name}')
            s3_client = S3Database.get_s3_connection(namespace, connection_name, secret)
            bucket_name = secret['bucket_name']
            
            # Get file object
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            file_content = response['Body'].read()
            
            # Determine file type and parse with Polars
            file_extension = file_key.lower().split('.')[-1]
            
            if file_extension == 'csv':
                # Use Polars to scan CSV
                df = pl.read_csv(BytesIO(file_content))
                data = df.head(rows).to_dicts()
                
            elif file_extension in ['json', 'jsonl', 'ndjson']:
                # Parse JSON/JSONL with Polars
                if file_extension == 'json':
                    # Regular JSON - try to read as JSON first
                    try:
                        df = pl.read_json(BytesIO(file_content))
                        data = df.head(rows).to_dicts()
                    except:
                        # Fallback to manual parsing for complex JSON
                        content_str = file_content.decode('utf-8')
                        json_data = json.loads(content_str)
                        if isinstance(json_data, list):
                            data = json_data[:rows]
                        else:
                            data = [json_data]
                else:
                    # JSONL/NDJSON
                    df = pl.read_ndjson(BytesIO(file_content))
                    data = df.head(rows).to_dicts()
                    
            elif file_extension == 'parquet':
                # Use Polars to scan Parquet
                df = pl.read_parquet(BytesIO(file_content))
                data = df.head(rows).to_dicts()
                
            else:
                return {
                    'data': [],
                    'error': True,
                    'message': f'Unsupported file type: {file_extension}. Supported types: csv, json, jsonl, ndjson, parquet'
                }
            
            return {
                'data': data,
                'error': False,
                'message': f'Successfully previewed {len(data)} rows from {file_key} using Polars'
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                message = f'File "{file_key}" not found in bucket'
            elif error_code == 'AccessDenied':
                message = f'Access denied to file "{file_key}"'
            else:
                message = f'AWS S3 Error ({error_code}): {e.response["Error"]["Message"]}'
            
            return {'data': [], 'error': True, 'message': message}
            
        except Exception as e:
            return {
                'data': [],
                'error': True,
                'message': f'Error previewing file with Polars: {str(e)}'
            }

    @staticmethod
    def get_anonymous_s3_client():
        """
        Create anonymous S3 client (for public buckets)
        
        Returns:
            boto3.client: Anonymous S3 client
        """
        return boto3.client('s3', config=Config(signature_version=UNSIGNED))