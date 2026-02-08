import boto3
import pandas as pd
import polars as pl
import json
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from botocore.client import Config
from botocore import UNSIGNED
import traceback
from io import BytesIO

class BucketUtil:
    """
    Bucket utility class for connection testing, authentication, and data preview
    Generic naming to support multiple cloud providers (AWS S3, Azure Blob, GCS, etc.)
    Similar to SQLDatabase.py pattern for SQL connections
    """

    @staticmethod
    def test_s3_connection(config):
        """
        Test S3 connection with provided credentials
        Similar to SQLDatabase.test_sql_connection()
        
        Args:
            config (dict): Configuration containing:
                - access_key_id: AWS Access Key ID
                - secret_access_key: AWS Secret Access Key
                - bucket_name: S3 bucket name
                - region: AWS region (optional)
        
        Returns:
            dict: {'result': str, 'error': bool}
        """
        message, error = '', False
        
        try:
            # Extract configuration
            access_key_id = config.get('access_key_id')
            secret_access_key = config.get('secret_access_key')
            bucket_name = config.get('bucket_name')
            region = config.get('region', 'us-east-1')
            
            # Validate required parameters
            if not access_key_id or not secret_access_key or not bucket_name:
                return {
                    'result': 'Missing required parameters: access_key_id, secret_access_key, or bucket_name',
                    'error': True
                }
            
            # Create S3 client with credentials
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
    def get_s3_client(access_key_id, secret_access_key, region='us-east-1'):
        """
        Create authenticated S3 client
        
        Args:
            access_key_id (str): AWS Access Key ID
            secret_access_key (str): AWS Secret Access Key
            region (str): AWS region
            
        Returns:
            boto3.client: Authenticated S3 client
        """
        return boto3.client(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region
        )

    @staticmethod
    def preview_s3_file(config, file_key, rows=10):
        """
        Preview data from S3 file (first N rows)
        
        Args:
            config (dict): S3 configuration
            file_key (str): S3 object key (file path)
            rows (int): Number of rows to preview (default: 10)
            
        Returns:
            dict: {'data': list, 'error': bool, 'message': str}
        """
        try:
            # Extract configuration
            access_key_id = config.get('access_key_id')
            secret_access_key = config.get('secret_access_key')
            bucket_name = config.get('bucket_name')
            region = config.get('region', 'us-east-1')
            
            # Create S3 client
            s3_client = BucketUtil.get_s3_client(access_key_id, secret_access_key, region)
            
            # Get file object
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            file_content = response['Body'].read()
            
            # Determine file type and parse accordingly
            file_extension = file_key.lower().split('.')[-1]
            
            if file_extension == 'csv':
                # Parse CSV
                df = pd.read_csv(BytesIO(file_content), nrows=rows)
                data = df.to_dict('records')
                
            elif file_extension in ['json', 'jsonl', 'ndjson']:
                # Parse JSON/JSONL
                content_str = file_content.decode('utf-8')
                
                if file_extension == 'json':
                    # Regular JSON
                    json_data = json.loads(content_str)
                    if isinstance(json_data, list):
                        data = json_data[:rows]
                    else:
                        data = [json_data]
                else:
                    # JSONL/NDJSON
                    lines = content_str.strip().split('\n')
                    data = []
                    for i, line in enumerate(lines[:rows]):
                        if line.strip():
                            data.append(json.loads(line))
                            
            elif file_extension == 'parquet':
                # Parse Parquet
                df = pd.read_parquet(BytesIO(file_content))
                data = df.head(rows).to_dict('records')
                
            else:
                return {
                    'data': [],
                    'error': True,
                    'message': f'Unsupported file type: {file_extension}. Supported types: csv, json, jsonl, ndjson, parquet'
                }
            
            return {
                'data': data,
                'error': False,
                'message': f'Successfully previewed {len(data)} rows from {file_key}'
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
                'message': f'Error previewing file: {str(e)}'
            }

    @staticmethod
    def preview_s3_file_with_polars(config, file_key, rows=10):
        """
        Preview data from S3 file using Polars (following DltPipeline.run_transform_preview pattern)
        
        Args:
            config (dict): S3 configuration
            file_key (str): S3 object key (file path)
            rows (int): Number of rows to preview (default: 10)
            
        Returns:
            dict: {'data': list, 'error': bool, 'message': str}
        """
        try:
            # Extract configuration
            access_key_id = config.get('access_key_id')
            secret_access_key = config.get('secret_access_key')
            bucket_name = config.get('bucket_name')
            region = config.get('region', 'us-east-1')
            
            # Create S3 client
            s3_client = BucketUtil.get_s3_client(access_key_id, secret_access_key, region)
            
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
    def list_s3_objects(config, prefix='', max_keys=100):
        """
        List objects in S3 bucket with optional prefix filter
        
        Args:
            config (dict): S3 configuration
            prefix (str): Object key prefix filter
            max_keys (int): Maximum number of objects to return
            
        Returns:
            dict: {'objects': list, 'error': bool, 'message': str}
        """
        try:
            # Extract configuration
            access_key_id = config.get('access_key_id')
            secret_access_key = config.get('secret_access_key')
            bucket_name = config.get('bucket_name')
            region = config.get('region', 'us-east-1')
            
            # Create S3 client
            s3_client = BucketUtil.get_s3_client(access_key_id, secret_access_key, region)
            
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
    def get_anonymous_s3_client():
        """
        Create anonymous S3 client (for public buckets)
        
        Returns:
            boto3.client: Anonymous S3 client
        """
        return boto3.client('s3', config=Config(signature_version=UNSIGNED))
