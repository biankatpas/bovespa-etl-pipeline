import os
import boto3
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def upload_file_to_s3(file_path, bucket_name, s3_key):
    """
    Upload a file to an AWS S3 bucket.

    Args:
        file_path (str): Path to the local file.
        bucket_name (str): Name of the S3 bucket.
        s3_key (str): The S3 object key (path in the bucket).

    Returns:
        bool: True if file was uploaded successfully, else False.
    """
    s3_client = boto3.client('s3')
    
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logging.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        logging.error(f"Error uploading {file_path}: {e}")
        return False
    
    return True


def upload_directory_to_s3(local_dir, bucket_name):
    """
    Upload all files in a local directory to an S3 bucket.

    Args:
        local_dir (str): Local directory path.
        bucket_name (str): S3 bucket name.
    
    Returns:
        None
    """
    for root, _, files in os.walk(local_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_dir)
            s3_key = relative_path.replace("\\", "/")
            
            upload_file_to_s3(local_file_path, bucket_name, s3_key)

# TODO: upload glue scripts to S3
