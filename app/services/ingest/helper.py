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


def upload_directory_to_s3(local_dir, bucket_name, s3_folder="raw"):
    """
    Upload all files in a local directory to an S3 bucket.

    Args:
        local_dir (str): Local directory path.
        bucket_name (str): S3 bucket name.
        s3_folder (str, optional): S3 folder to upload files into (default is "raw").
    
    Returns:
        None
    """
    for root, _, files in os.walk(local_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_dir)
            relative_path = relative_path.replace("\\", "/")
            s3_key = f"{s3_folder}/{relative_path}"
            
            upload_file_to_s3(local_file_path, bucket_name, s3_key)


def upload_glue_scripts_to_s3(local_glue_dir, bucket_name, s3_folder="scripts"):
    """
    Uploads all files in the local Glue scripts directory to the specified folder in the S3 bucket.

    Args:
        local_glue_dir (str): Path to the local directory containing Glue scripts.
        bucket_name (str): Name of the S3 bucket.
        s3_folder (str, optional): S3 folder to upload files into (default is "scripts").

    Returns:
        None
    """
    for root, _, files in os.walk(local_glue_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            filename = os.path.basename(local_file_path)
            s3_key = f"{s3_folder}/{filename}"
                        
            upload_file_to_s3(local_file_path, bucket_name, s3_key)
