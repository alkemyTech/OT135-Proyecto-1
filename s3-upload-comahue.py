import logging
import os
import boto3
from botocore.exceptions import ClientError
from auth import ACCESS_KEY, SECRET_KEY, BUCKET

def upload_file(file_name, object_name=None):
    '''
    Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    '''
    route = os.path.dirname(__file__)

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    s3_client.create_bucket(Bucket=BUCKET)
    try:        
        with open(f'{route}/txt/archivo.txt', 'rb') as f:
            s3_client.upload_fileobj(f, BUCKET, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
