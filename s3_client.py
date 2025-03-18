import os
import boto3
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError

load_dotenv()

ENDPOINT_URL = os.getenv("ENDPOINT_URL")
BUCKET_NAME = os.getenv("BUCKET_NAME")

class S3Client:
    def __init__(self, endpoint_url=ENDPOINT_URL, bucket_name=BUCKET_NAME):
        self.endpoint_url = endpoint_url
        self.bucket_name = bucket_name
        try:
            self.s3_client = boto3.client('s3', endpoint_url=endpoint_url)
            logging.info("S3 client created")
        except Exception as e:
            logging.error(f"Failed to create S3 client: {e}")
    #####################################
    def upload_file(self, file_path,s3_key):
        try:
            self.s3_client.upload_file(file_path, Bucket=self.bucket_name, Key=s3_key)
            logging.info(f"File uploaded to S3: {s3_key}")
            return True
        except Exception as e:
            logging.error(f"Failed to upload file to S3: {e}")
            return False
    #####################################
    def get_file(self, s3_key):
        try:
            # self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            logging.info(f"File downloaded from S3: {s3_key}")
            return response
        except Exception as e:
            logging.error(f"Failed to download file from S3: {e}")
            return False
    #####################################
    def put_file(self, s3_key, body):
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=body)
            logging.info(f"File uploaded to S3: {s3_key}")
            return True
        except Exception as e:
            logging.error(f"Failed to upload file to S3: {e}")
            return False