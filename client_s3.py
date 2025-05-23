import os
import boto3
from dotenv import load_dotenv
import logging
from datetime import datetime
from botocore.exceptions import ClientError
import hashlib
from pathlib import Path

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
    #####################################
    def download_sqlite_db(self, s3_key: str, local_path: str) -> bool:
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logging.info(f"📥 Pobrano bazę danych z S3: {s3_key}")
            self.save_etag_for_file(local_path, s3_key)
            return True
        except ClientError as e:
            logging.error(f"❌ Błąd podczas pobierania bazy danych: {e}")
            return False
    #####################################
    def upload_sqlite_db(self, s3_key: str, local_path: str, backup_prefix: str = None) -> bool:
        try:
            if backup_prefix:
                try:
                    self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
                    timestamp = datetime.today().strftime('%Y%m%d_%H%M')
                    backup_key = f"{backup_prefix}/jobs_{timestamp}.sqlite"
                    copy_source = {
                        'Bucket': self.bucket_name,
                        'Key': s3_key
                    }
                    self.s3_client.copy_object(
                        Bucket=self.bucket_name,
                        CopySource=copy_source,
                        Key=backup_key
                    )
                    logging.info(f"💾 Backup bazy wykonany w S3: {s3_key} ➜ {backup_key}")
                except ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        logging.info(f"ℹ️ Brak pliku do backupu na S3: {s3_key}")
                    else:
                        raise

            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            logging.info(f"📤 Wysłano bazę danych na S3: {s3_key}")
            self.save_etag_for_file(local_path, s3_key)
            return True

        except ClientError as e:
            logging.error(f"❌ Błąd podczas wysyłania bazy danych: {e}")
            return False
    #####################################
    def with_synced_sqlite_db(self, s3_key: str, local_path: str, backup_prefix: str = None):
        def decorator(func):
            def wrapper(*args, **kwargs):
                if not self.download_sqlite_db(s3_key, local_path):
                    raise RuntimeError("Nie udało się pobrać bazy SQLite z S3.")
                try:
                    result = func(local_path, *args, **kwargs)
                    return result
                finally:
                    if not self.upload_sqlite_db(s3_key, local_path, backup_prefix):
                        raise RuntimeError("Nie udało się wysłać zaktualizowanej bazy SQLite na S3.")
            return wrapper
        return decorator
    

# s3 = S3Client()

# @s3.with_synced_sqlite_db(
#     s3_key="jobs/sql/jobs.sqlite",
#     local_path="/tmp/jobs.sqlite",
#     backup_prefix="jobs/sql/backup"
# )
# def insert_offer(db_path, offer):
#     import sqlite3
#     conn = sqlite3.connect(db_path)
#     cur = conn.cursor()
#     cur.execute("INSERT INTO offers (title, company) VALUES (?, ?)", (offer['title'], offer['company']))
#     conn.commit()
#     conn.close()

# # Wywołanie:
# insert_offer({"title": "Data Scientist", "company": "AI Corp"})
    #####################################
    def get_local_md5(self, filepath: str) -> str:
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    #####################################
    def get_s3_etag(self, s3_key: str) -> str:
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            etag = response['ETag'].strip('"')  # usunięcie cudzysłowów
            return etag
        except Exception as e:
            logging.warning(f"Nie udało się pobrać ETag z S3: {e}")
            return None
    #####################################
    def is_sqlite_up_to_date(self, local_path: str, s3_key: str) -> bool:
        etag_path = Path(f"{local_path}.etag")
        if not etag_path.exists():
            logging.info("📄 Brak lokalnego pliku .etag – pobieranie wymagane")
            return False

        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            s3_etag = response['ETag'].strip('"')

            with open(etag_path, "r") as f:
                local_etag = f.read().strip()

            logging.info(f"🔍 Porównanie ETag: lokalny={local_etag}, S3={s3_etag}")
            return s3_etag == local_etag

        except Exception as e:
            logging.warning(f"❌ Błąd pobierania ETag z S3: {e}")
            return False
    #####################################
    def save_etag_for_file(self, local_path: str, s3_key: str):
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            s3_etag = response['ETag'].strip('"')
            with open(f"{local_path}.etag", "w") as f:
                f.write(s3_etag)
            logging.info(f"💾 Zapisano ETag do pliku: {local_path}.etag")
        except Exception as e:
            logging.warning(f"❌ Nie udało się zapisać ETag: {e}")

    ######################################