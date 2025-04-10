import logging
import os
from dotenv import load_dotenv
from io import BytesIO
import boto3
from sqlalchemy.orm import Session
from sql_import_offers import import_offers_from_jsonl
from sql_models import ImportedFile
import re
from datetime import datetime, timezone, timedelta

load_dotenv()


# Ustawienia S3
ENDPOINT_URL = os.getenv("ENDPOINT_URL")
BUCKET_NAME = os.getenv("BUCKET_NAME")
PREFIX = "jobs/"

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL)
###################################################
def was_file_imported(session: Session, filename: str) -> bool:
    return session.query(ImportedFile).filter_by(filename=filename).first() is not None
###################################################
def extract_date(key):
    match = re.search(r"justjoinit_(\d{4})-(\d{2})-(\d{2})\.jsonl", key)
    if match:
        return datetime(int(match[1]), int(match[2]), int(match[3]))
    print(f"⚠️ Nie udało się sparsować daty z klucza: {key}")
    return datetime.min
###################################################
def get_jsonl_from_s3(key: str) -> BytesIO:
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return BytesIO(response["Body"].read())
###################################################
def import_all_from_s3(session: Session):
    paginator = s3.get_paginator("list_objects_v2")
    all_files = []

    today = datetime.now(timezone.utc).date()

    # Zbieramy wszystkie pliki .jsonl
    for result in paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX):
        for obj in result.get("Contents", []):
            if obj["Key"].endswith(".jsonl"):
                file_date = extract_date(obj["Key"]).date()
                # Pomijamy pliki z ostatnich 2 dni: dzisiaj, wczoraj, przedwczoraj
                if file_date >= today - timedelta(days=1):
                    logging.info(f"\u23ed Pomijam plik z ostatnich 2 dni: {obj['Key']}")
                    continue
                all_files.append(obj)

    # Sortujemy pliki od najnowszego do najstarszego
    all_files.sort(key=lambda x: extract_date(x["Key"]), reverse=True)
    # all_files.sort(key=lambda x: extract_date(x["Key"]), reverse=False)

    # Statystyki
    files_total = len(all_files)
    files_imported = 0
    files_skipped = 0
    files_failed = 0
    offers_total = 0
    offers_ok = 0
    offers_failed = 0
    offers_duplikate = 0

    for obj in all_files:
        key = obj["Key"]
        logging.info(f"Znaleziono plik: {key}")
        filename = key.split("/")[-1]

        if was_file_imported(session, filename):
            logging.info(f"⏭ Pomijam już zaimportowany plik: {filename}")
            files_skipped += 1
            continue

        try:
            logging.info(f"⬇️  Importuję plik: {filename} z klucza {key}")
            stream = get_jsonl_from_s3(key)
            # import_offers_from_jsonl(stream, session, filename)
            lines_ok, lines_failed, lines_duplikate, lines_total = import_offers_from_jsonl(stream, session, filename)
            offers_total += lines_total
            offers_ok += lines_ok
            offers_failed += lines_failed
            offers_duplikate += lines_duplikate
            files_imported += 1
        except Exception as e:
            logging.exception(f"❌ Błąd importu pliku {filename}: {e}")
            files_failed += 1

    return files_total, files_imported, files_skipped, files_failed, offers_total, offers_ok, offers_failed, offers_duplikate  