import time
from time import sleep
from pathlib import Path
import logging
import random
from dotenv import load_dotenv
from log_manager import LogManager
from notification import DiscordNotifier
from client_justjoin import JustJoinClient
from client_s3 import S3Client
from scheduler import TaskScheduler
from datetime import datetime, timedelta
import shutil
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sql_models import Base
from sql_import_offers import import_offers_from_jsonl
from sql_import_s3 import import_all_from_s3
from sqlalchemy import create_engine



load_dotenv()

timezone = os.getenv("TZ", "UTC")
os.environ['TZ'] = timezone
time.tzset()  # działa na Linux/Unix

DATA_FOLDER = Path("data/")
SQL_DATAFOLDER = DATA_FOLDER / "sql"
RAW_DATA_FOLDER = DATA_FOLDER / "raw"

DATA_FOLDER.mkdir(exist_ok=True)
SQL_DATAFOLDER.mkdir(exist_ok=True)
RAW_DATA_FOLDER.mkdir(exist_ok=True)

SQL_FILE_NAME = "jobs.sqlite"
SQL_DATABASE_URL = f"sqlite:///{SQL_DATAFOLDER}/{SQL_FILE_NAME}"

log = LogManager("main.log")

# scheduler = TaskScheduler()

##########################################################################################
def jobs_download(ppage=100):
    log_download = LogManager("justjoinit.log")
    s3 = S3Client()
    notifier = DiscordNotifier()
    notifier.send("Pobieranie ofert z JustJoin.it")
    logging.info("Pobieranie ofert z JustJoin.it")
    jjc = JustJoinClient(offers_per_page=ppage)
    current_page = 1
    sleep = 15
    pages_total = 0
    pages_readed = 0
    offers_total = 0
    offers_readed = 0
    offers_saved = 0
    offers_skipped = 0
    
    while True:
        logging.info(f"Pobieranie ofert ze strony {current_page}...")
        offers, total_pages, total_offers, next_page = jjc.get_page(current_page)
        offers_total = total_offers
        pages_total = total_pages
        pages_readed = current_page
        offers_readed += len(offers)
        # Sprawdzenie, czy są oferty do przetworzenia
        if offers is None or total_offers == 0:
            logging.info("Brak ofert do pobrania.")
            break

        # Zapis ofert lokalnie
        # success, saved, duplikates = jjc.save_offers_local(LOCAL_DATA_FOLDER, offers)
        # Zapis ofert do s3
        success, saved, duplicates = jjc.save_offers_s3(s3, offers)
        offers_saved += saved
        offers_skipped += duplicates
        if not success:
            logging.info("Wszystkie oferty na stronie to duplikaty. Zakończono pobieranie.")
            break

        # Jeśli nie ma kolejnej strony, zakończ pętlę
        if next_page is None or next_page == "null":
            logging.info("Nie ma więcej stron ofert. Zakończono pobieranie.")
            break

        # Aktualizacja numeru strony do pobrania
        current_page = next_page

        # Pauza, aby nie przeciążać serwera (opcjonalnie)
        rsleep = random.randint(1, sleep)
        time.sleep(rsleep)
        
    end_text = f"Zakończono pobieranie ofert z JustJoin.it: wczytano {offers_readed} ofert z {pages_readed} stron. Zapisano {offers_saved} ofert, pominięto {offers_skipped} duplikatów."
    logging.info(end_text)  
    notifier.send(end_text)
    send = log_download.upload_logs_s3(s3, backup_type="download")
    if send:
        notifier.send("Logi przesłane do S3")
    return True
#####################################################
def jobs_sql():
    log_sql = LogManager("sql.log")
    s3 = S3Client()
    notifier = DiscordNotifier()
    notifier.send("Importowanie ofert z S3 do SQLite")
    logging.info("Importowanie ofert z S3 do SQLite")

    local_path = SQL_DATAFOLDER / SQL_FILE_NAME
    # s3_key = f"jobs/sql/jobs.sqlite"
    s3_key = f"jobs/sql/{SQL_FILE_NAME}"

    # ✅ 1. Czy plik lokalny istnieje?
    if local_path.exists():
        # 🔄 1a. Backup lokalnej bazy do S3
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        backup_key = f"jobs/sql/backup/jobs_local_{timestamp}.sqlite"
        s3.upload_file(str(local_path), backup_key)
        logging.info(f"🛡 Wysłano backup lokalnej bazy do S3: {backup_key}")

        # 🔍 1b. Porównanie MD5 z wersją w S3
        if s3.is_sqlite_up_to_date(str(local_path), s3_key):
            logging.info("✅ Lokalna baza danych jest aktualna – pomijam pobieranie")
        else:
            logging.info("📦 Baza lokalna nieaktualna – pobieram z S3")
            if not s3.download_sqlite_db(s3_key, str(local_path)):
                logging.error("❌ Nie udało się pobrać pliku SQLite z S3 – nadpisuję lokalną pustą bazą")
                local_path.touch()
    else:
        # ❗ Plik lokalny nie istnieje – tworzymy nowy
        logging.warning("📄 Brak lokalnej bazy – tworzę nową lokalną bazę danych")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if not s3.download_sqlite_db(s3_key, str(local_path)):
            logging.error("❌ Nie udało się pobrać pliku SQLite z S3 – tworzę nową lokalną bazę danych")
            local_path.touch()
    
    # Utworzenie sesji SQLAlchemy i importowanie danych
    engine = create_engine(SQL_DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    Base.metadata.create_all(engine)
    try:
        files_total, files_imported, files_skipped, files_failed, offers_total, offers_ok, offers_failed, offers_duplikate = import_all_from_s3(session)
        logging.info("SQL import completed successfully")
    except Exception as e:
        logging.error(f"SQL import failed: {e}")
    finally:
        session.close()
        engine.dispose()

    # Zapisanie pliku SQLite z powrotem na S3
    if not s3.upload_sqlite_db(s3_key, local_path, backup_prefix="jobs/sql/backup"):
        logging.error("Nie udało się wysłać pliku SQLite na S3")
        return

    # Po zakończeniu sprawdź, czy istnieje plik błędów i wyślij go do S3
    failed_path = "offers_failed.jsonl"
    if os.path.exists(failed_path):
        today = datetime.today()  # Define the current date
        failed_key = f"jobs/sql/offers_failed_{today.strftime('%Y%m%d')}.jsonl"
        s3.upload_file(failed_path, failed_key)
        if s3.upload_file(failed_path, failed_key):
            logging.info(f"📤 Wysłano plik błędów do S3: {failed_key}")
            os.remove(failed_path)
            logging.info("🗑 Usunięto lokalny plik błędów")



    end_text = f"Zakończono import ofert z S3: wczytano {files_imported} plików z {files_total} plików. Zapisano {offers_ok} ofert, pominięto {offers_duplikate} duplikatów, {offers_failed} błędów."
    logging.info(end_text) 
    notifier.send(end_text) 
    send = log_sql.upload_logs_s3(s3, backup_type="sql")
    if send:
        notifier.send("Logi sql przesłane do S3")
    return True
#####################################################
def jobs_scraper():
    log_scraper = LogManager("scraper.log")
    s3 = S3Client()
    jjc = JustJoinClient()
    notifier = DiscordNotifier()
    notifier.send("Scrapowanie ofert JustJoin.it")
    logging.info("Scrapowanie ofert JustJoin.it")

    local_path = SQL_DATAFOLDER / SQL_FILE_NAME
    # s3_key = "jobs/sql/jobs.sqlite"
    s3_key = f"jobs/sql/{SQL_FILE_NAME}"

    # ✅ Sprawdzenie lokalnej bazy danych
    if local_path.exists():
        # 🔄 Backup lokalnej bazy do S3
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        backup_key = f"jobs/sql/backup/jobs_local_{timestamp}.sqlite"
        s3.upload_file(str(local_path), backup_key)
        logging.info(f"🛡 Wysłano backup lokalnej bazy do S3: {backup_key}")

        # 🔍 Porównanie MD5 z wersją w S3
        if s3.is_sqlite_up_to_date(str(local_path), s3_key):
            logging.info("✅ Lokalna baza danych jest aktualna – pomijam pobieranie")
        else:
            logging.info("📦 Baza lokalna nieaktualna – pobieram z S3")
            if not s3.download_sqlite_db(s3_key, str(local_path)):
                logging.error("❌ Nie udało się pobrać pliku SQLite z S3 – przerywam działanie")
                raise SystemExit(1)
    else:
        # ❗ Plik lokalny nie istnieje – próbujemy pobrać z S3
        logging.warning("📄 Brak lokalnej bazy – próbuję pobrać z S3")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if not s3.download_sqlite_db(s3_key, str(local_path)):
            logging.error("❌ Nie udało się pobrać pliku SQLite z S3 – przerywam działanie")
            raise SystemExit(1)

    # Utworzenie sesji SQLAlchemy i importowanie danych
    engine = create_engine(SQL_DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    Base.metadata.create_all(engine)

    try:
        total, success, errors, no_notes, skills_updated, skills_nice = jjc.scrape_offer_details(SQL_DATABASE_URL)
        logging.info("Scraping completed successfully")
    except Exception as e:
        logging.error(f"Scraping failed: {e}")
        session.close()
        engine.dispose()
        return

    session.close()
    engine.dispose()

    if not s3.upload_sqlite_db(s3_key, local_path, backup_prefix="jobs/sql/backup"):
        logging.error("Nie udało się wysłać pliku SQLite na S3")
        return

    end_text = (
        f"Zakończono scrapowanie ofert z JustJoin.it:\n"
        f"➡️ Łącznie: {total} | ✅ OK: {success} | ❌ błędy: {errors} | ⛔ bez notatek: {no_notes}\n"
        f"📊 Zaktualizowane skille: {skills_updated}, w tym jako nice-to-have: {skills_nice}"
    )
    logging.info(end_text)
    notifier.send(end_text) 
    send = log_scraper.upload_logs_s3(s3, backup_type="scraper")
    if send:
        notifier.send("Logi scraper przesłane do S3")
    return True
#####################################################
def main():
    scheduler = TaskScheduler()
    # Uruchomienie głównego zadania
    try:
        # jobs_download()
        # jobs_sql()
        # sleep(3600)  # 1 godzina
        jobs_scraper()
        sleep(60*3) 
        # Dodajemy zadanie do harmonogramu, np. codziennie o 10:00
        # print("Uruchomiono harmonogram")
        # scheduler.add_daily_job("04:00", jobs_sql)
        # scheduler.add_daily_job("08:30", jobs_download)
        # scheduler.add_daily_job("13:15", jobs_scraper)
        # # # Uruchamiamy harmonogram
        # scheduler.run_pending()
    except Exception as e:
        logging.error(f"Błąd głównego zadania: {e}")
#####################################################
if __name__ == "__main__":
    main()
