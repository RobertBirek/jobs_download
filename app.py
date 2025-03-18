import time
from pathlib import Path
import logging
import random
from dotenv import load_dotenv
from log_manager import LogManager
from notification import DiscordNotifier
from justjoin_client import JustJoinClient
from s3_client import S3Client
from scheduler import TaskScheduler
from functools import partial

load_dotenv()

LOCAL_DATA_FOLDER = Path("data/")

log = LogManager("justjoinit.log")

scheduler = TaskScheduler()

##########################################################################################
def fetch_offers_from_jj(ppage=100):
    s3 = S3Client()
    notifier = DiscordNotifier()
    notifier.send("Pobieranie ofert z JustJoin.it")
    logging.info("Pobieranie ofert z JustJoin.it")
    jjc = JustJoinClient(offers_per_page=ppage)
    current_page = 1
    sleep = 15
    while True:
        logging.info(f"Pobieranie ofert ze strony {current_page}...")
        offers, total_pages, total_offers, next_page = jjc.get_page(current_page)
        
        # Sprawdzenie, czy są oferty do przetworzenia
        if offers is None or total_offers == 0:
            logging.info("Brak ofert do pobrania.")
            break

        # Zapis ofert lokalnie
        # success = jjc.save_offers_local(LOCAL_DATA_FOLDER, offers)
        # Zapis ofert do s3
        success = jjc.save_offers_s3(s3, offers)
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
        

    logging.info("Zakończono pobieranie ofert z JustJoin.it")  
    send = log.upload_logs_s3(s3)
    if send:
        notifier.send("Zakończono pobieranie ofert z JustJoin.it. Logi przesłane do S3")
    

#####################################################
# fetch_offers_from_jj(100)

# Dodajemy zadanie do harmonogramu, np. codziennie o 10:00
scheduler.add_daily_job("12:30", fetch_offers_from_jj)
# scheduler.add_daily_job("10:00", lambda: fetch_offers_from_jj(100))
# scheduler.add_daily_job("12:30", partial(fetch_offers_from_jj, 100))
# Uruchamiamy harmonogram
scheduler.run_pending()