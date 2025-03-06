import os
import json
import requests
import schedule
import boto3
import time
import datetime
import random
import logging
from dotenv import load_dotenv
from pathlib import Path

# Ustawienie strefy czasowej
os.environ["TZ"] = "Europe/Warsaw"
time.tzset()  # Aktualizacja czasu dla procesu

# load_dotenv()
load_dotenv(dotenv_path=Path(".env"))

LOCAL_DATA_FOLDER = Path("data/")
os.makedirs(LOCAL_DATA_FOLDER, exist_ok=True)

ENDPOINT_URL = f"https://fra1.digitaloceanspaces.com"
BUCKET_NAME = "gotoit.robertbirek"

s3_client = boto3.client('s3', endpoint_url=ENDPOINT_URL)

PROXY_URL = os.environ.get("PROXY_URL")

LOG_FILE = "justjoinit.log"

########################################################
# Reset existing handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Configure logging to write to app.log
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG if needed
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
########################################################
def upload_logs_to_s3():
    today = datetime.date.today().strftime("%Y-%m-%d")  # Format: YYYY-MM-DD
    year, month, day = today.split('-')
    now = datetime.datetime.now()
    timestamp = now.strftime("%H-%M-%S")  # Znacznik czasowy w nazwie pliku

    # Nowa nazwa pliku z timestampem
    log_filename = f"{LOG_FILE.replace('.log', '')}_{timestamp}.log"

    s3_key = f"jobs/{year}/{month}/{day}/{log_filename}"
    # Sprawdzenie, czy plik logów istnieje
    if not os.path.exists(LOG_FILE):
        logging.warning(f"Plik logów {LOG_FILE} nie istnieje, pomijam wysyłkę do S3.")
        return
    try:
        s3_client.upload_file(LOG_FILE, BUCKET_NAME, s3_key)
        logging.info(f"Plik logów {LOG_FILE} wysłany do S3 jako {s3_key}")
    except Exception as e:
        logging.error(f"Błąd przy wysyłaniu logów do S3: {e}")
########################################################
def send_discord_notification(message):
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    if not webhook_url:
        logging.warning("Brak URL webhooka Discorda. Powiadomienie nie zostanie wysłane.")
        return
    
    payload = {"content": message}
    
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        logging.info("Powiadomienie Discord wysłane.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Błąd wysyłania powiadomienia na Discord: {e}")

########################################################
def fetch_proxy_list():
    download_url=PROXY_URL
    logging.info(download_url)
    try:
        # Pobranie listy proxy
        response = requests.get(download_url)
        response.raise_for_status()  # Sprawdzenie błędów HTTP

        # Rozdzielenie listy proxy na linie
        return response.text.strip().split("\n")
    except Exception as e:
        logging.error(f"Wystąpił błąd podczas pobierania listy proxy: {e}")
        return []
########################################################
def get_random_proxy():
    proxy_list = fetch_proxy_list()
    if proxy_list:
        random_proxy = random.choice(proxy_list)
        
        parts = random_proxy.split(":")
        
        ip = parts[0]
        port = parts[1]
        username = parts[2] #if len(parts) > 2 else None
        password = parts[3] #if len(parts) > 3 else None

        proxy_url = f"http://{username}:{password}@{ip}:{port}"
        
        return proxy_url     
    
    return None
########################################################
def get_offers_justjoinit(page=1,offers_per_page=1):
    base_url = 'https://api.justjoin.it/v2/user-panel/offers'
    headers = {"Version": "2"}
    params = {
        "sortBy": "published",
        "orderBy": "DESC",
        "perPage": offers_per_page,
        "page": page,
        "salaryCurrencies": "PLN"
    }

    proxy_url = get_random_proxy()
    proxies = {
        "http": proxy_url
    }

    logging.info(f"Strona {page} zapisana przez proxy {proxy_url}")

    try:
        response = requests.get(base_url, headers=headers, params=params, proxies=proxies, timeout=10)
        response.raise_for_status()
        try:
            response_json = response.json()
        except ValueError as e:
            logging.error(f"Error parsing JSON response for page {page}: {e}")
            return None, 0, 0, None
        
        meta = response_json.get("meta", {})
        total_pages = meta.get("totalPages", 0)
        total_offers = meta.get("totalItems", 0)
        next_page = meta.get("nextPage", "null")
        offers = response_json.get("data", [])
        logging.info(f"Total pages: {total_pages}, Total offers: {total_offers}, Current page: {page}, Offerts per page: {offers_per_page}, Next page: {next_page}")
        return offers, total_pages, total_offers, next_page
    
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP request error on page {page}: {e}")
        return None, 0, 0, None
########################################################
def save_offers_local_by_date(offers, save_dir=LOCAL_DATA_FOLDER):
    # Słownik do śledzenia, które slugi już zostały zapisane dla danej daty
    
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    
    seen_slugs = {}
    total_offers = len(offers)
    duplicate_count = 0

    for offer in offers:
        published_at_str = offer.get("publishedAt")
        slug = offer.get("slug")

        if not published_at_str or not slug:
            logging.error(f"Niepoprawna oferta: {offer}")
            continue
        
        try:
            # Konwersja daty ze standardu ISO
            published_date = datetime.datetime.fromisoformat(published_at_str.replace("Z", "+00:00")).date()
            # published_date = datetime.datetime.fromisoformat(published_at_str).date()
            date_str = published_date.isoformat()  # Format: YYYY-MM-DD
        except Exception as e:
            logging.error("Błąd przetwarzania daty dla oferty:", offer, e)
            continue

        # Nazwa pliku wynikowego dla danej daty
        # output_filename = f"justjoinit_{date_str}.jsonl"
        # output_filename = os.path.join(save_dir, f"justjoinit_{date_str}.jsonl")
        output_filename = save_dir / f"justjoinit_{date_str}.jsonl"

        # Jeśli nie śledzimy jeszcze slugów dla tej daty, inicjujemy zbiór
        if date_str not in seen_slugs:
            seen_slugs[date_str] = set()
            # Opcjonalnie: jeśli plik już istnieje, wczytujemy z niego dotychczas zapisane slugi,
            # aby nie zapisać duplikatów przy wielokrotnym uruchomieniu programu.
            if output_filename.exists():
                with output_filename.open("r", encoding="utf-8") as f_out:
                    for line in f_out:
                        try:
                            # existing_offer = json.loads(line.strip())
                            # existing_slug = existing_offer.get("slug")
                            existing_slug = json.loads(line.strip()).get("slug")
                            if existing_slug:
                                seen_slugs[date_str].add(existing_slug)
                        except Exception as e:
                            logging.error("Błąd przy wczytywaniu oferty z istniejącego pliku:", e)
        # Sprawdzenie, czy oferta o danym slug już została dodana
        if slug in seen_slugs[date_str]:
            logging.info(f"Duplikat oferty '{slug}' dla daty {date_str} - pomijam.")
            duplicate_count += 1
            continue
        
        seen_slugs[date_str].add(slug)

        # Zapis oferty do pliku (każda oferta w osobnej linii - format JSON Lines)
        with output_filename.open("a", encoding="utf-8") as out_file:
            json.dump(offer, out_file, ensure_ascii=False)
            out_file.write("\n")
        
        logging.info(f"Oferta z kluczem '{slug}' dodana do pliku {output_filename}.")

    if total_offers == duplicate_count:
        logging.info(f"Same duplikaty, przerywam.")
        return False
    return True
########################################################
def save_offers_s3_by_date(offers, bucket_name, s3_client):
    # Grupujemy oferty według daty publikacji
    offers_by_date = {}
    
    total_offers = len(offers)
    duplicate_count = 0

    for offer in offers:
        published_at_str = offer.get("publishedAt")
        slug = offer.get("slug")

        if not published_at_str or not slug:
            logging.error(f"Niepoprawna oferta: {offer}")
            continue
        
        try:
            # Konwersja daty ze standardu ISO
            published_date = datetime.datetime.fromisoformat(published_at_str.replace("Z", "+00:00")).date()
            date_str = published_date.isoformat()  # Format: YYYY-MM-DD
        except Exception as e:
            logging.error(f"Błąd przetwarzania daty dla oferty: {offer}: {e}")
            continue
        
        offers_by_date.setdefault(date_str, []).append(offer)

    # Przetwarzamy oferty dla każdej daty osobno
    for date_str, offers_list in offers_by_date.items():
        year, month, day = date_str.split('-')
        # Ustalanie klucza na S3: np. jobs/2025/03/05/justjoinit.jsonl
        output_filename = f"justjoinit_{date_str}.jsonl"
        key = f"jobs/{year}/{month}/{day}/{output_filename}"
        
        seen_slugs = set()
        existing_content = ""
        
        # Próba pobrania istniejącego obiektu z S3
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            existing_content = response['Body'].read().decode('utf-8')
            # Parsowanie istniejących ofert, aby wyłapać duplikaty
            for line in existing_content.splitlines():
                try:
                    existing_offer = json.loads(line)
                    existing_slug = existing_offer.get("slug")
                    if existing_slug:
                        seen_slugs.add(existing_slug)
                except Exception as e:
                    logging.error(f"Błąd przy wczytywaniu oferty z S3 {key}: {e}")
        except s3_client.exceptions.NoSuchKey:
            logging.info(f"Obiekt {key} nie istnieje. Zostanie utworzony nowy.")
        except Exception as e:
            logging.error(f"Błąd przy pobieraniu obiektu {key} z S3: {e}")

        new_lines = []
        for offer in offers_list:
            slug = offer.get("slug")
            if slug in seen_slugs:
                logging.info(f"Duplikat oferty '{slug}' dla daty {date_str} - pomijam.")
                duplicate_count += 1 # Zliczanie duplikatów
                continue
            seen_slugs.add(slug)
            new_lines.append(json.dumps(offer, ensure_ascii=False))

        if new_lines:
            if existing_content and not existing_content.endswith("\n"):
                existing_content += "\n"
            updated_content = existing_content + "\n".join(new_lines) + "\n"
            try:
                s3_client.put_object(Bucket=bucket_name, Key=key, Body=updated_content.encode('utf-8'))
                logging.info(f"Zapisano {len(new_lines)} nowych ofert do obiektu S3: {key}.")
            except Exception as e:
                logging.error(f"Błąd przy zapisywaniu obiektu {key} do S3: {e}")
        else:
            logging.info(f"Wszystkie oferty dla daty {date_str} są duplikatami.")

    if total_offers == duplicate_count:
        logging.info(f"Same duplikaty, przerywam.")
        return False
    return True

########################################################
def fetch_offers(start_page=1, offers_per_page_count=1, page_count=1, sleep=100):
    try:
        page = start_page
        offers, total_pages, total_offers, next_page = get_offers_justjoinit(page, offers_per_page_count)
        
        if offers:
            # saved = save_offers_local_by_date(offers)
            saved = save_offers_s3_by_date(offers, BUCKET_NAME, s3_client)
            if saved:
                if next_page is not None and next_page != "null":
                    if page_count > 1:
                        rsleep = random.randint(1, sleep)
                        time.sleep(rsleep)
                        fetch_offers(int(next_page), offers_per_page_count, page_count - 1, sleep)
                    else:
                        logging.info("Reached the user-defined page limit.")
                else:
                    logging.info(f"End of data at page {page}. Total pages: {total_pages}, Total offers: {total_offers}.")
            else:
                send_discord_notification("Downloads offers is ok today")
                upload_logs_to_s3()
                raise Exception("Duplicates only, stopping.")
        else:
            sleep *=10
            if sleep < 1000:
                rsleep = random.randint(1, sleep)
                time.sleep(rsleep)
                logging.error(f"An error occurred while fetching page {page}.")
                fetch_offers(int(start_page), offers_per_page_count, page_count, rsleep)
            else:
                logging.error(f"An error occurred while fetching page {page}, going to next page {page + 1}")
                fetch_offers(int(start_page) + 1, offers_per_page_count, page_count - 1, 10)
    except Exception as e:
        logging.error(f"Critical error while fetching offers: {e}")
        if "Duplicates only" in str(e):
            return

########################################################

# fetch_offers(1,100,10)

# Ustaw pobieranie pliku codziennie o konkretnej godzinie, np. 10:30
schedule.every().day.at("12:30").do(fetch_offers,1,100,50)

logging.info("Uruchomiono planowanie pobierania. Program czeka na ustalony czas...")
while True:
    schedule.run_pending()
    time.sleep(1)
