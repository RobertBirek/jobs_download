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

load_dotenv()

LOCAL_DATA_FOLDER = Path("data/")
os.makedirs(LOCAL_DATA_FOLDER, exist_ok=True)

PROXY_URL = os.environ.get("PROXY_URL")

# Reset existing handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Configure logging to write to app.log
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG if needed
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


# Ustawienia
target_date_str = "2025-02-19"  # data, którą chcemy zebrać
target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
output_filename = f"dane_{target_date_str}.jsonl"  # plik, do którego zapiszemy wpisy

########################################################
def fetch_proxy_list():
    download_url = PROXY_URL
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
def save_page_by_date(offers, save_dir=LOCAL_DATA_FOLDER):
    # Słownik do śledzenia, które slugi już zostały zapisane dla danej daty
    seen_slugs = {}
    for offer in offers:
        published_at_str = offer.get("publishedAt")
        if not published_at_str:
            logging.error("Oferta bez daty:", offer)
            continue
        slug = offer.get("slug")
        if not slug:
            logging.error("Oferta bez klucza slug:", offer)
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
        output_filename = os.path.join(save_dir, f"justjoinit_{date_str}.jsonl")

        # Jeśli nie śledzimy jeszcze slugów dla tej daty, inicjujemy zbiór
        if date_str not in seen_slugs:
            seen_slugs[date_str] = set()
            # Opcjonalnie: jeśli plik już istnieje, wczytujemy z niego dotychczas zapisane slugi,
            # aby nie zapisać duplikatów przy wielokrotnym uruchomieniu programu.
            if os.path.exists(output_filename):
                with open(output_filename, "r", encoding="utf-8") as f_out:
                    for line in f_out:
                        try:
                            existing_offer = json.loads(line.strip())
                            existing_slug = existing_offer.get("slug")
                            if existing_slug:
                                seen_slugs[date_str].add(existing_slug)
                        except Exception as e:
                            logging.error("Błąd przy wczytywaniu oferty z istniejącego pliku:", e)
        # Sprawdzenie, czy oferta o danym slug już została dodana
        if slug in seen_slugs[date_str]:
            logging.info(f"Duplikat oferty z kluczem '{slug}' dla daty {date_str} - pomijam.")
            continue
        else:
            seen_slugs[date_str].add(slug)

        # Zapis oferty do pliku (każda oferta w osobnej linii - format JSON Lines)
        with open(output_filename, "a", encoding="utf-8") as out_file:
            json.dump(offer, out_file, ensure_ascii=False)
            out_file.write("\n")
        
        logging.info(f"Oferta z kluczem '{slug}' dodana do pliku {output_filename}.")
    return True

########################################################
def save_page(page, offers, save_dir=LOCAL_DATA_FOLDER):
    try:
        Path(save_dir).mkdir(parents=True, exist_ok=True)
        file_path = os.path.join(save_dir, f'justjoinit_offers__page_{page}.json')
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(offers, f, indent=4, ensure_ascii=False)
        logging.info(f"Strona {page} zapisana w {file_path}")
        return True
    except FileNotFoundError as e:
        logging.error(f"Nie można zapisać strony {page}: katalog nie istnieje. {e}")
        return False
    except PermissionError as e:
        logging.error(f"Brak uprawnień do zapisu strony {page} w {save_dir}. {e}")
        return False
    except TypeError as e:
        logging.error(f"Błąd w danych do zapisu dla strony {page}: {e}")
        return False
    except Exception as e:
        logging.error(f"Nieoczekiwany błąd przy zapisie strony {page}: {e}")
        return False
########################################################
def fetch_offers(start_page=1, offers_per_page_count=1, page_count=1, sleep=100):
    try:
        page = start_page
        offers, total_pages, total_offers, next_page = get_offers_justjoinit(page, offers_per_page_count)
        
        if offers:
            # save_page(page, offers)
            save_page_by_date(offers)
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

########################################################

fetch_offers(1,100,50)

# Ustaw pobieranie pliku codziennie o konkretnej godzinie, np. 10:30
# schedule.every().day.at("10:30").do(fetch_offers())

# logging.info("Uruchomiono planowanie pobierania. Program czeka na ustalony czas...")
# while True:
#     schedule.run_pending()
#     time.sleep(60)
