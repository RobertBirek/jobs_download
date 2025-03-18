import os
import json
import requests
import time
import datetime
from dotenv import load_dotenv
from pathlib import Path
import logging
from botocore.exceptions import ClientError
from proxy_manager import ProxyManager
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class JustJoinClient:
    def __init__(self,offers_per_page=1):
        self.base_url = "https://api.justjoin.it/v2/user-panel/offers"
        self.proxy_manager = ProxyManager()
        self.total_offers = 0
        self.offers_per_page = offers_per_page
        self.total_pages = 0
        self.current_page = 1
        self.next_page = 1
        self.offers = []
        self.offers, self.total_pages, self.total_offers, self.next_page = self.get_page(self.current_page)

    #####################################
    @retry(
        stop=stop_after_attempt(5),  # Maksymalnie 5 prób
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Wykładnicze opóźnienie między próbami (2s, 4s, 8s...)
        retry=retry_if_exception_type(requests.exceptions.RequestException),  # Ponawiaj tylko w przypadku błędów sieciowych
        reraise=True  # Jeśli po 5 próbach nadal jest błąd, rzuć wyjątek
    )
    def get_page(self,page=1):
        headers = {"Version": "2"}
        params = {
            "sortBy": "published",
            "orderBy": "DESC",
            "perPage": self.offers_per_page,
            "page": page,
            "salaryCurrencies": "PLN"
        }
        proxy_url = self.proxy_manager.get_random_proxy()
        try:
            if proxy_url is None:
                response = requests.get(self.base_url, headers=headers, params=params, timeout=10)
                logging.info(f"Pobieranie strony {page} bez użycia proxy")              
            else:
                proxies = {
                    "http": proxy_url
                }
                response = requests.get(self.base_url, headers=headers, params=params, proxies=proxies, timeout=10)
                logging.info(f"Pobieranie strony {page} przy użyciu proxy {proxy_url}")
            response.raise_for_status()
            try:
                response_json = response.json()
            except ValueError as e:
                logging.error(f"Błąd parsowania JSON dla strony {page}: {e}")
                return None, 0, 0, None
            
            meta = response_json.get("meta", {})
            self.total_pages = meta.get("totalPages", 0)
            self.total_offers = meta.get("totalItems", 0)
            self.next_page = meta.get("nextPage", "null")
            self.offers = response_json.get("data", [])
            logging.info(
                f"Oferty: {self.total_offers}, strony: {self.total_pages}, oferty na stronę: {self.offers_per_page}, "
                f"aktualna strona: {self.current_page}, następna strona: {self.next_page}"
            )
            return self.offers, self.total_pages, self.total_offers, self.next_page
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Błąd HTTP przy pobieraniu strony {page}: {e}")
            raise
            # return None, 0, 0, None
    #####################################
    def save_offers_local(self,local_path,offers):
        save_dir = Path(local_path)
        save_dir.mkdir(parents=True, exist_ok=True)
        
        seen_slugs = {}
        total_offers = len(offers)
        duplicate_count = 0

        for offer in offers:
            published_at_str = offer.get("publishedAt")
            slug = offer.get("slug")

            if not published_at_str or not slug:
                logging.warning(f"Niepoprawna oferta: {offer}")
                continue
            try:
                # Konwersja daty ze standardu ISO
                published_date = datetime.datetime.fromisoformat(published_at_str.replace("Z", "+00:00")).date()
                # published_date = datetime.datetime.fromisoformat(published_at_str).date()
                date_str = published_date.isoformat()  # Format: YYYY-MM-DD
            except Exception as e:
                logging.error(f"Błąd przetwarzania daty dla oferty {offer}: {e}")
                continue

            # Nazwa pliku wynikowego dla danej daty
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
                logging.warning(f"Duplikat oferty '{slug}' dla daty {date_str} - pomijam.")
                duplicate_count += 1
                continue
            
            seen_slugs[date_str].add(slug)

            # Zapis oferty do pliku (każda oferta w osobnej linii - format JSON Lines)
            with output_filename.open("a", encoding="utf-8") as out_file:
                json.dump(offer, out_file, ensure_ascii=False)
                out_file.write("\n")
            
            logging.info(f"Oferta z kluczem '{slug}' dodana do pliku {output_filename}.")

        if total_offers == duplicate_count:
            logging.warning("Wszystkie oferty to duplikaty, przerwanie zapisu.")
            return False
        return True
    #####################################
    def save_offers_s3(self,s3_client,offers):
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
            # key = f"jobs/{year}/{month}/{day}/{output_filename}"
            s3_key = f"jobs/year={year}/month={month}/day={day}/{output_filename}"
            
            seen_slugs = set()
            existing_content = ""
            
            # Próba pobrania istniejącego obiektu z S3
            try:
                #response = s3_client.get_object(s3_key)
                response = s3_client.get_file(s3_key)
                if not response:
                    logging.info(f"Obiekt {s3_key} nie istnieje lub wystąpił błąd. Zostanie utworzony nowy.")
                    existing_content = ""
                else:
                    existing_content = response['Body'].read().decode('utf-8')
                # Parsowanie istniejących ofert, aby wyłapać duplikaty
                for line in existing_content.splitlines():
                    try:
                        existing_offer = json.loads(line)
                        existing_slug = existing_offer.get("slug")
                        if existing_slug:
                            seen_slugs.add(existing_slug)
                    except Exception as e:
                        logging.error(f"Błąd przy wczytywaniu oferty z S3 {s3_key}: {e}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    logging.info(f"Obiekt {s3_key} nie istnieje. Zostanie utworzony nowy.")
                    existing_content = ""
                else:
                    logging.error(f"Błąd przy pobieraniu obiektu {s3_key} z S3: {e}")
                    existing_content = ""
            except Exception as e:
                logging.error(f"Błąd przy pobieraniu obiektu {s3_key} z S3: {e}")
                existing_content = ""

            new_lines = []
            for offer in offers_list:
                slug = offer.get("slug")
                if slug in seen_slugs:
                    logging.warning(f"Duplikat oferty '{slug}' dla daty {date_str} - pomijam.")
                    duplicate_count += 1 # Zliczanie duplikatów
                    continue
                seen_slugs.add(slug)
                new_lines.append(json.dumps(offer, ensure_ascii=False))

            if new_lines:
                if existing_content and not existing_content.endswith("\n"):
                    existing_content += "\n"
                updated_content = existing_content + "\n".join(new_lines) + "\n"
                try:
                    s3_client.put_file(s3_key,updated_content.encode('utf-8'))
                    logging.info(f"Zapisano {len(new_lines)} nowych ofert do obiektu S3: {s3_key}.")
                except Exception as e:
                    logging.error(f"Błąd przy zapisywaniu obiektu {s3_key} do S3: {e}")
            else:
                logging.warning(f"Wszystkie oferty dla daty {date_str} są duplikatami.")

        if total_offers == duplicate_count:
            logging.warning("Wszystkie oferty to duplikaty, przerwanie zapisu do S3.")
            return False
        return True
