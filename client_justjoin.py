import os
import json
import requests
import requests_cache
import time
import datetime
from dotenv import load_dotenv
from pathlib import Path
import logging
from botocore.exceptions import ClientError
from proxy_manager import ProxyManager
import random
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from scraper_db import Database
from scraper_pages import Pages
from scraper_parser_gpt import OfferParserGPT

class JustJoinClient:
    def __init__(self,offers_per_page=1):
        requests_cache.install_cache("justjoin_cache", backend="sqlite", expire_after=86400)
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
        wait=wait_exponential(multiplier=1, min=7, max=23),  # Wykładnicze opóźnienie między próbami (2s, 4s, 8s...)
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
        saved_offers = 0
        duplicate_offers = 0

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
                duplicate_offers += 1
                continue
            
            seen_slugs[date_str].add(slug)

            # Zapis oferty do pliku (każda oferta w osobnej linii - format JSON Lines)
            with output_filename.open("a", encoding="utf-8") as out_file:
                json.dump(offer, out_file, ensure_ascii=False)
                out_file.write("\n")
                saved_offers += 1
            
            logging.info(f"Oferta z kluczem '{slug}' dodana do pliku {output_filename}.")

        if total_offers == duplicate_offers:
            logging.warning("Wszystkie oferty to duplikaty, przerwanie zapisu.")
            return False, saved_offers, duplicate_offers
        return True, saved_offers, duplicate_offers
    #####################################
    def save_offers_s3(self,s3_client,offers):
        # Grupujemy oferty według daty publikacji
        offers_by_date = {}
        total_offers = len(offers)
        saved_offers = 0
        duplicate_offers = 0

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
                    duplicate_offers += 1 # Zliczanie duplikatów
                    continue
                seen_slugs.add(slug)
                new_lines.append(json.dumps(offer, ensure_ascii=False))

            if new_lines:
                if existing_content and not existing_content.endswith("\n"):
                    existing_content += "\n"
                updated_content = existing_content + "\n".join(new_lines) + "\n"
                try:
                    s3_client.put_file(s3_key,updated_content.encode('utf-8'))
                    saved_offers += len(new_lines)
                    logging.info(f"Zapisano {len(new_lines)} nowych ofert do obiektu S3: {s3_key}.")
                except Exception as e:
                    logging.error(f"Błąd przy zapisywaniu obiektu {s3_key} do S3: {e}")
            else:
                logging.warning(f"Wszystkie oferty dla daty {date_str} są duplikatami.")

        if total_offers == duplicate_offers:
            logging.warning("Wszystkie oferty to duplikaty, przerwanie zapisu do S3.")
            return False, saved_offers, duplicate_offers
        return True, saved_offers, duplicate_offers
    #####################################
    def scrape_offer_details(self, db_url: str, delay_range=(2, 10)):
        db = Database(db_url)
        pages = Pages(self.proxy_manager)

        slugs = db.get_unscraped_slugs()
        logging.info(f"Pobrano {len(slugs)} slugów do przetworzenia.")

        total = len(slugs)
        success = 0
        errors = 0
        no_notes = 0
        skills_updated = 0
        skills_nice_to_have = 0

        for slug_entry in slugs:
            time.sleep(random.uniform(*delay_range))
            slug = slug_entry.slug
            offer_id = slug_entry.offer_id
            url = f"https://justjoin.it/job-offer/{slug}"

            try:
                logging.info(f"[START] Przetwarzanie oferty {slug} (offer_id={offer_id})")
                notes = pages.get_page_notes(url)
                if notes is None:
                    no_notes += 1
                    raise ValueError("Failed to fetch page content")

                raw_text = pages.extract_description_text(url)
                parsed = OfferParserGPT(raw_text or "").parse()

                db.save_scraper_entry(
                    offer_id=offer_id,
                    status="ok",
                    url=url,
                    notes=notes,
                    experience_description=parsed.experience_description if parsed else None,
                    years_of_experience=parsed.years_of_experience if parsed else None,
                    interview_mode=parsed.interview_mode if parsed else None,
                    position_title=parsed.position_title if parsed else None,
                    position_level=parsed.position_level if parsed else None,
                    responsibilities=parsed.responsibilities if parsed else None,
                    requirements=parsed.requirements if parsed else None,
                    benefits=parsed.benefits if parsed else None,
                    industry=parsed.industry if parsed else None,
                    company_size=parsed.company_size if parsed else None
                )

                logging.info(f"Zapisano notatki i dane strukturalne dla oferty {offer_id}")
                success += 1

                required_skills = db.get_required_skills_for_offer(offer_id)
                skill_names = [s.name for s in required_skills]
                logging.info(f"Wymagane skille: {skill_names}")

                skill_levels = pages.get_skill_levels(url, skill_names)
                logging.info(f"Znalezione poziomy skilli: {skill_levels}")

                for skill in required_skills:
                    level = skill_levels.get(skill.name, None)
                    if level is not None:
                        logging.info(f"Aktualizuję skill '{skill.name}' (id={skill.id}) do poziomu {level}")
                        db.update_skill_level(offer_id, skill.id, level)
                        skills_updated += 1
                        if level == 1:
                            logging.info(f"Dodaję '{skill.name}' do nice-to-have (level=1)")
                            db.add_or_update_nice_to_have_skill(offer_id, skill.id, level)
                            skills_nice_to_have += 1

                logging.info(f"[OK] {slug}")

            except Exception as e:
                logging.error(f"[ERROR] {slug}: {e}")
                db.save_scraper_entry(offer_id, "error", url, str(e))
                errors += 1

        return total, success, errors, no_notes, skills_updated, skills_nice_to_have