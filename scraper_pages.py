import requests
import requests_cache
import logging
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class Pages:
    def __init__(self, proxy_manager):
        self.proxy_manager = proxy_manager
        requests_cache.install_cache("justjoin_cache", backend="sqlite", expire_after=86400)
    ##################################################
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=10, max=60),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True
    )
    def get_page(self, url):
        try:
            proxy_url = self.proxy_manager.get_random_proxy()

            if proxy_url is None:
                response = requests.get(url, timeout=10)
                logging.info(f"Pobieranie strony {url} bez użycia proxy")
            else:
                proxies = {
                    "http": proxy_url
                }
                response = requests.get(url, proxies=proxies, timeout=10)
                logging.info(f"Pobieranie strony {url} przy użyciu proxy {proxy_url}")

            if response.status_code == 404:
                raise ValueError("Not Found (404)")
            response.raise_for_status()
            
            if response.text is None:
                raise ValueError("Failed to fetch page content")
            
            return response.text
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            return None
    ##################################################
    def page_getfrom_css(self, text, css_selector):
        try:
            soup = BeautifulSoup(text, 'html.parser')
            for tag in soup(['style', 'script']):
                tag.decompose()

            css_content = soup.select(css_selector)

            return css_content
        except Exception as e:
            logging.error(f"Error parsing page: {e}")
            return None
    ##################################################
    def get_page_notes(self, url):
        try:
            text = self.get_page(url)
            if text is None:
                raise ValueError("Failed to fetch page content")
            
            sections = self.page_getfrom_css(text, 'div.MuiBox-root.css-16nvqld')
            
            tech_stack_div = None
            job_description_div = None

            for div in sections:
                header = div.find("h3")
                if header:
                    if "tech stack" in header.text.lower():
                        tech_stack_div = div
                    elif "description" in header.text.lower() or "job description" in header.text.lower():
                        job_description_div = div

            if not tech_stack_div and not job_description_div:
                raise ValueError(f"Nie znaleziono wymaganych sekcji (tech stack / job description) na stronie {url}")

            notes = f'<div class="offer">\n<div class="job_techstack">\n{tech_stack_div}\n</div>\n<div class="job_description">\n{job_description_div}\n</div>\n</div>'

            return notes
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            return None
    ##################################################
    def get_skill_levels(self, url, skill_names):
        try:
            text = self.get_page(url)
            if text is None:
                raise ValueError("Failed to fetch page content")
            
            sections = self.page_getfrom_css(text, 'div.css-qsaw8')

            skill_levels = {}

            for section in sections:
                h4 = section.find('h4')
                ul = section.find('ul', class_='css-1qii1b7')

                if not h4 or not ul:
                    continue

                skill_name = h4.get_text(strip=True).lower()
                if skill_name in [s.lower() for s in skill_names]:
                    lis = ul.find_all('li')
                    level = sum(1 for li in lis if 'css-j1kr6i' in li.get('class', []))
                    skill_levels[h4.get_text(strip=True)] = level

            return skill_levels
        except Exception as e:
            logging.error(f"Error extracting skill levels from {url}: {e}")
            return {}

    ##################################################
    def extract_description_text(self, url):
        try:
            text = self.get_page(url)
            if text is None:
                raise ValueError("Failed to fetch page content")

            sections = self.page_getfrom_css(text, 'div.MuiBox-root.css-16nvqld')
            
            job_description_div = None

            for div in sections:
                header = div.find("h3")
                if header:
                    if "description" in header.text.lower() or "job description" in header.text.lower():
                        job_description_div = div

            if not job_description_div:
                raise ValueError(f"Nie znaleziono wymaganych sekcji (job description) na stronie {url}")

            job_description = job_description_div.get_text(separator='\n', strip=True)
            return job_description
        except Exception as e:
            logging.error(f"Error extracting job description from {url}: {e}")
            return None