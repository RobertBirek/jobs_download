import os
from dotenv import load_dotenv
import requests
import random
import logging

load_dotenv()

class ProxyManager:
    def __init__(self):
        self.proxy_url = os.environ.get("PROXY_URL")
        self.proxy_list = self.fetch_proxy_list()  # zapisanie listy do zmiennej
    #####################################
    def fetch_proxy_list(self):
        try:
            response = requests.get(self.proxy_url)
            response.raise_for_status()  # Sprawdzenie błędów HTTP
            # Rozdzielenie listy proxy na linie
            return response.text.strip().split("\n")
        except Exception as e:
            logging.error(f"Wystąpił błąd podczas pobierania listy proxy: {e}")
            return []
    #####################################
    def get_random_proxy(self):
        if not self.proxy_list:
            self.proxy_list = self.fetch_proxy_list()

        proxy_list = self.proxy_list
        if proxy_list:
            random_proxy = random.choice(proxy_list)
            parts = random_proxy.split(":")
            if len(parts) >= 4:
                ip = parts[0]
                port = parts[1]
                username = parts[2]
                password = parts[3]
                proxy_url = f"http://{username}:{password}@{ip}:{port}"
                return proxy_url
            else:
                logging.error("Nieprawidłowy format danych proxy.")
                return None
        return None
    #####################################