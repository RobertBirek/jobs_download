import os
import requests
from dotenv import load_dotenv
import logging

load_dotenv()

class DiscordNotifier:
    def __init__(self):
        self.webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    #####################################    
    def send(self, message):
        if not self.webhook_url:
            logging.warning("Brak webhooka Discorda")
            return   
        try:
            requests.post(self.webhook_url, json={"content": message})
            logging.info("Wysłano powiadomienie")
        except Exception as e:
            logging.error(f"Błąd Discorda: {e}")