# app.py
import os
import time
from dotenv import load_dotenv
from pathlib import Path
from log_manager import LogManager
from proxy_manager import ProxyManager
from justjoin_client import JustJoinClient
from notification import DiscordNotifier
from scheduler import TaskScheduler

# Konfiguracja środowiska
load_dotenv(Path(".env"))

# Inicjalizacja komponentów
log_manager = LogManager()

proxy_manager = ProxyManager(
    proxy_url=os.getenv("PROXY_URL")
)

api = JustJoinAPI(proxy_manager)
notifier = DiscordNotifier(os.getenv("DISCORD_WEBHOOK_URL"))

def main_job():
    try:
        result = api.fetch_offers(page=1, per_page=100)
        # Tutaj dodaj logikę zapisu ofert
        notifier.send("Pobrano nowe oferty!")
        log_manager.upload_logs()
    except Exception as e:
        logging.error(f"Błąd głównego zadania: {e}")

if __name__ == "__main__":
    scheduler = TaskScheduler()
    scheduler.add_daily_job("12:30", main_job)
    logging.info("Uruchomiono harmonogram")
    scheduler.run_pending()