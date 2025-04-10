import logging
import os
import datetime

class LogManager:
    def __init__(self, log_file="justjoinit.log"):
        self.log_file = log_file
        self._configure_logging()
    ####################################################
    def _configure_logging(self):
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding="utf-8"),
                logging.StreamHandler()
            ]
        )
    ####################################################
    def upload_logs_s3(self,s3_client, backup_type="download"):
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        log_filename = f"{self.log_file.replace('.log', '')}_{timestamp}.log"
        
        if backup_type == "download":
            year = now.strftime("%Y")
            month = now.strftime("%m")
            day = now.strftime("%d")
            s3_key = f"jobs/year={year}/month={month}/day={day}/{log_filename}"
        elif backup_type == "sql":
            s3_key = f"jobs/sql/logs/{log_filename}"
        elif backup_type == "scraper":
            s3_key = f"jobs/scraper/logs/{log_filename}"
        
        if not os.path.exists(self.log_file):
            logging.warning(f"Plik logów {self.log_file} nie istnieje")
            return
        try:
            s3_client.upload_file(self.log_file, s3_key)
            logging.info(f"Logi przesłane do S3: {s3_key}")
            with open(self.log_file, 'w', encoding="utf-8") as f:
                pass # Nic nie robimy, plik jest otwarty w trybie zapisu i natychmiast zamykany
            logging.info(f"Plik lokalny {self.log_file} został wyczyszczony.")
            return True
        except Exception as e:
            logging.error(f"Błąd przesyłania logów: {e}")
            return False
    ####################################################