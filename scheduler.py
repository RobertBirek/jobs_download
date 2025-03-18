import schedule
import time
import logging
from functools import partial

class TaskScheduler:
    def __init__(self):
        self.jobs = []
        
    def add_daily_job(self, time, job):
        job_name = getattr(job, '__name__', str(job))  # Pobranie nazwy funkcji lub zamiana na string
        self.jobs.append(schedule.every().day.at(time).do(job))
        logging.info(f"Zadanie dodane: {job_name} o {time}")
    def run_pending(self):
        logging.info("Scheduler uruchomiony. Oczekiwanie na zadania...")
        while True:
            schedule.run_pending()
            time.sleep(1)