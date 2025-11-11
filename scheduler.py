from apscheduler.schedulers.blocking import BlockingScheduler
from jobs.sync_ds import main as sync_ds
from loguru import logger

scheduler = BlockingScheduler()
scheduler.add_job(sync_ds, "interval", minutes=5)

if __name__ == "__main__":
    logger.info("Starting scheduler...")
    scheduler.start()