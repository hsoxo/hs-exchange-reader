import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger as _logger

from jobs.sync_klines import sync_klines_1h, sync_klines_1m
from jobs.sync_symbols import sync_symbols


def ensure_extra_fields(record):
    for key in ("job_id", "exchange", "inst_type", "symbol"):
        record["extra"].setdefault(key, "")
    return record


_logger = _logger.patch(ensure_extra_fields)

LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level:<5}</level> | "
    "<cyan>{extra[job_id]}</cyan> "
    "[{extra[inst_type]}:{extra[exchange]:<7}]{extra[symbol]} | "
    "<level>{message}</level>"
)

_logger.remove()
_logger.add(sys.stdout, format=LOG_FORMAT, enqueue=True)

scheduler = BlockingScheduler()
scheduler.add_job(sync_klines_1m, "interval", days=1, max_instances=1)
scheduler.add_job(sync_klines_1h, "interval", days=1, max_instances=1)
scheduler.add_job(sync_symbols, "interval", days=1, max_instances=1)

if __name__ == "__main__":
    logger = _logger.bind(job_id="MAIN")
    logger.info("Starting scheduler...")
    scheduler.start()
