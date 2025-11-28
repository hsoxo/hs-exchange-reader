from utils.logger import logger  # noqa: I001

import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from jobs.sync_funding_rate import sync_funding_rate
from jobs.sync_klines import sync_klines_1h, sync_klines_1m
from jobs.sync_long_short_ratio import sync_long_short_ratio_1d, sync_long_short_ratio_1h, sync_long_short_ratio_5m
from jobs.sync_symbols import sync_symbols
from utils.http_session import shutdown
from utils.start_logo import print_banner


async def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(sync_symbols, "interval", days=1, max_instances=1)

    scheduler.add_job(sync_klines_1m, "interval", days=1, max_instances=1)
    scheduler.add_job(sync_klines_1h, "interval", days=1, max_instances=1)

    scheduler.add_job(
        sync_long_short_ratio_5m,
        "cron",
        minute="*/5",
        second=5,
        max_instances=1,
        misfire_grace_time=30,
        coalesce=True,
    )

    scheduler.add_job(
        sync_long_short_ratio_1h,
        "cron",
        minute=0,
        second="5, 30",
        max_instances=1,
        misfire_grace_time=60,
        coalesce=True,
    )

    scheduler.add_job(
        sync_long_short_ratio_1d,
        "cron",
        hour=0,
        minute=0,
        second="5, 30",
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )

    scheduler.add_job(
        sync_funding_rate,
        "cron",
        minute="0,1,5,30",
        second="5",
        misfire_grace_time=60,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()

    await asyncio.Event().wait()  # 防止退出


if __name__ == "__main__":
    print_banner()
    logger.info("Starting scheduler...")
    asyncio.run(main())
    shutdown()
