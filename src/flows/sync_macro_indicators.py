from macro_markets.macro_indicators import get_macro_klines
from prefect import flow

from databases.doris import get_stream_loader
from utils.logger import logger as _logger
from utils.prefect_decorators import flow_timing

logger = _logger.bind(job_id="MACRO_INDICATORS")


@flow(name="sync-macro-indicators")
@flow_timing
async def sync_macro_indicators():
    logger.info("Starting sync_macro_indicators...")
    results = await get_macro_klines(logger)
    await get_stream_loader().send_rows(results, "macro_kline_raw_1m")


if __name__ == "__main__":
    import asyncio

    asyncio.run(sync_macro_indicators())
