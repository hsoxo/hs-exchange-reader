from macro_markets.macro_indicators import get_macro_klines

from databases.doris import get_stream_loader
from utils.logger import logger as _logger

logger = _logger.bind(job_id="MACRO_INDICATORS")


async def sync_macro_indicators():
    logger.info("Starting sync_macro_indicators...")
    results = await get_macro_klines(logger)
    await get_stream_loader().send_rows(results, "macro_kline_raw_1m")


if __name__ == "__main__":
    import asyncio

    asyncio.run(sync_macro_indicators())
