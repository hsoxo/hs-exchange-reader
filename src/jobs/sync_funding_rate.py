import asyncio

from exchanges._base_ import BaseClient
from exchanges.binance import BinancePerpClient
from exchanges.bitget import BitgetPerpClient
from exchanges.bybit import BybitPerpClient
from exchanges.okx import OkxPerpClient
from utils.logger import logger as _logger


async def update_funding_rate(client: BaseClient):
    await client.update_funding_rate()


async def sync_funding_rate():
    logger = _logger.bind(job_id="FUNDING_RATE")
    clients: list[BaseClient] = [
        BinancePerpClient(logger),
        BitgetPerpClient(logger),
        BybitPerpClient(logger),
        OkxPerpClient(logger),
    ]

    await asyncio.gather(*(update_funding_rate(client) for client in clients))


if __name__ == "__main__":
    asyncio.run(sync_funding_rate())
