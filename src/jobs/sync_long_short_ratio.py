import asyncio
from typing import Literal

from constants import InstType

from exchanges._base_ import BaseClient
from exchanges.binance import BinancePerpClient
from exchanges.bitget import BitgetPerpClient
from exchanges.bybit import BybitPerpClient
from exchanges.okx import OkxPerpClient
from utils.logger import logger as _logger

from .constants import COINS
from .utils import get_symbols


async def update_long_short_ratio(client: BaseClient, coins: [str], interval: Literal["5m", "1h", "1d"]):
    symbols = await get_symbols(client.exchange_name, coins, "USDT", InstType.PERP)
    for i in symbols:
        if interval == "5m":
            await client.update_long_short_ratio_5m(i)
        elif interval == "1h":
            await client.update_long_short_ratio_1h(i)
        elif interval == "1d":
            await client.update_long_short_ratio_1d(i)


async def sync_long_short_ratio_5m():
    logger = _logger.bind(job_id="LONG_SHORT_RATIO[5m]")
    clients: list[BaseClient] = [
        BinancePerpClient(logger),
        BitgetPerpClient(logger),
        BybitPerpClient(logger),
        OkxPerpClient(logger),
    ]

    await asyncio.gather(*(update_long_short_ratio(client, COINS, "5m") for client in clients))


async def sync_long_short_ratio_1h():
    logger = _logger.bind(job_id="LONG_SHORT_RATIO[1h]")
    clients: list[BaseClient] = [
        BinancePerpClient(logger),
        BitgetPerpClient(logger),
        BybitPerpClient(logger),
        OkxPerpClient(logger),
    ]

    await asyncio.gather(*(update_long_short_ratio(client, COINS, "1h") for client in clients))


async def sync_long_short_ratio_1d():
    logger = _logger.bind(job_id="LONG_SHORT_RATIO[1d]")
    clients: list[BaseClient] = [
        BinancePerpClient(logger),
        BitgetPerpClient(logger),
        BybitPerpClient(logger),
        OkxPerpClient(logger),
    ]

    await asyncio.gather(*(update_long_short_ratio(client, COINS, "1d") for client in clients))


if __name__ == "__main__":
    asyncio.run(sync_long_short_ratio_5m())
