import asyncio

from prefect import flow

from exchanges._base_ import BaseClient
from exchanges.aster import AsterPerpClient, AsterSpotClient
from exchanges.binance import BinancePerpClient, BinanceSpotClient
from exchanges.bitget import BitgetPerpClient, BitgetSpotClient
from exchanges.bitmart import BitmartPerpClient, BitmartSpotClient
from exchanges.bybit import BybitPerpClient, BybitSpotClient
from exchanges.coinbase import CoinbaseSpotClient
from exchanges.gate import GatePerpClient, GateSpotClient
from exchanges.kraken import KrakenSpotClient
from exchanges.mexc import MexcPerpClient, MexcSpotClient
from exchanges.okx import OkxPerpClient, OkxSpotClient
from exchanges.woox import WooxPerpClient, WooxSpotClient
from utils.logger import logger as _logger
from utils.prefect_decorators import flow_timing, task


@task(name="update-symbols-task", retries=2, retry_delay_seconds=3)
async def update_symbols_task(client: BaseClient):
    await client.update_all_symbols()


@flow(name="sync-symbols")
@flow_timing
async def sync_symbols():
    logger = _logger.bind(job_id="SYMBOLS")
    clients = [
        AsterSpotClient(logger),
        AsterPerpClient(logger),
        BinanceSpotClient(logger),
        BinancePerpClient(logger),
        BitgetSpotClient(logger),
        BitgetPerpClient(logger),
        BitmartSpotClient(logger),
        BitmartPerpClient(logger),
        BybitSpotClient(logger),
        BybitPerpClient(logger),
        CoinbaseSpotClient(logger),
        GateSpotClient(logger),
        GatePerpClient(logger),
        KrakenSpotClient(logger),
        MexcSpotClient(logger),
        MexcPerpClient(logger),
        OkxSpotClient(logger),
        OkxPerpClient(logger),
        WooxSpotClient(logger),
        WooxPerpClient(logger),
    ]

    await asyncio.gather(*(update_symbols_task.submit(client) for client in clients))


if __name__ == "__main__":

    async def sync_symbols_test():
        logger = _logger.bind(job_id="SYMBOLS")
        client = BinanceSpotClient(logger)
        await update_symbols_task(client)

    asyncio.run(sync_symbols_test())
