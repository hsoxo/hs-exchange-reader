import asyncio

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
from loguru import logger as _logger


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

    await asyncio.gather(*(client.update_all_symbols() for client in clients))


if __name__ == "__main__":
    asyncio.run(sync_symbols())
