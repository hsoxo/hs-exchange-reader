import asyncio
from typing import Literal

from exchanges._base_ import BaseClient
from exchanges.aster import AsterPerpClient
from exchanges.binance import BinancePerpClient, BinanceSpotClient
from exchanges.bitget import BitgetPerpClient, BitgetSpotClient
from exchanges.bitmart import BitmartPerpClient, BitmartSpotClient
from exchanges.bybit import BybitPerpClient, BybitSpotClient
from exchanges.gate import GateSpotClient
from exchanges.kraken import KrakenSpotClient
from exchanges.mexc import MexcPerpClient, MexcSpotClient
from exchanges.okx import OkxPerpClient, OkxSpotClient
from exchanges.woox import WooxPerpClient, WooxSpotClient
from utils.logger import logger as _logger

from .constants import COINS
from .utils import get_symbols


async def update_kline(client: BaseClient, coins: [str], interval: Literal["1m", "1h", "1d"]):
    symbols = await get_symbols(client.exchange_name, coins, "USDT", client.inst_type)
    for i in symbols:
        await client.update_kline(i.symbol, interval, 1735689600000)


async def sync_klines_1m():
    logger = _logger.bind(job_id="KLINE[1m]")
    clients = [
        AsterPerpClient(logger),
        BinancePerpClient(logger),
        BitgetPerpClient(logger),
        BitmartPerpClient(logger),
        BybitPerpClient(logger),
        MexcPerpClient(logger),
        OkxPerpClient(logger),
        WooxPerpClient(logger),
        BinanceSpotClient(logger),
        BitgetSpotClient(logger),
        BitmartSpotClient(logger),
        BybitSpotClient(logger),
        GateSpotClient(logger),
        KrakenSpotClient(logger),
        MexcSpotClient(logger),
        OkxSpotClient(logger),
        WooxSpotClient(logger),
    ]

    await asyncio.gather(*(update_kline(c, COINS, "1m") for c in clients))


async def sync_klines_1h():
    logger = _logger.bind(job_id="KLINE[1h]")
    clients = [
        AsterPerpClient(logger),
        BinancePerpClient(logger),
        BitgetPerpClient(logger),
        BitmartPerpClient(logger),
        BybitPerpClient(logger),
        MexcPerpClient(logger),
        OkxPerpClient(logger),
        WooxPerpClient(logger),
        BinanceSpotClient(logger),
        BitgetSpotClient(logger),
        BitmartSpotClient(logger),
        BybitSpotClient(logger),
        GateSpotClient(logger),
        KrakenSpotClient(logger),
        MexcSpotClient(logger),
        OkxSpotClient(logger),
        WooxSpotClient(logger),
    ]

    await asyncio.gather(*(update_kline(c, COINS, "1h") for c in clients))
