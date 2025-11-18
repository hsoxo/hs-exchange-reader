import asyncio
from typing import Literal

from constants import InstType
from databases.mysql import async_engine
from exchanges.aster import AsterPerpClient
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
from sqlalchemy import text


async def get_symbols(exchange: str, base_asset: [str], quote_asset: str, inst_type: InstType):
    async with async_engine.begin() as conn:
        result = await conn.execute(
            text(f"""
        SELECT s.symbol FROM exchange_symbol s
        LEFT JOIN exchange_info i ON s.exchange_id = i.id
        WHERE i.name = '{exchange}' AND s.base_asset IN {str(tuple(base_asset)).replace(",)", ")")} AND s.quote_asset = '{quote_asset}' AND s.inst_type = {inst_type.value}
        """),
        )
        symbols = [i[0] for i in result.all()]
    return symbols


async def update_kline_aster(_logger, coins: [str], interval: Literal["1m", "1h", "1d"]):
    perp_client = AsterPerpClient(_logger)
    symbols = await get_symbols("aster", coins, "USDT", InstType.PERP)
    for i in symbols:
        await perp_client.update_kline(i, interval, 1735689600000)


async def update_kline_binance(_logger, coins: [str], interval: Literal["1m", "1h", "1d"]):
    spot_client = BinanceSpotClient(_logger)
    perp_client = BinancePerpClient(_logger)
    symbols = await get_symbols("binance", coins, "USDT", InstType.SPOT)
    for i in symbols:
        await spot_client.update_kline(i, interval, 1735689600000)
    symbols = await get_symbols("binance", coins, "USDT", InstType.PERP)
    for i in symbols:
        await perp_client.update_kline(i, interval, 1735689600000)


async def update_kline_bitget(_logger, coins: [str], interval: Literal["1m", "1h", "1d"]):
    spot_client = BitgetSpotClient(_logger)
    perp_client = BitgetPerpClient(_logger)
    symbols = await get_symbols("bitget", coins, "USDT", InstType.SPOT)
    for i in symbols:
        await spot_client.update_kline(i, interval, 1735689600000)
    symbols = await get_symbols("bitget", coins, "USDT", InstType.PERP)
    for i in symbols:
        await perp_client.update_kline(i, interval, 1735689600000)


async def update_kline_bitmart(_logger, coins: [str], interval: Literal["1m", "1h", "1d"]):
    spot_client = BitmartSpotClient(_logger)
    perp_client = BitmartPerpClient(_logger)
    symbols = await get_symbols("bitmart", coins, "USDT", InstType.SPOT)
    for i in symbols:
        await spot_client.update_kline(i, interval, 1735689600000)
    symbols = await get_symbols("bitmart", coins, "USDT", InstType.PERP)
    for i in symbols:
        await perp_client.update_kline(i, interval, 1735689600000)


async def update_kline_bybit(_logger, coins: [str], interval: Literal["1m", "1h", "1d"]):
    spot_client = BybitSpotClient(_logger)
    perp_client = BybitPerpClient(_logger)
    symbols = await get_symbols("bybit", coins, "USDT", InstType.SPOT)
    for i in symbols:
        await spot_client.update_kline(i, interval, 1735689600000)
    symbols = await get_symbols("bybit", coins, "USDT", InstType.PERP)
    for i in symbols:
        await perp_client.update_kline(i, interval, 1735689600000)


async def update_kline_gate(_logger, coins: [str], interval: Literal["1m", "1h", "1d"]):
    spot_client = GateSpotClient(_logger)
    perp_client = GatePerpClient(_logger)
    symbols = await get_symbols("gate", coins, "USDT", InstType.SPOT)
    for i in symbols:
        await spot_client.update_kline(i, interval, 1735689600000)
    symbols = await get_symbols("gate", coins, "USDT", InstType.PERP)
    for i in symbols:
        await perp_client.update_kline(i, interval, 1735689600000)


async def update_kline_mexc(_logger, coins: [str], interval: Literal["1m", "1h", "1d"]):
    spot_client = MexcSpotClient(_logger)
    perp_client = MexcPerpClient(_logger)
    symbols = await get_symbols("mexc", coins, "USDT", InstType.SPOT)
    for i in symbols:
        await spot_client.update_kline(i, interval, 1735689600000)
    symbols = await get_symbols("mexc", coins, "USDT", InstType.PERP)
    for i in symbols:
        await perp_client.update_kline(i, interval, 1735689600000)


async def update_kline_okx(_logger, coins: [str], interval: Literal["1m", "1h", "1d"]):
    spot_client = OkxSpotClient(_logger)
    perp_client = OkxPerpClient(_logger)
    symbols = await get_symbols("okx", coins, "USDT", InstType.SPOT)
    for i in symbols:
        await spot_client.update_kline(i, interval, 1735689600000)
    symbols = await get_symbols("okx", coins, "USDT", InstType.PERP)
    for i in symbols:
        await perp_client.update_kline(i, interval, 1735689600000)


async def update_kline_woox(_logger, coins: [str], interval: Literal["1m", "1h", "1d"]):
    spot_client = WooxSpotClient(_logger)
    perp_client = WooxPerpClient(_logger)
    symbols = await get_symbols("woox", coins, "USDT", InstType.SPOT)
    for i in symbols:
        await spot_client.update_kline(i, interval, 1735689600000)
    symbols = await get_symbols("woox", coins, "USDT", InstType.PERP)
    for i in symbols:
        await perp_client.update_kline(i, interval, 1735689600000)


async def update_kline_coinbase(_logger, coins: [str], interval: Literal["1m", "1h", "1d"]):
    spot_client = CoinbaseSpotClient(_logger)
    symbols = await get_symbols("coinbase", coins, "USDT", InstType.SPOT)
    for i in symbols:
        await spot_client.update_kline(i, interval, 1735689600000)


async def update_kline_kraken(_logger, coins: [str], interval: Literal["1m", "1h", "1d"]):
    spot_client = KrakenSpotClient(_logger)
    symbols = await get_symbols("kraken", coins, "USDT", InstType.SPOT)
    for i in symbols:
        await spot_client.update_kline(i, interval, 1735689600000)


async def sync_klines_1m():
    logger = _logger.bind(job_id="KLINE[1m]")
    coins = ["BTC", "ETH", "SOL", "BNB", "XRP", "LTC", "ADA", "DOGE", "GIGGLE", "ZEC", "AIA", "ASTER"]
    updaters = [
        update_kline_aster,
        update_kline_binance,
        update_kline_bitget,
        update_kline_bitmart,
        update_kline_bybit,
        update_kline_gate,
        update_kline_mexc,
        update_kline_okx,
        update_kline_woox,
    ]

    await asyncio.gather(*(updater(logger, coins, "1m") for updater in updaters))


async def sync_klines_1h():
    logger = _logger.bind(job_id="KLINE[1h]")
    coins = ["BTC", "ETH", "SOL", "BNB", "XRP", "LTC", "ADA", "DOGE", "GIGGLE", "ZEC", "AIA", "ASTER"]
    updaters = [
        update_kline_aster,
        update_kline_binance,
        update_kline_bitget,
        update_kline_bitmart,
        update_kline_bybit,
        update_kline_gate,
        update_kline_mexc,
        update_kline_okx,
        update_kline_woox,
    ]

    await asyncio.gather(*(updater(logger, coins, "1h") for updater in updaters))


if __name__ == "__main__":
    asyncio.run(sync_klines_1m())
