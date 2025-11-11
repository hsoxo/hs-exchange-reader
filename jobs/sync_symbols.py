import asyncio

from exchanges.bitget import BitgetSpotClient, BitgetPerpClient
from exchanges.bitmart import BitmartSpotClient, BitmartPerpClient
from exchanges.bybit import BybitSpotClient, BybitPerpClient
from exchanges.gate import GateSpotClient, GatePerpClient
from exchanges.mexc import MexcSpotClient, MexcPerpClient
from exchanges.okx import OkxSpotClient, OkxPerpClient
from exchanges.woox import WooxSpotClient, WooxPerpClient
from exchanges.aster import AsterPerpClient


async def sync_symbols():
    clients = [
        AsterPerpClient(),
        BitgetSpotClient(),
        BitgetPerpClient(),
        BitmartSpotClient(),
        BitmartPerpClient(),
        BybitSpotClient(),
        BybitPerpClient(),
        GateSpotClient(),
        GatePerpClient(),
        MexcSpotClient(),
        MexcPerpClient(),
        OkxSpotClient(),
        OkxPerpClient(),
        WooxSpotClient(),
        WooxPerpClient(),
    ]

    await asyncio.gather(*(client.update_all_symbols() for client in clients))


if __name__ == "__main__":
    asyncio.run(sync_symbols())
