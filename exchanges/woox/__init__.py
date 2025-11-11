from exchanges._base_ import BaseClient
import pandas as pd
from constants import SymbolStatus
from utils import precision, to_decimal_str
from decimal import Decimal


class WooxSpotClient(BaseClient):
    """https://docs.woox.io/#general-information"""

    exchange_id = 1009
    inst_type = 0
    base_url = "https://api.woox.io"

    status_map = {
        "TRADING": SymbolStatus.ACTIVE,
        "SUSPENDED": SymbolStatus.HALTED,
    }

    async def get_exchange_info(self):
        """
        https://docs.woox.io/#available-symbols-public
        """
        return await self.send_request("GET", "/v1/public/info")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["rows"]:
            symbol = sym["symbol"]
            inst_type, base, quote = symbol.split("_")
            if inst_type == "SPOT":
                rows.append(
                    {
                        "symbol": symbol,
                        "base_asset": base,
                        "quote_asset": quote,
                        "status": self.status_map.get(sym["status"]),
                        "exchange_id": self.exchange_id,
                        "inst_type": self.inst_type,
                        "tick_size": sym["quote_tick"],
                        "step_size": sym["base_tick"],
                        "price_precision": precision(sym["quote_tick"]),
                        "quantity_precision": precision(sym["base_tick"]),
                        "onboard_time": float(sym["listing_time"]) * 1000,
                    }
                )
        return pd.DataFrame(rows)


class WooxPerpClient(BaseClient):
    """https://docs.woox.io/#general-information"""

    exchange_id = 1009
    inst_type = 1
    base_url = "https://api.woox.io"

    status_map = {
        "live": SymbolStatus.ACTIVE,
        "suspend": SymbolStatus.HALTED,
        "preopen": SymbolStatus.PENDING,
        "test": SymbolStatus.PENDING,
    }

    async def get_exchange_info(self):
        """
        https://docs.woox.io/#available-symbols-public
        """
        return await self.send_request("GET", "/v1/public/info")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["rows"]:
            symbol = sym["symbol"]
            inst_type, base, quote = symbol.split("_")
            if inst_type == "PERP":
                rows.append(
                    {
                        "symbol": symbol,
                        "base_asset": base,
                        "quote_asset": quote,
                        "status": self.status_map.get(sym["status"]),
                        "exchange_id": self.exchange_id,
                        "inst_type": self.inst_type,
                        "tick_size": sym["quote_tick"],
                        "step_size": sym["base_tick"],
                        "price_precision": precision(sym["quote_tick"]),
                        "quantity_precision": precision(sym["base_tick"]),
                        "onboard_time": float(sym["listing_time"]) * 1000,
                    }
                )
        return pd.DataFrame(rows)
