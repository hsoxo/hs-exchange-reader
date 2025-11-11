from exchanges._base_ import BaseClient
import pandas as pd
from constants import SymbolStatus
from utils import precision, to_decimal_str
from decimal import Decimal


class BybitSpotClient(BaseClient):
    """https://bybit-exchange.github.io/docs/v5/intro"""

    exchange_id = 1002
    inst_type = 0
    base_url = "https://api.bybit.com"

    status_map = {
        "Trading": SymbolStatus.ACTIVE,
    }

    async def get_exchange_info(self):
        """
        https://bybit-exchange.github.io/docs/v5/market/instrument
        """
        return await self.send_request(
            "GET", "/v5/market/instruments-info?category=spot"
        )

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["result"]["list"]:
            tick_size = str(sym["priceFilter"]["tickSize"])
            step_size = str(sym["lotSizeFilter"]["basePrecision"])
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseCoin"],
                    "quote_asset": sym["quoteCoin"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": tick_size,
                    "step_size": step_size,
                    "price_precision": precision(tick_size),
                    "quantity_precision": precision(step_size),
                }
            )
        return pd.DataFrame(rows)


class BybitPerpClient(BaseClient):
    """https://bybit-exchange.github.io/docs/v5/intro"""

    exchange_id = 1002
    inst_type = 1
    base_url = "https://api.bybit.com"

    status_map = {
        "Trading": SymbolStatus.ACTIVE,
    }

    async def get_exchange_info(self):
        """
        https://bybit-exchange.github.io/docs/v5/market/instrument
        """
        return await self.send_request(
            "GET", "/v5/market/instruments-info?category=linear"
        )

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["result"]["list"]:
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseCoin"],
                    "quote_asset": sym["quoteCoin"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["priceFilter"]["tickSize"],
                    "step_size": sym["lotSizeFilter"]["qtyStep"],
                    "price_precision": int(
                        sym.get("priceScale", precision(sym["priceFilter"]["tickSize"]))
                    ),
                    "quantity_precision": precision(sym["lotSizeFilter"]["qtyStep"]),
                }
            )
        return pd.DataFrame(rows)
