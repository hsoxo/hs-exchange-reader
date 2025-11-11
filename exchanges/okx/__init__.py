from exchanges._base_ import BaseClient
import pandas as pd
from constants import SymbolStatus
from utils import precision, to_decimal_str
from decimal import Decimal


class OkxSpotClient(BaseClient):
    """https://www.okx.com/docs-v5/en/#public-data"""

    exchange_id = 1001
    inst_type = 0
    base_url = "https://www.okx.com/api"

    status_map = {
        "live": SymbolStatus.ACTIVE,
        "suspend": SymbolStatus.HALTED,
        "preopen": SymbolStatus.PENDING,
        "test": SymbolStatus.PENDING,
    }

    async def get_exchange_info(self):
        """
        https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-instruments
        """
        return await self.send_request("GET", "/v5/public/instruments?instType=SPOT")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["data"]:
            rows.append(
                {
                    "symbol": sym["instId"],
                    "base_asset": sym["baseCcy"],
                    "quote_asset": sym["quoteCcy"],
                    "status": self.status_map.get(sym["state"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["tickSz"],
                    "step_size": sym["lotSz"],
                    "price_precision": precision(sym["tickSz"]),
                    "quantity_precision": precision(sym["lotSz"]),
                    "onboard_time": sym["listTime"],
                }
            )
        return pd.DataFrame(rows)


class OkxPerpClient(BaseClient):
    """https://www.okx.com/docs-v5/en/#public-data"""

    exchange_id = 1001
    inst_type = 1
    base_url = "https://www.okx.com/api"

    status_map = {
        "live": SymbolStatus.ACTIVE,
        "suspend": SymbolStatus.HALTED,
        "preopen": SymbolStatus.PENDING,
        "test": SymbolStatus.PENDING,
    }

    async def get_exchange_info(self):
        """
        https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-instruments
        """
        return await self.send_request("GET", "/v5/public/instruments?instType=SWAP")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["data"]:
            inst_family = sym["instFamily"]
            base, quote = inst_family.split("-")
            rows.append(
                {
                    "symbol": sym["instId"],
                    "base_asset": base,
                    "quote_asset": quote,
                    "status": self.status_map.get(sym["state"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["tickSz"],
                    "step_size": sym["lotSz"],
                    "price_precision": precision(sym["tickSz"]),
                    "quantity_precision": precision(sym["lotSz"]),
                    "onboard_time": sym["listTime"],
                }
            )
        return pd.DataFrame(rows)
