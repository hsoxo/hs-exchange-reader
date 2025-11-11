from exchanges._base_ import BaseClient
import pandas as pd
from constants import SymbolStatus
from utils import precision, to_decimal_str
from decimal import Decimal


class MexcSpotClient(BaseClient):
    """https://www.mexc.com/api-docs/spot-v3/introduction"""

    exchange_id = 1008
    inst_type = 0
    base_url = "https://api.mexc.com/api"

    status_map = {
        "1": SymbolStatus.ACTIVE,
        "2": SymbolStatus.HALTED,
        "3": SymbolStatus.CLOSED,
    }

    async def get_exchange_info(self):
        """
        https://www.mexc.com/api-docs/spot-v3/market-data-endpoints#exchange-information
        """
        return await self.send_request("GET", "/v3/exchangeInfo")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["symbols"]:
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseAsset"],
                    "quote_asset": sym["quoteAsset"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["quoteAmountPrecision"],
                    "step_size": sym["baseSizePrecision"],
                    "price_precision": sym["quoteAssetPrecision"],
                    "quantity_precision": sym["baseAssetPrecision"],
                }
            )
        return pd.DataFrame(rows)


class MexcPerpClient(BaseClient):
    """https://www.mexc.com/api-docs/futures/update-log"""

    exchange_id = 1008
    inst_type = 1
    base_url = "https://contract.mexc.com/api"

    status_map = {
        0: SymbolStatus.ACTIVE,
        1: SymbolStatus.HALTED,
        2: SymbolStatus.CLOSED,
        3: SymbolStatus.CLOSED,
        4: SymbolStatus.HALTED,
    }

    async def get_exchange_info(self):
        """
        https://www.gate.com/docs/developers/apiv4/zh_CN/#%E6%9F%A5%E8%AF%A2%E6%89%80%E6%9C%89%E7%9A%84%E5%90%88%E7%BA%A6%E4%BF%A1%E6%81%AF
        """
        return await self.send_request("GET", "/v1/contract/detail")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["data"]:
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseCoin"],
                    "quote_asset": sym["quoteCoin"],
                    "status": self.status_map.get(sym["state"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["priceUnit"],
                    "step_size": sym["volUnit"],
                    "price_precision": sym["priceScale"],
                    "quantity_precision": sym["amountScale"],
                    "onboard_time": sym["openingTime"] * 1000,
                }
            )
        return pd.DataFrame(rows)
