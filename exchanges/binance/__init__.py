from exchanges._base_ import BaseClient
import pandas as pd
from constants import SymbolStatus
from utils import precision


def get_price_precision(filters) -> int:
    """根据字符串数值计算小数位数（如 0.01000000 → 2）"""
    price_filter = next(f for f in filters if f["filterType"] == "PRICE_FILTER")
    return price_filter["tickSize"]


def get_quantity_precision(filters) -> int:
    """根据字符串数值计算小数位数（如 0.01000000 → 2）"""
    lot_size = next(f for f in filters if f["filterType"] == "LOT_SIZE")
    return lot_size["stepSize"]


class BinanceSpotClient(BaseClient):
    """https://developers.binance.com/docs/binance-spot-api-docs"""

    exchange_id = 1001
    inst_type = 0
    base_url = "https://api.binance.com"

    status_map = {
        "TRADING": SymbolStatus.ACTIVE,
        "END_OF_DAY": SymbolStatus.CLOSED,
        "HALT": SymbolStatus.HALTED,
        "BREAK": SymbolStatus.HALTED,
    }

    async def get_exchange_info(self):
        """
        https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints#exchange-information
        """
        return await self.send_request("GET", "/api/v3/exchangeInfo")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["symbols"]:
            tick = step = None
            for f in sym["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    tick = f.get("tickSize")
                elif f["filterType"] == "LOT_SIZE":
                    step = f.get("stepSize")
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseAsset"],
                    "quote_asset": sym["quoteAsset"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": tick,
                    "step_size": step,
                    "price_precision": precision(tick),
                    "quantity_precision": precision(step),
                }
            )
        return pd.DataFrame(rows)


class BinancePerpClient(BaseClient):
    """https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info"""

    exchange_id = 1001
    inst_type = 1
    base_url = "https://fapi.binance.com"

    status_map = {
        "TRADING": SymbolStatus.ACTIVE,
        "PENDING_TRADING": SymbolStatus.PENDING,
        "PRE_DELIVERING": SymbolStatus.HALTED,
        "DELIVERING": SymbolStatus.HALTED,
        "DELIVERED": SymbolStatus.HALTED,
        "PRE_SETTLE": SymbolStatus.HALTED,
        "SETTLING": SymbolStatus.HALTED,
        "CLOSE": SymbolStatus.CLOSED,
    }

    async def get_exchange_info(self):
        """
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information
        """
        return await self.send_request("GET", "/fapi/v1/exchangeInfo")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["symbols"]:
            tick = step = None
            for f in sym["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    tick = f.get("tickSize")
                elif f["filterType"] == "LOT_SIZE":
                    step = f.get("stepSize")
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseAsset"],
                    "quote_asset": sym["quoteAsset"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": tick,
                    "step_size": step,
                    "price_precision": sym["pricePrecision"],
                    "quantity_precision": sym["quantityPrecision"],
                }
            )
        return pd.DataFrame(rows)
