from exchanges._base_ import BaseClient
import pandas as pd
from constants import SymbolStatus
from utils import precision, to_decimal_str
from decimal import Decimal


class GateSpotClient(BaseClient):
    """https://www.gate.com/docs/developers/apiv4/zh_CN/"""

    exchange_id = 1006
    inst_type = 0
    base_url = "https://api.gateio.ws/api/v4"

    status_map = {
        "untradable": SymbolStatus.CLOSED,
        "buyable": SymbolStatus.ACTIVE,
        "sellable": SymbolStatus.ACTIVE,
        "tradable": SymbolStatus.ACTIVE,
    }

    async def get_exchange_info(self):
        """
        https://www.gate.com/docs/developers/apiv4/zh_CN/#%E6%9F%A5%E8%AF%A2%E6%89%80%E6%9C%89%E5%B8%81%E7%A7%8D%E4%BF%A1%E6%81%AF
        """
        return await self.send_request("GET", "/spot/currency_pairs")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data:
            rows.append(
                {
                    "symbol": sym["id"],
                    "base_asset": sym["base"],
                    "quote_asset": sym["quote"],
                    "status": self.status_map.get(sym["trade_status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": to_decimal_str(sym["precision"]),
                    "step_size": to_decimal_str(sym["amount_precision"]),
                    "price_precision": sym["precision"],
                    "quantity_precision": sym["amount_precision"],
                    "onboard_time": min(sym["sell_start"], sym["buy_start"]) * 1000,
                }
            )
        return pd.DataFrame(rows)


class GatePerpClient(BaseClient):
    """https://www.gate.com/docs/developers/apiv4/zh_CN/#futures"""

    exchange_id = 1006
    inst_type = 1
    base_url = "https://api.gateio.ws/api/v4"

    status_map = {
        "prelaunch": SymbolStatus.PENDING,
        "trading": SymbolStatus.ACTIVE,
        "delisting": SymbolStatus.HALTED,
        "delisted": SymbolStatus.CLOSED,
        "circuit_breaker": SymbolStatus.HALTED,
    }

    async def get_exchange_info(self):
        """
        https://www.gate.com/docs/developers/apiv4/zh_CN/#%E6%9F%A5%E8%AF%A2%E6%89%80%E6%9C%89%E7%9A%84%E5%90%88%E7%BA%A6%E4%BF%A1%E6%81%AF
        """
        return await self.send_request("GET", "/futures/usdt/contracts")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data:
            name = sym["name"]
            rows.append(
                {
                    "symbol": name,
                    "base_asset": name.split("_")[0],
                    "quote_asset": name.split("_")[1],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["order_price_round"],
                    "step_size": 1,
                    "price_precision": precision(sym["order_price_round"]),
                    "quantity_precision": 0,
                    "onboard_time": sym["launch_time"] * 1000,
                }
            )
        return pd.DataFrame(rows)
