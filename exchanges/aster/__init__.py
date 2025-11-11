from exchanges._base_ import BaseClient
import pandas as pd
from constants import SymbolStatus


class AsterPerpClient(BaseClient):
    """https://github.com/asterdex/api-docs/blob/master/aster-finance-futures-api-v3.md"""

    exchange_id = 1003
    inst_type = 1
    base_url = "https://fapi.asterdex.com"

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
        https://github.com/asterdex/api-docs/blob/master/aster-finance-futures-api-v3.md#exchange-information
        """
        return await self.send_request("GET", "/fapi/v3/exchangeInfo")

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
