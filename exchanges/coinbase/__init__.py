from typing import ClassVar

from constants import InstType, SymbolStatus
from utils import precision

from exchanges._base_ import BaseClient


class CoinbaseSpotClient(BaseClient):
    """https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/introduction"""

    exchange_name = "coinbase"
    inst_type = InstType.SPOT
    base_url = "https://api.exchange.coinbase.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "online": SymbolStatus.ACTIVE,
        "offline": SymbolStatus.CLOSED,
        "internal": SymbolStatus.HALTED,
        "delisted": SymbolStatus.CLOSED,
    }

    async def get_exchange_info(self):
        """
        https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-all-known-trading-pairs
        """
        return await self.send_request("GET", "/products")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data:
            rows.append(
                {
                    "symbol": sym["id"],
                    "base_asset": sym["base_currency"],
                    "quote_asset": sym["quote_currency"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["quote_increment"],
                    "step_size": sym["base_increment"],
                    "price_precision": precision(sym["quote_increment"]),
                    "quantity_precision": precision(sym["base_increment"]),
                }
            )
        return rows

    async def get_kline(
        self,
        symbol: str,
        interval: str = "1m",
        start_ms: int | None = None,
        end_ms: int | None = None,
        sleep_ms: int = 100,
    ):
        """
        https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles

        [
            [
                1763446800, // time
                0.03344,  // low
                0.03344,  // high
                0.03344,  // open
                0.03344,  // close
                0.08291914,  // volume
            ]
        ]
        """
        interval_map = {
            "1m": "60",
            "1h": "3600",
            "1d": "86400",
        }
        limit = 300
        async for results in self._get_kline(
            url=f"/products/{symbol}/candles",
            params={
                "granularity": interval_map.get(interval),
            },
            get_data=lambda d: d,
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": int(d[0]),
                "open": d[3],
                "high": d[2],
                "low": d[1],
                "close": d[4],
                "volume": d[5],
            },
            start_time_key="start",
            end_time_key="end",
            limit=limit,
            time_unit="s",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results
