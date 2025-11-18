from typing import ClassVar

from constants import InstType, SymbolStatus

from exchanges._base_ import BaseClient


class AsterSpotClient(BaseClient):
    """https://github.com/asterdex/api-docs/blob/master/aster-finance-spot-api.md"""

    exchange_name = "aster"
    inst_type = InstType.SPOT
    base_url = "https://sapi.asterdex.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
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
        https://github.com/asterdex/api-docs/blob/master/aster-finance-spot-api.md#trading-specification-information
        """
        return await self.send_request("GET", "/api/v1/exchangeInfo")

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
        https://github.com/asterdex/api-docs/blob/master/aster-finance-spot-api.md#k-line-data

        [
          [
            1499040000000,      // Open time
            "0.01634790",       // Open
            "0.80000000",       // High
            "0.01575800",       // Low
            "0.01577100",       // Close
            "148976.11427815",  // Volume
            1499644799999,      // Close time
            "2434.19055334",    // Quote asset volume
            308,                // Number of trades
            "1756.87402397",    // Taker buy base asset volume
            "28.46694368",      // Taker buy quote asset volume
            "17928899.62484339" // Ignore.
          ]
        ]
        """
        limit = 1000
        async for results in self._get_kline(
            url="/api/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            get_data=lambda d: d,
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": d[0],
                "open": d[1],
                "high": d[2],
                "low": d[3],
                "close": d[4],
                "volume": d[5],
                "quote_volume": d[7],
                "count": d[8],
            },
            start_time_key="startTime",
            end_time_key="endTime",
            limit=limit,
            time_unit="ms",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results


class AsterPerpClient(BaseClient):
    """https://github.com/asterdex/api-docs/blob/master/aster-finance-futures-api-v3.md"""

    exchange_name = "aster"
    inst_type = InstType.PERP
    base_url = "https://fapi.asterdex.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
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
            if sym["contractType"] == "PERPETUAL":
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
        https://github.com/asterdex/api-docs/blob/master/aster-finance-futures-api-v3.md#klinecandlestick-data

        [
          [
            1499040000000,      // Open time
            "0.01634790",       // Open
            "0.80000000",       // High
            "0.01575800",       // Low
            "0.01577100",       // Close
            "148976.11427815",  // Volume
            1499644799999,      // Close time
            "2434.19055334",    // Quote asset volume
            308,                // Number of trades
            "1756.87402397",    // Taker buy base asset volume
            "28.46694368",      // Taker buy quote asset volume
            "17928899.62484339" // Ignore.
          ]
        ]
        """
        limit = 1000
        async for results in self._get_kline(
            url="/fapi/v3/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            get_data=lambda d: d,
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": d[0],
                "open": d[1],
                "high": d[2],
                "low": d[3],
                "close": d[4],
                "volume": d[5],
                "quote_volume": d[7],
                "count": d[8],
            },
            start_time_key="startTime",
            end_time_key="endTime",
            limit=limit,
            time_unit="ms",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results
