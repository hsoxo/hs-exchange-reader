from datetime import UTC, datetime
from typing import ClassVar

from constants import InstType, SymbolStatus

from databases.mysql import ExchangeSymbol
from exchanges._base_ import BaseClient
from utils import align_to_5m


def get_price_precision(filters) -> int:
    """根据字符串数值计算小数位数（如 0.01000000 → 2）"""
    price_filter = next(f for f in filters if f["filterType"] == "PRICE_FILTER")
    return price_filter["tickSize"]


def get_quantity_precision(filters) -> int:
    """根据字符串数值计算小数位数（如 0.01000000 → 2）"""
    lot_size = next(f for f in filters if f["filterType"] == "LOT_SIZE")
    return lot_size["stepSize"]


class BinancePerpClient(BaseClient):
    """https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info"""

    exchange_name = "binance"
    inst_type = InstType.PERP
    base_url = "https://fapi.binance.com"

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
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information
        """
        return await self.send_request("GET", "/fapi/v1/exchangeInfo")

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
        https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data

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
            url="/fapi/v1/klines",
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

    async def get_long_short_ratio(self, symbol: ExchangeSymbol, interval: str = "5m"):
        """
        https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#top-trader-long-short-ratio

        interval: "5m","15m","30m","1h","2h","4h","6h","12h","1d"
        """
        s = symbol.symbol
        top_position_ratio = await self.send_request(
            "GET", "/futures/data/topLongShortPositionRatio", params={"symbol": s, "period": interval}
        )
        pos_dict = {}
        for i in top_position_ratio:
            pos_dict[align_to_5m(i["timestamp"])] = {
                "top_trader_pos_long": i["longAccount"],
                "top_trader_pos_short": i["shortAccount"],
            }
        top_account_ratio = await self.send_request(
            "GET", "/futures/data/topLongShortAccountRatio", params={"symbol": s, "period": interval}
        )
        acc_dict = {}
        for i in top_account_ratio:
            acc_dict[align_to_5m(i["timestamp"])] = {
                "top_trader_acc_long": i["longAccount"],
                "top_trader_acc_short": i["shortAccount"],
            }

        retail_ratio = await self.send_request(
            "GET", "/futures/data/globalLongShortAccountRatio", params={"symbol": s, "period": interval}
        )
        retail_dict = {}
        for i in retail_ratio:
            retail_dict[align_to_5m(i["timestamp"])] = {
                "retail_acc_long": i["longAccount"],
                "retail_acc_short": i["shortAccount"],
            }

        merged = []
        all_ts = sorted(set(pos_dict.keys()) | set(acc_dict.keys()) | set(retail_dict.keys()))

        for ts in all_ts:
            row = {
                "dt": ts,
                "symbol": symbol.symbol,
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type.value,
                **pos_dict.get(ts, {}),
                **acc_dict.get(ts, {}),
                **retail_dict.get(ts, {}),
                "updated_at": datetime.now(),
            }
            merged.append(row)

        return merged

    def get_adl_data(self, symbol: ExchangeSymbol):
        """
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/ADL-Risk
        """
        s = symbol.symbol
        adl_data = self.send_request("GET", "/fapi/v1/symbolAdlRisk", params={"symbol": s})
        return adl_data

    async def get_funding_rate(self, *args, **kwargs):
        """
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History
        """
        history_funding_rate = await self.send_request("GET", "/fapi/v1/fundingRate")
        funding_info = await self.send_request("GET", "/fapi/v1/fundingInfo")
        funding_info_dict = {i["symbol"]: i for i in funding_info}

        merged = []
        for i in history_funding_rate:
            info = funding_info_dict.get(i["symbol"])
            if not info:
                continue

            merged.append(
                {
                    "exchange_id": self.exchange_id,
                    "symbol": i["symbol"],
                    "inst_type": self.inst_type.value,
                    "dt": datetime.fromtimestamp(i["fundingTime"] / 1000, tz=UTC),
                    "funding_rate": i["fundingRate"],
                    "funding_interval": info["fundingIntervalHours"] * 60,
                    "adjusted_cap": info["adjustedFundingRateCap"],
                    "adjusted_floor": info["adjustedFundingRateFloor"],
                }
            )
        return merged


if __name__ == "__main__":
    import asyncio

    from sqlalchemy import select
    from sqlalchemy.orm import Session

    from databases.mysql import sync_engine
    from utils.logger import logger as _logger

    client = BinancePerpClient(_logger)

    with Session(sync_engine) as conn:
        stmt = (
            select(ExchangeSymbol)
            .where(ExchangeSymbol.base_asset == "BTC")
            .where(ExchangeSymbol.quote_asset == "USDT")
            .where(ExchangeSymbol.exchange_id == client.exchange_id)
            .where(ExchangeSymbol.inst_type == client.inst_type)
        )

        symbol = conn.execute(stmt).scalars().one_or_none()
    print("symbol", symbol)

    async def main():
        data = await client.get_funding_rate()
        print(data)

    asyncio.run(main())
