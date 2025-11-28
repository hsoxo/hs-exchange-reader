import asyncio
from datetime import UTC, datetime
from typing import ClassVar

from constants import InstType, SymbolStatus

from databases.mysql import ExchangeSymbol
from exchanges._base_ import BaseClient
from utils import align_to_5m


class BitgetPerpClient(BaseClient):
    """https://www.bitget.com/api-doc/contract/intro"""

    exchange_name = "bitget"
    inst_type = InstType.PERP
    base_url = "https://api.bitget.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "normal": SymbolStatus.ACTIVE,
        "listed": SymbolStatus.PENDING,
        "maintain": SymbolStatus.HALTED,
        "limit_open": SymbolStatus.HALTED,
        "restrictedAPI": SymbolStatus.HALTED,
        "off": SymbolStatus.CLOSED,
    }

    async def get_exchange_info(self):
        """
        https://www.bitget.com/api-doc/contract/market/Get-All-Symbols-Contracts
        """
        return await self.send_request("GET", "/api/mix/v1/market/contracts?productType=umcbl")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["data"]:
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseCoin"],
                    "quote_asset": sym["quoteCoin"],
                    "status": self.status_map.get(sym["symbolStatus"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": f"{10 ** (-int(sym['pricePlace'])):.{int(sym['pricePlace'])}f}",
                    "step_size": sym["sizeMultiplier"],
                    "price_precision": sym["pricePlace"],
                    "quantity_precision": sym["volumePlace"],
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
        https://www.bitget.com/api-doc/contract/market/Get-Candle-Data

        {
            "code": "00000",
            "msg": "success",
            "requestTime": 1695800278693,
            "data": [
                [
                    "1656604800000",  // System timestamp, Unix millisecond timestamp
                    "37834.5",        // Open price
                    "37849.5",        // High price
                    "37773.5",        // Low price
                    "37773.5",        // Close price
                    "428.3462",       // Volume
                    "16198849.1079"   // Quote volume
                ],
            ]
        }
        """
        interval_map = {
            "1m": "1m",
            "1h": "1H",
            "1d": "1D",
        }
        limit = 1000
        async for results in self._get_kline(
            url="/api/v2/mix/market/candles",
            params={
                "symbol": symbol,
                "productType": "usdt-futures",
                "granularity": interval_map.get(interval),
                "kLineType": "MARKET",
                "limit": limit,
            },
            get_data=lambda d: d["data"],
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": int(d[0]),
                "open": d[1],
                "high": d[2],
                "low": d[3],
                "close": d[4],
                "volume": d[5],
                "quote_volume": d[6],
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
        https://www.bitget.com/zh-CN/api-doc/common/apidata/Position-Long-Short

        interval: "5m","15m","30m","1h","2h","4h","6h","12h","1d"
        """
        s = symbol.symbol.replace("_UMCBL", "")
        top_position_ratio = await self.send_request(
            "GET", "/api/v2/mix/market/position-long-short", params={"symbol": s, "period": interval}
        )
        if top_position_ratio["code"] == "40054":
            return []

        pos_dict = {}
        for i in top_position_ratio["data"]:
            pos_dict[align_to_5m(i["ts"])] = {
                "top_trader_pos_long": i["longPositionRatio"],
                "top_trader_pos_short": i["shortPositionRatio"],
            }

        top_account_ratio = await self.send_request(
            "GET", "/api/v2/mix/market/account-long-short", params={"symbol": s, "period": interval}
        )
        acc_dict = {}
        for i in top_account_ratio["data"]:
            acc_dict[align_to_5m(i["ts"])] = {
                "top_trader_acc_long": i["longAccountRatio"],
                "top_trader_acc_short": i["shortAccountRatio"],
            }

        retail_ratio = await self.send_request(
            "GET", "/api/v2/mix/market/long-short", params={"symbol": s, "period": interval}
        )
        retail_dict = {}
        for i in retail_ratio["data"]:
            retail_dict[align_to_5m(i["ts"])] = {
                "retail_acc_long": i["longRatio"],
                "retail_acc_short": i["shortRatio"],
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
        await asyncio.sleep(0.5)
        return merged

    async def get_funding_rate(self, next_funding_times_by_symbol: dict[str, int], *args, **kwargs):
        """
        https://www.bitget.com/zh-CN/api-doc/contract/market/Get-Current-Funding-Rate
        """
        funding_info = await self.send_request("GET", "/api/v2/mix/market/current-fund-rate?productType=usdt-futures")

        merged = []
        now_ts = int(datetime.now().timestamp() * 1000)

        for i in funding_info["data"]:
            symbol = i["symbol"]
            db_next_ts = next_funding_times_by_symbol.get(symbol)

            if db_next_ts is None or now_ts >= db_next_ts:
                funding_history = await self.send_request(
                    "GET",
                    "/api/v2/mix/market/history-fund-rate",
                    params={"symbol": symbol, "productType": "usdt-futures"},
                )
                for j in funding_history["data"]:
                    merged.append(
                        {
                            "exchange_id": self.exchange_id,
                            "symbol": i["symbol"],
                            "inst_type": self.inst_type.value,
                            "dt": datetime.fromtimestamp(int(j["fundingTime"]) / 1000, tz=UTC),
                            "funding_rate": j["fundingRate"],
                            "funding_interval": float(i["fundingRateInterval"]) * 60,
                            "adjusted_cap": i["maxFundingRate"],
                            "adjusted_floor": i["minFundingRate"],
                        }
                    )
                await asyncio.sleep(0.06)
        return merged


if __name__ == "__main__":
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    from databases.mysql import sync_engine
    from utils.logger import logger as _logger

    client = BitgetPerpClient(_logger)

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
        data = await client.get_funding_rate(symbol)
        print(data)

    asyncio.run(main())
