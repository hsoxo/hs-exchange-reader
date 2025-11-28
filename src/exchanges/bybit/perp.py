import asyncio
from datetime import UTC, datetime
from typing import ClassVar

from constants import InstType, SymbolStatus

from databases.mysql.models import ExchangeSymbol
from exchanges._base_ import BaseClient
from utils import align_to_5m, precision


class BybitPerpClient(BaseClient):
    """https://bybit-exchange.github.io/docs/v5/intro"""

    exchange_name = "bybit"
    inst_type = InstType.PERP
    base_url = "https://api.bybit.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "Trading": SymbolStatus.ACTIVE,
    }

    async def get_exchange_info(self):
        """
        https://bybit-exchange.github.io/docs/v5/market/instrument
        """
        return await self.send_request("GET", "/v5/market/instruments-info?category=linear")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["result"]["list"]:
            if sym["contractType"] == "LinearPerpetual":
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
                        "price_precision": int(sym.get("priceScale", precision(sym["priceFilter"]["tickSize"]))),
                        "quantity_precision": precision(sym["lotSizeFilter"]["qtyStep"]),
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
        https://bybit-exchange.github.io/docs/v5/market/kline

        {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "symbol": "BTCUSD",
                "category": "inverse",
                "list": [
                    [
                        "1670608800000",  // startTime
                        "17071", // open
                        "17073", // high
                        "17027", // low
                        "17055.5", // close
                        "268611", // volume
                        "15.74462667" // Turnover
                    ],
                ]
            },
            "retExtInfo": {},
            "time": 1672025956592
        }
        """
        interval_map = {
            "1m": "1",
            "1h": "60",
            "1d": "D",
        }
        limit = 1000
        async for results in self._get_kline(
            url="/v5/market/kline",
            params={
                "category": "linear",
                "symbol": symbol,
                "interval": interval_map.get(interval),
                "limit": limit,
            },
            get_data=lambda d: d["result"]["list"],
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
            start_time_key="start",
            end_time_key="end",
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
        https://www.bybit.com/en/markets/contractData/

        interval: "5m","15m","30m","1h","12h","24h"
        """
        s = symbol.symbol
        interval = {
            "1d": "24h",
        }.get(interval, interval)
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9",
            "Referer": "https://www.bybit.com/en/markets/contractData/",
            "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "macOS",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        }
        top_position_ratio = await self.send_request(
            "GET",
            "https://www.bybit.com/x-api/de/cht/market-index/derivatives-data/v1/perpetual/trading-data-metrics",
            params={"symbol": s, "period": interval, "metrics_type": 2},
            headers=headers,
        )
        pos_dict = {}
        for i in top_position_ratio["result"]["topHolderPosList"]:
            pos_dict[align_to_5m(i["timestamp"])] = {
                "top_trader_pos_long": i["longPosAccounts"],
                "top_trader_pos_short": i["shortPosAccounts"],
            }

        retail_ratio = await self.send_request(
            "GET",
            "https://www.bybit.com/x-api/de/cht/market-index/derivatives-data/v1/perpetual/trading-data-metrics",
            params={"symbol": s, "period": interval, "metrics_type": 3},
            headers=headers,
        )
        retail_dict = {}
        for i in retail_ratio["result"]["holderPosList"]:
            retail_dict[align_to_5m(i["timestamp"])] = {
                "retail_acc_long": i["longPosAccounts"],
                "retail_acc_short": i["shortPosAccounts"],
            }

        merged = []
        all_ts = sorted(set(pos_dict.keys()) | set(retail_dict.keys()))

        for ts in all_ts:
            row = {
                "dt": ts,
                "symbol": symbol.symbol,
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type.value,
                **pos_dict.get(ts, {}),
                **retail_dict.get(ts, {}),
                "updated_at": datetime.now(),
            }
            merged.append(row)

        return merged

    async def get_funding_rate(self, next_funding_times_by_symbol: dict[str, int], *args, **kwargs):
        """
        https://bybit-exchange.github.io/docs/v5/market/history-fund-rate
        """
        merged = []
        now_ts = int(datetime.now().timestamp() * 1000)

        instruments = await self.send_request("GET", "/v5/market/instruments-info", params={"category": "linear"})

        for i in instruments["result"]["list"]:
            symbol = i["symbol"]
            db_next_ts = next_funding_times_by_symbol.get(symbol)

            if db_next_ts is not None and now_ts < db_next_ts:
                continue

            history = await self.send_request(
                "GET",
                "/v5/market/funding/history",
                params={"category": "linear", "symbol": symbol},
            )

            if not history["result"].get("list"):
                continue

            for j in history["result"]["list"]:
                funding_time = int(j["fundingRateTimestamp"])
                funding_rate = float(j["fundingRate"])

                merged.append(
                    {
                        "exchange_id": self.exchange_id,
                        "symbol": symbol,
                        "inst_type": 1,  # 永续
                        "dt": datetime.fromtimestamp(funding_time / 1000, tz=UTC),
                        "funding_rate": funding_rate,
                        "funding_interval": i["fundingInterval"],
                        "adjusted_cap": i["upperFundingRate"],
                        "adjusted_floor": i["lowerFundingRate"],
                    }
                )

                await asyncio.sleep(0.06)

        return merged


if __name__ == "__main__":
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    from databases.mysql import sync_engine
    from databases.mysql.models import ExchangeSymbol
    from utils.logger import logger as _logger

    client = BybitPerpClient(_logger)

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
        data = await client.get_long_short_ratio(symbol)
        print(data)

    asyncio.run(main())
