from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar

from constants import InstType, SymbolStatus

from databases.mysql.models import ExchangeSymbol
from exchanges._base_ import BaseClient
from utils import align_to_5m, precision


class OkxPerpClient(BaseClient):
    """https://www.okx.com/docs-v5/en/#public-data"""

    exchange_name = "okx"
    inst_type = InstType.PERP
    base_url = "https://www.okx.com/api"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
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
        https://www.okx.com/docs-v5/en/#public-data-rest-api-get-mark-price-candlesticks-history
        {
            "code":"0",
            "msg":"",
            "data":[
                [
                    "1597026383085",  // open time
                    "3.721",  // open
                    "3.743",  // high
                    "3.677",  // low
                    "3.708",  // close
                    "1"  // confirm
                ]
            ]
        }

        """
        interval_map = {
            "1m": "1m",
            "1h": "1H",
        }
        limit = 1000
        async for results in self._get_kline(
            url="/v5/market/history-mark-price-candles",
            params={
                "instId": symbol,
                "bar": interval_map.get(interval),
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
            },
            start_time_key="after",
            end_time_key="before",
            limit=limit,
            time_unit="ms",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results

    @staticmethod
    def _split_okx_ratio_decimal(ratio_str: str):
        ratio = Decimal(ratio_str)
        long_ratio = ratio / (Decimal(1) + ratio)
        short_ratio = Decimal(1) - long_ratio
        return float(long_ratio), float(short_ratio)  # 插表时转 float

    async def get_long_short_ratio(self, symbol: ExchangeSymbol, interval: str = "5m"):
        """
        https://www.okx.com/docs-v5/en/#trading-statistics-rest-api-get-top-traders-contract-long-short-ratio

        interval: 5m/15m/30m/1H/2H/4H/6H/12H/1D
        """
        interval = {
            "1h": "1H",
            "1d": "1D",
        }.get(interval, interval)

        top_position_ratio = await self.send_request(
            "GET",
            "/v5/rubik/stat/contracts/long-short-position-ratio-contract-top-trader",
            params={"instId": symbol.symbol, "period": interval},
        )
        pos_dict = {}
        for ts, ratio in top_position_ratio["data"]:
            long, short = self._split_okx_ratio_decimal(ratio)
            pos_dict[align_to_5m(ts)] = {
                "top_trader_pos_long": long,
                "top_trader_pos_short": short,
            }

        top_account_ratio = await self.send_request(
            "GET",
            "/v5/rubik/stat/contracts/long-short-account-ratio-contract-top-trader",
            params={"instId": symbol.symbol, "period": interval},
        )
        acc_dict = {}
        for ts, ratio in top_account_ratio["data"]:
            long, short = self._split_okx_ratio_decimal(ratio)
            acc_dict[align_to_5m(ts)] = {
                "top_trader_acc_long": long,
                "top_trader_acc_short": short,
            }

        retail_ratio = await self.send_request(
            "GET",
            "/v5/rubik/stat/contracts/long-short-account-ratio",
            params={"ccy": symbol.base_asset, "period": interval},
        )
        retail_dict = {}
        for ts, ratio in retail_ratio["data"]:
            long, short = self._split_okx_ratio_decimal(ratio)
            retail_dict[align_to_5m(ts)] = {
                "retail_acc_long": long,
                "retail_acc_short": short,
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

    @staticmethod
    def _compute_funding_interval(funding_time: int, next_funding_time: int) -> int:
        """
        安全计算 funding_interval，自动对齐到标准区间
        funding_time / next_funding_time: 毫秒
        """
        minutes = (next_funding_time - funding_time) / 1000 / 60

        common_intervals = [60, 120, 180, 240, 360, 480, 720]

        # 找到最近的标准区间
        nearest = min(common_intervals, key=lambda x: abs(x - minutes))

        return nearest

    async def get_funding_rate(self, *args, **kwargs):
        """
        https://www.okx.com/docs-v5/en/#public-data-rest-api-get-funding-rate
        """
        funding_info = await self.send_request("GET", "https://www.okx.com/api/v5/public/funding-rate?instId=ANY")

        merged = []

        for i in funding_info["data"]:
            if i["instType"] != "SWAP":
                continue
            merged.append(
                {
                    "exchange_id": self.exchange_id,
                    "symbol": i["instId"],
                    "inst_type": self.inst_type.value,
                    "dt": datetime.fromtimestamp(int(i["fundingTime"]) / 1000, tz=UTC),
                    "funding_rate": i["fundingRate"],
                    "funding_interval": self._compute_funding_interval(
                        int(i["fundingTime"]), int(i["nextFundingTime"])
                    ),
                    "adjusted_cap": i["maxFundingRate"],
                    "adjusted_floor": i["minFundingRate"],
                }
            )
        return merged


if __name__ == "__main__":
    import asyncio

    from sqlalchemy import select
    from sqlalchemy.orm import Session

    from databases.mysql import ExchangeSymbol, sync_engine
    from utils.logger import logger as _logger

    client = OkxPerpClient(_logger)

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
