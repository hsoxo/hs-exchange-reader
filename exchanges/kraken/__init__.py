from decimal import Decimal
from typing import ClassVar

from constants import InstType, SymbolStatus

from exchanges._base_ import BaseClient

KRAKEN_NAME_MAP = {
    "XXBT": "BTC",
    "XBT": "BTC",
    "XETH": "ETH",
    "XXRP": "XRP",
    "XXLM": "XLM",
    "XDG": "DOGE",
    "XLTC": "LTC",
    "XETC": "ETC",
    "XXMR": "XMR",
    "XXTZ": "XTZ",
    # 法币
    "ZUSD": "USD",
    "ZEUR": "EUR",
    "ZJPY": "JPY",
    "ZGBP": "GBP",
    "ZCAD": "CAD",
    "ZCHF": "CHF",
}


class KrakenSpotClient(BaseClient):
    """https://docs.kraken.com/api/docs/guides/global-intro"""

    exchange_name = "kraken"
    inst_type = InstType.SPOT
    base_url = "https://api.kraken.com/0"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "online": SymbolStatus.ACTIVE,
        "cancel_only": SymbolStatus.HALTED,
        "post_only": SymbolStatus.HALTED,
        "limit_only": SymbolStatus.HALTED,
        "reduce_only": SymbolStatus.HALTED,
    }

    async def get_exchange_info(self):
        """
        https://docs.kraken.com/api/docs/rest-api/get-tradable-asset-pairs
        """
        return await self.send_request("GET", "/public/AssetPairs")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["result"].values():
            step_size = sym["lot_multiplier"] / (10 ** sym["lot_decimals"])

            rows.append(
                {
                    "symbol": sym["altname"],
                    "base_asset": KRAKEN_NAME_MAP.get(sym["base"], sym["base"]),
                    "quote_asset": KRAKEN_NAME_MAP.get(sym["quote"], sym["quote"]),
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["tick_size"],
                    "step_size": step_size,
                    "price_precision": sym["pair_decimals"],
                    "quantity_precision": sym["lot_decimals"],
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
        https://docs.kraken.com/api/docs/rest-api/get-ohlc-data

        {
            "error": [],
            "result": {
                "XBTUSDT": [
                    [
                        1763404440, // time
                        "92536.5",  // open
                        "92555.8",  // high
                        "92536.1",  // low
                        "92536.1",  // close
                        "92539.7",  // vwap
                        "0.00889648",  // volume
                        5  // count
                    ],
                ]
            }
        }
        """
        interval_map = {
            "1m": "1",
            "1h": "60",
            "1d": "1440",
        }
        limit = 720
        async for results in self._get_kline(
            url="/public/OHLC",
            params={
                "pair": symbol,
                "interval": interval_map.get(interval),
            },
            get_data=lambda d: d["result"][symbol],
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": int(d[0]),
                "open": d[1],
                "high": d[2],
                "low": d[3],
                "close": d[4],
                "volume": d[6],
                "quote_volume": float(Decimal(d[6]) * Decimal(d[5])),
            },
            start_time_key="since",
            end_time_key="",
            limit=limit,
            time_unit="s",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results
