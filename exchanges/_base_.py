from abc import ABC, abstractmethod
import asyncio
from datetime import datetime, timedelta
import time
import traceback
from typing import Literal
from urllib.parse import urlencode

from aiohttp import ClientSession, ClientTimeout
from constants import INTERVAL_TO_SECONDS
from databases.clickhouse import Kline1d, Kline1h, Kline1m, async_bulk_insert, get_async_client
from databases.mysql import ExchangeSymbol, async_upsert, sync_engine
from sqlalchemy import text


class BaseClient(ABC):
    def __init__(self, _logger):
        self._exchange_id = None
        self.session: ClientSession | None = None
        self.logger = _logger.bind(exchange=self.exchange_name, inst_type=self.inst_type.name)

    @abstractmethod
    def base_url(self):
        raise NotImplementedError

    @abstractmethod
    def exchange_name(self) -> str:
        raise NotImplementedError

    @property
    def exchange_id(self):
        if self._exchange_id:
            return self._exchange_id
        with sync_engine.begin() as conn:
            result = conn.execute(text("SELECT id FROM exchange_info WHERE name = :name"), {"name": self.exchange_name})
            row = result.scalar_one_or_none()
            return row

    @abstractmethod
    def inst_type(self):
        raise NotImplementedError

    async def _get_session(self) -> ClientSession:
        if self.session is None or self.session.closed:
            self.session = ClientSession(timeout=ClientTimeout(total=15))
        return self.session

    async def send_request(self, method: Literal["GET", "POST"], endpoint: str, params=None, headers=None) -> dict:
        url = f"{self.base_url}{endpoint}"
        session = await self._get_session()
        if method == "GET":
            if params:
                self.logger.debug(f"Request: {method} {url}?{urlencode(params)}")
            else:
                self.logger.debug(f"Request: {method} {url}")
            response = await session.get(url, params=params, headers=headers)
        elif method == "POST":
            self.logger.debug(f"Request: {method} {url}")
            response = await session.post(url, json=params, headers=headers)
        return await response.json()

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.debug("ClientSession closed")

    @abstractmethod
    async def get_all_symbols(self):
        raise NotImplementedError

    async def update_all_symbols(self):
        values = await self.get_all_symbols()
        await async_upsert(
            values,
            ExchangeSymbol,
            [
                "tick_size",
                "step_size",
                "price_precision",
                "quantity_precision",
                "status",
            ],
        )

    async def _get_kline(
        self,
        url: str,
        params: dict,
        get_data,
        format_item,
        start_time_key: str,
        limit: int,
        symbol: str,
        end_time_key: str | None = None,
        time_unit: Literal["ms", "s"] = "ms",
        interval: Literal["1m", "1h", "1d"] = "1m",
        start_ms: int | None = None,
        end_ms: int | None = None,
        sleep_ms: int = 100,
        force_start: bool = False,
        **kwargs,
    ):
        logger = self.logger.bind(symbol=symbol)
        client = await get_async_client()
        now_ms = int(time.time() * 1000)
        end_ms = end_ms or now_ms
        interval_ms = INTERVAL_TO_SECONDS[interval] * 1000
        second = 1 if time_unit == "s" else 1000

        # ✅ 1️⃣ 获取数据库中当前最大时间
        result = await client.query(f"""
        SELECT max(timestamp) FROM kline_{interval}
        WHERE exchange_id = {self.exchange_id}
            AND inst_type = '{self.inst_type}'
            AND symbol = '{symbol}'
        """)
        max_ts_in_db = result.result_rows[0][0] if result.result_rows and result.result_rows[0][0] else 0
        if start_ms is None:
            if max_ts_in_db > 0:
                start_ms = max_ts_in_db + interval_ms
            else:
                today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                start_ms = int((today - timedelta(days=180)).timestamp() * 1000)
        if not force_start and max_ts_in_db > 0 and start_ms < max_ts_in_db:
            start_ms = max_ts_in_db + interval_ms

        # ✅ 2️⃣ 查出缺口区间
        sql = f"""
        SELECT
            prev_ts + {interval_ms} AS missing_start,
            curr_ts - {interval_ms} AS missing_end
        FROM
        (
            SELECT
                lagInFrame(timestamp) OVER (ORDER BY timestamp) AS prev_ts,
                timestamp AS curr_ts
            FROM kline_{interval}
            WHERE exchange_id = {self.exchange_id}
            AND inst_type = '{self.inst_type}'
            AND symbol = '{symbol}'
            -- ✅ 关键：向前多取一条，确保窗口能计算到第一行
            AND timestamp BETWEEN ({start_ms} - {interval_ms}) AND {end_ms}
        )
        WHERE prev_ts IS NOT NULL
        AND curr_ts - prev_ts > {interval_ms}
        ORDER BY prev_ts
        """
        result = await client.query(sql)
        rows = [(prev, curr) for prev, curr in result.result_rows if prev is not None]

        missing_ranges = []

        for prev_ts, curr_ts in rows:
            # 只检测真实 gap
            if curr_ts - prev_ts > interval_ms:
                missing_start = prev_ts + interval_ms
                missing_end = curr_ts - interval_ms
                # 防止首行伪缺口：gap 太大且 prev_ts 小于 start_ms
                if missing_start < start_ms:
                    continue
                missing_ranges.append((missing_start, missing_end))

        # 头尾边界再单独检查
        if rows:
            first_curr = rows[0][1]
            last_curr = rows[-1][1]
            if first_curr > start_ms + interval_ms:
                missing_ranges.insert(0, (start_ms, first_curr - interval_ms))
            if last_curr < end_ms - interval_ms:
                missing_ranges.append((last_curr + interval_ms, end_ms))
        else:
            # 完全无数据
            missing_ranges = [(start_ms, end_ms)]

        def merge_missing_ranges(ranges, interval_ms, limit):
            if not ranges:
                return []

            merged = []
            batch_max_span = limit * interval_ms  # 一次最多能请求的时间跨度

            cur_start, cur_end = ranges[0]

            for s, e in ranges[1:]:
                # 如果当前 gap 与前一个 gap 相邻或在同一窗口范围内
                if s - cur_end <= batch_max_span:
                    cur_end = max(cur_end, e)
                else:
                    merged.append((cur_start, cur_end))
                    cur_start, cur_end = s, e

            merged.append((cur_start, cur_end))
            return merged

        missing_ranges = merge_missing_ranges(missing_ranges, interval_ms, limit)

        logger.info(f"{symbol}: Found {len(missing_ranges)} gaps")
        for s, e in missing_ranges:
            logger.debug(f" - gap {s} → {e}")

        params[start_time_key] = int(start_ms // (1000 / second))
        try:
            for start, end in missing_ranges:
                logger.info(f"📈 {symbol} 补齐区间: {start} → {end}")

                # 控制批量请求
                current = start
                while current <= end:
                    batch_end = min(current + limit * interval_ms, end)

                    params[start_time_key] = int(current // (1000 / second))
                    if end_time_key:
                        params[end_time_key] = int(batch_end // (1000 / second))

                    data = await self.send_request("GET", url, params=params)
                    batch = [format_item(d) for d in get_data(data)]
                    for d in batch:
                        d["timestamp"] = (d["timestamp"] // interval_ms) * interval_ms

                    if not batch:
                        logger.debug(f"[{symbol}] No data in {current} → {batch_end}")
                        current = batch_end + interval_ms
                        await asyncio.sleep(sleep_ms / 1000)
                        continue

                    # 对齐时间戳
                    for d in batch:
                        d["timestamp"] = (d["timestamp"] // interval_ms) * interval_ms

                    yield batch

                    current = max(d["timestamp"] for d in batch) + interval_ms
                    await asyncio.sleep(sleep_ms / 1000)
        except Exception as e:
            logger.error(
                {
                    "url": self.base_url + url,
                    "params": params,
                    "error": e,
                    "traceback": traceback.format_exc(),
                }
            )

    async def update_kline(
        self,
        symbol: str,
        interval: Literal["1m", "1h", "1d"] = "1m",
        start_ms: int | None = None,
        end_ms: int | None = None,
    ):
        self.logger.info(f"Updating kline: {interval} [{self.exchange_name}] ({symbol})")
        model = Kline1m
        if interval == "1h":
            model = Kline1h
        elif interval == "1d":
            model = Kline1d
        async for klines in self.get_kline(symbol, interval, start_ms, end_ms):
            await async_bulk_insert(klines, model)
