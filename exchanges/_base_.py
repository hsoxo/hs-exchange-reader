from abc import ABC, abstractmethod
import asyncio
from datetime import datetime, timedelta
import time
import traceback
from typing import Literal
from urllib.parse import urlencode

from aiohttp import ClientSession, ClientTimeout
from constants import INTERVAL_TO_SECONDS
from databases.doris import get_doris, get_stream_loader
from databases.mysql import ExchangeSymbol, async_upsert, sync_engine
from sqlalchemy import text


class BaseClient(ABC):
    def __init__(self, _logger):
        self._exchange_id = None
        self.session: ClientSession | None = None
        self.logger = _logger.bind(exchange=self.exchange_name, inst_type=self.inst_type.name)
        self.doris_client = get_doris()
        self.doris_stream_loader = get_stream_loader()

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
        self.logger.info(f"{self.exchange_name}: Symbols updated")

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
        """
        Doris 版本的 Kline 缺口扫描 + 批量补齐
        """
        logger = self.logger.bind(symbol=symbol)

        now_ms = int(time.time() * 1000)
        end_ms = end_ms or now_ms
        interval_ms = INTERVAL_TO_SECONDS[interval] * 1000
        second = 1 if time_unit == "s" else 1000

        # ----------------------------------------
        # 1) 查询 Doris 中当前最大 timestamp
        # ----------------------------------------
        q = f"""
        SELECT MAX(dt)
        FROM kline_{interval}
        WHERE exchange_id = {self.exchange_id}
          AND inst_type = '{self.inst_type}'
          AND symbol = '{symbol}'
        """
        r = await self.doris_client.query(q)
        logger.info("max_ts_in_db: %s", r[0][0])
        max_ts_in_db = int(r[0][0].timestamp()) * 1000 if r and r[0][0] else 0

        # 初始 start_ms 确定
        if start_ms is None:
            if max_ts_in_db > 0:
                start_ms = max_ts_in_db + interval_ms
            else:
                today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                start_ms = int((today - timedelta(days=180)).timestamp() * 1000)

        if not force_start and max_ts_in_db > 0 and start_ms < max_ts_in_db:
            start_ms = max_ts_in_db + interval_ms

        # --------------------------------------------------------------------
        # 2) Doris 扫描缺口（使用标准 SQL LAG 窗口函数）
        # --------------------------------------------------------------------
        sql = f"""
        SELECT
            prev_ts,
            curr_ts
        FROM (
            SELECT
                LAG(dt) OVER (ORDER BY dt) AS prev_ts,
                dt AS curr_ts
            FROM kline_{interval}
            WHERE exchange_id = {self.exchange_id}
              AND inst_type = '{self.inst_type}'
              AND symbol = '{symbol}'
              AND dt BETWEEN ({start_ms} - {interval_ms}) AND {end_ms}
        ) t
        WHERE prev_ts IS NOT NULL
          AND curr_ts - prev_ts > {interval_ms}
        ORDER BY prev_ts
        """

        r = await self.doris_client.query(sql)
        rows = [(row[0], row[1]) for row in r]

        missing_ranges = []

        # 生成基本缺口区间
        for prev_ts, curr_ts in rows:
            if curr_ts - prev_ts > interval_ms:
                missing_start = prev_ts + interval_ms
                missing_end = curr_ts - interval_ms
                if missing_start >= start_ms:
                    missing_ranges.append((missing_start, missing_end))

        # 头尾边界补 gap
        if rows:
            first_curr = rows[0][1]
            last_curr = rows[-1][1]

            if first_curr > start_ms + interval_ms:
                missing_ranges.insert(0, (start_ms, first_curr - interval_ms))

            if last_curr < end_ms - interval_ms:
                missing_ranges.append((last_curr + interval_ms, end_ms))
        else:
            # 完全没数据 → 整段都是缺口
            missing_ranges = [(start_ms, end_ms)]

        # --------------------------------------------------------------------
        # 3) 合并相邻 gap，降低 API 请求次数
        # --------------------------------------------------------------------
        def merge_missing_ranges(ranges, interval_ms, limit):
            if not ranges:
                return []

            merged = []
            batch_max_span = limit * interval_ms

            cur_start, cur_end = ranges[0]
            for s, e in ranges[1:]:
                if s - cur_end <= batch_max_span:
                    cur_end = max(cur_end, e)
                else:
                    merged.append((cur_start, cur_end))
                    cur_start, cur_end = s, e

            merged.append((cur_start, cur_end))
            return merged

        missing_ranges = merge_missing_ranges(missing_ranges, interval_ms, limit)

        # --------------------------------------------------------------------
        # 4) 打印缺口
        # --------------------------------------------------------------------
        logger.info(f"{symbol}: Found {len(missing_ranges)} gaps")
        for s, e in missing_ranges:
            logger.debug(f" - gap {s} → {e}")

        # --------------------------------------------------------------------
        # 5) 逐 gap 批量补数据
        # --------------------------------------------------------------------
        params[start_time_key] = int(start_ms // (1000 / second))

        try:
            for start, end in missing_ranges:
                logger.info(f"📈 {symbol}: 补齐区间 {start} → {end}")

                current = start
                while current <= end:
                    batch_end = min(current + limit * interval_ms, end)

                    params[start_time_key] = int(current // (1000 / second))
                    if end_time_key:
                        params[end_time_key] = int(batch_end // (1000 / second))

                    # 请求交易所 API
                    data = await self.send_request("GET", url, params=params)
                    batch = [format_item(d) for d in get_data(data)]

                    # 对齐 timestamp（强制对齐 OHLC）
                    for d in batch:
                        d["timestamp"] = (d["timestamp"] // interval_ms) * interval_ms

                    if not batch:
                        logger.debug(f"[{symbol}] No data in {current} → {batch_end}")
                        current = batch_end + interval_ms
                        await asyncio.sleep(sleep_ms / 1000)
                        continue

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
        async for klines in self.get_kline(symbol, interval, start_ms, end_ms):
            for kline in klines:
                kline["dt"] = datetime.fromtimestamp(kline["timestamp"] / 1000).strftime("%Y-%m-%d %H:%M:%S")
            await self.doris_stream_loader.send_rows(klines, "kline_" + interval)
