import asyncio
import traceback

from prefect import flow, task

from exchanges._base_ import BaseClient
from exchanges.binance import BinancePerpClient
from exchanges.bitget import BitgetPerpClient
from exchanges.bybit import BybitPerpClient
from exchanges.okx import OkxPerpClient
from utils.logger import logger as _logger
from utils.prefect_decorators import flow_timing


@task(name="update-funding-rate")
async def update_funding_rate_task(client_name: str, client: BaseClient):
    try:
        await client.update_funding_rate()
        return f"{client_name} ok"
    except Exception as e:
        _logger.error(f"[{client_name}] Failed: {e}")
        traceback.print_exc()
        await asyncio.sleep(1)
        return f"{client_name} failed"


@flow(name="sync-funding-rate")
@flow_timing
async def sync_funding_rate():
    logger = _logger.bind(job_id="FUNDING_RATE")

    clients: dict[str, BaseClient] = {
        "binance": BinancePerpClient(logger),
        "bitget": BitgetPerpClient(logger),
        "bybit": BybitPerpClient(logger),
        "okx": OkxPerpClient(logger),
    }

    await asyncio.gather(*[update_funding_rate_task.submit(name, client) for name, client in clients.items()])


if __name__ == "__main__":
    asyncio.run(sync_funding_rate())
