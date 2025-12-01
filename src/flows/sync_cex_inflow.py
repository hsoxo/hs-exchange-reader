import asyncio

from macro_markets.oklink.fetcher import OklinkOnchainInfo
from prefect import flow, task
from sqlalchemy import select
from sqlalchemy.orm import Session

from databases.doris import get_stream_loader
from databases.mysql import sync_engine
from databases.mysql.models import ExchangeInfo
from utils.prefect_decorators import flow_timing


def get_exchange_info():
    with Session(sync_engine) as conn:
        stmt = select(ExchangeInfo).where(ExchangeInfo.name.in_(["binance", "okx", "bybit", "bitget", "kraken"]))
        return conn.execute(stmt).scalars().all()


@task(name="sync-cex-inflow-task", retries=2, retry_delay_seconds=3)
async def sync_one_cex_inflow(exchange, oklink_onchain_info, stream_loader):
    try:
        inflow_rows = await oklink_onchain_info.get_inflow(exchange)
        await stream_loader.send_rows(inflow_rows, "cex_inflow_hourly")
        return f"{exchange.name} inflow ok"
    except Exception as e:
        return f"{exchange.name} inflow failed: {e}"


@flow(name="sync-cex-inflow")
@flow_timing
async def sync_cex_inflow():
    exchange_info = get_exchange_info()

    stream_loader = get_stream_loader()
    oklink_onchain_info = OklinkOnchainInfo()

    # 并发提交任务
    await asyncio.gather(
        *[sync_one_cex_inflow.submit(exchange, oklink_onchain_info, stream_loader) for exchange in exchange_info]
    )


if __name__ == "__main__":
    asyncio.run(sync_cex_inflow())
