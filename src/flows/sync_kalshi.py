from macro_markets.kalshi import KalshiClient
from prefect import flow

from utils.logger import logger as _logger
from utils.prefect_decorators import flow_timing


@flow(name="sync-kalshi")
@flow_timing
async def sync_kalshi_flow():
    logger = _logger.bind(job_id="KALSHI")
    client = KalshiClient(logger)
    await client.sync_market_meta()


if __name__ == "__main__":
    sync_kalshi_flow.serve(name="sync-kalshi", cron="* * * * *")
