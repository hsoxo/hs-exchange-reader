from datetime import UTC, datetime
import os

from flows.sync_cex_inflow import sync_cex_inflow
from flows.sync_funding_rate import sync_funding_rate
from flows.sync_kalshi import sync_kalshi_flow
from flows.sync_long_short_ratio import (
    sync_long_short_ratio_1d,
    sync_long_short_ratio_1h,
    sync_long_short_ratio_5m,
)
from flows.sync_macro_indicators import sync_macro_indicators
from flows.sync_onchain_tx import sync_onchain_large_transfer
from flows.sync_symbols import sync_symbols
from prefect import deploy
from prefect.server.schemas.schedules import IntervalSchedule, RRuleSchedule
from prefect.types.entrypoint import EntrypointType

ENV = os.getenv("ENV")
IMAGE_URL = os.getenv("IMAGE_URL")

POOL_MAP = {
    "staging": "clx-stg-docker",
    "production": "coinluxer-prod-docker",
}


# 秒级 cron → Prefect workaround 用 RRuleSchedule
def per_second_cron(seconds_list, minutes="*", hours="*", freq="MINUTELY"):
    return RRuleSchedule(
        dtstart=datetime.now(UTC),
        rrule=f"FREQ={freq};BYSECOND={','.join(map(str, seconds_list))};BYMINUTE={minutes};BYHOUR={hours}",
    )


if __name__ == "__main__":
    deployments = [
        # ------------------------------------------------------------------------------
        # sync_symbols: APS = interval(days=1)
        # ------------------------------------------------------------------------------
        sync_symbols.to_deployment(
            "sync-symbols",
            cron="0 0 * * *",  # 每天一次
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        # ------------------------------------------------------------------------------
        # sync_long_short_ratio_5m: APS = cron */5 at second=5
        # ------------------------------------------------------------------------------
        sync_long_short_ratio_5m.to_deployment(
            "sync-long-short-ratio-5m",
            schedule=per_second_cron([5], minutes="*/5"),
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        # ------------------------------------------------------------------------------
        # sync_long_short_ratio_1h: APS second="5,30" minute=0
        # ------------------------------------------------------------------------------
        sync_long_short_ratio_1h.to_deployment(
            "sync-long-short-ratio-1h",
            schedule=per_second_cron([5, 30], minutes="0"),
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        # ------------------------------------------------------------------------------
        # sync_long_short_ratio_1d: APS hour=0 minute=0 second=5,30
        # ------------------------------------------------------------------------------
        sync_long_short_ratio_1d.to_deployment(
            "sync-long-short-ratio-1d",
            schedule=per_second_cron([5, 30], minutes="0", hours="0"),
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        # ------------------------------------------------------------------------------
        # sync_funding_rate: APS = cron minute="0,1,5,30" second=5
        # ------------------------------------------------------------------------------
        sync_funding_rate.to_deployment(
            "sync-funding-rate",
            schedule=per_second_cron([5], minutes="0,1,5,30"),
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        # ------------------------------------------------------------------------------
        # sync_large_transfer: APS interval(seconds=30)
        # ------------------------------------------------------------------------------
        sync_onchain_large_transfer.to_deployment(
            "sync-large-transfer",
            schedule=IntervalSchedule(interval=30),
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        # ------------------------------------------------------------------------------
        # sync_cex_inflow: APS cron minute=0 second=5,30
        # ------------------------------------------------------------------------------
        sync_cex_inflow.to_deployment(
            "sync-cex-inflow",
            schedule=per_second_cron([5, 30], minutes="0"),
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        # ------------------------------------------------------------------------------
        # sync_macro_indicators: APS = interval(30s)
        # ------------------------------------------------------------------------------
        sync_macro_indicators.to_deployment(
            "sync-macro-indicators",
            schedule=IntervalSchedule(interval=30),
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        # ------------------------------------------------------------------------------
        # sync_kalshi: APS interval 60s
        # ------------------------------------------------------------------------------
        sync_kalshi_flow.to_deployment(
            "sync-kalshi",
            schedule=IntervalSchedule(interval=60),
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
    ]

    deploy(
        *deployments,
        image=IMAGE_URL,
        build=False,
        work_pool_name=POOL_MAP[ENV],
    )
