import asyncio
import functools
import time

from prefect import get_run_logger


def flow_timing(name: str = None):
    """
    Decorator for Prefect flows to log start/end time and elapsed seconds.
    """

    def decorator(fn):
        @functools.wraps(fn)
        async def async_wrapper(*args, **kwargs):
            logger = get_run_logger()
            _name = name or fn.__name__

            start_ts = time.time()
            logger.info(f"[FLOW STARTED] {_name} at {start_ts}")

            try:
                result = await fn(*args, **kwargs)
                return result
            finally:
                end_ts = time.time()
                elapsed = round(end_ts - start_ts, 3)
                logger.info(f"[FLOW ENDED] {_name} at {end_ts}, elapsed={elapsed}s")

        @functools.wraps(fn)
        def sync_wrapper(*args, **kwargs):
            logger = get_run_logger()
            _name = name or fn.__name__

            start_ts = time.time()
            logger.info(f"[FLOW STARTED] {_name} at {start_ts}")

            try:
                result = fn(*args, **kwargs)
                return result
            finally:
                end_ts = time.time()
                elapsed = round(end_ts - start_ts, 3)
                logger.info(f"[FLOW ENDED] {_name} at {end_ts}, elapsed={elapsed}s")

        # 支持 sync + async Flow
        if asyncio.iscoroutinefunction(fn):
            return async_wrapper
        return sync_wrapper

    return decorator
