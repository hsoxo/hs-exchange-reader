import logging
import os
import sys

from loguru import logger
from pythonjsonlogger import jsonlogger
import structlog

__all__ = ["logger"]


def setup_logging():
    env = os.getenv("ENV", "development").lower()

    logger.remove()

    if env == "development":
        logger.add(sys.stderr, level="DEBUG", backtrace=True, diagnose=True, colorize=True)

        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
        )

        class InterceptHandler(logging.Handler):
            def emit(self, record):
                logger_opt = logger.opt(depth=6, exception=record.exc_info)
                logger_opt.log(record.levelname, record.getMessage())

        logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
        return logger

    else:
        json_handler = logging.StreamHandler()
        json_handler.setFormatter(
            jsonlogger.JsonFormatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s",
                rename_fields={"asctime": "timestamp", "levelname": "level"},
            )
        )

        # 强制覆盖任何已有 handler（例如 uvicorn）
        logging.basicConfig(handlers=[json_handler], level=logging.INFO, force=True)

        structlog.configure(
            processors=[
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
        )

        # loguru -> structlog
        class StructlogSink:
            def write(self, message):
                msg = message.strip()
                if msg:
                    structlog.get_logger().info(msg)

        logger.add(StructlogSink(), level="INFO")

        return logger


logger = setup_logging()
