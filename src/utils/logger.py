import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys

from dotenv import load_dotenv
from loguru import logger as loguru_logger
import structlog

load_dotenv()


def configure_dev_logging():
    """
    Development = loguru + structlog console output
    """
    loguru_logger.remove()
    loguru_logger.add(sys.stderr, level="DEBUG", colorize=True, backtrace=True, diagnose=True)

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        logger_factory=structlog.stdlib.LoggerFactory(),
        context_class=dict,
    )

    # intercept Python logging -> loguru
    class InterceptHandler(logging.Handler):
        def emit(self, record):
            loguru_logger.opt(depth=6, exception=record.exc_info).log(record.levelname, record.getMessage())

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    return loguru_logger  # DEV 只用 loguru


def rename_keys(_, __, event_dict):
    if "timestamp" in event_dict:
        event_dict["ts"] = event_dict.pop("timestamp")
    if "event" in event_dict:
        event_dict["msg"] = event_dict.pop("event")
    return event_dict


def format_caller(_, __, event_dict):
    module = event_dict.get("module")
    func = event_dict.get("func_name")
    lineno = event_dict.get("lineno")

    if module and func and lineno:
        event_dict["caller"] = f"{module}:{func}:{lineno}"

    # 清理不需要的字段
    event_dict.pop("module", None)
    event_dict.pop("func_name", None)
    event_dict.pop("lineno", None)

    return event_dict


def configure_prod_logging():
    """
    Production logging:
    - structlog JSON → file + stdout
    - no loguru
    """
    loguru_logger.remove()
    try:
        log_dir = "/app/logs"
        os.makedirs(log_dir, exist_ok=True)
    except PermissionError:
        log_dir = "/tmp/logs"
        os.makedirs(log_dir, exist_ok=True)

    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "app.log"),
        when="midnight",
        interval=1,
        backupCount=3,
        encoding="utf-8",
        utc=True,
    )
    file_handler.setLevel(logging.INFO)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)

    logging.basicConfig(
        handlers=[file_handler, stdout_handler],
        level=logging.INFO,
        format="%(message)s",
        force=True,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.CallsiteParameterAdder(
                parameters=[
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.MODULE,
                    structlog.processors.CallsiteParameter.LINENO,
                ]
            ),
            format_caller,
            rename_keys,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(),
        context_class=dict,
    )

    class InterceptHandler(logging.Handler):
        def emit(self, record):
            try:
                level = record.levelno
            except Exception:
                level = logging.INFO

            struct_logger = structlog.get_logger(record.name)
            caller = f"{record.name}:{record.funcName}:{record.lineno}"

            struct_logger.bind(caller=caller).log(level, record.getMessage())

    root_logger = logging.getLogger()
    root_logger.handlers = [InterceptHandler()]
    root_logger.setLevel(logging.INFO)

    return structlog.get_logger()


def setup_logging():
    env = os.getenv("ENV", "development").lower()
    if env == "development":
        return configure_dev_logging()
    else:
        return configure_prod_logging()


logger = setup_logging()
