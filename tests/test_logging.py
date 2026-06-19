import structlog

from broker import Broker
from broker.logging_config import configure_logging


def test_configure_logging_returns_logger() -> None:
    configure_logging("ERROR")
    logger = structlog.get_logger("test")
    assert logger is not None


def test_broker_applies_log_level() -> None:
    configure_logging("INFO")
    broker = Broker(dsn="sqlite+aiosqlite:///:memory:", log_level="CRITICAL")
    assert broker.settings.log_level == "CRITICAL"


def test_configure_logging_accepts_lowercase_level() -> None:
    configure_logging("warning")
    logger = structlog.get_logger("test")
    assert logger is not None


def test_configure_logging_invalid_level_does_not_raise() -> None:
    configure_logging("NOT_A_LEVEL")
    logger = structlog.get_logger("test")
    assert logger is not None
