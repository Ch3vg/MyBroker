from broker import Broker
from broker.config import BrokerSettings


def test_broker_settings_defaults() -> None:
    settings = BrokerSettings()
    assert settings.dsn == "sqlite+aiosqlite:///./broker.db"
    assert settings.host == "0.0.0.0"
    assert settings.port == 8001
    assert settings.default_lock_ttl_seconds == 60
    assert settings.default_max_retries == 3
    assert settings.retry_delay_seconds == 5
    assert settings.dead_letter_enabled is True
    assert settings.default_pull_timeout_seconds == 30
    assert settings.max_pull_timeout_seconds == 120
    assert settings.pull_interval_seconds == 1
    assert settings.list_default_limit == 50
    assert settings.list_max_limit == 200
    assert settings.log_level == "INFO"


def test_broker_stores_custom_settings() -> None:
    broker = Broker(
        dsn="sqlite+aiosqlite:///custom.db",
        host="127.0.0.1",
        port=9000,
        default_max_retries=5,
        log_level="ERROR",
    )
    assert broker.settings.dsn == "sqlite+aiosqlite:///custom.db"
    assert broker.settings.host == "127.0.0.1"
    assert broker.settings.port == 9000
    assert broker.settings.default_max_retries == 5
    assert broker.settings.log_level == "ERROR"


def test_broker_app_is_cached(broker: Broker) -> None:
    assert broker.app is broker.app


def test_broker_run_calls_uvicorn(broker: Broker, mocker) -> None:
    mock_run = mocker.patch("broker.broker.uvicorn.run")
    broker.run(reload=False)
    mock_run.assert_called_once_with(
        broker.app,
        host=broker.settings.host,
        port=broker.settings.port,
        reload=False,
    )
