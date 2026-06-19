from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BrokerSettings:
    dsn: str = "sqlite+aiosqlite:///./broker.db"
    host: str = "0.0.0.0"
    port: int = 8001
    default_lock_ttl_seconds: int = 60
    default_max_retries: int = 3
    retry_delay_seconds: int = 5
    dead_letter_enabled: bool = True
    default_pull_timeout_seconds: int = 30
    max_pull_timeout_seconds: int = 120
    pull_interval_seconds: int = 1
    list_default_limit: int = 50
    list_max_limit: int = 200
    api_key: str | None = None
    log_level: str = "INFO"


DEFAULT_SETTINGS = BrokerSettings()
