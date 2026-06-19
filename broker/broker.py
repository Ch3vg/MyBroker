from typing import Any

import structlog
import uvicorn
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from broker.api.app import create_app
from broker.config import DEFAULT_SETTINGS, BrokerSettings
from broker.logging_config import configure_logging

logger = structlog.get_logger(__name__)


class Broker:
    def __init__(
        self,
        *,
        dsn: str = DEFAULT_SETTINGS.dsn,
        host: str = DEFAULT_SETTINGS.host,
        port: int = DEFAULT_SETTINGS.port,
        default_lock_ttl_seconds: int = DEFAULT_SETTINGS.default_lock_ttl_seconds,
        default_max_retries: int = DEFAULT_SETTINGS.default_max_retries,
        retry_delay_seconds: int = DEFAULT_SETTINGS.retry_delay_seconds,
        dead_letter_enabled: bool = DEFAULT_SETTINGS.dead_letter_enabled,
        default_pull_timeout_seconds: int = DEFAULT_SETTINGS.default_pull_timeout_seconds,
        max_pull_timeout_seconds: int = DEFAULT_SETTINGS.max_pull_timeout_seconds,
        pull_interval_seconds: int = DEFAULT_SETTINGS.pull_interval_seconds,
        list_default_limit: int = DEFAULT_SETTINGS.list_default_limit,
        list_max_limit: int = DEFAULT_SETTINGS.list_max_limit,
        log_level: str = DEFAULT_SETTINGS.log_level,
    ) -> None:
        self.settings = BrokerSettings(
            dsn=dsn,
            host=host,
            port=port,
            default_lock_ttl_seconds=default_lock_ttl_seconds,
            default_max_retries=default_max_retries,
            retry_delay_seconds=retry_delay_seconds,
            dead_letter_enabled=dead_letter_enabled,
            default_pull_timeout_seconds=default_pull_timeout_seconds,
            max_pull_timeout_seconds=max_pull_timeout_seconds,
            pull_interval_seconds=pull_interval_seconds,
            list_default_limit=list_default_limit,
            list_max_limit=list_max_limit,
            log_level=log_level,
        )
        configure_logging(self.settings.log_level)
        self._engine = create_async_engine(self.settings.dsn)
        self._app: FastAPI | None = None
        logger.info("broker_initialized", dsn=self.settings.dsn, host=self.settings.host, port=self.settings.port)

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @property
    def app(self) -> FastAPI:
        if self._app is None:
            self._app = create_app(self)
        return self._app

    def run(self, **uvicorn_kwargs: Any) -> None:
        uvicorn.run(
            self.app,
            host=self.settings.host,
            port=self.settings.port,
            **uvicorn_kwargs,
        )
