import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine

from broker import Broker
from broker.db.models import Task
from broker.db.schema import init_schema


def stress_db_backends() -> list[str]:
    backends = ["sqlite"]
    if os.getenv("BROKER_POSTGRES_DSN"):
        backends.append("postgresql")
    return backends


def is_postgres_dsn(dsn: str) -> bool:
    return dsn.startswith("postgresql")


def resolve_storage_dsn(storage_backend: str, memory_dsn: str) -> str:
    if storage_backend == "sqlite":
        return memory_dsn
    postgres_dsn = os.getenv("BROKER_POSTGRES_DSN")
    if not postgres_dsn:
        pytest.fail("postgresql backend requested but BROKER_POSTGRES_DSN is not set")
    return postgres_dsn


async def truncate_tasks(engine: AsyncEngine) -> None:
    await init_schema(engine)
    async with engine.begin() as conn:
        await conn.execute(delete(Task))


async def cleanup_postgres_test_db(dsn: str) -> None:
    if not is_postgres_dsn(dsn):
        return
    broker = Broker(dsn=dsn, log_level="WARNING")
    try:
        await truncate_tasks(broker.engine)
    finally:
        await broker.engine.dispose()


@asynccontextmanager
async def broker_test_client(dsn: str) -> AsyncIterator[AsyncClient]:
    broker = Broker(dsn=dsn, log_level="WARNING")
    if is_postgres_dsn(dsn):
        await truncate_tasks(broker.engine)
    app = broker.app
    try:
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as async_client:
                yield async_client
    finally:
        if is_postgres_dsn(dsn):
            await truncate_tasks(broker.engine)
        await broker.engine.dispose()
