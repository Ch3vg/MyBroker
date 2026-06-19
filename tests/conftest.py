import uuid
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from broker import Broker
from broker.db.schema import init_schema

STRESS_ITERATIONS = 10


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "stress: повторяет недетерминированный тест несколько раз (см. STRESS_ITERATIONS)",
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    marker = metafunc.definition.get_closest_marker("stress")
    if marker is None:
        return
    iterations = marker.kwargs.get("iterations", STRESS_ITERATIONS)
    metafunc.parametrize(
        "_stress_attempt",
        range(iterations),
        ids=lambda attempt: f"attempt-{attempt + 1}",
    )


@pytest.fixture
def memory_dsn() -> str:
    uid = uuid.uuid4().hex
    return f"sqlite+aiosqlite:///file:{uid}?mode=memory&cache=shared&uri=true"


@pytest.fixture
def broker(memory_dsn: str) -> Broker:
    return Broker(dsn=memory_dsn, log_level="WARNING")


@pytest.fixture
async def db_session(broker: Broker) -> AsyncIterator[AsyncSession]:
    await init_schema(broker.engine)
    session_factory = async_sessionmaker(broker.engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session


@pytest.fixture
async def client(broker: Broker) -> AsyncIterator[AsyncClient]:
    app = broker.app
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as async_client:
            yield async_client
