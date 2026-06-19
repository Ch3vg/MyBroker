import pytest
from httpx import ASGITransport, AsyncClient

from broker import Broker

MEMORY_DSN = "sqlite+aiosqlite:///file:memdb1?mode=memory&cache=shared&uri=true"


@pytest.fixture
def memory_dsn() -> str:
    return MEMORY_DSN


@pytest.fixture
def broker(memory_dsn: str) -> Broker:
    return Broker(dsn=memory_dsn, log_level="WARNING")


@pytest.fixture
async def client(broker: Broker) -> AsyncClient:
    transport = ASGITransport(app=broker.app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client
