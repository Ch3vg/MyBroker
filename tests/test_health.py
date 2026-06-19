import pytest
from httpx import AsyncClient
from sqlalchemy import inspect

from broker import Broker


@pytest.mark.asyncio
async def test_health_returns_ok(client: AsyncClient) -> None:
    response = await client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_app_lifespan_initializes_schema(broker: Broker) -> None:
    app = broker.app
    async with app.router.lifespan_context(app):
        async with broker.engine.connect() as conn:
            tables = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_table_names()
            )
        assert "tasks" in tables
