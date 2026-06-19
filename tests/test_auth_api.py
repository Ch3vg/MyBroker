import pytest
from collections.abc import AsyncIterator
from httpx import ASGITransport, AsyncClient

from broker import Broker

API_KEY = "test-secret-key"
AUTH_HEADERS = {"Authorization": f"Bearer {API_KEY}"}


@pytest.fixture
async def secured_client(memory_dsn: str) -> AsyncIterator[AsyncClient]:
    broker = Broker(dsn=memory_dsn, api_key=API_KEY, log_level="WARNING")
    app = broker.app
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.mark.asyncio
async def test_health_accessible_without_api_key(secured_client: AsyncClient) -> None:
    response = await secured_client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_protected_endpoint_without_key_returns_401(secured_client: AsyncClient) -> None:
    response = await secured_client.get("/api/v1/metrics")
    assert response.status_code == 401
    assert response.json() == {"detail": "Unauthorized"}


@pytest.mark.asyncio
async def test_protected_endpoint_with_wrong_key_returns_401(secured_client: AsyncClient) -> None:
    response = await secured_client.get(
        "/api/v1/metrics",
        headers={"Authorization": "Bearer wrong-key"},
    )
    assert response.status_code == 401
    assert response.json() == {"detail": "Unauthorized"}


@pytest.mark.asyncio
async def test_protected_endpoint_with_malformed_auth_returns_401(secured_client: AsyncClient) -> None:
    response = await secured_client.get(
        "/api/v1/metrics",
        headers={"Authorization": API_KEY},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_protected_endpoint_with_valid_key_succeeds(secured_client: AsyncClient) -> None:
    response = await secured_client.get("/api/v1/metrics", headers=AUTH_HEADERS)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_publish_requires_valid_api_key(secured_client: AsyncClient) -> None:
    denied = await secured_client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {}},
    )
    assert denied.status_code == 401

    allowed = await secured_client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {}},
        headers=AUTH_HEADERS,
    )
    assert allowed.status_code == 201
