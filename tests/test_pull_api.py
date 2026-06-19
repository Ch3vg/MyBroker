from datetime import UTC, datetime, timedelta
import asyncio
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker

from broker import Broker
from broker.db.enums import TaskStatus
from broker.db.models import Task
from helpers import broker_test_client, resolve_storage_dsn


async def _publish(client: AsyncClient, task_type: str = "job", **kwargs) -> str:
    payload = kwargs.pop("payload", {})
    response = await client.post(
        "/api/v1/tasks",
        json={"task_type": task_type, "payload": payload, **kwargs},
    )
    assert response.status_code == 201
    return response.json()["task_id"]


@pytest.mark.asyncio
async def test_pull_returns_task(client: AsyncClient) -> None:
    task_id = await _publish(client, payload={"x": 1})
    response = await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})
    assert response.status_code == 200
    body = response.json()
    assert body["task_id"] == task_id
    assert body["task_type"] == "job"
    assert body["payload"] == {"x": 1}
    assert body["lock_ttl_seconds"] == 60

    status = await client.get(f"/api/v1/tasks/{task_id}/status")
    assert status.json()["status"] == "PROCESSING"


@pytest.mark.asyncio
async def test_pull_returns_204_when_empty(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})
    assert response.status_code == 204
    assert response.content == b""


@pytest.mark.asyncio
async def test_pull_filters_task_types(client: AsyncClient) -> None:
    await _publish(client, task_type="wanted", payload={})
    await _publish(client, task_type="other", payload={})

    response = await client.get(
        "/api/v1/tasks/pull",
        params={"worker_id": "w1", "timeout": 0, "task_types": "wanted"},
    )
    assert response.status_code == 200
    assert response.json()["task_type"] == "wanted"


@pytest.mark.asyncio
async def test_pull_skips_delayed_task(client: AsyncClient) -> None:
    await _publish(client, delay_seconds=3600)
    response = await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_pull_reclaims_expired_lock(client: AsyncClient, broker: Broker) -> None:
    task_id = await _publish(client)
    first = await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})
    assert first.status_code == 200

    session_factory = async_sessionmaker(broker.engine, expire_on_commit=False)
    async with session_factory() as session:
        task = await session.get(Task, task_id)
        assert task is not None
        task.lock_until = datetime.now(UTC) - timedelta(seconds=1)
        task.status = TaskStatus.PROCESSING.value
        await session.commit()

    second = await client.get("/api/v1/tasks/pull", params={"worker_id": "w2", "timeout": 0})
    assert second.status_code == 200
    assert second.json()["task_id"] == task_id


@pytest.mark.stress
@pytest.mark.stress_db
@pytest.mark.asyncio
async def test_concurrent_pull_assigns_task_once(
    storage_backend: str,
    memory_dsn: str,
    _stress_attempt: int,
) -> None:
    dsn = resolve_storage_dsn(storage_backend, memory_dsn)
    task_type = f"concurrent.job.{uuid.uuid4().hex[:12]}"
    pull_params = {"timeout": 0, "task_types": task_type}
    async with broker_test_client(dsn) as client:
        task_id = await _publish(client, task_type=task_type)
        results = await asyncio.gather(
            client.get("/api/v1/tasks/pull", params={"worker_id": "w1", **pull_params}),
            client.get("/api/v1/tasks/pull", params={"worker_id": "w2", **pull_params}),
        )
    statuses = [response.status_code for response in results]
    assert statuses.count(200) == 1
    assert statuses.count(204) == 1
    winner = next(response for response in results if response.status_code == 200)
    assert winner.json()["task_id"] == task_id


@pytest.mark.asyncio
async def test_pull_caps_timeout_at_max(memory_dsn: str) -> None:
    broker = Broker(dsn=memory_dsn, max_pull_timeout_seconds=0, log_level="WARNING")
    app = broker.app
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/api/v1/tasks/pull",
                params={"worker_id": "w1", "timeout": 30},
            )
    assert response.status_code == 204
