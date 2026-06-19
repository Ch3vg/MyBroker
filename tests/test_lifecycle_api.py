from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker

from broker import Broker
from broker.db.models import Task


async def _publish_and_pull(client: AsyncClient, worker_id: str = "w1") -> str:
    publish = await client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {}},
    )
    task_id = publish.json()["task_id"]
    pull = await client.get("/api/v1/tasks/pull", params={"worker_id": worker_id, "timeout": 0})
    assert pull.status_code == 200
    assert pull.json()["task_id"] == task_id
    return task_id


@pytest.mark.asyncio
async def test_heartbeat_returns_200(client: AsyncClient) -> None:
    task_id = await _publish_and_pull(client)
    response = await client.post(
        f"/api/v1/tasks/{task_id}/heartbeat",
        json={"worker_id": "w1"},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_heartbeat_returns_409_for_stale_worker(client: AsyncClient) -> None:
    task_id = await _publish_and_pull(client, worker_id="w1")
    response = await client.post(
        f"/api/v1/tasks/{task_id}/heartbeat",
        json={"worker_id": "w2"},
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "STALE_TASK"


@pytest.mark.asyncio
async def test_heartbeat_returns_404_for_missing_task(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/tasks/00000000-0000-0000-0000-000000000000/heartbeat",
        json={"worker_id": "w1"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_ack_completes_task_via_api(client: AsyncClient) -> None:
    task_id = await _publish_and_pull(client)
    response = await client.post(
        f"/api/v1/tasks/{task_id}/ack",
        json={"worker_id": "w1"},
    )
    assert response.status_code == 200

    status = await client.get(f"/api/v1/tasks/{task_id}/status")
    assert status.json()["status"] == "COMPLETED"


@pytest.mark.asyncio
async def test_ack_returns_409_for_stale_worker(client: AsyncClient) -> None:
    task_id = await _publish_and_pull(client)
    response = await client.post(
        f"/api/v1/tasks/{task_id}/ack",
        json={"worker_id": "w2"},
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "STALE_TASK"


@pytest.mark.asyncio
async def test_nack_retries_task_via_api(client: AsyncClient) -> None:
    task_id = await _publish_and_pull(client)
    response = await client.post(
        f"/api/v1/tasks/{task_id}/nack",
        json={"worker_id": "w1", "reason": "timeout"},
    )
    assert response.status_code == 200

    status = await client.get(f"/api/v1/tasks/{task_id}/status")
    body = status.json()
    assert body["status"] == "PENDING"
    assert body["retries"] == 1


@pytest.mark.asyncio
async def test_nack_moves_task_to_dead_via_api(client: AsyncClient) -> None:
    publish = await client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {}, "max_retries": 1},
    )
    task_id = publish.json()["task_id"]
    await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})

    response = await client.post(
        f"/api/v1/tasks/{task_id}/nack",
        json={"worker_id": "w1", "reason": "failed"},
    )
    assert response.status_code == 200

    status = await client.get(f"/api/v1/tasks/{task_id}/status")
    assert status.json()["status"] == "DEAD"


@pytest.mark.asyncio
async def test_nack_returns_404_for_missing_task(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/tasks/00000000-0000-0000-0000-000000000000/nack",
        json={"worker_id": "w1", "reason": "missing"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_ack_returns_404_for_missing_task(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/tasks/00000000-0000-0000-0000-000000000000/ack",
        json={"worker_id": "w1"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_stale_lifecycle_after_lock_reclaimed(client: AsyncClient, broker: Broker) -> None:
    task_id = await _publish_and_pull(client, worker_id="w1")

    session_factory = async_sessionmaker(broker.engine, expire_on_commit=False)
    async with session_factory() as session:
        task = await session.get(Task, task_id)
        assert task is not None
        task.lock_until = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()

    reclaim = await client.get("/api/v1/tasks/pull", params={"worker_id": "w2", "timeout": 0})
    assert reclaim.status_code == 200

    heartbeat = await client.post(
        f"/api/v1/tasks/{task_id}/heartbeat",
        json={"worker_id": "w1"},
    )
    assert heartbeat.status_code == 409
    assert heartbeat.json()["detail"] == "STALE_TASK"

    ack = await client.post(
        f"/api/v1/tasks/{task_id}/ack",
        json={"worker_id": "w1"},
    )
    assert ack.status_code == 409

    nack = await client.post(
        f"/api/v1/tasks/{task_id}/nack",
        json={"worker_id": "w1", "reason": "late"},
    )
    assert nack.status_code == 409

    status = await client.get(f"/api/v1/tasks/{task_id}/status")
    assert status.json()["status"] == "PROCESSING"
