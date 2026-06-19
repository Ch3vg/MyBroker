from datetime import UTC, datetime

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_publish_task_returns_201(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/tasks",
        json={
            "task_type": "config.regenerate",
            "payload": {"config_id": "123"},
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert "task_id" in body
    assert body["task_id"]


@pytest.mark.asyncio
async def test_publish_task_with_delay_and_max_retries(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/tasks",
        json={
            "task_type": "email.send",
            "payload": {"to": "a@b.c"},
            "delay_seconds": 60,
            "max_retries": 7,
        },
    )
    assert response.status_code == 201
    task_id = response.json()["task_id"]

    status_response = await client.get(f"/api/v1/tasks/{task_id}/status")
    assert status_response.status_code == 200
    status_body = status_response.json()
    assert status_body["id"] == task_id
    assert status_body["status"] == "PENDING"
    assert status_body["retries"] == 0
    assert status_body["max_retries"] == 7
    available_at = datetime.fromisoformat(status_body["available_at"].replace("Z", "+00:00"))
    if available_at.tzinfo is None:
        available_at = available_at.replace(tzinfo=UTC)
    assert available_at > datetime.now(UTC)


@pytest.mark.asyncio
async def test_publish_task_uses_default_max_retries(client: AsyncClient, broker) -> None:
    response = await client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {}},
    )
    task_id = response.json()["task_id"]

    status_response = await client.get(f"/api/v1/tasks/{task_id}/status")
    assert status_response.json()["max_retries"] == broker.settings.default_max_retries


@pytest.mark.asyncio
async def test_publish_task_validation_errors(client: AsyncClient) -> None:
    empty_type = await client.post(
        "/api/v1/tasks",
        json={"task_type": "", "payload": {}},
    )
    assert empty_type.status_code == 422

    missing_payload = await client.post(
        "/api/v1/tasks",
        json={"task_type": "job"},
    )
    assert missing_payload.status_code == 422

    negative_delay = await client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {}, "delay_seconds": -1},
    )
    assert negative_delay.status_code == 422

    negative_retries = await client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {}, "max_retries": -1},
    )
    assert negative_retries.status_code == 422


@pytest.mark.asyncio
async def test_get_task_status_returns_fields(client: AsyncClient) -> None:
    publish = await client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {"k": "v"}},
    )
    task_id = publish.json()["task_id"]

    response = await client.get(f"/api/v1/tasks/{task_id}/status")
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"id", "status", "retries", "max_retries", "available_at", "created_at"}
    assert body["id"] == task_id
    assert body["status"] == "PENDING"
    assert body["retries"] == 0


@pytest.mark.asyncio
async def test_get_task_status_not_found(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks/00000000-0000-0000-0000-000000000000/status")
    assert response.status_code == 404
    assert response.json()["detail"] == "Task not found"
