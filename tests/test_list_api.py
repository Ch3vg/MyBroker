import pytest
from httpx import ASGITransport, AsyncClient

from broker import Broker


@pytest.mark.asyncio
async def test_list_tasks_returns_empty(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks")
    assert response.status_code == 200
    body = response.json()
    assert body == {"items": [], "total": 0, "limit": 50, "offset": 0}


@pytest.mark.asyncio
async def test_list_tasks_returns_published_tasks(client: AsyncClient) -> None:
    first = await client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {"n": 1}},
    )
    second = await client.post(
        "/api/v1/tasks",
        json={"task_type": "email.send", "payload": {}},
    )
    first_id = first.json()["task_id"]
    second_id = second.json()["task_id"]

    response = await client.get("/api/v1/tasks")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["limit"] == 50
    assert body["offset"] == 0
    assert len(body["items"]) == 2
    ids = {item["id"] for item in body["items"]}
    assert ids == {first_id, second_id}
    item = body["items"][0]
    assert set(item) == {
        "id",
        "task_type",
        "status",
        "retries",
        "max_retries",
        "available_at",
        "created_at",
    }


@pytest.mark.asyncio
async def test_list_tasks_filters_by_status(client: AsyncClient) -> None:
    publish = await client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {}, "max_retries": 1},
    )
    task_id = publish.json()["task_id"]
    await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})
    await client.post(
        f"/api/v1/tasks/{task_id}/nack",
        json={"worker_id": "w1", "reason": "fail"},
    )

    response = await client.get("/api/v1/tasks", params={"status": "DEAD"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == task_id
    assert body["items"][0]["status"] == "DEAD"


@pytest.mark.asyncio
async def test_list_tasks_filters_by_task_type(client: AsyncClient) -> None:
    await client.post("/api/v1/tasks", json={"task_type": "wanted", "payload": {}})
    await client.post("/api/v1/tasks", json={"task_type": "other", "payload": {}})

    response = await client.get("/api/v1/tasks", params={"task_type": "wanted"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["task_type"] == "wanted"


@pytest.mark.asyncio
async def test_list_tasks_supports_pagination(client: AsyncClient) -> None:
    for index in range(3):
        await client.post(
            "/api/v1/tasks",
            json={"task_type": "job", "payload": {"index": index}},
        )

    page = await client.get("/api/v1/tasks", params={"limit": 2, "offset": 1})
    assert page.status_code == 200
    body = page.json()
    assert body["total"] == 3
    assert body["limit"] == 2
    assert body["offset"] == 1
    assert len(body["items"]) == 2


@pytest.mark.asyncio
async def test_list_tasks_caps_limit_at_max(memory_dsn: str) -> None:
    broker = Broker(
        dsn=memory_dsn,
        list_max_limit=2,
        log_level="WARNING",
    )
    app = broker.app
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            for _ in range(3):
                await client.post("/api/v1/tasks", json={"task_type": "job", "payload": {}})
            response = await client.get("/api/v1/tasks", params={"limit": 10})

    body = response.json()
    assert body["limit"] == 2
    assert len(body["items"]) == 2
    assert body["total"] == 3


@pytest.mark.asyncio
async def test_list_tasks_rejects_invalid_status(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks", params={"status": "BROKEN"})
    assert response.status_code == 422
    assert response.json()["detail"] == "Invalid status"
