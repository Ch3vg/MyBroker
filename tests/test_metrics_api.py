import pytest
from httpx import AsyncClient

from broker.metrics import refresh_status_gauges


def _metric_value(body: str, name: str, labels: str | None = None) -> float:
    if labels is None:
        needle = f"{name} "
    else:
        needle = f"{name}{{{labels}}} "
    for line in body.splitlines():
        if line.startswith("#"):
            continue
        if line.startswith(needle):
            return float(line.rsplit(" ", maxsplit=1)[-1])
    raise AssertionError(f"metric not found: {needle.strip()}")


def _metric_value_or_zero(body: str, name: str, labels: str) -> float:
    try:
        return _metric_value(body, name, labels)
    except AssertionError:
        return 0.0


@pytest.mark.asyncio
async def test_metrics_returns_prometheus_format(client: AsyncClient) -> None:
    response = await client.get("/api/v1/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    body = response.text
    for metric_name in (
        "broker_tasks_pending",
        "broker_tasks_processing",
        "broker_tasks_dead",
        "broker_tasks_published_total",
        "broker_tasks_completed_total",
        "broker_tasks_nacked_total",
        "broker_pull_duration_seconds",
        "broker_pull_empty_total",
    ):
        assert f"# HELP {metric_name}" in body
        assert f"# TYPE {metric_name}" in body


@pytest.mark.asyncio
async def test_metrics_pending_gauge_after_publish(client: AsyncClient) -> None:
    await client.post(
        "/api/v1/tasks",
        json={"task_type": "email.send", "payload": {}},
    )
    body = (await client.get("/api/v1/metrics")).text
    assert _metric_value(body, "broker_tasks_pending", 'task_type="email.send"') == 1.0


@pytest.mark.asyncio
async def test_metrics_processing_gauge_after_pull(client: AsyncClient) -> None:
    await client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {}},
    )
    await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})
    body = (await client.get("/api/v1/metrics")).text
    assert _metric_value(body, "broker_tasks_processing", 'task_type="job"') == 1.0


@pytest.mark.asyncio
async def test_metrics_published_counter(client: AsyncClient) -> None:
    await client.post(
        "/api/v1/tasks",
        json={"task_type": "report", "payload": {}},
    )
    body = (await client.get("/api/v1/metrics")).text
    assert _metric_value(body, "broker_tasks_published_total", 'task_type="report"') == 1.0


@pytest.mark.asyncio
async def test_metrics_completed_counter_after_ack(client: AsyncClient) -> None:
    before = (await client.get("/api/v1/metrics")).text
    baseline = _metric_value_or_zero(before, "broker_tasks_completed_total", 'task_type="job"')
    publish = await client.post(
        "/api/v1/tasks",
        json={"task_type": "job", "payload": {}},
    )
    task_id = publish.json()["task_id"]
    await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})
    await client.post(f"/api/v1/tasks/{task_id}/ack", json={"worker_id": "w1"})
    body = (await client.get("/api/v1/metrics")).text
    assert _metric_value(body, "broker_tasks_completed_total", 'task_type="job"') == baseline + 1.0


@pytest.mark.asyncio
async def test_metrics_nacked_counter(client: AsyncClient) -> None:
    before = (await client.get("/api/v1/metrics")).text
    baseline = _metric_value_or_zero(before, "broker_tasks_nacked_total", 'task_type="unique-nack"')
    publish = await client.post(
        "/api/v1/tasks",
        json={"task_type": "unique-nack", "payload": {}},
    )
    task_id = publish.json()["task_id"]
    await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})
    await client.post(
        f"/api/v1/tasks/{task_id}/nack",
        json={"worker_id": "w1", "reason": "fail"},
    )
    body = (await client.get("/api/v1/metrics")).text
    assert _metric_value(body, "broker_tasks_nacked_total", 'task_type="unique-nack"') == baseline + 1.0


@pytest.mark.asyncio
async def test_metrics_dead_gauge_after_max_nacks(client: AsyncClient) -> None:
    publish = await client.post(
        "/api/v1/tasks",
        json={"task_type": "fragile", "payload": {}, "max_retries": 1},
    )
    task_id = publish.json()["task_id"]
    await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})
    await client.post(
        f"/api/v1/tasks/{task_id}/nack",
        json={"worker_id": "w1", "reason": "fail"},
    )
    body = (await client.get("/api/v1/metrics")).text
    assert _metric_value(body, "broker_tasks_dead", 'task_type="fragile"') == 1.0


@pytest.mark.asyncio
async def test_metrics_pull_empty_counter(client: AsyncClient) -> None:
    await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})
    body = (await client.get("/api/v1/metrics")).text
    assert _metric_value(body, "broker_pull_empty_total") >= 1.0


@pytest.mark.asyncio
async def test_metrics_pull_duration_histogram_recorded(client: AsyncClient) -> None:
    await client.get("/api/v1/tasks/pull", params={"worker_id": "w1", "timeout": 0})
    body = (await client.get("/api/v1/metrics")).text
    assert "broker_pull_duration_seconds_count" in body
    assert "broker_pull_duration_seconds_sum" in body


def test_refresh_status_gauges_clears_removed_labels() -> None:
    refresh_status_gauges([("PENDING", "old-type", 2)])
    refresh_status_gauges([("PENDING", "new-type", 1)])
    from prometheus_client import generate_latest

    body = generate_latest().decode()
    assert 'task_type="new-type"' in body
    assert _metric_value(body, "broker_tasks_pending", 'task_type="new-type"') == 1.0
    assert _metric_value(body, "broker_tasks_pending", 'task_type="old-type"') == 0.0
