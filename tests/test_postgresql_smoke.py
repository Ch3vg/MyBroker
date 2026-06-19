import os
import uuid

import pytest

from helpers import broker_test_client

pytestmark = pytest.mark.postgresql

POSTGRES_DSN = os.getenv("BROKER_POSTGRES_DSN")


@pytest.mark.skipif(not POSTGRES_DSN, reason="BROKER_POSTGRES_DSN is not set")
@pytest.mark.asyncio
async def test_postgres_publish_pull_ack_smoke() -> None:
    task_type = f"postgres.smoke.{uuid.uuid4().hex[:12]}"
    async with broker_test_client(POSTGRES_DSN) as client:
        publish = await client.post(
            "/api/v1/tasks",
            json={"task_type": task_type, "payload": {"ok": True}},
        )
        assert publish.status_code == 201
        task_id = publish.json()["task_id"]

        listed = await client.get(
            "/api/v1/tasks",
            params={"task_type": task_type, "status": "PENDING"},
        )
        assert listed.status_code == 200
        assert listed.json()["total"] == 1

        pulled = await client.get(
            "/api/v1/tasks/pull",
            params={"worker_id": "pg-smoke", "timeout": 0, "task_types": task_type},
        )
        assert pulled.status_code == 200
        assert pulled.json()["task_id"] == task_id

        ack = await client.post(
            f"/api/v1/tasks/{task_id}/ack",
            json={"worker_id": "pg-smoke"},
        )
        assert ack.status_code == 200

        completed = await client.get(
            "/api/v1/tasks",
            params={"status": "COMPLETED", "task_type": task_type},
        )
        assert completed.status_code == 200
        assert completed.json()["total"] == 1

        status = await client.get(f"/api/v1/tasks/{task_id}/status")
        assert status.json()["status"] == "COMPLETED"
