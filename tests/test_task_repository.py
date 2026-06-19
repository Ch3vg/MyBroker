from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from broker.config import BrokerSettings
from broker.db.enums import TaskStatus
from broker.repository.tasks import TaskRepository


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@pytest.fixture
def settings() -> BrokerSettings:
    return BrokerSettings(default_max_retries=3)


@pytest.fixture
def repository(db_session: AsyncSession, settings: BrokerSettings) -> TaskRepository:
    return TaskRepository(db_session, settings)


@pytest.mark.asyncio
async def test_create_task_with_defaults(repository: TaskRepository) -> None:
    before = datetime.now(UTC)
    task = await repository.create(
        task_type="config.regenerate",
        payload={"config_id": "abc"},
    )
    after = datetime.now(UTC)
    available_at = _as_utc(task.available_at)
    created_at = _as_utc(task.created_at)

    assert task.id
    assert task.task_type == "config.regenerate"
    assert task.payload == {"config_id": "abc"}
    assert task.status == TaskStatus.PENDING.value
    assert task.max_retries == 3
    assert task.retries == 0
    assert task.lock_until is None
    assert task.worker_id is None
    assert before <= available_at <= after + timedelta(seconds=1)
    assert before <= created_at <= after + timedelta(seconds=1)
    assert created_at == _as_utc(task.updated_at)


@pytest.mark.asyncio
async def test_create_task_with_delay_and_custom_max_retries(repository: TaskRepository) -> None:
    before = datetime.now(UTC)
    task = await repository.create(
        task_type="email.send",
        payload={"to": "user@example.com"},
        delay_seconds=30,
        max_retries=5,
    )
    after = datetime.now(UTC)
    available_at = _as_utc(task.available_at)

    assert task.max_retries == 5
    assert before + timedelta(seconds=29) <= available_at <= after + timedelta(seconds=31)


@pytest.mark.asyncio
async def test_get_by_id_returns_task(repository: TaskRepository) -> None:
    created = await repository.create(task_type="job", payload={"x": 1})
    loaded = await repository.get_by_id(created.id)
    assert loaded is not None
    assert loaded.id == created.id


@pytest.mark.asyncio
async def test_get_by_id_returns_none_for_missing_task(repository: TaskRepository) -> None:
    loaded = await repository.get_by_id("missing-id")
    assert loaded is None
