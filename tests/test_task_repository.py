from datetime import UTC, datetime, timedelta

import asyncio
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from broker.config import BrokerSettings
from broker.db.enums import TaskStatus
from broker.db.schema import init_schema
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


@pytest.mark.asyncio
async def test_pull_once_returns_pending_task(repository: TaskRepository) -> None:
    created = await repository.create(task_type="job", payload={"n": 1})
    pulled = await repository.pull_once(worker_id="worker-1")
    assert pulled is not None
    assert pulled.id == created.id
    assert pulled.status == TaskStatus.PROCESSING.value
    assert pulled.worker_id == "worker-1"
    assert pulled.lock_until is not None


@pytest.mark.asyncio
async def test_pull_once_returns_none_when_no_tasks(repository: TaskRepository) -> None:
    assert await repository.pull_once(worker_id="worker-1") is None


@pytest.mark.asyncio
async def test_pull_once_skips_future_available_at(repository: TaskRepository) -> None:
    await repository.create(task_type="job", payload={}, delay_seconds=3600)
    assert await repository.pull_once(worker_id="worker-1") is None


@pytest.mark.asyncio
async def test_pull_once_filters_by_task_types(repository: TaskRepository) -> None:
    await repository.create(task_type="type.a", payload={})
    task_b = await repository.create(task_type="type.b", payload={})
    pulled = await repository.pull_once(worker_id="worker-1", task_types=["type.b"])
    assert pulled is not None
    assert pulled.id == task_b.id


@pytest.mark.asyncio
async def test_pull_once_reclaims_expired_processing_task(repository: TaskRepository) -> None:
    task = await repository.create(task_type="job", payload={})
    pulled = await repository.pull_once(worker_id="worker-1")
    assert pulled is not None

    pulled.lock_until = datetime.now(UTC) - timedelta(seconds=1)
    pulled.status = TaskStatus.PROCESSING.value
    await repository._session.commit()

    reclaimed = await repository.pull_once(worker_id="worker-2")
    assert reclaimed is not None
    assert reclaimed.id == task.id
    assert reclaimed.worker_id == "worker-2"
    assert reclaimed.status == TaskStatus.PROCESSING.value


@pytest.mark.asyncio
async def test_concurrent_pull_assigns_single_task(broker, settings: BrokerSettings) -> None:
    await init_schema(broker.engine)
    session_factory = async_sessionmaker(broker.engine, expire_on_commit=False)

    async with session_factory() as session:
        repository = TaskRepository(session, settings)
        await repository.create(task_type="job", payload={})

    async def pull(worker_id: str) -> str | None:
        async with session_factory() as session:
            repository = TaskRepository(session, settings)
            task = await repository.pull_once(worker_id=worker_id)
            return task.id if task else None

    results = await asyncio.gather(pull("w1"), pull("w2"))
    pulled_ids = [task_id for task_id in results if task_id is not None]
    assert len(pulled_ids) == 1
