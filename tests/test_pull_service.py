import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from broker.config import BrokerSettings
from broker.db.schema import init_schema
from broker.repository.tasks import TaskRepository
from broker.services.pull import pull_with_polling

@pytest.fixture
def fast_settings() -> BrokerSettings:
    return BrokerSettings(
        default_pull_timeout_seconds=5,
        pull_interval_seconds=1,
    )


@pytest.mark.asyncio
async def test_pull_with_polling_returns_none_on_timeout(broker, fast_settings: BrokerSettings) -> None:
    await init_schema(broker.engine)
    session_factory = async_sessionmaker(broker.engine, expire_on_commit=False)
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    task = await pull_with_polling(
        session_factory,
        fast_settings,
        worker_id="w1",
        task_types=None,
        timeout_seconds=0,
        sleep=fake_sleep,
    )
    assert task is None
    assert sleeps == []


@pytest.mark.asyncio
async def test_pull_with_polling_waits_until_task_available(broker, fast_settings: BrokerSettings) -> None:
    await init_schema(broker.engine)
    session_factory = async_sessionmaker(broker.engine, expire_on_commit=False)
    iteration = {"count": 0}

    async def fake_sleep(_seconds: float) -> None:
        iteration["count"] += 1
        if iteration["count"] == 1:
            async with session_factory() as session:
                repository = TaskRepository(session, fast_settings)
                await repository.create(task_type="delayed", payload={"ready": True})

    task = await pull_with_polling(
        session_factory,
        fast_settings,
        worker_id="w1",
        task_types=None,
        timeout_seconds=3,
        sleep=fake_sleep,
    )
    assert task is not None
    assert task.task_type == "delayed"
    assert iteration["count"] >= 1
