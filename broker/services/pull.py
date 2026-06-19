import asyncio
import time
from collections.abc import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from broker.config import BrokerSettings
from broker.db.models import Task
from broker.repository.tasks import TaskRepository

SleepFn = Callable[[float], Awaitable[None]]


async def pull_with_polling(
    session_factory: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    *,
    worker_id: str,
    task_types: list[str] | None,
    timeout_seconds: int,
    sleep: SleepFn = asyncio.sleep,
) -> Task | None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        async with session_factory() as session:
            repository = TaskRepository(session, settings)
            task = await repository.pull_once(worker_id=worker_id, task_types=task_types)
            if task is not None:
                return task

        if time.monotonic() >= deadline:
            return None

        remaining = deadline - time.monotonic()
        await sleep(min(settings.pull_interval_seconds, remaining))
