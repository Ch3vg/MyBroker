import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from broker.config import BrokerSettings
from broker.db.enums import TaskStatus
from broker.db.models import Task


class TaskRepository:
    def __init__(self, session: AsyncSession, settings: BrokerSettings) -> None:
        self._session = session
        self._settings = settings

    async def create(
        self,
        *,
        task_type: str,
        payload: dict,
        delay_seconds: int = 0,
        max_retries: int | None = None,
    ) -> Task:
        now = datetime.now(UTC)
        task = Task(
            id=str(uuid.uuid4()),
            task_type=task_type,
            payload=payload,
            status=TaskStatus.PENDING.value,
            max_retries=max_retries if max_retries is not None else self._settings.default_max_retries,
            retries=0,
            available_at=now + timedelta(seconds=delay_seconds),
            lock_until=None,
            worker_id=None,
            created_at=now,
            updated_at=now,
        )
        self._session.add(task)
        await self._session.commit()
        await self._session.refresh(task)
        return task

    async def get_by_id(self, task_id: str) -> Task | None:
        return await self._session.get(Task, task_id)
