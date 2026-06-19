import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from broker.config import BrokerSettings
from broker.db.enums import TaskStatus
from broker.db.models import Task


class TaskRepository:
    def __init__(self, session: AsyncSession, settings: BrokerSettings) -> None:
        self._session = session
        self._settings = settings

    def _pull_conditions(
        self,
        now: datetime,
        task_types: list[str] | None,
    ) -> list:
        pullable = or_(
            Task.status == TaskStatus.PENDING.value,
            and_(
                Task.status == TaskStatus.PROCESSING.value,
                Task.lock_until.is_not(None),
                Task.lock_until <= now,
            ),
        )
        conditions = [pullable, Task.available_at <= now]
        if task_types:
            conditions.append(Task.task_type.in_(task_types))
        return conditions

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

    async def pull_once(
        self,
        *,
        worker_id: str,
        task_types: list[str] | None = None,
    ) -> Task | None:
        now = datetime.now(UTC)
        lock_ttl = timedelta(seconds=self._settings.default_lock_ttl_seconds)
        conditions = self._pull_conditions(now, task_types)
        candidate_id = (
            select(Task.id)
            .where(*conditions)
            .order_by(Task.created_at)
            .limit(1)
            .scalar_subquery()
        )
        stmt = (
            update(Task)
            .where(Task.id == candidate_id)
            .values(
                status=TaskStatus.PROCESSING.value,
                worker_id=worker_id,
                lock_until=now + lock_ttl,
                updated_at=now,
            )
            .returning(Task)
        )
        result = await self._session.execute(stmt)
        task = result.scalar_one_or_none()
        if task is None:
            await self._session.rollback()
            return None
        await self._session.commit()
        await self._session.refresh(task)
        return task
