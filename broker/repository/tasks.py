import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from broker.config import BrokerSettings
from broker.db.enums import TaskStatus
from broker.db.models import Task
from broker.repository.errors import StaleTaskError, TaskNotFoundError


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
        now = datetime.now(timezone.utc)
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

    async def count_by_status_and_type(self) -> list[tuple[str, str, int]]:
        stmt = (
            select(Task.status, Task.task_type, func.count())
            .group_by(Task.status, Task.task_type)
        )
        result = await self._session.execute(stmt)
        return [(status, task_type, count) for status, task_type, count in result.all()]

    async def list_tasks(
        self,
        *,
        status: str | None = None,
        task_type: str | None = None,
        limit: int,
        offset: int,
    ) -> tuple[list[Task], int]:
        filters = self._list_filters(status=status, task_type=task_type)
        count_stmt = select(func.count()).select_from(Task)
        list_stmt = select(Task).order_by(Task.created_at)
        if filters:
            count_stmt = count_stmt.where(*filters)
            list_stmt = list_stmt.where(*filters)
        total = int(await self._session.scalar(count_stmt) or 0)
        result = await self._session.execute(list_stmt.limit(limit).offset(offset))
        return list(result.scalars().all()), total

    def _list_filters(
        self,
        *,
        status: str | None,
        task_type: str | None,
    ) -> list:
        filters = []
        if status is not None:
            filters.append(Task.status == status)
        if task_type is not None:
            filters.append(Task.task_type == task_type)
        return filters

    async def pull_once(
        self,
        *,
        worker_id: str,
        task_types: list[str] | None = None,
    ) -> Task | None:
        now = datetime.now(timezone.utc)
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
            .where(Task.id == candidate_id, *conditions)
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

    async def _require_processing_for_worker(self, task_id: str, worker_id: str) -> Task:
        task = await self.get_by_id(task_id)
        if task is None:
            raise TaskNotFoundError
        if task.status != TaskStatus.PROCESSING.value or task.worker_id != worker_id:
            raise StaleTaskError
        return task

    async def heartbeat(self, task_id: str, worker_id: str) -> Task:
        task = await self._require_processing_for_worker(task_id, worker_id)
        now = datetime.now(timezone.utc)
        task.lock_until = now + timedelta(seconds=self._settings.default_lock_ttl_seconds)
        task.updated_at = now
        await self._session.commit()
        await self._session.refresh(task)
        return task

    async def ack(self, task_id: str, worker_id: str) -> Task:
        task = await self._require_processing_for_worker(task_id, worker_id)
        now = datetime.now(timezone.utc)
        task.status = TaskStatus.COMPLETED.value
        task.lock_until = None
        task.worker_id = None
        task.updated_at = now
        await self._session.commit()
        await self._session.refresh(task)
        return task

    async def nack(self, task_id: str, worker_id: str) -> Task:
        task = await self._require_processing_for_worker(task_id, worker_id)
        now = datetime.now(timezone.utc)
        task.retries += 1
        task.worker_id = None
        task.lock_until = None
        task.updated_at = now
        if task.retries < task.max_retries:
            task.status = TaskStatus.PENDING.value
            task.available_at = now + timedelta(seconds=self._settings.retry_delay_seconds)
        else:
            task.status = TaskStatus.DEAD.value
        await self._session.commit()
        await self._session.refresh(task)
        return task
