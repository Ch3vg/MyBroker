import time

from fastapi import APIRouter, HTTPException, Query, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from broker.api.deps import BrokerDep, SessionDep
from broker.api.query_params import parse_task_types
from broker.api.schemas.tasks import (
    NackRequest,
    PublishTaskRequest,
    PublishTaskResponse,
    PullTaskResponse,
    TaskListResponse,
    TaskStatusResponse,
    WorkerActionRequest,
)
from broker.db.enums import TaskStatus
from broker.metrics import (
    observe_pull_duration,
    record_completed,
    record_nacked,
    record_pull_empty,
    record_published,
)
from broker.repository.errors import StaleTaskError, TaskNotFoundError
from broker.repository.tasks import TaskRepository
from broker.services.pull import pull_with_polling

router = APIRouter(tags=["tasks"])

STALE_TASK_DETAIL = "STALE_TASK"


def _repository(broker: BrokerDep, session: AsyncSession) -> TaskRepository:
    return TaskRepository(session, broker.settings)


def _lifecycle_http_errors(exc: TaskNotFoundError | StaleTaskError) -> None:
    if isinstance(exc, TaskNotFoundError):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found") from exc
    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=STALE_TASK_DETAIL) from exc


@router.post("/tasks", status_code=status.HTTP_201_CREATED, response_model=PublishTaskResponse)
async def publish_task(
    body: PublishTaskRequest,
    session: SessionDep,
    broker: BrokerDep,
) -> PublishTaskResponse:
    repository = _repository(broker, session)
    task = await repository.create(
        task_type=body.task_type,
        payload=body.payload,
        delay_seconds=body.delay_seconds,
        max_retries=body.max_retries,
    )
    record_published(task.task_type)
    return PublishTaskResponse(task_id=task.id)


@router.get("/tasks", response_model=TaskListResponse)
async def list_tasks(
    session: SessionDep,
    broker: BrokerDep,
    task_status: str | None = Query(None, alias="status"),
    task_type: str | None = Query(None, min_length=1),
    limit: int | None = Query(None, ge=1),
    offset: int = Query(0, ge=0),
) -> TaskListResponse:
    if task_status is not None:
        try:
            TaskStatus(task_status)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid status",
            ) from exc
    page_limit = (
        broker.settings.list_default_limit
        if limit is None
        else min(limit, broker.settings.list_max_limit)
    )
    repository = _repository(broker, session)
    tasks, total = await repository.list_tasks(
        status=task_status,
        task_type=task_type,
        limit=page_limit,
        offset=offset,
    )
    return TaskListResponse(
        items=tasks,
        total=total,
        limit=page_limit,
        offset=offset,
    )


@router.get("/tasks/pull", response_model=PullTaskResponse)
async def pull_task(
    request: Request,
    broker: BrokerDep,
    worker_id: str = Query(min_length=1),
    task_types: list[str] | None = Query(None),
    timeout: int | None = Query(None, ge=0),
) -> PullTaskResponse | Response:
    timeout_seconds = (
        broker.settings.default_pull_timeout_seconds
        if timeout is None
        else min(timeout, broker.settings.max_pull_timeout_seconds)
    )
    started = time.perf_counter()
    task = await pull_with_polling(
        request.app.state.session_factory,
        broker.settings,
        worker_id=worker_id,
        task_types=parse_task_types(task_types),
        timeout_seconds=timeout_seconds,
    )
    observe_pull_duration(time.perf_counter() - started)
    if task is None:
        record_pull_empty()
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return PullTaskResponse(
        task_id=task.id,
        task_type=task.task_type,
        payload=task.payload,
        lock_ttl_seconds=broker.settings.default_lock_ttl_seconds,
    )


@router.post("/tasks/{task_id}/heartbeat", status_code=status.HTTP_200_OK)
async def heartbeat_task(
    task_id: str,
    body: WorkerActionRequest,
    session: SessionDep,
    broker: BrokerDep,
) -> None:
    repository = _repository(broker, session)
    try:
        await repository.heartbeat(task_id, body.worker_id)
    except (TaskNotFoundError, StaleTaskError) as exc:
        _lifecycle_http_errors(exc)


@router.post("/tasks/{task_id}/ack", status_code=status.HTTP_200_OK)
async def ack_task(
    task_id: str,
    body: WorkerActionRequest,
    session: SessionDep,
    broker: BrokerDep,
) -> None:
    repository = _repository(broker, session)
    try:
        task = await repository.ack(task_id, body.worker_id)
    except (TaskNotFoundError, StaleTaskError) as exc:
        _lifecycle_http_errors(exc)
    else:
        record_completed(task.task_type)


@router.post("/tasks/{task_id}/nack", status_code=status.HTTP_200_OK)
async def nack_task(
    task_id: str,
    body: NackRequest,
    session: SessionDep,
    broker: BrokerDep,
) -> None:
    repository = _repository(broker, session)
    try:
        task = await repository.nack(task_id, body.worker_id)
    except (TaskNotFoundError, StaleTaskError) as exc:
        _lifecycle_http_errors(exc)
    else:
        record_nacked(task.task_type)


@router.get("/tasks/{task_id}/status", response_model=TaskStatusResponse)
async def get_task_status(
    task_id: str,
    session: SessionDep,
    broker: BrokerDep,
) -> TaskStatusResponse:
    repository = _repository(broker, session)
    task = await repository.get_by_id(task_id)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    return TaskStatusResponse.model_validate(task)
