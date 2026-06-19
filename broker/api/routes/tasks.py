from fastapi import APIRouter, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from broker.api.deps import BrokerDep, SessionDep
from broker.api.schemas.tasks import PublishTaskRequest, PublishTaskResponse, TaskStatusResponse
from broker.repository.tasks import TaskRepository

router = APIRouter(tags=["tasks"])


def _repository(broker: BrokerDep, session: AsyncSession) -> TaskRepository:
    return TaskRepository(session, broker.settings)


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
    return PublishTaskResponse(task_id=task.id)


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
