from fastapi import APIRouter, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from broker.metrics import refresh_status_gauges
from broker.repository.tasks import TaskRepository

router = APIRouter(tags=["metrics"])


@router.get("/metrics")
async def get_metrics(request: Request) -> Response:
    broker = request.app.state.broker
    session_factory = request.app.state.session_factory
    async with session_factory() as session:
        repository = TaskRepository(session, broker.settings)
        counts = await repository.count_by_status_and_type()
    refresh_status_gauges(counts)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
