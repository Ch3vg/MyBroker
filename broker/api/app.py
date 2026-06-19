from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from broker.api.auth import ApiKeyMiddleware
from broker.api.deps import init_app_state
from broker.api.routes import health, metrics, tasks
from broker.db.schema import init_schema


def create_app(broker: "Broker") -> FastAPI:
    from broker.broker import Broker

    assert isinstance(broker, Broker)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await init_schema(broker.engine)
        yield
        await broker.engine.dispose()

    app = FastAPI(title="Task Broker", lifespan=lifespan)
    init_app_state(app, broker)
    if broker.settings.api_key is not None:
        app.add_middleware(ApiKeyMiddleware, api_key=broker.settings.api_key)
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(metrics.router, prefix="/api/v1")
    app.include_router(tasks.router, prefix="/api/v1")
    return app
