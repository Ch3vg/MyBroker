from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from broker.api.routes import health
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
    app.include_router(health.router, prefix="/api/v1")
    return app
