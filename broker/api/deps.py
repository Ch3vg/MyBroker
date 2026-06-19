from collections.abc import AsyncIterator
from typing import Annotated, Any

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from broker.db.session import create_session_factory


def get_broker(request: Request) -> Any:
    return request.app.state.broker


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    session_factory = request.app.state.session_factory
    async with session_factory() as session:
        yield session


def init_app_state(app, broker: Any) -> None:
    app.state.broker = broker
    app.state.session_factory = create_session_factory(broker.engine)


BrokerDep = Annotated[Any, Depends(get_broker)]
SessionDep = Annotated[AsyncSession, Depends(get_session)]
