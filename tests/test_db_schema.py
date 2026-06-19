import pytest
from sqlalchemy import inspect

from broker.db.models import Base, Task
from broker.db.schema import init_schema


@pytest.mark.asyncio
async def test_init_schema_creates_tasks_table(broker) -> None:
    await init_schema(broker.engine)

    async with broker.engine.connect() as conn:
        table_names = await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).get_table_names()
        )

    assert "tasks" in table_names


@pytest.mark.asyncio
async def test_tasks_table_has_expected_columns(broker) -> None:
    await init_schema(broker.engine)

    async with broker.engine.connect() as conn:
        columns = await conn.run_sync(
            lambda sync_conn: {
                col["name"]: col for col in inspect(sync_conn).get_columns("tasks")
            }
        )

    expected = {
        "id",
        "task_type",
        "payload",
        "status",
        "max_retries",
        "retries",
        "available_at",
        "lock_until",
        "worker_id",
        "created_at",
        "updated_at",
    }
    assert expected == set(columns.keys())


@pytest.mark.asyncio
async def test_init_schema_creates_pull_index(broker) -> None:
    await init_schema(broker.engine)

    async with broker.engine.connect() as conn:
        indexes = await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).get_indexes("tasks")
        )

    index_names = {index["name"] for index in indexes}
    assert "idx_tasks_pull" in index_names


@pytest.mark.asyncio
async def test_init_schema_is_idempotent(broker) -> None:
    await init_schema(broker.engine)
    await init_schema(broker.engine)

    async with broker.engine.connect() as conn:
        table_names = await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).get_table_names()
        )

    assert table_names.count("tasks") == 1


def test_task_model_metadata_matches_table() -> None:
    assert Task.__tablename__ == "tasks"
    column_names = {column.name for column in Base.metadata.tables["tasks"].columns}
    assert "available_at" in column_names
    assert "max_retries" in column_names
