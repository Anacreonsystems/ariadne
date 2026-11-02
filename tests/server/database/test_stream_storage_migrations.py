import math
import uuid
from typing import cast

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from prefect.server.database.alembic_commands import (
    alembic_downgrade,
    alembic_upgrade,
)
from prefect.server.database.interface import PrefectDBInterface
from prefect.utilities.asyncutils import run_sync_in_worker_thread

pytestmark = [pytest.mark.service("database"), pytest.mark.clear_db]

STREAM_TABLES = {
    "stream_partition",
    "stream_record",
    "stream_consumer",
    "stream_completion",
}


async def _table_names(database_engine: AsyncEngine) -> set[str]:
    async with database_engine.connect() as connection:
        return set(
            await connection.run_sync(
                lambda sync_connection: sa.inspect(sync_connection).get_table_names()
            )
        )


async def test_native_stream_storage_upgrade_and_downgrade(
    database_engine: AsyncEngine,
):
    assert STREAM_TABLES <= await _table_names(database_engine)

    await database_engine.dispose()
    try:
        await run_sync_in_worker_thread(alembic_downgrade, revision="-1")
        assert STREAM_TABLES.isdisjoint(await _table_names(database_engine))

        await database_engine.dispose()
        await run_sync_in_worker_thread(alembic_upgrade)
        assert STREAM_TABLES <= await _table_names(database_engine)
    finally:
        await database_engine.dispose()
        await run_sync_in_worker_thread(alembic_upgrade)


async def test_native_stream_storage_schema_matches_orm(
    db: PrefectDBInterface, database_engine: AsyncEngine
):
    orm_tables: dict[str, sa.Table] = {}
    for model in (
        db.StreamPartition,
        db.StreamRecord,
        db.StreamConsumer,
        db.StreamCompletion,
    ):
        table = cast(sa.Table, model.__table__)
        orm_tables[table.name] = table

    async with database_engine.connect() as connection:
        reflected = await connection.run_sync(
            lambda sync_connection: _reflect_stream_tables(sync_connection, orm_tables)
        )

    for table_name, orm_table in orm_tables.items():
        actual = reflected[table_name]
        expected_columns = {
            column.name: column.nullable for column in orm_table.columns
        }
        assert actual["columns"] == expected_columns
        assert actual["unique_constraints"] == _orm_unique_constraints(orm_table)
        assert actual["check_constraints"] == _orm_check_constraints(orm_table)
        assert actual["foreign_keys"] == _orm_foreign_keys(orm_table)


async def test_native_stream_json_preserves_null_and_signed_zero(
    db: PrefectDBInterface,
    flow_run,
):
    stream_id = uuid.uuid4()
    output_stream_id = uuid.uuid4()
    record_id = uuid.uuid4()
    null_record_id = uuid.uuid4()
    consumer_id = uuid.uuid4()
    async with db.session_context(begin_transaction=True) as session:
        session.add_all(
            [
                db.StreamPartition(
                    id=stream_id,
                    flow_run_id=flow_run.id,
                    limits={},
                ),
                db.StreamPartition(
                    id=output_stream_id,
                    flow_run_id=flow_run.id,
                    limits={},
                ),
            ]
        )
        await session.flush()
        session.add_all(
            [
                db.StreamRecord(
                    id=record_id,
                    stream_id=stream_id,
                    sequence=1,
                    producer_position=1,
                    value={"json_null": None, "negative_zero": -0.0},
                    value_digest="0" * 64,
                    size_bytes=39,
                ),
                db.StreamRecord(
                    id=null_record_id,
                    stream_id=stream_id,
                    sequence=2,
                    producer_position=2,
                    value=None,
                    value_digest="1" * 64,
                    size_bytes=4,
                ),
                db.StreamConsumer(
                    id=consumer_id,
                    flow_run_id=flow_run.id,
                    input_stream_id=stream_id,
                    output_stream_id=output_stream_id,
                    owner_id=uuid.uuid4(),
                    attempt=1,
                    owner_epoch=1,
                    managed_state={"negative_zero": -0.0},
                ),
            ]
        )

    async with db.session_context() as session:
        is_sql_null = await session.scalar(
            sa.text("SELECT value IS NULL FROM stream_record WHERE id = :record_id"),
            {"record_id": str(null_record_id)},
        )
        value = await session.scalar(
            sa.select(db.StreamRecord.value).where(db.StreamRecord.id == record_id)
        )
        state = await session.scalar(
            sa.select(db.StreamConsumer.managed_state).where(
                db.StreamConsumer.id == consumer_id
            )
        )
        null_value = await session.scalar(
            sa.select(db.StreamRecord.value).where(db.StreamRecord.id == null_record_id)
        )
    engine = await db.engine()
    async with engine.connect() as connection:
        json_types = await connection.run_sync(
            lambda sync_connection: {
                ("stream_record", "value"): next(
                    column["type"]
                    for column in sa.inspect(sync_connection).get_columns(
                        "stream_record"
                    )
                    if column["name"] == "value"
                ),
                ("stream_consumer", "managed_state"): next(
                    column["type"]
                    for column in sa.inspect(sync_connection).get_columns(
                        "stream_consumer"
                    )
                    if column["name"] == "managed_state"
                ),
            }
        )

    assert is_sql_null is False or is_sql_null == 0
    assert null_value is None
    assert isinstance(value, dict)
    assert isinstance(state, dict)
    assert value["json_null"] is None
    assert math.copysign(1.0, value["negative_zero"]) == -1.0
    assert math.copysign(1.0, state["negative_zero"]) == -1.0
    assert {str(column_type).upper() for column_type in json_types.values()} == {"JSON"}


async def test_native_stream_shape_constraints_reject_null_bypasses(
    db: PrefectDBInterface,
    flow_run,
):
    input_stream_id = uuid.uuid4()
    output_stream_id = uuid.uuid4()
    invalid_output_stream_id = uuid.uuid4()
    consumer_id = uuid.uuid4()
    async with db.session_context(begin_transaction=True) as session:
        session.add_all(
            [
                db.StreamPartition(
                    id=stream_id,
                    flow_run_id=flow_run.id,
                    limits={},
                )
                for stream_id in (
                    input_stream_id,
                    output_stream_id,
                    invalid_output_stream_id,
                )
            ]
        )
        await session.flush()
        session.add(
            db.StreamConsumer(
                id=consumer_id,
                flow_run_id=flow_run.id,
                input_stream_id=input_stream_id,
                output_stream_id=output_stream_id,
                owner_id=uuid.uuid4(),
                attempt=1,
                owner_epoch=1,
            )
        )

    statements = [
        (
            "completed_partition_without_tail",
            sa.insert(db.StreamPartition).values(
                id=uuid.uuid4(),
                flow_run_id=flow_run.id,
                limits={},
                state="COMPLETED",
                tail_sequence=None,
                last_sequence=0,
                error=None,
            ),
        ),
        (
            "delivery_without_inflight_sequence",
            sa.insert(db.StreamConsumer).values(
                id=uuid.uuid4(),
                flow_run_id=flow_run.id,
                input_stream_id=input_stream_id,
                output_stream_id=invalid_output_stream_id,
                owner_id=uuid.uuid4(),
                attempt=1,
                owner_epoch=1,
                inflight_sequence=None,
                delivery_id=uuid.uuid4(),
            ),
        ),
        (
            "yield_completion_without_output",
            sa.insert(db.StreamCompletion).values(
                consumer_id=consumer_id,
                input_sequence=1,
                delivery_id=uuid.uuid4(),
                owner_epoch=1,
                kind="YIELD",
                intent_digest="0" * 64,
                output_sequence=None,
            ),
        ),
    ]
    accepted_bypasses: list[str] = []
    for name, statement in statements:
        try:
            async with db.session_context(begin_transaction=True) as session:
                await session.execute(statement)
        except IntegrityError:
            pass
        else:
            accepted_bypasses.append(name)
    assert not accepted_bypasses, (
        f"Shape constraints accepted NULL bypasses: {accepted_bypasses}"
    )


def _reflect_stream_tables(
    connection: sa.Connection, orm_tables: dict[str, sa.Table]
) -> dict[str, dict[str, object]]:
    inspector = sa.inspect(connection)
    reflected: dict[str, dict[str, object]] = {}
    for table_name in orm_tables:
        reflected[table_name] = {
            "columns": {
                column["name"]: column["nullable"]
                for column in inspector.get_columns(table_name)
            },
            "unique_constraints": {
                tuple(constraint["column_names"])
                for constraint in inspector.get_unique_constraints(table_name)
            },
            "check_constraints": {
                constraint["name"]
                for constraint in inspector.get_check_constraints(table_name)
            },
            "foreign_keys": {
                (
                    tuple(constraint["constrained_columns"]),
                    constraint["referred_table"],
                    tuple(constraint["referred_columns"]),
                    (constraint.get("options") or {}).get("ondelete"),
                )
                for constraint in inspector.get_foreign_keys(table_name)
            },
        }
    return reflected


def _orm_unique_constraints(table: sa.Table) -> set[tuple[str, ...]]:
    return {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, sa.UniqueConstraint)
    }


def _orm_check_constraints(table: sa.Table) -> set[str | None]:
    return {
        str(constraint.name) if constraint.name is not None else None
        for constraint in table.constraints
        if isinstance(constraint, sa.CheckConstraint)
    }


def _orm_foreign_keys(
    table: sa.Table,
) -> set[tuple[tuple[str, ...], str, tuple[str, ...], str | None]]:
    return {
        (
            tuple(element.parent.name for element in constraint.elements),
            constraint.referred_table.name,
            tuple(element.column.name for element in constraint.elements),
            constraint.ondelete,
        )
        for constraint in table.constraints
        if isinstance(constraint, sa.ForeignKeyConstraint)
    }
