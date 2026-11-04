import math
import uuid
from typing import cast

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from prefect.server.database import orm_models
from prefect.server.database.alembic_commands import (
    alembic_downgrade,
    alembic_upgrade,
)
from prefect.server.database.interface import PrefectDBInterface
from prefect.types._datetime import now
from prefect.utilities.asyncutils import run_sync_in_worker_thread

pytestmark = [pytest.mark.service("database"), pytest.mark.clear_db]

STREAM_TABLES = {
    "stream_partition",
    "stream_record",
    "stream_consumer",
    "stream_completion",
    "stream_submission",
    "stream_execution_authority",
    "stream_execution_attempt",
    "stream_finite_submission",
    "stream_finite_receipt",
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

    async def execution_attempt_columns() -> set[str]:
        async with database_engine.connect() as connection:
            return set(
                await connection.run_sync(
                    lambda sync_connection: {
                        column["name"]
                        for column in sa.inspect(sync_connection).get_columns(
                            "stream_execution_attempt"
                        )
                    }
                )
            )

    abort_columns = {
        "abort_operation_id",
        "abort_request_digest",
        "abort_outcome",
        "abort_outcome_digest",
    }
    finite_columns = {
        "finite_prefix_manifest_digest",
        "finite_prefix_task_run_id",
        "finite_prefix_sealed_at",
    }
    assert abort_columns <= await execution_attempt_columns()

    async def execution_authority_columns() -> set[str]:
        async with database_engine.connect() as connection:
            return set(
                await connection.run_sync(
                    lambda sync_connection: {
                        column["name"]
                        for column in sa.inspect(sync_connection).get_columns(
                            "stream_execution_authority"
                        )
                    }
                )
            )

    assert finite_columns <= await execution_authority_columns()

    await database_engine.dispose()
    try:
        await run_sync_in_worker_thread(alembic_downgrade, revision="-1")
        assert {
            "stream_finite_submission",
            "stream_finite_receipt",
        }.isdisjoint(await _table_names(database_engine))
        assert (
            STREAM_TABLES - {"stream_finite_submission", "stream_finite_receipt"}
        ) <= await _table_names(database_engine)
        assert finite_columns.isdisjoint(await execution_authority_columns())
        assert abort_columns <= await execution_attempt_columns()

        await database_engine.dispose()
        await run_sync_in_worker_thread(alembic_downgrade, revision="-1")
        assert (
            STREAM_TABLES - {"stream_finite_submission", "stream_finite_receipt"}
        ) <= await _table_names(database_engine)
        assert abort_columns.isdisjoint(await execution_attempt_columns())

        await database_engine.dispose()
        await run_sync_in_worker_thread(alembic_downgrade, revision="-1")
        assert {
            "stream_execution_authority",
            "stream_execution_attempt",
        }.isdisjoint(await _table_names(database_engine))
        assert (
            STREAM_TABLES
            - {
                "stream_finite_submission",
                "stream_finite_receipt",
                "stream_execution_authority",
                "stream_execution_attempt",
            }
        ) <= await _table_names(database_engine)

        await database_engine.dispose()
        await run_sync_in_worker_thread(alembic_downgrade, revision="-1")
        assert "stream_submission" not in await _table_names(database_engine)
        assert (
            STREAM_TABLES
            - {
                "stream_finite_submission",
                "stream_finite_receipt",
                "stream_submission",
                "stream_execution_authority",
                "stream_execution_attempt",
            }
        ) <= await _table_names(database_engine)

        await database_engine.dispose()
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
        db.StreamSubmission,
        db.StreamExecutionAuthority,
        db.StreamExecutionAttempt,
        db.StreamFiniteSubmission,
        db.StreamFiniteReceipt,
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


@pytest.mark.parametrize(
    "mutation",
    [
        {"manifest_set_digest": "0" * 64},
        {
            "manifest_set_digest": "0" * 64,
            "manifest_count": 1,
            "manifest_sealed_at": now("UTC"),
        },
        {
            "manifest_count": 1,
            "manifest_sealed_at": now("UTC"),
            "manifest_seal_operation_id": uuid.uuid4(),
            "manifest_seal_request_digest": "1" * 64,
        },
        {
            "manifest_set_digest": "0" * 64,
            "manifest_sealed_at": now("UTC"),
            "manifest_seal_operation_id": uuid.uuid4(),
            "manifest_seal_request_digest": "1" * 64,
        },
        {
            "manifest_set_digest": "0" * 64,
            "manifest_count": 1,
            "manifest_sealed_at": now("UTC"),
            "manifest_seal_operation_id": uuid.uuid4(),
        },
        {"generation": 65},
        {"prepared_generation": -1},
        {"prepared_generation": 2},
        {"finite_prefix_manifest_digest": "0" * 64},
        {
            "finite_prefix_manifest_digest": "0" * 64,
            "finite_prefix_task_run_id": uuid.uuid4(),
        },
        {
            "finite_prefix_task_run_id": uuid.uuid4(),
            "finite_prefix_sealed_at": now("UTC"),
        },
    ],
)
async def test_stream_execution_authority_schema_rejects_partial_seal_and_horizon(
    db: PrefectDBInterface,
    session,
    flow_run,
    mutation: dict[str, object],
):
    values: dict[str, object] = {
        "flow_run_id": flow_run.id,
        "generation": 1,
        "prepared_generation": 0,
        "holder_id": uuid.uuid4(),
        "holder_capability_digest": "0" * 64,
        "holder_event_public_key": "1" * 64,
        "event_key_id": "2" * 64,
        "supervisor_id": uuid.uuid4(),
        "supervisor_capability_digest": "3" * 64,
        "state": "CLAIMED",
        "flow_run_run_count": 0,
    }
    values.update(mutation)
    session.add(db.StreamExecutionAuthority(**values))
    with pytest.raises(IntegrityError):
        await session.flush()


@pytest.mark.parametrize(
    "mutation",
    [
        {"generation": 65},
        {"previous_generation": 1},
        {"kind": "TAKEOVER"},
        {"state": "FORGED"},
        {"recovery_operation_id": uuid.uuid4()},
        {
            "recovery_operation_id": uuid.uuid4(),
            "recovery_outcome": {"state": "prepared"},
            "recovery_outcome_digest": "4" * 64,
        },
        {
            "recovery_operation_id": uuid.uuid4(),
            "recovery_request_digest": "4" * 64,
            "recovery_outcome": {"state": "prepared"},
        },
        {"abort_operation_id": uuid.uuid4()},
        {
            "abort_operation_id": uuid.uuid4(),
            "abort_outcome": {"state": "TERMINAL"},
            "abort_outcome_digest": "5" * 64,
        },
        {
            "abort_operation_id": uuid.uuid4(),
            "abort_request_digest": "5" * 64,
            "abort_outcome": {"state": "TERMINAL"},
        },
        {
            "abort_operation_id": uuid.uuid4(),
            "abort_request_digest": "short",
            "abort_outcome": {"state": "TERMINAL"},
            "abort_outcome_digest": "5" * 64,
        },
        {
            "abort_operation_id": uuid.uuid4(),
            "abort_request_digest": "5" * 64,
            "abort_outcome": {"state": "TERMINAL"},
            "abort_outcome_digest": "short",
        },
    ],
)
async def test_stream_execution_attempt_schema_rejects_invalid_witness_shapes(
    db: PrefectDBInterface,
    session,
    flow_run,
    mutation: dict[str, object],
):
    values: dict[str, object] = {
        "flow_run_id": flow_run.id,
        "generation": 1,
        "operation_id": uuid.uuid4(),
        "kind": "CLAIM",
        "request_digest": "0" * 64,
        "outcome_digest": "1" * 64,
        "holder_id": uuid.uuid4(),
        "holder_event_public_key": "2" * 64,
        "event_key_id": "3" * 64,
        "state": "CLAIMED",
        "flow_run_run_count": 0,
    }
    values.update(mutation)
    session.add(db.StreamExecutionAttempt(**values))
    with pytest.raises(IntegrityError):
        await session.flush()


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        ({"canonical_size": 0}, "canonical_size_positive"),
        ({"request_digest": "short"}, "digest_lengths_valid"),
        ({"protocol": "yield-v2"}, "topology_shape_valid"),
        ({"state": "RELEASED"}, "release_shape_valid"),
    ],
)
async def test_stream_submission_schema_rejects_invalid_authority_shapes(
    db: PrefectDBInterface,
    session,
    flow_run,
    task_run,
    mutation: dict[str, object],
    match: str,
):
    stream_id = uuid.uuid4()
    session.add(
        db.StreamPartition(
            id=stream_id,
            flow_run_id=flow_run.id,
            producer_kind="SOURCE",
            producer_task_run_id=task_run.id,
            limits={},
        )
    )
    await session.flush()
    values: dict[str, object] = {
        "flow_run_id": flow_run.id,
        "submission_key": "source.documents",
        "request_digest": "0" * 64,
        "manifest_digest": "1" * 64,
        "canonical_size": 1,
        "parameters": {"negative_zero": -0.0},
        "parameters_digest": "2" * 64,
        "role": "SOURCE",
        "protocol": "source-v1",
        "task_run_id": task_run.id,
        "stream_id": stream_id,
        "task_name": "documents",
        "task_key": "documents-key",
        "task_version": "v1",
        "flow_version": "v1",
        "dynamic_key": "stream-documents",
        "limits": {},
        "task_policy": {"retries": 1},
        "tags": [],
        "labels": {},
    }
    values.update(mutation)
    session.add(db.StreamSubmission(**values))
    with pytest.raises(IntegrityError, match=match):
        await session.flush()


async def test_stream_submission_parameters_preserve_signed_zero(
    db: PrefectDBInterface,
    flow_run,
    task_run,
):
    stream_id = uuid.uuid4()
    async with db.session_context(begin_transaction=True) as session:
        session.add(
            db.StreamPartition(
                id=stream_id,
                flow_run_id=flow_run.id,
                producer_kind="SOURCE",
                producer_task_run_id=task_run.id,
                limits={},
            )
        )
        await session.flush()
        session.add(
            db.StreamSubmission(
                flow_run_id=flow_run.id,
                submission_key="source.signed-zero",
                request_digest="0" * 64,
                manifest_digest="1" * 64,
                canonical_size=1,
                parameters={"negative_zero": -0.0},
                parameters_digest="2" * 64,
                role="SOURCE",
                protocol="source-v1",
                task_run_id=task_run.id,
                stream_id=stream_id,
                task_name="signed-zero",
                task_key="signed-zero-key",
                task_version="v1",
                flow_version="v1",
                dynamic_key="stream-signed-zero",
                limits={},
                task_policy={"retries": 1},
                tags=[],
                labels={},
            )
        )
    async with db.session_context() as session:
        parameters = await session.scalar(
            sa.select(db.StreamSubmission.parameters).where(
                db.StreamSubmission.stream_id == stream_id
            )
        )
    assert math.copysign(1.0, parameters["negative_zero"]) == -1.0


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        ({"phase": "MIDDLE"}, "phase_valid"),
        ({"canonical_size": 0}, "canonical_size_positive"),
        ({"artifact_digest": "short"}, "digest_lengths_valid"),
        ({"state": "RELEASED"}, "release_shape_valid"),
    ],
)
async def test_finite_submission_schema_rejects_invalid_shapes(
    db: PrefectDBInterface,
    session: AsyncSession,
    flow_run: orm_models.FlowRun,
    task_run: orm_models.TaskRun,
    mutation: dict[str, object],
    match: str,
):
    values: dict[str, object] = {
        "flow_run_id": flow_run.id,
        "phase": "PREFIX",
        "submission_key": "finite.prefix",
        "request_digest": "0" * 64,
        "manifest_digest": "1" * 64,
        "canonical_size": 1,
        "parameters": {"negative_zero": -0.0},
        "parameters_digest": "2" * 64,
        "task_run_id": task_run.id,
        "task_name": "prefix",
        "task_key": "prefix-key",
        "task_version": "v1",
        "flow_version": "v1",
        "artifact_digest": "3" * 64,
        "dynamic_key": "finite-prefix",
        "task_policy": {"retries": 1},
        "tags": [],
        "labels": {},
    }
    values.update(mutation)
    session.add(db.StreamFiniteSubmission(**values))
    with pytest.raises(IntegrityError, match=match):
        await session.flush()


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        ({"run_count": 0}, "run_count_positive"),
        ({"result_size": 0}, "result_size_positive"),
        ({"request_digest": "short"}, "digest_lengths_valid"),
        ({"outcome_digest": "short"}, "digest_lengths_valid"),
        ({"result": sa.null()}, "(?i)not.?null"),
    ],
)
async def test_finite_receipt_schema_rejects_invalid_shapes(
    db: PrefectDBInterface,
    session: AsyncSession,
    flow_run: orm_models.FlowRun,
    task_run: orm_models.TaskRun,
    mutation: dict[str, object],
    match: str,
):
    manifest = db.StreamFiniteSubmission(
        flow_run_id=flow_run.id,
        phase="PREFIX",
        submission_key="finite.prefix",
        request_digest="0" * 64,
        manifest_digest="1" * 64,
        canonical_size=1,
        parameters={},
        parameters_digest="2" * 64,
        task_run_id=task_run.id,
        task_name="prefix",
        task_key="prefix-key",
        task_version="v1",
        flow_version="v1",
        artifact_digest="3" * 64,
        dynamic_key="finite-prefix",
        task_policy={"retries": 1},
        tags=[],
        labels={},
    )
    session.add(manifest)
    await session.flush()
    values: dict[str, object] = {
        "finite_submission_id": manifest.id,
        "task_run_id": task_run.id,
        "operation_id": uuid.uuid4(),
        "request_digest": "4" * 64,
        "outcome_digest": "5" * 64,
        "run_count": 1,
        "result": None,
        "result_digest": "6" * 64,
        "result_size": 4,
    }
    values.update(mutation)
    session.add(db.StreamFiniteReceipt(**values))
    with pytest.raises(IntegrityError, match=match):
        await session.flush()


async def test_finite_json_preserves_null_and_signed_zero(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    task_run: orm_models.TaskRun,
):
    async with db.session_context(begin_transaction=True) as session:
        manifest = db.StreamFiniteSubmission(
            flow_run_id=flow_run.id,
            phase="PREFIX",
            submission_key="finite.json",
            request_digest="0" * 64,
            manifest_digest="1" * 64,
            canonical_size=1,
            parameters={"negative_zero": -0.0},
            parameters_digest="2" * 64,
            task_run_id=task_run.id,
            task_name="prefix",
            task_key="prefix-key",
            task_version="v1",
            flow_version="v1",
            artifact_digest="3" * 64,
            dynamic_key="finite-prefix",
            task_policy={"retries": 1},
            tags=[],
            labels={},
        )
        session.add(manifest)
        await session.flush()
        session.add(
            db.StreamFiniteReceipt(
                finite_submission_id=manifest.id,
                task_run_id=task_run.id,
                operation_id=uuid.uuid4(),
                request_digest="4" * 64,
                outcome_digest="5" * 64,
                run_count=1,
                result=None,
                result_digest="6" * 64,
                result_size=4,
            )
        )

    async with db.session_context() as session:
        parameters = await session.scalar(
            sa.select(db.StreamFiniteSubmission.parameters).where(
                db.StreamFiniteSubmission.task_run_id == task_run.id
            )
        )
        result_is_sql_null = await session.scalar(
            sa.text(
                "SELECT result IS NULL FROM stream_finite_receipt "
                "WHERE task_run_id = :task_run_id"
            ),
            {"task_run_id": str(task_run.id)},
        )
        result = await session.scalar(
            sa.select(db.StreamFiniteReceipt.result).where(
                db.StreamFiniteReceipt.task_run_id == task_run.id
            )
        )
    assert math.copysign(1.0, parameters["negative_zero"]) == -1.0
    assert result_is_sql_null is False or result_is_sql_null == 0
    assert result is None


async def test_stream_submission_schema_has_one_source_root_index(
    database_engine: AsyncEngine,
):
    async with database_engine.connect() as connection:
        indexes = await connection.run_sync(
            lambda sync_connection: sa.inspect(sync_connection).get_indexes(
                "stream_submission"
            )
        )
    index = next(
        item
        for item in indexes
        if item["name"] == "uq_stream_submission__one_source_per_flow"
    )
    assert bool(index["unique"]) is True
    assert index["column_names"] == ["flow_run_id"]


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
