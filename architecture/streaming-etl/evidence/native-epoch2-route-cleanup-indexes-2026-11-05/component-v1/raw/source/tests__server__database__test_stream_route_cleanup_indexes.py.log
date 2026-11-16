"""Lookup-only migrations preserve pins and both atomic DAG cleanup paths."""

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from tests.server.api.test_stream_dag_executions import execution_api as execution_api
from tests.server.api.test_stream_dag_executions import parent_body
from tests.server.database.test_migrations import _isolated_postgres_schema_changes
from tests.server.database.test_stream_route_migrations import (
    data_snapshot,
    ddl_chain,
    ordered_cleanup,
)
from tests.server.models.test_stream_dag_executions import (
    produce_and_finish_region,
    ready_deployed_region,
    retire_ready_child,
    signed_child_event,
    stop_current_children,
)

from prefect.server.database.alembic_commands import alembic_downgrade, alembic_upgrade
from prefect.server.models import stream_dag_executions as lifecycle
from prefect.server.schemas import stream_dag_executions as wire
from prefect.server.services.task_run_recorder import dispatch_stream_subtask_event
from prefect.utilities.asyncutils import run_sync_in_worker_thread

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]
PREVIOUS = {"postgresql": "14e7dbaf6034", "sqlite": "25f8ecb07145"}
INDEX_HEAD = {"postgresql": "39c32d418d9a", "sqlite": "4ad43e529eab"}
EXPECTED = {
    "stream_record": {
        "ix_stream_record__payload_record_id": ["payload_record_id"],
        "ix_stream_record__route_receipt_id": ["route_receipt_id"],
    },
    "stream_route_receipt": {
        "ix_stream_route_receipt__input_payload_record_id": ["input_payload_record_id"],
        "ix_stream_route_receipt__input_reference_record_id": [
            "input_reference_record_id"
        ],
    },
}


async def schema_shape(engine):
    async with engine.connect() as connection:
        return await connection.run_sync(
            lambda conn: {
                table: {
                    "indexes": {
                        row["name"]: row["column_names"]
                        for row in sa.inspect(conn).get_indexes(table)
                    },
                    "foreign_keys": sa.inspect(conn).get_foreign_keys(table),
                    "checks": sa.inspect(conn).get_check_constraints(table),
                }
                for table in EXPECTED
            }
        )


async def test_lookup_index_migration_preserves_routed_rows_and_constraints(
    db, database_engine, flow_run
):
    await ddl_chain(db, flow_run.id)
    before = await data_snapshot(db)
    original = await schema_shape(database_engine)
    async with _isolated_postgres_schema_changes(db, database_engine):
        try:
            await run_sync_in_worker_thread(
                alembic_downgrade, revision=PREVIOUS[db.dialect.name]
            )
            await database_engine.dispose()
            old = await schema_shape(database_engine)
            for table, indexes in EXPECTED.items():
                assert not set(indexes) & old[table]["indexes"].keys()
                assert old[table]["foreign_keys"] == original[table]["foreign_keys"]
                assert old[table]["checks"] == original[table]["checks"]
            assert await data_snapshot(db) == before
            await run_sync_in_worker_thread(
                alembic_upgrade, revision=INDEX_HEAD[db.dialect.name]
            )
            await database_engine.dispose()
            upgraded = await schema_shape(database_engine)
            for table, indexes in EXPECTED.items():
                assert {
                    name: upgraded[table]["indexes"][name] for name in indexes
                } == indexes
            assert upgraded == original
            assert await data_snapshot(db) == before
        finally:
            await run_sync_in_worker_thread(alembic_upgrade, revision="head")
            await database_engine.dispose()


@pytest.mark.parametrize("rollback", [False, True])
async def test_indexed_cleanup_preserves_immediate_pins_and_atomicity(
    db, flow_run, rollback
):
    chain = await ddl_chain(db, flow_run.id)
    before = await data_snapshot(db)
    with pytest.raises(IntegrityError):
        async with db.session_context(begin_transaction=True) as session:
            await session.execute(
                sa.delete(db.StreamRecord).where(
                    db.StreamRecord.id == chain.payloads["A"]
                )
            )
    assert await data_snapshot(db) == before

    class RollbackCleanup(Exception):
        pass

    try:
        async with db.session_context(begin_transaction=True) as session:
            await session.execute(
                sa.select(db.FlowRun.id)
                .where(db.FlowRun.id == flow_run.id)
                .with_for_update()
            )
            await ordered_cleanup(session, db)
            if rollback:
                raise RollbackCleanup
    except RollbackCleanup:
        assert rollback
    if rollback:
        assert await data_snapshot(db) == before
    else:
        snapshot = await data_snapshot(db)
        assert all(
            not snapshot[name]
            for name in (
                "stream_record",
                "stream_route_receipt",
                "stream_completion",
                "stream_consumer",
            )
        )


@pytest.mark.parametrize("control", [False, True])
async def test_indexed_normal_and_control_finalizers_postgres(
    db, session, execution_api, deployment, control
):
    plan, credentials, children = await ready_deployed_region(
        db, session, execution_api, deployment
    )
    await produce_and_finish_region(db, children, documents=8)
    if control:
        async with db.session_context(begin_transaction=True) as transaction:
            outcome = await lifecycle.fail_stream_dag(
                transaction,
                db,
                plan.id,
                wire.StreamDAGFailRequest.model_validate(
                    parent_body(plan.id, "fail", plan.graph_digest)
                ),
                credentials,
            )
            assert outcome.execution.control is not None
            control_id = outcome.execution.control.operation_id
        await stop_current_children(db, children, control_id)
    else:
        async with db.session_context(begin_transaction=True) as transaction:
            for child in children:
                completed = await dispatch_stream_subtask_event(
                    transaction,
                    db,
                    child.bootstrap.assignment.id,
                    signed_child_event(child, "COMPLETED"),
                    child.credentials,
                )
                assert completed.accepted
        for child in children:
            await retire_ready_child(db, child)
    async with db.session_context(begin_transaction=True) as transaction:
        terminal = await lifecycle.read_stream_dag_execution(
            transaction, db, plan.id, credentials
        )
        assert terminal.cleanup_complete
        assert terminal.terminal_state == ("FAILED" if control else "COMPLETED")
        assert (
            terminal.counts.physical_slots_remaining
            == terminal.counts.tags_remaining
            == 0
        )
        assert terminal.counts.stopped == 4
        for model in (
            db.StreamRecord,
            db.StreamRouteReceipt,
            db.StreamCompletion,
            db.StreamConsumer,
        ):
            assert (
                await transaction.scalar(sa.select(sa.func.count()).select_from(model))
                == 0
            )
