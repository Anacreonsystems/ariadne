"""Real paired DAG migrations preserve P2 history and enforce relational scope."""

from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from tests.server.database.test_migrations import _isolated_postgres_schema_changes
from tests.server.models.test_stream_dags import dag_request

from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.database.alembic_commands import alembic_downgrade, alembic_upgrade
from prefect.server.models import stream_dags
from prefect.types._datetime import now
from prefect.utilities.asyncutils import run_sync_in_worker_thread

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


async def test_dag_topology_roundtrip_preserves_legacy_partition_and_capability_shape(
    db: PrefectDBInterface, database_engine: AsyncEngine, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        stream = db.StreamPartition(flow_run_id=flow_run.id, limits={})
        session.add(stream)
        await session.flush()
        stream_id = stream.id
    previous = "b48f2e6a7c31" if db.dialect.name == "postgresql" else "e59a3f7b8d42"
    new_tables = {
        "stream_graph_port",
        "stream_graph_edge",
        "stream_graph_edge_partition",
        "stream_physical_slot_allocation",
    }
    async with _isolated_postgres_schema_changes(db, database_engine):
        await database_engine.dispose()
        try:
            await run_sync_in_worker_thread(alembic_downgrade, previous)
            await database_engine.dispose()
            async with database_engine.connect() as connection:
                names = await connection.run_sync(
                    lambda sync: sa.inspect(sync).get_table_names()
                )
                assert new_tables.isdisjoint(names)
                assert (
                    await connection.scalar(
                        sa.text("SELECT COUNT(*) FROM stream_partition WHERE id=:id"),
                        {"id": str(stream_id)},
                    )
                    == 1
                )
                columns = await connection.run_sync(
                    lambda sync: sa.inspect(sync).get_columns("stream_physical_subtask")
                )
                assert "profile" not in {column["name"] for column in columns}
            await run_sync_in_worker_thread(alembic_upgrade, "head")
            await database_engine.dispose()
            async with database_engine.connect() as connection:
                names = await connection.run_sync(
                    lambda sync: sa.inspect(sync).get_table_names()
                )
                assert new_tables.issubset(names)
                assert (
                    await connection.scalar(
                        sa.text("SELECT COUNT(*) FROM stream_partition WHERE id=:id"),
                        {"id": str(stream_id)},
                    )
                    == 1
                )
                unique = await connection.run_sync(
                    lambda sync: sa.inspect(sync).get_unique_constraints(
                        "stream_worker_capability"
                    )
                )
                assert any(
                    set(row["column_names"])
                    == {"worker_id", "protocol", "protocol_version"}
                    for row in unique
                )
                assert not any(row["column_names"] == ["worker_id"] for row in unique)
        finally:
            await run_sync_in_worker_thread(alembic_upgrade, "head")
            await database_engine.dispose()


async def test_dag_history_without_submissions_refuses_destructive_downgrade(
    db: PrefectDBInterface, database_engine: AsyncEngine, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        graph = db.StreamGraphRevision(
            flow_run_id=flow_run.id,
            operation_id=uuid4(),
            profile="partitioned-dag-v1",
            assignment_epoch=1,
            graph_digest="11" * 32,
            request_digest="11" * 32,
            declaration={},
            state="SEALED",
        )
        session.add(graph)
    previous = "b48f2e6a7c31" if db.dialect.name == "postgresql" else "e59a3f7b8d42"
    async with _isolated_postgres_schema_changes(db, database_engine):
        await database_engine.dispose()
        try:
            with pytest.raises(RuntimeError, match="Cannot downgrade durable DAG"):
                await run_sync_in_worker_thread(alembic_downgrade, previous)
            await database_engine.dispose()
            async with database_engine.connect() as connection:
                assert (
                    await connection.scalar(
                        sa.text(
                            "SELECT COUNT(*) FROM stream_graph_revision WHERE profile='partitioned-dag-v1'"
                        )
                    )
                    == 1
                )
                assert (
                    await connection.scalar(
                        sa.text("SELECT COUNT(*) FROM stream_submission")
                    )
                    == 0
                )
                names = await connection.run_sync(
                    lambda sync: sa.inspect(sync).get_table_names()
                )
                assert "stream_physical_slot_allocation" in names
        finally:
            # Newer heads can commit their downgrade before this history guard.
            # Restore the full schema before the next test's strict cleanup.
            try:
                await run_sync_in_worker_thread(alembic_upgrade, "head")
            finally:
                await database_engine.dispose()


@pytest.mark.parametrize(
    "corruption",
    [
        "output-port-direction",
        "target-index",
        "physical-profile",
        "slot-without-admission",
        "unsupported-capability",
    ],
)
async def test_dag_migrated_relational_guards_reject_invalid_scope(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, corruption: str
):
    async with db.session_context(begin_transaction=True) as session:
        plan = await stream_dags.seal_dag_metadata(
            session, db, dag_request(flow_run.id)
        )
        edge = next(value for value in plan.edges if value.edge_uid == "A-C")
        pool = db.WorkPool(name="migration-" + uuid4().hex, type="process")
        session.add(pool)
        await session.flush()
        worker = db.Worker(work_pool_id=pool.id, name="migration-worker")
        session.add(worker)
        await session.flush()
        with pytest.raises(IntegrityError):
            async with session.begin_nested():
                if corruption == "output-port-direction":
                    consumer = next(
                        value for value in plan.operators if value.operator_uid == "C"
                    )
                    assert consumer.input_port is not None
                    await session.execute(
                        sa.update(db.StreamGraphEdge)
                        .where(db.StreamGraphEdge.id == edge.id)
                        .values(producer_port_id=consumer.input_port.id)
                    )
                elif corruption == "target-index":
                    await session.execute(
                        sa.update(db.StreamGraphEdgePartition)
                        .where(db.StreamGraphEdgePartition.id == edge.partitions[0].id)
                        .values(target_subtask_id=edge.partitions[1].target_subtask_id)
                    )
                elif corruption == "physical-profile":
                    await session.execute(
                        sa.update(db.StreamPhysicalSubtask)
                        .where(db.StreamPhysicalSubtask.id == plan.subtasks[0].id)
                        .values(profile="partitioned-fixed-v1")
                    )
                elif corruption == "slot-without-admission":
                    session.add(
                        db.StreamPhysicalSlotAllocation(
                            graph_revision_id=plan.id,
                            subtask_id=plan.subtasks[0].id,
                            worker_id=worker.id,
                            profile="partitioned-dag-v1",
                            admission_operation_id=uuid4(),
                            released=False,
                        )
                    )
                else:
                    session.add(
                        db.StreamWorkerCapability(
                            worker_id=worker.id,
                            work_pool_id=pool.id,
                            protocol="unknown-v1",
                            protocol_version=1,
                            host_id=uuid4(),
                            boot_id=uuid4(),
                            launcher_process_id=1,
                            launcher_start_ticks=1,
                            last_renewed_at=now("UTC"),
                        )
                    )
                await session.flush()
        assert (
            await stream_dags.read_dag_plan(session, db, plan.id, metadata_only=True)
        ).model_dump() == plan.model_dump()
