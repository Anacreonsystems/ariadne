"""Paired physical topology migrations against each actual database dialect."""

from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from tests.server.database.test_migrations import _isolated_postgres_schema_changes

from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.database.alembic_commands import alembic_downgrade, alembic_upgrade
from prefect.server.models import workers
from prefect.types._datetime import now
from prefect.utilities.asyncutils import run_sync_in_worker_thread

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


async def test_physical_topology_roundtrip_keeps_legacy_partition(
    db: PrefectDBInterface, database_engine: AsyncEngine, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        stream = db.StreamPartition(flow_run_id=flow_run.id, limits={})
        session.add(stream)
        await session.flush()
        stream_id = stream.id
    previous = "c95d1f8e3a42" if db.dialect.name == "postgresql" else "d06e2a9f4b53"
    async with _isolated_postgres_schema_changes(db, database_engine):
        await database_engine.dispose()
        try:
            await run_sync_in_worker_thread(alembic_downgrade, previous)
            await database_engine.dispose()
            async with database_engine.connect() as connection:
                names = await connection.run_sync(
                    lambda sync: sa.inspect(sync).get_table_names()
                )
                assert "stream_graph_revision" not in names
                assert (
                    await connection.scalar(
                        sa.text("SELECT COUNT(*) FROM stream_partition WHERE id=:id"),
                        {"id": str(stream_id)},
                    )
                    == 1
                )
            await run_sync_in_worker_thread(alembic_upgrade, "head")
            await database_engine.dispose()
            async with database_engine.connect() as connection:
                names = await connection.run_sync(
                    lambda sync: sa.inspect(sync).get_table_names()
                )
                assert {
                    "stream_graph_revision",
                    "stream_operator_run",
                    "stream_physical_subtask",
                    "stream_subtask_grant",
                    "stream_subtask_replacement",
                    "stream_subtask_allocation",
                    "stream_worker_capability",
                }.issubset(names)
                indexes = await connection.run_sync(
                    lambda sync: sa.inspect(sync).get_indexes("stream_submission")
                )
                source_index = next(
                    index
                    for index in indexes
                    if index["name"] == "uq_stream_submission__one_source_per_flow"
                )
                assert source_index["unique"]
                assert "graph_revision_id IS NULL" in str(
                    source_index["dialect_options"][db.dialect.name + "_where"]
                )
        finally:
            await run_sync_in_worker_thread(alembic_upgrade, "head")
            await database_engine.dispose()


@pytest.mark.parametrize(
    "state,pid,ticks,operation,constraint",
    [
        ("CLAIMED", 5, 10, True, "unregistered_claim"),
        ("REGISTERED", None, None, False, "registration_required"),
        ("RETIRED", 5, None, True, "process_shape_valid"),
        ("RELEASED", 0, 10, True, "process_shape_valid"),
    ],
)
async def test_physical_grant_process_checks_are_enforced_by_database(
    db: PrefectDBInterface,
    state: str,
    pid: int | None,
    ticks: int | None,
    operation: bool,
    constraint: str,
):
    # The CHECK must fire before FK validation; inspecting its exact name avoids
    # mistaking a missing parent FK for protection of an invalid process shape.
    async with db.session_context(begin_transaction=True) as session:
        grant = db.StreamSubtaskGrant(
            subtask_id=uuid4(),
            operation_id=uuid4(),
            request_digest="11" * 32,
            physical_epoch=1,
            holder_id=uuid4(),
            capability_digest="22" * 32,
            event_public_key="33" * 32,
            event_key_id="44" * 32,
            scope_digest="55" * 32,
            launcher_binding_kind="FLOW_AUTHORITY_LOCAL",
            flow_generation=1,
            flow_holder_id=uuid4(),
            host_id=uuid4(),
            boot_id=uuid4(),
            launcher_process_id=1,
            launcher_start_ticks=1,
            state=state,
            last_renewed_at=now("UTC"),
            child_process_id=pid,
            process_start_ticks=ticks,
            registration_operation_id=uuid4() if operation else None,
        )
        with pytest.raises(IntegrityError) as error:
            async with session.begin_nested():
                session.add(grant)
                await session.flush()
        assert "ck_stream_subtask_grant__" + constraint in str(error.value.orig)


@pytest.mark.parametrize(
    "case",
    [
        "admitted-without-admission",
        "sealed-with-admission",
        "released-without-receipt",
        "activated-without-admission",
    ],
)
async def test_graph_lifecycle_check_rejects_impossible_history(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, case: str
):
    values = {
        "flow_run_id": flow_run.id,
        "operation_id": uuid4(),
        "profile": "partitioned-fixed-v1",
        "assignment_epoch": 1,
        "graph_digest": "11" * 32,
        "request_digest": "22" * 32,
        "declaration": {},
        "state": "SEALED",
    }
    if case == "admitted-without-admission":
        values["state"] = "ADMITTED"
    elif case == "sealed-with-admission":
        values.update(
            admission_operation_id=uuid4(), admission_request_digest="33" * 32
        )
    elif case == "released-without-receipt":
        values["state"] = "RELEASED"
    else:
        values.update(
            state="ADMITTED",
            startup_completed_at=now("UTC"),
            activation_operation_id=uuid4(),
            activation_request_digest="44" * 32,
        )
    async with db.session_context(begin_transaction=True) as session:
        with pytest.raises(IntegrityError) as error:
            async with session.begin_nested():
                session.add(db.StreamGraphRevision(**values))
                await session.flush()
        assert "ck_stream_graph_revision__lifecycle_shape_valid" in str(
            error.value.orig
        )


async def test_downgrade_refuses_graph_history_without_submissions(
    db: PrefectDBInterface, database_engine: AsyncEngine, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        session.add(
            db.StreamGraphRevision(
                flow_run_id=flow_run.id,
                operation_id=uuid4(),
                profile="partitioned-fixed-v1",
                assignment_epoch=1,
                graph_digest="11" * 32,
                request_digest="22" * 32,
                declaration={},
                state="SEALED",
            )
        )
    previous = "c95d1f8e3a42" if db.dialect.name == "postgresql" else "d06e2a9f4b53"
    async with _isolated_postgres_schema_changes(db, database_engine):
        await database_engine.dispose()
        with pytest.raises(RuntimeError, match="Cannot downgrade graph-bound"):
            await run_sync_in_worker_thread(alembic_downgrade, previous)
        await database_engine.dispose()
        async with db.session_context() as session:
            assert (
                await session.scalar(
                    sa.select(sa.func.count()).select_from(db.StreamGraphRevision)
                )
                == 1
            )


@pytest.mark.parametrize(
    "profile,valid",
    [
        ("bounded-linear-v1", True),
        ("partitioned-fixed-v1", True),
        ("unreviewed-profile", False),
    ],
)
async def test_snapshot_profile_database_allowlist_is_exact(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, profile: str, valid: bool
):
    async with db.session_context(begin_transaction=True) as session:
        row = db.NativeStreamDeploymentSnapshot(
            flow_run_id=flow_run.id,
            deployment_id=uuid4(),
            work_pool_id=uuid4(),
            declaration={},
            declaration_digest="11" * 32,
            protocol_version=1,
            profile=profile,
            flow_version="v1",
            flow_code_digest="22" * 32,
            resource_profile="bounded-default-v1",
            state="ACTIVE",
        )
        if valid:
            session.add(row)
            await session.flush()
            assert row.profile == profile
        else:
            with pytest.raises(IntegrityError) as error:
                async with session.begin_nested():
                    session.add(row)
                    await session.flush()
            assert "profile_valid" in str(error.value.orig)


async def test_worker_placement_foreign_keys_and_slot_shape_are_migrated(
    db: PrefectDBInterface,
    database_engine: AsyncEngine,
):
    async with database_engine.connect() as connection:
        subtask_foreign_keys = await connection.run_sync(
            lambda sync: sa.inspect(sync).get_foreign_keys("stream_physical_subtask")
        )
        capability_foreign_keys = await connection.run_sync(
            lambda sync: sa.inspect(sync).get_foreign_keys("stream_worker_capability")
        )
    assignment = next(
        row
        for row in subtask_foreign_keys
        if row["constrained_columns"] == ["assigned_worker_id"]
    )
    assert assignment["referred_table"] == "worker"
    assert assignment["options"].get("ondelete") in (None, "NO ACTION", "RESTRICT")
    for column, target in (("worker_id", "worker"), ("work_pool_id", "work_pool")):
        foreign_key = next(
            row
            for row in capability_foreign_keys
            if row["constrained_columns"] == [column]
        )
        assert foreign_key["referred_table"] == target
        assert foreign_key["options"]["ondelete"] == "CASCADE"
    async with db.session_context(begin_transaction=True) as session:
        with pytest.raises(IntegrityError) as error:
            async with session.begin_nested():
                session.add(
                    db.StreamPhysicalSubtask(
                        graph_revision_id=uuid4(),
                        operator_run_id=uuid4(),
                        subtask_index=0,
                        assignment_epoch=1,
                        task_run_id=uuid4(),
                        submission_id=uuid4(),
                        output_stream_id=uuid4(),
                        assigned_worker_id=None,
                        physical_slot_released=True,
                    )
                )
                await session.flush()
        assert "ck_stream_physical_subtask__slot_assignment_valid" in str(
            error.value.orig
        )


async def test_worker_capability_cannot_reference_missing_pool(
    db: PrefectDBInterface,
    work_pool: orm_models.WorkPool,
):
    async with db.session_context(begin_transaction=True) as session:
        worker_name = "capability-fk-" + uuid4().hex
        await workers.worker_heartbeat(
            session=session, work_pool_id=work_pool.id, worker_name=worker_name
        )
        worker = await session.scalar(
            sa.select(db.Worker).where(
                db.Worker.work_pool_id == work_pool.id, db.Worker.name == worker_name
            )
        )
        assert worker is not None
        with pytest.raises(IntegrityError) as error:
            async with session.begin_nested():
                session.add(
                    db.StreamWorkerCapability(
                        worker_id=worker.id,
                        work_pool_id=uuid4(),
                        protocol="partitioned-fixed-v1",
                        protocol_version=1,
                        host_id=uuid4(),
                        boot_id=uuid4(),
                        launcher_process_id=1,
                        launcher_start_ticks=1,
                        last_renewed_at=now("UTC"),
                    )
                )
                await session.flush()
        if db.dialect.name == "postgresql":
            assert "fk_stream_worker_capability__work_pool_id__work_pool" in str(
                error.value.orig
            )
        else:
            assert "FOREIGN KEY constraint failed" in str(error.value.orig)
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamWorkerCapability)
            )
            == 0
        )
