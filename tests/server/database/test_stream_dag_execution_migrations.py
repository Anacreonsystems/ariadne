"""Paired lifecycle/launch DDL preserves NI011 data and exact physical scope.

Rows below are explicit relational fixtures, including metadata-only SQLite
assignments. They do not invoke or bypass unsupported SQLite execution APIs.
"""

from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4, uuid5

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from tests.fixtures import database as database_fixtures
from tests.server.database.test_migrations import _isolated_postgres_schema_changes
from tests.server.database.test_stream_route_migrations import (
    assert_fk,
    data_snapshot,
    ddl_chain,
    legacy_snapshot,
    seed_legacy_predecessor,
)
from tests.server.models.test_stream_dags import dag_request

from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.database.alembic_commands import alembic_downgrade, alembic_upgrade
from prefect.server.models import flow_runs, stream_dags, workers
from prefect.server.schemas import actions, core
from prefect.server.schemas import stream_dags as topology
from prefect.types._datetime import now
from prefect.utilities.asyncutils import run_sync_in_worker_thread

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]
PREVIOUS = {"postgresql": "f1c5b98d4e12", "sqlite": "02d6ca9e5f23"}
EXECUTION_HEAD = {"postgresql": "14e7dbaf6034", "sqlite": "25f8ecb07145"}
EXECUTION_TABLES = {
    "stream_dag_lifecycle_operation",
    "stream_dag_launch_attempt",
    "stream_dag_control_target",
}
BOOTSTRAP_COLUMNS = {
    "bootstrap_digest",
    "bootstrap_witness",
    "bootstrap_ready_operation_id",
    "bootstrap_ready_request_digest",
    "bootstrap_ready_signature",
    "bootstrap_ready_at",
    "bootstrap_ready_receipt",
}


@dataclass(frozen=True)
class History:
    plan: topology.StreamDAGPlan
    start_id: UUID
    control_id: UUID
    launch_id: UUID
    target_id: UUID
    grant_id: UUID
    worker_id: UUID
    peer_id: UUID
    pool_id: UUID
    subtask_id: UUID
    task_run_id: UUID


def lifecycle_values(
    plan: topology.StreamDAGPlan, operation_id: UUID, *, kind: str = "START"
) -> dict[str, Any]:
    values: dict[str, Any] = {
        "id": operation_id,
        "operation_id": operation_id,
        "graph_revision_id": plan.id,
        "profile": "partitioned-dag-v1",
        "flow_run_id": plan.flow_run_id,
        "kind": kind,
        "flow_generation": 1,
        "flow_holder_id": uuid4(),
        "flow_run_run_count": 1,
        "request_digest": "11" * 32,
        "accepted_receipt": {"operation_id": str(operation_id), "kind": kind},
        "outcome_digest": "22" * 32,
        "state": "ACCEPTED",
    }
    if kind == "CONTROL":
        values.update(
            state="STOPPING",
            effective_control_kind="FAIL",
            first_cause={"failure_kind": "START_EXCEPTION"},
            target_set_digest="33" * 32,
        )
    elif kind == "FINALIZE":
        values.update(
            state="COMPLETED",
            terminal_state="FAILED",
            terminal_outcome={"cleanup_complete": True},
            completed_at=now("UTC"),
        )
    return values


async def ddl_history(
    db: PrefectDBInterface, flow_run_id: UUID, *, registered: bool = True
) -> History:
    """Commit a four-slot region with one recorded birth and one frozen target.

    Flow-attempt and process fields are relational metadata. No Task body,
    physical process, readiness signature or execution service is fabricated.
    """
    async with db.session_context(begin_transaction=True) as session:
        plan = await stream_dags.seal_dag_metadata(
            session, db, dag_request(flow_run_id)
        )
        pool = await workers.create_work_pool(
            session,
            actions.WorkPoolCreate(name="ddl-dag-" + uuid4().hex, type="process"),
        )
        worker_rows: list[orm_models.Worker] = []
        for index in range(2):
            worker = await workers.record_worker_heartbeat(
                session=session,
                work_pool=pool,
                worker_name=f"ddl-worker-{index}",
                return_worker=True,
            )
            assert worker is not None
            worker_rows.append(worker)
        start_id = uuid5(plan.id, "prefect-stream-dag-start-v1")
        control_id = uuid5(plan.id, "prefect-stream-dag-ddl-control-v1")
        await session.execute(
            sa.update(db.StreamGraphRevision)
            .where(db.StreamGraphRevision.id == plan.id)
            .values(
                state="ADMITTED",
                admission_operation_id=start_id,
                admission_request_digest="44" * 32,
                placement_digest="55" * 32,
            )
        )
        for index, subtask in enumerate(plan.subtasks):
            worker_id = worker_rows[index // 2].id
            await session.execute(
                sa.update(db.StreamPhysicalSubtask)
                .where(db.StreamPhysicalSubtask.id == subtask.id)
                .values(assigned_worker_id=worker_id)
            )
            session.add(
                db.StreamPhysicalSlotAllocation(
                    graph_revision_id=plan.id,
                    subtask_id=subtask.id,
                    worker_id=worker_id,
                    profile="partitioned-dag-v1",
                    admission_operation_id=start_id,
                )
            )
        session.add(db.StreamDAGLifecycleOperation(**lifecycle_values(plan, start_id)))
        session.add(
            db.StreamDAGLifecycleOperation(
                **lifecycle_values(plan, control_id, kind="CONTROL")
            )
        )
        await session.flush()
        own = plan.subtasks[0]
        grant_id, launch_id, target_id = uuid4(), uuid4(), uuid4()
        holder_id, host_id, boot_id = uuid4(), uuid4(), uuid4()
        principal = {
            "worker_id": worker_rows[0].id,
            "work_pool_id": pool.id,
            "host_id": host_id,
            "boot_id": boot_id,
            "launcher_process_id": 100,
            "launcher_start_ticks": 1000,
            "flow_generation": 1,
            "flow_holder_id": holder_id,
        }
        session.add(
            db.StreamSubtaskGrant(
                id=grant_id,
                subtask_id=own.id,
                operation_id=uuid4(),
                request_digest="66" * 32,
                physical_epoch=1,
                holder_id=uuid4(),
                capability_digest="77" * 32,
                event_public_key="88" * 32,
                event_key_id="99" * 32,
                scope_digest="aa" * 32,
                launcher_binding_kind="ASSIGNED_WORKER",
                state="REGISTERED" if registered else "CLAIMED",
                child_process_id=200 if registered else None,
                process_start_ticks=2000 if registered else None,
                registration_operation_id=uuid4() if registered else None,
                last_renewed_at=now("UTC"),
                **principal,
            )
        )
        await session.flush()
        await session.execute(
            sa.update(db.StreamPhysicalSubtask)
            .where(db.StreamPhysicalSubtask.id == own.id)
            .values(current_grant_id=grant_id, physical_epoch=1)
        )
        common = {
            "graph_revision_id": plan.id,
            "subtask_id": own.id,
            "task_run_id": own.manifest.task_run_id,
            "start_operation_id": start_id,
            "worker_generation_digest": "bb" * 32,
            "flow_run_run_count": 1,
            **principal,
        }
        session.add(
            db.StreamDAGLaunchAttempt(
                id=launch_id,
                attempt_index=1,
                planned_physical_epoch=1,
                expected_physical_epoch=0,
                grant_id=grant_id,
                registered_grant_id=grant_id if registered else None,
                state="PID_RECORDED",
                prepare_operation_id=uuid4(),
                prepare_request_digest="cc" * 32,
                prepare_receipt={"kind": "PREPARE"},
                record_operation_id=uuid4(),
                record_request_digest="dd" * 32,
                record_receipt={"kind": "RECORD"},
                child_process_id=200,
                process_start_ticks=2000,
                local_journal_digest="ee" * 32,
                **common,
            )
        )
        session.add(
            db.StreamDAGControlTarget(
                id=target_id,
                control_operation_id=control_id,
                grant_id=grant_id,
                physical_epoch=1,
                state="PENDING",
                **common,
            )
        )
        await session.flush()
        await session.execute(
            sa.update(db.StreamGraphRevision)
            .where(db.StreamGraphRevision.id == plan.id)
            .values(current_control_operation_id=control_id)
        )
        return History(
            plan=plan,
            start_id=start_id,
            control_id=control_id,
            launch_id=launch_id,
            target_id=target_id,
            grant_id=grant_id,
            worker_id=worker_rows[0].id,
            peer_id=worker_rows[1].id,
            pool_id=pool.id,
            subtask_id=own.id,
            task_run_id=own.manifest.task_run_id,
        )


async def history_snapshot(db: PrefectDBInterface) -> dict[str, list[dict[str, Any]]]:
    async with db.session_context() as session:
        result: dict[str, list[dict[str, Any]]] = {}
        for model in (
            db.StreamDAGLifecycleOperation,
            db.StreamDAGLaunchAttempt,
            db.StreamDAGControlTarget,
            db.StreamGraphRevision,
            db.StreamPhysicalSubtask,
            db.StreamSubtaskGrant,
            db.StreamPhysicalSlotAllocation,
            db.StreamPartition,
        ):
            rows = await session.execute(sa.select(model.__table__).order_by(model.id))
            result[model.__tablename__] = [dict(row) for row in rows.mappings()]
        return result


async def test_execution_ddl_fixture_commits_exact_recorded_process_and_holds_four_slots(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    history = await ddl_history(db, flow_run.id)
    rows = await history_snapshot(db)
    assert len(rows["stream_dag_lifecycle_operation"]) == 2
    assert len(rows["stream_dag_launch_attempt"]) == 1
    assert len(rows["stream_dag_control_target"]) == 1
    assert len(rows["stream_physical_slot_allocation"]) == 4
    launch = rows["stream_dag_launch_attempt"][0]
    assert launch["registered_grant_id"] == history.grant_id
    assert launch["state"] == "PID_RECORDED"
    assert all(
        row["released"] is False for row in rows["stream_physical_slot_allocation"]
    )
    assert all(
        rows["stream_subtask_grant"][0][name] is None for name in BOOTSTRAP_COLUMNS
    )


async def test_actual_strict_fixture_purges_graph_operation_cycle_after_rollback(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    request: pytest.FixtureRequest,
):
    await ddl_history(db, flow_run.id)
    before = await history_snapshot(db)
    with pytest.raises(IntegrityError):
        async with db.session_context(begin_transaction=True) as session:
            await session.execute(sa.delete(db.StreamDAGLifecycleOperation))
    assert await history_snapshot(db) == before
    # Exercise the real fixture's target -> pointer -> launch -> operation ->
    # graph hierarchy order, rather than a second test-only cleanup algorithm.
    cleanup = database_fixtures.clear_db.__wrapped__(db, request)
    try:
        await anext(cleanup)
        assert all(not rows for rows in (await history_snapshot(db)).values())
    finally:
        await cleanup.aclose()


@pytest.mark.parametrize(
    "case",
    [
        "launch-worker",
        "launch-task",
        "launch-pid",
        "launch-ticks",
        "target-worker",
        "target-task",
    ],
)
async def test_execution_composites_reject_existing_sibling_coordinates_atomically(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, case: str
):
    history = await ddl_history(db, flow_run.id)
    sibling = next(row for row in history.plan.subtasks if row.id != history.subtask_id)
    choices = {
        "launch-worker": (
            db.StreamDAGLaunchAttempt,
            history.launch_id,
            {"worker_id": history.peer_id},
            "fk_dag_launch__assignment",
        ),
        "launch-task": (
            db.StreamDAGLaunchAttempt,
            history.launch_id,
            {"task_run_id": sibling.manifest.task_run_id},
            "fk_dag_launch__task_scope",
        ),
        "launch-pid": (
            db.StreamDAGLaunchAttempt,
            history.launch_id,
            {"child_process_id": 201},
            "fk_dag_launch__registered_process",
        ),
        "launch-ticks": (
            db.StreamDAGLaunchAttempt,
            history.launch_id,
            {"process_start_ticks": 2001},
            "fk_dag_launch__registered_process",
        ),
        "target-worker": (
            db.StreamDAGControlTarget,
            history.target_id,
            {"worker_id": history.peer_id},
            "fk_dag_target__assignment",
        ),
        "target-task": (
            db.StreamDAGControlTarget,
            history.target_id,
            {"task_run_id": sibling.manifest.task_run_id},
            "fk_dag_target__task_scope",
        ),
    }
    model, identity, change, constraint = choices[case]
    baseline = await history_snapshot(db)
    marker = uuid4()
    with pytest.raises(IntegrityError) as rejected:
        async with db.session_context(begin_transaction=True) as session:
            session.add(
                db.StreamPartition(id=marker, flow_run_id=flow_run.id, limits={})
            )
            await session.flush()
            await session.execute(
                sa.update(model).where(model.id == identity).values(**change)
            )
            pytest.fail("The scoped FK must refuse before transaction commit")
    assert_fk(rejected.value, db, constraint)
    assert await history_snapshot(db) == baseline


@pytest.mark.parametrize(
    "case",
    [
        "launch-start",
        "target-start",
        "target-control",
        "graph-control",
        "graph-terminal",
        "operation-flow",
    ],
)
async def test_foreign_graph_and_flow_journal_links_cannot_be_rebound(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, case: str
):
    history = await ddl_history(db, flow_run.id)
    async with db.session_context(begin_transaction=True) as session:
        other_flow = await flow_runs.create_flow_run(
            session, core.FlowRun(flow_id=flow_run.flow_id)
        )
    foreign = await ddl_history(db, other_flow.id)
    choices = {
        "launch-start": (
            db.StreamDAGLaunchAttempt,
            history.launch_id,
            {"start_operation_id": foreign.start_id},
            "fk_dag_launch__start_scope",
        ),
        "target-start": (
            db.StreamDAGControlTarget,
            history.target_id,
            {"start_operation_id": foreign.start_id},
            "fk_dag_target__start_scope",
        ),
        "target-control": (
            db.StreamDAGControlTarget,
            history.target_id,
            {"control_operation_id": foreign.control_id},
            "fk_dag_target__control_scope",
        ),
        "graph-control": (
            db.StreamGraphRevision,
            history.plan.id,
            {"current_control_operation_id": foreign.control_id},
            "fk_stream_graph__dag_control",
        ),
        "graph-terminal": (
            db.StreamGraphRevision,
            history.plan.id,
            {"terminal_operation_id": foreign.start_id},
            "fk_stream_graph__dag_terminal",
        ),
        "operation-flow": (
            db.StreamDAGLifecycleOperation,
            history.start_id,
            {"flow_run_id": foreign.plan.flow_run_id},
            "fk_dag_operation__graph_flow",
        ),
    }
    model, identity, change, constraint = choices[case]
    baseline = await history_snapshot(db)
    with pytest.raises(IntegrityError) as rejected:
        async with db.session_context(begin_transaction=True) as session:
            await session.execute(
                sa.update(model).where(model.id == identity).values(**change)
            )
    assert_fk(rejected.value, db, constraint)
    assert await history_snapshot(db) == baseline


async def test_dag_lifecycle_profile_fk_refuses_an_existing_fixed_graph(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    history = await ddl_history(db, flow_run.id)
    async with db.session_context(begin_transaction=True) as session:
        other_flow = await flow_runs.create_flow_run(
            session, core.FlowRun(flow_id=flow_run.flow_id)
        )
        fixed_id = uuid4()
        session.add(
            db.StreamGraphRevision(
                id=fixed_id,
                flow_run_id=other_flow.id,
                operation_id=uuid4(),
                profile="partitioned-fixed-v1",
                assignment_epoch=1,
                graph_digest="aa" * 32,
                request_digest="bb" * 32,
                declaration={},
                state="SEALED",
            )
        )
    values = lifecycle_values(history.plan, uuid4())
    values.update(graph_revision_id=fixed_id, flow_run_id=other_flow.id)
    baseline = await history_snapshot(db)
    with pytest.raises(IntegrityError) as rejected:
        async with db.session_context(begin_transaction=True) as session:
            session.add(db.StreamDAGLifecycleOperation(**values))
    assert_fk(rejected.value, db, "fk_dag_operation__graph_profile")
    assert await history_snapshot(db) == baseline


@pytest.mark.parametrize(
    "case",
    [
        "bootstrap-half",
        "ready-half",
        "closed-unstarted-recorded",
        "stopped-no-closed-set",
        "attempt-65",
    ],
)
async def test_partial_readiness_close_and_stop_evidence_cannot_commit(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, case: str
):
    history = await ddl_history(db, flow_run.id)
    choices = {
        "bootstrap-half": (
            db.StreamSubtaskGrant,
            history.grant_id,
            {"bootstrap_digest": "ff" * 32},
            "ck_stream_subtask_grant__bootstrap_shape_valid",
        ),
        "ready-half": (
            db.StreamSubtaskGrant,
            history.grant_id,
            {"bootstrap_ready_operation_id": uuid4()},
            "ck_stream_subtask_grant__bootstrap_ready_shape_valid",
        ),
        "closed-unstarted-recorded": (
            db.StreamDAGLaunchAttempt,
            history.launch_id,
            {
                "state": "CLOSED",
                "close_operation_id": uuid4(),
                "close_request_digest": "ff" * 32,
                "close_proof_kind": "CLOSED_UNSTARTED",
                "close_proof": {"recovery_complete": True},
                "close_receipt": {},
                "closed_at": now("UTC"),
            },
            "ck_stream_dag_launch_attempt__close_shape_valid",
        ),
        "stopped-no-closed-set": (
            db.StreamDAGControlTarget,
            history.target_id,
            {
                "state": "STOPPED",
                "stop_operation_id": uuid4(),
                "stop_request_digest": "ff" * 32,
                "stop_receipt": {},
                "stopped_at": now("UTC"),
            },
            "ck_stream_dag_control_target__stop_shape_valid",
        ),
        "attempt-65": (
            db.StreamDAGLaunchAttempt,
            history.launch_id,
            {"attempt_index": 65},
            "ck_stream_dag_launch_attempt__attempt_valid",
        ),
    }
    model, identity, change, constraint = choices[case]
    baseline = await history_snapshot(db)
    with pytest.raises(IntegrityError) as rejected:
        async with db.session_context(begin_transaction=True) as session:
            await session.execute(
                sa.update(model).where(model.id == identity).values(**change)
            )
    assert constraint in str(rejected.value.orig)
    assert await history_snapshot(db) == baseline


@pytest.mark.parametrize(
    "case",
    [
        "record-null-ticks",
        "control-null-kind",
        "finalize-null-state",
        "close-null-proof-kind",
        "grant-before-record",
    ],
)
async def test_sql_null_and_unrecorded_grant_links_cannot_bypass_checks(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, case: str
):
    history = await ddl_history(db, flow_run.id)
    baseline = await history_snapshot(db)
    with pytest.raises(IntegrityError) as rejected:
        async with db.session_context(begin_transaction=True) as session:
            if case == "record-null-ticks":
                await session.execute(
                    sa.update(db.StreamDAGLaunchAttempt)
                    .where(db.StreamDAGLaunchAttempt.id == history.launch_id)
                    .values(process_start_ticks=None)
                )
            elif case == "control-null-kind":
                await session.execute(
                    sa.update(db.StreamDAGLifecycleOperation)
                    .where(db.StreamDAGLifecycleOperation.id == history.control_id)
                    .values(effective_control_kind=None)
                )
            elif case == "finalize-null-state":
                values = lifecycle_values(history.plan, uuid4(), kind="FINALIZE")
                values["terminal_state"] = None
                session.add(db.StreamDAGLifecycleOperation(**values))
            elif case == "close-null-proof-kind":
                await session.execute(
                    sa.update(db.StreamDAGLaunchAttempt)
                    .where(db.StreamDAGLaunchAttempt.id == history.launch_id)
                    .values(
                        state="CLOSED",
                        close_operation_id=uuid4(),
                        close_request_digest="ff" * 32,
                        close_proof_kind=None,
                        close_proof={},
                        close_receipt={},
                        closed_at=now("UTC"),
                    )
                )
            else:
                await session.execute(
                    sa.update(db.StreamDAGLaunchAttempt)
                    .where(db.StreamDAGLaunchAttempt.id == history.launch_id)
                    .values(
                        state="PREPARED",
                        registered_grant_id=None,
                        child_process_id=None,
                        process_start_ticks=None,
                        record_operation_id=None,
                        record_request_digest=None,
                        record_receipt=None,
                        local_journal_digest=None,
                    )
                )
    expected = {
        "record-null-ticks": ("record_shape_valid", "grant_link_shape_valid"),
        "control-null-kind": ("control_shape_valid",),
        "finalize-null-state": ("terminal_shape_valid",),
        "close-null-proof-kind": ("close_shape_valid",),
        "grant-before-record": ("grant_link_shape_valid",),
    }
    assert any(name in str(rejected.value.orig) for name in expected[case])
    assert await history_snapshot(db) == baseline


async def test_migrated_execution_scope_and_indexes_are_exact_and_immediate(
    database_engine: AsyncEngine,
):
    expected = {
        "stream_dag_lifecycle_operation": {
            "fk_dag_operation__graph_profile": (
                "graph_revision_id,profile",
                "stream_graph_revision",
                "id,profile",
            ),
            "fk_dag_operation__graph_flow": (
                "graph_revision_id,flow_run_id",
                "stream_graph_revision",
                "id,flow_run_id",
            ),
            "fk_dag_operation__control_scope": (
                "control_operation_id,graph_revision_id",
                "stream_dag_lifecycle_operation",
                "id,graph_revision_id",
            ),
            "fk_dag_operation__subject_scope": (
                "subject_subtask_id,graph_revision_id",
                "stream_physical_subtask",
                "id,graph_revision_id",
            ),
        },
        "stream_dag_launch_attempt": {
            "fk_dag_launch__assignment": (
                "subtask_id,graph_revision_id,worker_id",
                "stream_physical_subtask",
                "id,graph_revision_id,assigned_worker_id",
            ),
            "fk_dag_launch__task_scope": (
                "subtask_id,graph_revision_id,task_run_id",
                "stream_physical_subtask",
                "id,graph_revision_id,task_run_id",
            ),
            "fk_dag_launch__start_scope": (
                "start_operation_id,graph_revision_id",
                "stream_dag_lifecycle_operation",
                "id,graph_revision_id",
            ),
            "fk_dag_launch__expected_grant": (
                "expected_grant_id,subtask_id,expected_physical_epoch",
                "stream_subtask_grant",
                "id,subtask_id,physical_epoch",
            ),
            "fk_dag_launch__replacement_scope": (
                "subtask_id,replacement_operation_id",
                "stream_subtask_replacement",
                "subtask_id,operation_id",
            ),
            "fk_dag_launch__grant_scope": (
                "grant_id,subtask_id,planned_physical_epoch",
                "stream_subtask_grant",
                "id,subtask_id,physical_epoch",
            ),
            "fk_dag_launch__registered_process": (
                "registered_grant_id,subtask_id,planned_physical_epoch,child_process_id,process_start_ticks",
                "stream_subtask_grant",
                "id,subtask_id,physical_epoch,child_process_id,process_start_ticks",
            ),
        },
        "stream_dag_control_target": {
            "fk_dag_target__control_scope": (
                "control_operation_id,graph_revision_id",
                "stream_dag_lifecycle_operation",
                "id,graph_revision_id",
            ),
            "fk_dag_target__start_scope": (
                "start_operation_id,graph_revision_id",
                "stream_dag_lifecycle_operation",
                "id,graph_revision_id",
            ),
            "fk_dag_target__assignment": (
                "subtask_id,graph_revision_id,worker_id",
                "stream_physical_subtask",
                "id,graph_revision_id,assigned_worker_id",
            ),
            "fk_dag_target__task_scope": (
                "subtask_id,graph_revision_id,task_run_id",
                "stream_physical_subtask",
                "id,graph_revision_id,task_run_id",
            ),
            "fk_dag_target__grant_scope": (
                "grant_id,subtask_id,physical_epoch",
                "stream_subtask_grant",
                "id,subtask_id,physical_epoch",
            ),
        },
        "stream_graph_revision": {
            "fk_stream_graph__dag_control": (
                "current_control_operation_id,id",
                "stream_dag_lifecycle_operation",
                "id,graph_revision_id",
            ),
            "fk_stream_graph__dag_terminal": (
                "terminal_operation_id,id",
                "stream_dag_lifecycle_operation",
                "id,graph_revision_id",
            ),
        },
    }

    def reflect(connection: sa.Connection) -> dict[str, Any]:
        inspector = sa.inspect(connection)
        return {
            name: {
                "fks": inspector.get_foreign_keys(name),
                "checks": inspector.get_check_constraints(name),
                "uniques": inspector.get_unique_constraints(name),
                "indexes": inspector.get_indexes(name),
            }
            for name in inspector.get_table_names()
            if name.startswith("stream_")
        }

    async with database_engine.connect() as connection:
        schema = await connection.run_sync(reflect)
    for table, foreign_keys in expected.items():
        actual = {row["name"]: row for row in schema[table]["fks"]}
        for name, (local, remote_table, remote) in foreign_keys.items():
            assert actual[name]["constrained_columns"] == local.split(",")
            assert actual[name]["referred_table"] == remote_table
            assert actual[name]["referred_columns"] == remote.split(",")
            assert not actual[name]["options"].get("deferrable")
    deferred = [
        (table, fk["name"])
        for table, metadata in schema.items()
        for fk in metadata["fks"]
        if fk["options"].get("deferrable")
    ]
    assert deferred == [
        ("stream_route_receipt", "fk_stream_route_receipt__input_reference")
    ]
    checks = {
        "stream_dag_launch_attempt": {
            "attempt_valid",
            "record_shape_valid",
            "grant_link_shape_valid",
            "registration_shape_valid",
            "close_shape_valid",
        },
        "stream_dag_control_target": {"grant_shape_valid", "stop_shape_valid"},
        "stream_dag_lifecycle_operation": {
            "identity_valid",
            "control_shape_valid",
            "terminal_shape_valid",
        },
        "stream_subtask_grant": {
            "bootstrap_shape_valid",
            "bootstrap_ready_shape_valid",
        },
    }
    for table, names in checks.items():
        actual = {row["name"] for row in schema[table]["checks"]}
        assert {f"ck_{table}__{name}" for name in names} <= actual
    indices = {
        row["name"]: row for row in schema["stream_dag_lifecycle_operation"]["indexes"]
    }
    for name in (
        "uq_dag_operation__one_control",
        "uq_dag_operation__one_finalize",
        "uq_dag_operation__producer_finish",
    ):
        assert indices[name]["unique"]
        assert indices[name]["dialect_options"]
    uniques = {
        tuple(row["column_names"])
        for row in schema["stream_dag_launch_attempt"]["uniques"]
    }
    assert {
        ("subtask_id", "attempt_index"),
        ("subtask_id", "prepare_operation_id"),
        ("subtask_id", "planned_physical_epoch"),
        ("grant_id",),
        ("registered_grant_id",),
    } <= uniques


@pytest.mark.parametrize(
    "kind,index",
    [
        ("CONTROL", "uq_dag_operation__one_control"),
        ("FINALIZE", "uq_dag_operation__one_finalize"),
        ("SOURCE_FINISH", "uq_dag_operation__producer_finish"),
    ],
)
async def test_one_control_terminal_and_producer_finish_journal_per_identity(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, kind: str, index: str
):
    history = await ddl_history(db, flow_run.id)
    values = lifecycle_values(history.plan, uuid4(), kind=kind)
    if kind == "SOURCE_FINISH":
        values["subject_subtask_id"] = history.subtask_id
    if kind != "CONTROL":
        async with db.session_context(begin_transaction=True) as session:
            session.add(db.StreamDAGLifecycleOperation(**values))
        values = {**values, "id": uuid4()}
        values["operation_id"] = values["id"]
    baseline = await history_snapshot(db)
    with pytest.raises(IntegrityError) as rejected:
        async with db.session_context(begin_transaction=True) as session:
            session.add(db.StreamDAGLifecycleOperation(**values))
    if db.dialect.name == "postgresql":
        assert index in str(rejected.value.orig)
    else:
        assert "UNIQUE constraint failed" in str(rejected.value.orig)
    assert await history_snapshot(db) == baseline


async def test_explicit_ni011_roundtrip_preserves_legacy_json_cursor_and_completion_bytes(
    db: PrefectDBInterface, database_engine: AsyncEngine, flow_run: orm_models.FlowRun
):
    async with _isolated_postgres_schema_changes(db, database_engine):
        await database_engine.dispose()
        try:
            await run_sync_in_worker_thread(
                alembic_downgrade, PREVIOUS[db.dialect.name]
            )
            await database_engine.dispose()
            await seed_legacy_predecessor(database_engine, flow_run.id)
            before = await legacy_snapshot(database_engine)
            assert any(
                '"signed_zero":-0.0' in row["value_text"] for row in before["records"]
            )
            await run_sync_in_worker_thread(
                alembic_upgrade, EXECUTION_HEAD[db.dialect.name]
            )
            await database_engine.dispose()
            assert await legacy_snapshot(database_engine) == before
            async with database_engine.connect() as connection:
                tables = await connection.run_sync(
                    lambda sync: sa.inspect(sync).get_table_names()
                )
                assert EXECUTION_TABLES.issubset(tables)
                columns = await connection.run_sync(
                    lambda sync: sa.inspect(sync).get_columns("stream_subtask_grant")
                )
                assert all(
                    column["nullable"]
                    for column in columns
                    if column["name"] in BOOTSTRAP_COLUMNS
                )
                assert BOOTSTRAP_COLUMNS <= {column["name"] for column in columns}
            await run_sync_in_worker_thread(
                alembic_downgrade, PREVIOUS[db.dialect.name]
            )
            await database_engine.dispose()
            assert await legacy_snapshot(database_engine) == before
        finally:
            try:
                await run_sync_in_worker_thread(alembic_upgrade, "head")
            finally:
                await database_engine.dispose()
        assert await legacy_snapshot(database_engine) == before


async def test_existing_routed_history_can_return_to_ni011_without_erasing_records(
    db: PrefectDBInterface, database_engine: AsyncEngine, flow_run: orm_models.FlowRun
):
    await ddl_chain(db, flow_run.id)
    before = await data_snapshot(db)
    raw_before = await legacy_snapshot(database_engine)
    async with _isolated_postgres_schema_changes(db, database_engine):
        await database_engine.dispose()
        try:
            await run_sync_in_worker_thread(
                alembic_downgrade, PREVIOUS[db.dialect.name]
            )
            await database_engine.dispose()
            assert await data_snapshot(db) == before
            assert await legacy_snapshot(database_engine) == raw_before
        finally:
            try:
                await run_sync_in_worker_thread(alembic_upgrade, "head")
            finally:
                await database_engine.dispose()
    assert await data_snapshot(db) == before


@pytest.mark.parametrize(
    "history_kind", ["lifecycle-only", "recorded-launch-and-target", "bootstrap-only"]
)
async def test_retained_execution_history_blocks_downgrade_and_always_restores_head(
    db: PrefectDBInterface,
    database_engine: AsyncEngine,
    flow_run: orm_models.FlowRun,
    history_kind: str,
):
    history = await ddl_history(db, flow_run.id)
    if history_kind != "recorded-launch-and-target":
        async with db.session_context(begin_transaction=True) as session:
            await session.execute(
                sa.update(db.StreamGraphRevision)
                .where(db.StreamGraphRevision.id == history.plan.id)
                .values(current_control_operation_id=None)
            )
            await session.execute(sa.delete(db.StreamDAGControlTarget))
            await session.execute(sa.delete(db.StreamDAGLaunchAttempt))
            if history_kind == "bootstrap-only":
                await session.execute(sa.delete(db.StreamDAGLifecycleOperation))
                await session.execute(
                    sa.update(db.StreamSubtaskGrant)
                    .where(db.StreamSubtaskGrant.id == history.grant_id)
                    .values(
                        bootstrap_digest="ff" * 32,
                        bootstrap_witness={"immutable": "fixture"},
                    )
                )
    before = await history_snapshot(db)
    async with _isolated_postgres_schema_changes(db, database_engine):
        await database_engine.dispose()
        try:
            with pytest.raises(
                RuntimeError,
                match="Cannot downgrade retained DAG execution or process-launch history",
            ):
                await run_sync_in_worker_thread(
                    alembic_downgrade, PREVIOUS[db.dialect.name]
                )
        finally:
            try:
                await run_sync_in_worker_thread(alembic_upgrade, "head")
            finally:
                await database_engine.dispose()
    assert await history_snapshot(db) == before
