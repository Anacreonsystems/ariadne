"""DAG topology metadata tests; native execution remains explicitly fenced."""

import asyncio
from collections import Counter
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Literal
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import flow_runs as flow_run_models
from prefect.server.models import stream_dags as m
from prefect.server.models import task_runs as task_run_models
from prefect.server.models import workers
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.models.stream_executions import (
    apply_flow_lifecycle_result,
    claim_execution,
    guard_flow_lifecycle,
    guard_task_lifecycle,
    unstarted_graph_tasks,
)
from prefect.server.models.stream_subtask_authority import (
    metadata_task_control,
    metadata_task_control_active,
)
from prefect.server.models.stream_worker_capacity import (
    physical_worker_occupancy,
    renew_worker_capability_metadata,
)
from prefect.server.orchestration.core_policy import CoreFlowPolicy
from prefect.server.schemas import actions, core, states
from prefect.server.schemas import stream_dags as s
from prefect.server.schemas import stream_subtasks as physical
from prefect.server.schemas.core import TaskRunPolicy
from prefect.settings import PREFECT_SERVER_API_AUTH_STRING, temporary_settings
from prefect.types import DateTime
from prefect.types._datetime import now
from tests.server.models.test_stream_executions import (
    _claim,
    _credentials,
    _recoverable_flow,
)

pytestmark = [pytest.mark.clear_db, pytest.mark.service("database")]


@pytest.fixture(autouse=True)
async def backend_scope(
    db: PrefectDBInterface, request: pytest.FixtureRequest
) -> AsyncGenerator[None, None]:
    if request.node.name.startswith("test_postgres_"):
        engine = await db.engine()
        if (
            db.dialect.name != "postgresql"
            or (engine.dialect.server_version_info or (0,))[0] != 17
        ):
            pytest.skip("Capacity semantics require PostgreSQL 17")
    elif request.node.name.startswith("test_sqlite_") and db.dialect.name != "sqlite":
        pytest.skip("SQLite refusal control")
    with temporary_settings({PREFECT_SERVER_API_AUTH_STRING: "native-dag:test"}):
        yield


def dag_request(
    flow_run_id: UUID, *, source_p: int = 1, branch_p: int = 2
) -> s.StreamDAGSealRequest:
    common = {
        "task_version": "dag-task-v1",
        "artifact_digest": "11" * 32,
        "context_key": "ctx",
        "task_policy": TaskRunPolicy(retries=2),
    }
    return s.StreamDAGSealRequest(
        flow_run_id=flow_run_id,
        operation_id=uuid4(),
        flow_version="dag-flow-v1",
        operators=[
            s.StreamDAGOperatorSpec(
                operator_uid="A",
                role="SOURCE",
                parallelism=source_p,
                task_name="source",
                task_key="source-key",
                entrypoint="tests/fixtures/native_partitioned_stream_flow.py:source",
                parameters=[{"split": i} for i in range(source_p)],
                output_port=s.StreamPortSpec(uid="out"),
                **common,
            ),
            s.StreamDAGOperatorSpec(
                operator_uid="B",
                role="OPERATOR",
                parallelism=source_p,
                task_name="branch-b",
                task_key="branch-b-key",
                entrypoint="tests/fixtures/native_partitioned_stream_flow.py:sign",
                parameters=[{} for _ in range(source_p)],
                input_port=s.StreamPortSpec(uid="in"),
                output_port=s.StreamPortSpec(uid="out"),
                input_key="upstream",
                **common,
            ),
            s.StreamDAGOperatorSpec(
                operator_uid="C",
                role="OPERATOR",
                parallelism=branch_p,
                task_name="branch-c",
                task_key="branch-c-key",
                entrypoint="tests/fixtures/native_partitioned_stream_flow.py:sign",
                parameters=[{} for _ in range(branch_p)],
                input_port=s.StreamPortSpec(uid="in"),
                output_port=s.StreamPortSpec(uid="out"),
                input_key="upstream",
                **common,
            ),
        ],
        edges=[
            s.StreamEdgeSpec(
                edge_uid="A-B",
                producer_operator_uid="A",
                output_port_uid="out",
                subscriber_uid="subscriber-B",
                consumer_operator_uid="B",
                input_port_uid="in",
                exchange="forward",
            ),
            s.StreamEdgeSpec(
                edge_uid="A-C",
                producer_operator_uid="A",
                output_port_uid="out",
                subscriber_uid="subscriber-C",
                consumer_operator_uid="C",
                input_port_uid="in",
                exchange="broadcast",
            ),
        ],
    )


async def test_seal_reuses_existing_stream_metadata_and_exact_read_plan(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    request = dag_request(flow_run.id)
    async with db.session_context(begin_transaction=True) as session:
        plan = await m.seal_dag_metadata(session, db, request)
    assert plan.execution_ready is False
    assert len(plan.subtasks) == 4
    assert all(
        row.assigned_worker_id is None
        and not row.physical_slot_reserved
        and not row.physical_slot_released
        for row in plan.subtasks
    )
    assert plan.leaf_operator_uids == ["B", "C"]
    async with db.session_context(begin_transaction=True) as session:
        replay = await m.seal_dag_metadata(session, db, request)
        read = await m.read_dag_plan(session, db, plan.id, metadata_only=True)
        assert (
            replay.model_dump(mode="json")
            == read.model_dump(mode="json")
            == plan.model_dump(mode="json")
        )
        counts = {}
        for model in (
            db.StreamGraphRevision,
            db.StreamOperatorRun,
            db.StreamGraphPort,
            db.StreamGraphEdge,
            db.StreamGraphEdgePartition,
            db.TaskRun,
            db.StreamSubmission,
            db.StreamPartition,
            db.StreamRecord,
            db.StreamSubtaskGrant,
            db.StreamPhysicalSlotAllocation,
        ):
            counts[model.__tablename__] = await session.scalar(
                sa.select(sa.func.count()).select_from(model)
            )
        assert counts == {
            "stream_graph_revision": 1,
            "stream_operator_run": 3,
            "stream_graph_port": 5,
            "stream_graph_edge": 2,
            "stream_graph_edge_partition": 3,
            "task_run": 4,
            "stream_submission": 4,
            "stream_partition": 7,
            "stream_record": 0,
            "stream_subtask_grant": 0,
            "stream_physical_slot_allocation": 0,
        }


async def metadata_workers(
    session: AsyncSession, db: PrefectDBInterface, count: int
) -> tuple[orm_models.WorkPool, list[tuple[UUID, physical.StreamWorkerPrincipal]]]:
    pool = await workers.create_work_pool(
        session=session,
        work_pool=actions.WorkPoolCreate(name="dag-" + uuid4().hex, type="process"),
    )
    host_id, boot_id = uuid4(), uuid4()
    result: list[tuple[UUID, physical.StreamWorkerPrincipal]] = []
    for index in range(count):
        name = "dag-worker-" + str(index)
        await workers.worker_heartbeat(
            session=session, work_pool_id=pool.id, worker_name=name
        )
        worker = await session.scalar(
            sa.select(db.Worker).where(
                db.Worker.work_pool_id == pool.id, db.Worker.name == name
            )
        )
        assert worker is not None
        principal = physical.StreamWorkerPrincipal(
            work_pool_id=pool.id,
            host_id=host_id,
            boot_id=boot_id,
            launcher_process_id=1000 + index,
            launcher_start_ticks=2000 + index,
        )
        await m.register_dag_capability_metadata(session, db, worker.id, principal)
        result.append((worker.id, principal))
    return pool, sorted(result, key=lambda value: value[0])


@pytest.mark.parametrize("source_p,branch_p,worker_count", [(1, 2, 2), (2, 2, 3)])
async def test_postgres_metadata_region_admits_complete_n_and_exact_replay(
    db: PrefectDBInterface,
    flow_run: orm_models.FlowRun,
    source_p: int,
    branch_p: int,
    worker_count: int,
):
    request = dag_request(flow_run.id, source_p=source_p, branch_p=branch_p)
    async with db.session_context(begin_transaction=True) as session:
        pool, worker_rows = await metadata_workers(session, db, worker_count)
        plan = await m.seal_dag_metadata(session, db, request)
        admission = s.StreamDAGAdmissionRequest(
            operation_id=uuid4(), graph_digest=plan.graph_digest
        )
        admitted = await m.admit_dag_metadata(
            session, db, plan.id, admission, work_pool_id=pool.id
        )
    expected = 2 * source_p + branch_p
    assert admitted.state == "ADMITTED" and len(admitted.subtasks) == expected
    assert all(
        row.physical_slot_reserved and not row.physical_slot_released
        for row in admitted.subtasks
    )
    assert Counter(row.assigned_worker_id for row in admitted.subtasks) == {
        worker_id: 2 for worker_id, _ in worker_rows
    }
    async with db.session_context(begin_transaction=True) as session:
        repeated = await m.admit_dag_metadata(
            session, db, plan.id, admission, work_pool_id=pool.id
        )
        assert repeated.model_dump(mode="json") == admitted.model_dump(mode="json")
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamPhysicalSlotAllocation)
            )
            == expected
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskAllocation)
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamRecord)
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskGrant)
            )
            == 0
        )
        assert await physical_worker_occupancy(
            session, db, [value[0] for value in worker_rows]
        ) == {worker_id: 2 for worker_id, _ in worker_rows}


async def test_postgres_n6_with_two_workers_refuses_without_partial_placement_or_tags(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    request = dag_request(flow_run.id, source_p=2, branch_p=2)
    for operator in request.operators:
        operator.tags = ["capacity"]
    async with db.session_context(begin_transaction=True) as session:
        pool, _ = await metadata_workers(session, db, 2)
        limit = db.ConcurrencyLimitV2(
            name="tag:capacity",
            limit=6,
            active=True,
            active_slots=0,
            slot_decay_per_second=0,
        )
        session.add(limit)
        plan = await m.seal_dag_metadata(session, db, request)
        with pytest.raises(NativeStreamError, match="complete DAG") as error:
            await m.admit_dag_metadata(
                session,
                db,
                plan.id,
                s.StreamDAGAdmissionRequest(
                    operation_id=uuid4(), graph_digest=plan.graph_digest
                ),
                work_pool_id=pool.id,
            )
        assert error.value.code == "REGION_CAPACITY"
        read = await m.read_dag_plan(session, db, plan.id, metadata_only=True)
        assert read.state == "SEALED" and read.placement_digest is None
        assert all(
            row.assigned_worker_id is None and not row.physical_slot_reserved
            for row in read.subtasks
        )
        await session.refresh(limit)
        assert limit.active_slots == 0
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamPhysicalSlotAllocation)
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskAllocation)
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskGrant)
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamRecord)
            )
            == 0
        )


async def test_postgres_tag_capacity_failure_rolls_back_complete_worker_reservation(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    request = dag_request(flow_run.id)
    for operator in request.operators:
        operator.tags = ["a", "b"]
    async with db.session_context(begin_transaction=True) as session:
        pool, _ = await metadata_workers(session, db, 2)
        limits = [
            db.ConcurrencyLimitV2(
                name="tag:" + tag,
                limit=bound,
                active=True,
                active_slots=0,
                slot_decay_per_second=0,
            )
            for tag, bound in (("a", 4), ("b", 3))
        ]
        session.add_all(limits)
        plan = await m.seal_dag_metadata(session, db, request)
        with pytest.raises(NativeStreamError) as error:
            await m.admit_dag_metadata(
                session,
                db,
                plan.id,
                s.StreamDAGAdmissionRequest(
                    operation_id=uuid4(), graph_digest=plan.graph_digest
                ),
                work_pool_id=pool.id,
            )
        assert error.value.code == "REGION_CAPACITY"
        for limit in limits:
            await session.refresh(limit)
            assert limit.active_slots == 0
        assert (
            await m.read_dag_plan(session, db, plan.id, metadata_only=True)
        ).state == "SEALED"
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamPhysicalSlotAllocation)
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskAllocation)
            )
            == 0
        )


@pytest.mark.parametrize(
    "corruption",
    [
        "extra-operator",
        "edge-binding",
        "manifest-code",
        "manifest-dynamic",
        "task-dynamic",
        "manifest-state",
        "task-inputs",
    ],
)
async def test_read_plan_refuses_corrupted_immutable_metadata(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, corruption: str
):
    async with db.session_context(begin_transaction=True) as session:
        plan = await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        if corruption == "extra-operator":
            session.add(
                db.StreamOperatorRun(
                    graph_revision_id=plan.id,
                    profile="partitioned-dag-v1",
                    operator_uid="extra",
                    role="OPERATOR",
                    parallelism=1,
                    specification={},
                )
            )
        elif corruption == "edge-binding":
            edge = await session.scalar(
                sa.select(db.StreamGraphEdge).where(
                    db.StreamGraphEdge.graph_revision_id == plan.id,
                    db.StreamGraphEdge.edge_uid == "A-B",
                )
            )
            assert edge is not None
            alternate = next(
                value for value in plan.operators if value.operator_uid == "C"
            )
            edge.producer_port_id = alternate.output_port.id
        else:
            manifest = await session.get(
                db.StreamSubmission,
                (
                    await session.get(db.StreamPhysicalSubtask, plan.subtasks[0].id)
                ).submission_id,
            )
            assert manifest is not None
            task = await session.get(db.TaskRun, manifest.task_run_id)
            assert task is not None
            if corruption == "manifest-code":
                manifest.task_version = "drifted-code"
            elif corruption == "manifest-dynamic":
                manifest.dynamic_key = "drifted-dynamic"
            elif corruption == "task-dynamic":
                task.dynamic_key = "drifted-dynamic"
            elif corruption == "task-inputs":
                task.task_inputs = {"poison": []}
            else:
                manifest.state = "RELEASED"
                manifest.released_at = now("UTC")
        await session.flush()
        with pytest.raises(NativeStreamError) as error:
            await m.read_dag_plan(session, db, plan.id, metadata_only=True)
        assert error.value.code == "IDENTITY_CONFLICT"


async def test_sqlite_metadata_admission_refuses_without_mutation(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        plan = await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        with pytest.raises(NativeStreamError) as error:
            await m.admit_dag_metadata(
                session,
                db,
                plan.id,
                s.StreamDAGAdmissionRequest(
                    operation_id=uuid4(), graph_digest=plan.graph_digest
                ),
                work_pool_id=uuid4(),
            )
        assert error.value.code == "UNSUPPORTED_BACKEND"
        assert (
            await m.read_dag_plan(session, db, plan.id, metadata_only=True)
        ).model_dump() == plan.model_dump()
        principal = physical.StreamWorkerPrincipal(
            work_pool_id=uuid4(),
            host_id=uuid4(),
            boot_id=uuid4(),
            launcher_process_id=1,
            launcher_start_ticks=1,
        )
        with pytest.raises(NativeStreamError) as capability_error:
            await m.register_dag_capability_metadata(session, db, uuid4(), principal)
        assert capability_error.value.code == "UNSUPPORTED_BACKEND"
        for model in (
            db.StreamPhysicalSlotAllocation,
            db.StreamSubtaskAllocation,
            db.StreamWorkerCapability,
            db.StreamSubtaskGrant,
            db.StreamRecord,
        ):
            assert (
                await session.scalar(sa.select(sa.func.count()).select_from(model)) == 0
            )


async def test_postgres_exact_subset_and_fragmented_hosts_do_not_hide_capacity(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        pool, rows = await metadata_workers(session, db, 6)
        # Early worker IDs each advertise a different incomplete host cohort.
        for worker_id, _ in rows[:4]:
            cap = await session.scalar(
                sa.select(db.StreamWorkerCapability).where(
                    db.StreamWorkerCapability.worker_id == worker_id,
                    db.StreamWorkerCapability.protocol == m.DAG_PROFILE,
                )
            )
            assert cap is not None
            cap.host_id = uuid4()
        await session.flush()
        plan = await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        admitted = await m.admit_dag_metadata(
            session,
            db,
            plan.id,
            s.StreamDAGAdmissionRequest(
                operation_id=uuid4(), graph_digest=plan.graph_digest
            ),
            work_pool_id=pool.id,
        )
        assert Counter(row.assigned_worker_id for row in admitted.subtasks) == {
            worker_id: 2 for worker_id, _ in rows[4:]
        }


async def test_postgres_surplus_workers_use_exact_two_worker_region(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        pool, rows = await metadata_workers(session, db, 6)
        plan = await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        admitted = await m.admit_dag_metadata(
            session,
            db,
            plan.id,
            s.StreamDAGAdmissionRequest(
                operation_id=uuid4(), graph_digest=plan.graph_digest
            ),
            work_pool_id=pool.id,
        )
        counts = Counter(row.assigned_worker_id for row in admitted.subtasks)
        assert len(counts) == 2 and set(counts.values()) == {2}
        assert set(counts) == {row[0] for row in rows[:2]}


async def test_postgres_dual_capability_coexists_and_identity_rotation_is_fenced(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):

    async with db.session_context(begin_transaction=True) as session:
        pool, rows = await metadata_workers(session, db, 2)
        for worker_id, principal in rows:
            await renew_worker_capability_metadata(
                session, db, worker_id, principal, protocol="partitioned-fixed-v1"
            )
        plan = await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        admitted = await m.admit_dag_metadata(
            session,
            db,
            plan.id,
            s.StreamDAGAdmissionRequest(
                operation_id=uuid4(), graph_digest=plan.graph_digest
            ),
            work_pool_id=pool.id,
        )
        assert len(admitted.subtasks) == 4
        assert await physical_worker_occupancy(
            session, db, [row[0] for row in rows]
        ) == {row[0]: 2 for row in rows}
        worker_id, original = rows[0]
        changed = original.model_copy(
            update={"launcher_start_ticks": original.launcher_start_ticks + 1}
        )
        with pytest.raises(NativeStreamError) as error:
            await m.register_dag_capability_metadata(session, db, worker_id, changed)
        assert error.value.code == "WORKER_IDENTITY_IN_USE"
        caps = (
            await session.scalars(
                sa.select(db.StreamWorkerCapability).where(
                    db.StreamWorkerCapability.worker_id == worker_id
                )
            )
        ).all()
        assert len(caps) == 2 and all(
            cap.launcher_start_ticks == original.launcher_start_ticks for cap in caps
        )


async def test_postgres_zero_occupancy_rotation_updates_both_advertisements(
    db: PrefectDBInterface,
):

    async with db.session_context(begin_transaction=True) as session:
        _, rows = await metadata_workers(session, db, 1)
        worker_id, original = rows[0]
        await renew_worker_capability_metadata(
            session, db, worker_id, original, protocol="partitioned-fixed-v1"
        )
        changed = original.model_copy(
            update={"launcher_start_ticks": original.launcher_start_ticks + 1}
        )
        await m.register_dag_capability_metadata(session, db, worker_id, changed)
        caps = (
            await session.scalars(
                sa.select(db.StreamWorkerCapability).where(
                    db.StreamWorkerCapability.worker_id == worker_id
                )
            )
        ).all()
        assert len(caps) == 2 and all(
            cap.launcher_start_ticks == changed.launcher_start_ticks for cap in caps
        )
        indexed = {cap.protocol: cap for cap in caps}
        assert (
            indexed[m.DAG_PROFILE].last_renewed_at
            - indexed["partitioned-fixed-v1"].last_renewed_at
        ).total_seconds() >= 120


@pytest.mark.parametrize("admitted", [False, True])
async def test_postgres_metadata_abort_releases_exactly_once_through_task_policy(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, admitted: bool
):

    request = dag_request(flow_run.id, source_p=2, branch_p=2)
    for operator in request.operators:
        operator.tags = ["abort"]
    async with db.session_context(begin_transaction=True) as session:
        pool, rows = await metadata_workers(session, db, 3)
        limit = db.ConcurrencyLimitV2(
            name="tag:abort",
            limit=6,
            active=True,
            active_slots=0,
            slot_decay_per_second=0,
        )
        session.add(limit)
        plan = await m.seal_dag_metadata(session, db, request)
        if admitted:
            plan = await m.admit_dag_metadata(
                session,
                db,
                plan.id,
                s.StreamDAGAdmissionRequest(
                    operation_id=uuid4(), graph_digest=plan.graph_digest
                ),
                work_pool_id=pool.id,
            )
            assert limit.active_slots == 6
        abort = physical.StreamGraphAbortRequest(
            operation_id=uuid4(),
            graph_digest=plan.graph_digest,
            error="Metadata region intentionally aborted before execution",
        )
        result = await m.abort_dag_metadata(session, db, plan.id, abort)
        assert result.state == "RELEASED"
        assert all(row.physical_slot_released == admitted for row in result.subtasks)
        assert not metadata_task_control_active(session)
        assert limit.active_slots == 0
        tasks = (
            await session.scalars(
                sa.select(db.TaskRun).where(db.TaskRun.flow_run_id == flow_run.id)
            )
        ).all()
        assert len(tasks) == 6 and all(
            task.state_type == states.StateType.FAILED
            and task.run_count == 0
            and task.state.state_details.retriable is False
            for task in tasks
        )
        state_count = await session.scalar(
            sa.select(sa.func.count()).select_from(db.TaskRunState)
        )
        assert state_count == 12
        assert (
            await m.abort_dag_metadata(session, db, plan.id, abort)
        ).model_dump() == result.model_dump()
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.TaskRunState)
            )
            == state_count
        )
        assert (
            await physical_worker_occupancy(session, db, [row[0] for row in rows]) == {}
        )
        partitions = (
            await session.scalars(
                sa.select(db.StreamPartition).where(
                    db.StreamPartition.flow_run_id == flow_run.id
                )
            )
        ).all()
        assert len(partitions) == 10 and all(
            row.state == "FAILED" and row.last_sequence == 0 for row in partitions
        )


async def test_metadata_abort_transaction_rollback_resets_private_authority(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):

    async with db.session_context(begin_transaction=True) as session:
        plan = await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        with pytest.raises(RuntimeError, match="rollback"):
            async with session.begin_nested():
                await m.abort_dag_metadata(
                    session,
                    db,
                    plan.id,
                    physical.StreamGraphAbortRequest(
                        operation_id=uuid4(),
                        graph_digest=plan.graph_digest,
                        error="rollback test",
                    ),
                )
                raise RuntimeError("rollback")
        assert not metadata_task_control_active(session)
        reread = await m.read_dag_plan(session, db, plan.id, metadata_only=True)
        assert reread.model_dump() == plan.model_dump()
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.TaskRunState)
            )
            == 4
        )


async def test_linear_parallel_dag_metadata_is_generic_and_source_only_is_rejected(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    request = dag_request(flow_run.id, source_p=2, branch_p=3)
    body = request.model_dump(mode="json")
    body["operators"] = body["operators"][:1] + [body["operators"][2]]
    body["edges"] = [body["edges"][1] | {"exchange": "rebalance"}]
    linear = s.StreamDAGSealRequest.model_validate(body)
    async with db.session_context(begin_transaction=True) as session:
        plan = await m.seal_dag_metadata(session, db, linear)
        assert (
            len(plan.operators) == 2
            and len(plan.edges) == 1
            and len(plan.subtasks) == 5
        )
        assert plan.leaf_operator_uids == ["C"]
    with pytest.raises(ValueError):
        s.StreamDAGSealRequest.model_validate(
            body | {"operators": body["operators"][:1], "edges": []}
        )


@pytest.mark.parametrize("corruption", ["operation", "request-digest"])
async def test_read_plan_refuses_graph_scalar_identity_drift(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, corruption: str
):
    async with db.session_context(begin_transaction=True) as session:
        plan = await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        graph = await session.get(db.StreamGraphRevision, plan.id)
        assert graph is not None
        if corruption == "operation":
            graph.operation_id = uuid4()
        else:
            graph.request_digest = "ff" * 32
        await session.flush()
        with pytest.raises(NativeStreamError) as error:
            await m.read_dag_plan(session, db, plan.id, metadata_only=True)
        assert error.value.code == "IDENTITY_CONFLICT"


@pytest.mark.parametrize("corruption", ["physical-epoch", "partition-owner"])
async def test_postgres_admission_refuses_prior_execution_evidence(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun, corruption: str
):
    async with db.session_context(begin_transaction=True) as session:
        pool, _ = await metadata_workers(session, db, 2)
        plan = await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        if corruption == "physical-epoch":
            row = await session.get(db.StreamPhysicalSubtask, plan.subtasks[0].id)
            assert row is not None
            grant = db.StreamSubtaskGrant(
                subtask_id=row.id,
                operation_id=uuid4(),
                request_digest="11" * 32,
                physical_epoch=1,
                holder_id=uuid4(),
                capability_digest="11" * 32,
                event_public_key="11" * 32,
                event_key_id="11" * 32,
                scope_digest="11" * 32,
                launcher_binding_kind="FLOW_AUTHORITY_LOCAL",
                flow_generation=1,
                flow_holder_id=uuid4(),
                host_id=uuid4(),
                boot_id=uuid4(),
                launcher_process_id=1,
                launcher_start_ticks=1,
                state="CLAIMED",
                last_renewed_at=now("UTC"),
            )
            session.add(grant)
            await session.flush()
            row.physical_epoch, row.current_grant_id = 1, grant.id
        else:
            stream = await session.get(
                db.StreamPartition, plan.subtasks[0].output_stream_id
            )
            assert stream is not None
            stream.owner_id, stream.owner_epoch, stream.attempt = uuid4(), 1, 1
        await session.flush()
        with pytest.raises(NativeStreamError) as error:
            await m.admit_dag_metadata(
                session,
                db,
                plan.id,
                s.StreamDAGAdmissionRequest(
                    operation_id=uuid4(), graph_digest=plan.graph_digest
                ),
                work_pool_id=pool.id,
            )
        assert error.value.code == "IDENTITY_CONFLICT"
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamPhysicalSlotAllocation)
            )
            == 0
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamSubtaskAllocation)
            )
            == 0
        )
        assert (
            await m.read_dag_plan(session, db, plan.id, metadata_only=True)
        ).state == "SEALED"


async def test_postgres_n6_first_running_and_reentry_reconcile_all_task_counters(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    await _recoverable_flow(db, flow_run)
    claim = _claim(flow_run.id)
    credentials = _credentials(claim)
    async with db.session_context(begin_transaction=True) as session:
        await claim_execution(session, db, claim)
        request = dag_request(flow_run.id, source_p=2, branch_p=2).model_copy(
            update={"flow_version": "flow-v1"}
        )
        plan = await m.seal_dag_metadata(session, db, request)
        for repetition in range(2):
            if repetition:
                await flow_run_models.set_flow_run_state(
                    session=session,
                    flow_run_id=flow_run.id,
                    state=states.Pending(),
                    flow_policy=CoreFlowPolicy,
                )
            authority = await guard_flow_lifecycle(
                session, db, flow_run.id, credentials, states.Running(), force=False
            )
            result = await flow_run_models.set_flow_run_state(
                session=session,
                flow_run_id=flow_run.id,
                state=states.Running(),
                flow_policy=CoreFlowPolicy,
            )
            await apply_flow_lifecycle_result(session, db, authority, result.state)
            current = await session.get(db.FlowRun, flow_run.id)
            assert current is not None
            counts = (
                await session.scalars(
                    sa.select(db.TaskRun.flow_run_run_count).where(
                        db.TaskRun.flow_run_id == flow_run.id
                    )
                )
            ).all()
            assert len(counts) == 6 and counts == [current.run_count] * 6
        assert (
            await m.read_dag_plan(session, db, plan.id, metadata_only=True)
        ).state == "SEALED"


async def test_existing_ordinary_task_refuses_dag_seal_without_side_effects(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        await task_run_models.create_task_run(
            session=session,
            task_run=core.TaskRun(
                flow_run_id=flow_run.id,
                task_key="ordinary",
                dynamic_key="0",
                state=states.Pending(),
            ),
        )
        with pytest.raises(NativeStreamError) as error:
            await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        assert error.value.code == "UNSUPPORTED_PROFILE"
        assert (
            await session.scalar(sa.select(sa.func.count()).select_from(db.TaskRun))
            == 1
        )
        for model in (
            db.StreamGraphRevision,
            db.StreamSubmission,
            db.StreamPartition,
            db.StreamPhysicalSubtask,
            db.StreamPhysicalSlotAllocation,
        ):
            assert (
                await session.scalar(sa.select(sa.func.count()).select_from(model)) == 0
            )


async def test_metadata_task_guard_refuses_public_shaped_proof_and_resets_after_exception(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        plan = await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        task_id = plan.subtasks[0].manifest.task_run_id
        session.info["metadata_task_control"] = {
            "graph_id": plan.id,
            "operation_id": uuid4(),
        }
        with pytest.raises(NativeStreamError) as error:
            await guard_task_lifecycle(
                session,
                db,
                task_id,
                None,
                states.Failed(state_details=states.StateDetails(retriable=False)),
                force=False,
            )
        assert error.value.code == "EXECUTION_PROFILE_NOT_READY"
        with pytest.raises(RuntimeError, match="inside private scope"):
            async with session.begin_nested():
                graph = await session.get(db.StreamGraphRevision, plan.id)
                assert graph is not None
                graph.state, graph.abort_operation_id = "ABORTING", uuid4()
                graph.abort_request_digest, graph.abort_error = "11" * 32, "rollback"
                with metadata_task_control(session, graph, graph.abort_operation_id):
                    assert metadata_task_control_active(session)
                    raise RuntimeError("inside private scope")
        assert not metadata_task_control_active(session)
        assert (
            await m.read_dag_plan(session, db, plan.id, metadata_only=True)
        ).state == "SEALED"
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.TaskRunState)
            )
            == 4
        )


async def test_postseal_extra_task_cannot_mutate_lifecycle_abort_or_reentry(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        plan = await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        extra = await task_run_models.create_task_run(
            session=session,
            task_run=core.TaskRun(
                flow_run_id=flow_run.id,
                task_key="unbound",
                dynamic_key="0",
                state=states.Pending(),
            ),
        )
        before_states = set(
            (await session.scalars(sa.select(db.TaskRunState.id))).all()
        )
        with pytest.raises(NativeStreamError) as lifecycle_error:
            await guard_task_lifecycle(
                session, db, extra.id, None, states.Running(), force=False
            )
        assert lifecycle_error.value.code == "IDENTITY_CONFLICT"
        with pytest.raises(NativeStreamError) as abort_error:
            await m.abort_dag_metadata(
                session,
                db,
                plan.id,
                physical.StreamGraphAbortRequest(
                    operation_id=uuid4(),
                    graph_digest=plan.graph_digest,
                    error="unbound task must refuse all compensation",
                ),
            )
        assert abort_error.value.code == "IDENTITY_CONFLICT"
        graph = await session.get(db.StreamGraphRevision, plan.id)
        assert graph is not None
        with pytest.raises(NativeStreamError) as reentry_error:
            await unstarted_graph_tasks(session, db, graph)
        assert reentry_error.value.code == "RECOVERY_NOT_READY"
        assert graph.state == "SEALED" and graph.abort_operation_id is None
        assert not metadata_task_control_active(session)
        assert (
            set((await session.scalars(sa.select(db.TaskRunState.id))).all())
            == before_states
        )
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamPhysicalSlotAllocation)
            )
            == 0
        )


async def test_postseal_extra_partition_refuses_abort_without_mutating_any_stream(
    db: PrefectDBInterface, flow_run: orm_models.FlowRun
):
    async with db.session_context(begin_transaction=True) as session:
        plan = await m.seal_dag_metadata(session, db, dag_request(flow_run.id))
        extra = db.StreamPartition(flow_run_id=flow_run.id, limits={})
        session.add(extra)
        await session.flush()
        state_ids = set((await session.scalars(sa.select(db.TaskRunState.id))).all())
        with pytest.raises(NativeStreamError) as error:
            await m.abort_dag_metadata(
                session,
                db,
                plan.id,
                physical.StreamGraphAbortRequest(
                    operation_id=uuid4(),
                    graph_digest=plan.graph_digest,
                    error="Extra partition must not be closed by graph compensation",
                ),
            )
        assert error.value.code == "IDENTITY_CONFLICT"
        partitions = (
            await session.scalars(
                sa.select(db.StreamPartition).where(
                    db.StreamPartition.flow_run_id == flow_run.id
                )
            )
        ).all()
        assert len(partitions) == 8 and all(
            row.state == "OPEN"
            and row.error is None
            and row.owner_id is None
            and row.last_sequence == 0
            for row in partitions
        )
        assert (
            set((await session.scalars(sa.select(db.TaskRunState.id))).all())
            == state_ids
        )
        graph = await session.get(db.StreamGraphRevision, plan.id)
        assert (
            graph is not None
            and graph.state == "SEALED"
            and graph.abort_operation_id is None
        )
        assert not metadata_task_control_active(session)
        assert (
            await session.scalar(
                sa.select(sa.func.count()).select_from(db.StreamPhysicalSlotAllocation)
            )
            == 0
        )


async def _renewal_worker(
    db: PrefectDBInterface,
    protocols: tuple[Literal["partitioned-fixed-v1", "partitioned-dag-v1"], ...],
) -> tuple[UUID, physical.StreamWorkerPrincipal]:
    async with db.session_context(begin_transaction=True) as session:
        pool = await workers.create_work_pool(
            session=session,
            work_pool=actions.WorkPoolCreate(
                name="renewal-" + uuid4().hex, type="process"
            ),
        )
        await workers.worker_heartbeat(
            session=session, work_pool_id=pool.id, worker_name="renewal-worker"
        )
        worker = await session.scalar(
            sa.select(db.Worker).where(db.Worker.work_pool_id == pool.id)
        )
        assert worker is not None
        principal = physical.StreamWorkerPrincipal(
            work_pool_id=pool.id,
            host_id=uuid4(),
            boot_id=uuid4(),
            launcher_process_id=1000,
            launcher_start_ticks=2000,
        )
        for protocol in protocols:
            await renew_worker_capability_metadata(
                session, db, worker.id, principal, protocol=protocol
            )
        return worker.id, principal


async def _renew_worker_for_test(
    db: PrefectDBInterface,
    worker_id: UUID,
    principal: physical.StreamWorkerPrincipal,
    protocol: Literal["partitioned-fixed-v1", "partitioned-dag-v1"],
    backend_pid: asyncio.Future[int] | None = None,
) -> None:
    async with db.session_context(begin_transaction=True) as session:
        if backend_pid is not None:
            pid = await session.scalar(sa.text("SELECT pg_backend_pid()"))
            assert isinstance(pid, int)
            backend_pid.set_result(pid)
        await renew_worker_capability_metadata(
            session, db, worker_id, principal, protocol=protocol
        )


async def _renewal_capabilities(
    session: AsyncSession, db: PrefectDBInterface, worker_id: UUID
) -> dict[str, tuple[UUID, UUID, int, int, DateTime]]:
    rows = (
        await session.scalars(
            sa.select(db.StreamWorkerCapability)
            .where(db.StreamWorkerCapability.worker_id == worker_id)
            .order_by(db.StreamWorkerCapability.protocol)
            .execution_options(populate_existing=True)
        )
    ).all()
    return {
        row.protocol: (
            row.id,
            row.host_id,
            row.launcher_process_id,
            row.launcher_start_ticks,
            row.last_renewed_at,
        )
        for row in rows
    }


async def _wait_for_worker_renewal_lock(
    db: PrefectDBInterface,
    backend_pid: int,
    holder_pid: int,
    clause: str,
    pending: asyncio.Task[None],
) -> None:
    async with db.session_context() as observer:
        async with asyncio.timeout(10):
            while True:
                await observer.execute(sa.text("SELECT pg_stat_clear_snapshot()"))
                row = (
                    await observer.execute(
                        sa.text(
                            "SELECT query, wait_event_type, pg_blocking_pids(pid) AS blockers "
                            "FROM pg_stat_activity WHERE pid=:pid"
                        ),
                        {"pid": backend_pid},
                    )
                ).one()
                if row.wait_event_type == "Lock" and holder_pid in row.blockers:
                    assert "FROM worker" in row.query and clause in row.query
                    return
                if pending.done():
                    await pending
                    pytest.fail("Renewal completed before the required Worker lock")
                await asyncio.sleep(0.01)


@pytest.mark.parametrize("protocol", ["partitioned-fixed-v1", "partitioned-dag-v1"])
async def test_postgres_same_identity_renewal_completes_while_data_holds_worker_share(
    db: PrefectDBInterface,
    protocol: Literal["partitioned-fixed-v1", "partitioned-dag-v1"],
):
    worker_id, principal = await _renewal_worker(
        db, ("partitioned-fixed-v1", "partitioned-dag-v1")
    )
    async with db.session_context(begin_transaction=True) as setup:
        await setup.execute(
            sa.update(db.StreamWorkerCapability)
            .where(db.StreamWorkerCapability.worker_id == worker_id)
            .values(last_renewed_at=now("UTC") - timedelta(seconds=5))
        )
    async with db.session_context(begin_transaction=True) as holder:
        # This is the Worker row-lock clause held through a native DAG DATA turn.
        await holder.execute(
            sa.select(db.Worker.id)
            .where(db.Worker.id == worker_id)
            .with_for_update(read=True)
        )
        before = await _renewal_capabilities(holder, db, worker_id)
        await asyncio.wait_for(
            _renew_worker_for_test(db, worker_id, principal, protocol), 5
        )
        after = await _renewal_capabilities(holder, db, worker_id)
        assert before.keys() == after.keys()
        for advertised in before:
            assert after[advertised][:-1] == before[advertised][:-1]
            if advertised == protocol:
                assert after[advertised][-1] > before[advertised][-1]
            else:
                assert after[advertised] == before[advertised]


async def test_postgres_exclusive_admission_worker_lock_still_blocks_same_identity_renewal(
    db: PrefectDBInterface,
):
    worker_id, principal = await _renewal_worker(db, ("partitioned-fixed-v1",))
    pid = asyncio.get_running_loop().create_future()
    pending = None
    try:
        async with db.session_context(begin_transaction=True) as holder:
            holder_pid = await holder.scalar(sa.text("SELECT pg_backend_pid()"))
            await holder.execute(
                sa.select(db.Worker.id)
                .where(db.Worker.id == worker_id)
                .with_for_update()
            )
            before = await _renewal_capabilities(holder, db, worker_id)
            pending = asyncio.create_task(
                _renew_worker_for_test(
                    db, worker_id, principal, "partitioned-fixed-v1", pid
                )
            )
            await _wait_for_worker_renewal_lock(
                db, await pid, holder_pid, "FOR SHARE", pending
            )
            assert await _renewal_capabilities(holder, db, worker_id) == before
        await asyncio.wait_for(pending, 10)
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize(
    "change", ["missing-protocol", "identity-rotation", "other-protocol-identity"]
)
async def test_postgres_registration_and_rotation_keep_exclusive_worker_fence(
    db: PrefectDBInterface, change: str
):
    protocols = (
        ("partitioned-dag-v1",)
        if change == "missing-protocol"
        else ("partitioned-dag-v1", "partitioned-fixed-v1")
    )
    worker_id, principal = await _renewal_worker(db, protocols)
    requested = "partitioned-fixed-v1"
    if change == "other-protocol-identity":
        async with db.session_context(begin_transaction=True) as setup:
            await setup.execute(
                sa.update(db.StreamWorkerCapability)
                .where(
                    db.StreamWorkerCapability.worker_id == worker_id,
                    db.StreamWorkerCapability.protocol == "partitioned-dag-v1",
                )
                .values(launcher_start_ticks=principal.launcher_start_ticks + 1)
            )
    if change == "identity-rotation":
        principal = principal.model_copy(
            update={"launcher_start_ticks": principal.launcher_start_ticks + 1}
        )
    pid = asyncio.get_running_loop().create_future()
    pending = None
    try:
        async with db.session_context(begin_transaction=True) as holder:
            holder_pid = await holder.scalar(sa.text("SELECT pg_backend_pid()"))
            await holder.execute(
                sa.select(db.Worker.id)
                .where(db.Worker.id == worker_id)
                .with_for_update(read=True)
            )
            before = await _renewal_capabilities(holder, db, worker_id)
            pending = asyncio.create_task(
                _renew_worker_for_test(db, worker_id, principal, requested, pid)
            )
            await _wait_for_worker_renewal_lock(
                db, await pid, holder_pid, "FOR UPDATE", pending
            )
            assert await _renewal_capabilities(holder, db, worker_id) == before
        await asyncio.wait_for(pending, 10)
        async with db.session_context() as observed:
            after = await _renewal_capabilities(observed, db, worker_id)
        assert set(after) == {"partitioned-fixed-v1", "partitioned-dag-v1"}
        assert all(row[3] == principal.launcher_start_ticks for row in after.values())
        if change != "missing-protocol":
            assert (
                after[requested][-1] - after["partitioned-dag-v1"][-1]
            ).total_seconds() >= 120
        else:
            assert after["partitioned-dag-v1"] == before["partitioned-dag-v1"]
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize("existing", [(), ("partitioned-dag-v1",)])
async def test_postgres_concurrent_first_or_missing_protocol_registration_is_serialized(
    db: PrefectDBInterface,
    existing: tuple[Literal["partitioned-fixed-v1", "partitioned-dag-v1"], ...],
):
    worker_id, principal = await _renewal_worker(db, existing)
    pending = []
    try:
        async with db.session_context(begin_transaction=True) as holder:
            holder_pid = await holder.scalar(sa.text("SELECT pg_backend_pid()"))
            await holder.execute(
                sa.select(db.Worker.id)
                .where(db.Worker.id == worker_id)
                .with_for_update()
            )
            for _ in range(2):
                pid = asyncio.get_running_loop().create_future()
                task = asyncio.create_task(
                    _renew_worker_for_test(
                        db, worker_id, principal, "partitioned-fixed-v1", pid
                    )
                )
                pending.append(task)
                await _wait_for_worker_renewal_lock(
                    db, await pid, holder_pid, "FOR SHARE", task
                )
        await asyncio.wait_for(asyncio.gather(*pending), 10)
        async with db.session_context() as observed:
            after = await _renewal_capabilities(observed, db, worker_id)
        assert set(after) == set(existing) | {"partitioned-fixed-v1"}
        assert all(row[3] == principal.launcher_start_ticks for row in after.values())
    finally:
        for task in pending:
            if not task.done():
                task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
