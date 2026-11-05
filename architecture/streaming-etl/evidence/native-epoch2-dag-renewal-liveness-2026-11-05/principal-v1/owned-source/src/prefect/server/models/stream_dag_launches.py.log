"""Assigned-worker process journal; missing grants never prove process death."""

import hmac
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, cast
from uuid import UUID, uuid5

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect._internal.stream_event_auth import dag_worker_generation_digest
from prefect._internal.stream_route_plan import graph_routing_digest
from prefect._internal.uuid7 import uuid7
from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import stream_dag_executions as lifecycle
from prefect.server.models import stream_dags
from prefect.server.models import stream_subtasks as physical_models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.models.stream_subtask_authority import (
    DAGWorkerStreamAuthorization,
    capability_digest,
    release_allocations,
    require_physical_backend,
)
from prefect.server.models.stream_subtasks import create_subtask_grant, grant_projection
from prefect.server.models.stream_worker_capacity import (
    renew_worker_capability_metadata,
)
from prefect.server.schemas import stream_dag_executions as s
from prefect.server.schemas import stream_dags as topology
from prefect.server.schemas import stream_subtasks as physical
from prefect.server.schemas.states import TERMINAL_STATES
from prefect.server.schemas.statuses import WorkerStatus
from prefect.types import DateTime
from prefect.types._datetime import now


def principal_identity(
    principal: physical.StreamWorkerPrincipal,
) -> tuple[UUID, UUID, UUID, int, int]:
    return (
        principal.work_pool_id,
        principal.host_id,
        principal.boot_id,
        principal.launcher_process_id,
        principal.launcher_start_ticks,
    )


def worker_generation(
    worker_id: UUID, principal: physical.StreamWorkerPrincipal
) -> str:
    # Normalize asyncpg UUID subclasses at the DB boundary, preserving the
    # shared helper's exact builtin UUID contract.
    return dag_worker_generation_digest(
        worker_id=UUID(str(worker_id)),
        work_pool_id=UUID(str(principal.work_pool_id)),
        host_id=UUID(str(principal.host_id)),
        boot_id=UUID(str(principal.boot_id)),
        launcher_process_id=principal.launcher_process_id,
        launcher_start_ticks=principal.launcher_start_ticks,
    )


@dataclass(frozen=True)
class DAGWorkerScope:
    graph: lifecycle.DAGGraphScope
    worker: orm_models.Worker
    capability: orm_models.StreamWorkerCapability
    subtask: orm_models.StreamPhysicalSubtask
    principal: physical.StreamWorkerPrincipal


async def lock_worker_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    principal: physical.StreamWorkerPrincipal,
    *,
    new_launch: bool = False,
    all_workers: bool = False,
    graph_update: bool = True,
) -> DAGWorkerScope:
    await require_physical_backend(session, db)
    graph_id = await session.scalar(
        sa.select(db.StreamPhysicalSubtask.graph_revision_id).where(
            db.StreamPhysicalSubtask.id == subtask_id
        )
    )
    if graph_id is None:
        raise NativeStreamError("NOT_FOUND", "Unknown DAG target")
    scope = await lifecycle.lock_dag_graph(
        session, db, graph_id, None, update=graph_update, worker=True
    )
    if all_workers:
        assigned = sa.select(db.StreamPhysicalSubtask.assigned_worker_id).where(
            db.StreamPhysicalSubtask.graph_revision_id == graph_id,
            db.StreamPhysicalSubtask.assigned_worker_id.is_not(None),
        )
        await session.execute(
            sa.select(db.Worker.id)
            .where(db.Worker.id.in_(assigned))
            .order_by(db.Worker.id)
            .with_for_update(read=True)
        )
    worker = await session.scalar(
        sa.select(db.Worker)
        .where(db.Worker.id == worker_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    capability = await session.scalar(
        sa.select(db.StreamWorkerCapability)
        .where(
            db.StreamWorkerCapability.worker_id == worker_id,
            db.StreamWorkerCapability.protocol == lifecycle.DAG_PROFILE,
            db.StreamWorkerCapability.protocol_version == 1,
        )
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    subtask = await session.scalar(
        sa.select(db.StreamPhysicalSubtask)
        .where(db.StreamPhysicalSubtask.id == subtask_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        worker is None
        or capability is None
        or subtask is None
        or worker.work_pool_id != principal.work_pool_id
        or subtask.assigned_worker_id != worker_id
        or subtask.graph_revision_id != graph_id
        or (
            capability.work_pool_id,
            capability.host_id,
            capability.boot_id,
            capability.launcher_process_id,
            capability.launcher_start_ticks,
        )
        != principal_identity(principal)
    ):
        raise NativeStreamError(
            "SUBTASK_SCOPE_DENIED", "Worker does not own the persisted DAG assignment"
        )
    if new_launch and (
        worker.status != WorkerStatus.ONLINE
        or capability.last_renewed_at < now("UTC") - timedelta(seconds=120)
    ):
        raise NativeStreamError(
            "WORKER_NOT_READY", "New launches require a current online DAG worker"
        )
    slot = await session.scalar(
        sa.select(db.StreamPhysicalSlotAllocation).where(
            db.StreamPhysicalSlotAllocation.subtask_id == subtask_id
        )
    )
    if (
        slot is None
        or slot.graph_revision_id != graph_id
        or slot.worker_id != worker_id
        or slot.released != subtask.physical_slot_released
    ):
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "DAG target has no exact physical reservation"
        )
    return DAGWorkerScope(scope, worker, capability, subtask, principal)


def launch_projection(
    row: orm_models.StreamDAGLaunchAttempt,
) -> s.StreamDAGLaunchAttempt:
    return s.StreamDAGLaunchAttempt(
        id=row.id,
        graph_revision_id=row.graph_revision_id,
        subtask_id=row.subtask_id,
        start_operation_id=row.start_operation_id,
        worker_id=row.worker_id,
        worker_generation_digest=row.worker_generation_digest,
        attempt_index=row.attempt_index,
        expected_grant_id=row.expected_grant_id,
        expected_physical_epoch=row.expected_physical_epoch,
        replacement_operation_id=row.replacement_operation_id,
        state=cast(Literal["PREPARED", "PID_RECORDED", "CLOSED"], row.state),
        child_process_id=row.child_process_id,
        process_start_ticks=row.process_start_ticks,
        local_journal_digest=row.local_journal_digest,
        grant_id=row.grant_id,
    )


def launch_disposition(
    scope: DAGWorkerScope, row: orm_models.StreamDAGLaunchAttempt
) -> Literal["SPAWN_ALLOWED", "PREFLIGHT_ALLOWED", "STOP_REQUIRED"]:
    if (
        scope.graph.graph.current_control_operation_id is not None
        or scope.graph.graph.terminal_operation_id is not None
        or row.state == "CLOSED"
    ):
        return "STOP_REQUIRED"
    return "SPAWN_ALLOWED" if row.state == "PREPARED" else "PREFLIGHT_ALLOWED"


def launch_receipt(
    row: orm_models.StreamDAGLaunchAttempt,
    operation_id: UUID,
    kind: Literal["PREPARE", "RECORD"],
    request_digest: str,
) -> s.StreamDAGLaunchOperationReceipt:
    material = {
        "operation_id": str(operation_id),
        "launch_attempt_id": str(row.id),
        "kind": kind,
        "request_digest": request_digest,
        "accepted_at": now("UTC").isoformat(),
    }
    return lifecycle.make_dag_receipt(s.StreamDAGLaunchOperationReceipt, material)


async def locked_launch(
    session: AsyncSession,
    db: PrefectDBInterface,
    scope: DAGWorkerScope,
    launch_id: UUID,
) -> orm_models.StreamDAGLaunchAttempt:
    row = await session.scalar(
        sa.select(db.StreamDAGLaunchAttempt)
        .where(db.StreamDAGLaunchAttempt.id == launch_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    graph_scope = scope.graph
    if row is None or (
        row.graph_revision_id != graph_scope.graph.id
        or row.subtask_id != scope.subtask.id
        or row.task_run_id != scope.subtask.task_run_id
        or row.worker_id != scope.worker.id
        or row.worker_generation_digest
        != worker_generation(scope.worker.id, scope.principal)
        or row.start_operation_id != graph_scope.graph.admission_operation_id
        or (row.flow_generation, row.flow_holder_id, row.flow_run_run_count)
        != (
            graph_scope.authority.generation,
            graph_scope.authority.holder_id,
            graph_scope.flow.run_count,
        )
    ):
        raise NativeStreamError(
            "SUBTASK_SCOPE_DENIED",
            "Launch attempt is outside its current worker target",
        )
    return row


async def prepare_worker_stream_dag_launch(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamDAGLaunchPrepareRequest,
) -> s.StreamDAGLaunchOperationResult:
    scope = await lock_worker_subtask(
        session, db, worker_id, subtask_id, request.principal, all_workers=True
    )
    graph = scope.graph.graph
    expected_op = uuid5(
        subtask_id,
        "prefect-stream-dag-launch-v1/"
        + str(request.replacement_operation_id or request.start_operation_id),
    )
    if (
        request.operation_id != expected_op
        or request.start_operation_id != graph.admission_operation_id
    ):
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Launch preparation differs from its persisted start"
        )
    digest = lifecycle.dag_digest(request.model_dump(mode="json"))
    rows = list(
        (
            await session.scalars(
                sa.select(db.StreamDAGLaunchAttempt)
                .where(db.StreamDAGLaunchAttempt.subtask_id == subtask_id)
                .order_by(db.StreamDAGLaunchAttempt.attempt_index)
                .with_for_update()
            )
        ).all()
    )
    existing = next(
        (row for row in rows if row.prepare_operation_id == request.operation_id), None
    )
    if existing is not None:
        await locked_launch(session, db, scope, existing.id)
        if existing.prepare_request_digest != digest:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Launch preparation replay differs"
            )
        return s.StreamDAGLaunchOperationResult(
            receipt=s.StreamDAGLaunchOperationReceipt.model_validate(
                existing.prepare_receipt
            ),
            current=launch_projection(existing),
            disposition=launch_disposition(scope, existing),
        )
    lifecycle.require_mutable_execution(scope.graph)
    if scope.subtask.physical_slot_released:
        raise NativeStreamError(
            "SUBTASK_FENCED", "A released target cannot prepare another process birth"
        )
    if graph.current_control_operation_id is not None:
        raise NativeStreamError(
            "CONTROL_CONFLICT", "DAG is stopping; no new process birth is allowed"
        )
    if (
        scope.worker.status != WorkerStatus.ONLINE
        or scope.capability.last_renewed_at < now("UTC") - timedelta(seconds=120)
    ):
        raise NativeStreamError(
            "WORKER_NOT_READY", "New process birth requires a current worker"
        )
    expected_grant: UUID | None = None
    expected_epoch = 0
    if request.replacement_operation_id is not None:
        replacement = await session.scalar(
            sa.select(db.StreamSubtaskReplacement)
            .where(
                db.StreamSubtaskReplacement.subtask_id == subtask_id,
                db.StreamSubtaskReplacement.operation_id
                == request.replacement_operation_id,
            )
            .with_for_update()
        )
        if (
            replacement is None
            or replacement.state != "AUTHORIZED"
            or replacement.expected_grant_id != scope.subtask.current_grant_id
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT",
                "Replacement launch requires its current authorization",
            )
        expected_grant = scope.subtask.current_grant_id
        expected_epoch = scope.subtask.physical_epoch
        task = await session.scalar(
            sa.select(db.TaskRun)
            .where(db.TaskRun.id == scope.subtask.task_run_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        output = await session.scalar(
            sa.select(db.StreamPartition)
            .where(db.StreamPartition.id == scope.subtask.output_stream_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            task is None
            or task.flow_run_id != graph.flow_run_id
            or output is None
            or output.producer_task_run_id != task.id
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Replacement TaskRun or output identity differs"
            )
        if task.state_type in TERMINAL_STATES or output.state != "OPEN":
            raise NativeStreamError(
                "TERMINAL_RECONCILIATION_REQUIRED",
                "Terminal TaskRun or output requires verified retirement, not a new process",
            )
        if (
            task.run_count >= (task.empirical_policy.retries or 0) + 1
            or expected_epoch >= 64
        ):
            raise NativeStreamError(
                "RETRY_EXHAUSTED", "Physical TaskRun retry or epoch budget is exhausted"
            )
    elif scope.subtask.current_grant_id is not None or rows:
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Initial launch already has a process birth"
        )
    if (
        len(rows) >= 64
        or expected_epoch >= 64
        or [row.attempt_index for row in rows] != list(range(1, len(rows) + 1))
    ):
        raise NativeStreamError(
            "RETRY_EXHAUSTED", "DAG process birth budget is exhausted or incomplete"
        )
    if rows and any(row.state != "CLOSED" for row in rows):
        raise NativeStreamError(
            "PHYSICAL_STOP_REQUIRED",
            "Prior process birth must be closed before replacement",
        )
    row = db.StreamDAGLaunchAttempt(
        id=uuid7(),
        graph_revision_id=graph.id,
        subtask_id=subtask_id,
        task_run_id=scope.subtask.task_run_id,
        start_operation_id=request.start_operation_id,
        worker_id=worker_id,
        **request.principal.model_dump(),
        worker_generation_digest=worker_generation(worker_id, request.principal),
        flow_generation=scope.graph.authority.generation,
        flow_holder_id=scope.graph.authority.holder_id,
        flow_run_run_count=scope.graph.flow.run_count,
        attempt_index=len(rows) + 1,
        planned_physical_epoch=expected_epoch + 1,
        expected_grant_id=expected_grant,
        expected_physical_epoch=expected_epoch,
        replacement_operation_id=request.replacement_operation_id,
        state="PREPARED",
        prepare_operation_id=request.operation_id,
        prepare_request_digest=digest,
    )
    receipt = launch_receipt(row, request.operation_id, "PREPARE", digest)
    row.prepare_receipt = receipt.model_dump(mode="json")
    session.add(row)
    await session.flush()
    return s.StreamDAGLaunchOperationResult(
        receipt=receipt, current=launch_projection(row), disposition="SPAWN_ALLOWED"
    )


async def record_worker_stream_dag_launch(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    launch_attempt_id: UUID,
    request: s.StreamDAGLaunchRecordRequest,
) -> s.StreamDAGLaunchOperationResult:
    scope = await lock_worker_subtask(
        session, db, worker_id, subtask_id, request.principal
    )
    row = await locked_launch(session, db, scope, launch_attempt_id)
    if request.operation_id != uuid5(row.id, "prefect-stream-dag-record-v1"):
        raise NativeStreamError("IDENTITY_CONFLICT", "Launch record operation differs")
    digest = lifecycle.dag_digest(request.model_dump(mode="json"))
    if row.record_operation_id is not None:
        if (
            row.record_operation_id != request.operation_id
            or row.record_request_digest != digest
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Recorded process birth cannot change"
            )
        receipt = s.StreamDAGLaunchOperationReceipt.model_validate(row.record_receipt)
    else:
        if row.state != "PREPARED":
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Closed unstarted birth cannot record a PID"
            )
        receipt = launch_receipt(row, request.operation_id, "RECORD", digest)
        row.child_process_id = request.child_process_id
        row.process_start_ticks = request.process_start_ticks
        row.local_journal_digest = request.local_journal_digest
        row.record_operation_id = request.operation_id
        row.record_request_digest = digest
        row.record_receipt = receipt.model_dump(mode="json")
        row.state = "PID_RECORDED"
        await session.flush()
    return s.StreamDAGLaunchOperationResult(
        receipt=receipt,
        current=launch_projection(row),
        disposition=launch_disposition(scope, row),
    )


async def close_worker_stream_dag_launch(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    launch_attempt_id: UUID,
    request: s.StreamDAGLaunchCloseRequest,
) -> s.StreamDAGLaunchCloseReceipt:
    scope = await lock_worker_subtask(
        session, db, worker_id, subtask_id, request.principal
    )
    row = await locked_launch(session, db, scope, launch_attempt_id)
    if request.operation_id != uuid5(row.id, "prefect-stream-dag-close-v1"):
        raise NativeStreamError("IDENTITY_CONFLICT", "Launch close operation differs")
    digest = lifecycle.dag_digest(request.model_dump(mode="json"))
    if row.close_operation_id is not None:
        if (
            row.close_operation_id != request.operation_id
            or row.close_request_digest != digest
        ):
            raise NativeStreamError("IDENTITY_CONFLICT", "Launch close replay differs")
        return s.StreamDAGLaunchCloseReceipt.model_validate(row.close_receipt)
    if isinstance(request.proof, s.StreamDAGLaunchDeathProof):
        witness = request.proof.death_witness
        if (
            row.child_process_id is None
            or (
                witness.host_id,
                witness.boot_id,
                witness.child_process_id,
                witness.process_start_ticks,
            )
            != (row.host_id, row.boot_id, row.child_process_id, row.process_start_ticks)
            or witness.observed_at > now("UTC") + timedelta(seconds=30)
            or witness.observed_at < row.created
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT",
                "Death witness does not close this exact process birth",
            )
    else:
        if row.child_process_id is not None or row.state != "PREPARED":
            raise NativeStreamError(
                "PHYSICAL_STOP_REQUIRED",
                "A recorded process requires its exact death witness",
            )
        row.local_journal_digest = request.proof.local_journal_digest
    closed_at = now("UTC")
    material = {
        "operation_id": str(request.operation_id),
        "launch_attempt_id": str(row.id),
        "graph_revision_id": str(row.graph_revision_id),
        "subtask_id": str(subtask_id),
        "attempt_index": row.attempt_index,
        "proof_kind": request.proof.kind,
        "closed_at": closed_at.isoformat(),
        "request_digest": digest,
    }
    receipt = lifecycle.make_dag_receipt(s.StreamDAGLaunchCloseReceipt, material)
    row.close_operation_id = request.operation_id
    row.close_request_digest = digest
    row.close_proof_kind = request.proof.kind
    row.close_proof = request.proof.model_dump(mode="json")
    row.close_receipt = receipt.model_dump(mode="json")
    row.closed_at = cast(DateTime, closed_at)
    row.state = "CLOSED"
    await session.flush()
    return receipt


async def abort_worker_stream_dag_startup(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    graph_id: UUID,
    request: s.StreamDAGWorkerStartupAbortRequest,
) -> s.StreamDAGWorkerStartupAbortResult:
    await require_physical_backend(session, db)
    subtask_id = await session.scalar(
        sa.select(db.StreamPhysicalSubtask.id)
        .where(
            db.StreamPhysicalSubtask.graph_revision_id == graph_id,
            db.StreamPhysicalSubtask.assigned_worker_id == worker_id,
        )
        .order_by(db.StreamPhysicalSubtask.id)
        .limit(1)
    )
    if subtask_id is None:
        raise NativeStreamError(
            "SUBTASK_SCOPE_DENIED", "Worker has no target in this DAG"
        )
    scope = await lock_worker_subtask(
        session, db, worker_id, subtask_id, request.principal, all_workers=True
    )
    graph = scope.graph.graph
    if (
        request.start_operation_id != graph.admission_operation_id
        or request.graph_digest != graph.graph_digest
        or request.routing_digest != graph_routing_digest(graph.graph_digest)
    ):
        raise NativeStreamError("IDENTITY_CONFLICT", "Startup abort identity differs")
    if graph.startup_completed_at is not None:
        raise NativeStreamError(
            "ACTIVATION_ALREADY_COMMITTED", "Activation won the startup-abort race"
        )
    # No lower-row mutations precede the graph-wide control acquisition.
    control = await lifecycle.begin_dag_control(
        session, db, scope.graph, "STARTUP_ABORT", {"kind": "STARTUP_ABORT"}
    )
    return s.StreamDAGWorkerStartupAbortResult(
        control_operation_id=control.id,
        execution=await lifecycle.project_execution(session, db, scope.graph),
    )


async def poll_worker_stream_dag_subtasks(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    request: s.StreamDAGWorkerPollRequest,
) -> list[s.StreamDAGWorkerAssignment]:
    """Plain locator observations precede the sole Worker UPDATE advertisement."""
    await require_physical_backend(session, db)
    worker = await session.get(db.Worker, worker_id)
    if worker is None or worker.work_pool_id != request.principal.work_pool_id:
        raise NativeStreamError("SUBTASK_SCOPE_DENIED", "Worker pool differs")
    if request.active_subtask_ids:
        own = set(
            (
                await session.scalars(
                    sa.select(db.StreamPhysicalSubtask.id).where(
                        db.StreamPhysicalSubtask.id.in_(request.active_subtask_ids),
                        db.StreamPhysicalSubtask.assigned_worker_id == worker_id,
                        db.StreamPhysicalSubtask.profile == lifecycle.DAG_PROFILE,
                    )
                )
            ).all()
        )
        if own != set(request.active_subtask_ids):
            raise NativeStreamError(
                "SUBTASK_SCOPE_DENIED",
                "Active target set contains a foreign assignment",
            )
    rows = list(
        (
            await session.scalars(
                sa.select(db.StreamPhysicalSubtask)
                .join(
                    db.StreamGraphRevision,
                    db.StreamGraphRevision.id
                    == db.StreamPhysicalSubtask.graph_revision_id,
                )
                .where(
                    db.StreamPhysicalSubtask.assigned_worker_id == worker_id,
                    db.StreamPhysicalSubtask.profile == lifecycle.DAG_PROFILE,
                    db.StreamGraphRevision.admission_operation_id.is_not(None),
                    sa.or_(
                        db.StreamPhysicalSubtask.physical_slot_released.is_(False),
                        db.StreamPhysicalSubtask.id.in_(request.active_subtask_ids),
                    ),
                )
                .order_by(
                    sa.case(
                        (
                            db.StreamPhysicalSubtask.id.in_(request.active_subtask_ids),
                            0,
                        ),
                        else_=1,
                    ),
                    db.StreamPhysicalSubtask.id,
                )
                .limit(request.limit)
            )
        ).all()
    )
    result: list[s.StreamDAGWorkerAssignment] = []
    for subtask in rows:
        graph = await session.get(db.StreamGraphRevision, subtask.graph_revision_id)
        assert graph is not None
        flow = await session.get(db.FlowRun, graph.flow_run_id)
        authority = await session.scalar(
            sa.select(db.StreamExecutionAuthority).where(
                db.StreamExecutionAuthority.flow_run_id == graph.flow_run_id
            )
        )
        if (
            flow is None
            or authority is None
            or graph.placement_digest is None
            or graph.admission_operation_id is None
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Assigned graph authority is incomplete"
            )
        plan = await stream_dags.dag_admission_topology_witness(session, db, graph)
        projected = next(row for row in plan.subtasks if row.id == subtask.id)
        declaration = topology.StreamDAGSealRequest.model_validate(graph.declaration)
        spec = next(
            row
            for row in declaration.operators
            if row.operator_uid == projected.operator_uid
        )
        if declaration.working_directory is None:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Assigned graph workspace is missing"
            )
        graph_scope = lifecycle.DAGGraphScope(
            session, session.get_transaction(), flow, authority, graph, False
        )
        execution = await lifecycle.project_execution(session, db, graph_scope)
        task = await session.get(db.TaskRun, subtask.task_run_id)
        output = await session.get(db.StreamPartition, subtask.output_stream_id)
        if task is None or task.state_type is None or output is None:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Assigned TaskRun or output is missing"
            )
        grant = (
            await session.get(db.StreamSubtaskGrant, subtask.current_grant_id)
            if subtask.current_grant_id
            else None
        )
        attempts = list(
            (
                await session.scalars(
                    sa.select(db.StreamDAGLaunchAttempt)
                    .where(db.StreamDAGLaunchAttempt.subtask_id == subtask.id)
                    .order_by(db.StreamDAGLaunchAttempt.attempt_index)
                )
            ).all()
        )
        # Old advertisements or a different supervisor cannot inherit live work.
        if any(
            row.worker_generation_digest
            != worker_generation(worker_id, request.principal)
            for row in attempts
        ):
            raise NativeStreamError(
                "WORKER_IDENTITY_IN_USE",
                "Assigned launches bind another worker generation",
            )
        assignment = s.StreamDAGExecutionAssignment.model_validate(
            {
                **projected.model_dump(mode="json"),
                "graph_revision_id": graph.id,
                "current_grant_id": subtask.current_grant_id,
                "physical_epoch": subtask.physical_epoch,
                "physical_slot_released": subtask.physical_slot_released,
            }
        )
        result.append(
            s.StreamDAGWorkerAssignment(
                flow_run_id=graph.flow_run_id,
                graph_digest=graph.graph_digest,
                routing_digest=graph_routing_digest(graph.graph_digest),
                placement_digest=graph.placement_digest,
                start_operation_id=graph.admission_operation_id,
                assignment=assignment,
                phase=cast(
                    Literal["STARTING", "ACTIVE", "STOPPING", "TERMINAL"],
                    execution.phase,
                ),
                current_grant=grant_projection(grant, subtask) if grant else None,
                bootstrap_ready=grant is not None
                and grant.bootstrap_ready_at is not None,
                working_directory=declaration.working_directory,
                task_configuration=physical.StreamWorkerTaskConfiguration(
                    entrypoint=spec.entrypoint,
                    task_key=spec.task_key,
                    role=spec.role,
                    parameters=spec.parameters[subtask.subtask_index],
                    task_version=spec.task_version,
                    artifact_digest=spec.artifact_digest,
                    context_key=spec.context_key,
                    timeout_seconds=spec.timeout_seconds,
                    task_policy=spec.task_policy,
                    effective_tags=sorted(set(spec.tags)),
                    input_key=spec.input_key,
                ),
                task_state=task.state_type,
                task_run_count=task.run_count,
                task_retriable=task.state.state_details.retriable
                if task.state
                else None,
                output_state=cast(Literal["OPEN", "COMPLETED", "FAILED"], output.state),
                output_attempt=output.attempt,
                control=execution.control,
                launch_attempts=[launch_projection(row) for row in attempts],
            )
        )
    await renew_worker_capability_metadata(
        session, db, worker_id, request.principal, protocol=lifecycle.DAG_PROFILE
    )
    return result


async def claim_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: physical.StreamSubtaskClaimRequest,
) -> physical.StreamSubtaskGrant:
    if (
        request.launcher_binding_kind != "ASSIGNED_WORKER"
        or request.worker_id != worker_id
        or request.work_pool_id is None
    ):
        raise NativeStreamError(
            "SUBTASK_SCOPE_DENIED", "DAG claim requires its assigned worker"
        )
    principal = physical.StreamWorkerPrincipal(
        work_pool_id=request.work_pool_id,
        host_id=request.host_id,
        boot_id=request.boot_id,
        launcher_process_id=request.launcher_process_id,
        launcher_start_ticks=request.launcher_start_ticks,
    )
    scope = await lock_worker_subtask(session, db, worker_id, subtask_id, principal)
    digest = lifecycle.dag_digest(request.model_dump(mode="json"))
    grant = await session.scalar(
        sa.select(db.StreamSubtaskGrant)
        .where(
            db.StreamSubtaskGrant.subtask_id == subtask_id,
            db.StreamSubtaskGrant.operation_id == request.operation_id,
        )
        .with_for_update()
    )
    if grant is not None:
        if grant.request_digest != digest or grant.id != scope.subtask.current_grant_id:
            raise NativeStreamError("IDENTITY_CONFLICT", "DAG claim replay differs")
        linked = await session.scalar(
            sa.select(db.StreamDAGLaunchAttempt.id).where(
                db.StreamDAGLaunchAttempt.grant_id == grant.id
            )
        )
        if linked is None:
            raise NativeStreamError("SUBTASK_FENCED", "DAG grant has no launch link")
        return grant_projection(grant, scope.subtask)
    lifecycle.require_mutable_execution(scope.graph)
    if scope.subtask.physical_slot_released:
        raise NativeStreamError(
            "SUBTASK_FENCED", "A released target cannot claim another grant"
        )
    if scope.graph.graph.current_control_operation_id is not None:
        raise NativeStreamError("CONTROL_CONFLICT", "DAG stopping prevents a new claim")
    if (
        request.graph_digest != scope.graph.graph.graph_digest
        or request.assignment_epoch != scope.subtask.assignment_epoch
        or scope.subtask.current_grant_id is not None
    ):
        raise NativeStreamError("SUBTASK_FENCED", "Initial DAG claim identity differs")
    attempts = list(
        (
            await session.scalars(
                sa.select(db.StreamDAGLaunchAttempt)
                .where(
                    db.StreamDAGLaunchAttempt.subtask_id == subtask_id,
                    db.StreamDAGLaunchAttempt.expected_physical_epoch == 0,
                )
                .with_for_update()
            )
        ).all()
    )
    if (
        len(attempts) != 1
        or attempts[0].state != "PID_RECORDED"
        or attempts[0].grant_id is not None
    ):
        raise NativeStreamError(
            "SUBTASK_FENCED", "Claim requires one cap-free preflighted recorded launch"
        )
    attempt = await locked_launch(session, db, scope, attempts[0].id)
    grant = await create_subtask_grant(
        session, db, scope.graph.graph, scope.subtask, request, 1
    )
    attempt.grant_id = grant.id
    await session.flush()
    return grant_projection(grant, scope.subtask)


async def register_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: physical.StreamWorkerRegistrationRequest,
) -> physical.StreamSubtaskGrant:
    scope = await lock_worker_subtask(
        session, db, worker_id, subtask_id, request.principal
    )
    registration = request.registration
    grant = await session.scalar(
        sa.select(db.StreamSubtaskGrant)
        .where(db.StreamSubtaskGrant.id == registration.grant_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        grant is None
        or grant.id != scope.subtask.current_grant_id
        or grant.state not in {"CLAIMED", "REGISTERED"}
    ):
        raise NativeStreamError(
            "SUBTASK_FENCED", "Registration requires its current claim"
        )
    attempt_id = await session.scalar(
        sa.select(db.StreamDAGLaunchAttempt.id).where(
            db.StreamDAGLaunchAttempt.grant_id == grant.id
        )
    )
    if attempt_id is None:
        raise NativeStreamError(
            "SUBTASK_FENCED", "Registration requires a recorded launch"
        )
    attempt = await locked_launch(session, db, scope, attempt_id)
    if (registration.child_process_id, registration.process_start_ticks) != (
        attempt.child_process_id,
        attempt.process_start_ticks,
    ):
        raise NativeStreamError(
            "IDENTITY_CONFLICT",
            "Registration process differs from the immutable launch",
        )
    if grant.registration_operation_id is not None:
        if (
            grant.registration_operation_id != registration.operation_id
            or attempt.registered_grant_id != grant.id
        ):
            raise NativeStreamError("IDENTITY_CONFLICT", "Registration replay differs")
        return grant_projection(grant, scope.subtask)
    if (
        scope.graph.graph.current_control_operation_id is not None
        or attempt.state != "PID_RECORDED"
    ):
        raise NativeStreamError(
            "CONTROL_CONFLICT", "DAG stop fence prevents registration"
        )
    grant.registration_operation_id = registration.operation_id
    grant.child_process_id = registration.child_process_id
    grant.process_start_ticks = registration.process_start_ticks
    grant.state = "REGISTERED"
    grant.last_renewed_at = cast(DateTime, now("UTC"))
    await session.flush()
    attempt.registered_grant_id = grant.id
    await session.flush()
    return grant_projection(grant, scope.subtask)


async def renew_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: physical.StreamWorkerRenewRequest,
) -> physical.StreamSubtaskGrant:
    # Renewal updates the current grant while shared parent locks fence control.
    scope = await lock_worker_subtask(
        session, db, worker_id, subtask_id, request.principal, graph_update=False
    )
    grant = await session.scalar(
        sa.select(db.StreamSubtaskGrant)
        .where(db.StreamSubtaskGrant.id == request.grant_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        grant is None
        or grant.id != scope.subtask.current_grant_id
        or grant.state not in {"CLAIMED", "REGISTERED"}
        or scope.graph.graph.current_control_operation_id is not None
    ):
        raise NativeStreamError(
            "SUBTASK_FENCED", "DAG renewal requires the current unfenced grant"
        )
    grant.last_renewed_at = cast(DateTime, now("UTC"))
    await session.flush()
    return grant_projection(grant, scope.subtask)


async def stop_worker_stream_dag_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamDAGWorkerStopRequest,
) -> s.StreamDAGStopReceipt:
    scope = await lock_worker_subtask(
        session, db, worker_id, subtask_id, request.principal, all_workers=True
    )
    graph = scope.graph.graph
    if (
        graph.current_control_operation_id != request.control_operation_id
        or request.operation_id
        != uuid5(
            request.control_operation_id,
            "prefect-stream-dag-stop-v1/" + str(subtask_id),
        )
    ):
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "STOP differs from its current canonical control"
        )
    target = await session.scalar(
        sa.select(db.StreamDAGControlTarget)
        .where(
            db.StreamDAGControlTarget.control_operation_id
            == request.control_operation_id,
            db.StreamDAGControlTarget.subtask_id == subtask_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    control = await session.get(
        db.StreamDAGLifecycleOperation, request.control_operation_id
    )
    if (
        target is None
        or control is None
        or control.kind != "CONTROL"
        or control.graph_revision_id != graph.id
        or target.worker_id != worker_id
        or target.worker_generation_digest
        != worker_generation(worker_id, request.principal)
        or target.task_run_id != scope.subtask.task_run_id
        or (target.grant_id, target.physical_epoch)
        != (scope.subtask.current_grant_id, scope.subtask.physical_epoch)
    ):
        raise NativeStreamError(
            "SUBTASK_SCOPE_DENIED", "STOP is outside its frozen assigned control target"
        )
    digest = lifecycle.dag_digest(request.model_dump(mode="json"))
    if target.stop_operation_id is not None:
        if (
            target.stop_operation_id != request.operation_id
            or target.stop_request_digest != digest
        ):
            raise NativeStreamError("IDENTITY_CONFLICT", "STOP receipt replay differs")
        return s.StreamDAGStopReceipt.model_validate(target.stop_receipt)
    grant = (
        await session.scalar(
            sa.select(db.StreamSubtaskGrant)
            .where(db.StreamSubtaskGrant.id == target.grant_id)
            .with_for_update()
        )
        if target.grant_id
        else None
    )
    attempts = list(
        (
            await session.scalars(
                sa.select(db.StreamDAGLaunchAttempt)
                .where(db.StreamDAGLaunchAttempt.subtask_id == subtask_id)
                .order_by(db.StreamDAGLaunchAttempt.attempt_index)
                .with_for_update()
            )
        ).all()
    )
    if [row.attempt_index for row in attempts] != list(range(1, len(attempts) + 1)):
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Complete launch journal has a gap"
        )
    for attempt in attempts:
        await locked_launch(session, db, scope, attempt.id)
        if (
            attempt.state != "CLOSED"
            or attempt.close_receipt is None
            or attempt.close_request_digest is None
        ):
            raise NativeStreamError(
                "PHYSICAL_STOP_REQUIRED",
                "Every persisted process birth needs its exact closure",
            )
    if grant is not None and not any(
        row.grant_id == grant.id
        and row.registered_grant_id
        == (grant.id if grant.child_process_id is not None else None)
        for row in attempts
    ):
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Current grant has no exact closed launch"
        )
    all_closed = lifecycle.dag_digest(
        [
            {
                "launch_attempt_id": str(row.id),
                "attempt_index": row.attempt_index,
                "close_request_digest": row.close_request_digest,
                "close_receipt": row.close_receipt,
            }
            for row in attempts
        ]
    )
    task = await session.scalar(
        sa.select(db.TaskRun)
        .where(db.TaskRun.id == scope.subtask.task_run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if task is None or task.flow_run_id != scope.graph.flow.id:
        raise NativeStreamError("IDENTITY_CONFLICT", "STOP TaskRun scope differs")
    async with session.begin_nested():
        await lifecycle.terminate_controlled_task(
            session, db, scope.graph, control, task
        )
        await session.refresh(task)
        slot = await session.scalar(
            sa.select(db.StreamPhysicalSlotAllocation)
            .where(db.StreamPhysicalSlotAllocation.subtask_id == subtask_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            slot is None
            or slot.worker_id != worker_id
            or slot.graph_revision_id != graph.id
            or slot.released
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "STOP physical reservation differs"
            )
        await release_allocations(session, db, [subtask_id])
        slot.released = True
        slot.release_operation_id = request.operation_id
        slot.released_at = cast(DateTime, now("UTC"))
        scope.subtask.physical_slot_released = True
        if grant is not None:
            grant.state = "RETIRED"
        if task.state_type is None:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Stopped TaskRun has no terminal state"
            )
        receipt = lifecycle.make_dag_receipt(
            s.StreamDAGStopReceipt,
            {
                "operation_id": str(request.operation_id),
                "control_operation_id": str(control.id),
                "graph_revision_id": str(graph.id),
                "subtask_id": str(subtask_id),
                "task_run_id": str(task.id),
                "grant_id": str(target.grant_id) if target.grant_id else None,
                "physical_epoch": target.physical_epoch,
                "state": "STOPPED",
                "task_state": task.state_type.value,
                "tag_allocation_released": True,
                "physical_slot_released": True,
                "all_attempts_closed_digest": all_closed,
            },
        )
        target.state = "STOPPED"
        target.stop_operation_id = request.operation_id
        target.stop_request_digest = digest
        target.all_attempts_closed_digest = all_closed
        target.stop_receipt = receipt.model_dump(mode="json")
        target.stopped_at = cast(DateTime, now("UTC"))
        await session.flush()
        await lifecycle.finalize_stopped_dag_control(session, db, scope.graph, control)
    return receipt


async def worker_control_proof(
    session: AsyncSession,
    db: PrefectDBInterface,
    scope: DAGWorkerScope,
) -> DAGWorkerStreamAuthorization:
    grant = (
        await session.scalar(
            sa.select(db.StreamSubtaskGrant)
            .where(db.StreamSubtaskGrant.id == scope.subtask.current_grant_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if scope.subtask.current_grant_id
        else None
    )
    return DAGWorkerStreamAuthorization(
        flow_run_id=scope.graph.flow.id,
        graph_revision_id=scope.graph.graph.id,
        subtask_id=scope.subtask.id,
        worker_id=scope.worker.id,
        generation=scope.graph.authority.generation,
        holder_id=scope.graph.authority.holder_id,
        session=session,
        graph=scope.graph.graph,
        authority=scope.graph.authority,
        worker=scope.worker,
        subtask=scope.subtask,
        current_grant=grant,
        principal=physical.StreamWorkerPrincipal.model_validate(
            scope.principal.model_dump()
        ),
        graph_update=True,
        subtask_update=True,
    )


async def closed_attempt_set_digest(
    session: AsyncSession,
    db: PrefectDBInterface,
    scope: DAGWorkerScope,
) -> str:
    attempts = list(
        (
            await session.scalars(
                sa.select(db.StreamDAGLaunchAttempt)
                .where(db.StreamDAGLaunchAttempt.subtask_id == scope.subtask.id)
                .order_by(db.StreamDAGLaunchAttempt.attempt_index)
                .with_for_update()
            )
        ).all()
    )
    for row in attempts:
        await locked_launch(session, db, scope, row.id)
    return lifecycle.complete_closed_launch_digest(scope.graph, scope.subtask, attempts)


async def authorize_replacement(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: physical.StreamWorkerReplacementAuthorizeRequest,
) -> physical.StreamSubtaskReplacementAuthorization:
    scope = await lock_worker_subtask(
        session, db, worker_id, subtask_id, request.principal
    )
    proof = await worker_control_proof(session, db, scope)
    if proof.current_grant is None:
        raise NativeStreamError(
            "SUBTASK_FENCED", "Replacement requires its current registered grant"
        )
    linked = await session.scalar(
        sa.select(db.StreamDAGLaunchAttempt).where(
            db.StreamDAGLaunchAttempt.registered_grant_id == proof.current_grant.id
        )
    )
    if (
        linked is None
        or linked.subtask_id != subtask_id
        or linked.planned_physical_epoch != proof.current_grant.physical_epoch
    ):
        raise NativeStreamError(
            "SUBTASK_FENCED", "Replacement requires a launch-linked registered process"
        )
    return await physical_models.authorize_replacement(
        session, db, subtask_id, request.replacement, proof
    )


async def confirm_replacement(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: physical.StreamWorkerReplacementConfirmRequest,
) -> physical.StreamSubtaskGrant:
    scope = await lock_worker_subtask(
        session, db, worker_id, subtask_id, request.principal
    )
    proof = await worker_control_proof(session, db, scope)
    operation = request.replacement
    candidates = list(
        (
            await session.scalars(
                sa.select(db.StreamDAGLaunchAttempt)
                .where(
                    db.StreamDAGLaunchAttempt.subtask_id == subtask_id,
                    db.StreamDAGLaunchAttempt.replacement_operation_id
                    == operation.operation_id,
                )
                .with_for_update()
            )
        ).all()
    )
    if len(candidates) != 1:
        raise NativeStreamError(
            "SUBTASK_FENCED",
            "Replacement requires exactly one persisted candidate birth",
        )
    candidate = await locked_launch(session, db, scope, candidates[0].id)
    if candidate.state != "PID_RECORDED" or (
        candidate.child_process_id,
        candidate.process_start_ticks,
    ) != (operation.new_child_process_id, operation.new_process_start_ticks):
        raise NativeStreamError(
            "IDENTITY_CONFLICT",
            "Replacement process differs from its recorded candidate",
        )
    old = await session.scalar(
        sa.select(db.StreamDAGLaunchAttempt)
        .where(db.StreamDAGLaunchAttempt.grant_id == candidate.expected_grant_id)
        .with_for_update()
    )
    if old is None or old.state != "CLOSED":
        raise NativeStreamError(
            "PHYSICAL_STOP_REQUIRED", "Old process birth must close before replacement"
        )
    result = await physical_models.confirm_replacement(
        session, db, subtask_id, operation, proof
    )
    candidate.grant_id = result.id
    candidate.registered_grant_id = result.id
    await session.flush()
    return result


async def retire_verified_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: physical.StreamWorkerVerifiedRetireRequest,
) -> physical.StreamSubtaskVerifiedRetirement:
    scope = await lock_worker_subtask(
        session, db, worker_id, subtask_id, request.principal, all_workers=True
    )
    proof = await worker_control_proof(session, db, scope)
    closed_digest = await closed_attempt_set_digest(session, db, scope)
    operation = await session.scalar(
        sa.select(db.StreamSubtaskReplacement)
        .where(
            db.StreamSubtaskReplacement.subtask_id == subtask_id,
            db.StreamSubtaskReplacement.operation_id == request.retirement.operation_id,
        )
        .with_for_update()
    )
    if operation is None:
        raise NativeStreamError(
            "SUBTASK_AUTH_INVALID", "Retirement authorization is missing"
        )
    if operation.state == "RETIRED":
        envelope = operation.retirement_outcome
        if (
            envelope is None
            or envelope.get("all_attempts_closed_digest") != closed_digest
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Retirement complete launch set differs"
            )
        # The legacy core still authenticates exact challenge/body before replay.
        if operation.confirm_digest != lifecycle.dag_digest(
            request.retirement.model_dump(mode="json")
        ):
            raise NativeStreamError("IDENTITY_CONFLICT", "Retirement replay differs")
        if not hmac.compare_digest(
            operation.challenge_digest,
            capability_digest(request.retirement.challenge_capability),
        ):
            raise NativeStreamError(
                "SUBTASK_AUTH_INVALID", "Retirement challenge is invalid"
            )
        return physical.StreamSubtaskVerifiedRetirement.model_validate(
            envelope.get("receipt")
        )
    result = await physical_models.retire_verified_subtask(
        session, db, subtask_id, request.retirement, proof
    )
    operation.retirement_outcome = {
        "receipt": result.model_dump(mode="json"),
        "all_attempts_closed_digest": closed_digest,
    }
    if result.task_state == "COMPLETED":
        slot = await session.scalar(
            sa.select(db.StreamPhysicalSlotAllocation)
            .where(db.StreamPhysicalSlotAllocation.subtask_id == subtask_id)
            .with_for_update()
        )
        if slot is None or slot.released or scope.subtask.physical_slot_released:
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "Retirement physical reservation differs"
            )
        slot.released = True
        slot.release_operation_id = request.retirement.operation_id
        slot.released_at = cast(DateTime, now("UTC"))
        scope.subtask.physical_slot_released = True
        await session.flush()
        await lifecycle.finalize_completed_dag(session, db, scope.graph)
    else:
        await lifecycle.begin_dag_control(
            session,
            db,
            scope.graph,
            "FAIL",
            {
                "kind": "VERIFIED_PROCESS_FAILURE",
                "subtask_id": str(subtask_id),
                "outcome_kind": result.outcome_kind,
            },
        )
    await session.flush()
    return result
