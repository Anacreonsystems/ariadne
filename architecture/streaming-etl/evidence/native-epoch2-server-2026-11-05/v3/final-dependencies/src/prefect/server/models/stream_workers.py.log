"""Trusted worker gates over the existing physical stream control store."""

from datetime import timedelta
from typing import Any, cast
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models import stream_subtasks as core
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.models.stream_subtask_authority import (
    WorkerStreamAuthorization,
    require_physical_backend,
)
from prefect.server.schemas import stream_subtasks as s
from prefect.server.schemas.statuses import WorkerStatus
from prefect.types import DateTime
from prefect.types._datetime import now


def _principal_identity(
    principal: s.StreamWorkerPrincipal,
) -> tuple[UUID, UUID, UUID, int, int]:
    return (
        principal.work_pool_id,
        principal.host_id,
        principal.boot_id,
        principal.launcher_process_id,
        principal.launcher_start_ticks,
    )


def _capability_identity(
    capability: orm_models.StreamWorkerCapability,
) -> tuple[UUID, UUID, UUID, int, int]:
    return (
        capability.work_pool_id,
        capability.host_id,
        capability.boot_id,
        capability.launcher_process_id,
        capability.launcher_start_ticks,
    )


async def _renew_capability(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    principal: s.StreamWorkerPrincipal,
) -> None:
    """Advertise this protocol only after projection reads, without Flow locks."""
    worker = await session.scalar(
        sa.select(db.Worker)
        .where(db.Worker.id == worker_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        worker is None
        or worker.work_pool_id != principal.work_pool_id
        or worker.status != WorkerStatus.ONLINE
    ):
        raise NativeStreamError(
            "WORKER_NOT_READY", "A registered online worker in this pool is required"
        )
    capability = await session.scalar(
        sa.select(db.StreamWorkerCapability)
        .where(db.StreamWorkerCapability.worker_id == worker_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if capability is None or _capability_identity(capability) != _principal_identity(
        principal
    ):
        occupied = await session.scalar(
            sa.select(db.StreamPhysicalSubtask.id)
            .where(
                db.StreamPhysicalSubtask.assigned_worker_id == worker_id,
                db.StreamPhysicalSubtask.physical_slot_released.is_(False),
            )
            .limit(1)
        )
        if occupied is not None:
            raise NativeStreamError(
                "WORKER_IDENTITY_IN_USE",
                "Unreleased physical slots retain their exact worker identity",
            )
    if capability is None:
        capability = db.StreamWorkerCapability(worker_id=worker_id)
        session.add(capability)
    capability.work_pool_id = principal.work_pool_id
    capability.protocol = "partitioned-fixed-v1"
    capability.protocol_version = 1
    capability.host_id = principal.host_id
    capability.boot_id = principal.boot_id
    capability.launcher_process_id = principal.launcher_process_id
    capability.launcher_start_ticks = principal.launcher_start_ticks
    capability.last_renewed_at = cast(DateTime, now("UTC"))
    await session.flush()


async def poll_assignments(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    request: s.StreamWorkerPollRequest,
) -> list[s.StreamWorkerAssignment]:
    """Read bounded placement hints, then renew the worker's P2 advertisement."""
    await require_physical_backend(session, db)
    active = set(request.active_subtask_ids)
    if len(active) != len(request.active_subtask_ids) or len(active) > request.limit:
        raise NativeStreamError(
            "PROTOCOL_ERROR",
            "Active subtask identities must be distinct and fit the poll limit",
        )
    records = (
        await session.execute(
            sa.select(
                db.StreamPhysicalSubtask,
                db.StreamGraphRevision,
                db.StreamOperatorRun,
                db.TaskRun,
                db.StreamPartition,
            )
            .join(
                db.StreamGraphRevision,
                db.StreamGraphRevision.id == db.StreamPhysicalSubtask.graph_revision_id,
            )
            .join(
                db.StreamOperatorRun,
                db.StreamOperatorRun.id == db.StreamPhysicalSubtask.operator_run_id,
            )
            .join(db.TaskRun, db.TaskRun.id == db.StreamPhysicalSubtask.task_run_id)
            .join(
                db.StreamPartition,
                db.StreamPartition.id == db.StreamPhysicalSubtask.output_stream_id,
            )
            .where(
                db.StreamPhysicalSubtask.assigned_worker_id == worker_id,
                db.StreamGraphRevision.profile == "partitioned-fixed-v1",
                sa.or_(
                    sa.and_(
                        db.StreamPhysicalSubtask.physical_slot_released.is_(False),
                        db.StreamGraphRevision.state.in_(("ADMITTED", "ABORTING")),
                    ),
                    db.StreamPhysicalSubtask.id.in_(active),
                ),
            )
            .order_by(
                sa.case((db.StreamPhysicalSubtask.id.in_(active), 0), else_=1),
                db.StreamGraphRevision.created,
                db.StreamPhysicalSubtask.id,
            )
            .limit(request.limit)
            .execution_options(populate_existing=True)
        )
    ).all()
    results: list[s.StreamWorkerAssignment] = []
    for subtask, graph, operator, task, output in records:
        if task.state_type is None or output.state not in (
            "OPEN",
            "COMPLETED",
            "FAILED",
        ):
            raise NativeStreamError(
                "PROTOCOL_ERROR",
                "Physical assignment has no valid task/output observation",
            )
        if graph.placement_digest is None:
            raise NativeStreamError(
                "PROTOCOL_ERROR", "Assigned graph has no placement witness"
            )
        specification = s.StreamOperatorSpec.model_validate(operator.specification)
        partition_parameters = cast(
            dict[str, Any], specification.parameters[subtask.subtask_index]
        )
        working_directory = graph.declaration.get("working_directory")
        if not isinstance(working_directory, str):
            raise NativeStreamError(
                "PROTOCOL_ERROR", "Assigned graph has no frozen workspace"
            )
        grant = (
            await session.get(db.StreamSubtaskGrant, subtask.current_grant_id)
            if subtask.current_grant_id is not None
            else None
        )
        phase = (
            "RELEASED"
            if subtask.physical_slot_released or graph.state == "RELEASED"
            else "ABORTING"
            if graph.state == "ABORTING"
            else "ACTIVE"
            if graph.startup_completed_at is not None
            else "WAITING"
        )
        results.append(
            s.StreamWorkerAssignment(
                flow_run_id=graph.flow_run_id,
                graph_digest=graph.graph_digest,
                placement_digest=graph.placement_digest,
                assignment=await core.subtask_projection(session, db, subtask),
                phase=phase,
                abort_operation_id=graph.abort_operation_id,
                current_grant=core.grant_projection(grant, subtask)
                if grant is not None
                else None,
                working_directory=working_directory,
                task_configuration=s.StreamWorkerTaskConfiguration(
                    entrypoint=specification.entrypoint,
                    task_key=specification.task_key,
                    role=specification.role,
                    parameters=partition_parameters,
                    task_version=specification.task_version,
                    artifact_digest=specification.artifact_digest,
                    context_key=specification.context_key,
                    timeout_seconds=specification.timeout_seconds,
                    task_policy=specification.task_policy,
                    effective_tags=specification.tags,
                    input_key=specification.input_key,
                ),
                task_state=task.state_type,
                task_run_count=task.run_count,
                task_retriable=task.state.state_details.retriable
                if task.state is not None
                else None,
                output_state=output.state,
                output_attempt=output.attempt,
            )
        )
    # No Flow/Graph/Subtask ownership lock is acquired after this point. The
    # returned hints never authorize a later control operation.
    await _renew_capability(session, db, worker_id, request.principal)
    return results


async def _authorize_worker_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    principal: s.StreamWorkerPrincipal,
    *,
    terminal: bool = False,
    graph_update: bool = False,
) -> WorkerStreamAuthorization:
    """Acquire one authoritative prefix and issue an internal session-bound proof."""
    await require_physical_backend(session, db)
    locator = (
        await session.execute(
            sa.select(
                db.StreamPhysicalSubtask.graph_revision_id,
                db.StreamGraphRevision.flow_run_id,
            )
            .join(
                db.StreamGraphRevision,
                db.StreamGraphRevision.id == db.StreamPhysicalSubtask.graph_revision_id,
            )
            .where(db.StreamPhysicalSubtask.id == subtask_id)
        )
    ).one_or_none()
    if locator is None:
        raise NativeStreamError("NOT_FOUND", "Unknown physical subtask")
    await session.execute(
        sa.select(db.FlowRun.id)
        .where(db.FlowRun.id == locator.flow_run_id)
        .with_for_update(read=not graph_update)
    )
    authority = await session.scalar(
        sa.select(db.StreamExecutionAuthority)
        .where(db.StreamExecutionAuthority.flow_run_id == locator.flow_run_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    graph = await session.scalar(
        sa.select(db.StreamGraphRevision)
        .where(db.StreamGraphRevision.id == locator.graph_revision_id)
        .with_for_update(read=not graph_update)
        .execution_options(populate_existing=True)
    )
    if (
        authority is None
        or graph is None
        or authority.state
        not in (("ACTIVE", "TERMINAL", "RELEASED") if terminal else ("ACTIVE",))
        or authority.cancel_requested
        and not terminal
    ):
        raise NativeStreamError(
            "SUBTASK_FENCED", "Flow no longer permits this worker control"
        )
    worker_ids = {worker_id}
    if graph_update:
        worker_ids.update(
            worker
            for worker in await session.scalars(
                sa.select(db.StreamPhysicalSubtask.assigned_worker_id).where(
                    db.StreamPhysicalSubtask.graph_revision_id == graph.id,
                    db.StreamPhysicalSubtask.assigned_worker_id.is_not(None),
                )
            )
            if worker is not None
        )
    workers = {
        worker.id: worker
        for worker in await session.scalars(
            sa.select(db.Worker)
            .where(db.Worker.id.in_(worker_ids))
            .order_by(db.Worker.id)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
    }
    worker = workers.get(worker_id)
    capability = await session.scalar(
        sa.select(db.StreamWorkerCapability)
        .where(db.StreamWorkerCapability.worker_id == worker_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if (
        worker is None
        or worker.work_pool_id != principal.work_pool_id
        or capability is None
        or capability.protocol != "partitioned-fixed-v1"
        or capability.protocol_version != 1
        or _capability_identity(capability) != _principal_identity(principal)
    ):
        raise NativeStreamError(
            "SUBTASK_SCOPE_DENIED", "Worker registration does not match this supervisor"
        )
    subtask = await session.scalar(
        sa.select(db.StreamPhysicalSubtask)
        .where(db.StreamPhysicalSubtask.id == subtask_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        subtask is None
        or subtask.graph_revision_id != graph.id
        or subtask.assigned_worker_id != worker_id
        or subtask.assignment_epoch != graph.assignment_epoch
        or graph.profile != "partitioned-fixed-v1"
    ):
        raise NativeStreamError(
            "SUBTASK_SCOPE_DENIED", "Physical assignment belongs to another worker"
        )
    grant = (
        await session.scalar(
            sa.select(db.StreamSubtaskGrant)
            .where(db.StreamSubtaskGrant.id == subtask.current_grant_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if subtask.current_grant_id is not None
        else None
    )
    if subtask.current_grant_id is not None and (
        grant is None
        or grant.subtask_id != subtask.id
        or grant.physical_epoch != subtask.physical_epoch
        or grant.worker_id != worker_id
        or grant.launcher_binding_kind != "ASSIGNED_WORKER"
        or grant.flow_generation != authority.generation
        or grant.flow_holder_id != authority.holder_id
        or (
            grant.work_pool_id,
            grant.host_id,
            grant.boot_id,
            grant.launcher_process_id,
            grant.launcher_start_ticks,
        )
        != _principal_identity(principal)
    ):
        raise NativeStreamError(
            "SUBTASK_FENCED", "Current grant belongs to another exact supervisor"
        )
    return WorkerStreamAuthorization(
        flow_run_id=graph.flow_run_id,
        graph_revision_id=graph.id,
        subtask_id=subtask.id,
        worker_id=worker_id,
        generation=authority.generation,
        holder_id=authority.holder_id,
        session=session,
        graph=graph,
        authority=authority,
        worker=worker,
        subtask=subtask,
        current_grant=grant,
        principal=principal.model_copy(deep=True),
        graph_update=graph_update,
        subtask_update=True,
    )


async def claim_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamSubtaskClaimRequest,
) -> s.StreamSubtaskGrant:
    if (
        request.worker_id != worker_id
        or request.work_pool_id is None
        or request.launcher_binding_kind != "ASSIGNED_WORKER"
    ):
        raise NativeStreamError(
            "SUBTASK_SCOPE_DENIED", "Claim requires the assigned worker binding"
        )
    principal = s.StreamWorkerPrincipal(
        work_pool_id=request.work_pool_id,
        host_id=request.host_id,
        boot_id=request.boot_id,
        launcher_process_id=request.launcher_process_id,
        launcher_start_ticks=request.launcher_start_ticks,
    )
    scope = await _authorize_worker_subtask(
        session, db, worker_id, subtask_id, principal
    )
    capability = await session.scalar(
        sa.select(db.StreamWorkerCapability).where(
            db.StreamWorkerCapability.worker_id == worker_id
        )
    )
    if scope.current_grant is None and (
        scope.worker.status != WorkerStatus.ONLINE
        or capability is None
        or capability.last_renewed_at < now("UTC") - timedelta(seconds=120)
    ):
        raise NativeStreamError(
            "WORKER_NOT_READY", "New physical claims require a current online worker"
        )
    return await core.claim_subtask(session, db, subtask_id, request, scope)


async def register_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerRegistrationRequest,
) -> s.StreamSubtaskGrant:
    scope = await _authorize_worker_subtask(
        session, db, worker_id, subtask_id, request.principal
    )
    return await core.register_subtask(
        session, db, subtask_id, request.registration, scope
    )


async def renew_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerRenewRequest,
) -> s.StreamSubtaskGrant:
    scope = await _authorize_worker_subtask(
        session, db, worker_id, subtask_id, request.principal
    )
    return await core.renew_subtask(
        session,
        db,
        subtask_id,
        s.StreamSubtaskRenewRequest(grant_id=request.grant_id),
        scope,
    )


async def authorize_replacement(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerReplacementAuthorizeRequest,
) -> s.StreamSubtaskReplacementAuthorization:
    scope = await _authorize_worker_subtask(
        session, db, worker_id, subtask_id, request.principal, terminal=True
    )
    return await core.authorize_replacement(
        session, db, subtask_id, request.replacement, scope
    )


async def confirm_replacement(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerReplacementConfirmRequest,
) -> s.StreamSubtaskGrant:
    scope = await _authorize_worker_subtask(
        session, db, worker_id, subtask_id, request.principal
    )
    return await core.confirm_replacement(
        session, db, subtask_id, request.replacement, scope
    )


async def retire_verified_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerVerifiedRetireRequest,
) -> s.StreamSubtaskVerifiedRetirement:
    scope = await _authorize_worker_subtask(
        session, db, worker_id, subtask_id, request.principal, terminal=True
    )
    return await core.retire_verified_subtask(
        session, db, subtask_id, request.retirement, scope
    )


async def record_startup_stop(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerStartupStopRequest,
) -> s.StreamGraphStartupStopResult:
    scope = await _authorize_worker_subtask(
        session,
        db,
        worker_id,
        subtask_id,
        request.principal,
        terminal=True,
        graph_update=True,
    )
    return await core.acknowledge_startup_stop(
        session, db, subtask_id, request.stop, scope
    )


async def abort_startup(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    graph_id: UUID,
    request: s.StreamWorkerStartupAbortRequest,
) -> s.StreamGraphPlan:
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
            "SUBTASK_SCOPE_DENIED", "Graph has no assignment for this worker"
        )
    scope = await _authorize_worker_subtask(
        session,
        db,
        worker_id,
        subtask_id,
        request.principal,
        terminal=True,
        graph_update=True,
    )
    return await core.abort_graph(
        session,
        db,
        graph_id,
        s.StreamGraphAbortRequest(
            operation_id=core.startup_abort_operation_id(graph_id),
            graph_digest=scope.graph.graph_digest,
            error=core.STARTUP_ABORT_REASON,
        ),
        scope,
    )
