"""Least-privilege physical authority shared by the store and event recorder."""

import hashlib
import hmac
from collections import Counter
from dataclasses import dataclass, field
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.database import PrefectDBInterface, orm_models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.schemas.states import StateType
from prefect.server.schemas.stream_subtasks import (
    StreamSubtaskCredentials,
    StreamWorkerPrincipal,
)
from prefect.settings import PREFECT_SERVER_API_AUTH_STRING


@dataclass(frozen=True)
class WorkerStreamAuthorization:
    """Internal proof created only after the trusted worker API gate."""

    flow_run_id: UUID
    graph_revision_id: UUID
    subtask_id: UUID
    worker_id: UUID
    generation: int
    holder_id: UUID
    session: AsyncSession = field(repr=False)
    graph: orm_models.StreamGraphRevision = field(repr=False)
    authority: orm_models.StreamExecutionAuthority = field(repr=False)
    worker: orm_models.Worker = field(repr=False)
    subtask: orm_models.StreamPhysicalSubtask = field(repr=False)
    current_grant: orm_models.StreamSubtaskGrant | None = field(repr=False)
    principal: StreamWorkerPrincipal = field(repr=False)
    graph_update: bool = False
    subtask_update: bool = False
    principal_identity: tuple[UUID, UUID, UUID, int, int] = field(
        init=False, repr=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "principal_identity",
            (
                self.principal.work_pool_id,
                self.principal.host_id,
                self.principal.boot_id,
                self.principal.launcher_process_id,
                self.principal.launcher_start_ticks,
            ),
        )

    def validate(
        self,
        session: AsyncSession,
        *,
        flow_run_id: UUID | None = None,
        graph_revision_id: UUID | None = None,
        subtask_id: UUID | None = None,
    ) -> None:
        identity = (
            self.principal.work_pool_id,
            self.principal.host_id,
            self.principal.boot_id,
            self.principal.launcher_process_id,
            self.principal.launcher_start_ticks,
        )
        if (
            identity != self.principal_identity
            or self.worker.work_pool_id != self.principal.work_pool_id
            or (
                self.current_grant is not None
                and (
                    self.current_grant.worker_id != self.worker_id
                    or (
                        self.current_grant.work_pool_id,
                        self.current_grant.host_id,
                        self.current_grant.boot_id,
                        self.current_grant.launcher_process_id,
                        self.current_grant.launcher_start_ticks,
                    )
                    != identity
                )
            )
        ):
            raise NativeStreamError(
                "SUBTASK_SCOPE_DENIED", "Prelocked worker principal identity drifted"
            )
        if (
            session is not self.session
            or self.graph.flow_run_id != self.flow_run_id
            or self.graph.id != self.graph_revision_id
            or self.subtask.id != self.subtask_id
            or self.subtask.graph_revision_id != self.graph_revision_id
            or self.subtask.assigned_worker_id != self.worker_id
            or self.worker.id != self.worker_id
            or self.authority.flow_run_id != self.flow_run_id
            or self.authority.generation != self.generation
            or self.authority.holder_id != self.holder_id
            or (flow_run_id is not None and flow_run_id != self.flow_run_id)
            or (
                graph_revision_id is not None
                and graph_revision_id != self.graph_revision_id
            )
            or (subtask_id is not None and subtask_id != self.subtask_id)
            or ((self.current_grant is None) != (self.subtask.current_grant_id is None))
            or (
                self.current_grant is not None
                and self.current_grant.id != self.subtask.current_grant_id
            )
        ):
            raise NativeStreamError(
                "SUBTASK_SCOPE_DENIED",
                "Prelocked worker authority does not match this session or resource",
            )


async def require_physical_backend(
    session: AsyncSession, db: PrefectDBInterface
) -> None:
    connection = await session.connection()
    if (
        db.dialect.name != "postgresql"
        or not connection.dialect.server_version_info
        or connection.dialect.server_version_info[0] != 17
    ):
        raise NativeStreamError(
            "UNSUPPORTED_BACKEND", "Physical streams require PostgreSQL 17"
        )
    if not PREFECT_SERVER_API_AUTH_STRING.value():
        raise NativeStreamError(
            "UNPROTECTED_API",
            "Physical streams require a protected API authentication boundary",
        )


def capability_digest(capability: str) -> str:
    return hashlib.sha256(
        b"prefect-subtask-v1\0" + capability.encode("ascii")
    ).hexdigest()


async def require_subtask(
    session: AsyncSession,
    db: PrefectDBInterface,
    credentials: StreamSubtaskCredentials,
    *,
    subtask_id: UUID | None = None,
    flow_run_id: UUID | None = None,
    task_run_id: UUID | None = None,
    stream_id: UUID | None = None,
    require_registered: bool = True,
    grant_update: bool = False,
) -> tuple[orm_models.StreamPhysicalSubtask, orm_models.StreamSubtaskGrant]:
    """Validate scope in the same transaction as the protected mutation.

    Shared locks on graph and subtask prevent a replacement/abort CAS from
    passing validation while an old attempt writes. Flow shared lock preserves
    the existing whole-Flow cancellation fence.
    """
    await require_physical_backend(session, db)
    grant = await session.get(db.StreamSubtaskGrant, credentials.grant_id)
    if grant is None or not hmac.compare_digest(
        grant.capability_digest, capability_digest(credentials.capability)
    ):
        raise NativeStreamError("SUBTASK_AUTH_INVALID", "Invalid subtask credential")
    subtask = await session.get(db.StreamPhysicalSubtask, grant.subtask_id)
    if subtask is None:
        raise NativeStreamError("SUBTASK_FENCED", "Physical subtask no longer exists")
    graph = await session.get(db.StreamGraphRevision, subtask.graph_revision_id)
    if graph is None:
        raise NativeStreamError("SUBTASK_FENCED", "Graph no longer exists")
    await session.execute(
        sa.select(db.FlowRun.id)
        .where(db.FlowRun.id == graph.flow_run_id)
        .with_for_update(read=True)
    )
    authority = await session.scalar(
        sa.select(db.StreamExecutionAuthority)
        .where(db.StreamExecutionAuthority.flow_run_id == graph.flow_run_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    graph = await session.scalar(
        sa.select(db.StreamGraphRevision)
        .where(db.StreamGraphRevision.id == graph.id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    subtask = await session.scalar(
        sa.select(db.StreamPhysicalSubtask)
        .where(db.StreamPhysicalSubtask.id == grant.subtask_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    grant = await session.scalar(
        sa.select(db.StreamSubtaskGrant)
        .where(db.StreamSubtaskGrant.id == credentials.grant_id)
        .with_for_update(read=not grant_update)
        .execution_options(populate_existing=True)
    )
    assert subtask is not None and grant is not None and graph is not None
    if authority is None or authority.state != "ACTIVE" or authority.cancel_requested:
        raise NativeStreamError(
            "CANCEL_REQUESTED", "Flow execution no longer permits physical work"
        )
    if (
        grant.flow_generation != authority.generation
        or grant.flow_holder_id != authority.holder_id
    ):
        raise NativeStreamError(
            "SUBTASK_FENCED", "Parent Flow execution no longer owns this subtask"
        )
    if (
        graph.state != "ADMITTED"
        or subtask.assignment_epoch != graph.assignment_epoch
        or subtask.current_grant_id != grant.id
        or subtask.physical_epoch != grant.physical_epoch
        or grant.state
        not in (("REGISTERED",) if require_registered else ("CLAIMED", "REGISTERED"))
    ):
        raise NativeStreamError(
            "SUBTASK_FENCED",
            "Subtask assignment or physical attempt is no longer current",
        )
    if graph.startup_completed_at is None:
        raise NativeStreamError(
            "METADATA_NOT_READY", "Runnable physical region has not been activated"
        )
    if (
        (subtask_id is not None and subtask.id != subtask_id)
        or (flow_run_id is not None and graph.flow_run_id != flow_run_id)
        or (task_run_id is not None and subtask.task_run_id != task_run_id)
        or (
            stream_id is not None
            and stream_id not in (subtask.input_stream_id, subtask.output_stream_id)
        )
    ):
        raise NativeStreamError(
            "SUBTASK_SCOPE_DENIED", "Resource is outside the physical subtask scope"
        )
    return subtask, grant


async def require_parent_subtask(
    session: AsyncSession, db: PrefectDBInterface, flow_run_id: UUID, task_run_id: UUID
) -> orm_models.StreamPhysicalSubtask | None:
    """Parent terminal-output reconciliation after the Flow authority gate."""
    graph = await session.scalar(
        sa.select(db.StreamGraphRevision)
        .where(db.StreamGraphRevision.flow_run_id == flow_run_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if graph is None:
        return None
    await require_physical_backend(session, db)
    if graph.state != "ADMITTED" or graph.startup_completed_at is None:
        raise NativeStreamError(
            "STARTUP_ABORT_REQUIRED", "Unactivated graph requires startup compensation"
        )
    subtask = await session.scalar(
        sa.select(db.StreamPhysicalSubtask)
        .where(
            db.StreamPhysicalSubtask.graph_revision_id == graph.id,
            db.StreamPhysicalSubtask.task_run_id == task_run_id,
        )
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if subtask is None:
        raise NativeStreamError(
            "SUBTASK_SCOPE_DENIED", "TaskRun is outside this physical graph"
        )
    if subtask.current_grant_id is not None:
        await session.execute(
            sa.select(db.StreamSubtaskGrant)
            .where(db.StreamSubtaskGrant.id == subtask.current_grant_id)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
    return subtask


async def release_allocations(
    session: AsyncSession, db: PrefectDBInterface, subtask_ids: list[UUID]
) -> None:
    """Exactly-once release under durable terminal/abort authority, never TTL."""
    limit_ids = list(
        (
            await session.scalars(
                sa.select(db.StreamSubtaskAllocation.concurrency_limit_id)
                .where(
                    db.StreamSubtaskAllocation.subtask_id.in_(subtask_ids),
                    db.StreamSubtaskAllocation.released.is_(False),
                )
                .distinct()
            )
        ).all()
    )
    limits = {
        limit.id: limit
        for limit in (
            await session.scalars(
                sa.select(db.ConcurrencyLimitV2)
                .where(db.ConcurrencyLimitV2.id.in_(limit_ids))
                .order_by(db.ConcurrencyLimitV2.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).all()
    }
    allocations = (
        await session.scalars(
            sa.select(db.StreamSubtaskAllocation)
            .where(
                db.StreamSubtaskAllocation.subtask_id.in_(subtask_ids),
                db.StreamSubtaskAllocation.released.is_(False),
            )
            .order_by(
                db.StreamSubtaskAllocation.concurrency_limit_id,
                db.StreamSubtaskAllocation.subtask_id,
            )
            .with_for_update()
        )
    ).all()
    counts = Counter(row.concurrency_limit_id for row in allocations)
    for limit_id in sorted(counts, key=str):
        limit = limits.get(limit_id)
        if limit is not None:
            limit.active_slots = max(0, limit.active_slots - counts[limit_id])
    for allocation in allocations:
        allocation.released = True


async def release_terminal_allocation(
    session: AsyncSession,
    db: PrefectDBInterface,
    subtask: orm_models.StreamPhysicalSubtask,
) -> bool:
    """A retryable failed attempt retains its runnable region allocation."""
    task = await session.scalar(
        sa.select(db.TaskRun)
        .where(db.TaskRun.id == subtask.task_run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    output = await session.scalar(
        sa.select(db.StreamPartition)
        .where(db.StreamPartition.id == subtask.output_stream_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if task is None or output is None or output.producer_task_run_id != task.id:
        return False
    completed = (
        task.state_type == StateType.COMPLETED
        and output.state == "COMPLETED"
        and output.attempt == task.run_count
    )
    unowned = (
        output.owner_id is None and output.owner_epoch == 0 and output.attempt == 0
    )
    if unowned:
        unowned = not await session.scalar(
            sa.select(sa.exists().where(db.StreamRecord.stream_id == output.id))
        ) and not await session.scalar(
            sa.select(
                sa.exists().where(db.StreamConsumer.output_stream_id == output.id)
            )
        )
    exhausted = (
        task.state_type in (StateType.FAILED, StateType.CRASHED)
        and (
            task.run_count >= (task.empirical_policy.retries or 0) + 1
            or task.state is not None
            and task.state.state_details.retriable is False
        )
        and output.state == "FAILED"
        and (output.attempt == task.run_count or unowned)
    )
    if not (completed or exhausted):
        return False
    await release_allocations(session, db, [subtask.id])
    return True


async def require_no_assigned_physical_slots(
    session: AsyncSession, db: PrefectDBInterface, flow_run_id: UUID
) -> None:
    occupied = await session.scalar(
        sa.select(db.StreamPhysicalSubtask.id)
        .join(
            db.StreamGraphRevision,
            db.StreamGraphRevision.id == db.StreamPhysicalSubtask.graph_revision_id,
        )
        .where(
            db.StreamGraphRevision.flow_run_id == flow_run_id,
            db.StreamPhysicalSubtask.assigned_worker_id.is_not(None),
            db.StreamPhysicalSubtask.physical_slot_released.is_(False),
        )
        .limit(1)
    )
    if occupied is not None:
        raise NativeStreamError(
            "PHYSICAL_STOP_REQUIRED",
            "Assigned-worker cancellation requires a verified physical stop protocol",
        )


async def require_bounded_flow_recovery(
    session: AsyncSession, db: PrefectDBInterface, flow_run_id: UUID
) -> None:
    profile = await session.scalar(
        sa.select(db.NativeStreamDeploymentSnapshot.profile).where(
            db.NativeStreamDeploymentSnapshot.flow_run_id == flow_run_id
        )
    )
    graph = await session.scalar(
        sa.select(db.StreamGraphRevision.id).where(
            db.StreamGraphRevision.flow_run_id == flow_run_id
        )
    )
    if profile == "partitioned-fixed-v1" or graph is not None:
        raise NativeStreamError(
            "UNSUPPORTED_PROFILE",
            "Partitioned physical children cannot use bounded whole-Flow recovery",
        )
