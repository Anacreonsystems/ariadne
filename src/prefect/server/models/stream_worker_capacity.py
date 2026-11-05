"""One physical occupancy view across fixed-P2 and topology-only DAG profiles."""

from collections import Counter
from collections.abc import Iterable
from datetime import timedelta
from typing import Literal, cast
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.database import PrefectDBInterface
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.schemas.statuses import WorkerStatus
from prefect.server.schemas.stream_subtasks import StreamWorkerPrincipal
from prefect.types import DateTime
from prefect.types._datetime import now


async def physical_worker_occupancy(
    session: AsyncSession, db: PrefectDBInterface, worker_ids: Iterable[UUID]
) -> dict[UUID, int]:
    """Count each reservation once after the caller locks Workers in UUID order.

    Fixed-P2 retains its accepted assigned/unreleased-row authority. DAG slots
    require durable allocation rows because tag limits can be absent. No
    heartbeat, elapsed time or capability advertisement releases either kind.
    """
    ids = set(worker_ids)
    if not ids:
        return {}
    subtasks = list(
        (
            await session.scalars(
                sa.select(db.StreamPhysicalSubtask)
                .where(db.StreamPhysicalSubtask.assigned_worker_id.in_(ids))
                .execution_options(populate_existing=True)
            )
        ).all()
    )
    allocations = list(
        (
            await session.scalars(
                sa.select(db.StreamPhysicalSlotAllocation)
                .where(db.StreamPhysicalSlotAllocation.worker_id.in_(ids))
                .execution_options(populate_existing=True)
            )
        ).all()
    )
    by_subtask = {value.subtask_id: value for value in allocations}
    indexed = {value.id: value for value in subtasks}
    counts: Counter[UUID] = Counter()
    for subtask in subtasks:
        worker_id = subtask.assigned_worker_id
        assert worker_id is not None
        if subtask.profile == "partitioned-fixed-v1":
            if subtask.id in by_subtask:
                raise NativeStreamError(
                    "IDENTITY_CONFLICT", "Fixed-P2 has an unexpected DAG reservation"
                )
            if not subtask.physical_slot_released:
                counts[worker_id] += 1
        elif subtask.profile == "partitioned-dag-v1":
            allocation = by_subtask.get(subtask.id)
            if allocation is None or (
                allocation.worker_id != worker_id
                or allocation.graph_revision_id != subtask.graph_revision_id
                or allocation.released != subtask.physical_slot_released
            ):
                raise NativeStreamError(
                    "IDENTITY_CONFLICT",
                    "DAG assignment lacks its exact durable slot reservation",
                )
        else:
            raise NativeStreamError(
                "UNSUPPORTED_PROFILE", "Unknown physical occupancy profile"
            )
    for allocation in allocations:
        subtask = indexed.get(allocation.subtask_id)
        if subtask is None or subtask.profile != "partitioned-dag-v1":
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "DAG reservation has no exact physical assignment"
            )
        if not allocation.released:
            counts[allocation.worker_id] += 1
    return dict(counts)


async def renew_worker_capability_metadata(
    session: AsyncSession,
    db: PrefectDBInterface,
    worker_id: UUID,
    principal: StreamWorkerPrincipal,
    *,
    protocol: Literal["partitioned-fixed-v1", "partitioned-dag-v1"],
) -> None:
    """Renew one protocol; rotate every old identity only with zero occupancy.

    Other protocols are expired on rotation. A new supervisor cannot inherit a
    fresh protocol advertisement it has not explicitly renewed itself.
    """
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
            "WORKER_NOT_READY",
            "A registered online worker in the requested pool is required",
        )
    rows = list(
        (
            await session.scalars(
                sa.select(db.StreamWorkerCapability)
                .where(db.StreamWorkerCapability.worker_id == worker_id)
                .order_by(
                    db.StreamWorkerCapability.protocol,
                    db.StreamWorkerCapability.protocol_version,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).all()
    )
    identity = (
        principal.work_pool_id,
        principal.host_id,
        principal.boot_id,
        principal.launcher_process_id,
        principal.launcher_start_ticks,
    )
    different = any(
        (
            row.work_pool_id,
            row.host_id,
            row.boot_id,
            row.launcher_process_id,
            row.launcher_start_ticks,
        )
        != identity
        for row in rows
    )
    current_time = cast(DateTime, now("UTC"))
    if different:
        if (await physical_worker_occupancy(session, db, [worker_id])).get(
            worker_id, 0
        ):
            raise NativeStreamError(
                "WORKER_IDENTITY_IN_USE",
                "Unreleased physical reservations retain their exact supervisor identity",
            )
        for row in rows:
            (
                row.work_pool_id,
                row.host_id,
                row.boot_id,
                row.launcher_process_id,
                row.launcher_start_ticks,
            ) = identity
            if row.protocol != protocol:
                row.last_renewed_at = current_time - timedelta(seconds=121)
    row = next(
        (
            item
            for item in rows
            if item.protocol == protocol and item.protocol_version == 1
        ),
        None,
    )
    if row is None:
        row = db.StreamWorkerCapability(
            worker_id=worker_id,
            work_pool_id=principal.work_pool_id,
            protocol=protocol,
            protocol_version=1,
            host_id=principal.host_id,
            boot_id=principal.boot_id,
            launcher_process_id=principal.launcher_process_id,
            launcher_start_ticks=principal.launcher_start_ticks,
            last_renewed_at=current_time,
        )
        session.add(row)
    else:
        row.last_renewed_at = current_time
    await session.flush()
