"""Keep legacy prefix envelopes outside DAG direct-reference channels."""

from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.database import PrefectDBInterface
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.models.stream_executions import require_execution
from prefect.server.models.stream_subtask_authority import (
    require_physical_backend,
    require_subtask,
)
from prefect.server.schemas.stream_subtasks import StreamSubtaskCredentials
from prefect.server.schemas.streams import StreamExecutionCredentials


async def guard_legacy_stream_prefix(
    session: AsyncSession,
    db: PrefectDBInterface,
    stream_id: UUID,
    execution: StreamExecutionCredentials | StreamSubtaskCredentials | None,
) -> None:
    """DAG prefixes authenticate and refuse before any stream/record row lock.

    Both producer and edge locators are immutable topology metadata. A zero-DAG
    locator result preserves the legacy body, authorization and locking path.
    """
    producer = (
        sa.select(
            db.StreamGraphRevision.id,
            db.StreamGraphRevision.flow_run_id,
            db.StreamGraphRevision.profile,
        )
        .join(
            db.StreamPhysicalSubtask,
            db.StreamPhysicalSubtask.graph_revision_id == db.StreamGraphRevision.id,
        )
        .where(db.StreamPhysicalSubtask.output_stream_id == stream_id)
    )
    edge = (
        sa.select(
            db.StreamGraphRevision.id,
            db.StreamGraphRevision.flow_run_id,
            db.StreamGraphRevision.profile,
        )
        .join(
            db.StreamGraphEdgePartition,
            db.StreamGraphEdgePartition.graph_revision_id == db.StreamGraphRevision.id,
        )
        .where(db.StreamGraphEdgePartition.stream_id == stream_id)
    )
    mappings = (await session.execute(sa.union_all(producer, edge))).all()
    if not any(profile == "partitioned-dag-v1" for _, _, profile in mappings):
        return
    if len(mappings) != 1:
        raise NativeStreamError(
            "IDENTITY_CONFLICT", "Stream has ambiguous DAG topology bindings"
        )
    graph_id, flow_id, _ = mappings[0]
    await require_physical_backend(session, db)
    if isinstance(execution, StreamSubtaskCredentials):
        await require_subtask(session, db, execution, stream_id=stream_id)
    else:
        flow = await session.scalar(
            sa.select(db.FlowRun)
            .where(db.FlowRun.id == flow_id)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        if flow is None:
            raise NativeStreamError("NOT_FOUND", "DAG FlowRun no longer exists")
        # Refresh under the ordered Flow -> authority prefix before validating
        # credentials, including a caller with an older identity-map snapshot.
        await session.scalar(
            sa.select(db.StreamExecutionAuthority)
            .where(db.StreamExecutionAuthority.flow_run_id == flow_id)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        authority = await require_execution(
            session,
            db,
            flow_id,
            execution,
            allowed_states=("CLAIMED", "ACTIVE", "RECOVERING", "TERMINAL", "RELEASED"),
        )
        if authority is None:
            raise NativeStreamError(
                "EXECUTION_BINDING_REQUIRED",
                "DAG prefix requires its current execution binding",
            )
        graph = await session.scalar(
            sa.select(db.StreamGraphRevision)
            .where(db.StreamGraphRevision.id == graph_id)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        if (
            graph is None
            or graph.flow_run_id != flow_id
            or graph.profile != "partitioned-dag-v1"
        ):
            raise NativeStreamError(
                "IDENTITY_CONFLICT", "DAG prefix topology changed during authorization"
            )
    raise NativeStreamError(
        "EXECUTION_PROFILE_NOT_READY",
        "Legacy prefix envelopes cannot expose DAG payload/reference channels",
    )
