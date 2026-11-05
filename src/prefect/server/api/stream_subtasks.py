"""Native physical topology control and narrowly scoped child bootstrap."""

from typing import Annotated, cast
from uuid import UUID

from fastapi import Depends, HTTPException, status

from prefect.server.api.native_deployments import SecretRedactingRoute
from prefect.server.api.stream_execution_dependencies import ExecutionCredentials
from prefect.server.api.stream_subtask_dependencies import SubtaskCredentials
from prefect.server.api.streams import stream_http_error
from prefect.server.database import PrefectDBInterface, provide_database_interface
from prefect.server.events.schemas.events import Event
from prefect.server.models import stream_subtasks as models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.models.stream_subtask_authority import require_subtask
from prefect.server.models.streams import canonical_stream_payload
from prefect.server.schemas import stream_subtasks as s
from prefect.server.schemas.core import TaskRun
from prefect.server.services.task_run_recorder import (
    authorized_stream_events,
    record_bulk_task_run_events_in_session,
)
from prefect.server.utilities.server import PrefectRouter
from prefect.types._datetime import DateTime, now

Database = Annotated[PrefectDBInterface, Depends(provide_database_interface)]
router: PrefectRouter = PrefectRouter(
    prefix="/streams/v2", tags=["Streams"], route_class=SecretRedactingRoute
)


@router.post("/graphs/seal")
async def seal_graph(
    request: s.StreamGraphSealRequest, db: Database, execution: ExecutionCredentials
) -> s.StreamGraphPlan:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.seal_graph(session, db, request, execution)
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.get("/graphs/{graph_id:uuid}")
async def read_graph(
    graph_id: UUID, db: Database, execution: ExecutionCredentials
) -> s.StreamGraphPlan:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.read_graph(session, db, graph_id, execution)
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/graphs/{graph_id:uuid}/admit")
async def admit_region(
    graph_id: UUID,
    request: s.StreamRegionAdmissionRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamGraphPlan:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.admit_region(session, db, graph_id, request, execution)
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/graphs/{graph_id:uuid}/activate")
async def activate_graph(
    graph_id: UUID,
    request: s.StreamGraphActivateRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamGraphPlan:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.activate_graph(
                session, db, graph_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/graphs/{graph_id:uuid}/abort")
async def abort_graph(
    graph_id: UUID,
    request: s.StreamGraphAbortRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamGraphPlan:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.abort_graph(session, db, graph_id, request, execution)
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/subtasks/{subtask_id:uuid}/claim")
async def claim_subtask(
    subtask_id: UUID,
    request: s.StreamSubtaskClaimRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamSubtaskGrant:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.claim_subtask(
                session, db, subtask_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/subtasks/{subtask_id:uuid}/register")
async def register_subtask(
    subtask_id: UUID,
    request: s.StreamSubtaskRegistrationRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamSubtaskGrant:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.register_subtask(
                session, db, subtask_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/subtasks/{subtask_id:uuid}/renew")
async def renew_subtask(
    subtask_id: UUID,
    request: s.StreamSubtaskRenewRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamSubtaskGrant:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.renew_subtask(
                session, db, subtask_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/subtasks/{subtask_id:uuid}/replacements/authorize")
async def authorize_replacement(
    subtask_id: UUID,
    request: s.StreamSubtaskReplacementAuthorizeRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamSubtaskReplacementAuthorization:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.authorize_replacement(
                session, db, subtask_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/subtasks/{subtask_id:uuid}/replacements/confirm")
async def confirm_replacement(
    subtask_id: UUID,
    request: s.StreamSubtaskReplacementConfirmRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamSubtaskGrant:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.confirm_replacement(
                session, db, subtask_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.get("/subtasks/{subtask_id:uuid}/bootstrap")
async def bootstrap(
    subtask_id: UUID, db: Database, credentials: SubtaskCredentials
) -> s.StreamSubtaskBootstrap:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.bootstrap_subtask(session, db, subtask_id, credentials)
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.get("/subtasks/{subtask_id:uuid}/task-run")
async def read_task_run(
    subtask_id: UUID, db: Database, credentials: SubtaskCredentials
) -> TaskRun:
    return (await bootstrap(subtask_id, db, credentials)).task_run


@router.post("/subtasks/{subtask_id:uuid}/events")
async def task_event(
    subtask_id: UUID, event: Event, db: Database, credentials: SubtaskCredentials
) -> s.StreamSubtaskEventResult:
    try:
        canonical_stream_payload(event.model_dump(mode="json"), 256 * 1024)
        async with db.session_context(begin_transaction=True) as session:
            subtask, _ = await require_subtask(
                session, db, credentials, subtask_id=subtask_id
            )
            if (
                event.resource.prefect_object_id("prefect.task-run")
                != subtask.task_run_id
            ):
                raise NativeStreamError(
                    "SUBTASK_SCOPE_DENIED", "Event TaskRun is outside the grant"
                )
            authorized, native_ids = await authorized_stream_events(
                session, db, [event.receive()]
            )
            if authorized:
                await record_bulk_task_run_events_in_session(
                    authorized, db, session, native_ids, commit=False
                )
            task = await session.get(
                db.TaskRun, subtask.task_run_id, populate_existing=True
            )
            assert task is not None and task.state_type is not None
            return s.StreamSubtaskEventResult(
                event_id=event.id,
                accepted=bool(authorized),
                task_run_id=task.id,
                run_count=task.run_count,
                state_type=task.state_type,
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error
    except (KeyError, ValueError) as error:
        raise HTTPException(422, detail="Invalid physical TaskRun event") from error


@router.post(
    "/subtasks/{subtask_id:uuid}/lease/renew", status_code=status.HTTP_204_NO_CONTENT
)
async def renew_lease(
    subtask_id: UUID, db: Database, credentials: SubtaskCredentials
) -> None:
    try:
        async with db.session_context(begin_transaction=True) as session:
            _, grant = await require_subtask(
                session, db, credentials, subtask_id=subtask_id, grant_update=True
            )
            grant.last_renewed_at = cast(DateTime, now("UTC"))
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post(
    "/subtasks/{subtask_id:uuid}/lease/release", status_code=status.HTTP_204_NO_CONTENT
)
async def release_lease(
    subtask_id: UUID, db: Database, credentials: SubtaskCredentials
) -> None:
    try:
        async with db.session_context(begin_transaction=True) as session:
            await models.release_subtask(session, db, subtask_id, credentials)
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/subtasks/{subtask_id:uuid}/replacements/retire")
async def retire_verified_subtask(
    subtask_id: UUID,
    request: s.StreamSubtaskVerifiedRetireRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamSubtaskVerifiedRetirement:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.retire_verified_subtask(
                session, db, subtask_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/subtasks/{subtask_id:uuid}/startup-stop")
async def acknowledge_startup_stop(
    subtask_id: UUID,
    request: s.StreamGraphStartupStopRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> s.StreamGraphStartupStopResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.acknowledge_startup_stop(
                session, db, subtask_id, request, execution
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error
