"""Trusted worker controls for persisted physical streaming assignments."""

import base64
import binascii
import hmac
from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Request, status

from prefect.server.api.native_deployments import SecretRedactingRoute
from prefect.server.api.streams import stream_http_error
from prefect.server.database import PrefectDBInterface, provide_database_interface
from prefect.server.models import stream_workers as models
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.schemas import stream_subtasks as s
from prefect.server.utilities.server import PrefectRouter
from prefect.settings import PREFECT_SERVER_API_AUTH_STRING

Database = Annotated[PrefectDBInterface, Depends(provide_database_interface)]


async def trusted_worker(request: Request) -> None:
    """Keep worker controls inside the existing protected Basic boundary."""
    expected = PREFECT_SERVER_API_AUTH_STRING.value()
    header = request.headers.get("Authorization", "")
    try:
        scheme, token = header.split()
        if scheme != "Basic" or not expected:
            raise ValueError("Basic authentication is required")
        decoded = base64.b64decode(token, validate=True)
        if not hmac.compare_digest(decoded, expected.encode("utf-8")):
            raise ValueError("Basic authentication differs")
    except (ValueError, binascii.Error, UnicodeError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized"
        ) from None


router: PrefectRouter = PrefectRouter(
    prefix="/streams/v2/workers",
    tags=["Streams"],
    route_class=SecretRedactingRoute,
    dependencies=[Depends(trusted_worker)],
)


@router.post("/{worker_id:uuid}/poll")
async def poll_assignments(
    worker_id: UUID, request: s.StreamWorkerPollRequest, db: Database
) -> list[s.StreamWorkerAssignment]:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.poll_assignments(session, db, worker_id, request)
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{worker_id:uuid}/subtasks/{subtask_id:uuid}/claim")
async def claim_subtask(
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamSubtaskClaimRequest,
    db: Database,
) -> s.StreamSubtaskGrant:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.claim_subtask(
                session, db, worker_id, subtask_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{worker_id:uuid}/subtasks/{subtask_id:uuid}/register")
async def register_subtask(
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerRegistrationRequest,
    db: Database,
) -> s.StreamSubtaskGrant:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.register_subtask(
                session, db, worker_id, subtask_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{worker_id:uuid}/subtasks/{subtask_id:uuid}/renew")
async def renew_subtask(
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerRenewRequest,
    db: Database,
) -> s.StreamSubtaskGrant:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.renew_subtask(
                session, db, worker_id, subtask_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{worker_id:uuid}/subtasks/{subtask_id:uuid}/replacements/authorize")
async def authorize_replacement(
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerReplacementAuthorizeRequest,
    db: Database,
) -> s.StreamSubtaskReplacementAuthorization:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.authorize_replacement(
                session, db, worker_id, subtask_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{worker_id:uuid}/subtasks/{subtask_id:uuid}/replacements/confirm")
async def confirm_replacement(
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerReplacementConfirmRequest,
    db: Database,
) -> s.StreamSubtaskGrant:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.confirm_replacement(
                session, db, worker_id, subtask_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{worker_id:uuid}/subtasks/{subtask_id:uuid}/replacements/retire")
async def retire_verified_subtask(
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerVerifiedRetireRequest,
    db: Database,
) -> s.StreamSubtaskVerifiedRetirement:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.retire_verified_subtask(
                session, db, worker_id, subtask_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{worker_id:uuid}/subtasks/{subtask_id:uuid}/startup-stop")
async def record_startup_stop(
    worker_id: UUID,
    subtask_id: UUID,
    request: s.StreamWorkerStartupStopRequest,
    db: Database,
) -> s.StreamGraphStartupStopResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.record_startup_stop(
                session, db, worker_id, subtask_id, request
            )
    except NativeStreamError as error:
        raise stream_http_error(error) from error


@router.post("/{worker_id:uuid}/graphs/{graph_id:uuid}/startup-abort")
async def abort_startup(
    worker_id: UUID,
    graph_id: UUID,
    request: s.StreamWorkerStartupAbortRequest,
    db: Database,
) -> s.StreamGraphPlan:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await models.abort_startup(session, db, worker_id, graph_id, request)
    except NativeStreamError as error:
        raise stream_http_error(error) from error
