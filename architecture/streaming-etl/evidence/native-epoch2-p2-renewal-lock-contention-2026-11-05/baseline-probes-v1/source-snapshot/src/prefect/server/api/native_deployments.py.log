"""Native streaming deployment and worker enrollment routes."""

from collections.abc import Callable, Coroutine
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from starlette.responses import Response

from prefect.server.api.stream_execution_dependencies import ExecutionCredentials
from prefect.server.database import PrefectDBInterface, provide_database_interface
from prefect.server.models._stream_errors import NativeStreamError
from prefect.server.models.native_deployments import (
    apply_control,
    authorize_replacement,
    confirm_replacement,
    enroll_worker,
    finalize_cancelled_launch,
    read_plan,
    read_projection,
    recovery_candidates,
    register_process,
    renew_launch,
    retire_unstarted_launch,
    retire_verified_dead_launch,
)
from prefect.server.models.streams import NativeStreamStore
from prefect.server.schemas.native_deployments import (
    NativeStreamCancelFinalizeRequest,
    NativeStreamControlRequest,
    NativeStreamControlResult,
    NativeStreamDeploymentPlan,
    NativeStreamFlowProjection,
    NativeStreamLaunchHeartbeat,
    NativeStreamLaunchHeartbeatRequest,
    NativeStreamLaunchProjection,
    NativeStreamProcessRegistrationRequest,
    NativeStreamRecoveryCandidate,
    NativeStreamReplacementAuthorization,
    NativeStreamReplacementAuthorizeRequest,
    NativeStreamReplacementConfirmRequest,
    NativeStreamUnstartedRetireRequest,
    NativeStreamVerifiedRetireRequest,
    NativeStreamWorkerEnrollment,
    NativeStreamWorkerEnrollRequest,
)
from prefect.server.utilities.server import PrefectRouter

Database = Annotated[PrefectDBInterface, Depends(provide_database_interface)]


class _SecretRedactingRoute(APIRoute):
    """Keep enrollment capabilities out of request-validation responses."""

    def get_route_handler(
        self,
    ) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        route_handler = super().get_route_handler()

        async def redacted_route_handler(request: Request) -> Response:
            try:
                return await route_handler(request)
            except RequestValidationError as error:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail={
                        "code": "PROTOCOL_ERROR",
                        "message": "Invalid native deployment request",
                        "retryable": False,
                    },
                ) from error

        return redacted_route_handler


# Shared route validation boundary; retain the private spelling for legacy callers.
SecretRedactingRoute = _SecretRedactingRoute


router: PrefectRouter = PrefectRouter(
    prefix="/streams/v1/deployments",
    tags=["Streams"],
    route_class=_SecretRedactingRoute,
)


def _http_error(error: NativeStreamError) -> HTTPException:
    statuses = {
        "NOT_FOUND": status.HTTP_404_NOT_FOUND,
        "REPLAY_HORIZON_EXPIRED": status.HTTP_410_GONE,
        "PROTOCOL_ERROR": status.HTTP_422_UNPROCESSABLE_CONTENT,
        "UNSUPPORTED_BACKEND": status.HTTP_422_UNPROCESSABLE_CONTENT,
        "UNSUPPORTED_PROFILE": status.HTTP_422_UNPROCESSABLE_CONTENT,
    }
    return HTTPException(
        status_code=statuses.get(error.code, status.HTTP_409_CONFLICT),
        detail={
            "code": error.code,
            "message": str(error),
            "retryable": error.code == "METADATA_NOT_READY",
        },
    )


@router.post("/enroll")
async def enroll_native_stream_worker(
    request: NativeStreamWorkerEnrollRequest,
    db: Database,
) -> NativeStreamWorkerEnrollment:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await enroll_worker(session, db, request)
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.post("/launches/register-process")
async def register_native_stream_process(
    request: NativeStreamProcessRegistrationRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> NativeStreamLaunchProjection:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await register_process(session, db, request, execution)
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.post("/launches/heartbeat")
async def renew_native_stream_launch(
    request: NativeStreamLaunchHeartbeatRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> NativeStreamLaunchHeartbeat:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await renew_launch(session, db, request, execution)
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.post("/launches/retire-unstarted")
async def retire_unstarted_native_stream_launch(
    request: NativeStreamUnstartedRetireRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> NativeStreamControlResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            result = await retire_unstarted_launch(session, db, request, execution)
        await NativeStreamStore(db, execution).release_flow(request.flow_run_id)
        return result
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.post("/replacements/retire-verified")
async def retire_verified_native_stream_launch(
    request: NativeStreamVerifiedRetireRequest,
    db: Database,
) -> NativeStreamControlResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            result = await retire_verified_dead_launch(session, db, request)
        await NativeStreamStore(db).release_flow(
            request.flow_run_id, verified_server=True
        )
        return result
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.get("/replacements/candidates")
async def read_native_stream_recovery_candidates(
    worker_id: UUID,
    work_pool_id: UUID,
    host_id: UUID,
    db: Database,
) -> list[NativeStreamRecoveryCandidate]:
    try:
        async with db.session_context() as session:
            return await recovery_candidates(
                session,
                db,
                worker_id=worker_id,
                work_pool_id=work_pool_id,
                host_id=host_id,
            )
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.post("/replacements/authorize")
async def authorize_native_stream_replacement(
    request: NativeStreamReplacementAuthorizeRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> NativeStreamReplacementAuthorization:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await authorize_replacement(session, db, request, execution)
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.post("/replacements/confirm-death")
async def confirm_native_stream_replacement(
    request: NativeStreamReplacementConfirmRequest,
    db: Database,
) -> NativeStreamWorkerEnrollment:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await confirm_replacement(session, db, request)
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.post("/control/drain")
async def drain_native_stream_sources(
    request: NativeStreamControlRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> NativeStreamControlResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await apply_control(session, db, request, execution, kind="DRAIN")
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.post("/control/cancel")
async def cancel_native_stream_writes(
    request: NativeStreamControlRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> NativeStreamControlResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await apply_control(session, db, request, execution, kind="CANCEL")
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.post("/control/finalize-cancel")
async def finalize_native_stream_cancellation(
    request: NativeStreamCancelFinalizeRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> NativeStreamControlResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            result = await finalize_cancelled_launch(session, db, request, execution)
        await NativeStreamStore(db, execution).release_flow(
            request.flow_run_id, verified_server=execution is None
        )
        return result
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.get("/{flow_run_id}/plan")
async def read_native_stream_deployment_plan(
    flow_run_id: UUID,
    db: Database,
) -> NativeStreamDeploymentPlan:
    try:
        async with db.session_context() as session:
            return await read_plan(session, db, flow_run_id)
    except NativeStreamError as error:
        raise _http_error(error) from error


@router.get("/{flow_run_id}/projection")
async def read_native_stream_flow_projection(
    flow_run_id: UUID,
    db: Database,
) -> NativeStreamFlowProjection:
    try:
        async with db.session_context() as session:
            return await read_projection(session, db, flow_run_id)
    except NativeStreamError as error:
        raise _http_error(error) from error
