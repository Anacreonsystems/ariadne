"""Authenticated native stream storage routes."""

from collections.abc import Awaitable
from typing import Annotated, TypeVar
from uuid import UUID

from fastapi import Depends, HTTPException, Query, status

from prefect.server.api.stream_execution_dependencies import ExecutionCredentials
from prefect.server.database import PrefectDBInterface, provide_database_interface
from prefect.server.models.stream_executions import (
    abort_unsealed_execution,
    claim_execution,
    prepare_recovery,
    seal_manifest,
    takeover_execution,
)
from prefect.server.models.streams import NativeStreamError, NativeStreamStore
from prefect.server.schemas.streams import (
    ConsumerBinding,
    SourceAdmission,
    SourceBinding,
    StreamCompletionResult,
    StreamConsumerCompleteRequest,
    StreamConsumerFinishRequest,
    StreamConsumerOpenRequest,
    StreamConsumerReadRequest,
    StreamCreateRequest,
    StreamDelivery,
    StreamEnvelope,
    StreamExecutionBinding,
    StreamExecutionClaimRequest,
    StreamExecutionTakeoverRequest,
    StreamInfo,
    StreamManifestSealRequest,
    StreamManifestSealResult,
    StreamOutputFailRequest,
    StreamOutputRejectRequest,
    StreamOutputReserveRequest,
    StreamRecoveryPrepareRequest,
    StreamRecoveryResult,
    StreamReleaseRequest,
    StreamReleaseResult,
    StreamSourceAppendRequest,
    StreamSourceClaimRequest,
    StreamSourceCloseRequest,
    StreamSourceFailRequest,
    StreamSourceFinishRequest,
    StreamSourceOpenRequest,
    StreamSourceRejectRequest,
    StreamSourceReserveRequest,
    StreamSubmissionLookupRequest,
    StreamSubmissionManifest,
    StreamSubmissionReserveRequest,
    StreamUnsealedAbortRequest,
    StreamUnsealedAbortResult,
)
from prefect.server.utilities.server import PrefectRouter

T = TypeVar("T")
Database = Annotated[PrefectDBInterface, Depends(provide_database_interface)]

router: PrefectRouter = PrefectRouter(prefix="/streams/v1", tags=["Streams"])


def _stream_http_error(error: NativeStreamError) -> HTTPException:
    statuses = {
        "NOT_FOUND": status.HTTP_404_NOT_FOUND,
        "REPLAY_HORIZON_EXPIRED": status.HTTP_410_GONE,
        "RECOVERY_HORIZON_EXPIRED": status.HTTP_410_GONE,
        "PAYLOAD_TOO_LARGE": status.HTTP_413_CONTENT_TOO_LARGE,
        "PROTOCOL_ERROR": status.HTTP_422_UNPROCESSABLE_CONTENT,
        "UNSUPPORTED_BACKEND": status.HTTP_422_UNPROCESSABLE_CONTENT,
    }
    return HTTPException(
        status_code=statuses.get(error.code, status.HTTP_409_CONFLICT),
        detail={
            "code": error.code,
            "message": str(error),
            "retryable": error.code == "METADATA_NOT_READY",
        },
    )


async def _call(operation: Awaitable[T]) -> T:
    try:
        return await operation
    except NativeStreamError as error:
        raise _stream_http_error(error) from error


@router.post("/executions/claim")
async def claim_stream_execution(
    request: StreamExecutionClaimRequest,
    db: Database,
) -> StreamExecutionBinding:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await claim_execution(session, db, request)
    except NativeStreamError as error:
        raise _stream_http_error(error) from error


@router.post("/executions/takeover")
async def takeover_stream_execution(
    request: StreamExecutionTakeoverRequest,
    db: Database,
) -> StreamExecutionBinding:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await takeover_execution(session, db, request)
    except NativeStreamError as error:
        raise _stream_http_error(error) from error


@router.post("/executions/abort-unsealed")
async def abort_unsealed_stream_execution(
    request: StreamUnsealedAbortRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamUnsealedAbortResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await abort_unsealed_execution(session, db, request, execution)
    except NativeStreamError as error:
        raise _stream_http_error(error) from error


@router.post("/executions/prepare-recovery")
async def prepare_stream_recovery(
    request: StreamRecoveryPrepareRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamRecoveryResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await prepare_recovery(session, db, request, execution)
    except NativeStreamError as error:
        raise _stream_http_error(error) from error


@router.post("/executions/seal-manifest")
async def seal_stream_manifest(
    request: StreamManifestSealRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamManifestSealResult:
    try:
        async with db.session_context(begin_transaction=True) as session:
            return await seal_manifest(session, db, request, execution)
    except NativeStreamError as error:
        raise _stream_http_error(error) from error


@router.post("/submissions/reserve")
async def reserve_submission(
    request: StreamSubmissionReserveRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamSubmissionManifest:
    return await _call(NativeStreamStore(db, execution).reserve_submission(request))


@router.post("/submissions/lookup")
async def lookup_submission(
    request: StreamSubmissionLookupRequest,
    db: Database,
) -> StreamSubmissionManifest:
    return await _call(NativeStreamStore(db).lookup_submission(request))


@router.post("/create")
async def create_stream(
    request: StreamCreateRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamInfo:
    return await _call(
        NativeStreamStore(db, execution).create(
            request.flow_run_id,
            request.stream_id,
            limits=request.limits,
            protocol_version=request.protocol_version,
        )
    )


@router.post("/outputs/reserve")
async def reserve_output(
    request: StreamOutputReserveRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamInfo:
    return await _call(
        NativeStreamStore(db, execution).reserve_output(
            request.flow_run_id,
            request.stream_id,
            request.task_run_id,
            limits=request.limits,
            protocol_version=request.protocol_version,
        )
    )


@router.post("/outputs/reject")
async def reject_output(
    request: StreamOutputRejectRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamInfo:
    return await _call(
        NativeStreamStore(db, execution).reject_reserved_output(
            request.flow_run_id,
            request.stream_id,
            request.task_run_id,
            request.error,
        )
    )


@router.post("/outputs/fail")
async def fail_output(
    request: StreamOutputFailRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamInfo:
    return await _call(
        NativeStreamStore(db, execution).fail_task_output(
            request.flow_run_id,
            request.stream_id,
            request.task_run_id,
            request.attempt,
            request.error,
        )
    )


@router.post("/consumers/open")
async def open_consumer(
    request: StreamConsumerOpenRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> ConsumerBinding:
    return await _call(
        NativeStreamStore(db, execution).open_consumer(
            request.input_stream_id,
            request.output_stream_id,
            request.task_run_id,
            request.owner_id,
            request.attempt,
            metadata_lag=True,
            require_reserved_output=True,
        )
    )


@router.post("/consumers/read")
async def read_consumer(
    request: StreamConsumerReadRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamDelivery | None:
    return await _call(NativeStreamStore(db, execution).read(request.binding))


@router.post("/consumers/complete")
async def complete_consumer(
    request: StreamConsumerCompleteRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamCompletionResult:
    return await _call(
        NativeStreamStore(db, execution).complete(
            request.binding,
            request.token,
            request.kind,
            request.value,
            request.managed_state,
        )
    )


@router.post("/consumers/finish", status_code=status.HTTP_204_NO_CONTENT)
async def finish_consumer(
    request: StreamConsumerFinishRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> None:
    await _call(
        NativeStreamStore(db, execution).finish_consumer(request.binding, request.error)
    )


@router.post("/sources/claim")
async def claim_source(
    request: StreamSourceClaimRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> SourceBinding:
    return await _call(
        NativeStreamStore(db, execution).claim_source(
            request.stream_id,
            request.task_run_id,
            request.owner_id,
            request.attempt,
        )
    )


@router.post("/sources/reserve")
async def reserve_source(
    request: StreamSourceReserveRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamInfo:
    return await _call(
        NativeStreamStore(db, execution).reserve_source(
            request.flow_run_id,
            request.stream_id,
            request.task_run_id,
            limits=request.limits,
            protocol_version=request.protocol_version,
            source_protocol=request.source_protocol,
        )
    )


@router.post("/sources/reject")
async def reject_source(
    request: StreamSourceRejectRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamInfo:
    return await _call(
        NativeStreamStore(db, execution).reject_reserved_source(
            request.flow_run_id,
            request.stream_id,
            request.task_run_id,
            request.error,
        )
    )


@router.post("/sources/fail")
async def fail_source(
    request: StreamSourceFailRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamInfo:
    return await _call(
        NativeStreamStore(db, execution).fail_task_source(
            request.flow_run_id,
            request.stream_id,
            request.task_run_id,
            request.attempt,
            request.error,
        )
    )


@router.post("/sources/open")
async def open_source(
    request: StreamSourceOpenRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> SourceAdmission:
    return await _call(
        NativeStreamStore(db, execution).open_source(
            request.stream_id,
            request.task_run_id,
            request.owner_id,
            request.attempt,
            protocol_version=request.protocol_version,
            source_protocol=request.source_protocol,
            metadata_lag=True,
        )
    )


@router.post("/sources/append")
async def append_source(
    request: StreamSourceAppendRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamEnvelope:
    return await _call(
        NativeStreamStore(db, execution).append(
            request.binding, request.position, request.value
        )
    )


@router.post("/sources/finish", status_code=status.HTTP_204_NO_CONTENT)
async def finish_source(
    request: StreamSourceFinishRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> None:
    await _call(
        NativeStreamStore(db, execution).finish_source(request.binding, request.error)
    )


@router.post("/sources/close", status_code=status.HTTP_204_NO_CONTENT)
async def close_source(
    request: StreamSourceCloseRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> None:
    await _call(
        NativeStreamStore(db, execution).close_source(
            request.binding, request.expected_position
        )
    )


@router.post("/release")
async def release_streams(
    request: StreamReleaseRequest,
    db: Database,
    execution: ExecutionCredentials,
) -> StreamReleaseResult:
    released = await _call(
        NativeStreamStore(db, execution).release_flow(request.flow_run_id)
    )
    return StreamReleaseResult(released=released)


@router.get("/{stream_id:uuid}")
async def read_stream(
    stream_id: UUID,
    db: Database,
) -> StreamInfo:
    return await _call(NativeStreamStore(db).status(stream_id))


@router.get("/{stream_id:uuid}/prefix")
async def read_stream_prefix(
    stream_id: UUID,
    limit: Annotated[int, Query(ge=1)],
    db: Database,
) -> list[StreamEnvelope]:
    return await _call(NativeStreamStore(db).read_prefix(stream_id, limit))
