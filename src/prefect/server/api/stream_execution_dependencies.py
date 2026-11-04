"""Internal HTTP dependency for native stream execution authority."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header, HTTPException, status
from pydantic import ValidationError

from prefect.server.models.streams import NativeStreamError
from prefect.server.schemas.streams import StreamExecutionCredentials


def execution_http_error(error: NativeStreamError) -> HTTPException:
    statuses = {
        "NOT_FOUND": status.HTTP_404_NOT_FOUND,
        "RECOVERY_HORIZON_EXPIRED": status.HTTP_410_GONE,
        "REPLAY_HORIZON_EXPIRED": status.HTTP_410_GONE,
        "PAYLOAD_TOO_LARGE": status.HTTP_413_CONTENT_TOO_LARGE,
        "PROTOCOL_ERROR": status.HTTP_422_UNPROCESSABLE_CONTENT,
        "UNSUPPORTED_BACKEND": status.HTTP_422_UNPROCESSABLE_CONTENT,
        "UNSUPPORTED_PROFILE": status.HTTP_422_UNPROCESSABLE_CONTENT,
    }
    return HTTPException(
        status_code=statuses.get(error.code, status.HTTP_409_CONFLICT),
        detail={"code": error.code, "message": str(error), "retryable": False},
    )


async def execution_credentials(
    generation: Annotated[
        int | None, Header(alias="X-Prefect-Stream-Execution-Generation")
    ] = None,
    holder_id: Annotated[
        UUID | None, Header(alias="X-Prefect-Stream-Execution-Holder")
    ] = None,
    capability: Annotated[
        str | None, Header(alias="X-Prefect-Stream-Execution-Capability")
    ] = None,
) -> StreamExecutionCredentials | None:
    values = (generation, holder_id, capability)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise execution_http_error(
            NativeStreamError(
                "PROTOCOL_ERROR", "Execution binding headers must be complete"
            )
        )
    try:
        return StreamExecutionCredentials(
            generation=generation, holder_id=holder_id, capability=capability
        )
    except ValidationError as error:
        raise execution_http_error(
            NativeStreamError("PROTOCOL_ERROR", "Execution binding headers are invalid")
        ) from error


ExecutionCredentials = Annotated[
    StreamExecutionCredentials | None, Depends(execution_credentials)
]
